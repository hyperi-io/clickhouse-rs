//! Bounded-backoff retry for provably-idempotent TCP operations.
//!
//! - [`RetryPolicy`] -- caller knob (attempt count + bounded-exponential
//!   backoff), surfaced as `Client::with_tcp_retry`.
//! - [`run_with_retry`] -- the acquire+dispatch loop the
//!   [`crate::tcp::client_ext`] helpers wrap around repeatable operations
//!   (SELECT open, opt-in `ExecuteQuery`). **An in-flight INSERT is NEVER
//!   routed through here.**
//!
//! The retried region is `pool.get()` + the operation's *issue* only
//! (for a SELECT, opening the cursor): no row is consumed inside it, so a
//! transient connect/issue failure -- including a silently-dead pooled
//! connection -- replays safely; a mid-stream failure surfaces unchanged.
//! Endpoint failover lives in [`crate::tcp::pool`] (`create`
//! round-robins); this adds backoff passes on top. (Origin: differential
//! review C1, cpp `RetryGuard`.)

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use deadpool::managed::{Object, PoolError};

use crate::error::{Error, Result};
use crate::tcp::pool::{NativePool, TcpConnectionManager};

/// Bounded-backoff retry policy for idempotent TCP operations.
///
/// `None` (no policy) is a single acquire pass with no sleep -- endpoint
/// failover still happens inside `create`. A policy adds extra
/// acquire+dispatch passes with bounded-exponential backoff between them.
/// Backoff is deterministic (no jitter) in v1 to avoid an RNG dep; add
/// jitter at the call site if a reconnect storm synchronises retries.
#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    /// Total attempts including the first (`1` = no retry; matches the
    /// `retry: None` semantics).
    pub max_attempts: u32,
    /// Backoff before the 2nd attempt; doubles each attempt up to
    /// [`Self::max_backoff`].
    pub initial_backoff: Duration,
    /// Cap on a single backoff sleep, bounding the worst-case wait.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    /// Three attempts (two retries), 100ms initial backoff capped at 2s.
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(2),
        }
    }
}

/// Backoff before attempt `attempt + 1`, i.e. `backoff(_, 1)` is the
/// wait taken after the first attempt fails and before the second.
///
/// Computes `min(initial_backoff * 2^(attempt - 1), max_backoff)` with
/// saturating arithmetic so a large `attempt` can never overflow or
/// panic -- it simply saturates to `max_backoff`. `attempt` is 1-based
/// to match the retry loop's `1..=max_attempts` counter; `attempt == 0`
/// is treated as `1` (the shift would otherwise underflow).
pub(crate) fn backoff(policy: &RetryPolicy, attempt: u32) -> Duration {
    // 1-based; guard attempt == 0 so the `- 1` below cannot underflow.
    let exp = attempt.saturating_sub(1);
    // 2^exp as a multiplier, saturating: once exp reaches ~63 the
    // multiplier overflows u64, so cap the shift and let the
    // checked_mul below saturate to max_backoff regardless.
    let initial_ms = policy.initial_backoff.as_millis();
    // u128 math keeps the doubling exact until we clamp to max_backoff;
    // `1u128 << exp` saturates by capping `exp` to a value whose shift
    // still fits, after which the multiply dwarfs max_backoff anyway.
    let multiplier: u128 = if exp >= 127 { u128::MAX } else { 1u128 << exp };
    let scaled_ms = initial_ms.saturating_mul(multiplier);
    let max_ms = policy.max_backoff.as_millis();
    let capped_ms = scaled_ms.min(max_ms);
    // capped_ms <= max_backoff (in ms), which fits a Duration; the cast
    // is safe because we already clamped to max_backoff's millis.
    Duration::from_millis(capped_ms.min(u128::from(u64::MAX)) as u64)
}

/// True when `e` is worth replaying on a fresh connection: either the
/// crate's conservative [`Error::is_retriable`] says so (Network /
/// TimedOut / known-transient server codes) OR the error came from the
/// connection-open / pool-acquire path (an unresolvable host, a
/// refused connect, a pool-acquire timeout). Connect failures are
/// transient by nature -- a different endpoint or a moment later may
/// succeed -- so they retry the same way `is_retriable` transport
/// errors do.
///
/// Deliberately conservative: anything we cannot positively classify as
/// a transient transport/connect failure (a server Exception with a
/// non-transient code, a schema mismatch, a serde error) returns
/// `false` so it surfaces immediately rather than being masked under a
/// retry loop.
pub(crate) fn is_retriable_transport(e: &Error) -> bool {
    if e.is_retriable() {
        return true;
    }
    match e {
        // Allowlist, NOT a blanket `tcp:` prefix: only pool-acquire
        // failures (`map_pool_error` stamps `"tcp pool: ..."`) and
        // per-connect DNS resolve failures are transient -- a fresh
        // acquire (different endpoint, a moment later) may succeed. Every
        // OTHER `tcp:` Custom error is protocol/state/config (actor busy,
        // no INSERT session, block-size cap, TLS feature off, bad SNI)
        // and must NOT retry -- a new connection won't fix it.
        Error::Custom(msg) => {
            msg.starts_with("tcp pool:")
                || msg.starts_with("tcp: cannot resolve")
                || msg.ends_with("resolved to no addresses")
        }
        // ECONNREFUSED / reset / abort / connect-timeout reach us as
        // `Error::Other(io::Error)`; retry only transient io kinds, not
        // every `Other`.
        Error::Other(boxed) => boxed
            .downcast_ref::<std::io::Error>()
            .is_some_and(is_transient_io_kind),
        _ => false,
    }
}

/// `true` for the `io::ErrorKind`s that represent a transient
/// connect/network failure worth replaying on a fresh connection
/// (another endpoint, or the same one a moment later). Conservative:
/// anything not positively transient (e.g. `PermissionDenied`,
/// `InvalidInput`) is excluded.
fn is_transient_io_kind(e: &std::io::Error) -> bool {
    use std::io::ErrorKind;
    matches!(
        e.kind(),
        ErrorKind::ConnectionRefused
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::NotConnected
            | ErrorKind::BrokenPipe
            | ErrorKind::TimedOut
            | ErrorKind::Interrupted
            | ErrorKind::UnexpectedEof
            | ErrorKind::AddrNotAvailable
            | ErrorKind::HostUnreachable
            | ErrorKind::NetworkUnreachable
            | ErrorKind::NetworkDown
    )
}

/// Map a deadpool [`PoolError`] into a crate [`Error`].
///
/// The `Backend(Error)` variant already carries our typed error (a
/// connect/resolve failure or a handshake error from
/// [`crate::tcp::pool::TcpConnectionManager::create`]), so it is
/// preserved as-is -- this keeps a `Network` / `TimedOut` connect
/// failure typed so [`is_retriable_transport`] (and the caller) can see
/// it. Pool-internal conditions (acquire timeout, closed pool, missing
/// runtime, hook failure) carry no crate error, so they map to
/// `Error::Custom` with the `tcp pool:` prefix that
/// [`is_retriable_transport`] recognises as a transient acquire
/// failure.
pub(crate) fn map_pool_error(e: PoolError<Error>) -> Error {
    match e {
        PoolError::Backend(err) => err,
        other => Error::Custom(format!("tcp pool: {other}")),
    }
}

/// Acquire a connection and run `op`, retrying transient
/// transport/connect failures per `retry`.
///
/// `retry == None` (or a policy with `max_attempts <= 1`) means a
/// single acquire pass with no sleep -- endpoint failover still happens
/// inside the pool manager's `create`. With a policy of N attempts, a
/// failed attempt whose error [`is_retriable_transport`] sleeps
/// [`backoff`] then re-acquires (which round-robins to a fresh endpoint
/// start) and re-runs `op`; a non-retriable error short-circuits
/// immediately. After the final attempt the last error surfaces
/// unchanged (typed).
///
/// `op` is `Fn` (callable once per attempt) and receives the freshly
/// acquired [`Object`], which derefs to
/// [`crate::tcp::connection_actor::ConnectionHandle`].
pub(crate) async fn run_with_retry<T, F, Fut>(
    pool: &Arc<NativePool>,
    retry: Option<RetryPolicy>,
    op: F,
) -> Result<T>
where
    F: Fn(Object<TcpConnectionManager>) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    // Build the per-attempt fallible block: acquire (mapping a pool
    // error to a typed, retry-classifiable crate error) then run `op`.
    // A `pool.get()` failure is folded into the same `Result` so a
    // connect failure is classified by `is_retriable_transport`
    // identically to an `op` failure.
    let attempt_once = || async {
        let conn = pool.get().await.map_err(map_pool_error)?;
        op(conn).await
    };
    retry_loop(retry, attempt_once).await
}

/// Core retry/backoff loop, factored out so it is unit-testable
/// without a live pool: it drives any `FnMut() -> Future<Result<T>>`.
///
/// `attempt` is 1-based. On a retriable failure with attempts
/// remaining it sleeps [`backoff`] then retries; otherwise it returns
/// the error. `None` / `max_attempts <= 1` collapses to a single call.
async fn retry_loop<T, A, Fut>(retry: Option<RetryPolicy>, mut attempt_once: A) -> Result<T>
where
    A: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    // None => exactly one pass, no sleep (failover still happens inside
    // `create`). A policy with max_attempts 0 is treated as 1 so the
    // loop always runs at least once.
    let policy = retry.unwrap_or(RetryPolicy {
        max_attempts: 1,
        initial_backoff: Duration::ZERO,
        max_backoff: Duration::ZERO,
    });
    let max_attempts = policy.max_attempts.max(1);

    for attempt in 1..=max_attempts {
        match attempt_once().await {
            Ok(v) => return Ok(v),
            // Retriable with attempts left: back off, then loop to a
            // fresh acquire (which round-robins to a new endpoint start).
            Err(e) if attempt < max_attempts && is_retriable_transport(&e) => {
                tokio::time::sleep(backoff(&policy, attempt)).await;
            }
            // Non-retriable, or the final attempt: surface it.
            Err(e) => return Err(e),
        }
    }
    // The final attempt's `attempt < max_attempts` guard is false, so it
    // always hits the `Err(e) => return Err(e)` arm; with `max_attempts
    // >= 1` the loop never falls through here.
    unreachable!("retry_loop returns on every attempt")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::time::Duration;

    fn policy(max_attempts: u32, initial_ms: u64, max_ms: u64) -> RetryPolicy {
        RetryPolicy {
            max_attempts,
            initial_backoff: Duration::from_millis(initial_ms),
            max_backoff: Duration::from_millis(max_ms),
        }
    }

    #[test]
    fn backoff_doubles_then_caps() {
        let p = policy(10, 100, 2000);
        // 1-based: attempt 1 -> 100ms, 2 -> 200, 3 -> 400, 4 -> 800,
        // 5 -> 1600, 6 -> 3200 capped to 2000, and onwards stays capped.
        assert_eq!(backoff(&p, 1), Duration::from_millis(100));
        assert_eq!(backoff(&p, 2), Duration::from_millis(200));
        assert_eq!(backoff(&p, 3), Duration::from_millis(400));
        assert_eq!(backoff(&p, 4), Duration::from_millis(800));
        assert_eq!(backoff(&p, 5), Duration::from_millis(1600));
        assert_eq!(backoff(&p, 6), Duration::from_millis(2000));
        assert_eq!(backoff(&p, 7), Duration::from_millis(2000));
    }

    #[test]
    fn backoff_attempt_zero_is_initial() {
        // Defensive: attempt 0 must not underflow; treated as attempt 1.
        let p = policy(10, 100, 2000);
        assert_eq!(backoff(&p, 0), Duration::from_millis(100));
    }

    #[test]
    fn backoff_saturates_on_huge_attempt() {
        // A pathologically large attempt must saturate to max_backoff,
        // never overflow/panic.
        let p = policy(u32::MAX, 100, 2000);
        assert_eq!(backoff(&p, u32::MAX), Duration::from_millis(2000));
        assert_eq!(backoff(&p, 1000), Duration::from_millis(2000));
    }

    #[test]
    fn backoff_caps_even_with_huge_initial() {
        // initial > max: every backoff clamps to max immediately.
        let p = policy(5, 10_000, 2000);
        assert_eq!(backoff(&p, 1), Duration::from_millis(2000));
        assert_eq!(backoff(&p, 3), Duration::from_millis(2000));
    }

    fn server_error() -> Error {
        // Non-transient server code (UNKNOWN_TABLE = 60): NOT retriable.
        Error::ServerException {
            code: 60,
            name: None,
            message: "no such table".to_string(),
            stack_trace: None,
        }
    }

    fn transient_pool_error() -> Error {
        // Mirrors what `map_pool_error` stamps for a non-Backend pool
        // error -- recognised as a transient acquire failure.
        Error::Custom("tcp pool: Timeout occurred while creating a new object".to_string())
    }

    #[test]
    fn is_retriable_transport_classification() {
        assert!(is_retriable_transport(&Error::TimedOut));
        assert!(is_retriable_transport(&transient_pool_error()));
        assert!(is_retriable_transport(&Error::Custom(
            "tcp: cannot resolve \"bad:9000\"".to_string()
        )));
        // A retriable server code rides through is_retriable().
        assert!(is_retriable_transport(&Error::ServerException {
            code: 209, // SOCKET_TIMEOUT
            name: None,
            message: String::new(),
            stack_trace: None,
        }));
        // Resolver "no addresses" is transient (DNS blip / next endpoint).
        assert!(is_retriable_transport(&Error::Custom(
            "tcp: \"bad:9000\" resolved to no addresses".to_string()
        )));
        // Non-transient server error is NOT retriable.
        assert!(!is_retriable_transport(&server_error()));
        // An unrelated Custom string is NOT retriable (tight prefix).
        assert!(!is_retriable_transport(&Error::Custom(
            "some serde failure".to_string()
        )));
        // 18D-B: protocol/state `tcp:` Custom errors must NOT retry --
        // a fresh connection cannot fix them, and the old blanket
        // `starts_with("tcp:")` wrongly treated them as transient.
        for non_transient in [
            "tcp: actor busy in INSERT",
            "tcp: no INSERT session active",
            "tcp: INSERT block of 99 bytes exceeds the cap",
            "tcp: TLS requested but the `native-tls-rustls` feature is not enabled",
        ] {
            assert!(
                !is_retriable_transport(&Error::Custom(non_transient.to_string())),
                "must NOT retry: {non_transient}"
            );
        }
    }

    #[test]
    fn is_retriable_transport_classifies_connect_io_kinds() {
        use std::io;
        // A refused connect round-trips io::Error -> Error::Other and
        // must be retriable (the common multi-host failover case).
        let refused: Error =
            io::Error::new(io::ErrorKind::ConnectionRefused, "refused").into();
        assert!(matches!(refused, Error::Other(_)));
        assert!(is_retriable_transport(&refused));

        let reset: Error = io::Error::new(io::ErrorKind::ConnectionReset, "reset").into();
        assert!(is_retriable_transport(&reset));

        // A non-transient io kind wrapped in Other stays terminal.
        let denied: Error =
            io::Error::new(io::ErrorKind::PermissionDenied, "denied").into();
        assert!(matches!(denied, Error::Other(_)));
        assert!(!is_retriable_transport(&denied));

        // A non-io Other is terminal.
        let other = Error::Other("plain string boxed".into());
        assert!(!is_retriable_transport(&other));
    }

    #[tokio::test(start_paused = true)]
    async fn retry_then_succeed_sleeps_expected_backoffs() {
        // Fail twice (retriable) then succeed on the third attempt.
        // start_paused freezes time; tokio auto-advances over the
        // sleeps, and we assert the total elapsed equals the sum of the
        // expected backoffs (100ms + 200ms).
        let calls = Cell::new(0u32);
        let p = policy(3, 100, 2000);
        let start = tokio::time::Instant::now();
        let r: Result<u32> = retry_loop(Some(p), || {
            let n = calls.get() + 1;
            calls.set(n);
            async move {
                if n < 3 {
                    Err(Error::TimedOut)
                } else {
                    Ok(n)
                }
            }
        })
        .await;
        assert_eq!(r.unwrap(), 3);
        assert_eq!(calls.get(), 3, "should have run exactly three attempts");
        assert_eq!(
            start.elapsed(),
            Duration::from_millis(300),
            "should have slept 100ms + 200ms between the three attempts"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retry_exhaustion_surfaces_last_error() {
        // Always fails retriable: after max_attempts the last error
        // surfaces.
        let calls = Cell::new(0u32);
        let p = policy(3, 100, 2000);
        let r: Result<u32> = retry_loop(Some(p), || {
            calls.set(calls.get() + 1);
            async { Err(Error::TimedOut) }
        })
        .await;
        assert!(matches!(r, Err(Error::TimedOut)));
        assert_eq!(calls.get(), 3, "should have exhausted all three attempts");
    }

    #[tokio::test(start_paused = true)]
    async fn non_retriable_short_circuits_without_sleep() {
        // A non-retriable error returns immediately: one attempt, no
        // sleep, no extra passes.
        let calls = Cell::new(0u32);
        let p = policy(5, 100, 2000);
        let start = tokio::time::Instant::now();
        let r: Result<u32> = retry_loop(Some(p), || {
            calls.set(calls.get() + 1);
            async { Err(server_error()) }
        })
        .await;
        assert!(matches!(r, Err(Error::ServerException { code: 60, .. })));
        assert_eq!(calls.get(), 1, "non-retriable must not retry");
        assert_eq!(start.elapsed(), Duration::ZERO, "must not sleep");
    }

    #[tokio::test(start_paused = true)]
    async fn none_policy_single_pass_no_sleep() {
        // None => exactly one attempt, no sleep, even on a retriable
        // error (failover still happens inside create()).
        let calls = Cell::new(0u32);
        let start = tokio::time::Instant::now();
        let r: Result<u32> = retry_loop(None, || {
            calls.set(calls.get() + 1);
            async { Err(Error::TimedOut) }
        })
        .await;
        assert!(matches!(r, Err(Error::TimedOut)));
        assert_eq!(calls.get(), 1, "None must run exactly one pass");
        assert_eq!(start.elapsed(), Duration::ZERO, "None must not sleep");
    }

    #[tokio::test(start_paused = true)]
    async fn none_policy_success_first_try() {
        let r: Result<u32> = retry_loop(None, || async { Ok(7) }).await;
        assert_eq!(r.unwrap(), 7);
    }
}
