//! deadpool-managed connection pool for the TCP transport.
//!
//! Wraps [`crate::tcp::connection_actor::ConnectionHandle`] in a
//! [`deadpool::managed::Pool`] so callers can acquire ready-to-use
//! connections without having to drive the connect + handshake +
//! actor-spawn sequence themselves. `recycle` returns the handle when
//! it is alive (i.e. not poisoned by an I/O failure mid-command) and
//! drops it otherwise -- the next caller then sees a freshly
//! handshaken connection courtesy of [`Manager::create`].
//!
//! The pool is `pub(crate)` in this branch -- the public construction
//! site lands in the next branch as `Client::tcp(addr)`. Live
//! end-to-end coverage (a real ClickHouse server, real handshake,
//! pool acquire + ping + return + re-acquire) also rides with that
//! branch; the unit tests here cover the deadpool wiring against a
//! mock manager.
//!
//! # Recycle semantics
//!
//! [`ConnectionHandle::is_alive`] currently checks the poisoned flag
//! only -- it does not probe the underlying socket. That is sufficient
//! because the actor's reader sub-task poisons on any I/O failure
//! (read EOF, decode error, etc.) and the writer arms poison on any
//! send failure. Channel-closed (the actor task has exited and
//! dropped its receiver) is the one not-yet-detected mode; in
//! practice the actor's reader sub-task is what tears the connection
//! down first, so by the time the channel closes the poisoned flag
//! has already been set. A dedicated channel-closed probe is tracked
//! for a follow-up audit pass.
//!
//! # Timeout defaults
//!
//! - `max_size = 8` mirrors the existing HTTP pool default so a
//!   user switching transports does not silently see a different
//!   concurrency ceiling.
//! - `wait = 30s` bounds the acquire wait when every slot is busy;
//!   callers see a `Pool::get` error instead of hanging.
//! - `create = 10s` bounds connect + handshake; LAN handshakes
//!   complete in <100ms on a healthy server, so 10s only fires under
//!   network failure or a misconfigured backend.
//! - `recycle = 5s` is generous: `is_alive` is a single atomic load.
//!   The timeout exists to insure against a future recycle
//!   implementation that grows a socket probe, not for the current
//!   load-only check.
//!
//! Timeout configuration requires deadpool's `Runtime` to be set,
//! which is why [`build_pool`] threads `Runtime::Tokio1` through.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use deadpool::Runtime;
use deadpool::managed::{self, Metrics, Pool, RecycleError, RecycleResult};

use crate::error::{Error, Result};
use crate::tcp::connect::{self, ConnectKind};
use crate::tcp::connection_actor::{ActorConfig, ConnectionActor, ConnectionHandle};
use crate::tcp::handshake::HandshakeConfig;
use crate::tcp::retry::RetryPolicy;

/// Default maximum number of pooled TCP connections per pool.
///
/// Matches the existing HTTP pool default so a user switching
/// transports keeps the same concurrency ceiling. Callers that need
/// a different value tune through [`PoolConfig::max_size`] before
/// [`build_pool`] is called.
pub(crate) const DEFAULT_MAX_SIZE: usize = 8;

/// Default upper bound on how long a caller waits for a pool slot.
///
/// Bounded so a saturated pool surfaces an `Err` to the caller
/// instead of blocking indefinitely. 30s mirrors the ceiling the
/// HTTP pool applies to its checkout path; users that need tighter
/// bounds tune through [`PoolConfig::acquire_timeout`].
pub(crate) const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

/// Default upper bound on connect + handshake. Real handshakes
/// against a healthy LAN server complete in tens of milliseconds;
/// 10s is conservative head-room against transient packet loss.
pub(crate) const DEFAULT_CREATE_TIMEOUT: Duration = Duration::from_secs(10);

/// Default upper bound on the recycle (is-alive) check. Recycle is a
/// single atomic load today, so 5s exists purely as insurance
/// against future probe-on-recycle changes.
pub(crate) const DEFAULT_RECYCLE_TIMEOUT: Duration = Duration::from_secs(5);

/// deadpool `Manager` implementation for ClickHouse TCP connections.
///
/// `create` opens a fresh socket via
/// [`crate::tcp::connect::open_handshaken`], drives the handshake,
/// then spawns the connection actor and returns its cheap-clone
/// handle. `recycle` returns `Ok(())` when the handle is alive and
/// drops it otherwise via [`RecycleError::Message`]; deadpool then
/// calls `create` again for the next caller so the pool slot
/// refills.
pub(crate) struct TcpConnectionManager {
    /// Candidate server addresses as the caller supplied them (e.g.
    /// `["ch-1:9000", "ch-2:9000"]`). INVARIANT: non-empty -- the
    /// builders (`Client::tcp` / `with_tcp_addrs`) enforce this, and
    /// [`Manager::create`]'s round-robin modulo arithmetic relies on
    /// it. Each is resolved per connection inside [`Manager::create`]
    /// via the async resolver, not pinned to a `SocketAddr` at
    /// construction -- so a hostname that gains new A records after the
    /// pool is built picks them up on the next connect, and an
    /// unresolvable host surfaces as a pool-acquire error rather than a
    /// constructor panic.
    pub endpoints: Vec<String>,
    /// Round-robin cursor shared across the pool. `create` does a
    /// `fetch_add(1)` to pick the starting endpoint for each connect,
    /// then walks the list from there -- so concurrent `create` calls
    /// (the pool opens connections lazily as slots fill) spread their
    /// starts across endpoints rather than all hammering endpoint 0.
    /// `Arc` because the manager is shared by the pool; `AtomicUsize`
    /// because `create` takes `&self`. Wrapping at `usize::MAX` is
    /// harmless -- the value is only ever used modulo `endpoints.len()`.
    pub next: Arc<AtomicUsize>,
    /// Plain TCP vs TLS selection. Carries the SNI on the TLS arm so
    /// the manager re-uses the same name across reconnects without
    /// having to re-derive it from the address.
    pub kind: ConnectKind,
    /// Handshake parameters (database, credentials, client name,
    /// chunked-mode preference). Cloned into each `create` call so
    /// the manager itself stays cheap to share across the pool.
    pub config: HandshakeConfig,
    /// Maximum connection age; `None` keeps connections for life. See
    /// [`PoolConfig::max_lifetime`].
    pub max_lifetime: Option<Duration>,
    /// Per-packet idle read timeout threaded to each spawned actor; see
    /// [`PoolConfig::read_timeout`]. `None` leaves reads bounded only by
    /// caller-side cancellation.
    pub read_timeout: Option<Duration>,
}

impl managed::Manager for TcpConnectionManager {
    type Type = ConnectionHandle;
    type Error = Error;

    async fn create(&self) -> Result<ConnectionHandle> {
        // Round-robin connect-failover: pick a starting endpoint via the
        // shared atomic cursor, then walk the whole list from there in
        // one pass, returning the first that connects. This delivers
        // multi-host + connect-failover "for free" through deadpool's
        // existing `create` path -- a poisoned connection is dropped on
        // `recycle`, and the next `create` round-robins to a (possibly
        // different) endpoint. `n >= 1` by the non-empty invariant on
        // `endpoints`, so the modulo is safe.
        let n = self.endpoints.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed) % n;
        let mut last_err: Option<Error> = None;
        for i in 0..n {
            let endpoint = &self.endpoints[(start + i) % n];
            // Resolve per connection via tokio's async resolver (not the
            // blocking std `ToSocketAddrs`), so a hostname that gains new
            // A records after the pool is built picks them up on
            // reconnect, and an unresolvable host surfaces as a pool
            // error rather than a constructor panic. First address only
            // -- multi-A-record fan-out is a separate follow-up.
            let resolved = match tokio::net::lookup_host(endpoint).await {
                Ok(mut addrs) => addrs.next().ok_or_else(|| {
                    Error::Custom(format!("tcp: {endpoint:?} resolved to no addresses"))
                }),
                Err(e) => Err(Error::Custom(format!(
                    "tcp: cannot resolve {endpoint:?}: {e}"
                ))),
            };
            let addr = match resolved {
                Ok(addr) => addr,
                Err(e) => {
                    // Remember and try the next endpoint -- a single
                    // unresolvable host must not sink the whole pass.
                    last_err = Some(e);
                    continue;
                }
            };
            match connect::open_handshaken(addr, &self.kind, &self.config).await {
                Ok((stream, hello)) => {
                    return Ok(ConnectionActor::spawn_with_config(
                        stream,
                        hello,
                        ActorConfig {
                            read_timeout: self.read_timeout,
                        },
                    ));
                }
                Err(e) => last_err = Some(e),
            }
        }
        // Every endpoint failed this pass. Surface the last error
        // unchanged (it stays typed, so retry classification sees it);
        // the `unwrap` is sound because `n >= 1` guarantees the loop ran
        // at least once and set `last_err` on its failing path.
        Err(last_err.expect("create loop ran at least once over a non-empty endpoint list"))
    }

    async fn recycle(
        &self,
        obj: &mut ConnectionHandle,
        metrics: &Metrics,
    ) -> RecycleResult<Error> {
        // Drop connections older than max_lifetime (deadpool tracks the
        // creation time in `Metrics`, so no per-handle timestamp needed).
        if let Some(max) = self.max_lifetime
            && metrics.created.elapsed() >= max
        {
            return Err(RecycleError::message("connection exceeded max_lifetime"));
        }
        if obj.is_alive() {
            Ok(())
        } else {
            Err(RecycleError::message("connection poisoned"))
        }
    }
}

/// Concrete pool type for TCP connections.
///
/// Type alias rather than a newtype so users (and the Client
/// integration that follows) interact with deadpool's surface
/// directly -- `pool.get().await` returns a deadpool `Object` that
/// derefs to `ConnectionHandle`, and dropping it returns the
/// connection to the pool.
pub(crate) type NativePool = Pool<TcpConnectionManager>;

/// Pool configuration knobs surfaced to callers. Maps to a
/// [`deadpool::managed::PoolConfig`] at build time but exposes only
/// the dials this transport actually cares about; the deadpool
/// queue-mode and any future hooks are pool-internal concerns.
///
/// Defaults are conservative: 8 connections, 30 s acquire wait,
/// 10 s create timeout, 5 s recycle timeout. Tune by mutating the
/// struct before handing it to [`build_pool`].
#[derive(Clone, Copy, Debug)]
pub(crate) struct PoolConfig {
    /// Maximum number of connections the pool will hold.
    pub max_size: usize,
    /// How long a caller may wait for a free pool slot before
    /// `Pool::get` returns `Err`. `None` waits indefinitely.
    pub acquire_timeout: Option<Duration>,
    /// How long [`Manager::create`] may take before deadpool aborts
    /// it. `None` waits indefinitely.
    pub create_timeout: Option<Duration>,
    /// How long [`Manager::recycle`] may take before deadpool aborts
    /// it. `None` waits indefinitely.
    pub recycle_timeout: Option<Duration>,
    /// Maximum age of a pooled connection. On `recycle`, a connection
    /// older than this is dropped (and a fresh one created on the next
    /// acquire) rather than reused -- bounding connection-state drift on
    /// long-lived pools (server restarts, rotated DNS, server-local
    /// session state), mirroring clickhouse-go's `ConnMaxLifetime`.
    /// `None` (default) keeps connections for life. Idle-socket death is
    /// handled separately by the actor's reader sub-task (poison on
    /// EOF/I/O error); this dial is purely an age bound.
    pub max_lifetime: Option<Duration>,
    /// Per-packet idle read timeout, threaded into every spawned actor.
    /// A query/stream/begin-insert read that goes this long without
    /// receiving ANY packet poisons the connection and surfaces
    /// [`Error::TimedOut`]. The timer resets on each packet, so it bounds
    /// the gap BETWEEN packets, not total query time -- a long streaming
    /// SELECT that keeps delivering blocks never trips it. `None`
    /// (default) leaves reads bounded only by caller-side cancellation.
    pub read_timeout: Option<Duration>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: DEFAULT_MAX_SIZE,
            acquire_timeout: Some(DEFAULT_ACQUIRE_TIMEOUT),
            create_timeout: Some(DEFAULT_CREATE_TIMEOUT),
            recycle_timeout: Some(DEFAULT_RECYCLE_TIMEOUT),
            max_lifetime: None,
            read_timeout: None,
        }
    }
}

/// Aggregated TCP-client knobs the `Client` builder threads through
/// to [`build_pool`]. Held inside `Client` so `with_tcp_pool_*` /
/// `with_tcp_addrs` builders can rebuild the pool with new dials;
/// matches the HTTP `pool_config` shape on the same struct.
///
/// Endpoints are stored as the original strings the caller supplied
/// (e.g. `["127.0.0.1:9000"]`) so we can re-resolve at rebuild time
/// rather than caching a `SocketAddr` that may have gone stale.
#[derive(Clone, Debug, Default)]
pub(crate) struct TcpClientConfig {
    /// Candidate server addresses as the caller supplied them.
    /// Resolved at pool-build time, not at `Client::tcp` time, so a
    /// hostname that gains new A records after construction picks them
    /// up on rebuild. `Client::tcp` / `tcp_tls` set a single-element
    /// list; `with_tcp_addrs` replaces it with the full list. A
    /// default-constructed config (HTTP clients) leaves it empty; the
    /// TCP pool is only built once a constructor has populated it.
    pub endpoints: Vec<String>,
    /// Plain vs TLS transport selection. Defaults to `Plain`; set by
    /// `Client::tcp_tls` under the `native-tls-rustls` feature.
    pub kind: ConnectKindConfig,
    /// Handshake parameters (database, credentials, quota_key).
    pub handshake: HandshakeConfig,
    /// Pool dials.
    pub pool: PoolConfig,
    /// Bounded-backoff retry policy for idempotent operations
    /// (SELECT, opt-in `ExecuteQuery`, Ping). `None` (default) means
    /// no extra passes -- endpoint failover still happens inside
    /// `create`. Consulted at dispatch time by
    /// [`crate::tcp::client_ext`], so changing it does NOT require a
    /// pool rebuild.
    pub retry: Option<RetryPolicy>,
}

/// Cargo-feature-independent mirror of [`ConnectKind`].
///
/// `ConnectKind` itself feature-gates its `Tls` arm so a build
/// without `native-tls-rustls` cannot construct it. `Client` holds
/// the choice unconditionally so its struct shape is identical
/// across feature configurations -- the conversion to `ConnectKind`
/// happens at pool-build time, where the feature gate is in scope.
#[derive(Clone, Debug, Default)]
pub(crate) enum ConnectKindConfig {
    #[default]
    Plain,
    /// TLS over TCP. Held even when `native-tls-rustls` is off so the
    /// struct layout stays stable; pool build then returns an error
    /// if the feature is missing rather than silently downgrading to
    /// plain.
    Tls {
        server_name: String,
    },
}

/// Build a TCP connection pool over the supplied endpoint list,
/// handshake config, and dials. The `Runtime::Tokio1` wiring is
/// mandatory once any timeout is set; we set it unconditionally so a
/// caller adding a timeout later does not see the build silently fail.
///
/// # Panics
///
/// Panics if `endpoints` is empty -- the round-robin in
/// [`Manager::create`] divides by `endpoints.len()`. The `Client`
/// builders assert non-emptiness before reaching here, so an empty
/// list is a programming error, not a runtime condition.
///
/// # Errors
///
/// Returns [`Error::Custom`] when the underlying
/// [`deadpool::managed::PoolBuilder::build`] fails. The only documented
/// build failure today is `BuildError::NoRuntimeSpecified`, which this
/// function rules out by always supplying a runtime -- but we map
/// the error explicitly so a future deadpool revision that adds new
/// build failures still surfaces as a typed crate error rather than
/// a panic.
/// Build-time TLS intent for the TCP pool's connect path.
///
/// Distinguishes "no trust was configured" (-> default anchors are fine)
/// from "a trust WAS configured but could not be resolved" (-> fail
/// closed, never silently fall back to broad default trust). Carried as
/// a build-time-only input rather than on `PoolConfig` because it is not
/// a runtime dial.
#[cfg(feature = "native-tls-rustls")]
pub(crate) enum TcpTls {
    /// Caller never customised trust. The TLS arm resolves the default
    /// native+webpki anchors (happy path).
    NotConfigured,
    /// A trust was configured and resolved to this config.
    Resolved(std::sync::Arc<tokio_rustls::rustls::ClientConfig>),
    /// A trust WAS configured but failed to resolve. Refuse to build a
    /// TLS pool rather than fall back to broad default trust.
    ConfiguredButFailed,
}

pub(crate) fn build_pool(
    endpoints: Vec<String>,
    kind: ConnectKindConfig,
    config: HandshakeConfig,
    pool_cfg: PoolConfig,
    // Build-time TLS intent from the `Client` (its `with_tls_*`
    // builders, or `with_tls_config`). See [`TcpTls`]. Carried as a
    // separate arg rather than on `PoolConfig` because it is a build-
    // time-only input, not a runtime dial.
    #[cfg(feature = "native-tls-rustls")] tls: TcpTls,
) -> Result<NativePool> {
    assert!(
        !endpoints.is_empty(),
        "build_pool requires a non-empty endpoint list"
    );
    let kind = match kind {
        ConnectKindConfig::Plain => ConnectKind::Plain,
        // Resolve the shared trust to one ClientConfig at pool build.
        // Fail-closed: a configured-but-unresolved trust must NOT fall
        // back to default anchors (that would silently broaden trust).
        // It builds a TlsFailClosed pool rather than erroring here, so a
        // builder chain like `tcp_tls().with_tls_roots_exclusive()
        // .try_with_tls_root_ca(..)` does not panic on the transient
        // unresolved state -- the refusal surfaces at connect time, never
        // as a silent downgrade to plain/HTTP or to default trust.
        #[cfg(feature = "native-tls-rustls")]
        ConnectKindConfig::Tls { server_name } => match tls {
            TcpTls::Resolved(config) => ConnectKind::Tls {
                server_name,
                config,
            },
            TcpTls::NotConfigured => {
                let config = crate::tls::build_client_config(
                    &crate::tls::TlsConfigSource::Trust(crate::tls::TlsTrust::default()),
                )?;
                ConnectKind::Tls {
                    server_name,
                    config,
                }
            }
            TcpTls::ConfiguredButFailed => ConnectKind::TlsFailClosed,
        },
        #[cfg(not(feature = "native-tls-rustls"))]
        ConnectKindConfig::Tls { .. } => {
            return Err(Error::Custom(
                "tcp: TLS requested but the `native-tls-rustls` feature is not enabled"
                    .into(),
            ));
        }
    };
    let manager = TcpConnectionManager {
        endpoints,
        next: Arc::new(AtomicUsize::new(0)),
        kind,
        config,
        max_lifetime: pool_cfg.max_lifetime,
        read_timeout: pool_cfg.read_timeout,
    };
    Pool::builder(manager)
        .max_size(pool_cfg.max_size)
        .wait_timeout(pool_cfg.acquire_timeout)
        .create_timeout(pool_cfg.create_timeout)
        .recycle_timeout(pool_cfg.recycle_timeout)
        .runtime(Runtime::Tokio1)
        .build()
        .map_err(|e| Error::Custom(format!("tcp: failed to build pool: {e}")))
}

#[cfg(test)]
mod tests {
    //! Pool unit tests use a mock `Manager` that hands out
    //! `ConnectionHandle`s built over loopback `TcpStream`s. This
    //! exercises deadpool's recycle protocol against the real
    //! `is_alive` / `poison` contract on `ConnectionHandle` without
    //! requiring a live ClickHouse server. The real
    //! `TcpConnectionManager` is exercised end-to-end via the
    //! `Client::tcp` live tests that land in the next branch.
    use super::*;
    use crate::tcp::connection_actor::ConnectionActor;
    use crate::tcp::protocol::{DBMS_TCP_PROTOCOL_VERSION, ServerHello};
    use crate::tcp::transport::MaybeTlsStream;
    // `Arc`, `AtomicUsize`, `Ordering` are brought in via `super::*`
    // (the manager now uses them for the round-robin cursor).
    use tokio::net::{TcpListener, TcpStream};

    /// Spin up a loopback `TcpStream` and wrap it in a
    /// `ConnectionHandle`. Each call binds an ephemeral port; the
    /// server-side `TcpStream` is dropped immediately so the
    /// connection is half-closed -- the handle still works for
    /// `is_alive` / `poison` semantics, which is all the pool tests
    /// need.
    async fn fabricate_handle() -> ConnectionHandle {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connect_fut = TcpStream::connect(addr);
        let accept_fut = async { listener.accept().await.unwrap().0 };
        let (client, _server) = tokio::join!(connect_fut, accept_fut);
        let client = client.unwrap();
        let _ = client.set_nodelay(true);
        let stream = MaybeTlsStream::Plain(client);
        let hello = ServerHello {
            server_name: "mock".to_string(),
            version: (1, 0, 0),
            revision: DBMS_TCP_PROTOCOL_VERSION,
            timezone: None,
            display_name: None,
        };
        ConnectionActor::spawn(stream, hello)
    }

    /// Mock manager: each `create` returns a freshly fabricated
    /// handle and bumps a counter so the test can assert how many
    /// distinct handles the pool has produced. `recycle` mirrors the
    /// real manager's logic so the pool's drop-poisoned behaviour
    /// is exercised against the real contract.
    struct TestManager {
        creates: Arc<AtomicUsize>,
        max_lifetime: Option<Duration>,
    }

    impl TestManager {
        fn new() -> Self {
            Self {
                creates: Arc::new(AtomicUsize::new(0)),
                max_lifetime: None,
            }
        }

        fn create_count(&self) -> usize {
            self.creates.load(Ordering::Acquire)
        }
    }

    impl managed::Manager for TestManager {
        type Type = ConnectionHandle;
        type Error = Error;

        async fn create(&self) -> Result<ConnectionHandle> {
            self.creates.fetch_add(1, Ordering::AcqRel);
            Ok(fabricate_handle().await)
        }

        async fn recycle(
            &self,
            obj: &mut ConnectionHandle,
            metrics: &Metrics,
        ) -> RecycleResult<Error> {
            // Mirror the real manager: age check, then liveness.
            if let Some(max) = self.max_lifetime
                && metrics.created.elapsed() >= max
            {
                return Err(RecycleError::message("connection exceeded max_lifetime"));
            }
            if obj.is_alive() {
                Ok(())
            } else {
                Err(RecycleError::message("connection poisoned"))
            }
        }
    }

    fn build_test_pool(mgr: TestManager, cfg: PoolConfig) -> Pool<TestManager> {
        Pool::<TestManager>::builder(mgr)
            .max_size(cfg.max_size)
            .wait_timeout(cfg.acquire_timeout)
            .create_timeout(cfg.create_timeout)
            .recycle_timeout(cfg.recycle_timeout)
            .runtime(Runtime::Tokio1)
            .build()
            .expect("test pool builds")
    }

    /// Poisoning a handle before returning it to the pool must cause
    /// the next acquire to hit `create` again -- the poisoned
    /// connection is dropped, not reused.
    #[tokio::test]
    async fn pool_recycle_drops_poisoned_handle() {
        let mgr = TestManager::new();
        let creates = Arc::clone(&mgr.creates);
        let pool = build_test_pool(mgr, PoolConfig::default());

        let first = pool.get().await.expect("acquire");
        assert_eq!(creates.load(Ordering::Acquire), 1);
        // Poison and return to the pool. recycle should refuse.
        first.poison();
        assert!(!first.is_alive());
        drop(first);

        let _second = pool.get().await.expect("acquire again");
        assert_eq!(
            creates.load(Ordering::Acquire),
            2,
            "poisoned handle should have been dropped and a fresh one created"
        );
    }

    /// A healthy handle returned to the pool must be re-used on the
    /// next acquire -- `create` should NOT be called a second time.
    #[tokio::test]
    async fn pool_recycle_keeps_alive_handle() {
        let mgr = TestManager::new();
        let creates = Arc::clone(&mgr.creates);
        let pool = build_test_pool(mgr, PoolConfig::default());

        let first = pool.get().await.expect("acquire");
        assert_eq!(creates.load(Ordering::Acquire), 1);
        assert!(first.is_alive());
        drop(first);

        let _second = pool.get().await.expect("acquire again");
        assert_eq!(
            creates.load(Ordering::Acquire),
            1,
            "alive handle should have been reused, not re-created"
        );
    }

    /// A connection older than `max_lifetime` must be dropped on the
    /// next acquire rather than reused, even though it is still alive.
    /// Mirrors go's `ConnMaxLifetime` / `database/sql` lifetime cap and
    /// guards against long-lived TCP sessions accumulating server-side
    /// state. The age check reads deadpool's `Metrics.created`, so no
    /// per-handle timestamp is needed.
    #[tokio::test]
    async fn pool_recycle_drops_aged_connection() {
        let creates = Arc::new(AtomicUsize::new(0));
        let mgr = TestManager {
            creates: Arc::clone(&creates),
            max_lifetime: Some(Duration::from_millis(10)),
        };
        let pool = build_test_pool(mgr, PoolConfig::default());

        let first = pool.get().await.expect("acquire");
        assert_eq!(creates.load(Ordering::Acquire), 1);
        assert!(first.is_alive(), "fresh handle is alive");
        drop(first);

        // Let the connection age past max_lifetime before re-acquiring.
        tokio::time::sleep(Duration::from_millis(30)).await;

        let _second = pool.get().await.expect("acquire again");
        assert_eq!(
            creates.load(Ordering::Acquire),
            2,
            "connection older than max_lifetime should be dropped and re-created"
        );
    }

    /// `max_size` must cap concurrent acquires: once `max_size`
    /// handles are checked out, a further acquire blocks until one
    /// is returned. Verify by issuing N+1 acquires under a tight
    /// max_size and checking the last one only completes after a
    /// drop.
    #[tokio::test]
    async fn pool_max_size_blocks_excess_acquires() {
        let cfg = PoolConfig {
            max_size: 2,
            // Disable wait_timeout for this test -- we want the third
            // acquire to block, not error.
            acquire_timeout: None,
            ..PoolConfig::default()
        };
        let mgr = TestManager::new();
        let pool = build_test_pool(mgr, cfg);

        let a = pool.get().await.expect("a");
        let b = pool.get().await.expect("b");

        // Third acquire must not complete while a + b are held.
        let pool_clone = pool.clone();
        let third = tokio::spawn(async move { pool_clone.get().await.is_ok() });

        // Give the third acquire a moment to run; it should still be
        // pending because both slots are taken.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !third.is_finished(),
            "third acquire should block while pool is saturated"
        );

        // Releasing one slot unblocks the third acquire.
        drop(a);
        let acquired = tokio::time::timeout(Duration::from_secs(2), third)
            .await
            .expect("third acquire should unblock once a slot is freed")
            .expect("join");
        assert!(acquired, "third acquire should succeed after a drop");

        drop(b);
    }

    /// A bounded `acquire_timeout` must surface as an `Err` when the
    /// pool stays saturated; the caller never hangs indefinitely.
    #[tokio::test]
    async fn pool_acquire_timeout_returns_err() {
        let cfg = PoolConfig {
            max_size: 1,
            acquire_timeout: Some(Duration::from_millis(10)),
            ..PoolConfig::default()
        };
        let mgr = TestManager::new();
        let pool = build_test_pool(mgr, cfg);

        // Hold the only slot.
        let _held = pool.get().await.expect("first acquire");

        // The second acquire must return Err within a generous bound;
        // 200 ms gives plenty of headroom over the 10 ms timeout to
        // avoid flakiness on a loaded CI runner while still proving
        // the call does not hang.
        let start = tokio::time::Instant::now();
        let result = tokio::time::timeout(Duration::from_millis(200), pool.get()).await;
        let elapsed = start.elapsed();
        match result {
            Ok(Err(_)) => {
                assert!(
                    elapsed < Duration::from_millis(200),
                    "acquire should have errored quickly, took {elapsed:?}"
                );
            }
            Ok(Ok(_)) => panic!("acquire should not have succeeded with one held slot"),
            Err(_) => panic!("acquire hung past the wait_timeout bound"),
        }
    }

    // ---- Endpoint rotation + connect-failover -------------------------
    //
    // These exercise the REAL `TcpConnectionManager::create` against
    // loopback listeners running a minimal mock ClickHouse handshake
    // (read client Hello -> write ServerHello -> read addendum). That is
    // the smallest server that lets `open_handshaken` succeed, so we can
    // observe WHICH endpoint a given `create` landed on (round-robin)
    // and that a refused endpoint is skipped within one pass (failover).

    // `Manager` provides `create`, which these tests call directly on
    // the real manager (not via the pool); `DBMS_TCP_PROTOCOL_VERSION`
    // + `ServerHello` are already imported at the module-test top.
    use crate::native::io::{ClickHouseRead, ClickHouseWrite};
    use crate::tcp::connect::ConnectKind;
    use crate::tcp::protocol::ServerPacketId;
    use deadpool::managed::Manager as _;
    use tokio::io::AsyncWriteExt as _;

    /// Drive the server side of one handshake on an accepted socket:
    /// consume the client Hello (8 fields), emit a valid ServerHello,
    /// then consume the addendum quota-key string. Mirrors the
    /// hand-rolled server in `handshake.rs`'s roundtrip test.
    async fn serve_one_handshake(mut sock: TcpStream) {
        // Client Hello: packet id, name, major, minor, revision,
        // database, user, password.
        let _ = sock.read_var_uint().await;
        let _ = sock.read_utf8_string().await;
        let _ = sock.read_var_uint().await;
        let _ = sock.read_var_uint().await;
        let _ = sock.read_var_uint().await;
        let _ = sock.read_utf8_string().await;
        let _ = sock.read_utf8_string().await;
        let _ = sock.read_utf8_string().await;
        // ServerHello reply (revision well above the timezone /
        // display_name / version_patch gates, so write all three).
        let _ = sock.write_var_uint(ServerPacketId::Hello as u64).await;
        let _ = sock.write_string("mock-ch".as_bytes()).await;
        let _ = sock.write_var_uint(25).await;
        let _ = sock.write_var_uint(4).await;
        let _ = sock.write_var_uint(DBMS_TCP_PROTOCOL_VERSION).await;
        let _ = sock.write_string("Etc/UTC".as_bytes()).await;
        let _ = sock.write_string("mock".as_bytes()).await;
        let _ = sock.write_var_uint(7).await;
        let _ = sock.flush().await;
        // Addendum quota-key (revision is above the addendum gate).
        let _ = sock.read_utf8_string().await;
    }

    /// Bind a loopback listener and spawn a task that accepts up to
    /// `accepts` connections, each tagged with its 0-based endpoint
    /// index pushed onto the shared `order` log so a test can read the
    /// acceptance order. Returns the bound address.
    async fn spawn_mock_endpoint(
        idx: usize,
        accepts: usize,
        order: Arc<std::sync::Mutex<Vec<usize>>>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            for _ in 0..accepts {
                let Ok((sock, _)) = listener.accept().await else {
                    break;
                };
                order.lock().unwrap().push(idx);
                serve_one_handshake(sock).await;
            }
        });
        addr
    }

    /// Build a real `TcpConnectionManager` over the supplied endpoints
    /// (plain, default handshake, no lifetime / read timeout). Shares
    /// the round-robin cursor seeded at 0 so the first `create` starts
    /// on endpoint 0.
    fn manager_over(endpoints: Vec<String>) -> TcpConnectionManager {
        TcpConnectionManager {
            endpoints,
            next: Arc::new(AtomicUsize::new(0)),
            kind: ConnectKind::Plain,
            config: HandshakeConfig::default(),
            max_lifetime: None,
            read_timeout: None,
        }
    }

    /// Successive `create` calls must land on endpoints in rotation:
    /// with the cursor seeded at 0, three creates over three endpoints
    /// hit 0, 1, 2 in order.
    #[tokio::test]
    async fn create_round_robins_across_endpoints() {
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut endpoints = Vec::new();
        for idx in 0..3 {
            endpoints.push(spawn_mock_endpoint(idx, 1, Arc::clone(&order)).await);
        }
        let mgr = manager_over(endpoints);

        for _ in 0..3 {
            mgr.create().await.expect("create connects to the next endpoint");
        }

        let observed = order.lock().unwrap().clone();
        assert_eq!(
            observed,
            vec![0, 1, 2],
            "three creates should round-robin across the three endpoints in order"
        );
    }

    /// A refusing endpoint (a bound-then-immediately-dropped listener,
    /// so connects are refused) must be skipped within a single
    /// `create` pass, connecting to the next accepting endpoint.
    #[tokio::test]
    async fn create_fails_over_to_next_endpoint() {
        // A listener bound then dropped frees its port; connects to it
        // are refused (ECONNREFUSED) rather than hanging.
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let refusing = dead.local_addr().unwrap().to_string();
        drop(dead);

        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let accepting = spawn_mock_endpoint(1, 1, Arc::clone(&order)).await;

        // Cursor seeded at 0 => the first create starts on the refusing
        // endpoint, must skip it, and connect to the accepting one in
        // the SAME pass.
        let mgr = manager_over(vec![refusing, accepting]);
        mgr.create()
            .await
            .expect("create should fail over to the accepting endpoint");

        let observed = order.lock().unwrap().clone();
        assert_eq!(
            observed,
            vec![1],
            "create should have connected only to the accepting endpoint after skipping the refusing one"
        );
    }

    /// When every endpoint refuses, a single `create` pass tries them
    /// all and surfaces the last connect error (typed) -- it does not
    /// hang or panic.
    #[tokio::test]
    async fn create_surfaces_error_when_all_endpoints_refuse() {
        let mut refusing = Vec::new();
        for _ in 0..2 {
            let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
            refusing.push(dead.local_addr().unwrap().to_string());
            drop(dead);
        }
        let mgr = manager_over(refusing);
        // `ConnectionHandle` is not `Debug`, so avoid `expect_err`;
        // match the Result directly.
        let err = match mgr.create().await {
            Ok(_) => panic!("all endpoints refuse; create must not connect"),
            Err(e) => e,
        };
        // A refused connect round-trips io::Error -> Error::Other; the
        // classifier must treat it as a retriable transport failure.
        assert!(
            crate::tcp::retry::is_retriable_transport(&err),
            "an all-endpoints-refused connect error should be retriable, got {err:?}"
        );
    }

    /// Fail-closed: a TLS pool whose trust was configured but failed to
    /// resolve (`TcpTls::ConfiguredButFailed`) BUILDS (so a transient
    /// unresolved state mid-builder-chain does not panic, and we never
    /// silently downgrade the transport), but its connect kind is
    /// `TlsFailClosed` so every connection attempt refuses rather than
    /// falling back to default native+webpki anchors.
    #[cfg(feature = "native-tls-rustls")]
    #[tokio::test]
    async fn tls_pool_fails_closed_when_configured_trust_unresolved() {
        let pool = build_pool(
            vec!["127.0.0.1:9440".to_string()],
            ConnectKindConfig::Tls {
                server_name: "example.invalid".to_string(),
            },
            HandshakeConfig::default(),
            PoolConfig::default(),
            TcpTls::ConfiguredButFailed,
        )
        .expect("configured-but-failed trust still BUILDS a (fail-closed) pool");
        // Connecting must refuse -- fail closed, no default-trust fallback.
        let err = match pool.get().await {
            Ok(_) => panic!("a fail-closed TLS pool must refuse to connect"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("refusing to connect with default trust"),
            "expected fail-closed connect error, got: {msg}"
        );
    }
}
