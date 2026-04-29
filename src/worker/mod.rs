//! Generic command-driven background worker.
//!
//! Originally authored at HyperI for the DFE Loader project's per-table
//! buffer + orchestrator pattern; ported into clickhouse-rs to consolidate
//! the existing in-tree actors (HTTP `AsyncInserter`, native
//! `AsyncNativeInserter`, `DynamicBatcher`) and unblock the socket-state
//! work in [`crate::native::connection_actor`].
//!
//! Two improvements suggested during this port — per-command
//! [`tracing::Span`] propagation and an explicit `Shutdown { tx }`-style
//! ack — were cribbed from `sqlx-sqlite/src/connection/worker.rs` by
//! Austin Bonander. The improvements will be backported to DFE so the
//! loader benefits from the same upgrades.
//!
//! # Broad applicability
//!
//! The same trait drives every long-lived stateful background task:
//!
//! - `native::connection_actor::ConnectionActor` — socket I/O state
//! - `async_inserter::AsyncInserter<T>` (HTTP) — auto-flushing inserter
//! - `native::async_inserter::AsyncNativeInserter<T>` — native equivalent
//! - `dynamic::batcher::DynamicBatcher` — schema-aware batched insert
//! - Future: schema-cache background refresh, pool health probes,
//!   metrics flushers, observability emitters
//!
//! # Why this shape
//!
//! Modelled on `sqlx-sqlite/src/connection/worker.rs` (per-connection
//! thread + per-command tracing span + explicit shutdown ack) and the
//! [`Connection`](https://docs.rs/tokio-postgres/latest/tokio_postgres/struct.Connection.html)
//! struct in `tokio-postgres` (canonical Rust async actor reference).
//!
//! Note that `sqlx-postgres` chose the *other* valid route for
//! cancellation safety — cancel-safe I/O primitives
//! (`BufferedSocket::try_read`) instead of an actor task. We use actors
//! because ClickHouse needs:
//!
//! - a writer side-channel for the protocol Cancel packet,
//! - full-duplex INSERT exception detection during writes,
//! - idle keepalive,
//!
//! none of which I/O-primitive cancel-safety addresses.

use std::future::Future;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::MissedTickBehavior;

// Tracing instrumentation is conditional on the optional `tracing` feature
// (matches the rest of clickhouse-rs). When the feature is off, `Span` is
// a zero-sized no-op so the trait API and runner are identical with or
// without it.
#[cfg(feature = "tracing")]
use tracing::{Instrument, Span, debug, trace};

#[cfg(not(feature = "tracing"))]
mod tracing_compat {
    /// No-op stand-in for [`tracing::Span`] when the `tracing` feature
    /// is off. Identical API surface, zero size, zero cost.
    #[derive(Clone, Copy, Default)]
    pub struct Span;
    impl Span {
        #[inline]
        pub fn enter(&self) -> SpanGuard {
            SpanGuard
        }
    }
    pub struct SpanGuard;

    pub trait Instrument: Sized {
        #[inline]
        fn instrument(self, _: Span) -> Self {
            self
        }
    }
    impl<F: std::future::Future> Instrument for F {}
}

#[cfg(not(feature = "tracing"))]
use tracing_compat::{Instrument, Span};

#[cfg(not(feature = "tracing"))]
impl Span {
    #[inline]
    pub fn current() -> Self {
        Self
    }
}

// Tracing-feature-gated logging macros. With the feature on, these
// expand to `tracing::{debug,trace}!`. Without it, they swallow all
// tokens (including `kw = expr` syntax) without evaluating anything.
#[cfg(not(feature = "tracing"))]
macro_rules! debug {
    ($($_:tt)*) => {};
}
#[cfg(not(feature = "tracing"))]
macro_rules! trace {
    ($($_:tt)*) => {};
}

/// A long-running command-driven background task.
///
/// Implementors define the per-actor pieces (state + command type +
/// per-command logic + optional periodic tick + optional shutdown
/// cleanup); the runner ([`spawn`]) handles the lifecycle plumbing
/// (channel, `select!` loop, panic safety, tracing).
///
/// All futures returned by trait methods must be `Send` because the
/// runner is spawned on the multi-threaded tokio runtime.
///
/// # Example
///
/// ```no_run
/// use std::time::Duration;
/// use clickhouse::worker::{CommandWorker, spawn};
/// use tokio::sync::oneshot;
///
/// enum Cmd {
///     Increment,
///     Get { reply: oneshot::Sender<u64> },
/// }
///
/// struct Counter { n: u64 }
///
/// impl CommandWorker for Counter {
///     type Command = Cmd;
///     fn name() -> &'static str { "counter" }
///
///     fn handle(&mut self, cmd: Cmd) -> impl std::future::Future<Output = ()> + Send + '_ {
///         async move {
///             match cmd {
///                 Cmd::Increment => self.n += 1,
///                 Cmd::Get { reply } => { let _ = reply.send(self.n); }
///             }
///         }
///     }
/// }
///
/// # async fn run() {
/// let control = spawn(Counter { n: 0 }, 16);
/// control.handle().send(Cmd::Increment).await.unwrap();
/// let (tx, rx) = oneshot::channel();
/// control.handle().send(Cmd::Get { reply: tx }).await.unwrap();
/// assert_eq!(rx.await.unwrap(), 1);
/// control.shutdown().await.unwrap();
/// # }
/// ```
pub trait CommandWorker: Send + 'static {
    /// Typed commands this worker accepts.
    ///
    /// Replies are conventionally embedded in command variants as
    /// [`tokio::sync::oneshot::Sender<R>`] for single-reply commands or
    /// [`tokio::sync::mpsc::Sender<R>`] for streaming-reply commands.
    /// Cancellation-on-drop is the standard signal — when the caller
    /// drops the receiver, the implementor's `handle` should observe
    /// `is_closed()` (or the next `send().await` returning `Err`) and
    /// abort/drain the operation cleanly.
    type Command: Send + 'static;

    /// Process one command. Sequential — only one command in flight at a
    /// time, so `&mut self` is safe.
    ///
    /// Replies are sent through the embedded reply channel. Errors are
    /// surfaced through the reply (not propagated up); per-command
    /// failures should not exit the worker.
    fn handle(&mut self, cmd: Self::Command) -> impl Future<Output = ()> + Send + '_;

    /// Called when no command is pending and [`idle_interval`] has
    /// elapsed since the last command (or since the previous tick).
    ///
    /// Use for periodic work: interval flush, keepalive ping, cache
    /// refresh, scheduled cleanup.
    ///
    /// Default: no-op.
    ///
    /// [`idle_interval`]: Self::idle_interval
    fn on_idle(&mut self) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    /// How often [`on_idle`] fires when no command is pending.
    /// `None` (default) disables periodic ticks.
    ///
    /// [`on_idle`]: Self::on_idle
    fn idle_interval(&self) -> Option<Duration> {
        None
    }

    /// Called once when the command channel closes (last sender dropped)
    /// — the worker's final-cleanup hook before its task exits.
    ///
    /// Use for: final flush, drain of in-flight state, server-side
    /// goodbye, releasing OS resources. Will not be called if `handle`
    /// or `on_idle` panics — wrap risky cleanup in
    /// [`std::panic::catch_unwind`] or similar if needed.
    ///
    /// Default: no-op.
    fn on_shutdown(&mut self) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    /// Label for tracing spans + diagnostics. Default: `"worker"`.
    /// Implementors should override with a more specific name
    /// (e.g. `"clickhouse.connection-actor"`).
    fn name() -> &'static str {
        "worker"
    }
}

/// Owns a spawned worker task.
///
/// Dropping `WorkerControl` drops the inbound command sender, which
/// triggers an implicit graceful shutdown (the worker drains any
/// pending commands, runs [`CommandWorker::on_shutdown`], then exits).
///
/// For *explicit* graceful shutdown with completion ack, call
/// [`WorkerControl::shutdown`] which awaits the underlying
/// [`JoinHandle`].
pub struct WorkerControl<C> {
    tx: mpsc::Sender<(C, Span)>,
    join: JoinHandle<()>,
    name: &'static str,
}

/// Cheap-clone send-side handle.
///
/// Multiple producers can hold one to push commands concurrently. The
/// handle does NOT hold the worker alive on its own — the
/// [`WorkerControl`] returned by [`spawn`] does.
#[derive(Clone)]
pub struct WorkerHandle<C> {
    tx: mpsc::Sender<(C, Span)>,
    name: &'static str,
}

impl<C> std::fmt::Debug for WorkerControl<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerControl")
            .field("name", &self.name)
            .field("alive", &!self.tx.is_closed())
            .finish()
    }
}

impl<C> std::fmt::Debug for WorkerHandle<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerHandle")
            .field("name", &self.name)
            .field("alive", &!self.tx.is_closed())
            .finish()
    }
}

impl<C: Send + 'static> WorkerControl<C> {
    /// Cheap-clone send-side handle. Multiple producers can hold one.
    #[must_use]
    pub fn handle(&self) -> WorkerHandle<C> {
        WorkerHandle {
            tx: self.tx.clone(),
            name: self.name,
        }
    }

    /// True while the worker task is still running and accepting
    /// commands. Becomes false once the worker has exited (server EOF,
    /// explicit shutdown, panic).
    #[must_use]
    pub fn is_alive(&self) -> bool {
        !self.tx.is_closed()
    }

    /// Send one command. Returns `Err` if the worker has exited
    /// (channel closed).
    ///
    /// The current [`tracing::Span`] is captured and propagated to the
    /// worker so cross-task tracing is intact (Austin Bonander pattern).
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] containing the original command if the
    /// worker task is no longer running.
    pub async fn send(&self, cmd: C) -> Result<(), SendError<C>> {
        self.tx
            .send((cmd, Span::current()))
            .await
            .map_err(|e| SendError(e.0.0))
    }

    /// Try to send one command without waiting. Returns `Err` if the
    /// channel is full or closed.
    ///
    /// # Errors
    ///
    /// Returns a [`TrySendError`] indicating whether the channel is full
    /// or closed.
    pub fn try_send(&self, cmd: C) -> Result<(), TrySendError<C>> {
        self.tx
            .try_send((cmd, Span::current()))
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full((c, _)) => TrySendError::Full(c),
                mpsc::error::TrySendError::Closed((c, _)) => TrySendError::Closed(c),
            })
    }

    /// Explicit graceful shutdown. Drops the sender (closing the
    /// command channel), then awaits the worker task's
    /// [`JoinHandle`].
    ///
    /// Equivalent to letting `WorkerControl` drop, but waits for and
    /// reports the final task status (so the caller knows shutdown
    /// completed and can detect panics).
    ///
    /// # Errors
    ///
    /// Returns [`JoinError`] if the worker task panicked.
    pub async fn shutdown(self) -> Result<(), JoinError> {
        let Self { tx, join, name } = self;
        debug!(target: "clickhouse::worker", worker = name, "explicit shutdown requested");
        drop(tx);
        join.await
    }
}

impl<C: Send + 'static> WorkerHandle<C> {
    /// Send one command. Returns `Err` if the worker has exited.
    ///
    /// The current [`tracing::Span`] is captured and propagated.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] if the worker is no longer running.
    pub async fn send(&self, cmd: C) -> Result<(), SendError<C>> {
        self.tx
            .send((cmd, Span::current()))
            .await
            .map_err(|e| SendError(e.0.0))
    }

    /// Try to send one command without waiting.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] if the channel is full,
    /// [`TrySendError::Closed`] if the worker has exited.
    pub fn try_send(&self, cmd: C) -> Result<(), TrySendError<C>> {
        self.tx
            .try_send((cmd, Span::current()))
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full((c, _)) => TrySendError::Full(c),
                mpsc::error::TrySendError::Closed((c, _)) => TrySendError::Closed(c),
            })
    }

    /// True while the worker task is still running.
    #[must_use]
    pub fn is_alive(&self) -> bool {
        !self.tx.is_closed()
    }
}

/// Returned when the worker has exited and a command can no longer be sent.
///
/// Holds the command back so the caller can recover or DLQ it.
#[derive(Debug)]
pub struct SendError<C>(pub C);

impl<C> std::fmt::Display for SendError<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("worker has exited; cannot send command")
    }
}

impl<C: std::fmt::Debug> std::error::Error for SendError<C> {}

/// Returned by [`WorkerControl::try_send`] / [`WorkerHandle::try_send`].
#[derive(Debug)]
pub enum TrySendError<C> {
    /// The channel is at capacity. Caller can retry later.
    Full(C),
    /// The worker has exited. Caller should not retry.
    Closed(C),
}

impl<C> std::fmt::Display for TrySendError<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full(_) => f.write_str("worker channel full"),
            Self::Closed(_) => f.write_str("worker has exited"),
        }
    }
}

impl<C: std::fmt::Debug> std::error::Error for TrySendError<C> {}

/// Spawn a [`CommandWorker`].
///
/// `capacity` is the bounded mpsc channel size — small enough to
/// surface backpressure to producers, large enough to absorb burstiness.
/// Production callers typically use 16–64; high-fan-in inserters use up
/// to ~8 K (see [`crate::async_inserter`]).
///
/// Returns a [`WorkerControl`] that owns the spawned task. Drop or
/// [`shutdown`](WorkerControl::shutdown) to terminate.
///
/// # Panics
///
/// Panics in `handle` / `on_idle` / `on_shutdown` propagate as a
/// [`JoinError`] surfaced by [`WorkerControl::shutdown`]. The runner
/// itself does not catch panics — match `sqlx-sqlite`'s policy where
/// the worker is considered broken and the connection is discarded.
pub fn spawn<W: CommandWorker>(worker: W, capacity: usize) -> WorkerControl<W::Command> {
    let name = W::name();
    let (tx, rx) = mpsc::channel(capacity);
    #[cfg(feature = "tracing")]
    let join = {
        let span = tracing::info_span!(target: "clickhouse::worker", "worker", worker = name);
        tokio::spawn(run::<W>(worker, rx).instrument(span))
    };
    #[cfg(not(feature = "tracing"))]
    let join = tokio::spawn(run::<W>(worker, rx));
    WorkerControl { tx, join, name }
}

async fn run<W: CommandWorker>(mut worker: W, mut rx: mpsc::Receiver<(W::Command, Span)>) {
    let name = W::name();
    debug!(target: "clickhouse::worker", worker = name, "started");

    let interval = worker.idle_interval();
    let mut idle = interval.map(|period| {
        let mut iv = tokio::time::interval(period);
        iv.set_missed_tick_behavior(MissedTickBehavior::Delay);
        iv
    });

    loop {
        let next = match idle.as_mut() {
            Some(iv) => tokio::select! {
                biased;
                cmd = rx.recv() => cmd.map(NextEvent::Command),
                _ = iv.tick() => Some(NextEvent::Idle),
            },
            None => rx.recv().await.map(NextEvent::Command),
        };

        match next {
            Some(NextEvent::Command((cmd, caller_span))) => {
                let _enter = caller_span.enter();
                trace!(target: "clickhouse::worker", worker = name, "handling command");
                worker.handle(cmd).await;
            }
            Some(NextEvent::Idle) => {
                trace!(target: "clickhouse::worker", worker = name, "idle tick");
                worker.on_idle().await;
            }
            None => break, // all senders dropped — graceful shutdown
        }
    }

    debug!(target: "clickhouse::worker", worker = name, "draining; running on_shutdown");
    worker.on_shutdown().await;
    debug!(target: "clickhouse::worker", worker = name, "exited");
}

enum NextEvent<C> {
    Command((C, Span)),
    Idle,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::{Notify, oneshot};

    /// Toy worker: a counter with increment/get/wait-for-shutdown.
    struct Counter {
        n: u64,
        idle_ticks: Arc<AtomicU64>,
        on_shutdown_fired: Arc<Notify>,
    }

    #[derive(Debug)]
    enum CounterCmd {
        Inc,
        Get(oneshot::Sender<u64>),
    }

    impl CommandWorker for Counter {
        type Command = CounterCmd;
        fn name() -> &'static str {
            "counter-test"
        }

        fn handle(&mut self, cmd: CounterCmd) -> impl Future<Output = ()> + Send + '_ {
            async move {
                match cmd {
                    CounterCmd::Inc => self.n += 1,
                    CounterCmd::Get(reply) => {
                        let _ = reply.send(self.n);
                    }
                }
            }
        }

        fn idle_interval(&self) -> Option<Duration> {
            // Fast tick for tests; real workers use seconds.
            Some(Duration::from_millis(20))
        }

        fn on_idle(&mut self) -> impl Future<Output = ()> + Send + '_ {
            let counter = self.idle_ticks.clone();
            async move {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }

        fn on_shutdown(&mut self) -> impl Future<Output = ()> + Send + '_ {
            let notify = self.on_shutdown_fired.clone();
            async move {
                notify.notify_one();
            }
        }
    }

    #[tokio::test]
    async fn counter_handles_command_and_replies() {
        let control = spawn(
            Counter {
                n: 0,
                idle_ticks: Arc::new(AtomicU64::new(0)),
                on_shutdown_fired: Arc::new(Notify::new()),
            },
            8,
        );

        control.send(CounterCmd::Inc).await.unwrap();
        control.send(CounterCmd::Inc).await.unwrap();

        let (tx, rx) = oneshot::channel();
        control.send(CounterCmd::Get(tx)).await.unwrap();
        assert_eq!(rx.await.unwrap(), 2);

        control.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn handle_clones_send_independently() {
        let control = spawn(
            Counter {
                n: 0,
                idle_ticks: Arc::new(AtomicU64::new(0)),
                on_shutdown_fired: Arc::new(Notify::new()),
            },
            8,
        );

        let h1 = control.handle();
        let h2 = control.handle();
        h1.send(CounterCmd::Inc).await.unwrap();
        h2.send(CounterCmd::Inc).await.unwrap();

        let (tx, rx) = oneshot::channel();
        control.send(CounterCmd::Get(tx)).await.unwrap();
        assert_eq!(rx.await.unwrap(), 2);

        control.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn idle_ticks_fire_when_quiet() {
        let idle_ticks = Arc::new(AtomicU64::new(0));
        let control = spawn(
            Counter {
                n: 0,
                idle_ticks: idle_ticks.clone(),
                on_shutdown_fired: Arc::new(Notify::new()),
            },
            8,
        );

        // Wait long enough for several ticks.
        tokio::time::sleep(Duration::from_millis(120)).await;
        let ticks = idle_ticks.load(Ordering::Relaxed);
        assert!(
            ticks >= 3,
            "expected several idle ticks within 120 ms, got {ticks}"
        );

        control.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn on_shutdown_fires_when_handles_dropped() {
        let notify = Arc::new(Notify::new());
        let control = spawn(
            Counter {
                n: 0,
                idle_ticks: Arc::new(AtomicU64::new(0)),
                on_shutdown_fired: notify.clone(),
            },
            8,
        );

        // Drop the control — the worker should run on_shutdown then exit.
        drop(control);

        // The notify should fire shortly.
        tokio::time::timeout(Duration::from_millis(200), notify.notified())
            .await
            .expect("on_shutdown did not fire within 200 ms");
    }

    #[tokio::test]
    async fn explicit_shutdown_returns_after_drain() {
        let notify = Arc::new(Notify::new());
        let control = spawn(
            Counter {
                n: 0,
                idle_ticks: Arc::new(AtomicU64::new(0)),
                on_shutdown_fired: notify.clone(),
            },
            8,
        );

        control.send(CounterCmd::Inc).await.unwrap();
        control.shutdown().await.unwrap();

        // Already-fired before shutdown returned.
        let already_fired =
            tokio::time::timeout(Duration::from_millis(10), notify.notified()).await;
        assert!(
            already_fired.is_ok(),
            "on_shutdown should fire by the time shutdown returns"
        );
    }

    #[tokio::test]
    async fn send_after_shutdown_returns_send_error_with_command() {
        let control = spawn(
            Counter {
                n: 0,
                idle_ticks: Arc::new(AtomicU64::new(0)),
                on_shutdown_fired: Arc::new(Notify::new()),
            },
            8,
        );
        let handle = control.handle();
        control.shutdown().await.unwrap();

        let result = handle.send(CounterCmd::Inc).await;
        assert!(result.is_err(), "send after shutdown should fail");
        // The original command is preserved for caller-side recovery.
        let SendError(_cmd) = result.unwrap_err();
    }

    #[tokio::test]
    async fn try_send_full_returns_command_back() {
        // Capacity 1 — we'll fill it before the worker drains.
        struct Slow;
        impl CommandWorker for Slow {
            type Command = ();
            fn name() -> &'static str {
                "slow"
            }
            fn handle(&mut self, _: ()) -> impl Future<Output = ()> + Send + '_ {
                async move {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }

        let control = spawn(Slow, 1);
        // First send fills the slot and the worker starts handling it.
        control.send(()).await.unwrap();
        // Give the worker a moment to take the first command off the queue
        // and start sleeping.
        tokio::time::sleep(Duration::from_millis(10)).await;
        // Push a second command — should land in the queue (cap=1).
        control.try_send(()).unwrap();
        // Third should hit Full (queue full + worker still busy).
        match control.try_send(()) {
            Err(TrySendError::Full(())) => {}
            other => panic!("expected TrySendError::Full, got {other:?}"),
        }

        control.shutdown().await.unwrap();
    }
}
