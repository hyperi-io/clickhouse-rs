//! Background-task socket-state owner for native ClickHouse connections.
//!
//! Built on the generic [`crate::worker::CommandWorker`] trait. Owns the
//! writer half of the TCP/TLS stream and a packet-receiver fed by an
//! internal reader sub-task; together they give the actor full-duplex
//! visibility (read loop is always running, even while a write command
//! is in flight) without the caller having to manage cancel-safety at
//! the protocol layer.
//!
//! # Why the actor instead of direct ownership
//!
//! The previous design held `&mut NativeConnection` (reader+writer
//! halves of the TCP stream) in the caller's future for the duration of
//! a query/insert. That model was structurally cancellation-unsafe — any
//! `tokio::select!`/`tokio::time::timeout`/HTTP-disconnect that dropped
//! the future mid-`read_packet()` left the socket in an unknown state,
//! and the only recovery was `discard()` -> TCP teardown. See
//! [`crate::worker`] module docs for the SQLx pool-poisoning case study
//! that motivates this pattern.
//!
//! Three things become possible only with actor-owned I/O state:
//!
//! 1. **Protocol-level Cancel packet** — abort an in-flight server-side
//!    query without tearing down the socket. (Today's design literally
//!    cannot send Cancel because the writer is borrowed by the future
//!    that is reading.)
//! 2. **Full-duplex INSERT exception detection** — server can send
//!    `Exception` between client data blocks; with the always-running
//!    reader sub-task the exception is surfaced before the next wasted
//!    block goes on the wire.
//! 3. **Idle keepalive** — actor can send Ping between commands without
//!    racing the caller for the writer.
//!
//! # Internal layout
//!
//! ```text
//!     callers                  ConnectionActor
//!   (ConnectionHandle           (CommandWorker)            reader_task
//!    .ping().await)                                       (independent)
//!         |                          |                          |
//!         v                          |                          |
//!   WorkerHandle<Cmd>                |                          |
//!         |                          |                          |
//!         v                          |                          |
//!     mpsc::channel ----------> handle(Cmd) ----------> writer half ----> socket
//!                                    ^                                    |
//!                                    |                                    |
//!                                pkt_rx <---------- pkt_tx <-- read_packet
//! ```
//!
//! Tracer-bullet phase: only `Ping` is implemented. ExecuteQuery, INSERT,
//! and streaming cursor follow in subsequent commits on the same branch.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::io::{BufReader, BufWriter, ReadHalf, WriteHalf};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::error::{Error, Result};
use crate::native::protocol::{NativeCompressionMethod, ServerHello};
use crate::native::reader::{self, ServerPacket};
use crate::native::tcp::{CONN_READ_BUFFER, CONN_WRITE_BUFFER, MaybeTlsStream};
use crate::native::writer;
use crate::worker::{self, CommandWorker, WorkerControl, WorkerHandle};

/// Bounded internal channel between the reader sub-task and the actor's
/// command loop. 64 packets covers typical Progress/ProfileInfo
/// interleaving for a SELECT without stalling the reader.
const PACKET_CHANNEL_CAPACITY: usize = 64;

/// Default keepalive interval — comfortable margin under common
/// NAT/firewall idle timeouts (typically 5 min).
const DEFAULT_KEEPALIVE: Duration = Duration::from_secs(120);

/// Default capacity of the public command channel. 16 covers the
/// typical "one in-flight + a few queued" pattern; high-fan-in callers
/// should bypass via direct `worker::spawn`.
const DEFAULT_CMD_CHANNEL: usize = 16;

// ---------------------------------------------------------------------------
// Public command enum (extended in subsequent commits)
// ---------------------------------------------------------------------------

/// Commands the [`ConnectionActor`] accepts.
///
/// Replies travel back through embedded reply channels — `oneshot` for
/// single-reply commands, `mpsc` for streaming. Cancellation-on-drop:
/// when the caller drops the receiver, the actor observes `is_closed()`
/// (or the next `send().await` returning `Err`) and aborts/drains.
#[allow(dead_code)] // variants land per-commit on this branch
#[derive(Debug)]
pub(crate) enum ConnectionCmd {
    /// Send a Ping; reply with `()` when Pong arrives.
    Ping { reply: oneshot::Sender<Result<()>> },
    // Subsequent variants:
    //   ExecuteQuery { query_id, query, extra_settings, reply: oneshot<Result<()>> }
    //   BeginInsert { query, reply: oneshot<Result<Vec<(String,String)>>> }
    //   SendInsertBlock { column_bytes, num_columns, num_rows, reply: oneshot<Result<()>> }
    //   FinishInsert { reply: oneshot<Result<()>> }
    //   ExecuteStream { query_id, query, extra_settings, results: mpsc<Result<ServerPacket>> }
}

// ---------------------------------------------------------------------------
// Public handle (cheap clone) and owned connection (RAII)
// ---------------------------------------------------------------------------

/// Cheap-clone send-side handle to a [`ConnectionActor`].
///
/// Also exposes the negotiated [`ServerHello`] (for protocol revision
/// checks) and the poison flag (for pool-recycle decisions). Multiple
/// producers can hold one to send commands concurrently — the actor
/// serialises them.
#[derive(Clone)]
#[allow(dead_code)] // wired in subsequent commits + by pool.rs migration
pub(crate) struct ConnectionHandle {
    inner: WorkerHandle<ConnectionCmd>,
    server_hello: Arc<ServerHello>,
    poisoned: Arc<AtomicBool>,
}

#[allow(dead_code)] // wired in subsequent commits
impl ConnectionHandle {
    /// True while the actor is alive AND the connection is not poisoned.
    /// Replaces the old `NativeConnection::check_alive()` poll-trick.
    #[must_use]
    pub(crate) fn is_alive(&self) -> bool {
        self.inner.is_alive() && !self.poisoned.load(Ordering::Acquire)
    }

    /// Mark the connection as broken so the pool drops it on `recycle()`.
    /// Idempotent; does not close the actor immediately.
    pub(crate) fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    /// Negotiated server hello info (immutable after handshake).
    #[must_use]
    pub(crate) fn server_hello(&self) -> &ServerHello {
        &self.server_hello
    }

    /// Negotiated server revision.
    #[must_use]
    pub(crate) fn server_revision(&self) -> u64 {
        self.server_hello.revision_version
    }

    /// Send a Ping and await Pong.
    ///
    /// # Errors
    ///
    /// - [`Error::Custom`] if the actor has exited (channel closed).
    /// - Whatever the server returns (Exception → [`Error::BadResponse`]).
    pub(crate) async fn ping(&self) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .send(ConnectionCmd::Ping { reply })
            .await
            .map_err(|_| Error::Custom("connection actor closed".into()))?;
        rx.await
            .map_err(|_| Error::Custom("connection actor dropped during ping".into()))?
    }
}

/// RAII owner of a spawned [`ConnectionActor`] + its reader sub-task.
///
/// Dropping triggers graceful shutdown:
/// 1. The worker's command channel closes (actor finishes any pending
///    command, then `on_shutdown` runs).
/// 2. The reader sub-task is aborted (so it doesn't outlive the writer).
///
/// For *explicit* shutdown with completion ack, call
/// [`OwnedConnection::shutdown`].
///
/// Internally uses an `Option<Inner>` so `shutdown(self)` can extract
/// the fields without conflicting with the `Drop` impl (Rust forbids
/// destructuring a type that implements `Drop`).
#[allow(dead_code)] // wired in subsequent commits + by pool.rs migration
pub(crate) struct OwnedConnection {
    inner: Option<OwnedInner>,
    server_hello: Arc<ServerHello>,
    poisoned: Arc<AtomicBool>,
}

struct OwnedInner {
    control: WorkerControl<ConnectionCmd>,
    reader_handle: JoinHandle<()>,
}

#[allow(dead_code)] // wired in subsequent commits
impl OwnedConnection {
    /// Cheap-clone send-side handle. Multiple producers can hold one.
    ///
    /// # Panics
    ///
    /// Panics if called after [`shutdown`](Self::shutdown). Don't.
    #[must_use]
    pub(crate) fn handle(&self) -> ConnectionHandle {
        let inner = self
            .inner
            .as_ref()
            .expect("OwnedConnection used after shutdown");
        ConnectionHandle {
            inner: inner.control.handle(),
            server_hello: Arc::clone(&self.server_hello),
            poisoned: Arc::clone(&self.poisoned),
        }
    }

    /// True while the actor is alive AND the connection is not poisoned.
    #[must_use]
    pub(crate) fn is_alive(&self) -> bool {
        self.inner.as_ref().is_some_and(|i| i.control.is_alive())
            && !self.poisoned.load(Ordering::Acquire)
    }

    /// Negotiated server hello info.
    #[must_use]
    pub(crate) fn server_hello(&self) -> &ServerHello {
        &self.server_hello
    }

    /// Mark the connection as broken so the pool drops it on `recycle()`.
    pub(crate) fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    /// Explicit graceful shutdown. Closes the command channel, awaits
    /// the actor's drain (running `on_shutdown`), then aborts the
    /// reader sub-task.
    ///
    /// Equivalent to letting `OwnedConnection` drop, but reports
    /// completion / panics through the [`tokio::task::JoinError`].
    pub(crate) async fn shutdown(mut self) -> std::result::Result<(), tokio::task::JoinError> {
        let Some(OwnedInner {
            control,
            reader_handle,
        }) = self.inner.take()
        else {
            return Ok(());
        };
        let result = control.shutdown().await;
        // Reader task is independent — abort it so it doesn't outlive
        // the writer's socket close.
        reader_handle.abort();
        result
    }
}

impl Drop for OwnedConnection {
    fn drop(&mut self) {
        // Implicit shutdown path: just abort the reader. The actor
        // itself exits when its WorkerControl drops (cmd channel
        // closes); on_shutdown then runs one last time.
        if let Some(inner) = self.inner.take() {
            inner.reader_handle.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// The actor
// ---------------------------------------------------------------------------

/// Background task that owns the writer half + the receive end of the
/// internal packet channel. Implements [`CommandWorker`] so the
/// generic runner does the lifecycle plumbing.
pub(crate) struct ConnectionActor {
    writer: BufWriter<WriteHalf<MaybeTlsStream>>,
    pkt_rx: mpsc::Receiver<Result<ServerPacket>>,
    #[allow(dead_code)] // used by ExecuteQuery / BeginInsert in next commits
    server_hello: Arc<ServerHello>,
    #[allow(dead_code)]
    compression: NativeCompressionMethod,
    #[allow(dead_code)]
    settings: Vec<(String, String)>,
    poisoned: Arc<AtomicBool>,
    keepalive: Duration,
    last_activity: Instant,
}

impl CommandWorker for ConnectionActor {
    type Command = ConnectionCmd;

    fn name() -> &'static str {
        "clickhouse.native-connection"
    }

    fn idle_interval(&self) -> Option<Duration> {
        Some(self.keepalive)
    }

    fn handle(&mut self, cmd: ConnectionCmd) -> impl std::future::Future<Output = ()> + Send + '_ {
        async move {
            self.last_activity = Instant::now();
            match cmd {
                ConnectionCmd::Ping { reply } => {
                    let result = self.do_ping().await;
                    if result.is_err() {
                        self.poisoned.store(true, Ordering::Release);
                    }
                    let _ = reply.send(result);
                }
            }
        }
    }

    fn on_idle(&mut self) -> impl std::future::Future<Output = ()> + Send + '_ {
        async move {
            // Only send keepalive if we've actually been idle for the
            // full interval — bursty traffic shouldn't trigger probes.
            if self.last_activity.elapsed() < self.keepalive {
                return;
            }
            if let Err(_e) = self.do_ping().await {
                // Keepalive failure → poison so the pool discards on
                // next recycle. The actor itself stays alive to surface
                // the error to any pending caller.
                self.poisoned.store(true, Ordering::Release);
            }
            self.last_activity = Instant::now();
        }
    }

    // on_shutdown: default (no-op). ClickHouse native protocol has no
    // explicit "client goodbye" packet — closing the writer is the
    // shutdown signal.
}

impl ConnectionActor {
    /// Create the actor, spawn the reader sub-task, and hand the actor
    /// to [`worker::spawn`].
    ///
    /// Caller passes the already-handshaken halves of the TCP/TLS
    /// stream plus the negotiated handshake state. Connection setup
    /// (TLS upgrade, hello exchange) lives in
    /// [`crate::native::connection::NativeConnection::open`] for now;
    /// this constructor takes the post-handshake state.
    #[allow(dead_code)] // wired by NativeConnection migration in next commit
    pub(crate) fn spawn(
        reader_half: ReadHalf<MaybeTlsStream>,
        writer_half: WriteHalf<MaybeTlsStream>,
        server_hello: ServerHello,
        compression: NativeCompressionMethod,
        settings: Vec<(String, String)>,
    ) -> OwnedConnection {
        Self::spawn_with_keepalive(
            reader_half,
            writer_half,
            server_hello,
            compression,
            settings,
            DEFAULT_KEEPALIVE,
        )
    }

    /// Same as [`spawn`](Self::spawn) but with a custom keepalive
    /// interval. Tests use very short intervals.
    #[allow(dead_code)] // used by tests + future config plumbing
    pub(crate) fn spawn_with_keepalive(
        reader_half: ReadHalf<MaybeTlsStream>,
        writer_half: WriteHalf<MaybeTlsStream>,
        server_hello: ServerHello,
        compression: NativeCompressionMethod,
        settings: Vec<(String, String)>,
        keepalive: Duration,
    ) -> OwnedConnection {
        let writer = BufWriter::with_capacity(CONN_WRITE_BUFFER, writer_half);
        let server_hello = Arc::new(server_hello);
        let poisoned = Arc::new(AtomicBool::new(false));

        // Spawn the reader sub-task — owns the reader half, pushes every
        // packet (or error) to pkt_tx until the socket closes.
        let (pkt_tx, pkt_rx) = mpsc::channel(PACKET_CHANNEL_CAPACITY);
        let revision = server_hello.revision_version;
        let reader_handle = tokio::spawn(reader_task(
            BufReader::with_capacity(CONN_READ_BUFFER, reader_half),
            pkt_tx,
            revision,
            compression,
        ));

        let actor = ConnectionActor {
            writer,
            pkt_rx,
            server_hello: Arc::clone(&server_hello),
            compression,
            settings,
            poisoned: Arc::clone(&poisoned),
            keepalive,
            last_activity: Instant::now(),
        };

        let control = worker::spawn(actor, DEFAULT_CMD_CHANNEL);

        OwnedConnection {
            inner: Some(OwnedInner {
                control,
                reader_handle,
            }),
            server_hello,
            poisoned,
        }
    }

    /// Send Ping, drain packets until Pong (skipping any stray
    /// Progress/ProfileInfo). Internal helper for both [`handle`]
    /// (caller-issued Ping) and [`on_idle`] (keepalive Ping).
    async fn do_ping(&mut self) -> Result<()> {
        writer::send_ping(&mut self.writer).await?;
        loop {
            let pkt = self.recv_packet().await?;
            match pkt {
                ServerPacket::Pong => return Ok(()),
                ServerPacket::Exception(err) => {
                    return Err(Error::BadResponse(err.to_string()));
                }
                _ => continue,
            }
        }
    }

    /// Receive the next packet from the reader sub-task. Returns
    /// `Err` if the reader has exited (server EOF / network error).
    async fn recv_packet(&mut self) -> Result<ServerPacket> {
        match self.pkt_rx.recv().await {
            Some(Ok(p)) => Ok(p),
            Some(Err(e)) => Err(e),
            None => Err(Error::Custom(
                "reader task exited (server closed connection)".into(),
            )),
        }
    }
}

/// Background reader sub-task. Loops on `read_packet` until any error
/// or socket EOF, pushing every packet to the actor's internal channel.
async fn reader_task(
    mut reader: BufReader<ReadHalf<MaybeTlsStream>>,
    pkt_tx: mpsc::Sender<Result<ServerPacket>>,
    revision: u64,
    compression: NativeCompressionMethod,
) {
    loop {
        match reader::read_packet(&mut reader, revision, compression).await {
            Ok(pkt) => {
                if pkt_tx.send(Ok(pkt)).await.is_err() {
                    return; // actor exited
                }
            }
            Err(e) => {
                let _ = pkt_tx.send(Err(e)).await;
                return; // socket dead; actor will see pkt_rx close
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-time sanity: ConnectionActor satisfies CommandWorker.
    /// (The `worker::spawn` call above already enforces this; this test
    /// is here so any future signature drift is caught loud.)
    #[test]
    fn connection_actor_implements_command_worker() {
        fn assert_command_worker<W: CommandWorker>() {}
        assert_command_worker::<ConnectionActor>();
    }

    #[test]
    fn handle_is_clone() {
        // Compile-time check.
        fn assert_clone<T: Clone>() {}
        assert_clone::<ConnectionHandle>();
    }
}
