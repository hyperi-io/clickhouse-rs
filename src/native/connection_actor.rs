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

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::io::{BufReader, BufWriter, ReadHalf, WriteHalf};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::error::{Error, Result};
use crate::native::connection::TlsConfig;
use crate::native::protocol::{
    ChunkedProtocolMode, DBMS_TCP_PROTOCOL_VERSION, NativeCompressionMethod, ServerHello,
};
use crate::native::reader::{self, ServerPacket};
use crate::native::tcp::{self, CONN_READ_BUFFER, CONN_WRITE_BUFFER, MaybeTlsStream};
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

    /// Execute a non-streaming query (DDL, SET, INSERT-without-data,
    /// or a SELECT whose result rows the caller doesn't want).
    ///
    /// Sends Query + empty data block, drains response packets until
    /// `EndOfStream` or `Exception`.
    ///
    /// **Cancellation:** if the caller drops `reply` before the
    /// response arrives, the actor sends the protocol-level
    /// [`writer::send_cancel`] packet so the server stops computing,
    /// then drains to `EndOfStream` so the connection stays reusable.
    /// This is the cancel-safety win — no pool poisoning, no socket
    /// teardown, no wasted server CPU.
    ExecuteQuery {
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
        reply: oneshot::Sender<Result<()>>,
    },

    /// Begin an INSERT session. Sends `INSERT INTO ... FORMAT Native`,
    /// reads the schema Data block (0 rows) the server returns, and
    /// transitions the actor into [`InsertActive`](ActorState::InsertActive)
    /// state. Subsequent commands other than `SendInsertBlock` /
    /// `FinishInsert` are rejected with an error reply.
    ///
    /// Returns the column headers `(name, type_name)` declared by the
    /// server.
    BeginInsert {
        query: String,
        reply: oneshot::Sender<Result<Vec<(String, String)>>>,
    },

    /// Send one data block during an active INSERT.
    ///
    /// **Full-duplex correctness fix:** before sending the block, the
    /// actor drains any packets the reader sub-task has buffered
    /// (using `try_recv`, non-blocking). If a `ServerPacket::Exception`
    /// has arrived (e.g. server detected a constraint violation from
    /// the previous block), the insert is aborted immediately — no
    /// more bytes go on the wire. Today's `NativeConnection` only
    /// reads the response stream in `finish_insert()`, so a server
    /// Exception isn't observed until potentially many MB later.
    SendInsertBlock {
        column_bytes: Vec<u8>,
        num_columns: usize,
        num_rows: usize,
        reply: oneshot::Sender<Result<()>>,
    },

    /// Finish the active INSERT: send the empty terminator block,
    /// drain to `EndOfStream`, and return to `Idle` state. After this
    /// the connection is ready for any other command.
    FinishInsert { reply: oneshot::Sender<Result<()>> },

    /// Execute a streaming SELECT and forward each `ServerPacket`
    /// (Data, Progress, ProfileInfo, EndOfStream, Exception) into the
    /// caller's mpsc until terminal packet.
    ///
    /// **Cancel-on-drop:** if the caller drops the receiver mid-stream
    /// the actor sends [`writer::send_cancel`] to the server, drains
    /// remaining packets to `EndOfStream`, and returns the connection
    /// to the pool reusable. This is the streaming cancel-safety win —
    /// today's cursor leaves the connection in an unknown state on
    /// drop, forcing pool teardown.
    ExecuteStream {
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
        results: mpsc::Sender<Result<ServerPacket>>,
    },
}

/// Internal state machine — what is the actor in the middle of?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActorState {
    /// No operation in flight; all commands accepted.
    Idle,
    /// `BeginInsert` succeeded; only `SendInsertBlock` and
    /// `FinishInsert` are accepted. Other commands receive an error
    /// reply (`busy in INSERT session`).
    InsertActive,
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

    /// Execute a non-streaming query.
    ///
    /// `query_id` is sent verbatim in the query packet header; pass `""`
    /// to let the server generate its own. `extra_settings` are merged
    /// over the connection-level settings (per-query overrides).
    ///
    /// **Cancellation safety:** if this future is cancelled (caller's
    /// timeout/select! drops the rx side), the actor sends the
    /// ClickHouse Cancel packet and drains to `EndOfStream`. The
    /// connection is **not** poisoned — it's safely returned to the
    /// pool. This is the foundational cancel-safety win.
    ///
    /// # Errors
    ///
    /// - [`Error::Custom`] if the actor has exited.
    /// - [`Error::BadResponse`] if the server returns Exception.
    pub(crate) async fn execute_query(
        &self,
        query_id: &str,
        query: &str,
        extra_settings: &[(String, String)],
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .send(ConnectionCmd::ExecuteQuery {
                query_id: query_id.to_owned(),
                query: query.to_owned(),
                extra_settings: extra_settings.to_vec(),
                reply,
            })
            .await
            .map_err(|_| Error::Custom("connection actor closed".into()))?;
        rx.await
            .map_err(|_| Error::Custom("connection actor dropped during query".into()))?
    }

    /// Begin an INSERT session. Sends the INSERT statement, reads the
    /// schema Data block, returns the column headers
    /// `(name, type_name)`. Subsequent commands other than
    /// `send_insert_block` / `finish_insert` will be rejected until
    /// the session ends.
    ///
    /// # Errors
    ///
    /// - [`Error::Custom`] if the actor is closed.
    /// - [`Error::BadResponse`] if the server rejects the statement.
    pub(crate) async fn begin_insert(&self, query: &str) -> Result<Vec<(String, String)>> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .send(ConnectionCmd::BeginInsert {
                query: query.to_owned(),
                reply,
            })
            .await
            .map_err(|_| Error::Custom("connection actor closed".into()))?;
        rx.await
            .map_err(|_| Error::Custom("connection actor dropped during BeginInsert".into()))?
    }

    /// Send one data block during an active INSERT. The actor will
    /// drain any pending server packets first and abort early if the
    /// server has reported an Exception (full-duplex correctness fix).
    ///
    /// # Errors
    ///
    /// - [`Error::Custom`] if the actor is closed or in the wrong
    ///   state (use [`begin_insert`](Self::begin_insert) first).
    /// - [`Error::BadResponse`] if the server has reported an Exception
    ///   for this INSERT.
    pub(crate) async fn send_insert_block(
        &self,
        column_bytes: Vec<u8>,
        num_columns: usize,
        num_rows: usize,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .send(ConnectionCmd::SendInsertBlock {
                column_bytes,
                num_columns,
                num_rows,
                reply,
            })
            .await
            .map_err(|_| Error::Custom("connection actor closed".into()))?;
        rx.await
            .map_err(|_| Error::Custom("connection actor dropped during SendInsertBlock".into()))?
    }

    /// Finish the active INSERT. Sends the empty terminator block,
    /// drains to `EndOfStream`, returns the actor to `Idle` state.
    ///
    /// # Errors
    ///
    /// - [`Error::Custom`] if the actor is closed or not in an INSERT
    ///   session.
    /// - [`Error::BadResponse`] if the server reports a final Exception.
    pub(crate) async fn finish_insert(&self) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .send(ConnectionCmd::FinishInsert { reply })
            .await
            .map_err(|_| Error::Custom("connection actor closed".into()))?;
        rx.await
            .map_err(|_| Error::Custom("connection actor dropped during FinishInsert".into()))?
    }

    /// Begin a streaming SELECT. Returns the receive end of an mpsc
    /// channel that the actor will push every received `ServerPacket`
    /// into until `EndOfStream` or `Exception`.
    ///
    /// **Cancel-on-drop:** drop the returned receiver to abort the
    /// stream — the actor sends the protocol Cancel packet, drains to
    /// `EndOfStream`, and the connection stays usable in the pool.
    /// No need for `drain()` calls or explicit cancellation; it's
    /// automatic.
    ///
    /// `capacity` bounds the in-flight packet buffer. Larger
    /// capacities = more read-ahead at the cost of memory; the actor's
    /// reader sub-task will pause when full, providing natural
    /// backpressure to the server (kernel TCP buffer fills, server
    /// pauses sending). Production callers typically use 64–256.
    ///
    /// # Errors
    ///
    /// Returns `Err` only if the actor is closed at submit time. Per-
    /// packet errors (server Exception, I/O failure mid-stream) are
    /// delivered through the returned receiver.
    pub(crate) async fn execute_stream(
        &self,
        query_id: &str,
        query: &str,
        extra_settings: &[(String, String)],
        capacity: usize,
    ) -> Result<mpsc::Receiver<Result<ServerPacket>>> {
        let (results, rx) = mpsc::channel(capacity);
        self.inner
            .send(ConnectionCmd::ExecuteStream {
                query_id: query_id.to_owned(),
                query: query.to_owned(),
                extra_settings: extra_settings.to_vec(),
                results,
            })
            .await
            .map_err(|_| Error::Custom("connection actor closed".into()))?;
        Ok(rx)
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
    server_hello: Arc<ServerHello>,
    compression: NativeCompressionMethod,
    settings: Vec<(String, String)>,
    poisoned: Arc<AtomicBool>,
    keepalive: Duration,
    last_activity: Instant,
    /// Protocol-state machine. Idle by default; transitions to
    /// `InsertActive` on `BeginInsert` and back on `FinishInsert`.
    state: ActorState,
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
            // State-aware dispatch: in InsertActive state only the
            // INSERT continuation commands are accepted; everything
            // else gets a "busy" error reply so callers don't silently
            // hang.
            match (cmd, self.state) {
                (ConnectionCmd::Ping { reply }, ActorState::Idle) => {
                    let result = self.do_ping().await;
                    if result.is_err() {
                        self.poisoned.store(true, Ordering::Release);
                    }
                    let _ = reply.send(result);
                }
                (
                    ConnectionCmd::ExecuteQuery {
                        query_id,
                        query,
                        extra_settings,
                        reply,
                    },
                    ActorState::Idle,
                ) => {
                    let result = self
                        .do_execute_query(&query_id, &query, &extra_settings, reply)
                        .await;
                    if result.is_err() {
                        self.poisoned.store(true, Ordering::Release);
                    }
                }
                (ConnectionCmd::BeginInsert { query, reply }, ActorState::Idle) => {
                    match self.do_begin_insert(&query).await {
                        Ok(headers) => {
                            self.state = ActorState::InsertActive;
                            let _ = reply.send(Ok(headers));
                        }
                        Err(e) => {
                            self.poisoned.store(true, Ordering::Release);
                            let _ = reply.send(Err(e));
                        }
                    }
                }
                (
                    ConnectionCmd::SendInsertBlock {
                        column_bytes,
                        num_columns,
                        num_rows,
                        reply,
                    },
                    ActorState::InsertActive,
                ) => {
                    let result = self
                        .do_send_insert_block(&column_bytes, num_columns, num_rows)
                        .await;
                    if let Err(ref e) = result {
                        // Distinguish I/O failure (poison) from
                        // server-side Exception during INSERT (state
                        // returns to Idle, but socket is fine — the
                        // server has already drained on its side and
                        // sent EndOfStream, which our drain consumed).
                        if matches!(e, Error::BadResponse(_)) {
                            self.state = ActorState::Idle;
                        } else {
                            self.poisoned.store(true, Ordering::Release);
                        }
                    }
                    let _ = reply.send(result);
                }
                (ConnectionCmd::FinishInsert { reply }, ActorState::InsertActive) => {
                    let result = self.do_finish_insert().await;
                    if result.is_err() {
                        self.poisoned.store(true, Ordering::Release);
                    }
                    self.state = ActorState::Idle;
                    let _ = reply.send(result);
                }
                (
                    ConnectionCmd::ExecuteStream {
                        query_id,
                        query,
                        extra_settings,
                        results,
                    },
                    ActorState::Idle,
                ) => {
                    let result = self
                        .do_execute_stream(&query_id, &query, &extra_settings, results)
                        .await;
                    if result.is_err() {
                        self.poisoned.store(true, Ordering::Release);
                    }
                }
                // Mismatched state — reject without disturbing the actor.
                (cmd, state) => self.reject_for_state(cmd, state),
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
    /// Connect, perform the ClickHouse hello/addendum handshake, and
    /// spawn the actor with the resulting state. Returns an
    /// [`OwnedConnection`] ready to accept commands via its
    /// [`ConnectionHandle`].
    ///
    /// This mirrors [`crate::native::connection::NativeConnection::open`]
    /// for the actor-backed path. Pool integration (and the migration
    /// of `NativeConnection` itself to delegate to this) live in
    /// later commits on this branch.
    pub(crate) async fn open(
        addr: &SocketAddr,
        database: &str,
        username: &str,
        password: &str,
        compression: NativeCompressionMethod,
        settings: Vec<(String, String)>,
        tls: &TlsConfig,
    ) -> Result<OwnedConnection> {
        let stream = connect_stream(addr, tls).await?;
        let (read_half, write_half) = tokio::io::split(stream);
        let mut reader = BufReader::with_capacity(CONN_READ_BUFFER, read_half);
        let mut writer = BufWriter::with_capacity(CONN_WRITE_BUFFER, write_half);

        // ClickHouse hello/addendum exchange.
        writer::send_hello(&mut writer, database, username, password).await?;
        let chunked_modes = (
            ChunkedProtocolMode::default(),
            ChunkedProtocolMode::default(),
        );
        let server_hello =
            reader::read_hello(&mut reader, DBMS_TCP_PROTOCOL_VERSION, chunked_modes).await?;
        writer::send_addendum(&mut writer, &server_hello).await?;

        // Hand the already-wrapped halves to spawn — keeps any bytes
        // that landed in the BufReader during the hello exchange. We
        // don't unwrap to raw halves because that would discard the
        // BufReader's internal buffer.
        Ok(Self::spawn_buffered(
            reader,
            writer,
            server_hello,
            compression,
            settings,
            DEFAULT_KEEPALIVE,
        ))
    }

    /// Same as [`open`](Self::open) but with already-handshaken halves.
    ///
    /// Used by tests that simulate the server side directly. Production
    /// callers use [`open`](Self::open).
    #[allow(dead_code)] // used by tests
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
        let reader = BufReader::with_capacity(CONN_READ_BUFFER, reader_half);
        let writer = BufWriter::with_capacity(CONN_WRITE_BUFFER, writer_half);
        Self::spawn_buffered(
            reader,
            writer,
            server_hello,
            compression,
            settings,
            keepalive,
        )
    }

    /// Spawn an actor over already-buffered halves. Used by
    /// [`open`](Self::open) so any bytes that landed in the
    /// BufReader during the hello exchange aren't lost.
    fn spawn_buffered(
        reader: BufReader<ReadHalf<MaybeTlsStream>>,
        writer: BufWriter<WriteHalf<MaybeTlsStream>>,
        server_hello: ServerHello,
        compression: NativeCompressionMethod,
        settings: Vec<(String, String)>,
        keepalive: Duration,
    ) -> OwnedConnection {
        let server_hello = Arc::new(server_hello);
        let poisoned = Arc::new(AtomicBool::new(false));

        // Spawn the reader sub-task — owns the reader half, pushes every
        // packet (or error) to pkt_tx until the socket closes.
        let (pkt_tx, pkt_rx) = mpsc::channel(PACKET_CHANNEL_CAPACITY);
        let revision = server_hello.revision_version;
        let reader_handle = tokio::spawn(reader_task(reader, pkt_tx, revision, compression));

        let actor = ConnectionActor {
            writer,
            pkt_rx,
            server_hello: Arc::clone(&server_hello),
            compression,
            settings,
            state: ActorState::Idle,
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

    /// Send Query + empty data block, drain response packets until
    /// `EndOfStream` or `Exception`.
    ///
    /// **Cancel-on-drop:** between each `recv_packet` we check whether
    /// the caller's `reply` oneshot has been dropped (`is_closed()`).
    /// If so, send the protocol-level Cancel packet, drain remaining
    /// packets to `EndOfStream`, return Ok — the connection stays in
    /// the pool, ready for the next caller. The original caller has
    /// already moved on; we don't try to deliver a result.
    ///
    /// Returns `Err` only on an I/O failure that broke the socket
    /// (caller poisons the connection on Err so the pool discards it).
    /// Server-side Exception is **not** an I/O failure — it's reported
    /// through `reply` as `Err(BadResponse)` and we return `Ok(())`.
    async fn do_execute_query(
        &mut self,
        query_id: &str,
        query: &str,
        extra_settings: &[(String, String)],
        reply: oneshot::Sender<Result<()>>,
    ) -> Result<()> {
        let revision = self.server_hello.revision_version;
        let compression = self.compression;
        let settings = merge_settings(&self.settings, extra_settings);

        // Send the query. If the writer dies here, propagate up so
        // the caller poisons the connection.
        writer::send_query(
            &mut self.writer,
            query_id,
            query,
            &settings,
            revision,
            compression,
        )
        .await?;
        writer::send_empty_block(&mut self.writer, compression).await?;

        let mut reply = Some(reply);
        let mut cancelled = false;

        loop {
            // Caller-cancellation check before each blocking recv.
            // `reply.is_closed()` returns true the moment the caller
            // drops their receiver — that's our signal to send Cancel
            // and drain.
            if !cancelled
                && let Some(r) = reply.as_ref()
                && r.is_closed()
            {
                writer::send_cancel(&mut self.writer).await?;
                cancelled = true;
                // Drop the now-useless reply slot.
                reply = None;
            }

            let pkt = self.recv_packet().await?;
            match pkt {
                ServerPacket::EndOfStream => {
                    if let Some(r) = reply.take() {
                        let _ = r.send(Ok(()));
                    }
                    return Ok(());
                }
                ServerPacket::Exception(err) => {
                    if let Some(r) = reply.take() {
                        let _ = r.send(Err(Error::BadResponse(err.to_string())));
                    }
                    return Ok(());
                }
                _ => {
                    // Discard Data/Progress/ProfileInfo — caller asked
                    // for execute, not stream.
                    continue;
                }
            }
        }
    }

    /// Begin an INSERT: send the INSERT statement + empty data block,
    /// drain response packets until the server returns the schema
    /// `Data` block (0 rows). Returns the column headers
    /// `(name, type_name)` declared by the server.
    async fn do_begin_insert(&mut self, query: &str) -> Result<Vec<(String, String)>> {
        let revision = self.server_hello.revision_version;
        let compression = self.compression;

        writer::send_query(
            &mut self.writer,
            "",
            query,
            &self.settings,
            revision,
            compression,
        )
        .await?;
        writer::send_empty_block(&mut self.writer, compression).await?;

        loop {
            let pkt = self.recv_packet().await?;
            match pkt {
                ServerPacket::Data(block) => {
                    return Ok(block
                        .column_headers
                        .into_iter()
                        .map(|h| (h.name, h.type_name))
                        .collect());
                }
                ServerPacket::Exception(err) => {
                    return Err(Error::BadResponse(err.to_string()));
                }
                _ => continue, // skip Progress/ProfileInfo
            }
        }
    }

    /// Send one data block during an active INSERT.
    ///
    /// **Full-duplex check:** before writing, drain any packets the
    /// reader has buffered (non-blocking `try_recv`). If the server has
    /// sent a `ServerPacket::Exception` (e.g. constraint violation
    /// detected on a previous block), abort early — no point sending
    /// another MB of data the server will reject.
    ///
    /// The drain also consumes any `Progress`/`ProfileInfo` packets
    /// the server emits between blocks; they're not surfaced to the
    /// caller (yet — observability hooks are a future enhancement).
    async fn do_send_insert_block(
        &mut self,
        column_bytes: &[u8],
        num_columns: usize,
        num_rows: usize,
    ) -> Result<()> {
        // Drain any pending packets non-blockingly. This is the
        // full-duplex correctness fix.
        loop {
            match self.pkt_rx.try_recv() {
                Ok(Ok(ServerPacket::Exception(err))) => {
                    // Server has rejected the insert. Drain any
                    // remaining packets up to EndOfStream so the
                    // socket stays clean.
                    self.drain_to_eos().await;
                    return Err(Error::BadResponse(err.to_string()));
                }
                Ok(Ok(_other)) => continue, // Progress / ProfileInfo / Log — discard
                Ok(Err(e)) => return Err(e),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return Err(Error::Custom("reader task exited mid-INSERT".into()));
                }
            }
        }

        writer::send_data_block(
            &mut self.writer,
            num_columns,
            num_rows,
            column_bytes,
            self.compression,
        )
        .await
    }

    /// Finish an INSERT: send the empty terminator block, then drain
    /// to `EndOfStream` (or surface any server-side Exception).
    async fn do_finish_insert(&mut self) -> Result<()> {
        writer::send_empty_block(&mut self.writer, self.compression).await?;
        loop {
            let pkt = self.recv_packet().await?;
            match pkt {
                ServerPacket::EndOfStream => return Ok(()),
                ServerPacket::Exception(err) => {
                    return Err(Error::BadResponse(err.to_string()));
                }
                _ => continue, // skip Progress/ProfileInfo
            }
        }
    }

    /// Best-effort drain of all packets up to EndOfStream. Used after
    /// a server Exception during INSERT to keep the socket clean for
    /// the next caller. Errors are swallowed — we're already in an
    /// error path.
    async fn drain_to_eos(&mut self) {
        while let Some(Ok(pkt)) = self.pkt_rx.recv().await {
            if matches!(pkt, ServerPacket::EndOfStream) {
                return;
            }
        }
    }

    /// Streaming SELECT execution.
    ///
    /// Pumps every received `ServerPacket` (Data/Progress/ProfileInfo/
    /// EndOfStream/Exception) into the caller's mpsc `results` channel.
    ///
    /// **Cancel-on-drop semantics:** if `results.send(...).await`
    /// returns `Err` (caller dropped the receiver), the actor sends
    /// the protocol Cancel packet and switches to drain mode — it
    /// keeps reading packets until `EndOfStream` so the connection
    /// stays clean for the next caller. Connection is NOT poisoned;
    /// cancellation is treated as a clean termination of this stream.
    ///
    /// Returns Ok always for clean cancellation; returns Err only on
    /// I/O failure that broke the socket (caller-handle() then poisons).
    async fn do_execute_stream(
        &mut self,
        query_id: &str,
        query: &str,
        extra_settings: &[(String, String)],
        results: mpsc::Sender<Result<ServerPacket>>,
    ) -> Result<()> {
        let revision = self.server_hello.revision_version;
        let compression = self.compression;
        let settings = merge_settings(&self.settings, extra_settings);

        writer::send_query(
            &mut self.writer,
            query_id,
            query,
            &settings,
            revision,
            compression,
        )
        .await?;
        writer::send_empty_block(&mut self.writer, compression).await?;

        let mut cancelled = false;

        loop {
            // Caller-cancellation check before each blocking recv.
            // `results.is_closed()` returns true the moment the caller
            // drops their receiver. send_cancel here is the SQL
            // protocol Cancel — server will stop streaming.
            if !cancelled && results.is_closed() {
                writer::send_cancel(&mut self.writer).await?;
                cancelled = true;
            }

            let pkt = match self.recv_packet().await {
                Ok(p) => p,
                Err(e) => {
                    // I/O failed mid-stream. If the caller is still
                    // listening, surface the error to them.
                    if !cancelled {
                        let _ = results.send(Err(e)).await;
                    }
                    // Return Err so handle() poisons the connection.
                    return Err(Error::Custom("stream read failed".into()));
                }
            };

            let is_terminal = matches!(pkt, ServerPacket::EndOfStream | ServerPacket::Exception(_));

            if !cancelled {
                if results.send(Ok(pkt)).await.is_err() {
                    // Caller just dropped receiver. Issue Cancel and
                    // continue draining. We DON'T break here — we
                    // need to consume packets until EndOfStream so
                    // the socket is clean.
                    if !cancelled {
                        // (race-tight check above might have missed
                        // the close — handle the second-chance path)
                        if let Err(e) = writer::send_cancel(&mut self.writer).await {
                            return Err(e);
                        }
                        cancelled = true;
                    }
                    if is_terminal {
                        return Ok(());
                    }
                    continue;
                }
            }

            if is_terminal {
                return Ok(());
            }
        }
    }

    /// Reject a command sent in the wrong state with a clear error.
    /// Caller's reply channel receives the error so they don't hang.
    fn reject_for_state(&self, cmd: ConnectionCmd, state: ActorState) {
        let err = || {
            Error::Custom(format!(
                "connection actor busy ({state:?}); command rejected"
            ))
        };
        match cmd {
            ConnectionCmd::Ping { reply } => {
                let _ = reply.send(Err(err()));
            }
            ConnectionCmd::ExecuteQuery { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            ConnectionCmd::BeginInsert { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            ConnectionCmd::SendInsertBlock { reply, .. } => {
                let _ = reply.send(Err(err()));
            }
            ConnectionCmd::FinishInsert { reply } => {
                let _ = reply.send(Err(err()));
            }
            ConnectionCmd::ExecuteStream { results, .. } => {
                // try_send so we don't block if the channel is full;
                // best-effort surface to the caller.
                let _ = results.try_send(Err(err()));
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

/// Merge connection-level `base` settings with per-query `extra`,
/// where `extra` overrides duplicates. Mirrors the helper in
/// [`crate::native::connection`].
fn merge_settings(base: &[(String, String)], extra: &[(String, String)]) -> Vec<(String, String)> {
    if extra.is_empty() {
        return base.to_vec();
    }
    let mut merged = base.to_vec();
    for (k, v) in extra {
        if let Some(slot) = merged.iter_mut().find(|(ek, _)| ek == k) {
            slot.1.clone_from(v);
        } else {
            merged.push((k.clone(), v.clone()));
        }
    }
    merged
}

/// Dispatch TCP vs TLS connection based on config.
///
/// Mirrors `NativeConnection::connect_stream` but as a free function
/// so the actor module doesn't have to reach into connection.rs's
/// inherent impl. Same logic, kept in sync by hand for now —
/// consolidation lands when `NativeConnection` itself migrates.
#[cfg(feature = "native-tls-rustls")]
async fn connect_stream(addr: &SocketAddr, tls: &TlsConfig) -> Result<MaybeTlsStream> {
    match tls {
        Some((config, server_name)) => {
            tcp::connect_tls(addr, config.clone(), server_name.clone()).await
        }
        None => tcp::connect(addr).await,
    }
}

#[cfg(not(feature = "native-tls-rustls"))]
async fn connect_stream(addr: &SocketAddr, _tls: &TlsConfig) -> Result<MaybeTlsStream> {
    tcp::connect(addr).await
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

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
        fn assert_clone<T: Clone>() {}
        assert_clone::<ConnectionHandle>();
    }

    // ---------------------------------------------------------------
    // In-process protocol mock — uses a localhost TCP loopback so we
    // exercise the actual MaybeTlsStream + tokio::io::split + actor
    // wiring (DuplexStream wouldn't fit MaybeTlsStream's enum).
    //
    // The "server" task reads bytes the actor writes and emits
    // protocol-correct response bytes. Just enough of the wire format
    // to verify the actor's command dispatch + cancel-on-drop, NOT a
    // full ClickHouse impl.
    // ---------------------------------------------------------------

    /// Spawn a paired (actor, server-side TcpStream) for one test.
    /// The server side is owned by the test; the actor side is wrapped
    /// in MaybeTlsStream and split, then handed to ConnectionActor.
    async fn paired() -> (OwnedConnection, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connect = TcpStream::connect(addr);
        let accept = async { listener.accept().await.unwrap().0 };
        let (client, server) = tokio::join!(connect, accept);
        let client = client.unwrap();
        // Disable Nagle so writes flush to the test server immediately.
        let _ = client.set_nodelay(true);
        let _ = server.set_nodelay(true);

        let (reader_half, writer_half) = tokio::io::split(MaybeTlsStream::Plain(client));

        // Stub a ServerHello — the actor only reads server_revision
        // (for protocol version) and that's not used by Ping.
        let server_hello = ServerHello {
            revision_version: 54_476, // recent revision; doesn't matter for Ping
            ..Default::default()
        };

        let owned = ConnectionActor::spawn_with_keepalive(
            reader_half,
            writer_half,
            server_hello,
            NativeCompressionMethod::None,
            vec![],
            // Long keepalive so it never fires during these tests.
            Duration::from_secs(3600),
        );
        (owned, server)
    }

    /// Read exactly one byte from the server side.
    async fn read_byte(server: &mut TcpStream) -> u8 {
        let mut buf = [0u8; 1];
        server.read_exact(&mut buf).await.unwrap();
        buf[0]
    }

    #[tokio::test]
    async fn ping_pong_roundtrip() {
        let (owned, mut server) = paired().await;
        let handle = owned.handle();

        // Server task: read one Ping byte (varint 4 = single byte 0x04),
        // then write one Pong byte (varint 4 = single byte 0x04).
        let server_task = tokio::spawn(async move {
            let b = read_byte(&mut server).await;
            assert_eq!(b, 4, "expected client Ping (varint 4)");
            server.write_all(&[4]).await.unwrap();
        });

        let result = handle.ping().await;
        assert!(result.is_ok(), "ping failed: {result:?}");
        server_task.await.unwrap();

        // Connection still alive after ping — no poison.
        assert!(handle.is_alive());

        owned.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn handle_sees_actor_dead_after_socket_close() {
        let (owned, server) = paired().await;
        let handle = owned.handle();

        // Server-side closes the socket immediately. The actor's reader
        // sub-task sees EOF, errors propagate, the actor exits, and
        // its handle reports !is_alive (eventually — there's a small
        // race where the actor's worker task sees pkt_rx.recv() return
        // None and exits the loop).
        drop(server);

        // Try a Ping; expect it to fail (writer either errors mid-send
        // or the actor exits before responding).
        let _ = handle.ping().await;

        // Within a short grace period the handle should report dead.
        for _ in 0..50 {
            if !handle.is_alive() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            !handle.is_alive(),
            "handle should report dead after socket close"
        );
    }

    #[tokio::test]
    async fn explicit_shutdown_completes_cleanly() {
        let (owned, _server) = paired().await;
        // No Ping; just shutdown immediately.
        let result = owned.shutdown().await;
        assert!(result.is_ok(), "shutdown failed: {result:?}");
    }

    #[tokio::test]
    async fn drop_aborts_reader_task() {
        let (owned, server) = paired().await;
        let handle = owned.handle();
        // Drop the OwnedConnection — its Drop should abort the reader
        // sub-task so the socket is released. Server-side detects
        // close on next read.
        drop(owned);
        // server-side read should see EOF eventually.
        let mut server = server;
        let mut buf = [0u8; 1];
        let result = tokio::time::timeout(Duration::from_millis(500), server.read(&mut buf)).await;
        match result {
            Ok(Ok(0)) => {} // EOF — expected
            Ok(Ok(n)) => panic!("expected EOF, got {n} bytes"),
            Ok(Err(_)) => {} // I/O error — also acceptable
            Err(_) => panic!("server-side read did not see EOF within 500 ms"),
        }
        // Handle should report dead (channel closed because actor exited).
        for _ in 0..50 {
            if !handle.is_alive() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!handle.is_alive());
    }
}
