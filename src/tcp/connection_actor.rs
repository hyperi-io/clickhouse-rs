//! Background-task socket-state owner for ClickHouse TCP connections.
//!
//! Built on the generic [`crate::worker::CommandWorker`] primitive. The
//! actor owns the writer half of a [`MaybeTlsStream`] for the lifetime
//! of the connection; an independent reader sub-task owns the read
//! half and forwards every decoded server packet through a bounded
//! mpsc channel. Together they give the actor full-duplex visibility
//! (the read loop is always running, even while a write command is in
//! flight) without burdening callers with cancellation safety at the
//! protocol layer.
//!
//! # Why an actor instead of borrowed I/O
//!
//! The classic shape -- callers hold `&mut Connection` across the
//! reader / writer halves for the duration of a query -- is
//! structurally cancellation-unsafe: any `tokio::select!`,
//! `tokio::time::timeout`, or HTTP-disconnect that drops the future
//! mid-`read_packet()` leaves the socket in an unknown state. Recovery
//! collapses to tearing down the TCP connection. Owning the I/O state
//! inside a long-lived task means dropping a caller's future never
//! disturbs the wire; instead the actor sees the reply-channel close
//! and reacts at protocol level (Cancel packet, drain to EndOfStream,
//! return to pool).
//!
//! Three capabilities only this shape unlocks:
//!
//! 1. Protocol-level Cancel during an in-flight query -- the writer is
//!    free to send a Cancel packet because no caller holds it.
//! 2. Full-duplex Exception detection during INSERT -- the reader
//!    sub-task surfaces a server-side rejection before the next block
//!    is written.
//! 3. Idle keepalive -- the actor can send Ping between commands
//!    without racing the caller for the writer.
//!
//! This branch ships Ping (the foundational tracer bullet) plus
//! `ExecuteQuery` -- the simplest write/drain command shape, used by
//! `Client::execute` for DDL / INSERT-without-rows / SET / etc. The
//! actor sends a Query packet followed by an empty-block terminator,
//! then drains response packets until EndOfStream or a server
//! Exception. Receiver drop mid-flight triggers a protocol-level
//! Cancel + bounded drain, leaving the socket reusable. INSERT
//! streaming and SELECT-with-rows cursors land in subsequent branches.
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
//! # Reader-half ownership
//!
//! `tokio::io::split` returns owned halves backed by an internal
//! `BiLock`. They are `Send + 'static` whenever the underlying stream
//! is, which is exactly what `tokio::spawn` needs for the reader
//! sub-task. The split is performed by [`crate::tcp::connect::split_buffered`]
//! at the point we hand the stream to the actor, so the actor owns the
//! writer half and only the writer half; the reader task owns its read
//! half and only its read half.

#![allow(dead_code)] // The full command surface lands in subsequent branches.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::io::{BufReader, BufWriter, ReadHalf, WriteHalf};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::error::{Error, Result};
use crate::tcp::client_info::ClientInfo;
use crate::tcp::connect;
use crate::tcp::protocol::ServerHello;
use crate::tcp::reader::{self, ServerPacket};
use crate::tcp::transport::MaybeTlsStream;
use crate::tcp::writer::{self, CLIENT_NAME, CLIENT_VERSION_MAJOR_STR, CLIENT_VERSION_MINOR_STR};
use crate::worker::{self, CommandWorker, WorkerControl, WorkerHandle};

/// Bounded internal channel between the reader sub-task and the
/// actor's command loop. 64 packets covers typical
/// Progress / ProfileInfo / Log interleaving for a single command
/// without stalling the reader. Falling behind blocks the reader,
/// which propagates kernel TCP backpressure to the server.
const PACKET_CHANNEL_CAPACITY: usize = 64;

/// Capacity of the actor's command mpsc. 16 covers the "one in-flight
/// plus a few queued" pattern that pooled connections see; high
/// fan-in callers (an inserter feeding hundreds of blocks/sec) should
/// be served by a different connection entirely.
const DEFAULT_CMD_CHANNEL: usize = 16;

/// Capacity of the per-stream mpsc that
/// [`ConnectionHandle::execute_stream_cursor`] allocates between the
/// actor and the [`crate::tcp::cursor::TcpRawCursor`]. 16 matches the
/// in-flight + a-few-queued shape the reader sub-task's own
/// `PACKET_CHANNEL_CAPACITY = 64` already buffers behind; raising it
/// would only delay backpressure, not reduce memory pressure.
const STREAM_CHANNEL_CAPACITY: usize = 16;

/// Upper bound on how long the actor waits to drain response packets
/// to EndOfStream after sending a Cancel or after a server Exception.
/// A wedged server that ignores Cancel must not block the actor task
/// (and thereby the pool slot) indefinitely. 30 s is a conservative
/// v1 cap that leaves comfortable headroom for normal cancellation
/// latency over LAN/WAN connections; per-query tuning is a later
/// concern once dials are wired through `Client`.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Soft cap on a single INSERT block's pre-encoded payload. A caller
/// that hands the actor an enormous block would otherwise stall the
/// actor task (and its pool slot) for the duration of one large socket
/// write, blocking every other command on that connection -- an
/// effective self-DoS for a misconfigured client. Blocks past this cap
/// are rejected with an error instead of transmitted; split the batch
/// into smaller blocks (server `max_insert_block_size` defaults to 1M
/// rows, far under this byte ceiling for typical row widths). 512 MiB
/// is generous head-room over any sane block; it exists to catch
/// pathological inputs, not to tune throughput.
const MAX_INSERT_BLOCK_BYTES: usize = 512 * 1024 * 1024;

/// Runtime state of the actor. `Idle` (post-handshake, between
/// commands, after FinishInsert / Exception); `InsertActive` between
/// a successful `BeginInsert` and the matching `FinishInsert` or
/// surfaced Exception.
///
/// Captured as a plain field rather than as type-state: the actor is
/// a single sequential task driving a wire protocol, and the small
/// state machine is easier to reason about as a runtime enum than as
/// phantom-type generics threaded through the `CommandWorker`
/// machinery. Out-of-state commands surface as
/// `Error::Custom("tcp: ...")` replies instead of panics so the
/// caller can recover (e.g. propagate the typed error to a
/// transaction-level handler).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActorState {
    Idle,
    InsertActive,
}

/// Commands the [`ConnectionActor`] accepts.
///
/// Each variant embeds its own reply channel -- `oneshot` for single
/// replies, `mpsc` for streams -- so the actor never has to track
/// caller identity. The streaming SELECT variant lands in Task 8.
pub(crate) enum ConnectionCmd {
    /// Send a Ping; reply with `Ok(())` when Pong arrives, `Err` on
    /// I/O failure or a server Exception in place of Pong.
    Ping { reply: oneshot::Sender<Result<()>> },
    /// Run a query that does not produce client-visible rows (DDL,
    /// `SET`, INSERT-with-no-data, etc.). The actor writes the Query
    /// packet, an empty-block terminator, then drains response packets
    /// until EndOfStream (`Ok`) or a server Exception (`Err`). Receiver
    /// drop mid-flight triggers a protocol-level Cancel followed by a
    /// bounded drain so the connection stays reusable.
    ExecuteQuery {
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
        reply: oneshot::Sender<Result<()>>,
    },
    /// Begin an INSERT session. The actor writes the Query packet
    /// (typically `INSERT INTO ... FORMAT Native`), drains protocol
    /// chatter until the server's schema-block Data packet arrives,
    /// transitions to [`ActorState::InsertActive`], and replies with
    /// the `(name, type_name)` column pairs the schema block carried.
    /// The columns vec is `Vec::new()` on a Task-7-vintage actor when
    /// the server's revision does not write columns into the schema
    /// block (no live ClickHouse server does this in practice -- 25.x
    /// always emits names + types); callers should treat an empty vec
    /// only as a "no schema information available" signal.
    BeginInsert {
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
        reply: oneshot::Sender<Result<Vec<(String, String)>>>,
    },
    /// Send a single Native-format data block during an in-flight
    /// INSERT. `column_bytes` is the pre-encoded payload from
    /// [`crate::native::encode_columns`]; the actor is purely a
    /// transport here, never re-encoding.
    ///
    /// Before writing the actor non-blockingly drains any packets the
    /// server has already pushed (Exception from a previous block's
    /// constraint violation, Progress / Log, etc.). An Exception
    /// surfaced through this path aborts the INSERT before any more
    /// bytes go on the wire -- the full-duplex correctness win the
    /// HTTP transport cannot get.
    SendInsertBlock {
        column_bytes: Vec<u8>,
        num_columns: u64,
        num_rows: u64,
        reply: oneshot::Sender<Result<()>>,
    },
    /// Terminate an INSERT session. The actor writes the empty-block
    /// sentinel, drains response packets to EndOfStream (or surfaces
    /// an Exception that arrives in between), then returns the actor
    /// to [`ActorState::Idle`] so the connection can be reused.
    FinishInsert { reply: oneshot::Sender<Result<()>> },
    /// Run a streaming SELECT. The actor sends the Query packet plus
    /// empty-block terminator, then forwards each non-`Pong`/
    /// non-`Log` server packet -- `Data` schema block,
    /// `DataBlock` payload, `Progress`, `ProfileInfo`, `EndOfStream`
    /// -- through `results`. The caller dropping the receiver
    /// (`results.closed()`) is observed by the actor and triggers a
    /// protocol-level Cancel + bounded drain so the connection stays
    /// reusable. A server Exception mid-stream is forwarded as
    /// `Err(exc.into_error())` then drained.
    ///
    /// The decoder runs INSIDE the reader sub-task
    /// (see [`crate::tcp::reader::read_packet`]); this command is
    /// purely a packet-forwarding pipeline. Per-row deserialisation
    /// happens in [`crate::tcp::cursor::TcpRawCursor`].
    ExecuteStream {
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
        results: mpsc::Sender<Result<ServerPacket>>,
    },
}

/// Cheap-clone send-side handle to a [`ConnectionActor`].
///
/// Holds a `WorkerHandle` (the mpsc sender), a shared
/// [`ServerHello`] for callers that need protocol revision facts, a
/// shared poisoning flag the pool consults on `recycle`, and a shared
/// [`WorkerControl`] that ties the actor task's lifetime to the
/// handle. All are reference-counted so cloning is `O(1)`.
///
/// The actor task lives exactly as long as the last `ConnectionHandle`
/// clone: the `Arc<WorkerControl>` drops with the final clone, and
/// `WorkerControl`'s `Drop` signals graceful shutdown (drain +
/// `on_shutdown` + exit), which closes the socket and lets the reader
/// sub-task fall out. The pool slot holds the canonical handle, so a
/// poisoned connection dropped on `recycle` refusal shuts its actor
/// down deterministically -- no detached keep-alive task, no leak.
#[derive(Clone)]
pub struct ConnectionHandle {
    inner: WorkerHandle<ConnectionCmd>,
    server_hello: Arc<ServerHello>,
    poisoned: Arc<AtomicBool>,
    /// Owns the actor task. Shared so every clone counts toward the
    /// task's lifetime; the last drop triggers graceful shutdown via
    /// `WorkerControl::drop`. Never read -- held only for its `Drop`.
    _control: Arc<WorkerControl<ConnectionCmd>>,
}

impl ConnectionHandle {
    /// True until the connection is poisoned. The pool consults this
    /// on `recycle` to decide whether to keep the connection or drop
    /// it.
    ///
    /// Poisoning is set by the actor on any I/O failure and by
    /// [`Self::poison`] from outside (e.g. a caller that observed an
    /// inconsistent state). It is not reset; once poisoned a handle
    /// stays not-alive for life.
    #[must_use]
    pub fn is_alive(&self) -> bool {
        !self.poisoned.load(Ordering::Acquire)
    }

    /// Mark the connection as broken. Idempotent and lock-free.
    pub(crate) fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    /// Negotiated server hello info (immutable after handshake).
    #[must_use]
    pub(crate) fn server_hello(&self) -> &ServerHello {
        &self.server_hello
    }

    /// Send a Ping and await Pong.
    ///
    /// Stray Progress / Log / ProfileEvents packets that arrive
    /// between Ping and Pong are drained inside the actor. The
    /// connection is poisoned on any I/O failure or reader sub-task
    /// exit.
    ///
    /// # Errors
    ///
    /// - [`Error::Custom`] if the actor has exited (command channel
    ///   closed) or dropped the reply channel before sending.
    /// - Any error from the underlying [`crate::tcp::writer::send_ping`]
    ///   or the reader sub-task.
    pub async fn ping(&self) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .send(ConnectionCmd::Ping { reply })
            .await
            .map_err(|_| Error::Custom("tcp: connection actor closed".into()))?;
        rx.await
            .map_err(|_| Error::Custom("tcp: connection actor dropped ping reply".into()))?
    }

    /// Run a query that does not stream rows back to the caller (DDL,
    /// `SET`, INSERT-without-data, `KILL QUERY`, etc.). Returns once
    /// the server emits EndOfStream OR an Exception, OR -- if the
    /// caller's awaiting future is cancelled mid-flight -- once the
    /// actor has sent a Cancel packet and drained to EndOfStream.
    ///
    /// Dropping the returned future (e.g. through `tokio::select!`,
    /// `tokio::time::timeout`, or HTTP-disconnect) closes the reply
    /// channel, which the actor observes via `reply.closed()` and
    /// reacts to at protocol level. The socket stays usable; the
    /// caller never sees a poisoned connection from a successful
    /// cancellation.
    ///
    /// # Errors
    ///
    /// - [`Error::Custom`] if the actor has exited (command channel
    ///   closed) or dropped the reply channel before sending.
    /// - [`Error::ServerException`] if the server returned an
    ///   Exception in place of EndOfStream.
    /// - Any error from the underlying writer, the reader sub-task,
    ///   or the drain timeout.
    pub async fn execute_query(
        &self,
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .send(ConnectionCmd::ExecuteQuery {
                query_id,
                query,
                extra_settings,
                reply,
            })
            .await
            .map_err(|_| Error::Custom("tcp: connection actor closed".into()))?;
        rx.await
            .map_err(|_| Error::Custom("tcp: connection actor dropped execute_query reply".into()))?
    }

    /// Begin an INSERT session and return the column metadata the
    /// server echoed in its schema block.
    ///
    /// `query` is the full SQL text (typically
    /// `INSERT INTO <table> FORMAT Native`). The actor sends a Query
    /// packet, drains protocol chatter until the schema-block Data
    /// packet arrives, transitions internal state to in-INSERT, and
    /// replies with the schema's `(name, type_name)` pairs.
    ///
    /// Subsequent [`Self::send_insert_block`] calls write Native
    /// blocks; the matching [`Self::finish_insert`] terminates the
    /// session.
    ///
    /// # v1 caveat
    ///
    /// Servers below the custom-serialization revision (any modern
    /// 25.x server is above it) write the schema body the same way as
    /// the encoder shipping in Phase 2 -- (name, type, flag) per
    /// column. The actor consumes those bytes off the wire via
    /// [`crate::tcp::reader::read_empty_data_block_schema`] regardless
    /// of revision; the returned vec carries exactly what the wire
    /// carried.
    ///
    /// # Errors
    ///
    /// - [`Error::Custom`] if the actor is already in `InsertActive`,
    ///   the command channel is closed, or the reply channel was
    ///   dropped.
    /// - [`Error::ServerException`] if the server rejected the INSERT
    ///   (auth, parse error, missing column, etc.) before the schema
    ///   block.
    /// - Any error from the underlying writer or the reader sub-task.
    pub async fn begin_insert(
        &self,
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
    ) -> Result<Vec<(String, String)>> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .send(ConnectionCmd::BeginInsert {
                query_id,
                query,
                extra_settings,
                reply,
            })
            .await
            .map_err(|_| Error::Custom("tcp: connection actor closed".into()))?;
        rx.await
            .map_err(|_| Error::Custom("tcp: connection actor dropped begin_insert reply".into()))?
    }

    /// Send one Native-format block during an in-flight INSERT.
    ///
    /// `column_bytes` is the pre-encoded Native payload from
    /// [`crate::native::encode_columns`]; the actor never re-encodes.
    /// The actor non-blockingly drains any server-pushed packets
    /// before writing -- a server Exception surfaced through that
    /// drain aborts the INSERT before any more bytes go on the wire.
    ///
    /// # Errors
    ///
    /// - [`Error::Custom`] if the actor is not in `InsertActive`, the
    ///   command channel is closed, or the reply channel was dropped.
    /// - [`Error::ServerException`] if the server emitted an Exception
    ///   from a previous block (full-duplex detection).
    /// - Any I/O error from the underlying writer.
    pub async fn send_insert_block(
        &self,
        column_bytes: Vec<u8>,
        num_columns: u64,
        num_rows: u64,
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
            .map_err(|_| Error::Custom("tcp: connection actor closed".into()))?;
        rx.await.map_err(|_| {
            Error::Custom("tcp: connection actor dropped send_insert_block reply".into())
        })?
    }

    /// Run a streaming SELECT and forward server packets through the
    /// returned receiver.
    ///
    /// The actor sends a Query packet (followed by the empty-block
    /// terminator that signals end-of-prequery), then pumps each
    /// non-`Pong` / non-`Log` server packet into the supplied `tx`.
    /// Packet ordering matches the wire: the server emits a schema
    /// block ([`ServerPacket::Data`] with `num_rows == 0`) first,
    /// then zero or more payload blocks ([`ServerPacket::DataBlock`]),
    /// terminated by [`ServerPacket::EndOfStream`]. The terminal
    /// packet is also forwarded so the cursor can use it as the
    /// "no more rows" sentinel.
    ///
    /// Dropping the receiver (`tx` reaches `closed()`) triggers a
    /// protocol-level Cancel inside the actor followed by a bounded
    /// drain to EndOfStream; the connection stays reusable. A server
    /// Exception mid-stream surfaces as
    /// [`Error::ServerException`] on the receiver and the actor
    /// drains the trailing EndOfStream itself.
    ///
    /// # Errors
    ///
    /// - [`Error::Custom`] if the actor has exited (command channel
    ///   closed) or dropped the reply before sending.
    /// - Send errors from the actor reach the caller via the
    ///   `results` channel, not the `Ok(())` return.
    pub(crate) async fn execute_stream(
        &self,
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
        results: mpsc::Sender<Result<ServerPacket>>,
    ) -> Result<()> {
        self.inner
            .send(ConnectionCmd::ExecuteStream {
                query_id,
                query,
                extra_settings,
                results,
            })
            .await
            .map_err(|_| Error::Custom("tcp: connection actor closed".into()))
    }

    /// Open a streaming SELECT and hand back a
    /// [`crate::tcp::cursor::TcpRawCursor`] that yields decoded
    /// blocks until `EndOfStream`.
    ///
    /// Internally allocates an mpsc pair with `STREAM_CHANNEL_CAPACITY`
    /// slots, dispatches `ExecuteStream`, and wraps the receiver. The
    /// channel capacity is small (16) -- the actor's reader sub-task
    /// already buffers up to `PACKET_CHANNEL_CAPACITY` (64) packets on
    /// the other side, so a second large buffer here would just delay
    /// memory pressure without reducing it.
    ///
    /// # Errors
    ///
    /// - [`Error::Custom`] if the actor command channel is closed.
    /// - The first error returned by the actor reaches the caller via
    ///   `TcpRawCursor::next_block()`, not this constructor.
    pub async fn execute_stream_cursor(
        &self,
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
    ) -> Result<crate::tcp::cursor::TcpRawCursor> {
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
        self.execute_stream(query_id, query, extra_settings, tx)
            .await?;
        Ok(crate::tcp::cursor::TcpRawCursor::from_receiver(rx))
    }

    /// Terminate an in-flight INSERT session.
    ///
    /// The actor writes an empty Data block (the server's INSERT
    /// end-of-input sentinel), drains response packets to EndOfStream
    /// or a server Exception, and returns to `Idle` so the connection
    /// can be reused.
    ///
    /// # Errors
    ///
    /// - [`Error::Custom`] if the actor is not in `InsertActive`, the
    ///   command channel is closed, or the reply channel was dropped.
    /// - [`Error::ServerException`] if the server rejected the INSERT
    ///   on commit (constraint violation discovered during merge,
    ///   etc.).
    /// - Any I/O or drain-timeout error.
    pub async fn finish_insert(&self) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .send(ConnectionCmd::FinishInsert { reply })
            .await
            .map_err(|_| Error::Custom("tcp: connection actor closed".into()))?;
        rx.await
            .map_err(|_| Error::Custom("tcp: connection actor dropped finish_insert reply".into()))?
    }
}

/// Internal message type carried over the reader -> actor mpsc.
/// Splitting `Packet` from `Error` lets the actor distinguish a
/// successfully decoded protocol packet from an I/O or decode failure
/// without forcing every consumer to pattern-match on `Result`.
enum ReaderMessage {
    Packet(ServerPacket),
    Error(Error),
}

/// Tunables threaded into the actor at spawn time. Kept as a small
/// struct (rather than positional `spawn` args) so future per-connection
/// dials can be added without churning every `spawn` call site.
#[derive(Clone, Copy, Debug, Default)]
pub struct ActorConfig {
    /// Per-packet idle read timeout. When `Some(d)`, a query/stream/
    /// begin-insert read that goes `d` without receiving ANY packet is
    /// treated as a stalled server: the connection is poisoned and the
    /// caller sees [`Error::TimedOut`]. The timer resets on every packet,
    /// so a long-but-progressing streaming SELECT never trips it -- this
    /// is an idle gap bound, NOT a whole-query deadline. `None` (default)
    /// leaves reads bounded only by caller-side cancellation, matching the
    /// pre-dial behaviour.
    pub read_timeout: Option<Duration>,
}

/// Background task that owns the writer half and the receive end of
/// the reader -> actor channel. Implements [`CommandWorker`] so the
/// generic runner from [`crate::worker`] handles the lifecycle
/// plumbing (command loop, drain on shutdown, panic surfacing).
pub struct ConnectionActor {
    writer: BufWriter<WriteHalf<MaybeTlsStream>>,
    pkt_rx: mpsc::Receiver<ReaderMessage>,
    /// The reader sub-task lives as long as the actor. Dropping the
    /// `JoinHandle` does NOT cancel the task; we keep it so that
    /// observability tooling can attach to it in later branches and
    /// so the read half's lifetime is visibly tied to the actor.
    _reader_task: JoinHandle<()>,
    server_hello: Arc<ServerHello>,
    poisoned: Arc<AtomicBool>,
    /// Idle or InsertActive. Gates which commands are accepted in
    /// `CommandWorker::handle` -- see [`ActorState`] rustdoc.
    state: ActorState,
    /// Per-packet idle read timeout; see [`ActorConfig::read_timeout`].
    read_timeout: Option<Duration>,
}

/// Per-packet idle-timeout future. Resolves after `timeout` elapses, or
/// never (`pending`) when `timeout` is `None`. Callers recreate it on
/// each loop iteration so it bounds the gap BETWEEN packets, not the
/// total query duration. Used as a `select!` arm alongside the
/// `pkt_rx.recv()` arm: whichever fires first wins.
async fn idle_timeout(timeout: Option<Duration>) {
    match timeout {
        Some(d) => tokio::time::sleep(d).await,
        None => std::future::pending::<()>().await,
    }
}

impl ConnectionActor {
    /// Spawn an actor over a freshly handshaken stream. Returns the
    /// cheap-clone handle, which owns the actor task via a shared
    /// `WorkerControl` (see [`ConnectionHandle`]): the task lives until
    /// the last handle clone drops, then shuts down gracefully.
    ///
    /// The stream is split into buffered read / write halves via
    /// [`crate::tcp::connect::split_buffered`]. The read half is
    /// moved into the reader sub-task (`tokio::spawn`); the write
    /// half is moved into the actor. Both halves drop together when
    /// the actor task exits, which closes the underlying socket.
    pub fn spawn(stream: MaybeTlsStream, server_hello: ServerHello) -> ConnectionHandle {
        Self::spawn_with_config(stream, server_hello, ActorConfig::default())
    }

    /// Spawn an actor with explicit [`ActorConfig`] tunables. `spawn` is
    /// the zero-config shortcut (`ActorConfig::default()`); the pool's
    /// `Manager::create` uses this form to thread the connection's
    /// `read_timeout` dial through.
    pub fn spawn_with_config(
        stream: MaybeTlsStream,
        server_hello: ServerHello,
        config: ActorConfig,
    ) -> ConnectionHandle {
        let (reader_half, writer_half) = connect::split_buffered(stream);

        let server_hello = Arc::new(server_hello);
        let poisoned = Arc::new(AtomicBool::new(false));

        // Reader sub-task: owns the read half, feeds packets through
        // the bounded mpsc until the socket closes or the actor's
        // receiver drops.
        let revision = server_hello.revision;
        let (pkt_tx, pkt_rx) = mpsc::channel(PACKET_CHANNEL_CAPACITY);
        let reader_task = tokio::spawn(reader_loop(reader_half, pkt_tx, revision));

        let actor = ConnectionActor {
            writer: writer_half,
            pkt_rx,
            _reader_task: reader_task,
            server_hello: Arc::clone(&server_hello),
            poisoned: Arc::clone(&poisoned),
            state: ActorState::Idle,
            read_timeout: config.read_timeout,
        };

        let control = worker::spawn(actor, DEFAULT_CMD_CHANNEL);
        let inner = control.handle();
        // Tie the worker's lifetime to the ConnectionHandle clones by
        // holding the WorkerControl behind an Arc on the handle itself.
        // When the last clone drops, the Arc drops, WorkerControl::drop
        // signals graceful shutdown, and the socket closes. An earlier
        // shape parked the control on a detached `pending()` task --
        // which held it (and its command-channel sender) for the whole
        // runtime, so the worker never saw the channel close and the
        // actor task + reader sub-task + socket FD leaked for every
        // connection ever created.
        ConnectionHandle {
            inner,
            server_hello,
            poisoned,
            _control: Arc::new(control),
        }
    }

    /// Send a Ping and wait for the Pong. Stray Progress / Log /
    /// ProfileEvents packets that interleave between Ping and Pong
    /// are logged at trace level and discarded; a server Exception
    /// surfaces as `Error::ServerException`. Reader sub-task exit or
    /// any I/O error poisons the connection.
    async fn do_ping(&mut self) -> Result<()> {
        writer::send_ping(&mut self.writer).await.inspect_err(|_| {
            self.poisoned.store(true, Ordering::Release);
        })?;
        loop {
            let msg = tokio::select! {
                biased;
                () = idle_timeout(self.read_timeout) => {
                    self.poisoned.store(true, Ordering::Release);
                    return Err(Error::TimedOut);
                }
                msg = self.pkt_rx.recv() => msg,
            };
            match msg {
                Some(ReaderMessage::Packet(ServerPacket::Pong)) => return Ok(()),
                Some(ReaderMessage::Packet(ServerPacket::Exception(exc))) => {
                    // Server-side rejection. Not an I/O failure -- the
                    // socket is still usable -- but the caller needs
                    // to see the error.
                    return Err(exc.into_error());
                }
                Some(ReaderMessage::Packet(other)) => {
                    tracing::trace!(
                        target: "clickhouse::tcp",
                        ?other,
                        "ignoring interleaved packet during ping"
                    );
                }
                Some(ReaderMessage::Error(e)) => {
                    self.poisoned.store(true, Ordering::Release);
                    return Err(e);
                }
                None => {
                    // Reader sub-task exited without sending an Error
                    // (e.g. its sender dropped). Treat as a torn-down
                    // socket; poison and surface.
                    self.poisoned.store(true, Ordering::Release);
                    return Err(Error::Custom(
                        "tcp: reader sub-task exited mid-ping".into(),
                    ));
                }
            }
        }
    }

    /// Write a Query packet (followed by an empty-block terminator)
    /// and drain server response packets until EndOfStream or an
    /// Exception. Watches the reply channel via `reply.closed()` so a
    /// caller-side cancellation triggers a protocol-level Cancel and a
    /// bounded drain instead of tearing the socket down.
    ///
    /// The `query_id` is echoed into `ClientInfo.initial_query_id` so
    /// the server-side `system.query_log.initial_query_id` matches the
    /// id the client uses for tracing and for
    /// `KILL QUERY WHERE query_id = ?`.
    async fn do_execute_query(
        &mut self,
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
        mut reply: oneshot::Sender<Result<()>>,
    ) {
        // Build ClientInfo per-query: 10 small strings plus a few
        // primitives. Profiling has not shown this to be hot; caching
        // on `Self` and rewriting query_id per call would save one
        // string clone per query and is a follow-up if it ever matters.
        let major: u64 = CLIENT_VERSION_MAJOR_STR.parse().unwrap_or(0);
        let minor: u64 = CLIENT_VERSION_MINOR_STR.parse().unwrap_or(0);
        let mut client_info = ClientInfo::for_initial_query(
            CLIENT_NAME,
            major,
            minor,
            self.server_hello.revision,
            "",
        );
        client_info.initial_query_id = query_id.clone();

        // Send the Query + empty-block terminator. Either failing
        // poisons -- the wire is half-written.
        let send_result = async {
            writer::send_query(
                &mut self.writer,
                self.server_hello.revision,
                &query_id,
                &query,
                &extra_settings,
                &client_info,
            )
            .await?;
            writer::send_empty_block(&mut self.writer, self.server_hello.revision).await
        }
        .await;
        if let Err(e) = send_result {
            self.poisoned.store(true, Ordering::Release);
            let _ = reply.send(Err(e));
            return;
        }

        // Drain response packets. `biased;` orders the cancel branch
        // first so a reply-drop racing against an in-flight packet
        // always wins -- we never want to deliver a successful Ok to
        // a caller that has already moved on.
        loop {
            tokio::select! {
                biased;

                _ = reply.closed() => {
                    // Caller cancelled. Send a Cancel packet, drain to
                    // EndOfStream (with timeout), then exit. The reply
                    // send is a no-op at this point because the
                    // receiver is gone.
                    if let Err(e) = writer::send_cancel(&mut self.writer).await {
                        self.poisoned.store(true, Ordering::Release);
                        tracing::warn!(
                            target: "clickhouse::tcp",
                            error = %e,
                            "failed to send Cancel after caller dropped reply"
                        );
                        return;
                    }
                    if let Err(e) = self.drain_to_end_of_stream().await {
                        tracing::warn!(
                            target: "clickhouse::tcp",
                            error = %e,
                            "drain after Cancel did not reach EndOfStream"
                        );
                        // drain_to_end_of_stream poisons on its own
                        // failure modes; nothing else to do here.
                    }
                    return;
                }

                () = idle_timeout(self.read_timeout) => {
                    // Server went silent for longer than read_timeout
                    // mid-query (a stalled backend, not a slow-but-
                    // progressing one -- the timer resets per packet).
                    // Poison so the pool drops this connection, and
                    // surface a retriable TimedOut.
                    self.poisoned.store(true, Ordering::Release);
                    let _ = reply.send(Err(Error::TimedOut));
                    return;
                }

                msg = self.pkt_rx.recv() => match msg {
                    Some(ReaderMessage::Packet(ServerPacket::EndOfStream)) => {
                        let _ = reply.send(Ok(()));
                        return;
                    }
                    Some(ReaderMessage::Packet(ServerPacket::Exception(exc))) => {
                        // A query Exception is TERMINAL: the server emits
                        // EndOfStream only on success (TCPHandler sends
                        // sendLogs + sendEndOfStream on the success path,
                        // and only sendException on the error path -- no
                        // trailing EndOfStream), and it keeps the
                        // connection open for reuse. Surface the typed
                        // error immediately and leave the connection
                        // un-poisoned. Do NOT drain -- there is nothing
                        // to drain, and draining would block until
                        // DRAIN_TIMEOUT and then poison a healthy
                        // connection. Matches cpp-client + clickhouse-go.
                        let _ = reply.send(Err(exc.into_error()));
                        return;
                    }
                    Some(ReaderMessage::Packet(_)) => {
                        // Data / Progress / Log / TableColumns /
                        // ProfileInfo / ProfileEvents / Pong /
                        // TimezoneUpdate -- nothing to surface for an
                        // execute_query call. Drain through and keep
                        // reading.
                        continue;
                    }
                    Some(ReaderMessage::Error(e)) => {
                        self.poisoned.store(true, Ordering::Release);
                        let _ = reply.send(Err(e));
                        return;
                    }
                    None => {
                        self.poisoned.store(true, Ordering::Release);
                        let _ = reply.send(Err(Error::Custom(
                            "tcp: reader sub-task exited mid-query".into(),
                        )));
                        return;
                    }
                },
            }
        }
    }

    /// Open an INSERT session: write the Query packet + empty-block
    /// terminator, drain protocol chatter until the server emits its
    /// schema-block Data packet, return the `(name, type_name)` pairs
    /// it carried. The caller's `handle()` arm transitions to
    /// `InsertActive` on success.
    ///
    /// The state transition is deliberately externalised so an
    /// `Err` return (server Exception, I/O failure) leaves the actor
    /// in `Idle` -- the connection can still be used for further
    /// commands. On I/O failure the actor is poisoned and the pool
    /// recycle path will drop it.
    async fn do_begin_insert(
        &mut self,
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
    ) -> Result<Vec<(String, String)>> {
        let major: u64 = CLIENT_VERSION_MAJOR_STR.parse().unwrap_or(0);
        let minor: u64 = CLIENT_VERSION_MINOR_STR.parse().unwrap_or(0);
        let mut client_info = ClientInfo::for_initial_query(
            CLIENT_NAME,
            major,
            minor,
            self.server_hello.revision,
            "",
        );
        client_info.initial_query_id = query_id.clone();

        // Send Query + empty-block terminator (the same shape cpp
        // `SendQuery()` uses for INSERTs -- the empty trailing block
        // signals end-of-prequery and prompts the server to respond
        // with its schema block).
        let send_result = async {
            writer::send_query(
                &mut self.writer,
                self.server_hello.revision,
                &query_id,
                &query,
                &extra_settings,
                &client_info,
            )
            .await?;
            writer::send_empty_block(&mut self.writer, self.server_hello.revision).await
        }
        .await;
        if let Err(e) = send_result {
            self.poisoned.store(true, Ordering::Release);
            return Err(e);
        }

        // Drain until the schema block (Data with num_rows == 0) or
        // an Exception. Progress / Log / TableColumns are normal
        // pre-schema chatter -- discard.
        loop {
            let msg = tokio::select! {
                biased;
                () = idle_timeout(self.read_timeout) => {
                    self.poisoned.store(true, Ordering::Release);
                    return Err(Error::TimedOut);
                }
                msg = self.pkt_rx.recv() => msg,
            };
            match msg {
                Some(ReaderMessage::Packet(ServerPacket::Data { columns, .. })) => {
                    // num_rows is always 0 for schema blocks now that
                    // read_packet routes num_rows > 0 through the
                    // DataBlock variant. Schema block reached -- return
                    // its (name, type_name) pairs to the caller.
                    return Ok(columns);
                }
                Some(ReaderMessage::Packet(ServerPacket::DataBlock(block))) => {
                    // A payload block before the schema block is a
                    // protocol violation -- INSERT clients always see
                    // schema first. Poison and surface so the caller
                    // doesn't sit in a broken INSERT.
                    self.poisoned.store(true, Ordering::Release);
                    return Err(Error::BadResponse(format!(
                        "tcp: server sent payload Data block (num_rows={}) \
                         before INSERT schema block",
                        block.num_rows
                    )));
                }
                Some(ReaderMessage::Packet(ServerPacket::Exception(exc))) => {
                    return Err(exc.into_error());
                }
                Some(ReaderMessage::Packet(ServerPacket::EndOfStream)) => {
                    // EndOfStream before any schema block means the
                    // server accepted-and-finished the query without
                    // expecting INSERT data (e.g. INSERT INTO ...
                    // SELECT, where the server fetches data itself).
                    // For Task 7 we treat this as a user mistake:
                    // BeginInsert is for client-feeding INSERT only.
                    return Err(Error::BadResponse(
                        "tcp: server returned EndOfStream before INSERT schema block \
                         (was this INSERT ... SELECT?)"
                            .into(),
                    ));
                }
                Some(ReaderMessage::Packet(_)) => continue,
                Some(ReaderMessage::Error(e)) => {
                    self.poisoned.store(true, Ordering::Release);
                    return Err(e);
                }
                None => {
                    self.poisoned.store(true, Ordering::Release);
                    return Err(Error::Custom(
                        "tcp: reader sub-task exited mid begin_insert".into(),
                    ));
                }
            }
        }
    }

    /// Send one Native block during an in-flight INSERT.
    ///
    /// Non-blockingly drains any packets the server has already
    /// pushed -- a server Exception surfaces here and aborts the
    /// INSERT before any more bytes hit the wire. This is the
    /// full-duplex correctness path: a constraint-violating row in
    /// block N can surface as an error on block N+1's send call
    /// without socket teardown.
    ///
    /// State transitions to `Idle` only on Exception. I/O failure
    /// poisons; successful write keeps the session in `InsertActive`.
    async fn do_send_insert_block(
        &mut self,
        column_bytes: Vec<u8>,
        num_columns: u64,
        num_rows: u64,
        reply: oneshot::Sender<Result<()>>,
    ) {
        // Reject a pathologically large block before it can stall the
        // actor on one giant socket write. The connection stays usable
        // (nothing hit the wire) and stays in InsertActive -- the
        // caller can re-send a smaller block or finish the INSERT.
        if column_bytes.len() > MAX_INSERT_BLOCK_BYTES {
            let _ = reply.send(Err(Error::Custom(format!(
                "tcp: INSERT block of {} bytes exceeds the {MAX_INSERT_BLOCK_BYTES} byte cap; \
                 split the batch into smaller blocks",
                column_bytes.len()
            ))));
            return;
        }

        // Full-duplex check FIRST. `try_recv` is non-blocking, so we
        // drain everything queued without waiting for new packets.
        while let Ok(msg) = self.pkt_rx.try_recv() {
            match msg {
                ReaderMessage::Packet(ServerPacket::Exception(exc)) => {
                    // Server aborted the INSERT mid-stream. The Exception
                    // is terminal (no EndOfStream follows) and the
                    // connection stays usable, so return to Idle and
                    // surface the error WITHOUT draining -- a drain would
                    // wait for an EndOfStream that never comes and stall
                    // until DRAIN_TIMEOUT before poisoning a healthy
                    // connection.
                    self.state = ActorState::Idle;
                    let _ = reply.send(Err(exc.into_error()));
                    return;
                }
                ReaderMessage::Packet(_) => continue, // Progress / Log -- keep draining
                ReaderMessage::Error(e) => {
                    self.poisoned.store(true, Ordering::Release);
                    self.state = ActorState::Idle;
                    let _ = reply.send(Err(e));
                    return;
                }
            }
        }

        let result = writer::send_data_block(
            &mut self.writer,
            self.server_hello.revision,
            "",
            &column_bytes,
            num_columns,
            num_rows,
        )
        .await;
        if result.is_err() {
            self.poisoned.store(true, Ordering::Release);
            self.state = ActorState::Idle;
        }
        let _ = reply.send(result);
    }

    /// Terminate an INSERT session: write the empty-block sentinel,
    /// drain response packets to EndOfStream (or surface an
    /// Exception). Caller's `handle()` arm returns the actor to Idle
    /// regardless of outcome.
    async fn do_finish_insert(&mut self) -> Result<()> {
        if let Err(e) = writer::send_empty_block(&mut self.writer, self.server_hello.revision).await
        {
            self.poisoned.store(true, Ordering::Release);
            return Err(e);
        }
        self.drain_to_end_of_stream().await
    }

    /// Streaming SELECT body. Writes the Query packet + empty-block
    /// terminator, then forwards every packet the reader sub-task
    /// emits into `results`. Schema blocks
    /// ([`ServerPacket::Data`]), payload blocks
    /// ([`ServerPacket::DataBlock`]), Progress / ProfileInfo /
    /// TableColumns / TimezoneUpdate, and the terminal EndOfStream
    /// all reach the caller in wire order so the cursor can stitch
    /// them back together. `Pong` and `Log` are protocol chatter
    /// with no cursor relevance and are dropped here.
    ///
    /// Cancellation works the same way as
    /// [`do_execute_query`]: `results.closed()` (the caller dropping
    /// the receiver) wins the `tokio::select!` with `biased;` and
    /// triggers a protocol Cancel + bounded drain.
    async fn do_execute_stream(
        &mut self,
        query_id: String,
        query: String,
        extra_settings: Vec<(String, String)>,
        results: mpsc::Sender<Result<ServerPacket>>,
    ) {
        let major: u64 = CLIENT_VERSION_MAJOR_STR.parse().unwrap_or(0);
        let minor: u64 = CLIENT_VERSION_MINOR_STR.parse().unwrap_or(0);
        let mut client_info = ClientInfo::for_initial_query(
            CLIENT_NAME,
            major,
            minor,
            self.server_hello.revision,
            "",
        );
        client_info.initial_query_id = query_id.clone();

        let send_result = async {
            writer::send_query(
                &mut self.writer,
                self.server_hello.revision,
                &query_id,
                &query,
                &extra_settings,
                &client_info,
            )
            .await?;
            writer::send_empty_block(&mut self.writer, self.server_hello.revision).await
        }
        .await;
        if let Err(e) = send_result {
            self.poisoned.store(true, Ordering::Release);
            let _ = results.send(Err(e)).await;
            return;
        }

        loop {
            tokio::select! {
                biased;

                _ = results.closed() => {
                    if let Err(e) = writer::send_cancel(&mut self.writer).await {
                        self.poisoned.store(true, Ordering::Release);
                        tracing::warn!(
                            target: "clickhouse::tcp",
                            error = %e,
                            "failed to send Cancel after stream receiver dropped"
                        );
                        return;
                    }
                    if let Err(e) = self.drain_to_end_of_stream().await {
                        tracing::warn!(
                            target: "clickhouse::tcp",
                            error = %e,
                            "drain after stream Cancel did not reach EndOfStream"
                        );
                    }
                    return;
                }

                () = idle_timeout(self.read_timeout) => {
                    // Server stalled mid-stream past read_timeout (the
                    // timer resets on every block, so a slow-but-
                    // progressing stream never reaches here). Poison so
                    // the pool drops the connection; surface retriable
                    // TimedOut to the cursor.
                    self.poisoned.store(true, Ordering::Release);
                    let _ = results.send(Err(Error::TimedOut)).await;
                    return;
                }

                msg = self.pkt_rx.recv() => match msg {
                    Some(ReaderMessage::Packet(ServerPacket::EndOfStream)) => {
                        // Forward EndOfStream as the cursor's
                        // "no more rows" sentinel; ignore send-error
                        // here -- the stream is complete either way.
                        let _ = results.send(Ok(ServerPacket::EndOfStream)).await;
                        return;
                    }
                    Some(ReaderMessage::Packet(ServerPacket::Exception(exc))) => {
                        // A streaming Exception is terminal: forward the
                        // typed error and stop. The server sends no
                        // EndOfStream after it, so do NOT drain (it would
                        // stall until DRAIN_TIMEOUT and then poison a
                        // healthy connection).
                        let _ = results.send(Err(exc.into_error())).await;
                        return;
                    }
                    Some(ReaderMessage::Packet(ServerPacket::Pong))
                    | Some(ReaderMessage::Packet(ServerPacket::Log))
                    | Some(ReaderMessage::Packet(ServerPacket::ProfileEvents)) => {
                        // Protocol chatter -- nothing the cursor cares
                        // about. ProfileEvents in particular arrives on
                        // every live query; the reader has already
                        // consumed its block bytes, so we just drop the
                        // marker here and keep reading.
                        continue;
                    }
                    Some(ReaderMessage::Packet(pkt)) => {
                        if results.send(Ok(pkt)).await.is_err() {
                            // Caller dropped the receiver between our
                            // last select and now. Send Cancel + drain
                            // so the next caller's stream pointer is
                            // clean.
                            if let Err(e) = writer::send_cancel(&mut self.writer).await {
                                self.poisoned.store(true, Ordering::Release);
                                tracing::warn!(
                                    target: "clickhouse::tcp",
                                    error = %e,
                                    "failed to send Cancel after late stream receiver drop"
                                );
                                return;
                            }
                            if let Err(e) = self.drain_to_end_of_stream().await {
                                tracing::warn!(
                                    target: "clickhouse::tcp",
                                    error = %e,
                                    "drain after late stream Cancel did not reach EndOfStream"
                                );
                            }
                            return;
                        }
                    }
                    Some(ReaderMessage::Error(e)) => {
                        self.poisoned.store(true, Ordering::Release);
                        let _ = results.send(Err(e)).await;
                        return;
                    }
                    None => {
                        self.poisoned.store(true, Ordering::Release);
                        let _ = results.send(Err(Error::Custom(
                            "tcp: reader sub-task exited mid-stream".into(),
                        ))).await;
                        return;
                    }
                },
            }
        }
    }

    /// Drain response packets until EndOfStream, bounded by
    /// [`DRAIN_TIMEOUT`]. Used after sending Cancel (post caller-drop)
    /// or after a server Exception so the next caller starts on a
    /// clean stream pointer.
    ///
    /// On timeout the connection is poisoned and an error is returned;
    /// pool recycle in a later branch drops the connection on the
    /// `is_alive()` check.
    async fn drain_to_end_of_stream(&mut self) -> Result<()> {
        let drain = async {
            loop {
                match self.pkt_rx.recv().await {
                    Some(ReaderMessage::Packet(ServerPacket::EndOfStream)) => return Ok(()),
                    Some(ReaderMessage::Packet(ServerPacket::Exception(exc))) => {
                        // A second Exception arriving mid-drain --
                        // surface it. The first error (if any) has
                        // already been sent to the caller via the
                        // reply channel; this one only affects the
                        // drain's return value.
                        return Err(exc.into_error());
                    }
                    Some(ReaderMessage::Packet(_)) => continue,
                    Some(ReaderMessage::Error(e)) => {
                        self.poisoned.store(true, Ordering::Release);
                        return Err(e);
                    }
                    None => {
                        self.poisoned.store(true, Ordering::Release);
                        return Err(Error::Custom(
                            "tcp: reader sub-task exited mid-drain".into(),
                        ));
                    }
                }
            }
        };
        match tokio::time::timeout(DRAIN_TIMEOUT, drain).await {
            Ok(r) => r,
            Err(_) => {
                self.poisoned.store(true, Ordering::Release);
                Err(Error::Custom(
                    "tcp: drain to EndOfStream exceeded 30s timeout".into(),
                ))
            }
        }
    }
}

impl CommandWorker for ConnectionActor {
    type Command = ConnectionCmd;

    fn name() -> &'static str {
        "clickhouse.tcp-connection"
    }

    async fn handle(&mut self, cmd: Self::Command) {
        // State-machine gate. The actor accepts Ping / ExecuteQuery /
        // BeginInsert only in `Idle`; SendInsertBlock / FinishInsert
        // only in `InsertActive`. Out-of-state commands reply with
        // `Error::Custom`; the actor task itself stays alive so the
        // caller can recover (e.g. issue FinishInsert to reset).
        match (self.state, cmd) {
            (ActorState::InsertActive, ConnectionCmd::Ping { reply }) => {
                let _ = reply.send(Err(Error::Custom("tcp: actor busy in INSERT".into())));
            }
            (ActorState::InsertActive, ConnectionCmd::ExecuteQuery { reply, .. }) => {
                let _ = reply.send(Err(Error::Custom("tcp: actor busy in INSERT".into())));
            }
            (ActorState::InsertActive, ConnectionCmd::BeginInsert { reply, .. }) => {
                let _ = reply.send(Err(Error::Custom("tcp: actor already in INSERT".into())));
            }
            (ActorState::InsertActive, ConnectionCmd::ExecuteStream { results, .. }) => {
                // Forward a single Err frame so the cursor's first
                // poll surfaces the misuse cleanly. The actor stays
                // alive; the caller can finish_insert and retry.
                let _ = results
                    .send(Err(Error::Custom("tcp: actor busy in INSERT".into())))
                    .await;
            }
            (ActorState::Idle, ConnectionCmd::SendInsertBlock { reply, .. }) => {
                let _ = reply.send(Err(Error::Custom(
                    "tcp: no INSERT session active".into(),
                )));
            }
            (ActorState::Idle, ConnectionCmd::FinishInsert { reply }) => {
                let _ = reply.send(Err(Error::Custom(
                    "tcp: no INSERT session active".into(),
                )));
            }
            (_, ConnectionCmd::Ping { reply }) => {
                let result = self.do_ping().await;
                if reply.send(result).is_err() {
                    // Caller dropped the receiver before we finished.
                    // Not an error in itself -- the socket is in a
                    // known state -- but worth a warn so an
                    // unexpected pattern shows up in logs.
                    tracing::warn!(
                        target: "clickhouse::tcp",
                        "ping reply dropped by caller before send"
                    );
                }
            }
            (
                _,
                ConnectionCmd::ExecuteQuery {
                    query_id,
                    query,
                    extra_settings,
                    reply,
                },
            ) => {
                // do_execute_query owns the reply Sender for the full
                // duration so it can watch `reply.closed()` and react
                // to caller-side cancellation at protocol level.
                self.do_execute_query(query_id, query, extra_settings, reply)
                    .await;
            }
            (
                _,
                ConnectionCmd::BeginInsert {
                    query_id,
                    query,
                    extra_settings,
                    reply,
                },
            ) => {
                let result = self
                    .do_begin_insert(query_id, query, extra_settings)
                    .await;
                if result.is_ok() {
                    self.state = ActorState::InsertActive;
                }
                if reply.send(result).is_err() {
                    tracing::warn!(
                        target: "clickhouse::tcp",
                        "begin_insert reply dropped by caller before send"
                    );
                }
            }
            (
                _,
                ConnectionCmd::SendInsertBlock {
                    column_bytes,
                    num_columns,
                    num_rows,
                    reply,
                },
            ) => {
                self.do_send_insert_block(column_bytes, num_columns, num_rows, reply)
                    .await;
            }
            (_, ConnectionCmd::FinishInsert { reply }) => {
                let result = self.do_finish_insert().await;
                // Whether finish succeeded or surfaced an Exception,
                // the INSERT session is over -- return to Idle so the
                // connection is reusable for non-INSERT commands.
                self.state = ActorState::Idle;
                if reply.send(result).is_err() {
                    tracing::warn!(
                        target: "clickhouse::tcp",
                        "finish_insert reply dropped by caller before send"
                    );
                }
            }
            (
                _,
                ConnectionCmd::ExecuteStream {
                    query_id,
                    query,
                    extra_settings,
                    results,
                },
            ) => {
                // do_execute_stream owns `results` for the full
                // command lifetime so it can watch `results.closed()`
                // and react to receiver-drop at protocol level
                // (same pattern as do_execute_query).
                self.do_execute_stream(query_id, query, extra_settings, results)
                    .await;
            }
        }
    }
    // on_idle / idle_interval: default no-op. An idle-keepalive driver
    // (periodic Ping between commands) is a later-branch concern.
    // on_shutdown: default. Closing the writer is the only shutdown
    // signal ClickHouse's TCP protocol recognises -- the kernel does
    // that for us when the writer half drops.
}

impl Drop for ConnectionActor {
    fn drop(&mut self) {
        if self.poisoned.load(Ordering::Acquire) {
            tracing::warn!(
                target: "clickhouse::tcp",
                "connection actor dropped while poisoned"
            );
        } else {
            tracing::debug!(
                target: "clickhouse::tcp",
                "connection actor dropped cleanly"
            );
        }
    }
}

/// Reader sub-task body. Owns the read half for the lifetime of the
/// connection. Reads packets one at a time and forwards them through
/// the channel; on I/O or decode failure sends a single `Error`
/// message and exits. On receiver close (actor dropped) exits
/// silently -- the actor has already torn the connection down.
async fn reader_loop(
    mut r: BufReader<ReadHalf<MaybeTlsStream>>,
    tx: mpsc::Sender<ReaderMessage>,
    server_revision: u64,
) {
    loop {
        match reader::read_packet(&mut r, server_revision).await {
            Ok(pkt) => {
                if tx.send(ReaderMessage::Packet(pkt)).await.is_err() {
                    // Actor dropped the receiver. Exit silently;
                    // nothing else can usefully observe the socket.
                    return;
                }
            }
            Err(e) => {
                let _ = tx.send(ReaderMessage::Error(e)).await;
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::io::ClickHouseWrite;
    use crate::tcp::protocol::{ClientPacketId, DBMS_TCP_PROTOCOL_VERSION, ServerPacketId};
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    // Compile-time check: ConnectionHandle must be Send + Sync so it
    // can cross task boundaries through any pool. A regression here
    // would silently break the actor's async-friendly story.
    static_assertions::assert_impl_all!(ConnectionHandle: Send, Sync);

    /// Test scaffold -- pair an actor over a loopback TcpStream with
    /// the "server" side of the same connection so we can script the
    /// wire format directly. Using a real loopback rather than
    /// `tokio::io::duplex` because `MaybeTlsStream::Plain` wraps a
    /// concrete `TcpStream`; duplex wouldn't fit the enum.
    async fn paired() -> (ConnectionHandle, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connect_fut = TcpStream::connect(addr);
        let accept_fut = async { listener.accept().await.unwrap().0 };
        let (client, server) = tokio::join!(connect_fut, accept_fut);
        let client = client.unwrap();
        let _ = client.set_nodelay(true);
        let _ = server.set_nodelay(true);

        let stream = MaybeTlsStream::Plain(client);
        let hello = ServerHello {
            server_name: "mock".to_string(),
            version: (1, 0, 0),
            revision: DBMS_TCP_PROTOCOL_VERSION,
            timezone: None,
            display_name: None,
        };
        let handle = ConnectionActor::spawn(stream, hello);
        (handle, server)
    }

    /// Like [`paired`] but threads an explicit [`ActorConfig`] (e.g. a
    /// `read_timeout`) into the spawned actor.
    async fn paired_with_config(config: ActorConfig) -> (ConnectionHandle, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connect_fut = TcpStream::connect(addr);
        let accept_fut = async { listener.accept().await.unwrap().0 };
        let (client, server) = tokio::join!(connect_fut, accept_fut);
        let client = client.unwrap();
        let _ = client.set_nodelay(true);
        let _ = server.set_nodelay(true);
        let stream = MaybeTlsStream::Plain(client);
        let hello = ServerHello {
            server_name: "mock".to_string(),
            version: (1, 0, 0),
            revision: DBMS_TCP_PROTOCOL_VERSION,
            timezone: None,
            display_name: None,
        };
        let handle = ConnectionActor::spawn_with_config(stream, hello, config);
        (handle, server)
    }

    /// Read one byte from the server side.
    async fn read_byte(server: &mut TcpStream) -> u8 {
        let mut buf = [0u8; 1];
        tokio::io::AsyncReadExt::read_exact(server, &mut buf)
            .await
            .unwrap();
        buf[0]
    }

    #[tokio::test]
    async fn ping_happy_path() {
        let (handle, mut server) = paired().await;

        // Server side: read the Ping varint, reply with Pong.
        let server_task = tokio::spawn(async move {
            let id = read_byte(&mut server).await;
            assert_eq!(u64::from(id), ClientPacketId::Ping as u64);
            server
                .write_var_uint(ServerPacketId::Pong as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();
            // Hold the connection open so the read half stays usable;
            // dropping `server` here would close the socket and
            // poison the actor on the next read.
            server
        });

        handle.ping().await.expect("ping should succeed");
        assert!(handle.is_alive());

        // Release the server side; reader sub-task will exit on the
        // resulting socket close, but the test has already passed.
        let _server = server_task.await.unwrap();
    }

    #[tokio::test]
    async fn ping_during_reader_disconnect() {
        let (handle, server) = paired().await;

        // Drop the server side immediately. The reader sub-task will
        // observe EOF on its next read; the ping should surface as
        // an error and the handle should be poisoned.
        drop(server);

        let result = handle.ping().await;
        assert!(result.is_err(), "ping should fail after server hangup");
        assert!(
            !handle.is_alive(),
            "handle should be poisoned after reader-task exit"
        );
    }

    #[tokio::test]
    async fn double_drop_safety() {
        let (handle, mut server) = paired().await;

        // Issue a ping; the server-side task is intentionally lazy
        // (sleeps before replying) so the call is in flight when we
        // drop the second handle clone.
        let clone = handle.clone();
        let ping_fut = tokio::spawn(async move { clone.ping().await });

        // Read the Ping byte to confirm the actor sent it, then
        // reply slowly so the future is alive across the drop.
        let server_task = tokio::spawn(async move {
            let _ = read_byte(&mut server).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            server
                .write_var_uint(ServerPacketId::Pong as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();
            server
        });

        // Drop the original handle while the ping is in flight; the
        // worker stays alive because the spawned ping-future still
        // holds its own clone.
        drop(handle);

        // Ping completes normally despite the drop.
        let ping_result = ping_fut.await.unwrap();
        assert!(ping_result.is_ok(), "ping should still succeed: {ping_result:?}");

        // Server side cleans up.
        let _server = server_task.await.unwrap();
    }

    // -----------------------------------------------------------------
    // ExecuteQuery
    // -----------------------------------------------------------------

    /// Drain (discard) bytes from the server side of the loopback until
    /// either `cancel_byte_seen` flips to true (a Cancel packet has
    /// arrived) or `eof` is observed. The Query packet plus its
    /// empty-block terminator is verbose; tests just need to swallow
    /// the bytes so the kernel buffer doesn't fill and stall the actor.
    async fn drain_client_bytes(server: &mut TcpStream) -> Vec<u8> {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        // Best-effort drain with a short overall budget -- tests that
        // need the bytes back inspect `buf` after.
        let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, server.read(&mut chunk)).await {
                Ok(Ok(0)) | Err(_) => break,
                Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
                Ok(Err(_)) => break,
            }
        }
        buf
    }

    #[tokio::test]
    async fn execute_query_happy_path() {
        let (handle, mut server) = paired().await;

        // Server side: swallow whatever the client writes, then send
        // EndOfStream so do_execute_query returns Ok.
        let server_task = tokio::spawn(async move {
            // Brief drain so the actor's writes don't stall on a full
            // socket buffer (the Query packet is small enough that
            // this never blocks in practice, but a real read keeps
            // the symmetry obvious).
            let _drained = drain_client_bytes(&mut server).await;
            server
                .write_var_uint(ServerPacketId::EndOfStream as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();
            server
        });

        let result = handle
            .execute_query("q1".into(), "SELECT 1".into(), Vec::new())
            .await;
        assert!(result.is_ok(), "execute_query should succeed: {result:?}");
        assert!(handle.is_alive());

        let _server = server_task.await.unwrap();
    }

    #[tokio::test]
    async fn execute_query_surfaces_server_exception() {
        let (handle, mut server) = paired().await;

        // Server side: drain client bytes, then write a query Exception
        // and NOTHING after it. A real server does not send EndOfStream
        // after a query Exception (it is terminal), so the actor must
        // surface the error promptly WITHOUT draining. If it drained, it
        // would block until DRAIN_TIMEOUT and then poison the
        // connection -- which the is_alive assertion below would catch.
        let server_task = tokio::spawn(async move {
            let _drained = drain_client_bytes(&mut server).await;
            server
                .write_var_uint(ServerPacketId::Exception as u64)
                .await
                .unwrap();
            server.write_i32_le(60i32).await.unwrap();
            server
                .write_string("DB::Exception".as_bytes())
                .await
                .unwrap();
            server.write_string("table not found".as_bytes()).await.unwrap();
            server.write_string("".as_bytes()).await.unwrap();
            tokio::io::AsyncWriteExt::write_u8(&mut server, 0u8)
                .await
                .unwrap(); // has_nested = false
            server.flush().await.unwrap();
            server
        });

        let err = handle
            .execute_query("q2".into(), "SELECT * FROM nope".into(), Vec::new())
            .await
            .expect_err("expected server Exception to surface");
        match err {
            Error::ServerException { code, .. } => assert_eq!(code, 60),
            other => panic!("expected ServerException, got {other:?}"),
        }
        // A query Exception is terminal and non-fatal: the connection
        // stays alive and reusable (NOT poisoned, NOT drained).
        assert!(
            handle.is_alive(),
            "connection should remain alive after a server Exception"
        );

        let _server = server_task.await.unwrap();
    }

    #[tokio::test]
    async fn execute_query_cancel_on_reply_drop() {
        let (handle, mut server) = paired().await;

        // Server side: drain the initial Query bytes, then sit quiet
        // -- no EndOfStream yet, so the actor stays in the drain loop.
        // Once the client drops the reply, the actor will send Cancel;
        // we read until we observe the Cancel varint (0x03), then
        // reply EndOfStream so the actor's bounded drain finishes.
        let server_task = tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            // Drain bytes until we have a fair chance of seeing the
            // Query+empty-block stream fully written.
            let mut initial = Vec::new();
            let mut chunk = [0u8; 4096];
            // Read with a small timeout per chunk so we eventually stop
            // and let the test issue the drop.
            loop {
                match tokio::time::timeout(
                    Duration::from_millis(50),
                    server.read(&mut chunk),
                )
                .await
                {
                    Ok(Ok(0)) => break,
                    Ok(Ok(n)) => initial.extend_from_slice(&chunk[..n]),
                    _ => break,
                }
            }
            assert!(
                !initial.is_empty(),
                "server should have seen at least the Query packet"
            );

            // Now keep reading; we expect a Cancel byte (0x03) to
            // arrive after the test drops the future.
            let mut saw_cancel = false;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            while tokio::time::Instant::now() < deadline {
                let remaining =
                    deadline.saturating_duration_since(tokio::time::Instant::now());
                match tokio::time::timeout(remaining, server.read(&mut chunk)).await {
                    Ok(Ok(0)) => break,
                    Ok(Ok(n)) => {
                        if chunk[..n].contains(&(ClientPacketId::Cancel as u8)) {
                            saw_cancel = true;
                            break;
                        }
                    }
                    _ => break,
                }
            }
            assert!(saw_cancel, "server should have observed Cancel byte");

            // Reply EndOfStream so the actor's drain finishes within
            // the timeout, leaving the connection reusable.
            server
                .write_var_uint(ServerPacketId::EndOfStream as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();
            server
        });

        // Issue execute_query but drop the future quickly via a
        // timeout that resolves before the server sends EndOfStream.
        // `tokio::time::timeout` drops the inner future on expiry,
        // which drops the oneshot::Receiver -- the cancel path.
        let cancel_handle = handle.clone();
        let exec_fut = cancel_handle.execute_query(
            "qcancel".into(),
            "SELECT sleep(10)".into(),
            Vec::new(),
        );
        // Brief timeout so the actor has time to flush the Query
        // packet to the server before we drop the future.
        let timeout_result =
            tokio::time::timeout(Duration::from_millis(150), exec_fut).await;
        assert!(
            timeout_result.is_err(),
            "execute_query should not have completed inside the timeout"
        );

        // Wait for the server-side task to observe the Cancel and send
        // EndOfStream.
        let mut server = server_task.await.unwrap();

        // The actor must still be alive (not poisoned) and the
        // connection still usable -- prove it by reusing the same
        // handle for a ping.
        assert!(
            handle.is_alive(),
            "handle should remain alive after a successful cancel-and-drain"
        );

        // Drive a ping over the same connection. The server-side task
        // now answers Pong; the actor must see it cleanly.
        let ping_server_task = tokio::spawn(async move {
            let id = read_byte(&mut server).await;
            assert_eq!(u64::from(id), ClientPacketId::Ping as u64);
            server
                .write_var_uint(ServerPacketId::Pong as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();
            server
        });
        handle
            .ping()
            .await
            .expect("connection should be reusable after cancel");
        let _server = ping_server_task.await.unwrap();
    }

    // -----------------------------------------------------------------
    // INSERT lifecycle (BeginInsert / SendInsertBlock / FinishInsert)
    // -----------------------------------------------------------------

    /// Write a Data packet with `num_rows = 0` and the supplied
    /// `(name, type_name)` pairs as the schema body. Matches the byte
    /// shape the encoder produces (and the live server emits) for the
    /// custom-serialization revision the test harness uses.
    async fn write_schema_block(
        server: &mut TcpStream,
        columns: &[(&str, &str)],
    ) {
        server
            .write_var_uint(ServerPacketId::Data as u64)
            .await
            .unwrap();
        server.write_string(b"").await.unwrap(); // table_name
        // Block info field pairs + terminator.
        server.write_var_uint(1).await.unwrap();
        AsyncWriteExt::write_u8(server, 0).await.unwrap();
        server.write_var_uint(2).await.unwrap();
        server.write_i32_le(-1).await.unwrap();
        server.write_var_uint(0).await.unwrap();
        server
            .write_var_uint(columns.len() as u64)
            .await
            .unwrap();
        server.write_var_uint(0).await.unwrap(); // num_rows
        for (name, ty) in columns {
            server.write_string(name.as_bytes()).await.unwrap();
            server.write_string(ty.as_bytes()).await.unwrap();
            // Custom-serialization flag (the test harness uses
            // DBMS_TCP_PROTOCOL_VERSION, which is above the gate, so
            // the encoder emits this byte and the actor's reader
            // consumes it).
            AsyncWriteExt::write_u8(server, 0).await.unwrap();
        }
        server.flush().await.unwrap();
    }

    /// Write a single server Exception packet with the supplied code +
    /// message, and nothing after it. This is the realistic terminal
    /// shape: a real server sends NO EndOfStream after a query
    /// Exception, so the actor must surface the error without draining.
    async fn write_server_exception(server: &mut TcpStream, code: i32, message: &str) {
        server
            .write_var_uint(ServerPacketId::Exception as u64)
            .await
            .unwrap();
        server.write_i32_le(code).await.unwrap();
        server.write_string(b"DB::Exception").await.unwrap();
        server.write_string(message.as_bytes()).await.unwrap();
        server.write_string(b"").await.unwrap();
        AsyncWriteExt::write_u8(server, 0).await.unwrap(); // has_nested = false
        server.flush().await.unwrap();
    }

    #[tokio::test]
    async fn insert_state_machine_rejects_ping_when_busy() {
        let (handle, mut server) = paired().await;

        // Server side: drain the BeginInsert Query bytes, send the
        // schema block, then hold the connection open so the actor
        // sits in InsertActive while we issue the ping.
        let server_task = tokio::spawn(async move {
            let _drained = drain_client_bytes(&mut server).await;
            write_schema_block(&mut server, &[("n", "UInt64")]).await;
            // Hold the connection -- keep the server side alive so the
            // ping rejection is observed before the actor sees EOF.
            tokio::time::sleep(Duration::from_millis(200)).await;
            server
        });

        // Drive BeginInsert; the call returns once the schema block
        // arrives.
        let headers = handle
            .begin_insert("q_busy".into(), "INSERT INTO t FORMAT Native".into(), Vec::new())
            .await
            .expect("begin_insert should succeed against scripted server");
        assert_eq!(
            headers,
            vec![("n".to_string(), "UInt64".to_string())]
        );

        // Ping while busy -- must reject without exiting InsertActive.
        let err = handle
            .ping()
            .await
            .expect_err("ping during InsertActive must error");
        match err {
            Error::Custom(msg) => assert!(
                msg.contains("busy"),
                "expected 'busy' in ping error, got {msg}"
            ),
            other => panic!("expected Custom busy error, got {other:?}"),
        }
        assert!(handle.is_alive());

        let _server = server_task.await.unwrap();
    }

    #[tokio::test]
    async fn send_insert_block_without_begin_errs() {
        let (handle, _server) = paired().await;

        let err = handle
            .send_insert_block(Vec::new(), 0, 0)
            .await
            .expect_err("send_insert_block in Idle must error");
        match err {
            Error::Custom(msg) => assert!(
                msg.contains("no INSERT session"),
                "expected 'no INSERT session' in error, got {msg}"
            ),
            other => panic!("expected Custom error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn finish_insert_returns_to_idle() {
        let (handle, mut server) = paired().await;

        // Server side: drain BeginInsert bytes, send schema block.
        // Then drain SendInsertBlock + FinishInsert bytes, send
        // EndOfStream so do_finish_insert returns Ok. Finally
        // answer the post-finish Ping with Pong.
        let server_task = tokio::spawn(async move {
            let _drained = drain_client_bytes(&mut server).await;
            write_schema_block(&mut server, &[("n", "UInt64")]).await;
            // Drain SendInsertBlock + FinishInsert (just bytes; we
            // do not parse them here).
            let _block_bytes = drain_client_bytes(&mut server).await;
            server
                .write_var_uint(ServerPacketId::EndOfStream as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();
            // Now serve the Ping that follows: read the Ping varint
            // and reply Pong.
            let id = read_byte(&mut server).await;
            assert_eq!(u64::from(id), ClientPacketId::Ping as u64);
            server
                .write_var_uint(ServerPacketId::Pong as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();
            server
        });

        let headers = handle
            .begin_insert("q_finish".into(), "INSERT INTO t FORMAT Native".into(), Vec::new())
            .await
            .expect("begin_insert should succeed");
        assert_eq!(headers.len(), 1);

        // Send one (empty) block -- the actor is a transport here,
        // it does not validate the payload.
        handle
            .send_insert_block(Vec::new(), 1, 0)
            .await
            .expect("send_insert_block should succeed");

        handle
            .finish_insert()
            .await
            .expect("finish_insert should return Ok on EndOfStream");

        // Prove the actor returned to Idle: a Ping must now succeed.
        handle
            .ping()
            .await
            .expect("connection should be reusable after finish_insert");
        assert!(handle.is_alive());

        let _server = server_task.await.unwrap();
    }

    #[tokio::test]
    async fn full_duplex_exception_aborts_send() {
        let (handle, mut server) = paired().await;

        // Server side: drain BeginInsert bytes, send schema block, then
        // push a single Exception (no EndOfStream after it -- the
        // realistic terminal shape) simulating a constraint violation
        // surfaced before the client's second SendInsertBlock.
        let server_task = tokio::spawn(async move {
            let _drained = drain_client_bytes(&mut server).await;
            write_schema_block(&mut server, &[("n", "UInt64")]).await;
            // Push an Exception "between blocks" -- give the actor a
            // moment to handle the schema block first.
            tokio::time::sleep(Duration::from_millis(20)).await;
            write_server_exception(&mut server, 241, "MEMORY_LIMIT_EXCEEDED").await;
            server
        });

        handle
            .begin_insert("q_fd".into(), "INSERT INTO t FORMAT Native".into(), Vec::new())
            .await
            .expect("begin_insert should succeed");

        // Wait long enough for the Exception to land in the reader's
        // mpsc queue, then call send_insert_block -- the try_recv
        // drain must see the Exception and abort before writing.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let err = handle
            .send_insert_block(Vec::new(), 1, 0)
            .await
            .expect_err("send_insert_block must surface the queued Exception");
        match err {
            Error::ServerException { code, .. } => assert_eq!(code, 241),
            other => panic!("expected ServerException(241), got {other:?}"),
        }

        // Actor should be Idle again; connection still usable for
        // non-INSERT commands.
        assert!(handle.is_alive());

        let _server = server_task.await.unwrap();
    }

    // -----------------------------------------------------------------
    // ExecuteStream (streaming SELECT)
    // -----------------------------------------------------------------

    /// Write a Data packet with one UInt64 column (n rows) for the
    /// streaming-SELECT tests. The encoder shape mirrors the
    /// server-side `SendData()` path: Data id, table_name, block info,
    /// num_columns, num_rows, then per-column (name, type_name,
    /// custom_ser_flag) + n u64 values.
    async fn write_uint64_payload_block(
        server: &mut TcpStream,
        values: &[u64],
    ) {
        server
            .write_var_uint(ServerPacketId::Data as u64)
            .await
            .unwrap();
        server.write_string(b"").await.unwrap(); // table_name
        // Block info field pairs + terminator.
        server.write_var_uint(1).await.unwrap();
        AsyncWriteExt::write_u8(server, 0).await.unwrap();
        server.write_var_uint(2).await.unwrap();
        server.write_i32_le(-1).await.unwrap();
        server.write_var_uint(0).await.unwrap();
        server.write_var_uint(1).await.unwrap(); // num_columns
        server
            .write_var_uint(values.len() as u64)
            .await
            .unwrap(); // num_rows
        // Column header: name, type_name, custom_ser_flag.
        server.write_string(b"n").await.unwrap();
        server.write_string(b"UInt64").await.unwrap();
        AsyncWriteExt::write_u8(server, 0).await.unwrap();
        // Column body: n x u64 LE.
        for v in values {
            server.write_u64_le(*v).await.unwrap();
        }
        server.flush().await.unwrap();
    }

    #[tokio::test]
    async fn execute_stream_yields_schema_then_payload_then_eos() {
        let (handle, mut server) = paired().await;

        let server_task = tokio::spawn(async move {
            let _drained = drain_client_bytes(&mut server).await;
            // Schema block (num_rows = 0) then a payload with three
            // values then EndOfStream.
            write_schema_block(&mut server, &[("n", "UInt64")]).await;
            write_uint64_payload_block(&mut server, &[10, 20, 30]).await;
            server
                .write_var_uint(ServerPacketId::EndOfStream as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();
            server
        });

        let mut cursor = handle
            .execute_stream_cursor(
                "rs_stream_unit".into(),
                "SELECT number AS n FROM numbers(3)".into(),
                Vec::new(),
            )
            .await
            .expect("execute_stream_cursor should succeed");

        // First block: schema (num_rows = 0, schema vec non-empty).
        let schema_block = cursor
            .next_block()
            .await
            .expect("schema block decode")
            .expect("schema block");
        assert_eq!(schema_block.num_rows, 0);
        assert_eq!(
            schema_block.schema,
            vec![("n".to_string(), "UInt64".to_string())]
        );

        // Second block: payload (num_rows = 3, UInt64 column).
        let payload = cursor
            .next_block()
            .await
            .expect("payload decode")
            .expect("payload block");
        assert_eq!(payload.num_rows, 3);
        match &payload.columns[0] {
            crate::native::DecodedColumn::UInt64(values) => {
                assert_eq!(values, &vec![10u64, 20, 30]);
            }
            other => panic!("expected UInt64, got {other:?}"),
        }

        // Terminal EndOfStream surfaces as Ok(None); cursor remains
        // safe to poll past completion.
        assert!(cursor.next_block().await.unwrap().is_none());
        assert!(cursor.next_block().await.unwrap().is_none());
        assert!(handle.is_alive());

        let _server = server_task.await.unwrap();
    }

    #[tokio::test]
    async fn execute_stream_surfaces_server_exception() {
        let (handle, mut server) = paired().await;

        // Server side: drain Query bytes, then a single Exception (no
        // EndOfStream after it -- the realistic terminal shape).
        let server_task = tokio::spawn(async move {
            let _drained = drain_client_bytes(&mut server).await;
            // Need not send a schema block first -- the server can
            // reject the query before any data flows.
            write_server_exception(&mut server, 60, "table not found").await;
            server
        });

        let mut cursor = handle
            .execute_stream_cursor(
                "rs_stream_err".into(),
                "SELECT * FROM doesnt_exist".into(),
                Vec::new(),
            )
            .await
            .expect("execute_stream_cursor should succeed");

        let err = cursor
            .next_block()
            .await
            .expect_err("server Exception must surface");
        match err {
            Error::ServerException { code, .. } => assert_eq!(code, 60),
            other => panic!("expected ServerException, got {other:?}"),
        }
        // Subsequent polls return Ok(None) -- cursor is terminal after
        // surfacing the error.
        assert!(cursor.next_block().await.unwrap().is_none());

        // Connection still usable after a server-side rejection.
        assert!(handle.is_alive());
        let _server = server_task.await.unwrap();
    }

    #[tokio::test]
    async fn execute_query_read_timeout_poisons() {
        // Server drains the Query bytes then goes silent -- it never
        // sends EndOfStream. With a short read_timeout the actor must
        // surface a retriable TimedOut (not hang) and poison the
        // connection so the pool drops it.
        let (handle, mut server) = paired_with_config(ActorConfig {
            read_timeout: Some(Duration::from_millis(80)),
        })
        .await;

        let server_task = tokio::spawn(async move {
            let _drained = drain_client_bytes(&mut server).await;
            // Hold the socket open but stay silent -- a stalled backend.
            tokio::time::sleep(Duration::from_millis(500)).await;
            server
        });

        let err = handle
            .execute_query("rq_timeout".into(), "SELECT sleep(9)".into(), Vec::new())
            .await
            .expect_err("a silent server must surface TimedOut, not hang");
        assert!(matches!(err, Error::TimedOut), "got {err:?}");
        assert!(err.is_retriable(), "TimedOut must be retriable");
        assert!(!handle.is_alive(), "timed-out connection must be poisoned");

        let _server = server_task.await.unwrap();
    }

    #[tokio::test]
    async fn execute_stream_read_timeout_poisons() {
        // Server sends the schema block then stalls before any payload.
        // The cursor's first post-schema poll must surface TimedOut and
        // poison the connection -- not block until the caller gives up.
        let (handle, mut server) = paired_with_config(ActorConfig {
            read_timeout: Some(Duration::from_millis(80)),
        })
        .await;

        let server_task = tokio::spawn(async move {
            // Send the schema block immediately (do NOT drain first --
            // drain_client_bytes runs a 200ms budget, which would delay
            // the schema past the 80ms read_timeout). The client's small
            // Query packet stays buffered in the kernel; the actor's send
            // does not stall. After the schema, go silent so the next
            // per-packet idle window elapses.
            write_schema_block(&mut server, &[("n", "UInt64")]).await;
            tokio::time::sleep(Duration::from_millis(500)).await;
            server
        });

        let mut cursor = handle
            .execute_stream_cursor(
                "rs_timeout".into(),
                "SELECT number AS n FROM numbers(9)".into(),
                Vec::new(),
            )
            .await
            .expect("execute_stream_cursor should succeed");

        // Schema block arrives first.
        let schema = cursor.next_block().await.unwrap().expect("schema block");
        assert_eq!(schema.num_rows, 0);

        // Next poll waits on a silent server -> TimedOut, not a hang.
        let err = cursor
            .next_block()
            .await
            .expect_err("silent server mid-stream must surface TimedOut");
        assert!(matches!(err, Error::TimedOut), "got {err:?}");
        assert!(!handle.is_alive(), "timed-out connection must be poisoned");

        let _server = server_task.await.unwrap();
    }

    #[tokio::test]
    async fn execute_stream_aligns_rows_across_multiple_blocks() {
        // Two payload blocks of different widths back-to-back then EOS.
        // The cursor must yield each block's rows in order with no
        // cross-boundary misalignment -- the silent-misalignment edge
        // the differential review flagged.
        let (handle, mut server) = paired().await;

        let server_task = tokio::spawn(async move {
            let _drained = drain_client_bytes(&mut server).await;
            write_schema_block(&mut server, &[("n", "UInt64")]).await;
            write_uint64_payload_block(&mut server, &[10, 20, 30]).await;
            write_uint64_payload_block(&mut server, &[40, 50]).await;
            server
                .write_var_uint(ServerPacketId::EndOfStream as u64)
                .await
                .unwrap();
            server.flush().await.unwrap();
            server
        });

        let mut cursor = handle
            .execute_stream_cursor(
                "rs_multiblock".into(),
                "SELECT number AS n FROM numbers(5)".into(),
                Vec::new(),
            )
            .await
            .expect("execute_stream_cursor should succeed");

        // Schema, then block 1 (3 rows), then block 2 (2 rows), then EOS.
        let schema = cursor.next_block().await.unwrap().expect("schema block");
        assert_eq!(schema.num_rows, 0);

        let b1 = cursor.next_block().await.unwrap().expect("first payload");
        assert_eq!(b1.num_rows, 3);
        match &b1.columns[0] {
            crate::native::DecodedColumn::UInt64(v) => assert_eq!(v, &vec![10u64, 20, 30]),
            other => panic!("expected UInt64, got {other:?}"),
        }

        let b2 = cursor.next_block().await.unwrap().expect("second payload");
        assert_eq!(b2.num_rows, 2);
        match &b2.columns[0] {
            crate::native::DecodedColumn::UInt64(v) => assert_eq!(v, &vec![40u64, 50]),
            other => panic!("expected UInt64, got {other:?}"),
        }

        assert!(cursor.next_block().await.unwrap().is_none());
        assert!(handle.is_alive());

        let _server = server_task.await.unwrap();
    }
}
