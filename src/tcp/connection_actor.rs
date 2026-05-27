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
//! This branch ships the foundation only. The `ConnectionCmd` enum has
//! a single `Ping` variant -- a tracer bullet that exercises the
//! reader / writer / mpsc / worker plumbing end to end. ExecuteQuery,
//! INSERT, streaming cursor, and Cancel-on-drop land in subsequent
//! branches.
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
use crate::tcp::connect;
use crate::tcp::protocol::ServerHello;
use crate::tcp::reader::{self, ServerPacket};
use crate::tcp::transport::MaybeTlsStream;
use crate::tcp::writer;
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

/// Commands the [`ConnectionActor`] accepts.
///
/// Each variant embeds its own reply channel -- `oneshot` for single
/// replies, `mpsc` for streams -- so the actor never has to track
/// caller identity. Subsequent branches extend this enum with
/// `ExecuteQuery`, `BeginInsert`, `SendInsertBlock`, `FinishInsert`,
/// `ExecuteStream`. The tracer-bullet single-variant shape here makes
/// the plumbing (worker spawn + reader sub-task + reply channel)
/// reviewable in isolation.
pub(crate) enum ConnectionCmd {
    /// Send a Ping; reply with `Ok(())` when Pong arrives, `Err` on
    /// I/O failure or a server Exception in place of Pong.
    Ping { reply: oneshot::Sender<Result<()>> },
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
pub(crate) struct ConnectionHandle {
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
    pub(crate) fn is_alive(&self) -> bool {
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
    pub(crate) async fn ping(&self) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .send(ConnectionCmd::Ping { reply })
            .await
            .map_err(|_| Error::Custom("tcp: connection actor closed".into()))?;
        rx.await
            .map_err(|_| Error::Custom("tcp: connection actor dropped ping reply".into()))?
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
pub(crate) struct ActorConfig {
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
pub(crate) struct ConnectionActor {
    writer: BufWriter<WriteHalf<MaybeTlsStream>>,
    pkt_rx: mpsc::Receiver<ReaderMessage>,
    /// The reader sub-task lives as long as the actor. Dropping the
    /// `JoinHandle` does NOT cancel the task; we keep it so that
    /// observability tooling can attach to it in later branches and
    /// so the read half's lifetime is visibly tied to the actor.
    _reader_task: JoinHandle<()>,
    server_hello: Arc<ServerHello>,
    poisoned: Arc<AtomicBool>,
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
    pub(crate) fn spawn(stream: MaybeTlsStream, server_hello: ServerHello) -> ConnectionHandle {
        Self::spawn_with_config(stream, server_hello, ActorConfig::default())
    }

    /// Spawn an actor with explicit [`ActorConfig`] tunables. `spawn` is
    /// the zero-config shortcut (`ActorConfig::default()`); the pool's
    /// `Manager::create` uses this form to thread the connection's
    /// `read_timeout` dial through.
    pub(crate) fn spawn_with_config(
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
}

impl CommandWorker for ConnectionActor {
    type Command = ConnectionCmd;

    fn name() -> &'static str {
        "clickhouse.tcp-connection"
    }

    async fn handle(&mut self, cmd: Self::Command) {
        match cmd {
            ConnectionCmd::Ping { reply } => {
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
}
