//! TCP connection setup for the ClickHouse native transport.
//!
//! [`connect_plain`] opens a [`TcpStream`] to the supplied address,
//! enables `TCP_NODELAY`, and configures TCP keepalive via
//! [`socket2::SockRef`]. Defaults are 60s idle / 20s interval / 3
//! retries -- chosen to survive Kubernetes kube-proxy iptables
//! connection-tracking idle timeouts (~30-60s) without producing
//! excessive probe traffic.
//!
//! [`open_handshaken`] is the high-level entry point the connection
//! pool will call: connect, then drive [`crate::tcp::handshake`]
//! against the unsplit stream, returning the ready-to-use stream and
//! the negotiated [`ServerHello`]. The plain / TLS choice is encoded
//! in [`ConnectKind`]; this branch ships the plain path only, with the
//! TLS variant wired by a later branch behind the `native-tls-rustls`
//! feature.
//!
//! The handshake is half-duplex (send Hello -> recv ServerHello ->
//! send addendum), so the connection actor that follows can split
//! the stream once for its long-lived read/write loop without the
//! handshake needing its own split. See [`split_buffered`] for the
//! actor-side helper.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{BufReader, BufWriter, ReadHalf, WriteHalf};
use tokio::net::TcpStream;

use crate::error::{Error, Result};
use crate::tcp::handshake::{HandshakeConfig, handshake};
use crate::tcp::protocol::ServerHello;
use crate::tcp::transport::{CONN_READ_BUFFER, CONN_WRITE_BUFFER, MaybeTlsStream};

/// Idle time before the first keepalive probe is sent. Matches the
/// rationale in the module docstring.
const TCP_KEEPALIVE_IDLE: Duration = Duration::from_secs(60);

/// Interval between successive keepalive probes once probing starts.
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(20);

/// Number of failed probes before the connection is declared dead.
const TCP_KEEPALIVE_RETRIES: u32 = 3;

/// Open a plain TCP connection to `addr`, configure `TCP_NODELAY` and
/// keepalive, and return it wrapped in [`MaybeTlsStream::Plain`]. The
/// TLS variant is wired by a later branch behind the
/// `native-tls-rustls` feature.
pub(crate) async fn connect_plain(addr: SocketAddr) -> Result<MaybeTlsStream> {
    let socket = TcpStream::connect(addr).await.map_err(Error::from)?;
    socket.set_nodelay(true).map_err(Error::from)?;

    // socket2 borrows the raw fd / SOCKET from the tokio socket; the
    // mirrored handle is dropped at end of scope without closing the
    // underlying descriptor. Same approach hyper-util uses internally.
    let sock_ref = socket2::SockRef::from(&socket);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(TCP_KEEPALIVE_IDLE)
        .with_interval(TCP_KEEPALIVE_INTERVAL)
        .with_retries(TCP_KEEPALIVE_RETRIES);
    sock_ref
        .set_tcp_keepalive(&keepalive)
        .map_err(Error::from)?;

    Ok(MaybeTlsStream::Plain(socket))
}

/// Split the stream into buffered read/write halves for the
/// connection actor's long-lived loop. 64 KiB caps match
/// [`CONN_READ_BUFFER`] / [`CONN_WRITE_BUFFER`].
///
/// Borrowed-half split via [`tokio::io::split`] is used here rather
/// than `TcpStream::into_split` because `MaybeTlsStream` is a wrapper
/// enum -- the TLS variant cannot expose an `into_split` of its own.
/// Borrowed halves block re-merge across moves, but the actor never
/// re-merges: it owns the halves for the lifetime of the connection
/// and drops them together.
pub(crate) fn split_buffered(
    stream: MaybeTlsStream,
) -> (
    BufReader<ReadHalf<MaybeTlsStream>>,
    BufWriter<WriteHalf<MaybeTlsStream>>,
) {
    let (r, w) = tokio::io::split(stream);
    (
        BufReader::with_capacity(CONN_READ_BUFFER, r),
        BufWriter::with_capacity(CONN_WRITE_BUFFER, w),
    )
}

/// Connect-side selector: plain TCP versus TLS.
///
/// Encodes which transport [`open_handshaken`] reaches for. This
/// branch ships only the `Plain` variant; the TLS variant (carrying
/// the SNI / hostname the server certificate is validated against) is
/// added by a later branch behind the `native-tls-rustls` feature, at
/// which point the match in [`open_handshaken`] gains a second arm.
///
/// Held as a separate value rather than a flag on [`HandshakeConfig`]
/// because the transport choice is a property of the connection, not
/// of the post-connect handshake exchange.
#[derive(Clone, Debug)]
pub enum ConnectKind {
    /// Plain TCP. No TLS upgrade.
    Plain,
}

/// Connect to `addr` and drive the handshake to completion. Returns
/// the ready-to-use stream plus the negotiated [`ServerHello`].
///
/// `kind` selects plain or TLS transport. The downstream handshake is
/// identical: `handshake()` is transport-agnostic, operating over
/// `AsyncRead + AsyncWrite` against the [`MaybeTlsStream`] adapter.
///
/// The handshake is half-duplex (send Hello -> flush -> recv
/// ServerHello -> send addendum -> flush), so it runs directly
/// against `&mut MaybeTlsStream` without splitting. The connection
/// actor that follows is the one that calls [`split_buffered`] for
/// its long-lived read/write loop.
///
/// Chunked-packet protocol mode is NOT negotiated this round -- the
/// unchunked protocol is advertised and the server falls back.
/// Adding chunked mode would extend the addendum exchange with two
/// extra strings; see [`crate::tcp::handshake::handshake`] for the
/// deferral note.
pub async fn open_handshaken(
    addr: SocketAddr,
    kind: &ConnectKind,
    cfg: &HandshakeConfig,
) -> Result<(MaybeTlsStream, ServerHello)> {
    let mut stream = match kind {
        ConnectKind::Plain => connect_plain(addr).await?,
    };
    let hello = handshake(&mut stream, cfg).await?;
    Ok((stream, hello))
}
