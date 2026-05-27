//! TCP connection setup for the ClickHouse native transport.
//!
//! [`connect_plain`] opens a [`TcpStream`] to the supplied address,
//! enables `TCP_NODELAY`, and configures TCP keepalive via
//! [`socket2::SockRef`]. Defaults are 60s idle / 20s interval / 3
//! retries -- chosen to survive Kubernetes kube-proxy iptables
//! connection-tracking idle timeouts (~30-60s) without producing
//! excessive probe traffic.
//!
//! [`connect_tls`] (under the `native-tls-rustls` feature) mirrors
//! the same socket-level setup, then drives the rustls handshake
//! against the supplied SNI string before returning a
//! [`MaybeTlsStream::Tls`].
//!
//! [`open_handshaken`] is the high-level entry point the connection
//! pool will call: connect, then drive [`crate::tcp::handshake`]
//! against the unsplit stream, returning the ready-to-use stream and
//! the negotiated [`ServerHello`]. The plain / TLS choice is encoded
//! in [`ConnectKind`].
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

/// Open a raw [`TcpStream`] to `addr`, then apply the
/// `TCP_NODELAY` + keepalive socket-level setup shared between the
/// plain and TLS paths.
///
/// Factored out so [`connect_plain`] and [`connect_tls`] cannot drift
/// on socket-level dials: both call this, both inherit the same
/// keepalive cadence.
async fn connect_socket(addr: SocketAddr) -> Result<TcpStream> {
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

    Ok(socket)
}

/// Open a plain TCP connection to `addr`, configure `TCP_NODELAY` and
/// keepalive, and return it wrapped in [`MaybeTlsStream::Plain`]. The
/// TLS variant is [`connect_tls`], gated on the `native-tls-rustls`
/// feature.
pub(crate) async fn connect_plain(addr: SocketAddr) -> Result<MaybeTlsStream> {
    let socket = connect_socket(addr).await?;
    Ok(MaybeTlsStream::Plain(socket))
}

/// Open a TCP connection, then upgrade it to TLS with rustls and
/// return it wrapped in [`MaybeTlsStream::Tls`].
///
/// `server_name` is the SNI / hostname the server certificate will be
/// validated against. It is independent of the address `addr` so a
/// caller can connect to an IP literal while presenting a
/// hostname-based SNI -- the common pattern when the resolver runs
/// outside the rust client (e.g. a service mesh).
///
/// `config` is the resolved [`rustls::ClientConfig`] -- the SAME shared
/// trust the HTTP transport uses. The pool builds it once (from the
/// `Client`'s `with_tls_*` trust, or the default native+webpki anchors)
/// and clones the `Arc` into each connect, so swapping default trust
/// for a private CA is a `Client` builder call, not a change here. See
/// [`crate::tls`] for the trust-resolution rules.
#[cfg(feature = "native-tls-rustls")]
pub(crate) async fn connect_tls(
    addr: SocketAddr,
    server_name: &str,
    config: std::sync::Arc<tokio_rustls::rustls::ClientConfig>,
) -> Result<MaybeTlsStream> {
    use tokio_rustls::TlsConnector;

    let connector = TlsConnector::from(config);

    let socket = connect_socket(addr).await?;

    // `ServerName::try_from` accepts both DNS names and IP literals;
    // an invalid SNI shape (empty string, illegal characters, etc.)
    // surfaces as a typed Custom error rather than a panic.
    let sni = rustls_pki_types::ServerName::try_from(server_name.to_string())
        .map_err(|e| Error::Custom(format!("tcp: invalid SNI {server_name:?}: {e}")))?;
    let tls = connector
        .connect(sni, socket)
        .await
        .map_err(Error::from)?;
    Ok(MaybeTlsStream::Tls(Box::new(tls)))
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
/// Encodes which transport [`open_handshaken`] reaches for. The TLS
/// variant carries the SNI / hostname the server certificate will be
/// validated against. Held as a separate value rather than a flag on
/// [`HandshakeConfig`] because the SNI is a property of the
/// connection, not of the post-connect handshake exchange.
///
/// The TLS variant is feature-gated on `native-tls-rustls`; without
/// that feature only `Plain` exists and the match in
/// [`open_handshaken`] is exhaustive on `Plain` alone.
#[derive(Clone, Debug)]
pub enum ConnectKind {
    /// Plain TCP. No TLS upgrade.
    Plain,
    /// TLS over TCP. `server_name` is the SNI sent in the ClientHello
    /// and the name the server certificate is validated against;
    /// `config` is the resolved rustls trust the pool built once and
    /// shares (cloned `Arc`) across reconnects.
    #[cfg(feature = "native-tls-rustls")]
    Tls {
        server_name: String,
        config: std::sync::Arc<tokio_rustls::rustls::ClientConfig>,
    },
    /// A TLS trust was configured but could not be resolved. The pool
    /// still builds (so we never silently downgrade the transport or
    /// broaden trust), but every connection attempt fails closed here.
    #[cfg(feature = "native-tls-rustls")]
    TlsFailClosed,
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
        #[cfg(feature = "native-tls-rustls")]
        ConnectKind::Tls {
            server_name,
            config,
        } => connect_tls(addr, server_name, config.clone()).await?,
        #[cfg(feature = "native-tls-rustls")]
        ConnectKind::TlsFailClosed => {
            return Err(Error::Custom(
                "tcp: TLS trust was configured but could not be resolved; \
                 refusing to connect with default trust"
                    .into(),
            ));
        }
    };
    let hello = handshake(&mut stream, cfg).await?;
    Ok((stream, hello))
}
