//! Stream abstraction over plain TCP or TLS.
//!
//! Same pattern as `hyper-rustls::MaybeHttpsStream` and
//! `tungstenite::MaybeTlsStream`: a single concrete type implementing
//! `AsyncRead + AsyncWrite + Unpin` so connection actors can call
//! `tokio::io::split()` regardless of whether TLS is in use.
//!
//! The `Tls` variant is feature-gated behind `native-tls-rustls`;
//! without that feature it does not exist at compile time and pattern
//! matching is exhaustive on `Plain` alone. The TLS connector itself
//! is wired by the `06d-tcp-tls` branch.

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// Per-connection buffered read capacity. 64 KiB matches `BufReader`
/// default and is well under typical Native-format Data block sizes.
pub(crate) const CONN_READ_BUFFER: usize = 64 * 1024;

/// Per-connection buffered write capacity. Symmetric with
/// [`CONN_READ_BUFFER`].
pub(crate) const CONN_WRITE_BUFFER: usize = 64 * 1024;

/// Plain-or-TLS adapter. The `Tls` variant boxes the TLS stream both
/// because `TlsStream` is large and because it sits behind a feature
/// gate; boxing keeps the unboxed `Plain` variant cheap.
#[allow(clippy::large_enum_variant)] // Tls variant only exists with feature; boxed anyway.
pub(crate) enum MaybeTlsStream {
    Plain(TcpStream),
    #[cfg(feature = "native-tls-rustls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "native-tls-rustls")]
            MaybeTlsStream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "native-tls-rustls")]
            MaybeTlsStream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "native-tls-rustls")]
            MaybeTlsStream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "native-tls-rustls")]
            MaybeTlsStream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}
