//! TCP handshake orchestration.
//!
//! Drives the post-connect, pre-query exchange:
//!
//! 1. Client Hello -- `writer::send_hello` (cpp-client `SendHello()`
//!    lines 1192-1205).
//! 2. Server Hello -- `reader::read_hello` (cpp-client
//!    `ReceiveHello()` 1207-1253).
//! 3. Post-Hello addendum -- `writer::send_addendum` (cpp-client
//!    handshake addendum write at lines 672-674), conditional on the
//!    negotiated server revision.
//!
//! Returns the [`ServerHello`] so the caller can pin its connection
//! state to the negotiated revision for every subsequent packet.
//!
//! ## Chunked-packet protocol -- deferred
//!
//! ClickHouse 24.x added an OPTIONAL chunked-packet protocol mode
//! gated on `DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS`
//! (54470). When both peers advertise that revision, the addendum
//! exchange grows two extra strings (send-direction mode and
//! recv-direction mode, each one of `"notchunked"`,
//! `"notchunked_optional"`, `"chunked"`, `"chunked_optional"`).
//!
//! This branch does NOT implement chunked mode. The unchunked
//! protocol is implicitly advertised by omitting the extra strings,
//! and the server falls back. A later branch can extend
//! [`handshake`] with chunked-mode negotiation without changing the
//! function's external signature.

use crate::error::Result;
use crate::native::io::{ClickHouseRead, ClickHouseWrite};
use crate::tcp::protocol::ServerHello;
use crate::tcp::{reader, writer};

/// Handshake parameters supplied by the caller. Mirrors the inputs
/// `clickhouse-cpp-client` reads from its `ClientOptions` at
/// handshake time: `default_database`, `user`, `password`, and the
/// quota-key string emitted in the post-Hello addendum.
///
/// The defaults match server defaults: empty quota-key, `"default"`
/// database, `"default"` user, empty password. A caller that wants
/// anonymous access against a freshly installed ClickHouse server
/// can use [`HandshakeConfig::default`] unchanged.
#[derive(Debug, Clone)]
pub struct HandshakeConfig {
    pub database: String,
    pub user: String,
    pub password: String,
    pub quota_key: String,
}

impl Default for HandshakeConfig {
    fn default() -> Self {
        Self {
            database: "default".to_string(),
            user: "default".to_string(),
            password: String::new(),
            quota_key: String::new(),
        }
    }
}

/// Drive the TCP handshake on `stream`, returning the negotiated
/// [`ServerHello`].
///
/// `stream` must satisfy both [`ClickHouseRead`] and
/// [`ClickHouseWrite`]; in practice the actor passes an unsplit
/// [`crate::tcp::transport::MaybeTlsStream`] (which has blanket
/// impls of both traits via the standard
/// `AsyncRead + AsyncWrite + Unpin + Send + Sync` bound set).
/// The handshake is half-duplex -- each step flushes before the
/// next read or write -- so a single `&mut` reference to the
/// unsplit stream is sufficient.
///
/// On a server error (auth failure, server still starting, etc.)
/// the server replies with an Exception packet in place of Hello.
/// [`reader::read_hello`] flattens that into an
/// [`crate::error::Error::ServerException`] before this function
/// returns.
///
/// See the module docstring for the chunked-packet protocol
/// deferral note.
pub(crate) async fn handshake<S>(stream: &mut S, cfg: &HandshakeConfig) -> Result<ServerHello>
where
    S: ClickHouseRead + ClickHouseWrite,
{
    writer::send_hello(stream, &cfg.database, &cfg.user, &cfg.password).await?;
    let hello = reader::read_hello(stream).await?;
    writer::send_addendum(stream, hello.revision, &cfg.quota_key).await?;
    Ok(hello)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tcp::protocol::{
        DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM, DBMS_TCP_PROTOCOL_VERSION, ServerPacketId,
    };
    use tokio::io::{AsyncWriteExt, duplex};

    #[test]
    fn default_handshake_config_uses_server_defaults() {
        let cfg = HandshakeConfig::default();
        assert_eq!(cfg.database, "default");
        assert_eq!(cfg.user, "default");
        assert!(cfg.password.is_empty());
        assert!(cfg.quota_key.is_empty());
    }

    /// End-to-end check on an in-memory duplex pair: handshake drives
    /// send_hello -> read_hello -> send_addendum and returns the
    /// parsed ServerHello. The "server" side of the duplex pre-seeds
    /// a Hello reply, then asserts the client's Hello + addendum
    /// bytes arrive in order.
    #[tokio::test]
    async fn handshake_roundtrip_on_duplex() {
        // 4 KiB duplex is plenty for the handshake -- Hello is <100 B
        // and the addendum is a single short string.
        let (mut client_side, mut server_side) = duplex(4096);

        // Seed the server-side reader with the ServerHello bytes
        // BEFORE driving the client handshake. The duplex pair is
        // a bounded ring; writing here only blocks if the ring
        // fills, which it does not for ~64 B of Hello payload.
        server_side
            .write_var_uint(ServerPacketId::Hello as u64)
            .await
            .unwrap();
        server_side
            .write_string("ClickHouse test".as_bytes())
            .await
            .unwrap();
        server_side.write_var_uint(25).await.unwrap();
        server_side.write_var_uint(4).await.unwrap();
        server_side
            .write_var_uint(DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        // Revision is well above timezone / display_name /
        // version_patch gates, so write all three.
        server_side.write_string("Etc/UTC".as_bytes()).await.unwrap();
        server_side
            .write_string("ch-test".as_bytes())
            .await
            .unwrap();
        server_side.write_var_uint(7).await.unwrap();
        server_side.flush().await.unwrap();

        let cfg = HandshakeConfig {
            database: "default".into(),
            user: "user".into(),
            password: "pw".into(),
            quota_key: "qk".into(),
        };
        let hello = handshake(&mut client_side, &cfg).await.unwrap();
        assert_eq!(hello.server_name, "ClickHouse test");
        assert_eq!(hello.revision, DBMS_TCP_PROTOCOL_VERSION);
        assert!(hello.revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM);

        // Drop the client side so the server-side reader hits EOF
        // after consuming the client Hello + addendum bytes.
        drop(client_side);

        // Verify the client's Hello packet ID arrived first on the
        // server-side reader.
        let packet_id = server_side.read_var_uint().await.unwrap();
        assert_eq!(packet_id, 0); // ClientPacketId::Hello
        // Then the client name, version major/minor, advertised
        // revision, database, user, password.
        assert_eq!(
            server_side.read_utf8_string().await.unwrap(),
            crate::tcp::writer::CLIENT_NAME
        );
        let _major = server_side.read_var_uint().await.unwrap();
        let _minor = server_side.read_var_uint().await.unwrap();
        let _rev = server_side.read_var_uint().await.unwrap();
        assert_eq!(server_side.read_utf8_string().await.unwrap(), "default");
        assert_eq!(server_side.read_utf8_string().await.unwrap(), "user");
        assert_eq!(server_side.read_utf8_string().await.unwrap(), "pw");
        // Addendum: quota_key (server revision >= addendum gate).
        assert_eq!(server_side.read_utf8_string().await.unwrap(), "qk");
    }

    /// When the negotiated revision is below
    /// `DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM`, the addendum is
    /// silently suppressed -- the server does not expect any
    /// addendum bytes from that vintage.
    #[tokio::test]
    async fn handshake_suppresses_addendum_below_gate() {
        let (mut client_side, mut server_side) = duplex(4096);

        // Pre-seed a Hello whose revision is below the addendum
        // gate. Server name + zero patch are all we need.
        let legacy_revision = DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM - 1;
        server_side
            .write_var_uint(ServerPacketId::Hello as u64)
            .await
            .unwrap();
        server_side
            .write_string("legacy".as_bytes())
            .await
            .unwrap();
        server_side.write_var_uint(24).await.unwrap();
        server_side.write_var_uint(0).await.unwrap();
        server_side.write_var_uint(legacy_revision).await.unwrap();
        server_side.write_string("UTC".as_bytes()).await.unwrap();
        server_side.write_string("legacy-ch".as_bytes()).await.unwrap();
        server_side.write_var_uint(0).await.unwrap();
        server_side.flush().await.unwrap();

        let cfg = HandshakeConfig {
            database: "default".into(),
            user: "user".into(),
            password: String::new(),
            quota_key: "should-not-be-sent".into(),
        };
        let hello = handshake(&mut client_side, &cfg).await.unwrap();
        assert_eq!(hello.revision, legacy_revision);

        drop(client_side);

        // Consume the client Hello so we land on the addendum
        // position in the server-side stream.
        let _ = server_side.read_var_uint().await.unwrap(); // packet id
        let _ = server_side.read_utf8_string().await.unwrap(); // client name
        let _ = server_side.read_var_uint().await.unwrap(); // major
        let _ = server_side.read_var_uint().await.unwrap(); // minor
        let _ = server_side.read_var_uint().await.unwrap(); // revision
        let _ = server_side.read_utf8_string().await.unwrap(); // database
        let _ = server_side.read_utf8_string().await.unwrap(); // user
        let _ = server_side.read_utf8_string().await.unwrap(); // password

        // No addendum bytes should follow -- the next read hits
        // EOF on the closed client side.
        let next = server_side.read_var_uint().await;
        assert!(
            next.is_err(),
            "expected EOF after Hello when server revision is below addendum gate"
        );
    }
}
