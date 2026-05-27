//! Client-side TCP packet encoders.
//!
//! Mirrors clickhouse-cpp-client `Client::Impl`:
//!
//! - [`send_hello`] -- `SendHello()` lines 1192-1205.
//! - [`send_addendum`] -- writes quota_key when the server advertises
//!   at least `DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM`.
//! - [`send_query`] -- `SendQuery()` lines ~970-1130, settings serialised
//!   as `(name, flags varint, value)` triples.
//! - [`send_data_block`] / [`send_empty_block`] -- `SendData()` 1172-1181
//!   plus `WriteBlock()` 1140-1170. The caller passes pre-encoded
//!   Native-format `column_bytes`; this layer is transport-only.
//! - [`send_cancel`] -- `SendCancel()` 1006-1009.
//! - [`send_ping`] -- `Ping()` 596-606.
//!
//! Wire primitives come from [`crate::native::io::ClickHouseWrite`]
//! (varint, length-prefixed string) and tokio's `AsyncWriteExt`
//! (fixed-width LE). No new io.rs in this module.

use tokio::io::AsyncWriteExt;

use crate::error::{Error, Result};
use crate::native::io::ClickHouseWrite;
use crate::tcp::client_info::ClientInfo;
use crate::tcp::protocol::{
    ClientPacketId, DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM,
    DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS, DBMS_MIN_REVISION_WITH_BLOCK_INFO,
    DBMS_MIN_REVISION_WITH_CLIENT_INFO, DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET,
    DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS,
    DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES, DBMS_TCP_PROTOCOL_VERSION, QueryProcessingStage,
};

/// Client name advertised in Hello and ClientInfo. cpp-client sends
/// "clickhouse-cpp"; this is the rust-client equivalent. The server
/// records it for `system.query_log` so a distinct value helps operators
/// distinguish rust-client traffic from other drivers.
pub(crate) const CLIENT_NAME: &str = "ClickHouse rust-client";

/// Client major version, as `&'static str` from Cargo's env. Parsed to
/// u64 at runtime in send_hello/ClientInfo via `.parse().unwrap_or(0)`;
/// a const-fn parse does not compile against `env!`. The fallback to 0
/// is a defensive default that never triggers for a well-formed crate
/// (CARGO_PKG_VERSION_MAJOR is always a decimal integer).
pub(crate) const CLIENT_VERSION_MAJOR_STR: &str = env!("CARGO_PKG_VERSION_MAJOR");
/// Client minor version. Same const-fn-parse caveat as
/// [`CLIENT_VERSION_MAJOR_STR`].
pub(crate) const CLIENT_VERSION_MINOR_STR: &str = env!("CARGO_PKG_VERSION_MINOR");

/// Revision this client advertises when no server-negotiated revision
/// is available yet (i.e. during Hello). After Handshake the server's
/// revision is used everywhere else.
pub(crate) const CLIENT_REVISION_FALLBACK: u64 = DBMS_TCP_PROTOCOL_VERSION;

#[inline]
fn client_version_major() -> u64 {
    CLIENT_VERSION_MAJOR_STR.parse().unwrap_or(0)
}

#[inline]
fn client_version_minor() -> u64 {
    CLIENT_VERSION_MINOR_STR.parse().unwrap_or(0)
}

/// Send the client Hello packet. Matches cpp-client `SendHello()`
/// lines 1192-1205.
pub(crate) async fn send_hello<W: ClickHouseWrite>(
    w: &mut W,
    database: &str,
    user: &str,
    password: &str,
) -> Result<()> {
    w.write_var_uint(ClientPacketId::Hello as u64).await?;
    w.write_string(CLIENT_NAME.as_bytes()).await?;
    w.write_var_uint(client_version_major()).await?;
    w.write_var_uint(client_version_minor()).await?;
    w.write_var_uint(DBMS_TCP_PROTOCOL_VERSION).await?;
    w.write_string(database.as_bytes()).await?;
    w.write_string(user.as_bytes()).await?;
    w.write_string(password.as_bytes()).await?;
    w.flush().await?;
    Ok(())
}

/// Send the post-Hello addendum. Currently only carries the quota_key
/// string, and only when the server advertises at least
/// `DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM` (54458). Older servers do
/// not expect any addendum bytes -- silently no-op so the caller can
/// invoke this unconditionally.
pub(crate) async fn send_addendum<W: ClickHouseWrite>(
    w: &mut W,
    server_revision: u64,
    quota_key: &str,
) -> Result<()> {
    if server_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM {
        w.write_string(quota_key.as_bytes()).await?;
        w.flush().await?;
    }
    Ok(())
}

/// Send a Query packet. Mirrors cpp-client `SendQuery()` lines ~970-1130
/// in field order. The caller has already negotiated `server_revision`
/// from the post-Hello handshake.
///
/// `extra_settings` is `(name, value)` pairs serialised with `flags = 0`
/// per cpp; the flag is reserved for server-side use (custom = 2 etc.)
/// and zero is the correct value for plain client-supplied settings.
/// Servers older than `DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS`
/// (54429) cannot accept string-serialised settings; this function
/// returns `Error::Other` rather than silently dropping them (matches
/// cpp `UnimplementedError`).
///
/// Setting names and values are emitted verbatim as length-prefixed
/// strings; the server consumes them as plain settings (no SQL parsing
/// of the value at this layer). Validating the contents -- rejecting
/// control bytes or unexpected values -- is the caller's
/// responsibility; this function transmits whatever bytes it is given.
pub(crate) async fn send_query<W: ClickHouseWrite>(
    w: &mut W,
    server_revision: u64,
    query_id: &str,
    query: &str,
    extra_settings: &[(String, String)],
    client_info: &ClientInfo,
) -> Result<()> {
    w.write_var_uint(ClientPacketId::Query as u64).await?;
    w.write_string(query_id.as_bytes()).await?;

    if server_revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO {
        client_info.write_to(w, server_revision).await?;
    }

    if server_revision >= DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS {
        for (name, value) in extra_settings {
            w.write_string(name.as_bytes()).await?;
            // flags = 0 for plain client-supplied settings; cpp uses
            // non-zero only for server-side custom settings.
            w.write_var_uint(0).await?;
            w.write_string(value.as_bytes()).await?;
        }
    } else if !extra_settings.is_empty() {
        return Err(Error::Other(
            "tcp: cannot send query settings to server older than 20.1.2.4 \
             (revision 54429); upgrade the server or drop the settings"
                .into(),
        ));
    }
    // Empty string marks end-of-settings, written unconditionally.
    w.write_string(b"").await?;

    if server_revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
        // Interserver secret is empty for non-distributed clients.
        w.write_string(b"").await?;
    }

    w.write_var_uint(QueryProcessingStage::Complete as u64)
        .await?;
    // Compression off; subsequent branches negotiate this via Client config.
    w.write_var_uint(0).await?;
    w.write_string(query.as_bytes()).await?;

    if server_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS {
        // No bound parameters supported at this layer yet; emit the
        // empty-string terminator cpp writes at lines 1124. Param
        // support lands later without changing this terminator's
        // placement.
        w.write_string(b"").await?;
    }

    w.flush().await?;
    Ok(())
}

/// Send a Data packet. Mirrors cpp `SendData()` (1172-1181) + `WriteBlock()`
/// (1140-1170). `column_bytes` is the pre-encoded Native-format payload
/// from [`crate::native::encode`] -- this layer does not re-encode.
///
/// `table_name` is "" for INSERT-into-default-target. cpp writes this
/// only when the server advertises at least
/// `DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES` (50264); a 25.x server is
/// always above this threshold.
pub(crate) async fn send_data_block<W: ClickHouseWrite>(
    w: &mut W,
    server_revision: u64,
    table_name: &str,
    column_bytes: &[u8],
    num_columns: u64,
    num_rows: u64,
) -> Result<()> {
    w.write_var_uint(ClientPacketId::Data as u64).await?;

    if server_revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES {
        w.write_string(table_name.as_bytes()).await?;
    }

    // Block info -- three (field_id, value) pairs + terminator per cpp
    // WriteBlock lines 1142-1148. bucket_num default is -1 from the cpp
    // BlockInfo struct (block.h line 9); a non-distributed client never
    // overrides this.
    if server_revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO {
        w.write_var_uint(1).await?;
        w.write_u8(0).await?; // is_overflows = false
        w.write_var_uint(2).await?;
        w.write_i32_le(-1).await?; // bucket_num = -1
        w.write_var_uint(0).await?; // terminator
    }

    w.write_var_uint(num_columns).await?;
    w.write_var_uint(num_rows).await?;
    w.write_all(column_bytes).await?;
    w.flush().await?;
    Ok(())
}

/// Send an empty Data block. The server uses an empty client-side Data
/// block as the end-of-input sentinel for INSERTs (cpp `FinalizeQuery()`
/// at line 1132-1138).
pub(crate) async fn send_empty_block<W: ClickHouseWrite>(
    w: &mut W,
    server_revision: u64,
) -> Result<()> {
    send_data_block(w, server_revision, "", &[], 0, 0).await
}

/// Send a Cancel packet (single varint, then flush). cpp `SendCancel()`
/// 1006-1009.
pub(crate) async fn send_cancel<W: ClickHouseWrite>(w: &mut W) -> Result<()> {
    w.write_var_uint(ClientPacketId::Cancel as u64).await?;
    w.flush().await?;
    Ok(())
}

/// Send a Ping packet (single varint, then flush). The server replies
/// with `ServerCodes::Pong` (4). cpp `Ping()` lines 596-606.
pub(crate) async fn send_ping<W: ClickHouseWrite>(w: &mut W) -> Result<()> {
    w.write_var_uint(ClientPacketId::Ping as u64).await?;
    w.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::io::ClickHouseRead;

    #[tokio::test]
    async fn send_ping_writes_single_byte_varint() {
        let mut buf = Vec::new();
        send_ping(&mut buf).await.unwrap();
        // ClientPacketId::Ping = 4, single-byte varint.
        assert_eq!(buf, vec![4]);
    }

    #[tokio::test]
    async fn send_cancel_writes_single_byte_varint() {
        let mut buf = Vec::new();
        send_cancel(&mut buf).await.unwrap();
        // ClientPacketId::Cancel = 3, single-byte varint.
        assert_eq!(buf, vec![3]);
    }

    #[tokio::test]
    async fn send_hello_byte_layout() {
        let mut buf = Vec::new();
        send_hello(&mut buf, "default", "user", "pw")
            .await
            .unwrap();
        // First byte is ClientPacketId::Hello (0).
        assert_eq!(buf[0], 0);
        let mut cur = std::io::Cursor::new(&buf[1..]);
        let name = cur.read_utf8_string().await.unwrap();
        assert_eq!(name, CLIENT_NAME);
        let major = cur.read_var_uint().await.unwrap();
        let minor = cur.read_var_uint().await.unwrap();
        let revision = cur.read_var_uint().await.unwrap();
        assert_eq!(major, client_version_major());
        assert_eq!(minor, client_version_minor());
        assert_eq!(revision, DBMS_TCP_PROTOCOL_VERSION);
        assert_eq!(cur.read_utf8_string().await.unwrap(), "default");
        assert_eq!(cur.read_utf8_string().await.unwrap(), "user");
        assert_eq!(cur.read_utf8_string().await.unwrap(), "pw");
    }

    #[tokio::test]
    async fn send_data_block_empty_layout() {
        let mut buf = Vec::new();
        send_empty_block(&mut buf, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();

        let mut cur = std::io::Cursor::new(&buf[..]);
        // Packet ID = Data (2).
        assert_eq!(cur.read_var_uint().await.unwrap(), ClientPacketId::Data as u64);
        // table_name = "" (revision is well above
        // DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES).
        assert_eq!(cur.read_utf8_string().await.unwrap(), "");
        // Block info field 1: id=1, u8 is_overflows=0.
        assert_eq!(cur.read_var_uint().await.unwrap(), 1);
        let mut b = [0u8; 1];
        tokio::io::AsyncReadExt::read_exact(&mut cur, &mut b)
            .await
            .unwrap();
        assert_eq!(b[0], 0);
        // Block info field 2: id=2, i32_le bucket_num=-1.
        assert_eq!(cur.read_var_uint().await.unwrap(), 2);
        let bucket = tokio::io::AsyncReadExt::read_i32_le(&mut cur)
            .await
            .unwrap();
        assert_eq!(bucket, -1);
        // Block info terminator: id=0.
        assert_eq!(cur.read_var_uint().await.unwrap(), 0);
        // num_columns = 0, num_rows = 0, then no column payload.
        assert_eq!(cur.read_var_uint().await.unwrap(), 0);
        assert_eq!(cur.read_var_uint().await.unwrap(), 0);
        // Whole buffer consumed.
        assert_eq!(cur.position() as usize, buf.len());
    }

    #[tokio::test]
    async fn send_query_rejects_old_revision() {
        let mut buf = Vec::new();
        let ci = ClientInfo::for_initial_query(
            CLIENT_NAME,
            client_version_major(),
            client_version_minor(),
            DBMS_TCP_PROTOCOL_VERSION,
            "",
        );
        // Pick a revision below DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS
        // (54429) but high enough that send_query reaches the settings
        // loop before failing.
        let old_revision = DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS - 1;
        let settings = vec![("max_block_size".to_string(), "1024".to_string())];
        let err = send_query(
            &mut buf,
            old_revision,
            "qid",
            "SELECT 1",
            &settings,
            &ci,
        )
        .await
        .expect_err("expected Error::Other for too-old server revision");
        match err {
            Error::Other(_) => {}
            other => panic!("expected Error::Other, got {other:?}"),
        }
    }
}
