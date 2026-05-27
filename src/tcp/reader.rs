//! Server-side TCP packet decoders.
//!
//! Mirrors clickhouse-cpp-client `Client::Impl::ReceivePacket()`
//! (lines 679-830), `ReceiveHello()` (1207-1253), and
//! `ReceiveException()` (955-1004). Field order and revision-gating
//! match cpp-client byte-for-byte; revision-constant values match
//! ClickHouse server `Core/ProtocolDefines.h`.
//!
//! Wire primitives (varint, length-prefixed string) come from
//! [`crate::native::io::ClickHouseRead`]; tokio's `AsyncReadExt`
//! supplies fixed-width LE reads on the same trait object. No new
//! io.rs in this module.
//!
//! Data packets surface header-only -- `(table_name, num_columns,
//! num_rows)`. The column-bytes payload is decoded by the cursor
//! that lands alongside the connection actor in a subsequent
//! branch, using [`crate::native::decode`]. Keeping the payload
//! out of this layer keeps the reader transport-only and avoids
//! buffering an entire Data block in memory before the cursor can
//! stream it.

use tokio::io::AsyncReadExt;

use crate::error::{Error, Result};
use crate::native::io::ClickHouseRead;
use crate::tcp::protocol::{
    DBMS_MIN_REVISION_WITH_BLOCK_INFO, DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO,
    DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME, DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE,
    DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES, DBMS_MIN_REVISION_WITH_VERSION_PATCH,
    DBMS_TCP_PROTOCOL_VERSION, Exception, ProfileInfo, Progress, ServerHello, ServerPacketId,
    TCP_EXCEPTION_STACK_TRACE_CAP, TableColumns,
};

/// Revision at which the server started emitting `total_rows_to_read`
/// inside the Progress packet. Value matches clickhouse-cpp-client
/// `client.cpp` line 25 (`#define DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS 51554`).
/// Introduced here because the reader is the first consumer; placing
/// it alongside the other revision constants in `protocol.rs` would
/// cascade a diff into 05b1 without need.
///
/// Note this gate is always satisfied at any revision this client
/// negotiates (the `CLIENT_INFO` floor is 54032, well above 51554), so
/// the field is in practice always present; modern servers write it
/// unconditionally. The gate is retained as documentation of the
/// historical introduction point and as defensive cover for the
/// theoretical sub-51554 server we never reach.
pub(crate) const DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS: u64 = 51554;

/// Decoded server-to-client packet.
///
/// `Data` carries header fields only; the column-bytes payload is
/// pulled from the underlying reader by the cursor + decoder layer
/// in a follow-up branch. `Log` and `ProfileEvents` discard their
/// block payloads at the reader -- the connection actor records
/// them via `tracing` but does not surface them to callers.
#[derive(Debug)]
pub(crate) enum ServerPacket {
    /// Data block header. Caller (cursor) reads `num_columns` columns
    /// of `num_rows` rows each from the same underlying reader.
    /// `table_name` is the empty string for default-target INSERTs;
    /// servers below `DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES` (50264)
    /// omit it on the wire (the field is `None` in that case).
    Data {
        table_name: Option<String>,
        num_columns: u64,
        num_rows: u64,
    },
    Exception(Exception),
    Progress(Progress),
    ProfileInfo(ProfileInfo),
    Pong,
    EndOfStream,
    /// Server-log packet. This client never requests log forwarding
    /// (`send_logs_level` is left unset), so `read_packet` rejects an
    /// unsolicited Log packet as a protocol error rather than
    /// producing this variant. Retained so the actor's match arms stay
    /// exhaustive and to reserve the shape for a future log-forwarding
    /// feature.
    Log,
    TableColumns(TableColumns),
    /// ProfileEvents payload is read and discarded at this layer.
    ProfileEvents,
    /// Timezone update string sent mid-stream when the server's
    /// session timezone changes.
    TimezoneUpdate(String),
}

/// Read the server Hello packet. Mirrors cpp `ReceiveHello()`
/// lines 1207-1253. If the server sent an Exception in place of
/// Hello (auth failure, server still starting, etc.), the wire
/// frame is read and flattened into [`Error::ServerException`]
/// via [`Exception::into_error`].
pub(crate) async fn read_hello<R: ClickHouseRead>(r: &mut R) -> Result<ServerHello> {
    let packet_type = r.read_var_uint().await?;
    let id = ServerPacketId::from_u64(packet_type)?;
    match id {
        ServerPacketId::Hello => {
            let server_name = r.read_utf8_string().await?;
            let major = r.read_var_uint().await?;
            let minor = r.read_var_uint().await?;
            let revision = r.read_var_uint().await?;
            // Field presence is governed by the NEGOTIATED (effective)
            // revision = min(what we advertised, what the server
            // reports). The server gates the fields it writes here on
            // OUR advertised revision (TCPHandler::sendHello), so the
            // client must read them on the same value -- not on the raw
            // server revision, which can be higher and would make us
            // expect fields the server did not send. `revision` is
            // stored raw on `ServerHello` for version reporting; gating
            // uses `effective`. (Today all handled fields sit below the
            // advertised pin, so the two agree; this is the correct,
            // future-bump-safe value.)
            let effective = revision.min(DBMS_TCP_PROTOCOL_VERSION);
            let timezone = if effective >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
                Some(r.read_utf8_string().await?)
            } else {
                None
            };
            let display_name = if effective >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
                Some(r.read_utf8_string().await?)
            } else {
                None
            };
            let patch = if effective >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
                r.read_var_uint().await?
            } else {
                0
            };
            Ok(ServerHello {
                server_name,
                version: (major, minor, patch),
                revision,
                timezone,
                display_name,
            })
        }
        ServerPacketId::Exception => {
            let exc = read_exception(r).await?;
            Err(exc.into_error())
        }
        other => Err(Error::BadResponse(format!(
            "tcp: unexpected packet during handshake: {other:?}"
        ))),
    }
}

/// Read a server Exception frame. Public entry into the recursive
/// inner reader. The recursive call site itself uses [`Box::pin`]
/// to break the async-fn-cycle's otherwise-infinite future size.
pub(crate) async fn read_exception<R: ClickHouseRead>(r: &mut R) -> Result<Exception> {
    read_exception_inner(r).await
}

/// Recursive inner reader. Each frame is: signed-LE i32 `code`,
/// length-prefixed `name`, `message`, `stack_trace`, one-byte
/// `has_nested` flag. The `stack_trace` field is truncated to
/// [`TCP_EXCEPTION_STACK_TRACE_CAP`] (1 MiB) at parse time;
/// truncation emits a `tracing::warn!` so the cap is visible.
/// This mirrors cpp `ReceiveException()` 955-1004 with the
/// added body cap.
async fn read_exception_inner<R: ClickHouseRead>(r: &mut R) -> Result<Exception> {
    // cpp reads the code as a fixed-width int32 (ReadFixed<int32_t>),
    // i.e. signed little-endian. Server-side codes are small positives
    // in practice but the wire is signed.
    let code = r.read_i32_le().await?;
    let name = r.read_utf8_string().await?;
    let message = r.read_utf8_string().await?;
    let mut stack_trace = r.read_utf8_string().await?;
    if stack_trace.len() > TCP_EXCEPTION_STACK_TRACE_CAP {
        tracing::warn!(
            truncated_from = stack_trace.len(),
            cap = TCP_EXCEPTION_STACK_TRACE_CAP,
            "tcp: server exception stack_trace truncated"
        );
        stack_trace.truncate(TCP_EXCEPTION_STACK_TRACE_CAP);
    }
    let has_nested = r.read_u8().await? != 0;
    let nested = if has_nested {
        // Box::pin at the recursive call site: rustc requires
        // boxed indirection for any async-fn cycle, not just at
        // the public entry. Without this, the future has an
        // infinitely sized type.
        Some(Box::new(Box::pin(read_exception_inner(r)).await?))
    } else {
        None
    };
    Ok(Exception {
        code,
        name,
        message,
        stack_trace,
        nested,
    })
}

/// Read a Progress packet. Field set widens with the negotiated
/// revision, matching cpp `case ServerCodes::Progress` 735-764:
///
/// - `rows_read`, `bytes_read` are always present.
/// - `total_rows_to_read` is sent at revisions >=
///   [`DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS`] (51554).
/// - `written_rows`, `written_bytes` are sent at revisions >=
///   [`DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO`] (54420).
///
/// Omitted fields read back as zero.
pub(crate) async fn read_progress<R: ClickHouseRead>(
    r: &mut R,
    server_revision: u64,
) -> Result<Progress> {
    let rows_read = r.read_var_uint().await?;
    let bytes_read = r.read_var_uint().await?;
    let total_rows_to_read = if server_revision >= DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS {
        r.read_var_uint().await?
    } else {
        0
    };
    let (written_rows, written_bytes) =
        if server_revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO {
            let wr = r.read_var_uint().await?;
            let wb = r.read_var_uint().await?;
            (wr, wb)
        } else {
            (0, 0)
        };
    Ok(Progress {
        rows_read,
        bytes_read,
        total_rows_to_read,
        written_rows,
        written_bytes,
    })
}

/// Read a ProfileInfo packet. Five varints followed by a one-byte
/// `applied_limit` flag. Mirrors cpp `case ServerCodes::ProfileInfo`
/// 706-732.
///
/// cpp reads the trailing `calculated_rows_before_limit` flag too;
/// it is not surfaced through this client's [`ProfileInfo`] struct
/// and is discarded after read so the stream pointer advances
/// correctly to the next packet.
pub(crate) async fn read_profile_info<R: ClickHouseRead>(r: &mut R) -> Result<ProfileInfo> {
    let rows = r.read_var_uint().await?;
    let blocks = r.read_var_uint().await?;
    let bytes = r.read_var_uint().await?;
    let applied_limit = r.read_u8().await? != 0;
    let rows_before_limit = r.read_var_uint().await?;
    // calculated_rows_before_limit -- discarded, see rustdoc above.
    let _ = r.read_u8().await?;
    Ok(ProfileInfo {
        rows,
        blocks,
        bytes,
        applied_limit,
        rows_before_limit,
    })
}

/// Read a TableColumns packet -- two length-prefixed strings:
/// the external-table name (empty for default INSERT target)
/// followed by the columns-definition DDL fragment.
pub(crate) async fn read_table_columns<R: ClickHouseRead>(r: &mut R) -> Result<TableColumns> {
    let external_table_name = r.read_utf8_string().await?;
    let columns_definition = r.read_utf8_string().await?;
    Ok(TableColumns {
        external_table_name,
        columns_definition,
    })
}

/// Read the Data block header -- `(table_name, num_columns,
/// num_rows)`. Mirrors cpp `SendData()` field order on the
/// client side (writer side at line 1172-1181 is the inverse)
/// and `ReadBlock()` for the block-info section (853-887).
///
/// The column-bytes payload is left in the reader; the cursor
/// + Native decoder in a later branch consumes it.
async fn read_data_block_header<R: ClickHouseRead>(
    r: &mut R,
    server_revision: u64,
) -> Result<(Option<String>, u64, u64)> {
    let table_name = if server_revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES {
        Some(r.read_utf8_string().await?)
    } else {
        None
    };

    // Block info -- (field_id varint, value) pairs + zero terminator.
    // cpp `ReadBlock` 854-877. At every revision this client
    // negotiates the server emits exactly field 1 (is_overflows, u8)
    // then field 2 (bucket_num, i32 LE) then the zero terminator;
    // field 3 (out_of_order_buckets) only appears at rev >= 54480,
    // above our 54459 pin. The values themselves carry no meaning for
    // a non-distributed client and are discarded -- but we assert the
    // field ids rather than accept any (varint, u8)(varint, i32)
    // (varint) shape, so a misaligned or hostile encoder surfaces as a
    // clean BadResponse instead of a silently mis-parsed block. A
    // future revision bump past 54480 must revisit this (and is a
    // full re-audit event per the protocol-version pin policy).
    if server_revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO {
        let field1 = r.read_var_uint().await?;
        if field1 != 1 {
            return Err(Error::BadResponse(format!(
                "tcp: block info field id {field1} (expected 1 = is_overflows)"
            )));
        }
        let _is_overflows = r.read_u8().await?;
        let field2 = r.read_var_uint().await?;
        if field2 != 2 {
            return Err(Error::BadResponse(format!(
                "tcp: block info field id {field2} (expected 2 = bucket_num)"
            )));
        }
        let _bucket_num = r.read_i32_le().await?;
        let terminator = r.read_var_uint().await?;
        if terminator != 0 {
            return Err(Error::BadResponse(format!(
                "tcp: block info terminator {terminator} (expected 0)"
            )));
        }
    }

    let num_columns = r.read_var_uint().await?;
    let num_rows = r.read_var_uint().await?;
    Ok((table_name, num_columns, num_rows))
}

/// Dispatch a single server packet. Reads the leading varint
/// packet ID and dispatches to the per-packet decoder. Unknown
/// IDs return [`Error::BadResponse`] via
/// [`ServerPacketId::from_u64`].
///
/// For `Data` packets the column-bytes payload is *not* consumed
/// here -- only the header is returned. The caller must consume
/// the payload from the same reader before requesting the next
/// packet or the stream pointer will misalign.
pub(crate) async fn read_packet<R: ClickHouseRead>(
    r: &mut R,
    server_revision: u64,
) -> Result<ServerPacket> {
    let packet_type = r.read_var_uint().await?;
    let id = ServerPacketId::from_u64(packet_type)?;
    match id {
        ServerPacketId::Data => {
            let (table_name, num_columns, num_rows) =
                read_data_block_header(r, server_revision).await?;
            Ok(ServerPacket::Data {
                table_name,
                num_columns,
                num_rows,
            })
        }
        ServerPacketId::Exception => Ok(ServerPacket::Exception(read_exception(r).await?)),
        ServerPacketId::Progress => Ok(ServerPacket::Progress(
            read_progress(r, server_revision).await?,
        )),
        ServerPacketId::ProfileInfo => Ok(ServerPacket::ProfileInfo(read_profile_info(r).await?)),
        ServerPacketId::Pong => Ok(ServerPacket::Pong),
        ServerPacketId::EndOfStream => Ok(ServerPacket::EndOfStream),
        ServerPacketId::Log => Err(Error::BadResponse(
            "tcp: server sent an unsolicited Log packet (this client does not \
             request server-log forwarding)"
                .into(),
        )),
        ServerPacketId::TableColumns => Ok(ServerPacket::TableColumns(
            read_table_columns(r).await?,
        )),
        ServerPacketId::ProfileEvents => Ok(ServerPacket::ProfileEvents),
        ServerPacketId::TimezoneUpdate => Ok(ServerPacket::TimezoneUpdate(
            r.read_utf8_string().await?,
        )),
        // Totals, Extremes, Hello mid-stream are all unexpected
        // after the handshake; surface as BadResponse rather than
        // silently advancing the stream pointer past unknown
        // payload bytes.
        other => Err(Error::BadResponse(format!(
            "tcp: unexpected server packet {other:?} mid-stream"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::io::ClickHouseWrite;
    use crate::tcp::protocol::DBMS_TCP_PROTOCOL_VERSION;
    use std::io::Cursor;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn hello_writer_reader_roundtrip() {
        let mut buf = Vec::new();
        buf.write_var_uint(ServerPacketId::Hello as u64).await.unwrap();
        buf.write_string("ClickHouse server".as_bytes()).await.unwrap();
        buf.write_var_uint(25).await.unwrap();
        buf.write_var_uint(4).await.unwrap();
        buf.write_var_uint(DBMS_TCP_PROTOCOL_VERSION).await.unwrap();
        // Revision is well above the timezone / display_name /
        // version_patch gates, so write all three.
        buf.write_string("Etc/UTC".as_bytes()).await.unwrap();
        buf.write_string("ch-01".as_bytes()).await.unwrap();
        buf.write_var_uint(7).await.unwrap();
        let mut cur = Cursor::new(buf);
        let hello = read_hello(&mut cur).await.unwrap();
        assert_eq!(hello.server_name, "ClickHouse server");
        assert_eq!(hello.version, (25, 4, 7));
        assert_eq!(hello.revision, DBMS_TCP_PROTOCOL_VERSION);
        assert_eq!(hello.timezone.as_deref(), Some("Etc/UTC"));
        assert_eq!(hello.display_name.as_deref(), Some("ch-01"));
    }

    #[tokio::test]
    async fn exception_truncates_stack_trace_at_cap() {
        let big = "x".repeat(TCP_EXCEPTION_STACK_TRACE_CAP + 1024);
        let mut buf = Vec::new();
        buf.write_i32_le(100i32).await.unwrap();
        buf.write_string("DB::Exception".as_bytes()).await.unwrap();
        buf.write_string("msg".as_bytes()).await.unwrap();
        buf.write_string(big.as_bytes()).await.unwrap();
        buf.write_u8(0).await.unwrap();
        let mut cur = Cursor::new(buf);
        let exc = read_exception(&mut cur).await.unwrap();
        assert_eq!(exc.code, 100);
        assert_eq!(exc.name, "DB::Exception");
        assert_eq!(exc.message, "msg");
        assert_eq!(exc.stack_trace.len(), TCP_EXCEPTION_STACK_TRACE_CAP);
        assert!(exc.nested.is_none());
    }

    #[tokio::test]
    async fn progress_revision_gating_legacy() {
        // Legacy: only rows_read + bytes_read on the wire.
        let mut buf = Vec::new();
        buf.write_var_uint(100).await.unwrap();
        buf.write_var_uint(2048).await.unwrap();
        let mut cur = Cursor::new(buf);
        // Revision below the total-rows gate.
        let legacy_revision = DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS - 1;
        let p = read_progress(&mut cur, legacy_revision).await.unwrap();
        assert_eq!(p.rows_read, 100);
        assert_eq!(p.bytes_read, 2048);
        assert_eq!(p.total_rows_to_read, 0);
        assert_eq!(p.written_rows, 0);
        assert_eq!(p.written_bytes, 0);
    }

    #[tokio::test]
    async fn progress_revision_gating_modern() {
        let mut buf = Vec::new();
        buf.write_var_uint(100).await.unwrap();
        buf.write_var_uint(2048).await.unwrap();
        buf.write_var_uint(10_000).await.unwrap();
        buf.write_var_uint(50).await.unwrap();
        buf.write_var_uint(1024).await.unwrap();
        let mut cur = Cursor::new(buf);
        let p = read_progress(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        assert_eq!(p.rows_read, 100);
        assert_eq!(p.bytes_read, 2048);
        assert_eq!(p.total_rows_to_read, 10_000);
        assert_eq!(p.written_rows, 50);
        assert_eq!(p.written_bytes, 1024);
    }

    #[tokio::test]
    async fn profile_info_roundtrip() {
        let mut buf = Vec::new();
        buf.write_var_uint(1000).await.unwrap(); // rows
        buf.write_var_uint(5).await.unwrap(); // blocks
        buf.write_var_uint(32768).await.unwrap(); // bytes
        buf.write_u8(1).await.unwrap(); // applied_limit = true
        buf.write_var_uint(500).await.unwrap(); // rows_before_limit
        buf.write_u8(0).await.unwrap(); // calculated_rows_before_limit (discarded)
        let mut cur = Cursor::new(buf);
        let pi = read_profile_info(&mut cur).await.unwrap();
        assert_eq!(pi.rows, 1000);
        assert_eq!(pi.blocks, 5);
        assert_eq!(pi.bytes, 32768);
        assert!(pi.applied_limit);
        assert_eq!(pi.rows_before_limit, 500);
    }

    #[tokio::test]
    async fn table_columns_roundtrip() {
        let mut buf = Vec::new();
        buf.write_string("".as_bytes()).await.unwrap();
        buf.write_string("columns format version: 1\n2 columns:\n`a` Int32\n`b` String\n".as_bytes())
            .await
            .unwrap();
        let mut cur = Cursor::new(buf);
        let tc = read_table_columns(&mut cur).await.unwrap();
        assert_eq!(tc.external_table_name, "");
        assert!(tc.columns_definition.starts_with("columns format version"));
    }

    #[tokio::test]
    async fn read_packet_dispatches_pong() {
        let mut buf = Vec::new();
        buf.write_var_uint(ServerPacketId::Pong as u64).await.unwrap();
        let mut cur = Cursor::new(buf);
        let pkt = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        assert!(matches!(pkt, ServerPacket::Pong));
    }

    #[tokio::test]
    async fn read_packet_dispatches_end_of_stream() {
        let mut buf = Vec::new();
        buf.write_var_uint(ServerPacketId::EndOfStream as u64)
            .await
            .unwrap();
        let mut cur = Cursor::new(buf);
        let pkt = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        assert!(matches!(pkt, ServerPacket::EndOfStream));
    }

    #[tokio::test]
    async fn read_packet_rejects_unsolicited_log() {
        // This client never requests server-log forwarding, so a Log
        // packet is a protocol surprise: surface it as BadResponse
        // rather than silently misaligning the stream.
        let mut buf = Vec::new();
        buf.write_var_uint(ServerPacketId::Log as u64).await.unwrap();
        let mut cur = Cursor::new(buf);
        let err = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap_err();
        match err {
            Error::BadResponse(msg) => assert!(msg.contains("Log packet"), "got: {msg}"),
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }
}
