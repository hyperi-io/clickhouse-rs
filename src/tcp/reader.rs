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
//! Data packets carry either a schema block (`num_rows == 0`) or a
//! payload block (`num_rows > 0`). Schema blocks surface their
//! `(name, type_name)` column pairs in
//! [`ServerPacket::Data::columns`]; payload blocks are fully decoded
//! inline through [`crate::native::decode::decode_block`] and surface
//! as [`ServerPacket::DataBlock`]. Decoding inline -- inside the
//! reader sub-task -- is the only way the actor can advance past a
//! payload block without misaligning the next packet's leading
//! varuint: the column bytes are byte-after-byte interleaved with the
//! block header, so an out-of-band consumer would race the next
//! `read_packet` call. The telemetry `ProfileEvents` packet carries a
//! leading string plus a Native block and is read-and-discarded inline
//! for the same reason: the server emits it during normal query
//! execution, so its bytes must be consumed to keep the stream
//! aligned. Compressed blocks remain a Phase 3.5 concern; v1
//! negotiates `NativeCompressionMethod::None`.

use tokio::io::AsyncReadExt;

use crate::error::{Error, Result};
use crate::native::decode::{DecodedBlock, decode_block};
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
/// For `Data` packets, the empty schema block (num_rows = 0) emitted
/// by the server at the start of an INSERT has its `(name, type_name)`
/// column metadata consumed off the wire and surfaced in `columns`.
/// For data blocks with `num_rows > 0` the column payload is left in
/// the reader; the cursor + Native decoder in Task 8 consumes it.
/// `Log` and `ProfileEvents` blocks are both read-and-discarded at the
/// reader (their bytes must be consumed to keep the stream aligned, but
/// their contents are not surfaced to callers in v1).
#[derive(Debug)]
pub(crate) enum ServerPacket {
    /// Schema block (Data packet with `num_rows == 0`).
    ///
    /// `table_name` is the empty string for default-target INSERTs;
    /// servers below `DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES` (50264)
    /// omit it on the wire (the field is `None` in that case).
    /// `columns` carries the `(name, type_name)` pairs the server
    /// echoed for the upcoming INSERT or SELECT stream. Payload
    /// blocks surface separately as [`ServerPacket::DataBlock`].
    Data {
        table_name: Option<String>,
        num_columns: u64,
        num_rows: u64,
        columns: Vec<(String, String)>,
    },
    /// Fully decoded payload block (`num_rows > 0`). The column-bytes
    /// payload was consumed inline by
    /// [`crate::native::decode::decode_block`]; downstream cursors
    /// iterate rows out of the [`DecodedBlock`] directly. Decoding
    /// inline is the only way to keep the reader's stream pointer
    /// aligned against the next packet.
    DataBlock(DecodedBlock),
    Exception(Exception),
    Progress(Progress),
    ProfileInfo(ProfileInfo),
    Pong,
    EndOfStream,
    /// Server-log packet (sent when the caller set `send_logs_level`,
    /// which is forwarded onto the TCP Query). Its leading tag string +
    /// Native block are read and discarded at this layer to keep the
    /// stream aligned; the log lines are not surfaced to callers in v1.
    Log,
    TableColumns(TableColumns),
    /// ProfileEvents telemetry. The packet's leading string + Native
    /// block are read and discarded at this layer to keep the stream
    /// aligned; the event values are not surfaced to callers.
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
///
/// For empty schema blocks (num_rows == 0) the body has no value
/// bytes after the (name, type_name) pairs, so callers can use
/// [`read_empty_data_block_schema`] to consume those without a
/// full decoder.
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

/// Read the body of an empty Data block (num_rows == 0) -- the
/// `num_columns` pairs of `(name, type_name)` strings the server
/// emits at the start of an INSERT, plus the optional custom-
/// serialization flag byte per column on revisions at and above
/// `DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION` (54454).
/// Mirrors the inverse shape that [`crate::native::encode_columns`]
/// produces.
///
/// This helper exists so the actor's reader sub-task can advance
/// past the schema block without a full Native decoder. The
/// streaming-block path with non-zero rows lands in Task 8's
/// cursor and decoder.
///
/// # Errors
///
/// I/O errors from the underlying reader. The `(name, type_name)`
/// strings inherit the `MAX_STRING_SIZE` cap from
/// [`ClickHouseRead::read_utf8_string`], so a corrupt or malicious
/// schema block cannot OOM the client.
pub(crate) async fn read_empty_data_block_schema<R: ClickHouseRead>(
    r: &mut R,
    num_columns: u64,
    server_revision: u64,
) -> Result<Vec<(String, String)>> {
    let has_custom_ser = server_revision
        >= crate::native::encode::DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION;
    let mut out = Vec::with_capacity(usize::try_from(num_columns).unwrap_or(0));
    for _ in 0..num_columns {
        let name = r.read_utf8_string().await?;
        let type_name = r.read_utf8_string().await?;
        if has_custom_ser {
            let _flag = r.read_u8().await?;
        }
        out.push((name, type_name));
    }
    Ok(out)
}

/// Read and discard a server telemetry block (`Log` / `ProfileEvents`).
///
/// Both packets are framed as one leading length-prefixed string (the
/// log tag / host name) followed by a Native block -- cpp-client's
/// `ReceivePacket` handles both with `SkipString` + `ReadBlock`, and
/// clickhouse-go likewise reads-and-drops them.
/// [`read_data_block_header`] consumes the leading string in its
/// table-name slot (valid because this client always negotiates a
/// revision at or above `DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES`
/// (50264), so the slot is read) plus the block header; the block body
/// is then consumed and its decoded values dropped. The bytes MUST be
/// read or the next packet's leading varuint misaligns. v1 does not
/// surface log lines or profile events to callers.
async fn consume_telemetry_block<R: ClickHouseRead>(
    r: &mut R,
    server_revision: u64,
) -> Result<()> {
    let (_tag, num_columns, num_rows) = read_data_block_header(r, server_revision).await?;
    if num_rows == 0 {
        let _ = read_empty_data_block_schema(r, num_columns, server_revision).await?;
    } else {
        let _ = decode_block(r, num_columns, num_rows, server_revision).await?;
    }
    Ok(())
}

/// Dispatch a single server packet. Reads the leading varint
/// packet ID and dispatches to the per-packet decoder. Unknown
/// IDs return [`Error::BadResponse`] via
/// [`ServerPacketId::from_u64`].
///
/// For `Data` packets with `num_rows == 0` (the INSERT schema
/// block) the body's `(name, type_name)` pairs are consumed via
/// [`read_empty_data_block_schema`] and exposed in
/// `ServerPacket::Data::columns`. For `num_rows > 0` the column-
/// bytes payload is consumed inline via
/// [`crate::native::decode::decode_block`] and surfaced as
/// [`ServerPacket::DataBlock`].
pub(crate) async fn read_packet<R: ClickHouseRead>(
    r: &mut R,
    server_revision: u64,
) -> Result<ServerPacket> {
    let packet_type = r.read_var_uint().await?;
    let id = ServerPacketId::from_u64(packet_type)?;
    match id {
        // Totals (WITH TOTALS) and Extremes (extremes=1) are Native
        // result blocks framed identically to Data (cpp `ReceivePacket`
        // + clickhouse-go decode them the same way). Decode them through
        // the Data path so a `WITH TOTALS` / `extremes=1` query does not
        // poison the connection; they flow to the cursor as data blocks.
        ServerPacketId::Data | ServerPacketId::Totals | ServerPacketId::Extremes => {
            let (table_name, num_columns, num_rows) =
                read_data_block_header(r, server_revision).await?;
            if num_rows == 0 {
                let columns =
                    read_empty_data_block_schema(r, num_columns, server_revision).await?;
                Ok(ServerPacket::Data {
                    table_name,
                    num_columns,
                    num_rows,
                    columns,
                })
            } else {
                let block = decode_block(r, num_columns, num_rows, server_revision).await?;
                Ok(ServerPacket::DataBlock(block))
            }
        }
        ServerPacketId::Exception => Ok(ServerPacket::Exception(read_exception(r).await?)),
        ServerPacketId::Progress => Ok(ServerPacket::Progress(
            read_progress(r, server_revision).await?,
        )),
        ServerPacketId::ProfileInfo => Ok(ServerPacket::ProfileInfo(read_profile_info(r).await?)),
        ServerPacketId::Pong => Ok(ServerPacket::Pong),
        ServerPacketId::EndOfStream => Ok(ServerPacket::EndOfStream),
        ServerPacketId::Log => {
            // The server sends Log packets when the caller requested
            // them via the `send_logs_level` setting (which apps may set
            // globally and which is forwarded onto the TCP Query). Both
            // upstream clients consume the block; rejecting it would
            // poison every query under such a setting. Consume + drop;
            // surfacing log lines to callers is a follow-up.
            consume_telemetry_block(r, server_revision).await?;
            Ok(ServerPacket::Log)
        }
        ServerPacketId::TableColumns => Ok(ServerPacket::TableColumns(
            read_table_columns(r).await?,
        )),
        ServerPacketId::ProfileEvents => {
            // Sent during normal query execution (rev >= 54451, always
            // negotiated). Same string + Native block framing as Log.
            consume_telemetry_block(r, server_revision).await?;
            Ok(ServerPacket::ProfileEvents)
        }
        ServerPacketId::TimezoneUpdate => Ok(ServerPacket::TimezoneUpdate(
            r.read_utf8_string().await?,
        )),
        // A mid-stream Hello (or any other unexpected id) is a protocol
        // surprise; surface as BadResponse rather than silently
        // advancing the stream pointer past unknown payload bytes.
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
    async fn read_packet_consumes_empty_data_block_schema() {
        // The server's INSERT schema block: Data packet, table_name = "",
        // block-info, num_columns = 2, num_rows = 0, then per-column
        // (name, type_name, custom_ser_flag).
        let mut buf = Vec::new();
        buf.write_var_uint(ServerPacketId::Data as u64).await.unwrap();
        buf.write_string(b"").await.unwrap(); // table_name
        // Block info -- mirror the writer side (field_id, value) pairs + terminator.
        buf.write_var_uint(1).await.unwrap();
        buf.write_u8(0).await.unwrap();
        buf.write_var_uint(2).await.unwrap();
        buf.write_i32_le(-1).await.unwrap();
        buf.write_var_uint(0).await.unwrap();
        buf.write_var_uint(2).await.unwrap(); // num_columns
        buf.write_var_uint(0).await.unwrap(); // num_rows
        // Column 1.
        buf.write_string(b"n").await.unwrap();
        buf.write_string(b"UInt64").await.unwrap();
        buf.write_u8(0).await.unwrap(); // custom-serialization flag
        // Column 2.
        buf.write_string(b"s").await.unwrap();
        buf.write_string(b"String").await.unwrap();
        buf.write_u8(0).await.unwrap();
        // Trailing sentinel so an over-read would show up as misalignment.
        buf.write_var_uint(ServerPacketId::EndOfStream as u64)
            .await
            .unwrap();

        let mut cur = Cursor::new(buf);
        let pkt = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        match pkt {
            ServerPacket::Data {
                table_name,
                num_columns,
                num_rows,
                columns,
            } => {
                assert_eq!(table_name.as_deref(), Some(""));
                assert_eq!(num_columns, 2);
                assert_eq!(num_rows, 0);
                assert_eq!(
                    columns,
                    vec![
                        ("n".to_string(), "UInt64".to_string()),
                        ("s".to_string(), "String".to_string()),
                    ]
                );
            }
            other => panic!("expected Data, got {other:?}"),
        }
        // The trailing EndOfStream must still be readable -- proves the
        // schema-block consume left the stream pointer aligned.
        let trailing = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        assert!(matches!(trailing, ServerPacket::EndOfStream));
    }

    #[tokio::test]
    async fn read_packet_consumes_profile_events_block() {
        // ProfileEvents framing: packet id, a leading host/tag string,
        // then a Native block (block-info, num_columns, num_rows,
        // per-column name/type/flag/data). The reader must consume the
        // whole thing so the next packet stays aligned. One UInt64
        // column, one row, value 42.
        let mut buf = Vec::new();
        buf.write_var_uint(ServerPacketId::ProfileEvents as u64)
            .await
            .unwrap();
        buf.write_string(b"host-01").await.unwrap(); // leading tag
        // Block info.
        buf.write_var_uint(1).await.unwrap();
        buf.write_u8(0).await.unwrap();
        buf.write_var_uint(2).await.unwrap();
        buf.write_i32_le(-1).await.unwrap();
        buf.write_var_uint(0).await.unwrap();
        buf.write_var_uint(1).await.unwrap(); // num_columns
        buf.write_var_uint(1).await.unwrap(); // num_rows
        buf.write_string(b"value").await.unwrap(); // col name
        buf.write_string(b"UInt64").await.unwrap(); // type
        buf.write_u8(0).await.unwrap(); // custom-serialization flag
        buf.write_u64_le(42).await.unwrap(); // the one row's value
        // Trailing sentinel.
        buf.write_var_uint(ServerPacketId::EndOfStream as u64)
            .await
            .unwrap();

        let mut cur = Cursor::new(buf);
        let pkt = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        assert!(matches!(pkt, ServerPacket::ProfileEvents));
        // The block bytes were consumed: the trailing EndOfStream reads
        // cleanly. Before the fix this misaligned on the first
        // ProfileEvents packet of any live query.
        let trailing = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        assert!(matches!(trailing, ServerPacket::EndOfStream));
    }

    #[tokio::test]
    async fn read_packet_consumes_log_block() {
        // A Log packet is framed exactly like ProfileEvents (leading tag
        // string + Native block) and must be consumed so the stream
        // stays aligned -- an app that set send_logs_level would
        // otherwise poison every TCP query. One String column, one row.
        let mut buf = Vec::new();
        buf.write_var_uint(ServerPacketId::Log as u64).await.unwrap();
        buf.write_string(b"log-tag").await.unwrap(); // leading tag
        // Block info.
        buf.write_var_uint(1).await.unwrap();
        buf.write_u8(0).await.unwrap();
        buf.write_var_uint(2).await.unwrap();
        buf.write_i32_le(-1).await.unwrap();
        buf.write_var_uint(0).await.unwrap();
        buf.write_var_uint(1).await.unwrap(); // num_columns
        buf.write_var_uint(1).await.unwrap(); // num_rows
        buf.write_string(b"text").await.unwrap(); // col name
        buf.write_string(b"String").await.unwrap(); // type
        buf.write_u8(0).await.unwrap(); // custom-serialization flag
        buf.write_string(b"hello from server").await.unwrap(); // the row value
        // Trailing sentinel proves the block was fully consumed.
        buf.write_var_uint(ServerPacketId::EndOfStream as u64)
            .await
            .unwrap();

        let mut cur = Cursor::new(buf);
        let pkt = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        assert!(matches!(pkt, ServerPacket::Log));
        let trailing = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        assert!(matches!(trailing, ServerPacket::EndOfStream));
    }

    #[tokio::test]
    async fn read_packet_decodes_totals_block_then_eos() {
        // A `WITH TOTALS` query emits a Totals packet (id 7) framed
        // exactly like Data: table_name, block-info, num_columns,
        // num_rows, then the column payload. The reader must decode it
        // (as a DataBlock) so the query does not poison the connection,
        // and leave the stream aligned for the trailing EndOfStream.
        // One UInt64 column, one totals row = 99.
        let mut buf = Vec::new();
        buf.write_var_uint(ServerPacketId::Totals as u64).await.unwrap();
        buf.write_string(b"").await.unwrap(); // table_name
        buf.write_var_uint(1).await.unwrap(); // block-info field 1
        buf.write_u8(0).await.unwrap();
        buf.write_var_uint(2).await.unwrap(); // block-info field 2
        buf.write_i32_le(-1).await.unwrap();
        buf.write_var_uint(0).await.unwrap(); // terminator
        buf.write_var_uint(1).await.unwrap(); // num_columns
        buf.write_var_uint(1).await.unwrap(); // num_rows
        buf.write_string(b"total").await.unwrap(); // col name
        buf.write_string(b"UInt64").await.unwrap(); // type
        buf.write_u8(0).await.unwrap(); // custom-serialization flag
        buf.write_u64_le(99).await.unwrap(); // the totals row
        buf.write_var_uint(ServerPacketId::EndOfStream as u64)
            .await
            .unwrap();

        let mut cur = Cursor::new(buf);
        let pkt = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        match pkt {
            ServerPacket::DataBlock(block) => {
                assert_eq!(block.num_rows, 1);
                match &block.columns[0] {
                    crate::native::decode::DecodedColumn::UInt64(v) => assert_eq!(v, &vec![99u64]),
                    other => panic!("expected UInt64 totals, got {other:?}"),
                }
            }
            other => panic!("expected DataBlock for Totals, got {other:?}"),
        }
        let trailing = read_packet(&mut cur, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        assert!(matches!(trailing, ServerPacket::EndOfStream));
    }
}
