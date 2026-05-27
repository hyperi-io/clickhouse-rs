//! ClickHouse TCP transport (port 9000) -- native binary protocol.
//!
//! Packet IDs, handshake structures, revision-gated feature constants.
//! The protocol is wire-revision-negotiated: client advertises
//! [`DBMS_TCP_PROTOCOL_VERSION`] in Hello, server replies with its own
//! revision, and the lower of the two governs every revision-gated
//! field thereafter.
//!
//! Bridge for ClickHouse-docs readers: ClickHouse's own docs call this
//! the "native protocol". Our codebase reserves "native" for the
//! columnar payload format (see [`crate::native`]). "TCP transport"
//! here means the wire protocol on port 9000.
//!
//! Wire primitives (varint, length-prefixed strings, fixed-width LE)
//! come from [`crate::native::io`]; this module is types-only.

use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// Revision constants
//
// Each constant gates a wire-format change. The two endpoints negotiate
// to `min(client_advertised, server_advertised)` after Hello; every
// field added at revision R is only on-wire when both peers advertise
// at least R. Values match ClickHouse server `Core/ProtocolDefines.h`
// and clickhouse-cpp-client `protocol.h` (mainline, commit e903492).
// ---------------------------------------------------------------------------

pub(crate) const DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES: u64 = 50264;
pub(crate) const DBMS_MIN_REVISION_WITH_BLOCK_INFO: u64 = 51903;
pub(crate) const DBMS_MIN_REVISION_WITH_CLIENT_INFO: u64 = 54032;
pub(crate) const DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE: u64 = 54058;
pub(crate) const DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO: u64 = 54060;
pub(crate) const DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME: u64 = 54372;
pub(crate) const DBMS_MIN_REVISION_WITH_VERSION_PATCH: u64 = 54401;
pub(crate) const DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO: u64 = 54420;
pub(crate) const DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS: u64 = 54429;
pub(crate) const DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET: u64 = 54441;
pub(crate) const DBMS_MIN_REVISION_WITH_OPENTELEMETRY: u64 = 54442;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH: u64 = 54448;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME: u64 = 54449;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_PARALLEL_REPLICAS: u64 = 54453;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION: u64 = 54454;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM: u64 = 54458;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY: u64 = 54458;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS: u64 = 54459;

/// Active protocol revision this client advertises in Hello. Matches
/// clickhouse-cpp-client mainline (commit e903492). Bump only when
/// adding wire-format support for a higher revision, never to "stay
/// current".
pub(crate) const DBMS_TCP_PROTOCOL_VERSION: u64 = DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS;

/// Cap on the flattened `stack_trace` field of an
/// [`Error::ServerException`] built from a nested wire-format
/// [`Exception`] chain. Matches the 1 MiB bad-response body cap used
/// elsewhere; protects against hostile or runaway server output.
pub(crate) const TCP_EXCEPTION_STACK_TRACE_CAP: usize = 1 << 20;

// ---------------------------------------------------------------------------
// Packet IDs
// ---------------------------------------------------------------------------

/// Packet IDs sent client -> server. Mirrors `ClientCodes` in
/// clickhouse-cpp-client `protocol.h`.
#[repr(u64)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientPacketId {
    Hello = 0,
    Query = 1,
    Data = 2,
    Cancel = 3,
    Ping = 4,
}

/// Packet IDs sent server -> client. Mirrors `ServerCodes` in
/// clickhouse-cpp-client `protocol.h`. Includes only the IDs we
/// actually handle in subsequent branches; unknown IDs surface as
/// [`Error::BadResponse`] via [`ServerPacketId::from_u64`].
#[repr(u64)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ServerPacketId {
    Hello = 0,
    Data = 1,
    Exception = 2,
    Progress = 3,
    Pong = 4,
    EndOfStream = 5,
    ProfileInfo = 6,
    Totals = 7,
    Extremes = 8,
    Log = 10,
    TableColumns = 11,
    ProfileEvents = 14,
    TimezoneUpdate = 17,
}

impl ServerPacketId {
    pub(crate) fn from_u64(i: u64) -> Result<Self> {
        use ServerPacketId::*;
        Ok(match i {
            0 => Hello,
            1 => Data,
            2 => Exception,
            3 => Progress,
            4 => Pong,
            5 => EndOfStream,
            6 => ProfileInfo,
            7 => Totals,
            8 => Extremes,
            10 => Log,
            11 => TableColumns,
            14 => ProfileEvents,
            17 => TimezoneUpdate,
            x => {
                return Err(Error::BadResponse(format!(
                    "tcp: unknown server packet id {x}"
                )));
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Misc protocol enums
// ---------------------------------------------------------------------------

/// Query processing stage sent in the Query packet. The client always
/// requests `Complete`; lower stages exist for distributed-server
/// internal traffic that this client does not generate.
#[repr(u64)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QueryProcessingStage {
    Complete = 2,
}

/// Block compression method negotiated in the Query packet. Only `None`
/// is wired this branch; `Lz4` / `Zstd` are filled in by the writer
/// branch alongside the lz4/zstd feature gates.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum NativeCompressionMethod {
    #[default]
    None,
    Lz4,
    Zstd,
}

/// Chunked-protocol mode negotiation values, sent during the handshake
/// addendum when both peers advertise at least
/// `DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS`. Names match
/// clickhouse-cpp-client.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ChunkedProtocolMode {
    #[default]
    NotChunked,
    NotChunkedOptional,
    Chunked,
    ChunkedOptional,
}

// ---------------------------------------------------------------------------
// Handshake + packet payload staging types
// ---------------------------------------------------------------------------

/// Server Hello payload parsed by the handshake reader.
#[derive(Debug, Clone, Default)]
pub(crate) struct ServerHello {
    pub(crate) server_name: String,
    pub(crate) version: (u64, u64, u64),
    pub(crate) revision: u64,
    pub(crate) timezone: Option<String>,
    pub(crate) display_name: Option<String>,
}

/// TCP-protocol staging type for a server exception. The wire format
/// is a singly-linked list of frames (`nested: Option<Box<Self>>`)
/// rooted at the outermost (most recent) exception; Phase 1's
/// [`Error::ServerException`] variant is flat. This struct holds the
/// parse intermediate before the reader flattens the chain into a
/// single [`Error::ServerException`] at the dispatch boundary via
/// [`Exception::into_error`].
///
/// Not exposed publicly. Callers only ever see the flat
/// [`Error::ServerException`].
#[derive(Debug, Clone)]
pub(crate) struct Exception {
    /// `code` is signed on the wire (matches Phase 1's `Error::ServerException`).
    pub(crate) code: i32,
    pub(crate) name: String,
    pub(crate) message: String,
    pub(crate) stack_trace: String,
    pub(crate) nested: Option<Box<Exception>>,
}

impl Exception {
    /// Flatten the nested chain into a single
    /// [`Error::ServerException`]. The outermost frame supplies `code`
    /// and `name`. Inner-frame messages are joined into the top-level
    /// message with ` | caused by: ` separators; inner-frame stack
    /// traces are joined with `\n---\n` separators.
    ///
    /// The final stack trace is capped at
    /// [`TCP_EXCEPTION_STACK_TRACE_CAP`] (1 MiB); truncation emits a
    /// `tracing::warn!` so an operator can see the cap was hit.
    pub(crate) fn into_error(self) -> Error {
        let Exception {
            code,
            name,
            message,
            stack_trace,
            mut nested,
        } = self;

        let mut combined_message = message;
        let mut combined_stack = stack_trace;

        while let Some(boxed) = nested {
            let frame = *boxed;
            if !frame.message.is_empty() {
                combined_message.push_str(" | caused by: ");
                combined_message.push_str(&frame.message);
            }
            if !frame.stack_trace.is_empty() {
                if !combined_stack.is_empty() {
                    combined_stack.push_str("\n---\n");
                }
                combined_stack.push_str(&frame.stack_trace);
            }
            nested = frame.nested;
        }

        if combined_stack.len() > TCP_EXCEPTION_STACK_TRACE_CAP {
            tracing::warn!(
                original_len = combined_stack.len(),
                cap = TCP_EXCEPTION_STACK_TRACE_CAP,
                "tcp: server exception stack_trace truncated"
            );
            combined_stack.truncate(TCP_EXCEPTION_STACK_TRACE_CAP);
        }

        Error::ServerException {
            code,
            name: if name.is_empty() { None } else { Some(name) },
            message: combined_message,
            stack_trace: if combined_stack.is_empty() {
                None
            } else {
                Some(combined_stack)
            },
        }
    }
}

/// Progress packet payload. Fields populated depend on the negotiated
/// revision; subsequent branches fill the optional ones (e.g.
/// `total_rows_to_read`, written counters) as revisions allow.
#[derive(Debug, Clone, Default)]
pub(crate) struct Progress {
    pub(crate) rows_read: u64,
    pub(crate) bytes_read: u64,
    pub(crate) total_rows_to_read: u64,
    pub(crate) written_rows: u64,
    pub(crate) written_bytes: u64,
}

/// ProfileInfo packet payload.
#[derive(Debug, Clone, Default)]
pub(crate) struct ProfileInfo {
    pub(crate) rows: u64,
    pub(crate) blocks: u64,
    pub(crate) bytes: u64,
    pub(crate) applied_limit: bool,
    pub(crate) rows_before_limit: u64,
}

/// TableColumns packet payload (sent before INSERT to describe the
/// destination schema).
#[derive(Debug, Clone, Default)]
pub(crate) struct TableColumns {
    pub(crate) external_table_name: String,
    pub(crate) columns_definition: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_packet_id_from_u64_round_trips_known() {
        let cases = [
            (0, ServerPacketId::Hello),
            (1, ServerPacketId::Data),
            (2, ServerPacketId::Exception),
            (3, ServerPacketId::Progress),
            (4, ServerPacketId::Pong),
            (5, ServerPacketId::EndOfStream),
            (6, ServerPacketId::ProfileInfo),
            (7, ServerPacketId::Totals),
            (8, ServerPacketId::Extremes),
            (10, ServerPacketId::Log),
            (11, ServerPacketId::TableColumns),
            (14, ServerPacketId::ProfileEvents),
            (17, ServerPacketId::TimezoneUpdate),
        ];
        for (i, expected) in cases {
            assert_eq!(ServerPacketId::from_u64(i).unwrap(), expected);
        }
    }

    #[test]
    fn server_packet_id_from_u64_rejects_unknown() {
        let err = ServerPacketId::from_u64(99).unwrap_err();
        match err {
            Error::BadResponse(msg) => {
                assert!(msg.contains("99"), "msg should mention id, got: {msg}");
            }
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }

    #[test]
    fn exception_into_error_flat() {
        let exc = Exception {
            code: 60,
            name: "UNKNOWN_TABLE".to_string(),
            message: "table foo does not exist".to_string(),
            stack_trace: "frame0".to_string(),
            nested: None,
        };
        match exc.into_error() {
            Error::ServerException {
                code,
                name,
                message,
                stack_trace,
            } => {
                assert_eq!(code, 60);
                assert_eq!(name.as_deref(), Some("UNKNOWN_TABLE"));
                assert_eq!(message, "table foo does not exist");
                assert_eq!(stack_trace.as_deref(), Some("frame0"));
            }
            other => panic!("expected ServerException, got {other:?}"),
        }
    }

    #[test]
    fn exception_into_error_flattens_nested_chain() {
        // Outer -> middle -> inner. Outer fields drive code + name.
        let inner = Exception {
            code: 999,
            name: "INNER".to_string(),
            message: "root cause".to_string(),
            stack_trace: "inner-stack".to_string(),
            nested: None,
        };
        let middle = Exception {
            code: 998,
            name: "MIDDLE".to_string(),
            message: "intermediate".to_string(),
            stack_trace: "middle-stack".to_string(),
            nested: Some(Box::new(inner)),
        };
        let outer = Exception {
            code: 60,
            name: "UNKNOWN_TABLE".to_string(),
            message: "top".to_string(),
            stack_trace: "outer-stack".to_string(),
            nested: Some(Box::new(middle)),
        };
        match outer.into_error() {
            Error::ServerException {
                code,
                name,
                message,
                stack_trace,
            } => {
                assert_eq!(code, 60);
                assert_eq!(name.as_deref(), Some("UNKNOWN_TABLE"));
                assert_eq!(
                    message,
                    "top | caused by: intermediate | caused by: root cause"
                );
                assert_eq!(
                    stack_trace.as_deref(),
                    Some("outer-stack\n---\nmiddle-stack\n---\ninner-stack")
                );
            }
            other => panic!("expected ServerException, got {other:?}"),
        }
    }

    #[test]
    fn exception_into_error_caps_stack_trace() {
        let big = "x".repeat(TCP_EXCEPTION_STACK_TRACE_CAP + 1024);
        let exc = Exception {
            code: 1,
            name: "N".to_string(),
            message: "m".to_string(),
            stack_trace: big,
            nested: None,
        };
        match exc.into_error() {
            Error::ServerException { stack_trace, .. } => {
                let s = stack_trace.expect("stack_trace populated");
                assert_eq!(s.len(), TCP_EXCEPTION_STACK_TRACE_CAP);
            }
            other => panic!("expected ServerException, got {other:?}"),
        }
    }

    #[test]
    fn exception_into_error_blank_fields_become_none() {
        let exc = Exception {
            code: 1,
            name: String::new(),
            message: "m".to_string(),
            stack_trace: String::new(),
            nested: None,
        };
        match exc.into_error() {
            Error::ServerException {
                name, stack_trace, ..
            } => {
                assert!(name.is_none());
                assert!(stack_trace.is_none());
            }
            other => panic!("expected ServerException, got {other:?}"),
        }
    }
}
