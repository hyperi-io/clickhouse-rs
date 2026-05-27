//! ClickHouse TCP transport (native binary protocol, port 9000).
//!
//! Submodules:
//! - [`protocol`] -- wire constants, packet IDs, server response staging types.
//! - [`transport`] -- `MaybeTlsStream` plain-or-TLS adapter + buffer sizes.
//! - [`client_info`] -- `ClientInfo` block emitted inside the Query packet.
//! - [`writer`] -- client-side packet encoders (Hello, Query, Data, Cancel,
//!   Ping, Addendum) over the [`crate::native::io::ClickHouseWrite`] trait.
//! - [`reader`] -- server-side packet decoders (Hello, Data header,
//!   Exception, Progress, ProfileInfo, TableColumns, Pong, EndOfStream,
//!   Log, ProfileEvents, TimezoneUpdate) over the
//!   [`crate::native::io::ClickHouseRead`] trait.
//! - [`connect`] -- `connect_plain` (TcpStream + TCP_NODELAY + keepalive)
//!   and `open_handshaken` (connect + handshake) entry points.
//! - [`handshake`] -- `HandshakeConfig` + `handshake()` orchestrator
//!   driving send-Hello / recv-ServerHello / send-addendum.
//! - [`connection_actor`] -- `CommandWorker` impl owning the writer half
//!   and a packet-receiver fed by an independent reader sub-task;
//!   `ConnectionHandle` is the cheap-clone send-side.
//!
//! Wire-format primitives (varint, length-prefixed string, fixed-width
//! LE) come from [`crate::native::io`]; this module does not duplicate
//! them. The Native columnar encoder / decoder also lives under
//! [`crate::native`]. Pool and `Client` integration land in subsequent
//! branches.

// The protocol staging types and transport adapter are wired by the
// handshake, writer, reader, and actor branches that follow. The
// module-scope dead-code allowance mirrors the Phase 2 native module:
// it scopes the quiet here so the rest of the crate still benefits
// from the lint.
#![allow(dead_code)]

pub(crate) mod client_info;
pub mod connect;
pub mod connection_actor;
pub mod handshake;
pub(crate) mod protocol;
pub(crate) mod reader;
pub(crate) mod transport;
pub(crate) mod writer;

// Re-exports for the public TCP API: callers (and the `tests/it/`
// integration suite) need `HandshakeConfig` to drive the handshake,
// `ServerHello` to inspect what the server advertised, and
// `MaybeTlsStream` as the opaque connected-stream return type.
pub use self::handshake::HandshakeConfig;
pub use self::protocol::ServerHello;
pub use self::transport::MaybeTlsStream;
