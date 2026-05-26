//! ClickHouse TCP transport (native binary protocol, port 9000).
//!
//! Submodules:
//! - [`protocol`] -- wire constants, packet IDs, server response staging types.
//! - [`transport`] -- `MaybeTlsStream` plain-or-TLS adapter + buffer sizes.
//!
//! Wire-format primitives (varint, length-prefixed string, fixed-width
//! LE) come from [`crate::native::io`]; this module does not duplicate
//! them. The Native columnar encoder / decoder also lives under
//! [`crate::native`]. Connection actor, handshake, pool, and `Client`
//! integration land in subsequent branches.

// The protocol staging types and transport adapter are wired by the
// handshake, writer, reader, and actor branches that follow. The
// module-scope dead-code allowance mirrors the Phase 2 native module:
// it scopes the quiet here so the rest of the crate still benefits
// from the lint.
#![allow(dead_code)]

pub(crate) mod protocol;
pub(crate) mod transport;
