//! ClickHouse Native columnar format primitive.
//!
//! The Native format is ClickHouse's columnar block payload encoding
//! (`format=Native` URL parameter, `Format::Native` constant). This
//! module implements a transport-agnostic primitive: it reads and
//! writes Native-format blocks against any
//! [`tokio::io::AsyncRead`]/[`tokio::io::AsyncWrite`] (or [`bytes::Buf`]/
//! [`bytes::BufMut`]) source.
//!
//! # Why this is its own module (vs. `rowbinary`)
//!
//! Upstream's `src/rowbinary/` is row-oriented: every field of every
//! row is dispatched through `serde::Serialize`. That works fine for
//! small inserts but loses the perf benefits of ClickHouse's columnar
//! layout, where each column is a contiguous run of typed values that
//! the server can write into its merge tree without a server-side
//! rows-to-columns transpose.
//!
//! `src/native/` exists to provide the columnar primitive: bulk
//! column-buffer writes using little-endian wire-format encoding
//! (LLVM-optimised on x86_64 where the conversion is the identity;
//! explicit byte-swap on big-endian hosts), amortised varint
//! length-prefix writes for strings, sparse-column wire format.
//! Explicit SIMD intrinsics are a deferred follow-up; the layout
//! is designed to be amenable so adding them is mechanical.
//!
//! # Module map
//!
//! - [`block_info`]: per-block flags (sub-block, bucket num).
//! - [`columns`]: typed column representations (numeric, string,
//!   array, nullable, low-cardinality, map, ...) and their
//!   serialise/deserialise impls.
//! - [`sparse`]: sparse-column wire format (offset list +
//!   non-default values) for columns the server emits as sparse.
//! - [`encode`]: high-level INSERT-block encoder (HTTP and TCP
//!   transports both feed bytes through this).
//! - [`compression`]: LZ4 / ZSTD framing for native blocks.
//! - [`io`]: extension traits over [`tokio::io::AsyncRead`]/
//!   [`tokio::io::AsyncWrite`] adding ClickHouse-specific helpers (varint, length-
//!   prefixed string, etc.). Also a synchronous bytes-based variant
//!   for in-memory composition.
//!
//! # Transport agnostic
//!
//! The Native format is **not** the TCP transport. The TCP transport
//! (a future `src/tcp/` module) speaks a wire protocol that
//! happens to use Native format for data blocks. HTTP can also carry
//! Native-format blocks via the `format=Native` URL parameter (layer
//! 05c). This module is the shared format-encoder/decoder both use.

// The Native primitive has no in-tree consumer yet; users are the
// sibling insert_native (HTTP `Format::Native` wiring) and the future
// `src/tcp/` transport. Until those land, every public-
// crate symbol here looks unused. The dead-code allowance scopes the
// quiet to this module so the rest of the crate still benefits from
// the lint.
#![allow(dead_code)]

pub mod block_info;
pub mod columns;
#[cfg(feature = "lz4")]
pub mod compression;
pub(crate) mod decode;
pub mod encode;
pub mod io;
pub mod sparse;

// Convenience re-exports for the HTTP path. Users composing
// `format=Native` request bodies typically need these together.
pub use block_info::BlockInfo;
pub use columns::ColumnType;
pub use encode::{ColumnSchema, encode_columns};

// Decoder primitives -- in-tree consumers are the TCP cursor in
// src/tcp/cursor.rs, the streaming actor in src/tcp/connection_actor.rs,
// and the reader in src/tcp/reader.rs. `decode_block` stays
// pub(crate) -- it's the reader sub-task's entry point and not
// useful directly to external code -- but the decoded types are
// part of the streaming surface that callers iterate, so they are
// re-exported here.
#[allow(unused_imports)]
pub(crate) use decode::decode_block;
pub use decode::{DecodedBlock, DecodedColumn};
