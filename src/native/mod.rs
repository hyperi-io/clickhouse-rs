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

// Layer 05a is the format primitive; consumers in upstream are layer
// 05c (HTTP `Format::Native` wiring -- next PR) and the future
// `src/tcp/` transport. Until those land, every public-
// crate symbol here looks unused. The dead-code allowance scopes the
// quiet to this module so the rest of the crate still benefits from
// the lint.
#![allow(dead_code)]

pub(crate) mod block_info;
pub(crate) mod columns;
#[cfg(feature = "lz4")]
pub(crate) mod compression;
pub(crate) mod encode;
pub(crate) mod io;
pub(crate) mod sparse;
