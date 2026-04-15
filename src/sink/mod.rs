//! Prototype sink abstraction for Inserter generification.
//!
//! Status: DRAFT for review. Not wired into any production code path.
//! Context: Slack discussion 2026-04-15 re: Austin's concern about
//! maintaining the same Inserter logic across RowBinary / Native / Arrow
//! / future formats. This module proposes a trait shape to absorb
//! TableBatcher and AsyncInserter functionality into a single
//! `Inserter<S: Sink>` where the format+transport is the pluggable bit.
//!
//! # Why not HKT
//!
//! Austin's gut worry: "my gut tells me it's going to be a lot of HKT
//! screwiness that may not even be expressible in the language."
//!
//! Answer: not HKT territory. Rust's trait hierarchy + conditional `impl`
//! blocks cover it in stable 1.89 (edition 2024, native AFIT). Proof:
//! this module compiles against upstream main with no nightly features.
//!
//! # The shape
//!
//! ```text
//! Sink             flush()              -- all sinks
//!  |
//!  +-- StreamSink  write_row()          -- RowBinary: encode bytes eagerly
//!  |
//!  +-- BatchSink   write_batch()        -- Native / Arrow: defer columnarise
//! ```
//!
//! `HttpRowBinarySink` implements both StreamSink AND BatchSink
//! (batch is just a loop). `HttpNativeSink`, `HttpArrowSink`,
//! `NativeTcpSink` implement only BatchSink because their formats
//! require the full row set to columnarise or block-encode.
//!
//! The user-facing `Inserter<S: Sink>` exposes `write` when `S: StreamSink`
//! and `write_all` when `S: BatchSink`, via conditional impl blocks. A
//! sink implementing both offers both methods; a batch-only sink offers
//! only `write_all` and row-by-row use is a compile error rather than a
//! runtime surprise.

use std::future::Future;

use crate::error::Result;
use crate::row::Row;

// Note: traits use `impl Future<Output = ...> + Send` rather than `async fn`
// to be explicit about Send bounds on the returned futures. The compiler warns
// against `async fn` in public traits (auto-trait bounds cannot be specified
// with the sugar). For a public library trait that callers will move across
// task boundaries, `+ Send` is required; the desugared form makes that
// guarantee part of the API contract.

/// Base sink trait: every sink must flush pending bytes to its transport.
pub trait Sink {
    /// Commit all prior `write_row` / `write_batch` calls. Returns stats
    /// about what was sent since the last flush.
    fn flush(&mut self) -> impl Future<Output = Result<FlushStats>> + Send;
}

/// Sinks that can encode rows one at a time.
///
/// Example: `HttpRowBinarySink` -- each row is RowBinary-encoded and
/// appended to a chunked POST body immediately.
pub trait StreamSink: Sink {
    fn write_row<T: Row + Sync>(
        &mut self,
        row: &T,
    ) -> impl Future<Output = Result<()>> + Send;
}

/// Sinks that require the full batch before encoding.
///
/// Examples:
/// - `HttpNativeSink`: transposes rows into columnar blocks at flush.
/// - `HttpArrowSink`: builds Arrow `RecordBatch` then serialises.
/// - `NativeTcpSink`: emits columnar blocks as TCP protocol packets.
pub trait BatchSink: Sink {
    fn write_batch<T: Row + Sync>(
        &mut self,
        rows: &[T],
    ) -> impl Future<Output = Result<()>> + Send;
}

/// Statistics returned from a flush operation.
#[derive(Debug, Default, Clone, Copy)]
pub struct FlushStats {
    pub bytes_sent: u64,
    pub rows_committed: u64,
}

pub mod inserter;
pub mod null;
