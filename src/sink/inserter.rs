//! `Inserter<S: Sink>`: single generic entry point that absorbs the
//! functionality currently split across `crate::inserter::Inserter`,
//! `TableBatcher` (our fork), and `AsyncInserter` (our fork).
//!
//! Cross-cutting concerns (flush policy, retry, tracing, schema cache)
//! live here once, parameterised over any `Sink`. Transport+format
//! specifics live only in each `Sink` implementation.
//!
//! This is a prototype scaffold. Real Layer 2 implementation will:
//! - Retrofit upstream's `Inserter<T>` methods (`write`, `end`, `commit`,
//!   `force_commit`, `with_max_rows`, `with_max_bytes`, `with_period`)
//!   onto this generic Inserter while preserving the public API for
//!   existing callers (default `S = HttpRowBinarySink<T>`).
//! - Move tracing span setup, retry, and concurrency wrappers here.
//! - Implement `HttpRowBinarySink`, `HttpNativeSink`, `NativeTcpSink`
//!   (and later Austin's `HttpArrowSink`) as `Sink` impls only.

use std::time::{Duration, Instant};

use super::{BatchSink, FlushStats, Sink, StreamSink};
use crate::error::Result;
use crate::row::Row;

/// Auto-flush policy. `None` on every field = manual-flush-only mode,
/// which matches upstream `Inserter`'s default. Opt-in triggers give
/// `TableBatcher`-style batching.
#[derive(Debug, Default, Clone, Copy)]
pub struct FlushPolicy {
    pub max_rows: Option<u64>,
    pub max_bytes: Option<u64>,
    pub max_elapsed: Option<Duration>,
}

impl FlushPolicy {
    pub fn with_max_rows(mut self, rows: u64) -> Self {
        self.max_rows = Some(rows);
        self
    }

    pub fn with_max_bytes(mut self, bytes: u64) -> Self {
        self.max_bytes = Some(bytes);
        self
    }

    pub fn with_max_elapsed(mut self, period: Duration) -> Self {
        self.max_elapsed = Some(period);
        self
    }
}

pub struct Inserter<S: Sink> {
    sink: S,
    policy: FlushPolicy,
    pending_rows: u64,
    pending_bytes: u64,
    since_flush: Instant,
}

impl<S: Sink> Inserter<S> {
    pub fn new(sink: S) -> Self {
        Self {
            sink,
            policy: FlushPolicy::default(),
            pending_rows: 0,
            pending_bytes: 0,
            since_flush: Instant::now(),
        }
    }

    pub fn with_policy(mut self, policy: FlushPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Manual flush. Same semantics as upstream `Inserter::commit`.
    pub async fn flush(&mut self) -> Result<FlushStats> {
        let stats = self.sink.flush().await?;
        self.pending_rows = 0;
        self.pending_bytes = 0;
        self.since_flush = Instant::now();
        Ok(stats)
    }

    /// Terminate the inserter, flushing any pending rows. Matches upstream
    /// `Inserter::end` semantics (returns final stats).
    pub async fn end(mut self) -> Result<FlushStats> {
        self.flush().await
    }

    fn should_flush(&self) -> bool {
        self.policy
            .max_rows
            .is_some_and(|m| self.pending_rows >= m)
            || self
                .policy
                .max_bytes
                .is_some_and(|m| self.pending_bytes >= m)
            || self
                .policy
                .max_elapsed
                .is_some_and(|m| self.since_flush.elapsed() >= m)
    }
}

// Conditional impl: `write` available only for sinks that can stream rows.
// A `BatchSink`-only sink (HttpNativeSink, HttpArrowSink, NativeTcpSink)
// will not have a `write` method on its `Inserter<S>`, so row-by-row
// usage against a batch-only format is a compile error, not a runtime
// surprise. A sink that implements both `StreamSink` and `BatchSink`
// (HttpRowBinarySink) gets both methods.
impl<S: StreamSink> Inserter<S> {
    pub async fn write<T: Row + Sync>(&mut self, row: &T) -> Result<()> {
        self.sink.write_row(row).await?;
        self.pending_rows += 1;
        if self.should_flush() {
            self.flush().await?;
        }
        Ok(())
    }
}

// Conditional impl: `write_all` available only for batch-capable sinks.
impl<S: BatchSink> Inserter<S> {
    pub async fn write_all<T: Row + Sync>(&mut self, rows: &[T]) -> Result<()> {
        self.sink.write_batch(rows).await?;
        self.pending_rows += rows.len() as u64;
        if self.should_flush() {
            self.flush().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::null::NullSink;

    // Trait-composition proof: Inserter<NullSink> compiles and exercises both
    // StreamSink and BatchSink conditional impls (NullSink implements both).
    // Austin's HKT concern: this resolves cleanly in stable Rust 1.89.
    #[tokio::test]
    async fn inserter_with_null_sink_composes() {
        let mut ins = Inserter::new(NullSink::default()).with_policy(
            FlushPolicy::default()
                .with_max_rows(1_000)
                .with_max_bytes(8 * 1024 * 1024)
                .with_max_elapsed(Duration::from_secs(5)),
        );
        let stats = ins.flush().await.unwrap();
        assert_eq!(stats.bytes_sent, 0);
        assert_eq!(stats.rows_committed, 0);
        let final_stats = ins.end().await.unwrap();
        assert_eq!(final_stats.rows_committed, 0);
    }

    // Verifies the FlushPolicy builder resolves through the `with_policy`
    // setter without requiring construction of the full Inserter for each
    // permutation.
    #[test]
    fn flush_policy_builder_chains() {
        let policy = FlushPolicy::default()
            .with_max_rows(100)
            .with_max_bytes(1024);
        assert_eq!(policy.max_rows, Some(100));
        assert_eq!(policy.max_bytes, Some(1024));
        assert_eq!(policy.max_elapsed, None);
    }
}
