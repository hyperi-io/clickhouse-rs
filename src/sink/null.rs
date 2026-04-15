//! `NullSink`: compiles-out stub that proves trait composition.
//!
//! Implements `Sink`, `StreamSink`, and `BatchSink` simultaneously on one
//! concrete type. This demonstrates the shape Austin was worried about:
//! one sink can be row-streaming AND batch-capable at the same time,
//! while another sink can be batch-only -- without any HKT gymnastics.
//!
//! Not used in production. Remove before Layer 2 final PR; real sinks
//! (`HttpRowBinarySink`, `HttpNativeSink`, etc.) replace this stub.

use super::{BatchSink, FlushStats, Sink, StreamSink};
use crate::error::Result;
use crate::row::Row;

#[derive(Default)]
pub struct NullSink {
    pending_rows: u64,
}

impl Sink for NullSink {
    async fn flush(&mut self) -> Result<FlushStats> {
        let stats = FlushStats {
            bytes_sent: 0,
            rows_committed: self.pending_rows,
        };
        self.pending_rows = 0;
        Ok(stats)
    }
}

impl StreamSink for NullSink {
    async fn write_row<T: Row + Sync>(&mut self, _row: &T) -> Result<()> {
        self.pending_rows += 1;
        Ok(())
    }
}

impl BatchSink for NullSink {
    async fn write_batch<T: Row + Sync>(&mut self, rows: &[T]) -> Result<()> {
        self.pending_rows += rows.len() as u64;
        Ok(())
    }
}
