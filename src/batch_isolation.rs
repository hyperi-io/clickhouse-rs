//! Batch insert with per-row failure isolation via bisection.
//!
//! [`Client::insert_batch_with_isolation`] sends the whole batch as
//! one INSERT; on server rejection (atomic for binary formats), it
//! bisects the batch and recurses until each bad row is isolated to
//! its own one-row INSERT (whose error is the per-row diagnostic).
//!
//! # Round-trip cost
//!
//! Upper bound: `1 + 2K * (1 + ceil(log2(N / max(K, 1))))`. For
//! `K == 1`: `1 + 2 * ceil(log2(N))`.
//!
//! | N | K (bad) | round trips |
//! |----|---------|-------------|
//! | 10000 | 0 | 1 |
//! | 10000 | 1 | ~28 |
//! | 10000 | 2 | ~56 |
//! | 10000 | 5 | ~140 |
//!
//! Actual count is usually slightly lower (see
//! `tests/it/batch_isolation.rs`). [`BatchInsertResult::round_trips`]
//! reports the real number; `> 100` suggests pre-filtering upstream.
//!
//! # Dedup requirement
//!
//! Destination table MUST have block-level dedup. Default for
//! `ReplicatedMergeTree`-family engines
//! (`replicated_deduplication_window=100`); for non-replicated, set
//! `insert_deduplicate=1` in session settings. Without it, a
//! sub-batch retry can produce duplicates.
//!
//! # Memory
//!
//! Whole batch held in memory. Not suitable for streaming workloads
//! above ~100K rows -- drive
//! [`AsyncInserter`][crate::async_inserter::AsyncInserter] directly
//! for those.

use crate::error::{Error, Result};
use crate::row::{Row, RowOwned, RowWrite};
use crate::Client;

/// Outcome of `insert_batch_with_isolation`. Invariant:
/// `succeeded_rows + failed.len() == input_batch_size`.
#[derive(Debug)]
pub struct BatchInsertResult<T> {
    /// Rows confirmed inserted by ClickHouse.
    pub succeeded_rows: u64,
    /// Server-rejected rows + the error from the minimal sub-batch
    /// that isolated each. Usually a one-row INSERT, in which case
    /// the error is the per-row diagnostic.
    pub failed: Vec<(T, Error)>,
    /// HTTP INSERT requests issued. `1` = clean fast path.
    pub round_trips: u64,
}

impl<T> BatchInsertResult<T> {
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.failed.is_empty()
    }
}

impl Client {
    /// Insert with bisection-based poison-pill isolation.
    ///
    /// See module docs for cost, dedup, memory.
    ///
    /// # Errors
    ///
    /// `Err` only for failures that block all progress (initial
    /// connect, missing table). Per-row failures are reported in
    /// `Ok(BatchInsertResult { failed, .. })`.
    pub async fn insert_batch_with_isolation<T>(
        &self,
        table: &str,
        rows: Vec<T>,
    ) -> Result<BatchInsertResult<T>>
    where
        T: Row + RowOwned + RowWrite + Send + Sync + 'static,
    {
        let mut succeeded_rows: u64 = 0;
        let mut failed: Vec<(T, Error)> = Vec::new();
        let mut round_trips: u64 = 0;

        // Iterative DFS. Recursion needs async-recursion lifetime
        // gymnastics; a stack doesn't.
        let mut work: Vec<Vec<T>> = vec![rows];
        while let Some(batch) = work.pop() {
            if batch.is_empty() {
                continue;
            }
            let batch_len = batch.len();
            round_trips = round_trips.saturating_add(1);
            match try_insert_one_batch(self, table, &batch).await {
                Ok(()) => {
                    tracing::debug!(
                        target: "clickhouse::batch_isolation",
                        table = %table,
                        batch_size = batch_len,
                        round_trips = round_trips,
                        "sub-batch landed"
                    );
                    succeeded_rows = succeeded_rows.saturating_add(batch_len as u64);
                }
                Err(e) => {
                    if batch_len == 1 {
                        tracing::debug!(
                            target: "clickhouse::batch_isolation",
                            table = %table,
                            round_trips = round_trips,
                            error = %e,
                            "one-row sub-batch failed"
                        );
                        let row = batch.into_iter().next().expect("len == 1");
                        failed.push((row, e));
                    } else {
                        tracing::debug!(
                            target: "clickhouse::batch_isolation",
                            table = %table,
                            batch_size = batch_len,
                            round_trips = round_trips,
                            error = %e,
                            "bisecting"
                        );
                        // Push right first so left is popped first
                        // (predictable DFS round-trip count).
                        let mid = batch_len / 2;
                        let mut left = batch;
                        let right = left.split_off(mid);
                        work.push(right);
                        work.push(left);
                    }
                }
            }
        }

        Ok(BatchInsertResult {
            succeeded_rows,
            failed,
            round_trips,
        })
    }
}

/// One INSERT for the whole sub-batch. `Ok` iff the server accepted.
async fn try_insert_one_batch<T>(
    client: &Client,
    table: &str,
    rows: &[T],
) -> Result<()>
where
    T: Row + RowOwned + RowWrite + Send + Sync + 'static,
{
    let mut insert = client.insert::<T>(table).await?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await
}
