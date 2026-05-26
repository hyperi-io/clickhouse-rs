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
//! # Client-driven dedup tokens (at-least-once retry safety)
//!
//! [`Client::insert_batch_with_isolation`] alone gives you intra-
//! bisection dedup safety (server-side dedup catches sub-batch
//! retries within one call). For **caller-driven retries** of the
//! whole `insert_batch_with_isolation` call after a transport
//! failure, use [`Client::insert_batch_with_isolation_with_token`]
//! and pass a stable `token_base` (e.g. a logical batch UUID).
//! Each sub-batch INSERT is sent with
//! `insert_deduplication_token = "{token_base}/{start}-{end}"`, where
//! `start..=end` is the row range within the original batch. Retries
//! with the same `token_base` and the same input `rows` produce the
//! same token set, so previously-landed sub-batches dedup at the
//! server.
//!
//! Caveat: tokens are forwarded by Distributed-engine tables to each
//! shard, but each shard does its own dedup check. Non-Replicated
//! MergeTree under Distributed gives no cross-node dedup; use
//! Replicated*MergeTree for retry-safe at-least-once.
//!
//! # Memory
//!
//! Whole batch held in memory. Not suitable for streaming workloads
//! above ~100K rows -- drive
//! [`AsyncInserter`][crate::async_inserter::AsyncInserter] directly
//! for those.

use crate::Client;
use crate::error::{Error, Result};
use crate::row::{Row, RowOwned, RowWrite};

/// Bisection round-trip ceiling. The default cap stops a
/// pathologically-poisoned batch (every row rejected) from
/// wedging the ingest pipeline by issuing `~2N` round trips. At
/// the cap the call returns early with the un-bisected remainder
/// reported under `failed` and `Error::Custom("max bisection
/// round trips exceeded")`. Sized for `N <= ~512` typical batches
/// with worst-case K=N adversarial input; raise via
/// [`Client::insert_batch_with_isolation_capped`] for legitimately
/// large batches with sparse failures.
const DEFAULT_MAX_ROUND_TRIPS: u64 = 2048;

/// Outcome of `insert_batch_with_isolation`. Invariant:
/// `succeeded_rows + failed.len() == input_batch_size`.
///
/// `#[non_exhaustive]` for forward-compat: future fields (e.g.
/// `total_input_rows`, `dedup_tokens_used`, `bisection_depth`)
/// are non-breaking additions for downstream code that pattern-
/// matches the struct.
#[derive(Debug)]
#[non_exhaustive]
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
        insert_batch_internal(self, table, rows, None, DEFAULT_MAX_ROUND_TRIPS).await
    }

    /// Variant of [`insert_batch_with_isolation`][Self::insert_batch_with_isolation]
    /// that accepts an explicit `max_round_trips` ceiling. Use when
    /// the default `2048` is too low for a legitimately large batch
    /// (>~500 rows with sparse failures) or too high for a
    /// latency-sensitive call site that wants to fail fast on a
    /// poisoned batch.
    ///
    /// When the cap is hit the result carries the un-bisected
    /// remainder under `failed` with `Error::Custom("max bisection
    /// round trips exceeded")`, allowing downstream code to
    /// classify the batch as poisoned beyond practical recovery.
    pub async fn insert_batch_with_isolation_capped<T>(
        &self,
        table: &str,
        rows: Vec<T>,
        max_round_trips: u64,
    ) -> Result<BatchInsertResult<T>>
    where
        T: Row + RowOwned + RowWrite + Send + Sync + 'static,
    {
        insert_batch_internal(self, table, rows, None, max_round_trips).await
    }

    /// Insert with bisection AND caller-controlled
    /// `insert_deduplication_token` injection. Identical semantics to
    /// [`insert_batch_with_isolation`][Self::insert_batch_with_isolation]
    /// except every sub-batch INSERT carries the token
    /// `{token_base}/{start}-{end}` where `start..=end` is the
    /// sub-batch's row range within the original input.
    ///
    /// **Use for at-least-once retry:** caller passes a stable
    /// `token_base` (logical batch id). Subsequent calls with the
    /// same `token_base` AND the same `rows` produce the same token
    /// set; previously-landed sub-batches dedup at the server.
    ///
    /// `token_base` should be unique per LOGICAL batch (not per
    /// attempt). A UUIDv7 or similar is appropriate.
    ///
    /// See [module docs](self#client-driven-dedup-tokens-at-least-once-retry-safety)
    /// for the Distributed-table caveat and Replicated-vs-non-
    /// Replicated implications.
    ///
    /// # Errors
    ///
    /// Same as [`insert_batch_with_isolation`][Self::insert_batch_with_isolation].
    pub async fn insert_batch_with_isolation_with_token<T>(
        &self,
        table: &str,
        rows: Vec<T>,
        token_base: impl Into<String>,
    ) -> Result<BatchInsertResult<T>>
    where
        T: Row + RowOwned + RowWrite + Send + Sync + 'static,
    {
        let token_base = token_base.into();
        insert_batch_internal(
            self,
            table,
            rows,
            Some(token_base.as_str()),
            DEFAULT_MAX_ROUND_TRIPS,
        )
        .await
    }
}

async fn insert_batch_internal<T>(
    client: &Client,
    table: &str,
    rows: Vec<T>,
    token_base: Option<&str>,
    max_round_trips: u64,
) -> Result<BatchInsertResult<T>>
where
    T: Row + RowOwned + RowWrite + Send + Sync + 'static,
{
    let mut succeeded_rows: u64 = 0;
    let mut failed: Vec<(T, Error)> = Vec::new();
    let mut round_trips: u64 = 0;

    // Iterative DFS. Each frame carries the sub-batch + its starting
    // offset within the original input, so we can derive a stable
    // per-sub-batch dedup token.
    let mut work: Vec<(Vec<T>, usize)> = vec![(rows, 0)];
    while let Some((batch, start)) = work.pop() {
        if batch.is_empty() {
            continue;
        }
        if round_trips >= max_round_trips {
            // Cap reached. Mark every remaining row across the work
            // stack as failed with the cap error so the caller can
            // classify the batch as poisoned and replay externally.
            tracing::warn!(
                target: "clickhouse::batch_isolation",
                table = %table,
                round_trips = round_trips,
                max_round_trips = max_round_trips,
                remaining_sub_batches = work.len() + 1,
                "max bisection round trips exceeded; abandoning remaining sub-batches"
            );
            let err = || Error::Custom(format!(
                "max bisection round trips ({max_round_trips}) exceeded",
            ));
            for row in batch {
                failed.push((row, err()));
            }
            for (more_batch, _) in work {
                for row in more_batch {
                    failed.push((row, err()));
                }
            }
            return Ok(BatchInsertResult {
                succeeded_rows,
                failed,
                round_trips,
            });
        }
        let batch_len = batch.len();
        round_trips = round_trips.saturating_add(1);
        let token = token_base.map(|base| sub_batch_token(base, start, batch_len));
        match try_insert_one_batch(client, table, &batch, token.as_deref()).await {
            Ok(()) => {
                tracing::debug!(
                    target: "clickhouse::batch_isolation",
                    table = %table,
                    batch_size = batch_len,
                    range_start = start,
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
                        range_start = start,
                        round_trips = round_trips,
                        error = %e,
                        "one-row sub-batch failed"
                    );
                    let Some(row) = batch.into_iter().next() else {
                        unreachable!("invariant: batch_len == 1 implies Some(row)")
                    };
                    failed.push((row, e));
                } else {
                    tracing::debug!(
                        target: "clickhouse::batch_isolation",
                        table = %table,
                        batch_size = batch_len,
                        range_start = start,
                        round_trips = round_trips,
                        error = %e,
                        "bisecting"
                    );
                    // Push right first so left is popped first
                    // (predictable DFS round-trip count).
                    let mid = batch_len / 2;
                    let mut left = batch;
                    let right = left.split_off(mid);
                    work.push((right, start + mid));
                    work.push((left, start));
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

/// `{base}/{start}-{end}` -- deterministic per-sub-batch token
/// derived from the row range. Inclusive end so a one-row sub-batch
/// at offset N reads as `{base}/N-N`.
fn sub_batch_token(base: &str, start: usize, len: usize) -> String {
    let end = start + len - 1;
    format!("{base}/{start}-{end}")
}

/// One INSERT for the whole sub-batch. `Ok` iff the server accepted.
/// `dedup_token` is wired to `insert_deduplication_token` setting if
/// `Some`.
async fn try_insert_one_batch<T>(
    client: &Client,
    table: &str,
    rows: &[T],
    dedup_token: Option<&str>,
) -> Result<()>
where
    T: Row + RowOwned + RowWrite + Send + Sync + 'static,
{
    let mut insert = client.insert::<T>(table).await?;
    if let Some(token) = dedup_token {
        insert = insert.with_setting("insert_deduplication_token", token);
    }
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sub_batch_token_format_inclusive_end() {
        assert_eq!(sub_batch_token("batch_42", 0, 100), "batch_42/0-99");
        assert_eq!(sub_batch_token("batch_42", 100, 50), "batch_42/100-149");
        assert_eq!(sub_batch_token("batch_42", 7, 1), "batch_42/7-7");
    }

    #[test]
    fn sub_batch_token_is_deterministic_across_calls() {
        // Same inputs -> same token. The DFS bisection produces a
        // deterministic sub-batch tree for any given (rows, failure-
        // pattern) pair, so token sets stay stable across retries.
        let a = sub_batch_token("logical_42", 200, 50);
        let b = sub_batch_token("logical_42", 200, 50);
        assert_eq!(a, b);
    }
}
