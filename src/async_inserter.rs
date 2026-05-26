//! Concurrent auto-flushing HTTP inserter (RFC #421 shape).
//!
//! `&self` writes from many tasks, bounded MPSC channel for
//! backpressure, background task owns the `Inserter<T>` state and the
//! period-flush timer. Built on the internal `worker` primitive so
//! the channel + select-loop + panic surfacing + span propagation
//! are shared.
//!
//! ```no_run
//! # async fn example() -> clickhouse::error::Result<()> {
//! # use clickhouse::Client;
//! # use clickhouse::async_inserter::{AsyncInserter, AsyncInserterConfig};
//! # #[derive(clickhouse::Row, serde::Serialize, serde::Deserialize)]
//! # struct MyRow { x: u32 }
//! let client = Client::default().with_url("http://localhost:8123");
//! let inserter: AsyncInserter<MyRow> = AsyncInserter::new(
//!     &client, "my_table", AsyncInserterConfig::default(),
//! );
//! inserter.write(MyRow { x: 1 }).await?;
//! inserter.write(MyRow { x: 2 }).await?;
//! let _q = inserter.flush().await?;
//! let _final = inserter.end().await?;
//! # Ok(()) }
//! ```

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, SystemTime};

use tokio::sync::oneshot;
use tokio::task::JoinError;

use crate::{
    Client,
    error::{Error, Result},
    inserter::{Inserter, Quantities},
    row::{RowOwned, RowWrite},
    worker::{self, CommandWorker, SendError, WorkerControl, WorkerHandle},
};

const DEFAULT_CHANNEL_CAPACITY: usize = 8192;

/// ClickHouse server-side `max_insert_block_size` default (24.x /
/// 25.x). Anything larger gets split server-side into multiple
/// blocks, each independently dedup'd -- which breaks the
/// atomic-fail assumption used by `batch_isolation` and surprises
/// callers expecting one block = one transaction. We warn (not
/// error) because operators can configure a larger value
/// server-side.
const SERVER_MAX_INSERT_BLOCK_SIZE: u64 = 1_048_576;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// `log_comment` setting strategy. Splits the server-side
/// `async_insert` flush queue per writer so one writer's bad row
/// does not poison another writer's queries
/// ([ClickHouse#86651](https://github.com/ClickHouse/ClickHouse/issues/86651)).
///
/// The server queues async inserts by
/// `(query_text, settings_hash, user)`; setting `log_comment` to a
/// distinct value per writer changes settings_hash and segregates
/// the queue. Setting comes through to ClickHouse as a plain
/// query-level setting; no schema change needed server-side.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub enum WriterId {
    /// Auto-generate a per-instance id (default). Format:
    /// `clickhouse-rs:async_inserter:{nanos:x}:{seq:x}` where
    /// `nanos` is process-time-unique and `seq` is a process-local
    /// atomic counter -- distinct across writers in one process
    /// AND likely-distinct across processes.
    #[default]
    Auto,
    /// Explicit value provided by the caller. Useful for grouping
    /// multiple AsyncInserter instances under one tag (e.g.
    /// per-shard-of-app, per-tenant).
    Custom(String),
    /// Don't inject `log_comment`. Use only when you've set a
    /// `log_comment` at the [`Client`] level, or you genuinely
    /// don't want per-writer queue splitting. Concurrent writers
    /// to a CHECK-constrained table can see false-positive errors
    /// from `#86651` without this mitigation.
    Disabled,
}

/// Synchronous per-flush observability hook. Fires on every commit
/// (auto-commit from write threshold, explicit flush, end). Called
/// from the worker task; must not block. First arg is the table
/// name; second is the [`Quantities`] of THIS commit (zeros for
/// no-op commits). `Arc` so all per-table commits share the same
/// callback instance across the AsyncInserter's lifetime.
///
/// # Per-call budget
///
/// Target < 1 microsecond per invocation. The dispatch is
/// synchronous on the worker task, so a slow callback serialises
/// flushes across all tables. Increment a counter, atomic-store
/// to an `AtomicU64`, or push to a bounded channel -- anything
/// heavier (lock acquisition, `tracing::info!` to a remote
/// subscriber, file I/O) should spawn a separate observer task
/// fed by the channel instead.
pub type CommitCallback = Arc<dyn Fn(&str, &Quantities) + Send + Sync + 'static>;

/// Thresholds for [`AsyncInserter`]. Defaults match ClickHouse's
/// recommended batch sizes.
#[derive(Clone)]
pub struct AsyncInserterConfig {
    /// Row-count flush threshold. Default: `100_000`.
    pub max_rows: u64,
    /// Byte-size flush threshold. Default: `10 MiB`.
    pub max_bytes: u64,
    /// Period flush. Default: `5 s`. `None` disables.
    pub max_period: Option<Duration>,
    /// MPSC capacity. Default: `8192`. Backpressure: full = producer blocks.
    pub channel_capacity: usize,
    /// Cross-table total-bytes watermark. When the sum of pending
    /// bytes across ALL per-table inserters exceeds this value
    /// after a write, the worker force-flushes every table.
    ///
    /// `None` (default) disables. Useful for many-table writers
    /// where individual per-table thresholds rarely trip but
    /// aggregate memory still grows. Set to a multiple of
    /// `max_bytes` (e.g. `Some(100 * max_bytes)`) sized to your
    /// process's memory budget.
    pub cross_table_max_bytes: Option<u64>,
    /// `log_comment` injection strategy. Default
    /// [`WriterId::Auto`] mitigates async_insert flush poisoning;
    /// see [`WriterId`] for the rationale.
    pub writer_id: WriterId,
    /// Optional per-flush observability hook. See [`CommitCallback`]
    /// and [`with_commit_callback`][Self::with_commit_callback].
    pub commit_callback: Option<CommitCallback>,
}

// Manual Debug because `dyn Fn` is not Debug.
impl std::fmt::Debug for AsyncInserterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncInserterConfig")
            .field("max_rows", &self.max_rows)
            .field("max_bytes", &self.max_bytes)
            .field("max_period", &self.max_period)
            .field("channel_capacity", &self.channel_capacity)
            .field("writer_id", &self.writer_id)
            .field("commit_callback", &self.commit_callback.as_ref().map(|_| "<closure>"))
            .finish()
    }
}

impl Default for AsyncInserterConfig {
    fn default() -> Self {
        Self {
            max_rows: 100_000,
            max_bytes: 10 * 1024 * 1024,
            max_period: Some(Duration::from_secs(5)),
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            cross_table_max_bytes: None,
            writer_id: WriterId::Auto,
            commit_callback: None,
        }
    }
}

impl AsyncInserterConfig {
    /// Override the row-count flush threshold.
    ///
    /// Warns at `tracing::warn` level if `n` exceeds ClickHouse's
    /// default `max_insert_block_size` (1,048,576). Larger values
    /// get split server-side into multiple blocks, each
    /// independently dedup'd -- breaking the atomic-fail assumption
    /// in [`crate::Client::insert_batch_with_isolation`]. Operators
    /// who've raised `max_insert_block_size` server-side can ignore
    /// the warning.
    pub fn with_max_rows(mut self, n: u64) -> Self {
        if n > SERVER_MAX_INSERT_BLOCK_SIZE {
            tracing::warn!(
                target: "clickhouse::async_inserter",
                max_rows = n,
                server_default = SERVER_MAX_INSERT_BLOCK_SIZE,
                "max_rows exceeds ClickHouse's default max_insert_block_size; \
                 inserts will be split server-side into multiple blocks, \
                 breaking per-INSERT atomicity. Set this lower or raise \
                 max_insert_block_size on the server."
            );
        }
        self.max_rows = n;
        self
    }

    /// Override the byte-size flush threshold.
    pub fn with_max_bytes(mut self, n: u64) -> Self {
        self.max_bytes = n;
        self
    }

    /// Override the period-based flush interval. Zero is clamped
    /// to 1 millisecond -- `tokio::time::interval` panics on a
    /// zero duration, so the setter normalises rather than
    /// deferring the panic to spawn time.
    pub fn with_max_period(mut self, d: Duration) -> Self {
        self.max_period = Some(d.max(Duration::from_millis(1)));
        self
    }

    /// Disable period-based flushing.
    pub fn without_period(mut self) -> Self {
        self.max_period = None;
        self
    }

    /// Override the bounded channel capacity. Zero is clamped to 1
    /// -- `tokio::sync::mpsc::channel(0)` panics, so the setter
    /// normalises rather than deferring the panic to spawn time.
    pub fn with_channel_capacity(mut self, cap: usize) -> Self {
        self.channel_capacity = cap.max(1);
        self
    }

    /// Set the cross-table total-bytes watermark. See
    /// [`cross_table_max_bytes`][Self::cross_table_max_bytes].
    pub fn with_cross_table_max_bytes(mut self, n: u64) -> Self {
        self.cross_table_max_bytes = Some(n);
        self
    }

    /// Disable the cross-table watermark (the default).
    pub fn without_cross_table_max_bytes(mut self) -> Self {
        self.cross_table_max_bytes = None;
        self
    }

    /// Use an explicit `log_comment` value for INSERTs from this
    /// AsyncInserter. Overrides the auto-generated default. See
    /// [`WriterId`] for the queue-splitting rationale.
    pub fn with_writer_id(mut self, id: impl Into<String>) -> Self {
        self.writer_id = WriterId::Custom(id.into());
        self
    }

    /// Disable `log_comment` injection. Only do this if you've set
    /// it at the [`Client`] level or you accept the
    /// [#86651](https://github.com/ClickHouse/ClickHouse/issues/86651)
    /// poisoning hazard.
    pub fn without_writer_id(mut self) -> Self {
        self.writer_id = WriterId::Disabled;
        self
    }

    /// Register a per-flush callback. Fires after every per-table
    /// commit -- threshold-triggered auto-commits during `write`,
    /// explicit `flush`, and final `end`. First arg is the table
    /// name; second is the [`Quantities`] of THIS commit (zeros for
    /// no-op commits when there were no buffered rows).
    ///
    /// Use this for per-table metrics, audit logs, or downstream
    /// pipeline notifications. The callback runs inline on the
    /// worker task -- must not block. For real work, channel out
    /// or spawn.
    ///
    /// Multi-table: fires once per (table, commit) pair, so a
    /// `flush` across N tables produces N callback invocations.
    pub fn with_commit_callback(
        mut self,
        cb: impl Fn(&str, &Quantities) + Send + Sync + 'static,
    ) -> Self {
        self.commit_callback = Some(Arc::new(cb));
        self
    }
}

/// Resolve a [`WriterId`] to the actual `log_comment` string (if
/// any) for this AsyncInserter instance. `None` skips injection.
fn resolve_writer_id(id: &WriterId) -> Option<String> {
    match id {
        WriterId::Auto => Some(generate_auto_writer_id()),
        WriterId::Custom(s) => Some(s.clone()),
        WriterId::Disabled => None,
    }
}

/// Generate `clickhouse-rs:async_inserter:{nanos:x}:{seq:x}`.
/// `nanos`: nanoseconds since UNIX epoch (zero if unavailable).
/// `seq`: process-local monotonic counter. Distinct across
/// AsyncInserters in one process and likely-distinct across
/// processes started at different times.
#[cold]
fn generate_auto_writer_id() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    format!("clickhouse-rs:async_inserter:{nanos:x}:{seq:x}")
}

/// Dispatch `cb(table, q)` iff `cb` is set AND the commit actually
/// did work (zero-transaction commits indicate no buffered rows --
/// observers don't need to see them). Shared by `write_one`,
/// `force_commit_all`, the `Flush` arm, and the `End` arm.
#[inline]
fn fire_callback(cb: Option<&CommitCallback>, table: &str, q: &Quantities) {
    if q.transactions > 0
        && let Some(cb) = cb
    {
        cb(table, q);
    }
}

/// Build one [`Inserter<T>`] for `table` from `client` + `config` +
/// optional `writer_id`. Shared by eager (single-table) construction
/// in [`AsyncInserter::spawn_inner`] and lazy (multi-table)
/// construction in [`InserterWorker::get_or_create`] -- keeps the
/// builder chain drift-proof.
fn build_inserter<T>(
    client: &Client,
    table: &str,
    config: &AsyncInserterConfig,
    writer_id: Option<&str>,
) -> Inserter<T>
where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    let inserter = client
        .inserter::<T>(table)
        .with_max_rows(config.max_rows)
        .with_max_bytes(config.max_bytes)
        .with_period(config.max_period);
    match writer_id {
        Some(id) => inserter.with_setting("log_comment", id),
        None => inserter,
    }
}

// ---------------------------------------------------------------------------
// Commands sent over the MPSC channel
// ---------------------------------------------------------------------------

/// Internal command enum; not part of the public API.
pub(crate) enum AsyncInserterCommand<T> {
    /// Write a row. `table = None` routes to the worker's default
    /// (single-table mode); `table = Some(t)` routes to an explicit
    /// table, creating its per-table buffer on first use. `None` on
    /// a multi-table inserter returns
    /// [`Error::AsyncInserterApiMisuse`]. `Arc<str>` so subsequent
    /// writes to the same table are refcount-bumps, not allocations
    /// -- see [`TableInterner`].
    Write {
        table: Option<Arc<str>>,
        row: T,
        reply: oneshot::Sender<Result<()>>,
    },
    /// Force-flush every per-table buffer; returns the summed [`Quantities`].
    Flush(oneshot::Sender<Result<Quantities>>),
    /// End every per-table buffer; returns the summed [`Quantities`] from
    /// the final commits.
    End(oneshot::Sender<Result<Quantities>>),
}

// ---------------------------------------------------------------------------
// Table-name interner
// ---------------------------------------------------------------------------

/// Per-instance soft cap on cached table names. A multi-tenant
/// ingest service receiving the table name from user input (or a
/// broken upstream client sending a fresh name per row) can
/// silently grow the interner unboundedly. Above the cap we log
/// once and fall through to "allocate a fresh `Arc<str>` per
/// call" -- correctness preserved, perf-degraded, operators see
/// the signal.
///
/// 16384 is generous for a bounded-set workload (the documented
/// `write_to` use case) and tight enough to surface
/// pathological growth before it OOMs the process.
const MAX_INTERNED_TABLES: usize = 16_384;

/// Cache of `Arc<str>` table names. First call for a table allocates;
/// subsequent calls return an `Arc::clone` (~5 ns refcount bump).
/// Useful at hyperscale -- a writer fanning out 1M rows/s across
/// even a few dozen tables otherwise allocates a fresh `String` per
/// `write_to` for the table name.
///
/// Stored in `AsyncInserterHandle` so all clones share one cache.
/// `RwLock` because the steady-state path is read-only; for very
/// high-QPS multi-table-fan-out workloads a per-bucket lock
/// structure (e.g. `dashmap`) is a follow-up worth benchmarking.
pub(crate) struct TableInterner {
    map: RwLock<HashMap<Arc<str>, ()>>,
    /// Set to `true` once we've emitted the over-cap warning, so
    /// the log line fires once per inserter rather than per write.
    cap_warned: AtomicBool,
}

impl TableInterner {
    fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            cap_warned: AtomicBool::new(false),
        }
    }

    /// Get-or-insert the `Arc<str>` for `key`. Returns a clone of
    /// the canonical entry, or -- if the per-inserter cap is
    /// already saturated and `key` is new -- a fresh allocation
    /// bypassing the cache (and a one-shot warning).
    fn intern(&self, key: &str) -> Arc<str> {
        // Hot path: read lock + Arc::clone.
        if let Some(arc) = self
            .map
            .read()
            .expect("interner read poisoned")
            .get_key_value(key)
            .map(|(k, _)| Arc::clone(k))
        {
            return arc;
        }
        // Slow path: insert. Re-check under the write lock in case
        // another caller raced us between read-drop and write-acquire.
        let mut map = self.map.write().expect("interner write poisoned");
        if let Some(arc) = map.get_key_value(key).map(|(k, _)| Arc::clone(k)) {
            return arc;
        }
        if map.len() >= MAX_INTERNED_TABLES {
            // Surface the signal once per interner lifetime.
            if !self.cap_warned.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    target: "clickhouse::async_inserter",
                    cached_tables = map.len(),
                    cap = MAX_INTERNED_TABLES,
                    "table-name interner reached its soft cap; \
                     subsequent unique table names allocate a fresh \
                     Arc per write. Check for unbounded table-name \
                     input (per-row uniqueness suggests a caller bug)."
                );
            }
            return Arc::from(key);
        }
        let arc: Arc<str> = Arc::from(key);
        map.insert(Arc::clone(&arc), ());
        arc
    }
}

// ---------------------------------------------------------------------------
// The worker -- holds the underlying Inserter
// ---------------------------------------------------------------------------

struct InserterWorker<T> {
    /// Used by `get_or_create` for lazy per-table construction.
    client: Client,
    config: AsyncInserterConfig,
    /// `Some` for single-table mode (routes `Write` commands here);
    /// `None` for multi-table (rejects `Write`, accepts `WriteTo`).
    /// `Arc<str>` so the per-`Write` clone is a refcount bump.
    default_table: Option<Arc<str>>,
    /// Resolved `log_comment` value, derived once at spawn time from
    /// [`WriterId`]. `None` means "don't inject"; same value
    /// re-applied to every per-table Inserter so they all share the
    /// same server-side flush queue partition.
    writer_id: Option<String>,
    /// Cloned from config for per-commit dispatch. Cached on the
    /// worker to skip the `config.commit_callback.as_ref()` lookup
    /// on the hot path.
    commit_callback: Option<CommitCallback>,
    /// Eager entry for single-table; lazy insert on first `WriteTo`.
    /// `BTreeMap` so iteration order is deterministic across runs --
    /// `Flush` / `End` visit tables in lexicographic order, which makes
    /// failure-attribution and test recording stable. Cost vs `HashMap`
    /// is one O(log N) compare per `WriteTo`; N is the per-AsyncInserter
    /// table count (typically <10), so the log-factor is negligible.
    /// Key is `Arc<str>` so the BTreeMap entry shares the same arc the
    /// handle-side interner produces -- no second allocation per write.
    inserters: BTreeMap<Arc<str>, Inserter<T>>,
    /// Set after `End`. Subsequent commands return safe defaults.
    ended: bool,
}

impl<T> InserterWorker<T>
where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    /// Get-or-create the inserter for `table`. Uses `BTreeMap::entry`
    /// for a single lookup on the hot path. The build chain is
    /// inlined (rather than calling a `&self` helper) because the
    /// `entry()` borrow of `self.inserters` would otherwise conflict
    /// with `&self` -- the alternative (contains_key + insert + get_mut)
    /// is three lookups per call. Same chain runs eagerly in
    /// [`AsyncInserter::spawn_inner`] for single-table mode.
    ///
    /// `table` is an `Arc<str>` from the handle's interner; the
    /// `entry(table.clone())` does an `Arc::clone` (~5 ns refcount
    /// bump) for fresh entries and zero allocation thereafter.
    fn get_or_create(&mut self, table: Arc<str>) -> &mut Inserter<T> {
        let client = &self.client;
        let config = &self.config;
        let writer_id = self.writer_id.as_deref();
        let table_str: &str = &table;
        self.inserters
            .entry(Arc::clone(&table))
            .or_insert_with(|| build_inserter(client, table_str, config, writer_id))
    }

    /// Write one row + run threshold-triggered auto-commit. Propagates
    /// serialise AND auto-commit errors (architecture.md section 11).
    ///
    /// Fires the per-commit callback if configured AND the commit
    /// actually completed (non-zero `transactions`); zero-transaction
    /// commits mean the threshold wasn't tripped, so no actual
    /// server-side INSERT happened.
    ///
    /// After the per-table commit, if the cross-table watermark
    /// is configured AND the sum of pending bytes across ALL
    /// inserters exceeds it, force-flush every table.
    async fn write_one(&mut self, table: Arc<str>, row: &T) -> Result<()> {
        let callback = self.commit_callback.clone();
        // Only clone the table arc when there's a callback to feed it
        // to; the common (callback = None) path skips the refcount bump.
        let table_for_callback =
            callback.as_ref().map(|_| Arc::clone(&table));
        {
            let inserter = self.get_or_create(table);
            inserter.write(row).await?;
            let quantities = inserter.commit().await?;
            if let Some(tab) = table_for_callback.as_deref() {
                fire_callback(callback.as_ref(), tab, &quantities);
            }
        }

        if let Some(threshold) = self.config.cross_table_max_bytes {
            let total: u64 = self
                .inserters
                .values()
                .map(|i| i.pending().bytes)
                .sum();
            if total >= threshold {
                tracing::debug!(
                    target: "clickhouse::async_inserter",
                    total_pending_bytes = total,
                    threshold = threshold,
                    "cross-table watermark tripped; force-flushing all tables"
                );
                self.force_commit_all().await?;
            }
        }
        Ok(())
    }

    /// Force-commit every per-table inserter. Used by the
    /// cross-table watermark check in [`write_one`][Self::write_one].
    /// Fires the per-commit callback for each non-zero commit.
    /// Stops on the first per-table error.
    async fn force_commit_all(&mut self) -> Result<()> {
        let callback = self.commit_callback.clone();
        for (table, inserter) in self.inserters.iter_mut() {
            let q = inserter.force_commit().await?;
            fire_callback(callback.as_ref(), table.as_ref(), &q);
        }
        Ok(())
    }

    /// Consume every per-table inserter (paired with its table arc)
    /// so each one's consuming [`Inserter::end()`] can run. Used by
    /// `End` and `on_shutdown`. The intermediate `Vec` is forced
    /// because we need to await each `end()` and a borrowed
    /// `BTreeMap` iter can't be held across `.await`.
    fn drain_inserters_with_tables(&mut self) -> Vec<(Arc<str>, Inserter<T>)> {
        std::mem::take(&mut self.inserters)
            .into_iter()
            .collect()
    }
}

impl<T> CommandWorker for InserterWorker<T>
where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    type Command = AsyncInserterCommand<T>;

    fn name() -> &'static str {
        "async_inserter"
    }

    fn idle_interval(&self) -> Option<Duration> {
        self.config.max_period
    }

    async fn handle(&mut self, cmd: Self::Command) {
        if self.ended {
            // Post-End: inserters consumed; reply with safe defaults so
            // callers don't hang while we wait for shutdown.
            match cmd {
                AsyncInserterCommand::Write { reply, .. } => {
                    let _ = reply.send(Err(Error::WorkerExited));
                }
                AsyncInserterCommand::Flush(reply) | AsyncInserterCommand::End(reply) => {
                    let _ = reply.send(Ok(Quantities::ZERO));
                }
            }
            return;
        }

        match cmd {
            AsyncInserterCommand::Write { table, row, reply } => {
                // Resolve explicit `Some(table)` first, fall back to
                // the worker's default. No default + no explicit table
                // = caller used the wrong API. Both sides are
                // `Arc<str>` -- no per-write allocation.
                let resolved: Option<Arc<str>> = table.or_else(|| self.default_table.clone());
                let Some(table) = resolved else {
                    let _ = reply.send(Err(Error::AsyncInserterApiMisuse {
                        method: "write",
                        hint: "use write_to(table, row) on a multi-table inserter",
                    }));
                    return;
                };
                let _ = reply.send(self.write_one(table, &row).await);
            }
            AsyncInserterCommand::Flush(reply) => {
                // All-or-nothing across tables (architecture.md 11.1 P4).
                // First failure aborts; not-yet-flushed tables keep
                // their buffers; already-flushed tables are in CH
                // (replay relies on server-side dedup -- 11.2).
                let mut total = Quantities::ZERO;
                let callback = self.commit_callback.clone();
                for (table, inserter) in self.inserters.iter_mut() {
                    match inserter.force_commit().await {
                        Ok(q) => {
                            total.bytes = total.bytes.saturating_add(q.bytes);
                            total.rows = total.rows.saturating_add(q.rows);
                            total.transactions =
                                total.transactions.saturating_add(q.transactions);
                            fire_callback(callback.as_ref(), table.as_ref(), &q);
                        }
                        Err(e) => {
                            let _ = reply.send(Err(e));
                            return;
                        }
                    }
                }
                let _ = reply.send(Ok(total));
            }
            AsyncInserterCommand::End(reply) => {
                let mut total = Quantities::ZERO;
                let mut last_err: Option<Error> = None;
                let callback = self.commit_callback.clone();
                for (table, inserter) in self.drain_inserters_with_tables() {
                    match inserter.end().await {
                        Ok(q) => {
                            total.bytes = total.bytes.saturating_add(q.bytes);
                            total.rows = total.rows.saturating_add(q.rows);
                            total.transactions =
                                total.transactions.saturating_add(q.transactions);
                            fire_callback(callback.as_ref(), table.as_ref(), &q);
                        }
                        Err(e) => last_err = Some(e),
                    }
                }
                self.ended = true;
                let _ = reply.send(last_err.map_or(Ok(total), Err));
            }
        }
    }

    async fn on_idle(&mut self) {
        if self.ended {
            return;
        }
        // commit() runs the threshold check; idle on a quiet worker is a no-op.
        for inserter in self.inserters.values_mut() {
            let _ = inserter.commit().await;
        }
    }

    async fn on_shutdown(&mut self) {
        if self.ended {
            return;
        }
        // Worker's final cleanup: drain each inserter; errors dropped
        // (no caller to receive them). Table names discarded -- the
        // commit callback fires only via the user-visible End command,
        // not implicit shutdown.
        for (_table, inserter) in self.drain_inserters_with_tables() {
            let _ = inserter.end().await;
        }
        self.ended = true;
    }
}

// ---------------------------------------------------------------------------
// AsyncInserter -- public API
// ---------------------------------------------------------------------------

/// Concurrent auto-flushing inserter for a single ClickHouse table (HTTP).
///
/// Differences from [`Inserter<T>`][crate::inserter::Inserter]:
/// `&self` writes (shareable via [`handle()`][Self::handle] clones,
/// no caller-side mutex); serialisation + I/O on a background task;
/// auto-flush on row/byte/period limits; bounded MPSC backpressure.
/// Implements RFC #421's shape on the internal `worker` primitive.
///
/// `T: RowOwned` because rows cross an MPSC channel. Any
/// `#[derive(Row)]` struct that owns its fields satisfies this.
///
/// # Shutdown discipline
///
/// Drop = best-effort final flush via implicit shutdown (errors
/// discarded). Call `.end().await` to receive the terminal
/// `Quantities` / error, including any panic from the background task.
///
/// For a process receiving SIGTERM (k8s pod termination, systemd
/// stop, etc.) the recommended pattern is to drive `.end().await`
/// before the tokio runtime exits:
///
/// ```ignore
/// # use clickhouse::async_inserter::AsyncInserter;
/// async fn graceful_shutdown<T>(inserter: AsyncInserter<T>) -> clickhouse::error::Result<()>
/// where T: clickhouse::Row + clickhouse::row::RowOwned + clickhouse::row::RowWrite + Send + Sync + 'static,
/// {
///     // wire this into your signal handler (e.g. tokio::signal::ctrl_c
///     // or signal_hook + tokio_util::sync::CancellationToken)
///     let _final = inserter.end().await?;
///     Ok(())
/// }
/// ```
///
/// If the value is dropped with rows still pending (i.e. `end()`
/// was not called and the worker had un-flushed state), Drop logs
/// a `tracing::warn!` at target `clickhouse::async_inserter` to
/// give operators a visible signal that the final batch may have
/// been lost.
#[must_use = "end with `.end().await` to observe the terminal flush result; \
              drop discards it"]
pub struct AsyncInserter<T>
where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    inner: AsyncInserterHandle<T>,
    control: Option<WorkerControl<AsyncInserterCommand<T>>>,
}

/// Cheap-clone write handle. Multiple handles can write concurrently.
/// Task stays alive while any handle or the original `AsyncInserter`
/// is alive. `.end()` on the original forces shutdown.
pub struct AsyncInserterHandle<T>
where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    handle: WorkerHandle<AsyncInserterCommand<T>>,
    /// Shared table-name interner. All clones reference the same
    /// `TableInterner` so a repeated `write_to("orders", ...)` from
    /// any handle hits the cache after the first call.
    interner: Arc<TableInterner>,
}

// Manual Clone: derive would add a spurious `T: Clone` bound, but
// `WorkerHandle<C>` is `Clone` for any `C` (see worker/mod.rs).
impl<T> Clone for AsyncInserterHandle<T>
where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            interner: Arc::clone(&self.interner),
        }
    }
}

/// Extract a readable string from a panicking `JoinError`. Handles
/// the common `panic!("literal")` and `panic!("formatted: {x}")`
/// payload types; falls back to a placeholder for anything else.
#[cold]
#[inline(never)]
fn describe_panic(err: JoinError) -> String {
    if !err.is_panic() {
        return err.to_string();
    }
    let payload: Box<dyn Any + Send + 'static> = err.into_panic();
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    "<non-string panic payload>".to_string()
}

impl<T> AsyncInserter<T>
where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    /// Single-table `AsyncInserter` for `table`. Spawns the background
    /// task immediately. [`write`][Self::write] routes to `table`;
    /// [`write_to`][Self::write_to] still works with other tables (a
    /// per-table buffer is created on first use).
    pub fn new(client: &Client, table: &str, config: AsyncInserterConfig) -> Self {
        Self::spawn_inner(client, Some(Arc::from(table)), config)
    }

    /// Multi-table `AsyncInserter`. No default table; callers must use
    /// [`write_to`][Self::write_to]. Per-table buffers share thresholds
    /// and a single background task (one period tick coordinates
    /// flushes across all tables). [`write`][Self::write] returns an
    /// error in this mode.
    pub fn new_multi_table(client: &Client, config: AsyncInserterConfig) -> Self {
        Self::spawn_inner(client, None, config)
    }

    fn spawn_inner(
        client: &Client,
        default_table: Option<Arc<str>>,
        config: AsyncInserterConfig,
    ) -> Self {
        // Resolve writer_id once at spawn time. All per-table
        // inserters share the same value so the server-side flush
        // queue gets partitioned per AsyncInserter instance (not per
        // table), which is the right granularity for #86651
        // mitigation.
        let writer_id = resolve_writer_id(&config.writer_id);

        let mut inserters: BTreeMap<Arc<str>, Inserter<T>> = BTreeMap::new();
        if let Some(table) = default_table.as_ref() {
            // Eager construction for single-table mode (inserter ready
            // before any write). Shares the build chain with
            // [`InserterWorker::get_or_create`] via `build_inserter`.
            let inserter = build_inserter(client, table.as_ref(), &config, writer_id.as_deref());
            inserters.insert(Arc::clone(table), inserter);
        }

        // Pre-warm the interner with the default table (if any) so
        // the single-table fast path resolves through it without a
        // second alloc. For multi-table mode, the interner starts
        // empty.
        let interner = Arc::new(TableInterner::new());
        if let Some(default) = default_table.as_ref() {
            let _ = interner.intern(default.as_ref());
        }

        let channel_capacity = config.channel_capacity;
        let commit_callback = config.commit_callback.clone();
        let worker = InserterWorker {
            client: client.clone(),
            config: config.clone(),
            default_table,
            writer_id,
            commit_callback,
            inserters,
            ended: false,
        };

        let control = worker::spawn(worker, channel_capacity);
        let handle = control.handle();
        Self {
            inner: AsyncInserterHandle {
                handle,
                interner,
            },
            control: Some(control),
        }
    }

    /// Cheap-clone write handle.
    pub fn handle(&self) -> AsyncInserterHandle<T> {
        self.inner.clone()
    }

    /// Serialise and buffer a row. See [`AsyncInserterHandle::write`].
    pub async fn write(&self, row: T) -> Result<()> {
        self.inner.write(row).await
    }

    /// Serialise + buffer a row for `table`. See
    /// [`AsyncInserterHandle::write_to`].
    pub async fn write_to(&self, table: &str, row: T) -> Result<()> {
        self.inner.write_to(table, row).await
    }

    /// Force-flush every per-table buffer. See
    /// [`AsyncInserterHandle::flush`].
    pub async fn flush(&self) -> Result<Quantities> {
        self.inner.flush().await
    }

    /// Graceful shutdown: flush remaining rows, end the underlying
    /// INSERT, and stop the background task. Returns the final batch's
    /// [`Quantities`].
    ///
    /// Consumes `self`. Cloned [`AsyncInserterHandle`]s become inert
    /// (their `write`/`flush` calls return errors) once shutdown
    /// completes.
    ///
    /// # Failure semantics
    ///
    /// Best-effort across tables (unlike [`flush`][Self::flush]).
    /// Every per-table end is attempted; `Quantities` sums successes;
    /// last error is surfaced. No replay path past end -- partial
    /// loss is the cost of graceful shutdown (the alternative loses
    /// MORE rows).
    ///
    /// If the End command succeeds but the background task itself
    /// panicked, the panic message is surfaced as [`Error::Custom`].
    /// An End-command error wins over a panic (the End error is
    /// usually the closer-to-source diagnostic).
    pub async fn end(mut self) -> Result<Quantities> {
        let end_result = self
            .inner
            .send_cmd(AsyncInserterCommand::End)
            .await;

        // Force a definite stop: signal + await JoinHandle so a panic in
        // the background task surfaces here instead of being silently
        // discarded by Drop.
        let join_result = if let Some(control) = self.control.take() {
            control.shutdown().await
        } else {
            Ok(())
        };

        match (end_result, join_result) {
            (Ok(q), Ok(())) => Ok(q),
            (Ok(_), Err(join_err)) if join_err.is_panic() => Err(Error::Custom(format!(
                "AsyncInserter background task panicked: {}",
                describe_panic(join_err)
            ))),
            // End reply error is closer to the underlying cause; prefer it.
            (Err(e), _) => Err(e),
            // Non-panic JoinError (task cancelled). End reply succeeded, so
            // the work is on the server; report success.
            (Ok(q), Err(_)) => Ok(q),
        }
    }
}

impl<T> Drop for AsyncInserter<T>
where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    fn drop(&mut self) {
        // `end()` consumes self and sets control = None; arriving
        // here with control still Some means the caller dropped us
        // without driving the terminal flush. Log so operators see
        // it (k8s pod-termination loses the in-flight batch
        // otherwise -- the rustdoc walks the SIGTERM recipe).
        if self.control.is_some() {
            tracing::warn!(
                target: "clickhouse::async_inserter",
                "AsyncInserter dropped without end().await; \
                 in-flight rows ship via best-effort shutdown and \
                 any commit error is discarded. Drive end().await \
                 from your signal handler to observe the terminal \
                 result."
            );
        }
    }
}

impl<T> AsyncInserterHandle<T>
where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    /// Single send-cmd-await-reply helper. Builds a oneshot, hands
    /// `tx` to the closure to embed in the command, sends to the
    /// worker, awaits the reply. Maps worker-exit at either step to
    /// [`Error::WorkerExited`].
    async fn send_cmd<R>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<R>>) -> AsyncInserterCommand<T>,
    ) -> Result<R> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.handle
            .send(build(resp_tx))
            .await
            .map_err(|_: SendError<_>| Error::WorkerExited)?;
        resp_rx.await.map_err(|_| Error::WorkerExited)?
    }

    /// Serialise and buffer a row. Asynchronously blocks if the
    /// channel is full. Returns after the row is buffered and any
    /// threshold-triggered auto-commit has completed.
    ///
    /// # Errors
    ///
    /// `Err` if the row failed to serialise, or if a threshold-
    /// triggered auto-commit during this `write` failed. Auto-commit
    /// errors propagate through the `Result` rather than being
    /// swallowed -- callers must observe before advancing any
    /// upstream commit pointer (e.g. Kafka offset). The row itself
    /// was buffered successfully; retry policy is the caller's
    /// (architecture.md section 11.5).
    pub async fn write(&self, row: T) -> Result<()> {
        self.send_cmd(|reply| AsyncInserterCommand::Write {
            table: None,
            row,
            reply,
        })
        .await
    }

    /// Serialise + buffer a row for `table`. Creates a per-table
    /// buffer on first sight. Works in single-table or multi-table
    /// mode (fan-in across schema-compatible tables).
    ///
    /// # Caveats
    ///
    /// `write_to` is intended for a **bounded set of tables** known
    /// in advance (e.g. dozens, not millions). The internal name
    /// interner caches every distinct `table` argument as an
    /// `Arc<str>` for the lifetime of the [`AsyncInserter`]; passing
    /// a fresh table name per row grows the interner unboundedly.
    /// Callers driving high-cardinality table fan-out should
    /// memoise or pre-canonicalise their names externally.
    ///
    /// # Errors
    ///
    /// Same as [`write`][Self::write]: serialisation + auto-commit
    /// errors propagate through the `Result`.
    pub async fn write_to(&self, table: &str, row: T) -> Result<()> {
        // First call for this table allocates the Arc<str>; every
        // subsequent call is an Arc::clone refcount bump.
        let table = Some(self.interner.intern(table));
        self.send_cmd(|reply| AsyncInserterCommand::Write { table, row, reply })
            .await
    }

    /// Force-flush every per-table buffer; returns summed
    /// [`Quantities`] (zero per empty buffer).
    ///
    /// # Failure semantics
    ///
    /// All-or-nothing across tables. First per-table failure aborts
    /// the rest; caller gets the error without summed `Quantities`.
    /// Pre-failure tables have already committed; post-failure tables
    /// keep their buffers for retry. Tables are visited in
    /// lexicographic order (`BTreeMap` iteration), so failure
    /// attribution is stable across runs.
    ///
    /// At-least-once replay needs server-side block-level dedup.
    /// `ReplicatedMergeTree` has this by default
    /// (`replicated_deduplication_window=100`); for non-replicated,
    /// set `insert_deduplicate=1`. See architecture.md sections
    /// 11.1 P4 and 11.2.
    ///
    /// # Errors
    ///
    /// First per-table flush error (HTTP 4xx/5xx, schema mismatch,
    /// etc.). Caller drives retry; the library does not preserve
    /// row buffers across transport failures
    /// (architecture.md section 11.5).
    pub async fn flush(&self) -> Result<Quantities> {
        self.send_cmd(AsyncInserterCommand::Flush).await
    }
}

#[cfg(test)]
mod setter_clamp_tests {
    use super::*;

    #[test]
    fn with_max_period_zero_clamps_to_one_millisecond() {
        let cfg = AsyncInserterConfig::default().with_max_period(Duration::ZERO);
        assert_eq!(cfg.max_period, Some(Duration::from_millis(1)));
    }

    #[test]
    fn with_max_period_above_minimum_unchanged() {
        let cfg = AsyncInserterConfig::default().with_max_period(Duration::from_secs(7));
        assert_eq!(cfg.max_period, Some(Duration::from_secs(7)));
    }

    #[test]
    fn with_channel_capacity_zero_clamps_to_one() {
        let cfg = AsyncInserterConfig::default().with_channel_capacity(0);
        assert_eq!(cfg.channel_capacity, 1);
    }

    #[test]
    fn with_channel_capacity_above_minimum_unchanged() {
        let cfg = AsyncInserterConfig::default().with_channel_capacity(16);
        assert_eq!(cfg.channel_capacity, 16);
    }
}

#[cfg(test)]
mod writer_id_tests {
    use super::*;

    #[test]
    fn writer_id_auto_yields_distinct_values_across_calls() {
        // Two back-to-back resolves of WriterId::Auto must produce
        // different strings, even within the same nanosecond. The
        // process-local atomic counter is what guarantees this; the
        // nanos prefix is best-effort cross-process distinctness.
        let a = resolve_writer_id(&WriterId::Auto).expect("Auto -> Some");
        let b = resolve_writer_id(&WriterId::Auto).expect("Auto -> Some");
        assert_ne!(a, b, "Auto must produce distinct ids per call: {a} == {b}");

        // Format sanity: both share the documented prefix.
        for v in [&a, &b] {
            assert!(
                v.starts_with("clickhouse-rs:async_inserter:"),
                "auto id missing prefix: {v}"
            );
        }
    }

    #[test]
    fn writer_id_custom_resolves_verbatim_and_disabled_resolves_none() {
        assert_eq!(
            resolve_writer_id(&WriterId::Custom("shard-7".into())),
            Some("shard-7".to_string()),
        );
        assert_eq!(resolve_writer_id(&WriterId::Disabled), None);
    }
}

#[cfg(test)]
mod interner_tests {
    use super::*;

    #[test]
    fn intern_returns_same_arc_for_same_key() {
        let interner = TableInterner::new();
        let a = interner.intern("orders");
        let b = interner.intern("orders");
        // Both Arc instances must point to the same allocation.
        assert!(Arc::ptr_eq(&a, &b), "repeat intern of same key should share alloc");
        assert_eq!(a.as_ref(), "orders");
    }

    #[test]
    fn intern_separates_distinct_keys() {
        let interner = TableInterner::new();
        let a = interner.intern("orders");
        let b = interner.intern("events");
        assert!(!Arc::ptr_eq(&a, &b));
        assert_eq!(a.as_ref(), "orders");
        assert_eq!(b.as_ref(), "events");
    }

    #[test]
    fn intern_refcount_bumps_on_cache_hit() {
        let interner = TableInterner::new();
        let first = interner.intern("orders");
        let strong_after_first = Arc::strong_count(&first);
        let _second = interner.intern("orders");
        let _third = interner.intern("orders");
        let strong_after_three = Arc::strong_count(&first);
        // Two more clones in the wild (+ 1 inside the interner's
        // HashMap key) means strong count rose by exactly the new
        // observations.
        assert_eq!(strong_after_three, strong_after_first + 2);
    }

    #[test]
    fn intern_is_thread_safe() {
        use std::sync::Arc;
        use std::thread;

        let interner = Arc::new(TableInterner::new());
        let mut handles = vec![];
        for i in 0..8 {
            let interner = Arc::clone(&interner);
            handles.push(thread::spawn(move || {
                // 4 threads intern "shared", 4 intern unique keys.
                if i < 4 {
                    interner.intern("shared")
                } else {
                    interner.intern(&format!("unique_{i}"))
                }
            }));
        }
        let arcs: Vec<Arc<str>> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        // All four "shared" arcs are pointer-equal.
        for window in arcs[..4].windows(2) {
            assert!(Arc::ptr_eq(&window[0], &window[1]));
        }
        // Unique keys are NOT pointer-equal to shared.
        for unique in &arcs[4..] {
            assert!(!Arc::ptr_eq(&arcs[0], unique));
        }
    }
}

#[cfg(test)]
mod callback_bound_tests {
    use super::*;

    // CommitCallback must remain Send + Sync + 'static so the worker
    // task (`tokio::spawn`-ed) can hold and dispatch it across thread
    // boundaries. A future refactor that drops any of those bounds
    // would compile in isolation but break the multi-thread runtime
    // at the point of construction; this test fails first.
    #[test]
    fn commit_callback_is_send_sync_static() {
        fn assert_send_sync_static<T: Send + Sync + 'static>() {}
        assert_send_sync_static::<CommitCallback>();
    }
}
