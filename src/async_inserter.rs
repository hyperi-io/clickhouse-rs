//! Concurrent auto-flushing HTTP inserter (RFC #421 shape).
//!
//! `&self` writes from many tasks, bounded MPSC channel for
//! backpressure, background task owns the `Inserter<T>` state and the
//! period-flush timer. Built on the [`worker`][crate::worker]
//! primitive so the channel + select-loop + panic surfacing + span
//! propagation are shared.
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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;

use crate::{
    Client,
    error::{Error, Result},
    inserter::{Inserter, Quantities},
    row::{RowOwned, RowWrite},
    worker::{self, CommandWorker, SendError, WorkerControl, WorkerHandle},
};

const DEFAULT_CHANNEL_CAPACITY: usize = 8192;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Thresholds for [`AsyncInserter`]. Defaults match ClickHouse's
/// recommended batch sizes.
#[derive(Debug, Clone)]
pub struct AsyncInserterConfig {
    /// Row-count flush threshold. Default: `100_000`.
    pub max_rows: u64,
    /// Byte-size flush threshold. Default: `10 MiB`.
    pub max_bytes: u64,
    /// Period flush. Default: `5 s`. `None` disables.
    pub max_period: Option<Duration>,
    /// MPSC capacity. Default: `8192`. Backpressure: full = producer blocks.
    pub channel_capacity: usize,
}

impl Default for AsyncInserterConfig {
    fn default() -> Self {
        Self {
            max_rows: 100_000,
            max_bytes: 10 * 1024 * 1024,
            max_period: Some(Duration::from_secs(5)),
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        }
    }
}

impl AsyncInserterConfig {
    /// Override the row-count flush threshold.
    pub fn with_max_rows(mut self, n: u64) -> Self {
        self.max_rows = n;
        self
    }

    /// Override the byte-size flush threshold.
    pub fn with_max_bytes(mut self, n: u64) -> Self {
        self.max_bytes = n;
        self
    }

    /// Override the period-based flush interval.
    pub fn with_max_period(mut self, d: Duration) -> Self {
        self.max_period = Some(d);
        self
    }

    /// Disable period-based flushing.
    pub fn without_period(mut self) -> Self {
        self.max_period = None;
        self
    }

    /// Override the bounded channel capacity.
    pub fn with_channel_capacity(mut self, cap: usize) -> Self {
        self.channel_capacity = cap;
        self
    }
}

// ---------------------------------------------------------------------------
// Commands sent over the MPSC channel
// ---------------------------------------------------------------------------

/// Internal command enum; not part of the public API.
#[doc(hidden)]
pub enum AsyncInserterCommand<T> {
    /// Write to the worker's preset default table (single-table mode only;
    /// errors with [`Error::Custom`] in multi-table mode).
    Write(T, oneshot::Sender<Result<()>>),
    /// Write to an explicit table (multi-table API). Lazily creates an
    /// internal [`Inserter<T>`] for the table on first use.
    WriteTo(String, T, oneshot::Sender<Result<()>>),
    /// Force-flush every per-table buffer; returns the summed [`Quantities`].
    Flush(oneshot::Sender<Result<Quantities>>),
    /// End every per-table buffer; returns the summed [`Quantities`] from
    /// the final commits.
    End(oneshot::Sender<Result<Quantities>>),
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
    /// Eager entry for single-table; lazy insert on first `WriteTo`.
    inserters: HashMap<String, Inserter<T>>,
    /// Set after `End`. Subsequent commands return safe defaults.
    ended: bool,
}

impl<T> InserterWorker<T>
where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    /// Get-or-create the inserter for `table`. Uses `HashMap::entry`
    /// for a single lookup on the hot path. New inserters inherit the
    /// worker's threshold config.
    fn get_or_create(&mut self, table: &str) -> &mut Inserter<T> {
        self.inserters
            .entry(table.to_string())
            .or_insert_with(|| {
                self.client
                    .inserter::<T>(table)
                    .with_max_rows(self.config.max_rows)
                    .with_max_bytes(self.config.max_bytes)
                    .with_period(self.config.max_period)
            })
    }

    /// Write one row + run threshold-triggered auto-commit. Propagates
    /// serialise AND auto-commit errors (architecture.md section 11).
    /// Shared between `Write` and `WriteTo` arms.
    async fn write_one(&mut self, table: &str, row: &T) -> Result<()> {
        let inserter = self.get_or_create(table);
        match inserter.write(row).await {
            Ok(()) => inserter.commit().await.map(|_| ()),
            Err(e) => Err(e),
        }
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
                AsyncInserterCommand::Write(_, resp) | AsyncInserterCommand::WriteTo(_, _, resp) => {
                    let _ = resp.send(Err(channel_closed_err()));
                }
                AsyncInserterCommand::Flush(resp) | AsyncInserterCommand::End(resp) => {
                    let _ = resp.send(Ok(Quantities::ZERO));
                }
            }
            return;
        }

        match cmd {
            AsyncInserterCommand::Write(row, resp) => {
                let Some(table) = self.default_table.clone() else {
                    let _ = resp.send(Err(Error::Custom(
                        "AsyncInserter::write called on a multi-table inserter; \
                         use write_to(table, row) instead"
                            .into(),
                    )));
                    return;
                };
                let _ = resp.send(self.write_one(table.as_ref(), &row).await);
            }
            AsyncInserterCommand::WriteTo(table, row, resp) => {
                let _ = resp.send(self.write_one(&table, &row).await);
            }
            AsyncInserterCommand::Flush(resp) => {
                // All-or-nothing across tables (architecture.md 11.1 P4).
                // First failure aborts; not-yet-flushed tables keep
                // their buffers; already-flushed tables are in CH
                // (replay relies on server-side dedup -- 11.2).
                let mut total = Quantities::ZERO;
                for inserter in self.inserters.values_mut() {
                    match inserter.force_commit().await {
                        Ok(q) => {
                            total.bytes = total.bytes.saturating_add(q.bytes);
                            total.rows = total.rows.saturating_add(q.rows);
                            total.transactions =
                                total.transactions.saturating_add(q.transactions);
                        }
                        Err(e) => {
                            let _ = resp.send(Err(e));
                            return;
                        }
                    }
                }
                let _ = resp.send(Ok(total));
            }
            AsyncInserterCommand::End(resp) => {
                let mut total = Quantities::ZERO;
                let mut last_err: Option<Error> = None;
                // `drain` consumes each inserter so the consuming
                // `Inserter::end()` can run.
                let entries: Vec<(String, Inserter<T>)> = self.inserters.drain().collect();
                for (_, inserter) in entries {
                    match inserter.end().await {
                        Ok(q) => {
                            total.bytes = total.bytes.saturating_add(q.bytes);
                            total.rows = total.rows.saturating_add(q.rows);
                            total.transactions =
                                total.transactions.saturating_add(q.transactions);
                        }
                        Err(e) => last_err = Some(e),
                    }
                }
                self.ended = true;
                let _ = resp.send(last_err.map_or(Ok(total), Err));
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
        // (no caller to receive them).
        let entries: Vec<(String, Inserter<T>)> = self.inserters.drain().collect();
        for (_, inserter) in entries {
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
/// Implements RFC #421's shape on the [`worker`][crate::worker]
/// primitive.
///
/// `T: RowOwned` because rows cross an MPSC channel. Any
/// `#[derive(Row)]` struct that owns its fields satisfies this.
///
/// Drop = best-effort final flush via implicit shutdown (errors
/// discarded). Call `.end().await` to receive the terminal
/// `Quantities` / error.
#[must_use = "end with `.end().await` to observe the terminal flush result; \
              drop discards it"]
pub struct AsyncInserter<T>
where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    handle: WorkerHandle<AsyncInserterCommand<T>>,
    control: Option<WorkerControl<AsyncInserterCommand<T>>>,
}

/// Cheap-clone write handle. Multiple handles can write concurrently.
/// Task stays alive while any handle or the original `AsyncInserter`
/// is alive. `.end()` on the original forces shutdown.
#[derive(Clone)]
pub struct AsyncInserterHandle<T>
where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    handle: WorkerHandle<AsyncInserterCommand<T>>,
}

// Call-site readability; compiles to `Error::WorkerExited` (no alloc).
#[inline]
fn channel_closed_err() -> Error {
    Error::WorkerExited
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
        let mut inserters: HashMap<String, Inserter<T>> = HashMap::new();
        if let Some(table) = default_table.as_ref() {
            // Eager construction for single-table mode (inserter ready
            // before any write).
            let inserter = client
                .inserter::<T>(table.as_ref())
                .with_max_rows(config.max_rows)
                .with_max_bytes(config.max_bytes)
                .with_period(config.max_period);
            inserters.insert(table.as_ref().to_string(), inserter);
        }

        let channel_capacity = config.channel_capacity;
        let worker = InserterWorker {
            client: client.clone(),
            config: config.clone(),
            default_table,
            inserters,
            ended: false,
        };

        let control = worker::spawn(worker, channel_capacity);
        let handle = control.handle();
        Self {
            handle,
            control: Some(control),
        }
    }

    /// Cheap-clone write handle.
    pub fn handle(&self) -> AsyncInserterHandle<T> {
        AsyncInserterHandle {
            handle: self.handle.clone(),
        }
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
        let (resp_tx, resp_rx) = oneshot::channel();
        self.handle
            .send(AsyncInserterCommand::Write(row, resp_tx))
            .await
            .map_err(|_: SendError<_>| channel_closed_err())?;
        resp_rx.await.map_err(|_| channel_closed_err())?
    }

    /// Serialise + buffer a row for `table`. Creates a per-table
    /// buffer on first sight. Works in single-table or multi-table
    /// mode (fan-in across schema-compatible tables).
    ///
    /// # Errors
    ///
    /// Same as [`write`][Self::write]: serialisation + auto-commit
    /// errors propagate through the `Result`.
    pub async fn write_to(&self, table: &str, row: T) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.handle
            .send(AsyncInserterCommand::WriteTo(
                table.to_string(),
                row,
                resp_tx,
            ))
            .await
            .map_err(|_: SendError<_>| channel_closed_err())?;
        resp_rx.await.map_err(|_| channel_closed_err())?
    }

    /// Force-flush every per-table buffer; returns summed
    /// [`Quantities`] (zero per empty buffer).
    ///
    /// # Failure semantics
    ///
    /// All-or-nothing across tables. First per-table failure aborts
    /// the rest; caller gets the error without summed `Quantities`.
    /// Pre-failure tables have already committed; post-failure tables
    /// keep their buffers for retry.
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
        let (resp_tx, resp_rx) = oneshot::channel();
        self.handle
            .send(AsyncInserterCommand::Flush(resp_tx))
            .await
            .map_err(|_: SendError<_>| channel_closed_err())?;
        resp_rx.await.map_err(|_| channel_closed_err())?
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
    pub async fn end(mut self) -> Result<Quantities> {
        let (resp_tx, resp_rx) = oneshot::channel();
        if self
            .handle
            .send(AsyncInserterCommand::End(resp_tx))
            .await
            .is_err()
        {
            return Ok(Quantities::ZERO);
        }
        let result = resp_rx
            .await
            .map_err(|_| channel_closed_err())
            .and_then(|r| r);

        // Force a definite stop: signal + await JoinHandle.
        if let Some(control) = self.control.take() {
            let _ = control.shutdown().await;
        }

        result
    }
}

impl<T> AsyncInserterHandle<T>
where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    /// Same as [`AsyncInserter::write`].
    pub async fn write(&self, row: T) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.handle
            .send(AsyncInserterCommand::Write(row, resp_tx))
            .await
            .map_err(|_: SendError<_>| channel_closed_err())?;
        resp_rx.await.map_err(|_| channel_closed_err())?
    }

    /// Serialise and buffer a row to `table` (same semantics as
    /// [`AsyncInserter::write_to`]).
    pub async fn write_to(&self, table: &str, row: T) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.handle
            .send(AsyncInserterCommand::WriteTo(
                table.to_string(),
                row,
                resp_tx,
            ))
            .await
            .map_err(|_: SendError<_>| channel_closed_err())?;
        resp_rx.await.map_err(|_| channel_closed_err())?
    }

    /// Force-flush all buffered rows across all tables (same semantics
    /// as [`AsyncInserter::flush`]).
    pub async fn flush(&self) -> Result<Quantities> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.handle
            .send(AsyncInserterCommand::Flush(resp_tx))
            .await
            .map_err(|_: SendError<_>| channel_closed_err())?;
        resp_rx.await.map_err(|_| channel_closed_err())?
    }
}
