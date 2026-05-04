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
    Write(T, oneshot::Sender<Result<()>>),
    Flush(oneshot::Sender<Result<Quantities>>),
    End(oneshot::Sender<Result<Quantities>>),
}

// ---------------------------------------------------------------------------
// The worker -- holds the underlying Inserter
// ---------------------------------------------------------------------------

struct InserterWorker<T> {
    /// `Option` so `End` / `on_shutdown` can `take` for the consuming
    /// `Inserter::end()`. Once `None`, subsequent commands are no-ops.
    inserter: Option<Inserter<T>>,
    /// Held so `idle_interval` can read the period; `Inserter<T>`
    /// stores its own copy but doesn't expose it.
    config: AsyncInserterConfig,
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
        let Some(inserter) = self.inserter.as_mut() else {
            // Already ended; subsequent commands no-op or report closed.
            match cmd {
                AsyncInserterCommand::Write(_, resp) => {
                    let _ = resp.send(Err(channel_closed_err()));
                }
                AsyncInserterCommand::Flush(resp) => {
                    let _ = resp.send(Ok(Quantities::ZERO));
                }
                AsyncInserterCommand::End(resp) => {
                    let _ = resp.send(Ok(Quantities::ZERO));
                }
            }
            return;
        };

        match cmd {
            AsyncInserterCommand::Write(row, resp) => {
                let result = inserter.write(&row).await;
                if result.is_ok() {
                    // commit() runs the threshold check; no-op if untripped.
                    let _ = inserter.commit().await;
                }
                let _ = resp.send(result);
            }
            AsyncInserterCommand::Flush(resp) => {
                let _ = resp.send(inserter.force_commit().await);
            }
            AsyncInserterCommand::End(resp) => {
                // Take out the inserter so the consuming end() can run.
                // After this, self.inserter is None and the worker is
                // effectively done; the runner will exit when shutdown
                // is signalled.
                // The runner serialises commands and the `End` arm is the
                // last one to run before shutdown, so `self.inserter` is
                // always `Some` here. The runner exits after this match
                // anyway; the second `End` would be a programmer error.
                let Some(inserter) = self.inserter.take() else {
                    unreachable!("End reached after the inserter was already consumed");
                };
                let _ = resp.send(inserter.end().await);
            }
        }
    }

    async fn on_idle(&mut self) {
        if let Some(inserter) = self.inserter.as_mut() {
            // commit() runs the threshold check; idle on a quiet worker is a no-op.
            let _ = inserter.commit().await;
        }
    }

    async fn on_shutdown(&mut self) {
        if let Some(inserter) = self.inserter.take() {
            let _ = inserter.end().await;
        }
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
    /// Create an `AsyncInserter` for `table`. Spawns the background
    /// task immediately.
    pub fn new(client: &Client, table: &str, config: AsyncInserterConfig) -> Self {
        let inserter = client
            .inserter::<T>(table)
            .with_max_rows(config.max_rows)
            .with_max_bytes(config.max_bytes)
            .with_period(config.max_period);

        let channel_capacity = config.channel_capacity;
        let worker = InserterWorker {
            inserter: Some(inserter),
            config,
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

    /// Serialise and buffer a row. Asynchronously blocks if the channel
    /// is full. Returns after the row is buffered and any threshold-
    /// triggered auto-commit has completed.
    pub async fn write(&self, row: T) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.handle
            .send(AsyncInserterCommand::Write(row, resp_tx))
            .await
            .map_err(|_: SendError<_>| channel_closed_err())?;
        resp_rx.await.map_err(|_| channel_closed_err())?
    }

    /// Force-flush buffered rows, returning committed [`Quantities`].
    /// Empty buffer = zero.
    pub async fn flush(&self) -> Result<Quantities> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.handle
            .send(AsyncInserterCommand::Flush(resp_tx))
            .await
            .map_err(|_: SendError<_>| channel_closed_err())?;
        resp_rx.await.map_err(|_| channel_closed_err())?
    }

    /// Graceful shutdown: flush, end INSERT, stop the task. Returns
    /// the final batch's [`Quantities`]. Consumes `self`; cloned
    /// handles become inert.
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

    /// Same as [`AsyncInserter::flush`].
    pub async fn flush(&self) -> Result<Quantities> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.handle
            .send(AsyncInserterCommand::Flush(resp_tx))
            .await
            .map_err(|_: SendError<_>| channel_closed_err())?;
        resp_rx.await.map_err(|_| channel_closed_err())?
    }
}
