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
use std::time::Duration;

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
}

// ---------------------------------------------------------------------------
// Commands sent over the MPSC channel
// ---------------------------------------------------------------------------

/// Internal command enum.
pub(crate) enum AsyncInserterCommand<T> {
    Write {
        row: T,
        reply: oneshot::Sender<Result<()>>,
    },
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
                AsyncInserterCommand::Write { reply, .. } => {
                    let _ = reply.send(Err(Error::WorkerExited));
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
            AsyncInserterCommand::Write { row, reply } => {
                // Propagate both serialise AND threshold-triggered commit
                // errors. Without this, a max_rows commit failure during
                // write would return Ok to the caller while the buffered
                // rows had been discarded by Insert::abort() -- silently
                // losing the rows.
                let result = match inserter.write(&row).await {
                    Ok(()) => inserter.commit().await.map(|_| ()),
                    Err(e) => Err(e),
                };
                let _ = reply.send(result);
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
            inner: AsyncInserterHandle { handle },
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

    /// Force-flush buffered rows. See [`AsyncInserterHandle::flush`].
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
        self.send_cmd(|reply| AsyncInserterCommand::Write { row, reply })
            .await
    }

    /// Force-flush buffered rows, returning committed [`Quantities`].
    /// Empty buffer = zero.
    ///
    /// # Errors
    ///
    /// Flush error (HTTP 4xx/5xx, schema mismatch, etc.). Caller
    /// drives retry; the library does not preserve row buffers across
    /// transport failures (architecture.md section 11.5).
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
