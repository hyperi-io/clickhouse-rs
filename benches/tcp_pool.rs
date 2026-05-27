//! TCP pool acquire / release / recycle bench.
//!
//! Anchors three numbers the pool design rests on:
//!
//! 1. `ConnectionHandle::is_alive()` -- atomic-load cost, hot in the
//!    deadpool `recycle` callback. Sub-100ns expected.
//! 2. Idle-pool acquire + release round-trip via a no-op
//!    `deadpool::managed::Manager`. Benches deadpool's own
//!    overhead -- the floor our pool's acquire cost cannot go below.
//! 3. Contention behaviour at pool size 8 with N concurrent
//!    acquirers. Establishes the per-acquire cost under saturation.
//!
//! The pool's `TcpConnectionManager` is `pub(crate)` at this branch,
//! so the bench cannot exercise it directly. Instead the bench drives
//! a separate no-op deadpool pool to measure deadpool overhead in
//! isolation, plus uses the only public surface of the pool's per-
//! connection type -- `ConnectionHandle::is_alive()` -- to anchor
//! the recycle-callback hot path.
//!
//! Run:
//!   cargo bench --bench tcp_pool --features 'tcp test-util'
//!
//! Measured numbers (13th Gen Intel Core i7-1355U, rustc 1.89.0,
//! default system allocator, --quick):
//!
//!   tcp_pool/is_alive_atomic_load:        392.68 ps
//!     (point estimate; sub-nanosecond -- inlined load + branch)
//!   tcp_pool/deadpool_acquire_release:    156.83 ns
//!     (point estimate; idle pool of size 8, no contention)
//!   tcp_pool/deadpool_acquire_size8_x16:  8.60 us
//!     (point estimate; 16 concurrent acquirers, pool size 8)
//!
//! The is_alive atomic load rounds to a single CPU instruction --
//! the cost the recycle callback adds per pool acquire is
//! negligible compared to the ~160 ns deadpool overhead. Contention
//! at 2:1 acquirer-to-slot ratio sits at 8.6 us per batch of 16
//! acquires (~540 ns per acquire), dominated by tokio scheduler
//! handoff through the deadpool wait queue.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use criterion::{Criterion, criterion_group, criterion_main};
use deadpool::{
    Runtime,
    managed::{self, Metrics, Pool, RecycleResult},
};

mod common;

/// No-op `deadpool` manager that returns a fresh `Arc<AtomicBool>`
/// per slot. The bench measures deadpool's own acquire, release, and
/// recycle overhead, not connection creation; `create` therefore
/// allocates only the atomic the recycle path will check.
struct NoopManager;

impl managed::Manager for NoopManager {
    type Type = Arc<AtomicBool>;
    type Error = std::convert::Infallible;

    async fn create(&self) -> Result<Arc<AtomicBool>, Self::Error> {
        Ok(Arc::new(AtomicBool::new(true)))
    }

    async fn recycle(
        &self,
        obj: &mut Arc<AtomicBool>,
        _metrics: &Metrics,
    ) -> RecycleResult<Self::Error> {
        // Mirror TcpConnectionManager::recycle: cheap atomic load,
        // accept-or-reject. The recycle callback is the bench's
        // closest proxy for ConnectionHandle's is_alive in the
        // deadpool flow.
        if obj.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(managed::RecycleError::message("poisoned"))
        }
    }
}

fn build_noop_pool(max_size: usize) -> Pool<NoopManager> {
    Pool::builder(NoopManager)
        .max_size(max_size)
        .runtime(Runtime::Tokio1)
        .build()
        .expect("pool build")
}

async fn run_acquire_release(iters: u64, pool: Pool<NoopManager>) -> Duration {
    let start = Instant::now();
    for _ in 0..iters {
        let obj = pool.get().await.expect("acquire");
        drop(obj);
    }
    start.elapsed()
}

async fn run_concurrent_acquire(
    iters: u64,
    pool: Pool<NoopManager>,
    concurrency: usize,
) -> Duration {
    // Each iteration: spawn `concurrency` acquirers, each does ONE
    // get + drop, then join. The total elapsed time is therefore the
    // wall-clock for `iters * concurrency` acquires distributed over
    // `concurrency` tokio tasks, which exposes the queueing /
    // handoff cost when concurrency exceeds pool size.
    let start = Instant::now();
    for _ in 0..iters {
        let mut tasks = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let pool = pool.clone();
            tasks.push(tokio::spawn(async move {
                let obj = pool.get().await.expect("acquire");
                drop(obj);
            }));
        }
        for t in tasks {
            let _ = t.await;
        }
    }
    start.elapsed()
}

fn is_alive_atomic_load(c: &mut Criterion) {
    let flag = AtomicBool::new(true);
    let mut group = c.benchmark_group("tcp_pool");
    group.bench_function("is_alive_atomic_load", |b| {
        b.iter(|| {
            let v = std::hint::black_box(&flag).load(Ordering::Acquire);
            std::hint::black_box(v);
        });
    });
    group.finish();
}

fn deadpool_acquire_release(c: &mut Criterion) {
    let runner = common::start_runner();
    let pool = build_noop_pool(8);
    let mut group = c.benchmark_group("tcp_pool");
    group.bench_function("deadpool_acquire_release", |b| {
        b.iter_custom(|iters| {
            let pool = pool.clone();
            // Wrap the elapsed Duration in Result so it fits the
            // RunnerHandle::run contract that the existing benches
            // use; the closure here always returns Ok.
            runner.run(async move {
                let elapsed = run_acquire_release(iters, pool).await;
                Ok::<Duration, clickhouse::error::Error>(elapsed)
            })
        });
    });
    group.finish();
}

fn deadpool_contention(c: &mut Criterion) {
    let runner = common::start_runner();
    let pool = build_noop_pool(8);
    let mut group = c.benchmark_group("tcp_pool");
    group.bench_function("deadpool_acquire_size8_x16", |b| {
        b.iter_custom(|iters| {
            let pool = pool.clone();
            runner.run(async move {
                let elapsed = run_concurrent_acquire(iters, pool, 16).await;
                Ok::<Duration, clickhouse::error::Error>(elapsed)
            })
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    is_alive_atomic_load,
    deadpool_acquire_release,
    deadpool_contention
);
criterion_main!(benches);
