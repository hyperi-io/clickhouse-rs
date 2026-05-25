//! AsyncInserter throughput / latency benches.
//!
//! Anchors the perf claims in the `async_inserter` module rustdoc:
//! bounded MPSC backpressure, cheap-clone write handle, end()
//! tail-latency. When citing numbers, quote criterion output
//! verbatim and record the host (CPU + rustc + allocator +
//! invocation line) so the comparison is reproducible.
//!
//! Run with:
//!   cargo bench --bench async_inserter --features 'inserter test-util'
//!
//! The bench drives writes against an in-process HTTP server that
//! drains the request body and replies 200 OK. The cost measured is
//! therefore the AsyncInserter dispatch path (channel + worker
//! resolve + serialise + commit threshold) plus the local-loopback
//! HTTP round-trip on flushes, NOT real ClickHouse server time.

use std::{
    convert::Infallible,
    future::Future,
    mem,
    net::SocketAddr,
    time::{Duration, Instant},
};

use bytes::Bytes;
use clickhouse::{
    Client, Row,
    async_inserter::{AsyncInserter, AsyncInserterConfig},
    error::Result,
};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use http_body_util::Full;
use hyper::{Request, Response, body::Incoming};
use serde::{Deserialize, Serialize};

mod common;

const ADDR: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)), 6543);

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
struct SampleRow {
    id: u64,
    payload: u64,
}

impl SampleRow {
    fn new(i: u64) -> Self {
        Self {
            id: i,
            payload: i.wrapping_mul(0x9E37_79B9_7F4A_7C15),
        }
    }
}

async fn drain_and_ok(request: Request<Incoming>) -> Response<Full<Bytes>> {
    common::skip_incoming(request).await;
    Response::new(Full::new(Bytes::new()))
}

fn make_client() -> Client {
    // Validation off: the in-process mock doesn't respond to the
    // DESCRIBE TABLE round-trip the validated path triggers. The
    // bench measures dispatch + write cost, not schema resolution.
    Client::default()
        .with_url(format!("http://{ADDR}"))
        .with_validation(false)
}

// ---------------------------------------------------------------------------
// bench: single-handle write throughput
// ---------------------------------------------------------------------------

async fn run_single_handle(iters: u64) -> Result<Duration> {
    let _server = common::start_server(ADDR, drain_and_ok).await;
    let client = make_client();
    // Disable period flushes so the cost we measure is the per-write
    // dispatch, not timer wakeups. max_rows forces at-least-one
    // threshold flush within the bench so end() tail latency does
    // not dominate.
    let config = AsyncInserterConfig::default()
        .without_period()
        .with_max_rows(iters.max(1024));
    let inserter: AsyncInserter<SampleRow> =
        AsyncInserter::new(&client, "bench_table", config);

    let start = Instant::now();
    for i in 0..iters {
        inserter.write(SampleRow::new(i)).await?;
    }
    inserter.flush().await?;
    let elapsed = start.elapsed();
    let _ = inserter.end().await?;
    Ok(elapsed)
}

// ---------------------------------------------------------------------------
// bench: multi-handle concurrent write throughput
// ---------------------------------------------------------------------------

async fn run_multi_handle<const HANDLES: usize>(iters: u64) -> Result<Duration> {
    let _server = common::start_server(ADDR, drain_and_ok).await;
    let client = make_client();
    // Larger channel so concurrent producers don't backpressure each other.
    let config = AsyncInserterConfig::default()
        .without_period()
        .with_max_rows((iters * HANDLES as u64).max(1024))
        .with_channel_capacity(65536);
    let inserter: AsyncInserter<SampleRow> =
        AsyncInserter::new(&client, "bench_table", config);

    let start = Instant::now();
    let mut tasks = Vec::with_capacity(HANDLES);
    for h in 0..HANDLES as u64 {
        let handle = inserter.handle();
        let task = tokio::spawn(async move {
            for i in 0..iters {
                handle
                    .write(SampleRow::new(h * iters + i))
                    .await
                    .expect("write");
            }
        });
        tasks.push(task);
    }
    for t in tasks {
        t.await.expect("join");
    }
    inserter.flush().await?;
    let elapsed = start.elapsed();
    let _ = inserter.end().await?;
    Ok(elapsed)
}

// ---------------------------------------------------------------------------
// bench: end() tail latency with pending rows
// ---------------------------------------------------------------------------

async fn run_end_with_pending(iters: u64) -> Result<Duration> {
    let _server = common::start_server(ADDR, drain_and_ok).await;
    let client = make_client();
    // Thresholds high so no threshold-flush trips; rows pile up
    // until end() drains them in one final commit.
    let config = AsyncInserterConfig::default()
        .without_period()
        .with_max_rows(u64::MAX)
        .with_max_bytes(u64::MAX);
    let inserter: AsyncInserter<SampleRow> =
        AsyncInserter::new(&client, "bench_table", config);

    for i in 0..iters {
        inserter.write(SampleRow::new(i)).await?;
    }
    let start = Instant::now();
    let _ = inserter.end().await?;
    Ok(start.elapsed())
}

// ---------------------------------------------------------------------------
// bench: backpressure under burst
// ---------------------------------------------------------------------------

async fn run_backpressure(iters: u64) -> Result<Duration> {
    let _server = common::start_server(ADDR, drain_and_ok).await;
    let client = make_client();
    // Deliberately small channel to force producer awaits on the
    // bounded mpsc.
    let config = AsyncInserterConfig::default()
        .without_period()
        .with_max_rows(u64::MAX)
        .with_max_bytes(u64::MAX)
        .with_channel_capacity(32);
    let inserter: AsyncInserter<SampleRow> =
        AsyncInserter::new(&client, "bench_table", config);

    let start = Instant::now();
    for i in 0..iters {
        inserter.write(SampleRow::new(i)).await?;
    }
    let elapsed = start.elapsed();
    let _ = inserter.end().await?;
    Ok(elapsed)
}

// ---------------------------------------------------------------------------
// criterion wiring
// ---------------------------------------------------------------------------

fn single_handle(c: &mut Criterion) {
    let runner = common::start_runner();
    let mut group = c.benchmark_group("async_inserter_single_handle");
    group.throughput(Throughput::Bytes(mem::size_of::<SampleRow>() as u64));
    group.bench_function("write", |b| {
        b.iter_custom(|iters| runner.run(run_single_handle(iters)));
    });
    group.finish();
}

fn multi_handle(c: &mut Criterion) {
    let runner = common::start_runner();
    let mut group = c.benchmark_group("async_inserter_multi_handle");
    group.throughput(Throughput::Bytes(mem::size_of::<SampleRow>() as u64));
    group.bench_function("4_handles", |b| {
        b.iter_custom(|iters| runner.run(run_multi_handle::<4>(iters)));
    });
    group.bench_function("8_handles", |b| {
        b.iter_custom(|iters| runner.run(run_multi_handle::<8>(iters)));
    });
    group.finish();
}

fn end_with_pending(c: &mut Criterion) {
    let runner = common::start_runner();
    let mut group = c.benchmark_group("async_inserter_end");
    group.throughput(Throughput::Bytes(mem::size_of::<SampleRow>() as u64));
    group.bench_function("end_with_pending_rows", |b| {
        b.iter_custom(|iters| runner.run(run_end_with_pending(iters)));
    });
    group.finish();
}

fn backpressure(c: &mut Criterion) {
    let runner = common::start_runner();
    let mut group = c.benchmark_group("async_inserter_backpressure");
    group.throughput(Throughput::Bytes(mem::size_of::<SampleRow>() as u64));
    group.bench_function("bounded_channel_32", |b| {
        b.iter_custom(|iters| runner.run(run_backpressure(iters)));
    });
    group.finish();
}

criterion_group!(
    benches,
    single_handle,
    multi_handle,
    end_with_pending,
    backpressure
);
criterion_main!(benches);

// Anchor the future trait import so the wiring lines compile without
// noise; `tokio::spawn` already pulls Future into scope at call sites.
#[allow(dead_code)]
fn _future_anchor<F: Future<Output = Result<Duration>>>(_: F) {}

// `Infallible` brought in so the prelude pulls in the right types
// even if a future refactor drops other body trait impls.
#[allow(dead_code)]
const _: Option<Infallible> = None;
