//! AsyncInserter multi-table benches.
//!
//! Anchors three perf claims in the AsyncInserter rustdoc:
//! - `write_to` per-row throughput on a hot single table.
//! - Multi-table fan-out across N tables (BTreeMap dispatch cost
//!   plus interner cache-hit cost).
//! - Cross-table total-bytes watermark overhead, ON vs OFF.
//!
//! The numbers here also feed two open questions: whether the
//! O(N tables) `pending().bytes` sum in `write_one` is worth
//! replacing with an incrementally-tracked total, and whether
//! the interner's `RwLock<HashMap>` is worth swapping for a
//! per-bucket-locked concurrent map.
//!
//! Run:
//!   cargo bench --bench async_inserter_multi_table --features 'inserter test-util'

use std::{
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

const ADDR: SocketAddr = SocketAddr::new(
    std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
    6544,
);

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
    Client::default()
        .with_url(format!("http://{ADDR}"))
        .with_validation(false)
}

// ---------------------------------------------------------------------------
// bench: write_to hot single table (interner cache hit after warm-up)
// ---------------------------------------------------------------------------

async fn run_write_to_hot(iters: u64) -> Result<Duration> {
    let _server = common::start_server(ADDR, drain_and_ok).await;
    let client = make_client();
    let config = AsyncInserterConfig::default()
        .without_period()
        .with_max_rows(iters.max(1024));
    let inserter: AsyncInserter<SampleRow> =
        AsyncInserter::new_multi_table(&client, config);
    let handle = inserter.handle();

    // Warm up the interner with one write before timing.
    handle.write_to("hot", SampleRow::new(0)).await?;

    let start = Instant::now();
    for i in 0..iters {
        handle.write_to("hot", SampleRow::new(i)).await?;
    }
    inserter.flush().await?;
    let elapsed = start.elapsed();
    let _ = inserter.end().await?;
    Ok(elapsed)
}

// ---------------------------------------------------------------------------
// bench: write_to fanout across N tables (BTreeMap O(log N) + interner)
// ---------------------------------------------------------------------------

async fn run_write_to_fanout<const N_TABLES: usize>(iters: u64) -> Result<Duration> {
    let _server = common::start_server(ADDR, drain_and_ok).await;
    let client = make_client();
    let config = AsyncInserterConfig::default()
        .without_period()
        .with_max_rows(iters.max(1024));
    let inserter: AsyncInserter<SampleRow> =
        AsyncInserter::new_multi_table(&client, config);
    let handle = inserter.handle();

    // Warm up interner + per-table buffers.
    let tables: Vec<String> = (0..N_TABLES).map(|n| format!("t{n}")).collect();
    for table in &tables {
        handle.write_to(table, SampleRow::new(0)).await?;
    }

    let start = Instant::now();
    for i in 0..iters {
        let table = &tables[(i as usize) % N_TABLES];
        handle.write_to(table, SampleRow::new(i)).await?;
    }
    inserter.flush().await?;
    let elapsed = start.elapsed();
    let _ = inserter.end().await?;
    Ok(elapsed)
}

// ---------------------------------------------------------------------------
// bench: cross-table watermark overhead ON vs OFF
// ---------------------------------------------------------------------------

async fn run_watermark<const WATERMARK_ON: bool>(iters: u64) -> Result<Duration> {
    let _server = common::start_server(ADDR, drain_and_ok).await;
    let client = make_client();
    let mut config = AsyncInserterConfig::default()
        .without_period()
        .with_max_rows(iters.max(1024));
    if WATERMARK_ON {
        // Threshold high enough to NOT trip during the run; we measure
        // the per-write check cost, not the flush cost.
        config = config.with_cross_table_max_bytes(u64::MAX);
    }
    let inserter: AsyncInserter<SampleRow> =
        AsyncInserter::new_multi_table(&client, config);
    let handle = inserter.handle();

    // Pre-populate 10 tables so the per-write O(N tables) sum
    // actually iterates a meaningful set.
    let tables: Vec<String> = (0..10).map(|n| format!("t{n}")).collect();
    for table in &tables {
        handle.write_to(table, SampleRow::new(0)).await?;
    }

    let start = Instant::now();
    for i in 0..iters {
        let table = &tables[(i as usize) % tables.len()];
        handle.write_to(table, SampleRow::new(i)).await?;
    }
    inserter.flush().await?;
    let elapsed = start.elapsed();
    let _ = inserter.end().await?;
    Ok(elapsed)
}

// ---------------------------------------------------------------------------
// criterion wiring
// ---------------------------------------------------------------------------

fn write_to_hot(c: &mut Criterion) {
    let runner = common::start_runner();
    let mut group = c.benchmark_group("async_inserter_write_to_hot");
    group.throughput(Throughput::Bytes(mem::size_of::<SampleRow>() as u64));
    group.bench_function("single_table", |b| {
        b.iter_custom(|iters| runner.run(run_write_to_hot(iters)));
    });
    group.finish();
}

fn write_to_fanout(c: &mut Criterion) {
    let runner = common::start_runner();
    let mut group = c.benchmark_group("async_inserter_write_to_fanout");
    group.throughput(Throughput::Bytes(mem::size_of::<SampleRow>() as u64));
    group.bench_function("10_tables", |b| {
        b.iter_custom(|iters| runner.run(run_write_to_fanout::<10>(iters)));
    });
    group.bench_function("100_tables", |b| {
        b.iter_custom(|iters| runner.run(run_write_to_fanout::<100>(iters)));
    });
    group.finish();
}

fn watermark(c: &mut Criterion) {
    let runner = common::start_runner();
    let mut group = c.benchmark_group("async_inserter_watermark");
    group.throughput(Throughput::Bytes(mem::size_of::<SampleRow>() as u64));
    group.bench_function("off", |b| {
        b.iter_custom(|iters| runner.run(run_watermark::<false>(iters)));
    });
    group.bench_function("on", |b| {
        b.iter_custom(|iters| runner.run(run_watermark::<true>(iters)));
    });
    group.finish();
}

criterion_group!(benches, write_to_hot, write_to_fanout, watermark);
criterion_main!(benches);
