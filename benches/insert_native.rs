//! `InsertNative` end-to-end benches.
//!
//! Anchors the cost of buffering rows + transposing them into a
//! Native columnar block + shipping the block over HTTP. Drives
//! writes against an in-process drain-and-OK HTTP mock; the cost
//! measured is the InsertNative path PLUS the local-loopback HTTP
//! round-trip, NOT real ClickHouse server time.
//!
//! On 05c-http-native-format-new (this branch) the encoder is the
//! baseline. The same bench file cascades into
//! 05c-chunked-blocks-new (adds chunked-flush threshold cost) and
//! 05c-encode-zero-copy-new (zero-copy encoder makes the end() leg
//! faster). Reviewers run on each tip and compare.
//!
//! Run:
//!   cargo bench --bench insert_native --features 'lz4 test-util'

use std::{
    mem,
    net::SocketAddr,
    time::{Duration, Instant},
};

use bytes::Bytes;
use clickhouse::{Client, Row, error::Result, insert_native::InsertNative};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use http_body_util::Full;
use hyper::{Request, Response, body::Incoming};
use serde::{Deserialize, Serialize};

mod common;

const ADDR: SocketAddr = SocketAddr::new(
    std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
    6545,
);

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
struct SampleRow {
    id: u64,
    name: String,
}

impl SampleRow {
    fn new(i: u64) -> Self {
        Self {
            id: i,
            name: format!("user-{i:06}"),
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

fn schema() -> Vec<(String, String)> {
    vec![
        ("id".to_string(), "UInt64".to_string()),
        ("name".to_string(), "String".to_string()),
    ]
}

// ---------------------------------------------------------------------------
// bench: write() per-row buffer cost (no flush)
// ---------------------------------------------------------------------------

async fn run_write_buffer(iters: u64) -> Result<Duration> {
    let _server = common::start_server(ADDR, drain_and_ok).await;
    let client = make_client();
    let cols = schema();
    let mut insert: InsertNative<SampleRow> =
        InsertNative::with_columns(&client, "bench_table", &cols)?;

    let start = Instant::now();
    for i in 0..iters {
        insert.write(&SampleRow::new(i))?;
    }
    let elapsed = start.elapsed();
    insert.end().await?;
    Ok(elapsed)
}

// ---------------------------------------------------------------------------
// bench: end() end-to-end (write N + encode + ship)
// ---------------------------------------------------------------------------

async fn run_end_to_end<const N: usize>(iters: u64) -> Result<Duration> {
    let _server = common::start_server(ADDR, drain_and_ok).await;
    let client = make_client();
    let cols = schema();

    let start = Instant::now();
    for _ in 0..iters {
        let mut insert: InsertNative<SampleRow> =
            InsertNative::with_columns(&client, "bench_table", &cols)?;
        for i in 0..N as u64 {
            insert.write(&SampleRow::new(i))?;
        }
        insert.end().await?;
    }
    Ok(start.elapsed())
}

// ---------------------------------------------------------------------------
// criterion wiring
// ---------------------------------------------------------------------------

fn write_buffer(c: &mut Criterion) {
    let runner = common::start_runner();
    let mut group = c.benchmark_group("insert_native_write_buffer");
    group.throughput(Throughput::Bytes(mem::size_of::<SampleRow>() as u64));
    group.bench_function("per_row", |b| {
        b.iter_custom(|iters| runner.run(run_write_buffer(iters)));
    });
    group.finish();
}

fn end_to_end(c: &mut Criterion) {
    let runner = common::start_runner();
    let mut group = c.benchmark_group("insert_native_end_to_end");
    group.bench_function("100_rows_per_call", |b| {
        b.iter_custom(|iters| runner.run(run_end_to_end::<100>(iters)));
    });
    group.bench_function("10000_rows_per_call", |b| {
        b.iter_custom(|iters| runner.run(run_end_to_end::<10_000>(iters)));
    });
    group.finish();
}

criterion_group!(benches, write_buffer, end_to_end);
criterion_main!(benches);
