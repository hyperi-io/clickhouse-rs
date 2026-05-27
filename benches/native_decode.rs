//! Native columnar decode throughput bench.
//!
//! Anchors per-column decode cost across the four type families that
//! dominate real workloads: scalar UInt32, variable-width String,
//! LowCardinality(String) with a small dictionary, and Array(UInt64).
//! Drives reads against an in-process loopback TCP mock that streams
//! pre-built Data packets so the time measured is dominated by the
//! decode path (`crate::native::decode::decode_block` plus the
//! actor's reader-loop scheduling), not the kernel read.
//!
//! This bench is load-bearing for the scalar numeric decode hot path.
//! The numeric columns decode in bulk -- one `read_exact` of the whole
//! column followed by a single `from_le_bytes` conversion pass (see
//! `read_le_column` in `crate::native::decode`) -- instead of one
//! async call per element. This bench is what that decision is
//! measured against. Re-run it on a QUIET host: a loaded or shared box
//! shows large (observed ~2x) run-to-run variance on the
//! sub-millisecond columns, which swamps the decode-loop difference,
//! so trust only same-host, low-variance comparisons.
//!
//! Run:
//!   cargo bench --bench native_decode --features 'tcp lz4 test-util'
//!
//! Baseline numbers below are the PRE-bulk-decode (per-element)
//! figures on a quiet 13th Gen Intel Core i7-1355U, rustc 1.89.0,
//! default system allocator, --quick. They are the "before" the
//! bulk-decode change improves on; the UInt32 column is the clearest
//! signal because it is pure scalar decode:
//!
//!   native_decode/uint32_100k_rows:           573.67 us
//!     (point estimate 665 MiB/s payload throughput)
//!   native_decode/string_100k_rows:           5.64 ms
//!     (point estimate 186 MiB/s payload throughput)
//!   native_decode/low_cardinality_100k_rows:  25.34 us
//!     (point estimate 3.69 GiB/s payload throughput)
//!   native_decode/array_uint64_10k_rows_x10:  728.88 us
//!     (point estimate 1.12 GiB/s payload throughput)
//!
//! UInt32 at ~570 us per 100k rows = ~175M rows/sec single-thread on
//! the per-element path. Bulk decode removes the ~100k per-element
//! await points and lowers the conversion to a `memcpy` on
//! little-endian targets; the UInt32 column is where the win shows up
//! most directly. String is dominated by the varint length parse plus
//! per-string `Vec<u8>` allocation (the bulk change does not touch
//! it). LowCardinality with a 50-entry dictionary uses one-byte
//! indices (the read_exact path, unchanged). Array(UInt64) is the
//! bulk change applied to the inner u64 child column.

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use clickhouse::HandshakeConfig;
use clickhouse::error::Result;
use clickhouse::native::{ColumnSchema, encode_columns};
use clickhouse::tcp::connect::{ConnectKind, open_handshaken};
use clickhouse::tcp::connection_actor::ConnectionActor;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

mod common;

const SERVER_PACKET_HELLO: u64 = 0;
const SERVER_PACKET_DATA: u64 = 1;
const SERVER_PACKET_END_OF_STREAM: u8 = 5;
const HELLO_REVISION: u64 = 54459;
const REVISION_FOR_ENCODER: u64 = 54459;

fn write_var_uint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(((value & 0x7F) | 0x80) as u8);
        value >>= 7;
    }
    out.push(value as u8);
}

fn write_string(out: &mut Vec<u8>, s: &str) {
    write_var_uint(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

fn build_server_hello() -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    write_var_uint(&mut out, SERVER_PACKET_HELLO);
    write_string(&mut out, "ClickHouse bench-mock");
    write_var_uint(&mut out, 25);
    write_var_uint(&mut out, 4);
    write_var_uint(&mut out, HELLO_REVISION);
    write_string(&mut out, "Etc/UTC");
    write_string(&mut out, "ch-bench-mock");
    write_var_uint(&mut out, 7);
    out
}

/// Build a Data packet ready to ship to the actor. `column_bytes`
/// is the per-column body produced by `encode_columns`. The function
/// prepends the packet ID, empty table name, block info, and the
/// num_columns / num_rows header so the bytes form a complete
/// Data-with-rows packet.
fn build_data_packet(num_columns: u64, num_rows: u64, column_bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(column_bytes.len() + 64);
    write_var_uint(&mut out, SERVER_PACKET_DATA);
    write_string(&mut out, ""); // table_name
    // Block info: (1, 0u8), (2, -1i32), 0 terminator.
    write_var_uint(&mut out, 1);
    out.push(0u8);
    write_var_uint(&mut out, 2);
    out.extend_from_slice(&(-1i32).to_le_bytes());
    write_var_uint(&mut out, 0);
    write_var_uint(&mut out, num_columns);
    write_var_uint(&mut out, num_rows);
    out.extend_from_slice(column_bytes);
    out
}

/// Spawn a mock server that streams the same pre-built Data packet
/// `n_blocks` times before sending EndOfStream. The actor's
/// streaming SELECT consumes them as a sequence of `DecodedBlock`s.
async fn spawn_mock_streaming(
    blocks: Vec<u8>,
    n_blocks: usize,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let hello_bytes = build_server_hello();
    let handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let _ = stream.set_nodelay(true);
        stream.write_all(&hello_bytes).await.expect("write hello");
        stream.flush().await.expect("flush hello");

        // Spawn a drain task in parallel so the writer side does
        // not block on filled kernel buffers while we ship a large
        // body.
        let (mut rh, mut wh) = stream.into_split();
        let drain = tokio::spawn(async move {
            let mut sink = [0u8; 8192];
            loop {
                match rh.read(&mut sink).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
        });

        for _ in 0..n_blocks {
            if wh.write_all(&blocks).await.is_err() {
                break;
            }
        }
        let _ = wh
            .write_all(&[SERVER_PACKET_END_OF_STREAM])
            .await;
        let _ = wh.flush().await;
        drop(wh);
        let _ = drain.await;
    });
    (addr, handle)
}

/// Build one Data packet covering N rows of UInt32, ready to ship.
/// Returns the packet bytes plus the schema (used for the
/// throughput accounting).
fn block_uint32(n: usize) -> Vec<u8> {
    let rows: Vec<Vec<u8>> = (0..n as u32)
        .map(|i| i.to_le_bytes().to_vec())
        .collect();
    let schema = ColumnSchema::from_headers(&[("x".to_string(), "UInt32".to_string())])
        .expect("schema");
    let body = encode_columns(&rows, &schema, REVISION_FOR_ENCODER).expect("encode");
    build_data_packet(1, n as u64, &body)
}

/// One Data packet of N String rows; ~10 bytes per string.
fn block_string(n: usize) -> Vec<u8> {
    let rows: Vec<Vec<u8>> = (0..n as u64)
        .map(|i| {
            let s = format!("s-{i:08}");
            let mut row = Vec::with_capacity(s.len() + 2);
            write_var_uint(&mut row, s.len() as u64);
            row.extend_from_slice(s.as_bytes());
            row
        })
        .collect();
    let schema = ColumnSchema::from_headers(&[("s".to_string(), "String".to_string())])
        .expect("schema");
    let body = encode_columns(&rows, &schema, REVISION_FOR_ENCODER).expect("encode");
    build_data_packet(1, n as u64, &body)
}

/// One Data packet of N LowCardinality(String) rows with a 50-entry
/// dictionary. Indexes cycle through the dictionary so the decoder
/// exercises the index-decode path as well as the dictionary block.
fn block_low_cardinality(n: usize) -> Vec<u8> {
    const DICT_SIZE: usize = 50;
    // Each row is a String -- encode_columns understands LowCard via
    // the same RowBinary input format (the columnar encoder will
    // de-duplicate strings into a dictionary on output).
    let rows: Vec<Vec<u8>> = (0..n as u64)
        .map(|i| {
            let s = format!("tag-{:02}", i % DICT_SIZE as u64);
            let mut row = Vec::with_capacity(s.len() + 2);
            write_var_uint(&mut row, s.len() as u64);
            row.extend_from_slice(s.as_bytes());
            row
        })
        .collect();
    let schema = ColumnSchema::from_headers(&[(
        "lc".to_string(),
        "LowCardinality(String)".to_string(),
    )])
    .expect("schema");
    let body = encode_columns(&rows, &schema, REVISION_FOR_ENCODER).expect("encode");
    build_data_packet(1, n as u64, &body)
}

/// One Data packet of N Array(UInt64) rows, 10 elements per row.
fn block_array_uint64(n: usize) -> Vec<u8> {
    let elements_per_row: u64 = 10;
    let rows: Vec<Vec<u8>> = (0..n as u64)
        .map(|i| {
            let mut row = Vec::with_capacity(8 + (elements_per_row as usize) * 8);
            write_var_uint(&mut row, elements_per_row);
            for j in 0..elements_per_row {
                row.extend_from_slice(&(i + j).to_le_bytes());
            }
            row
        })
        .collect();
    let schema = ColumnSchema::from_headers(&[("a".to_string(), "Array(UInt64)".to_string())])
        .expect("schema");
    let body = encode_columns(&rows, &schema, REVISION_FOR_ENCODER).expect("encode");
    build_data_packet(1, n as u64, &body)
}

/// Drive `iters` next_block() calls on a cursor backed by a mock
/// that streams `iters` copies of `block_bytes`. The handshake +
/// cursor creation run outside the timing window so the cost
/// measured is the steady-state next_block round-trip.
async fn run_next_block(iters: u64, block_bytes: Vec<u8>) -> Result<Duration> {
    let (addr, server) = spawn_mock_streaming(block_bytes, iters as usize).await;
    let cfg = HandshakeConfig::default();
    let (stream, hello) = open_handshaken(addr, &ConnectKind::Plain, &cfg).await?;
    let handle = ConnectionActor::spawn(stream, hello);

    let mut cursor = handle
        .execute_stream_cursor(
            "bench".to_string(),
            "SELECT bench".to_string(),
            Vec::new(),
        )
        .await?;

    let start = Instant::now();
    for _ in 0..iters {
        let block = cursor.next_block().await?;
        if block.is_none() {
            // Mock ran out early -- not a measurement failure,
            // just shorter-than-expected. Account for the partial
            // iteration in the elapsed time.
            break;
        }
    }
    let elapsed = start.elapsed();

    drop(cursor);
    drop(handle);
    server.abort();
    Ok(elapsed)
}

fn uint32_decode(c: &mut Criterion) {
    let runner = common::start_runner();
    let block = block_uint32(100_000);
    let payload_size = block.len() as u64;
    let mut group = c.benchmark_group("native_decode");
    group.throughput(Throughput::Bytes(payload_size));
    group.bench_function("uint32_100k_rows", |b| {
        let block = block.clone();
        b.iter_custom(|iters| runner.run(run_next_block(iters, block.clone())));
    });
    group.finish();
}

fn string_decode(c: &mut Criterion) {
    let runner = common::start_runner();
    let block = block_string(100_000);
    let payload_size = block.len() as u64;
    let mut group = c.benchmark_group("native_decode");
    group.throughput(Throughput::Bytes(payload_size));
    group.bench_function("string_100k_rows", |b| {
        let block = block.clone();
        b.iter_custom(|iters| runner.run(run_next_block(iters, block.clone())));
    });
    group.finish();
}

fn low_cardinality_decode(c: &mut Criterion) {
    let runner = common::start_runner();
    let block = block_low_cardinality(100_000);
    let payload_size = block.len() as u64;
    let mut group = c.benchmark_group("native_decode");
    group.throughput(Throughput::Bytes(payload_size));
    group.bench_function("low_cardinality_100k_rows", |b| {
        let block = block.clone();
        b.iter_custom(|iters| runner.run(run_next_block(iters, block.clone())));
    });
    group.finish();
}

fn array_uint64_decode(c: &mut Criterion) {
    let runner = common::start_runner();
    let block = block_array_uint64(10_000);
    let payload_size = block.len() as u64;
    let mut group = c.benchmark_group("native_decode");
    group.throughput(Throughput::Bytes(payload_size));
    group.bench_function("array_uint64_10k_rows_x10", |b| {
        let block = block.clone();
        b.iter_custom(|iters| runner.run(run_next_block(iters, block.clone())));
    });
    group.finish();
}

criterion_group!(
    benches,
    uint32_decode,
    string_decode,
    low_cardinality_decode,
    array_uint64_decode
);
criterion_main!(benches);
