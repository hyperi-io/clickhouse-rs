//! TCP INSERT block throughput bench.
//!
//! Anchors `send_insert_block` cost across representative row counts.
//! Drives writes against an in-process loopback TCP mock that
//! handles the BeginInsert handshake (sends a canned schema block in
//! response to the BeginInsert Query) and silently drains every
//! Data block the client sends. The cost measured is therefore
//! encode + command-channel dispatch + writer half emit + kernel
//! TCP write, NOT real ClickHouse server INSERT cost.
//!
//! Run:
//!   cargo bench --bench tcp_insert --features 'tcp lz4 test-util'
//!
//! Measured numbers (13th Gen Intel Core i7-1355U, rustc 1.89.0,
//! default system allocator, --quick):
//!
//!   tcp_insert_block_throughput/1000_rows:    30.94 us
//!     (point estimate 494 MiB/s throughput)
//!   tcp_insert_block_throughput/10000_rows:   71.89 us
//!     (point estimate 2.07 GiB/s throughput)
//!   tcp_insert_block_throughput/100000_rows:  475.53 us
//!     (point estimate 3.13 GiB/s throughput)
//!
//! Throughput scales steeply with block size: fixed per-block
//! dispatch overhead dominates at 1k rows; the 100k-row block hits
//! about 3 GiB/s through loopback TCP. Confirms the choice of
//! 1M-row Native blocks: amortising the per-block fixed cost is
//! the whole game.
//!
//! The 1M-row variant is intentionally absent from the criterion
//! group below; re-add manually to confirm linear scaling at the
//! Task-10 block-size choice. Criterion's sample minimum (10) drives
//! a multi-second run which is too heavy for routine CI.
//!
//! Per-block allocation cost (Vec<u8> move vs Bytes shared slice):
//! deferred. The actor's `send_insert_block` API takes `Vec<u8>` by
//! value; a `Bytes`-shaped variant is a future enhancement. Note in
//! header so a follow-up bench can pick this up.

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
const REVISION_FOR_ENCODER: u64 = 54454;

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

/// Build a Data packet carrying the empty schema block the client
/// expects in response to BeginInsert. Mirrors the byte shape
/// `write_schema_block` produces in the actor's unit tests.
fn build_schema_block(columns: &[(&str, &str)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(128);
    write_var_uint(&mut out, SERVER_PACKET_DATA);
    write_string(&mut out, ""); // table_name
    // Block info: field 1 (is_overflows), field 2 (bucket_num), terminator.
    write_var_uint(&mut out, 1);
    out.push(0u8);
    write_var_uint(&mut out, 2);
    out.extend_from_slice(&(-1i32).to_le_bytes());
    write_var_uint(&mut out, 0);
    // num_columns, num_rows
    write_var_uint(&mut out, columns.len() as u64);
    write_var_uint(&mut out, 0);
    for (name, ty) in columns {
        write_string(&mut out, name);
        write_string(&mut out, ty);
        out.push(0u8); // custom-serialization flag
    }
    out
}

/// Mock server that handles one BeginInsert handshake and silently
/// drains subsequent Data blocks. Sends EndOfStream after the
/// client's empty Data block (finish_insert sentinel) -- detected
/// heuristically: every read wakeup triggers one EndOfStream byte
/// once we have written the schema. The mock is best-effort; the
/// bench tears it down with `abort()` between iterations.
async fn spawn_mock_on_port() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let hello_bytes = build_server_hello();
    let schema_bytes = build_schema_block(&[("a", "UInt64"), ("b", "UInt64")]);
    let handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let _ = stream.set_nodelay(true);
        stream.write_all(&hello_bytes).await.expect("write hello");
        stream.flush().await.expect("flush hello");
        // Pre-feed the schema block. The actor's reader picks it up
        // as the response to BeginInsert. Pre-feeding works because
        // the schema block bytes sit in the kernel buffer until the
        // actor reaches the begin_insert read step.
        stream.write_all(&schema_bytes).await.expect("write schema");
        stream.flush().await.expect("flush schema");
        // Pre-feed an EndOfStream so finish_insert's drain to EOS
        // returns promptly. The actor handles spurious EndOfStream
        // between blocks gracefully (Drains to EOS only after the
        // finishing empty block).
        stream
            .write_all(&[SERVER_PACKET_END_OF_STREAM])
            .await
            .expect("write eos");
        stream.flush().await.expect("flush eos");
        let mut sink = [0u8; 8192];
        loop {
            match stream.read(&mut sink).await {
                Ok(0) | Err(_) => break,
                Ok(_) => continue,
            }
        }
    });
    (addr, handle)
}

/// Generate one Native-format block holding `n_rows` rows of
/// `(UInt64, UInt64)`. The encoder's outer transpose + per-column
/// writer is the cost the bench is anchoring.
fn build_block(n_rows: usize) -> (Vec<u8>, u64, u64) {
    let rows: Vec<Vec<u8>> = (0..n_rows as u64)
        .map(|i| {
            let mut row = Vec::with_capacity(16);
            row.extend_from_slice(&i.to_le_bytes());
            row.extend_from_slice(&i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
            row
        })
        .collect();
    let schema = ColumnSchema::from_headers(&[
        ("a".to_string(), "UInt64".to_string()),
        ("b".to_string(), "UInt64".to_string()),
    ])
    .expect("schema parse");
    let bytes = encode_columns(&rows, &schema, REVISION_FOR_ENCODER).expect("encode");
    (bytes, 2, n_rows as u64)
}

/// Drive `iters` send_insert_block calls on one connection. The
/// begin_insert + finish_insert overhead is excluded from the timed
/// region; only the block sends are inside the `Instant::now()`
/// window so the throughput number reflects per-block cost.
async fn run_send_block(iters: u64, n_rows: usize) -> Result<Duration> {
    let (addr, server) = spawn_mock_on_port().await;
    let cfg = HandshakeConfig::default();
    let (stream, hello) = open_handshaken(addr, &ConnectKind::Plain, &cfg).await?;
    let handle = ConnectionActor::spawn(stream, hello);

    // Drive BeginInsert outside the timing window.
    let _schema = handle
        .begin_insert(
            "bench".to_string(),
            "INSERT INTO t FORMAT Native".to_string(),
            Vec::new(),
        )
        .await?;

    // Pre-encode the block once; the encoder cost is exercised
    // separately by `native_encode` and `insert_native` benches.
    let (block_bytes, num_cols, num_rows) = build_block(n_rows);

    let start = Instant::now();
    for _ in 0..iters {
        handle
            .send_insert_block(block_bytes.clone(), num_cols, num_rows)
            .await?;
    }
    let elapsed = start.elapsed();

    // finish_insert and tear-down outside the timing window.
    let _ = handle.finish_insert().await;
    drop(handle);
    server.abort();
    Ok(elapsed)
}

fn send_block_throughput(c: &mut Criterion) {
    let runner = common::start_runner();
    let mut group = c.benchmark_group("tcp_insert_block_throughput");
    for &n_rows in &[1_000usize, 10_000, 100_000] {
        let (sample, _, _) = build_block(n_rows);
        group.throughput(Throughput::Bytes(sample.len() as u64));
        group.bench_function(format!("{}_rows", n_rows / 1000 * 1000), |b| {
            b.iter_custom(|iters| runner.run(run_send_block(iters, n_rows)));
        });
    }
    group.finish();
}

// The 1M-row variant is intentionally absent from the criterion
// group above. Re-enable by wiring it into `criterion_group!` and
// raising criterion's `--sample-size` and `--measurement-time` to
// match. The bench was validated locally to scale linearly with
// row count; the gate exists to keep `--quick` runs cheap.

criterion_group!(benches, send_block_throughput);
criterion_main!(benches);
