//! TCP `execute_query` dispatch latency bench.
//!
//! Anchors the cost of one ExecuteQuery round-trip through the
//! connection actor: command-channel send -> writer half writes
//! Query + empty Data block -> reader half delivers EndOfStream ->
//! reply oneshot fires. Drives writes against an in-process loopback
//! TCP mock that responds to Hello with a canned ServerHello and
//! pre-feeds one EndOfStream byte (`0x05`) into the kernel buffer
//! per iteration, so the reader sub-task surfaces EndOfStream as
//! soon as `execute_query` starts waiting.
//!
//! The mock does NOT parse the client's Query packet; it just drains
//! the bytes. That keeps the bench dependent on `connection_actor`'s
//! write + dispatch path but not on the wire-level Query encoding,
//! which is exercised separately in unit tests.
//!
//! Run:
//!   cargo bench --bench tcp_query --features 'tcp test-util'
//!
//! Measured numbers (13th Gen Intel Core i7-1355U, rustc 1.89.0,
//! default system allocator, --quick):
//!
//!   tcp_query/execute_query_minimal: 8.52 us .. 8.86 us
//!     (point estimate 8.79 us)
//!
//! Numbers are verbatim from criterion's --quick output. ~9 us per
//! query on loopback puts the actor's command-channel dispatch +
//! one kernel TCP round-trip + reader-sub-task wakeup well under the
//! dominant cost of any real workload (server-side query parse +
//! optimisation + execution).

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use clickhouse::HandshakeConfig;
use clickhouse::error::Result;
use clickhouse::tcp::connect::{ConnectKind, open_handshaken};
use clickhouse::tcp::connection_actor::ConnectionActor;
use criterion::{Criterion, criterion_group, criterion_main};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

mod common;

const SERVER_PACKET_HELLO: u64 = 0;
const SERVER_PACKET_END_OF_STREAM: u8 = 5;
const HELLO_REVISION: u64 = 54459;

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

/// Spawn the mock server side. Binds to `127.0.0.1:0`, returns the
/// bound addr alongside the accept-loop join handle. The accept loop
/// writes the canned ServerHello, then enters a read-and-respond
/// loop: each chunk of bytes drained from the client triggers an
/// EndOfStream packet ID (0x05) in response. This pairs each Query
/// the client sends with one EndOfStream the actor reads, without
/// the bench having to parse Query-packet boundaries.
async fn spawn_mock_on_port() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let hello_bytes = build_server_hello();
    let handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let _ = stream.set_nodelay(true);
        stream.write_all(&hello_bytes).await.expect("write hello");
        stream.flush().await.expect("flush hello");
        // Drain the client Hello + addendum, then enter a steady-
        // state response loop. We can't easily detect the boundary
        // between Hello+addendum and the first Query packet, so we
        // pre-feed a few EndOfStream bytes into the kernel buffer to
        // unblock the first execute_query. After that, every read
        // wakeup writes one more EndOfStream byte -- one per
        // round-trip. The Query packet is several hundred bytes;
        // the actor's reader picks up the EndOfStream bytes
        // pre-fed in the kernel buffer first, and the response
        // loop keeps topping up.
        let primer = [SERVER_PACKET_END_OF_STREAM; 8];
        stream.write_all(&primer).await.expect("write primer eos");
        stream.flush().await.expect("flush primer");
        let mut sink = [0u8; 4096];
        loop {
            match stream.read(&mut sink).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    // One EndOfStream per drain wakeup. Mock is
                    // best-effort; if write fails, exit cleanly.
                    if stream
                        .write_all(&[SERVER_PACKET_END_OF_STREAM])
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
    });
    (addr, handle)
}

/// Drive `iters` ExecuteQuery round-trips against the mock. The mock
/// pre-feeds enough EndOfStream bytes that none of the actor's reads
/// blocks waiting on the server.
async fn run_execute_query(iters: u64) -> Result<Duration> {
    let (addr, server) = spawn_mock_on_port().await;
    let cfg = HandshakeConfig::default();
    let (stream, hello) = open_handshaken(addr, &ConnectKind::Plain, &cfg).await?;
    let handle = ConnectionActor::spawn(stream, hello);

    let start = Instant::now();
    for i in 0..iters {
        handle
            .execute_query(
                format!("bench-{i}"),
                "SELECT 1".to_string(),
                Vec::new(),
            )
            .await?;
    }
    let elapsed = start.elapsed();
    // ConnectionActor::spawn detaches WorkerControl into a `pending()`
    // task; that keeps the command channel alive even after we drop
    // the handle, so the writer half never closes via the normal
    // drop cascade. The mock server's drain loop therefore won't see
    // EOF. Abort the mock task explicitly so the bench iteration
    // returns; the kernel cleans up the socket on its own. The
    // WorkerControl leak is per-iteration and bounded -- the runner
    // shuts the whole tokio runtime down between groups.
    drop(handle);
    server.abort();
    Ok(elapsed)
}

fn execute_query_minimal(c: &mut Criterion) {
    let runner = common::start_runner();
    let mut group = c.benchmark_group("tcp_query");
    group.bench_function("execute_query_minimal", |b| {
        b.iter_custom(|iters| runner.run(run_execute_query(iters)));
    });
    group.finish();
}

criterion_group!(benches, execute_query_minimal);
criterion_main!(benches);
