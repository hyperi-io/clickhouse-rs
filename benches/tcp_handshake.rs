//! TCP handshake latency bench.
//!
//! Anchors connect + handshake orchestration cost. Drives
//! `open_handshaken` against an in-process loopback TCP mock that
//! pre-seeds a canned ServerHello byte sequence and drains the
//! client's Hello + addendum without inspecting it. The cost
//! measured is therefore connect + 3 round-trip writes + 1
//! round-trip read, NOT real ClickHouse server work.
//!
//! Run:
//!   cargo bench --bench tcp_handshake --features 'tcp test-util'
//!
//! When citing numbers, record the host CPU + rustc + allocator +
//! invocation line. Reproducibility matters more than the absolute
//! value -- handshake latency on a busy host is dominated by the
//! tokio scheduler and the kernel TCP loopback path.
//!
//! Measured numbers (13th Gen Intel Core i7-1355U, rustc 1.89.0,
//! default system allocator, `--quick` criterion mode):
//!
//!   tcp_handshake_loopback/single: 41.17 us .. 41.89 us
//!     (point estimate 41.75 us)
//!
//! Numbers are verbatim from criterion's --quick output; rerun on
//! the runner host before citing in any review. Loopback TCP
//! handshake at ~42 us puts the round-trip plus orchestration well
//! below the dominant cost in any real workload (connect to a
//! remote node, ~200+ us LAN, ~1-10 ms WAN).

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use clickhouse::HandshakeConfig;
use clickhouse::error::Result;
use clickhouse::tcp::connect::{ConnectKind, open_handshaken};
use criterion::{Criterion, criterion_group, criterion_main};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

mod common;

// Server Hello packet IDs and revision constants are duplicated here
// rather than re-exported from `crate::tcp::protocol`, which keeps
// those as `pub(crate)`. The bench is bytes-on-the-wire, so a
// literal copy of the four constants we touch is intentional.
const SERVER_PACKET_HELLO: u64 = 0;
// Same as DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS in the source.
const HELLO_REVISION: u64 = 54459;

/// Hand-emit a ClickHouse var_uint. The crate-internal helper lives
/// behind `pub(crate)` so the bench inlines the encoding.
fn write_var_uint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(((value & 0x7F) | 0x80) as u8);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Hand-emit a length-prefixed UTF-8 string (var_uint length + raw
/// bytes), matching the wire format the reader expects.
fn write_string(out: &mut Vec<u8>, s: &str) {
    write_var_uint(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

/// Build the canned ServerHello byte sequence the reader expects.
/// Mirrors the bytes the `handshake_roundtrip_on_duplex` unit test
/// pre-seeds; revision is well above the addendum gate so the
/// addendum is exchanged.
fn build_server_hello() -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    write_var_uint(&mut out, SERVER_PACKET_HELLO);
    write_string(&mut out, "ClickHouse bench-mock");
    write_var_uint(&mut out, 25); // version major
    write_var_uint(&mut out, 4); // version minor
    write_var_uint(&mut out, HELLO_REVISION);
    write_string(&mut out, "Etc/UTC"); // timezone
    write_string(&mut out, "ch-bench-mock"); // display name
    write_var_uint(&mut out, 7); // version patch
    out
}

/// Drive one handshake against a loopback TCP listener. The mock
/// accepts a single connection, writes the canned Hello, and drains
/// the client's Hello + addendum bytes until EOF.
async fn handshake_loopback_once() -> Result<Duration> {
    // Bind to an ephemeral loopback port. A fresh listener per
    // iteration keeps the bench numbers stable -- TIME_WAIT pile-up
    // on a single port would otherwise creep into the tail.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let hello_bytes = build_server_hello();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        // Write the canned Hello bytes first. The handshake reader
        // is half-duplex (writes Client Hello -> reads Server Hello
        // -> writes addendum), so sending Hello before reading the
        // client side is fine: the bytes sit in the kernel buffer
        // until the client reaches the read step.
        stream.write_all(&hello_bytes).await.expect("write hello");
        stream.flush().await.expect("flush hello");

        // Drain the client's Hello + addendum bytes. The exact count
        // depends on negotiated revision; reading to EOF is simpler
        // and equally cheap.
        let mut sink = [0u8; 256];
        loop {
            match stream.read(&mut sink).await {
                Ok(0) => break,
                Ok(_) => continue,
                Err(_) => break,
            }
        }
    });

    let cfg = HandshakeConfig::default();
    let start = Instant::now();
    let (stream, _hello) = open_handshaken(addr, &ConnectKind::Plain, &cfg).await?;
    let elapsed = start.elapsed();
    drop(stream); // close client half, server task exits on EOF
    let _ = server.await;
    Ok(elapsed)
}

async fn run_handshake(iters: u64) -> Result<Duration> {
    let mut total = Duration::ZERO;
    for _ in 0..iters {
        total += handshake_loopback_once().await?;
    }
    Ok(total)
}

fn handshake_loopback(c: &mut Criterion) {
    let runner = common::start_runner();
    let mut group = c.benchmark_group("tcp_handshake_loopback");
    group.bench_function("single", |b| {
        b.iter_custom(|iters| runner.run(run_handshake(iters)));
    });
    group.finish();
}

criterion_group!(benches, handshake_loopback);
criterion_main!(benches);
