//! `with_auto_query_id` per-query cost bench.
//!
//! Validates two claims in the rustdoc:
//! 1. `with_auto_query_id` is "off by default; zero overhead for
//!    callers who don't need it" -- measure both paths to anchor.
//! 2. When ON, the cost is a UUIDv7 generation + a small String
//!    alloc per `Client::query` call (one-time, not per row).
//!
//! Run:
//!   cargo bench --bench auto_query_id --features 'uuid test-util'
//!
//! No HTTP server needed -- `Client::query` returns a `Query` builder
//! without going on the wire. The bench measures only the
//! auto-UUIDv7-then-wrap path.

use clickhouse::Client;
use criterion::{Criterion, criterion_group, criterion_main};

fn make_client_with_url() -> Client {
    Client::default()
        .with_url("http://127.0.0.1:9999")
        .with_validation(false)
}

/// Baseline: `with_auto_query_id` NOT enabled. Measures the
/// no-overhead path.
fn query_no_auto_id(c: &mut Criterion) {
    let client = make_client_with_url();
    c.bench_function("query_no_auto_query_id", |b| {
        b.iter(|| {
            let _ = std::hint::black_box(
                client.query(std::hint::black_box("SELECT 1")),
            );
        });
    });
}

/// `with_auto_query_id` ON: per-query UUIDv7 + String alloc.
fn query_with_auto_id(c: &mut Criterion) {
    let client = make_client_with_url().with_auto_query_id();
    c.bench_function("query_with_auto_query_id", |b| {
        b.iter(|| {
            let _ = std::hint::black_box(
                client.query(std::hint::black_box("SELECT 1")),
            );
        });
    });
}

criterion_group!(benches, query_no_auto_id, query_with_auto_id);
criterion_main!(benches);
