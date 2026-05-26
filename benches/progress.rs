//! `X-ClickHouse-Progress` header parser benches.
//!
//! Anchors the per-call cost of `progress::Progress::from_header_value`.
//! The parser is hand-rolled to dodge a `serde_json` allocation per
//! header; this bench measures whether that pay-off is still worth it
//! and sets a baseline for any future SIMD / unrolled-parse follow-up.
//!
//! Run:
//!   cargo bench --bench progress --features test-util
//!
//! When citing numbers, record CPU + rustc + allocator + invocation
//! verbatim.

use clickhouse::progress::Progress;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

/// Typical SELECT progress header (single update). Quoted-numeric
/// JSON; six known fields; representative of the steady-state shape
/// CH 24.x / 25.x emits.
const TYPICAL_SELECT: &str =
    r#"{"read_rows":"100","read_bytes":"2048","total_rows_to_read":"1000","written_rows":"0","written_bytes":"0","elapsed_ns":"500000"}"#;

/// Long-running streaming SELECT mid-flight. Values approach u64
/// max-digit width, exercising the digit-walk in `parse::<u64>()`.
const LONG_STREAMING: &str =
    r#"{"read_rows":"9876543210","read_bytes":"12345678901234","total_rows_to_read":"99999999999","written_rows":"0","written_bytes":"0","elapsed_ns":"54321098765"}"#;

/// INSERT-side progress (read_rows zero, written_* populated).
const INSERT_PROGRESS: &str =
    r#"{"read_rows":"0","read_bytes":"0","total_rows_to_read":"0","written_rows":"500","written_bytes":"4096","elapsed_ns":"100000"}"#;

/// Malformed input: parser should short-circuit to None.
const MALFORMED: &str = r#"{"read_rows":not_quoted}"#;

fn parse_typical(c: &mut Criterion) {
    let mut group = c.benchmark_group("progress_parse");
    group.throughput(Throughput::Bytes(TYPICAL_SELECT.len() as u64));
    group.bench_function("typical_select", |b| {
        b.iter(|| Progress::from_header_value(std::hint::black_box(TYPICAL_SELECT)));
    });
    group.throughput(Throughput::Bytes(LONG_STREAMING.len() as u64));
    group.bench_function("long_streaming_select", |b| {
        b.iter(|| Progress::from_header_value(std::hint::black_box(LONG_STREAMING)));
    });
    group.throughput(Throughput::Bytes(INSERT_PROGRESS.len() as u64));
    group.bench_function("insert_progress", |b| {
        b.iter(|| Progress::from_header_value(std::hint::black_box(INSERT_PROGRESS)));
    });
    group.throughput(Throughput::Bytes(MALFORMED.len() as u64));
    group.bench_function("malformed_short_circuit", |b| {
        b.iter(|| Progress::from_header_value(std::hint::black_box(MALFORMED)));
    });
    group.finish();
}

criterion_group!(benches, parse_typical);
criterion_main!(benches);
