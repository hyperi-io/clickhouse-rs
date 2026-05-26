//! Native columnar encoder benches.
//!
//! Anchors `native::encode::encode_columns` per-block cost across
//! representative column shapes. Same bench file ships on both:
//! - this branch (encoder baseline,
//!   per-cell `.to_vec()` outer pass).
//! - the zero-copy refactor follow-up (encoder
//!   refactored: slice-ref outer pass + pre-sized output).
//!
//! The performance comparison is therefore "run this bench on each
//! branch tip; record both number sets; compare". Numbers MUST be
//! verbatim from criterion output, NOT model-recalled.
//!
//! Run:
//!   cargo bench --bench native_encode --features 'lz4 test-util'
//!
//! When citing results, record CPU + rustc + allocator + invocation
//! + the branch tip SHA `git log -1 --oneline` of the runner.

use clickhouse::native::encode::{ColumnSchema, encode_columns};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

const REVISION: u64 = 54454; // modern, custom_serialization flag emitted

/// Build a row buffer containing N rows of `(u64, u64)` -- each row
/// is two little-endian u64s (16 bytes/row). Output `Vec<Vec<u8>>`
/// is the per-row RowBinary buffer the encoder consumes.
fn rows_uint64_pair(n: usize) -> Vec<Vec<u8>> {
    (0..n as u64)
        .map(|i| {
            let mut row = Vec::with_capacity(16);
            row.extend_from_slice(&i.to_le_bytes());
            row.extend_from_slice(&i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
            row
        })
        .collect()
}

/// Build a row buffer containing N rows of `(String, String)` -- each
/// row is two length-prefixed UTF-8 strings (RowBinary varint length).
/// Short strings keep the per-row overhead modest.
fn rows_string_pair(n: usize) -> Vec<Vec<u8>> {
    fn varuint(out: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            out.push(((value & 0x7F) | 0x80) as u8);
            value >>= 7;
        }
        out.push(value as u8);
    }
    (0..n as u64)
        .map(|i| {
            let a = format!("a{i:06}");
            let b = format!("b{i:06}");
            let mut row = Vec::with_capacity(a.len() + b.len() + 4);
            varuint(&mut row, a.len() as u64);
            row.extend_from_slice(a.as_bytes());
            varuint(&mut row, b.len() as u64);
            row.extend_from_slice(b.as_bytes());
            row
        })
        .collect()
}

fn schema(pairs: &[(&str, &str)]) -> Vec<ColumnSchema> {
    let headers: Vec<(String, String)> = pairs
        .iter()
        .map(|(n, t)| ((*n).to_string(), (*t).to_string()))
        .collect();
    ColumnSchema::from_headers(&headers).expect("schema parse")
}

// ---------------------------------------------------------------------------
// bench: scalar (UInt64, UInt64) -- the simplest hot path; no nesting.
// ---------------------------------------------------------------------------

fn scalar_uint64_pair(c: &mut Criterion) {
    let mut group = c.benchmark_group("native_encode_scalar_uint64_pair");
    let schema = schema(&[("a", "UInt64"), ("b", "UInt64")]);
    for &n in &[100u64, 10_000, 100_000] {
        let rows = rows_uint64_pair(n as usize);
        let row_bytes = rows.iter().map(Vec::len).sum::<usize>() as u64;
        group.throughput(Throughput::Bytes(row_bytes));
        group.bench_function(format!("{n}_rows"), |b| {
            b.iter(|| {
                let out = encode_columns(
                    std::hint::black_box(&rows),
                    std::hint::black_box(&schema),
                    REVISION,
                )
                .expect("encode");
                std::hint::black_box(out);
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// bench: (String, String) -- variable-length, exercises the
// string/varint fast-path AND the per-cell allocation pressure that
// the zero-copy refactor targets.
// ---------------------------------------------------------------------------

fn string_pair(c: &mut Criterion) {
    let mut group = c.benchmark_group("native_encode_string_pair");
    let schema = schema(&[("a", "String"), ("b", "String")]);
    for &n in &[100u64, 10_000, 100_000] {
        let rows = rows_string_pair(n as usize);
        let row_bytes = rows.iter().map(Vec::len).sum::<usize>() as u64;
        group.throughput(Throughput::Bytes(row_bytes));
        group.bench_function(format!("{n}_rows"), |b| {
            b.iter(|| {
                let out = encode_columns(
                    std::hint::black_box(&rows),
                    std::hint::black_box(&schema),
                    REVISION,
                )
                .expect("encode");
                std::hint::black_box(out);
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// bench: mixed schema (3 scalars + 2 strings + 1 array) -- typical
// production events table.
// ---------------------------------------------------------------------------

fn mixed_schema(c: &mut Criterion) {
    // Construct rows by interleaving scalar + string + array values
    // per row. The encoder's recursive paths (Array) plus its outer
    // transpose are both exercised.
    fn varuint(out: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            out.push(((value & 0x7F) | 0x80) as u8);
            value >>= 7;
        }
        out.push(value as u8);
    }

    let schema = schema(&[
        ("id", "UInt64"),
        ("score", "Int64"),
        ("at", "UInt64"),
        ("name", "String"),
        ("tag", "String"),
        ("items", "Array(UInt32)"),
    ]);

    let n_rows = 50_000usize;
    let rows: Vec<Vec<u8>> = (0..n_rows as u64)
        .map(|i| {
            let mut r = Vec::with_capacity(64);
            r.extend_from_slice(&i.to_le_bytes());
            r.extend_from_slice(&(i as i64).wrapping_mul(-3).to_le_bytes());
            r.extend_from_slice(&(1_700_000_000u64 + i).to_le_bytes());
            let name = format!("user-{i:06}");
            varuint(&mut r, name.len() as u64);
            r.extend_from_slice(name.as_bytes());
            let tag = format!("t{}", i % 100);
            varuint(&mut r, tag.len() as u64);
            r.extend_from_slice(tag.as_bytes());
            // 4-element Array(UInt32)
            varuint(&mut r, 4);
            for j in 0..4u32 {
                r.extend_from_slice(&(j + i as u32).to_le_bytes());
            }
            r
        })
        .collect();

    let mut group = c.benchmark_group("native_encode_mixed_schema");
    let row_bytes = rows.iter().map(Vec::len).sum::<usize>() as u64;
    group.throughput(Throughput::Bytes(row_bytes));
    group.bench_function(format!("{n_rows}_rows"), |b| {
        b.iter(|| {
            let out = encode_columns(
                std::hint::black_box(&rows),
                std::hint::black_box(&schema),
                REVISION,
            )
            .expect("encode");
            std::hint::black_box(out);
        });
    });
    group.finish();
}

criterion_group!(benches, scalar_uint64_pair, string_pair, mixed_schema);
criterion_main!(benches);
