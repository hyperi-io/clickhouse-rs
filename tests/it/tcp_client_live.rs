//! End-to-end live-server tests for `Client::tcp`.
//!
//! Driven by `CLICKHOUSE_TCP_URL` (defaults to `127.0.0.1:9000`).
//! Gated on `#[ignore]`; runs only with `--ignored` plus the `tcp`
//! feature. The `tcp` cfg gate lives at the `mod` declaration in
//! `tests/it/main.rs` so this file does not need an inner `#![cfg]`.
//!
//! These tests prove that the public `Client::tcp` surface composes
//! the underlying connection-actor primitives correctly: DDL via
//! `query().execute()`, streaming SELECT via raw blocks, and INSERT
//! through the `InsertNative<T>` wrapper.

use std::env;

use clickhouse::native::DecodedColumn;
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};

fn live_tcp_url() -> String {
    env::var("CLICKHOUSE_TCP_URL").unwrap_or_else(|_| "127.0.0.1:9000".to_string())
}

fn live_client() -> Client {
    let mut client = Client::tcp(live_tcp_url());
    if let Ok(db) = env::var("CLICKHOUSE_DATABASE") {
        client = client.with_database(db);
    }
    if let Ok(user) = env::var("CLICKHOUSE_USER") {
        client = client.with_user(user);
    }
    if let Ok(pw) = env::var("CLICKHOUSE_PASSWORD") {
        client = client.with_password(pw);
    }
    client
}

#[derive(Row, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct N {
    n: u64,
}

/// Simplest end-to-end: build a TCP client, run an execute-only
/// query, assert it succeeded.
#[tokio::test]
#[ignore = "requires CLICKHOUSE_TCP_URL env var pointing at a live ClickHouse server"]
async fn client_tcp_query_one() {
    let client = live_client();
    client
        .query("SELECT 1")
        .execute()
        .await
        .expect("SELECT 1 over TCP should succeed");
}

/// Streaming SELECT over TCP via the raw-block cursor.
///
/// A per-row `Row`-trait bridge is a follow-up; v1 of the cursor is
/// whole-block iteration via `Query::fetch_native_blocks()`.
#[tokio::test]
#[ignore = "requires CLICKHOUSE_TCP_URL env var pointing at a live ClickHouse server"]
async fn client_tcp_select_native_stream() {
    let client = live_client();
    let mut cursor = client
        .query("SELECT number AS n FROM numbers(5)")
        .fetch_native_blocks()
        .await
        .expect("streaming SELECT over TCP should succeed");

    let mut rows: Vec<u64> = Vec::new();
    while let Some(block) = cursor
        .next_block()
        .await
        .expect("next_block must not error mid-stream")
    {
        // Schema blocks land first with num_rows == 0; payload
        // blocks follow.
        if block.num_rows == 0 {
            continue;
        }
        // SELECT numbers() returns a UInt64 column.
        assert_eq!(block.columns.len(), 1, "single-column SELECT");
        match &block.columns[0] {
            DecodedColumn::UInt64(v) => rows.extend_from_slice(v),
            other => panic!("expected UInt64 column, got {other:?}"),
        }
    }
    assert_eq!(rows, vec![0, 1, 2, 3, 4]);
}

/// CREATE TEMPORARY TABLE + 1000-row insert via `Client::insert_native`
/// over the TCP transport. Mirrors the HTTP `insert_native` surface
/// the way the architectural decision intended: same builder, same
/// `write/end` API, transport selected at client-construction time.
#[tokio::test]
#[ignore = "requires CLICKHOUSE_TCP_URL env var pointing at a live ClickHouse server"]
async fn client_tcp_insert_native() {
    let client = live_client();
    // Temporary tables are scoped to the TCP session. With the pool
    // sized to 1 the CREATE + INSERT + SELECT all land on the same
    // physical connection -- otherwise a second pool slot might be
    // checked out for the INSERT and the temp table would not be
    // visible there.
    let client = client.with_tcp_pool_size(1);

    client
        .query("DROP TEMPORARY TABLE IF EXISTS rs_tcp_client_insert")
        .execute()
        .await
        .ok();
    client
        .query("CREATE TEMPORARY TABLE rs_tcp_client_insert (n UInt64) ENGINE = Memory")
        .execute()
        .await
        .expect("create temp table");

    let mut insert = client
        .insert_native::<N>("rs_tcp_client_insert")
        .await
        .expect("insert_native::<N> over TCP should succeed");
    for n in 0..1000u64 {
        insert.write(&N { n }).await.expect("write");
    }
    insert.end().await.expect("end");

    // Read it back to prove the rows actually landed.
    let mut cursor = client
        .query("SELECT count() AS n FROM rs_tcp_client_insert")
        .fetch_native_blocks()
        .await
        .expect("count via streaming SELECT");
    let mut counts: Vec<u64> = Vec::new();
    while let Some(block) = cursor.next_block().await.expect("next_block") {
        if block.num_rows == 0 {
            continue;
        }
        match &block.columns[0] {
            DecodedColumn::UInt64(v) => counts.extend_from_slice(v),
            other => panic!("expected UInt64 count, got {other:?}"),
        }
    }
    assert_eq!(counts, vec![1000]);
}
