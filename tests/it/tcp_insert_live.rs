//! Live-server integration tests for INSERT over the TCP connection
//! actor.
//!
//! Driven by `CLICKHOUSE_TCP_URL` (defaults to `127.0.0.1:9000`).
//! Gated on `#[ignore]`; runs only with `--ignored` plus the `tcp`
//! feature. The `tcp` cfg gate lives at the `mod` declaration in
//! `tests/it/main.rs` so this file does not need an inner `#![cfg]`.
//!
//! These tests cover the BeginInsert / SendInsertBlock / FinishInsert
//! lifecycle and the full-duplex Exception detection that surfaces a
//! server-side rejection before the next block goes on the wire --
//! the correctness win HTTP cannot match.

use std::env;
use std::net::SocketAddr;

use clickhouse::HandshakeConfig;
use clickhouse::error::Error;
use clickhouse::native::{ColumnSchema, encode_columns};
use clickhouse::tcp::connect::{ConnectKind, open_handshaken};
use clickhouse::tcp::connection_actor::{ConnectionActor, ConnectionHandle};

fn live_addr() -> SocketAddr {
    let url = env::var("CLICKHOUSE_TCP_URL").unwrap_or_else(|_| "127.0.0.1:9000".to_string());
    url.parse()
        .expect("CLICKHOUSE_TCP_URL must be a host:port pair")
}

fn live_handshake_config() -> HandshakeConfig {
    HandshakeConfig {
        database: env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".into()),
        user: env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()),
        password: env::var("CLICKHOUSE_PASSWORD").unwrap_or_default(),
        quota_key: env::var("CLICKHOUSE_QUOTA_KEY").unwrap_or_default(),
    }
}

async fn live_handle() -> (ConnectionHandle, u64) {
    let (stream, hello) =
        open_handshaken(live_addr(), &ConnectKind::Plain, &live_handshake_config())
            .await
            .expect("handshake against live server should succeed");
    let revision = hello.revision;
    (ConnectionActor::spawn(stream, hello), revision)
}

/// Encode `n` rows of a single UInt64 column to a Native-format
/// payload. Each row's RowBinary is just 8 little-endian bytes; the
/// Phase 2 encoder transposes those into the columnar block body
/// expected by `send_data_block`.
fn encode_u64_block(headers: &[(String, String)], values: &[u64], revision: u64) -> Vec<u8> {
    let schema = ColumnSchema::from_headers(headers).expect("UInt64 schema parses");
    let rows: Vec<Vec<u8>> = values.iter().map(|v| v.to_le_bytes().to_vec()).collect();
    encode_columns(&rows, &schema, revision).expect("encode UInt64 column")
}

#[tokio::test]
#[ignore = "requires CLICKHOUSE_TCP_URL env var pointing at a live ClickHouse server"]
async fn insert_a_few_blocks() {
    let (handle, revision) = live_handle().await;

    handle
        .execute_query(
            "rs_insert_ddl".into(),
            "CREATE TEMPORARY TABLE rs_tcp_insert (n UInt64) ENGINE = Memory".into(),
            Vec::new(),
        )
        .await
        .expect("create temp table");

    let headers = handle
        .begin_insert(
            "rs_insert_begin".into(),
            "INSERT INTO rs_tcp_insert (n) FORMAT Native".into(),
            Vec::new(),
        )
        .await
        .expect("begin_insert against live server should succeed");
    assert!(
        !headers.is_empty(),
        "live ClickHouse 24.x/25.x echoes column metadata in the schema block"
    );
    assert_eq!(headers[0].0, "n");
    assert!(
        headers[0].1.contains("UInt64"),
        "expected UInt64 type, got {}",
        headers[0].1
    );

    let values: Vec<u64> = (0..1000u64).collect();
    let bytes = encode_u64_block(&headers, &values, revision);
    handle
        .send_insert_block(bytes, 1, 1000)
        .await
        .expect("send_insert_block should succeed");
    handle
        .finish_insert()
        .await
        .expect("finish_insert should land EndOfStream");

    // Sanity: the temp-table is scoped to this TCP session, so a
    // count query on the same handle must observe all 1000 rows.
    // The Task-7 actor cannot SELECT-with-rows yet (Task 8 ships the
    // cursor); execute_query is enough to prove the count query
    // round-trips without error.
    handle
        .execute_query(
            "rs_insert_count".into(),
            "SELECT count() FROM rs_tcp_insert".into(),
            Vec::new(),
        )
        .await
        .expect("count query should round-trip");
    assert!(handle.is_alive());
}

#[tokio::test]
#[ignore = "requires CLICKHOUSE_TCP_URL env var pointing at a live ClickHouse server"]
async fn full_duplex_exception_detected_between_blocks() {
    let (handle, revision) = live_handle().await;

    // A temporary table with a CHECK constraint -- the server
    // rejects the block on commit when a row violates the predicate.
    handle
        .execute_query(
            "rs_fd_ddl".into(),
            "CREATE TEMPORARY TABLE rs_tcp_fd (n UInt64, CONSTRAINT only_small CHECK n < 100) \
             ENGINE = Memory"
                .into(),
            Vec::new(),
        )
        .await
        .expect("create constrained temp table");

    let headers = handle
        .begin_insert(
            "rs_fd_begin".into(),
            "INSERT INTO rs_tcp_fd (n) FORMAT Native".into(),
            Vec::new(),
        )
        .await
        .expect("begin_insert should succeed");

    // Block 1: a violating row (1000 fails the n < 100 constraint).
    let violating = encode_u64_block(&headers, &[1000u64], revision);
    let _first = handle.send_insert_block(violating, 1, 1).await;
    // Either block 1 itself errors (server rejected synchronously)
    // or the next call surfaces the queued Exception. Both are
    // valid full-duplex outcomes; the test asserts that the actor
    // eventually surfaces the rejection and leaves the connection
    // alive for the next caller.

    // Give the server a moment to push its Exception into the
    // reader's mpsc queue before we attempt block 2 -- this is
    // exactly the inter-block window try_recv is designed to drain.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let block2 = encode_u64_block(&headers, &[1u64], revision);
    let second = handle.send_insert_block(block2, 1, 1).await;

    // If block 1 was accepted-then-rejected on commit, block 2
    // surfaces the Exception. If the server rejected block 1
    // synchronously, the first send already returned the error;
    // block 2 then sees "no INSERT session" from the actor's
    // state-machine gate. Either path proves the full-duplex
    // detection works.
    let saw_exception = matches!(second, Err(Error::ServerException { .. }))
        || matches!(_first, Err(Error::ServerException { .. }));
    assert!(
        saw_exception,
        "expected a ServerException for constraint violation \
         (first = {_first:?}, second = {second:?})"
    );

    assert!(
        handle.is_alive(),
        "connection should survive a server constraint rejection"
    );
}
