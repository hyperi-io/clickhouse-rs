//! Live-server integration tests for `execute_query` over the TCP
//! connection actor.
//!
//! Driven by `CLICKHOUSE_TCP_URL` (defaults to `127.0.0.1:9000`).
//! Gated on `#[ignore]`; runs only with `--ignored` plus the `tcp`
//! feature. The `tcp` cfg gate lives at the `mod` declaration in
//! `tests/it/main.rs` so this file does not need an inner `#![cfg]`.

use std::env;
use std::net::SocketAddr;

use clickhouse::HandshakeConfig;
use clickhouse::error::Error;
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

async fn live_handle() -> ConnectionHandle {
    let (stream, hello) =
        open_handshaken(live_addr(), &ConnectKind::Plain, &live_handshake_config())
            .await
            .expect("handshake against live server should succeed");
    ConnectionActor::spawn(stream, hello)
}

#[tokio::test]
#[ignore = "requires CLICKHOUSE_TCP_URL env var pointing at a live ClickHouse server"]
async fn select_one() {
    let handle = live_handle().await;
    handle
        .execute_query("rs_select_one".into(), "SELECT 1".into(), Vec::new())
        .await
        .expect("SELECT 1 should succeed");
    assert!(handle.is_alive());
}

#[tokio::test]
#[ignore = "requires CLICKHOUSE_TCP_URL env var pointing at a live ClickHouse server"]
async fn server_exception_surfaces_as_typed_error() {
    let handle = live_handle().await;
    let err = handle
        .execute_query(
            "rs_throwif".into(),
            "SELECT throwIf(1, 'boom')".into(),
            Vec::new(),
        )
        .await
        .expect_err("throwIf(1, ...) must produce a server Exception");
    match err {
        Error::ServerException { code, message, .. } => {
            // Code 395 is FUNCTION_THROW_IF_VALUE_IS_NON_ZERO; the
            // server rarely changes it but the test only asserts the
            // variant + that the message round-trips, so the exact
            // code is not load-bearing.
            assert!(code > 0, "expected positive server error code, got {code}");
            assert!(
                message.contains("boom"),
                "expected server message to contain 'boom', got: {message}"
            );
        }
        other => panic!("expected ServerException, got {other:?}"),
    }
    // Connection must remain usable after a server-side rejection.
    assert!(handle.is_alive(), "connection should survive server Exception");
    handle
        .execute_query("rs_post_throw".into(), "SELECT 1".into(), Vec::new())
        .await
        .expect("connection should accept another query after Exception");
}

#[tokio::test]
#[ignore = "requires CLICKHOUSE_TCP_URL env var pointing at a live ClickHouse server"]
async fn cancel_on_reply_drop_releases_connection() {
    let handle = live_handle().await;

    // Long-running query: `sleepEachRow(0.1) FROM numbers(1000)` is
    // ~100 s of server-side work. We drop the future inside a 250 ms
    // timeout so the actor must Cancel + drain to keep the socket
    // reusable.
    let exec_handle = handle.clone();
    let slow_fut = exec_handle.execute_query(
        "rs_cancel".into(),
        "SELECT sleepEachRow(0.1) FROM numbers(1000)".into(),
        Vec::new(),
    );
    let dropped = tokio::time::timeout(std::time::Duration::from_millis(250), slow_fut).await;
    assert!(
        dropped.is_err(),
        "long-running query should not finish inside 250 ms"
    );

    // Allow the actor a generous window to Cancel + drain before we
    // reuse the connection. The server-side query takes at most a
    // small number of seconds to terminate after a Cancel.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    assert!(
        handle.is_alive(),
        "connection should remain alive after cancellation"
    );

    // Reuse the connection -- if cancel-and-drain worked the
    // following query completes cleanly on the same socket.
    handle
        .execute_query("rs_post_cancel".into(), "SELECT 1".into(), Vec::new())
        .await
        .expect("connection should be reusable after cancel-on-drop");
}
