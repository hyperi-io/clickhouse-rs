//! Live-server integration test for the TCP handshake.
//!
//! Drives `connect::open_handshaken` against a real ClickHouse 25.x
//! server reachable via `CLICKHOUSE_TCP_URL` (defaults to
//! `127.0.0.1:9000`). Asserts the handshake completes, the server
//! revision is at least the custom-serialization gate (54454, the
//! lower bound this client targets), and the server-name string is
//! non-empty.
//!
//! Gated on `#[ignore]`; runs only with the `--ignored` flag plus
//! the `tcp` feature. Default CI does not exercise it. The `tcp`
//! cfg gate lives at the `mod` declaration in `tests/it/main.rs`
//! so this file does not need an inner `#![cfg]`.

use std::env;
use std::net::SocketAddr;

use clickhouse::HandshakeConfig;
use clickhouse::tcp::connect::{ConnectKind, open_handshaken};

#[tokio::test]
#[ignore = "requires CLICKHOUSE_TCP_URL env var pointing at a live ClickHouse server"]
async fn handshake_against_live_server() {
    let url = env::var("CLICKHOUSE_TCP_URL").unwrap_or_else(|_| "127.0.0.1:9000".to_string());
    let addr: SocketAddr = url
        .parse()
        .expect("CLICKHOUSE_TCP_URL must be a host:port pair");

    let cfg = HandshakeConfig {
        database: env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".into()),
        user: env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()),
        password: env::var("CLICKHOUSE_PASSWORD").unwrap_or_default(),
        quota_key: env::var("CLICKHOUSE_QUOTA_KEY").unwrap_or_default(),
    };

    let (stream, hello) = open_handshaken(addr, &ConnectKind::Plain, &cfg)
        .await
        .expect("handshake against live server should succeed");

    assert!(
        !hello.server_name.is_empty(),
        "expected non-empty server_name from server Hello"
    );
    assert!(
        hello.revision >= 54454,
        "server revision {} is below the custom-serialization gate (54454); \
         upgrade ClickHouse to at least 24.x",
        hello.revision
    );

    // Drop the stream cleanly -- no Cancel/Quit packet exchange is
    // needed before a connection close from the client side.
    drop(stream);
}
