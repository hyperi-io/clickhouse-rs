//! Live-server integration test for the TCP+TLS transport.
//!
//! Drives `Client::tcp_tls` against a real ClickHouse 25.x server
//! reachable via `CLICKHOUSE_TCPS_URL` (typically port 9440) with the
//! certificate hostname supplied via `CLICKHOUSE_TCPS_SNI`. Asserts a
//! trivial `SELECT 1` completes end-to-end -- success here proves the
//! rustls handshake succeeded, the SNI matched, and the post-TLS
//! native handshake then ran cleanly.
//!
//! Gated on `#[ignore]`; runs only with the `--ignored` flag plus the
//! `tcp` + `native-tls-rustls` features. Default CI does not exercise
//! it. The cfg gate lives at the `mod` declaration in
//! `tests/it/main.rs` so this file does not need an inner `#![cfg]`.

use std::env;

use clickhouse::Client;

/// End-to-end TLS handshake + SELECT 1.
///
/// Requires both env vars; if either is unset the test panics on
/// purpose so a misconfigured runner surfaces loud rather than
/// quietly passing on a no-op.
#[tokio::test]
#[ignore = "requires CLICKHOUSE_TCPS_URL + CLICKHOUSE_TCPS_SNI env vars"]
async fn tls_handshake() {
    let url = env::var("CLICKHOUSE_TCPS_URL")
        .expect("CLICKHOUSE_TCPS_URL env var required (e.g. host:9440)");
    let sni =
        env::var("CLICKHOUSE_TCPS_SNI").expect("CLICKHOUSE_TCPS_SNI env var required");

    let mut client = Client::tcp_tls(url, sni);
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
        .query("SELECT 1")
        .execute()
        .await
        .expect("SELECT 1 over TLS should succeed");
}
