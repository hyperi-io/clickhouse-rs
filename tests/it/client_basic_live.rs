//! Live-ClickHouse integration test for `Client::ping` via `SELECT 1`.
//!
//! The mock-based suite in `conveniences.rs` proves ping issues the
//! expected `SELECT 1` HTTP request. This file proves the public API
//! resolves against a real server (Ok), and surfaces a clear error
//! against an unreachable URL.
//!
//! Skipped (with a clear message) when `CLICKHOUSE_URL` is unset.

use std::env;

use clickhouse::Client;

fn live_client() -> Option<Client> {
    let url = env::var("CLICKHOUSE_URL").ok()?;
    let mut client = Client::default().with_url(url);
    if let Ok(user) = env::var("CLICKHOUSE_USER") {
        client = client.with_user(user);
    }
    if let Ok(password) = env::var("CLICKHOUSE_PASSWORD") {
        client = client.with_password(password);
    }
    if let Ok(database) = env::var("CLICKHOUSE_DATABASE") {
        client = client.with_database(database);
    }
    Some(client)
}

#[tokio::test]
async fn ping_succeeds_against_live_server() {
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };
    client.ping().await.expect("ping live server");
}

#[tokio::test]
async fn ping_errors_against_unreachable_url() {
    // Bind-local port that nothing is listening on. We don't need
    // CLICKHOUSE_URL for this case -- it's a client-side error path
    // that's the same in any environment.
    let client = Client::default().with_url("http://127.0.0.1:1");
    let result = client.ping().await;
    assert!(
        result.is_err(),
        "ping against unreachable URL should error; got: {result:?}"
    );
}
