//! Live TLS-trust integration tests against an internal-CA cluster.
//!
//! These exercise the unified trust path: a private/internal CA PEM
//! loaded via [`Client::try_with_tls_root_ca`] must let BOTH the HTTP
//! and TCP transports verify a server whose chain roots in that CA.
//! Success proves the shared `src/tls.rs` trust resolution reaches the
//! HTTP connector and the TCP connect path identically.
//!
//! `#[ignore]`'d; runs only with `--ignored` plus the `tcp` +
//! `native-tls-rustls` features. Each test SKIPS (returns early) when
//! `CLICKHOUSE_CA_PEM` is unset, so a runner without the internal CA
//! does not fail -- it just does not exercise the leg. The CA PEM path
//! is non-secret test data; Derek provides where it lives. The cfg
//! gate lives at the `mod` declaration in `tests/it/main.rs`.

use std::env;

use clickhouse::Client;

/// Path to the non-secret internal CA PEM, or `None` to skip.
fn ca_pem() -> Option<String> {
    env::var("CLICKHOUSE_CA_PEM").ok()
}

/// HTTPS against an internal-CA server, trust supplied as a PEM file.
#[tokio::test]
#[ignore = "live: requires CLICKHOUSE_URL + CLICKHOUSE_CA_PEM"]
async fn https_with_internal_ca_pem() {
    let Some(ca) = ca_pem() else { return };
    let url = env::var("CLICKHOUSE_URL").expect("CLICKHOUSE_URL env var required");

    let mut client = Client::default()
        .with_url(url)
        .try_with_tls_root_ca(ca)
        .expect("load internal CA PEM");
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
        .expect("SELECT 1 over HTTPS with the internal CA should succeed");
}

/// TLS over TCP against an internal-CA server, same PEM trust.
#[tokio::test]
#[ignore = "live: requires CLICKHOUSE_TCPS_URL + CLICKHOUSE_TCPS_SNI + CLICKHOUSE_CA_PEM"]
async fn tcp_tls_with_internal_ca_pem() {
    let Some(ca) = ca_pem() else { return };
    let url = env::var("CLICKHOUSE_TCPS_URL")
        .expect("CLICKHOUSE_TCPS_URL env var required (e.g. host:9440)");
    let sni =
        env::var("CLICKHOUSE_TCPS_SNI").expect("CLICKHOUSE_TCPS_SNI env var required");

    let mut client = Client::tcp_tls(url, sni)
        .try_with_tls_root_ca(ca)
        .expect("load internal CA PEM");
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
        .expect("SELECT 1 over TCP TLS with the internal CA should succeed");
}
