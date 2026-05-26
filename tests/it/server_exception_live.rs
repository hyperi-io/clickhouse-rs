//! Live-ClickHouse integration test for the typed
//! `Error::ServerException` mapping against real server errors.
//!
//! The mock-based unit tests in `src/error.rs` and `src/response.rs`
//! cover the parser. This file proves the parser handles error
//! messages actually emitted by ClickHouse for known retriable and
//! non-retriable conditions, and that the `is_retriable()`
//! classification matches the documented retriable codes
//! (159, 202, 209, 210, 252, 279, 290, 999).
//!
//! Skipped (with a clear message) when `CLICKHOUSE_URL` is unset.

use std::env;

use clickhouse::Client;
use clickhouse::error::Error;

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
async fn timeout_exceeded_maps_to_retriable_159() {
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    // `max_execution_time = 1` + sleep(5) -> server kills the query
    // with TIMEOUT_EXCEEDED (code 159). The wire error message
    // contains both the code and the constant name.
    let result = client
        .query("SELECT sleep(3), sleep(3)")
        .with_setting("max_execution_time", "1")
        .execute()
        .await;
    let err = result.expect_err("timeout should produce an error");

    match &err {
        Error::ServerException { code, name, .. } => {
            assert_eq!(*code, 159, "expected TIMEOUT_EXCEEDED (159); got name={name:?}");
        }
        other => panic!("expected Error::ServerException; got {other:?}"),
    }
    assert!(err.is_retriable(), "code 159 should be classified retriable");
}

#[tokio::test]
async fn syntax_error_maps_to_non_retriable() {
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    // Deliberate SQL syntax error.
    let result = client.query("THIS IS NOT VALID SQL").execute().await;
    let err = result.expect_err("bad SQL should produce an error");

    if let Error::ServerException { code, .. } = &err {
        // 62 = SYNTAX_ERROR; some versions report adjacent codes.
        // What matters: it's NOT in the retriable set.
        assert!(
            !err.is_retriable(),
            "syntax error (code {code}) must NOT be classified retriable"
        );
    } else {
        panic!("expected Error::ServerException; got {err:?}");
    }
}

#[tokio::test]
async fn unknown_table_maps_to_non_retriable() {
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    let result = client
        .query("SELECT 1 FROM _it_se_definitely_does_not_exist")
        .execute()
        .await;
    let err = result.expect_err("missing table should error");

    if let Error::ServerException { code, name, .. } = &err {
        // 60 = UNKNOWN_TABLE in older versions; 81 in newer.
        // Either way, NOT retriable.
        assert!(
            !err.is_retriable(),
            "unknown-table error (code {code}, name={name:?}) must NOT be retriable"
        );
    } else {
        panic!("expected Error::ServerException; got {err:?}");
    }
}

#[tokio::test]
async fn is_retriable_code_classifies_known_set() {
    // Pure logic check -- doesn't need a live server, but lives in
    // this file because it exercises the public surface that the typed-error path shipped.
    // The retriable set per src/error.rs is conservative.
    for code in [159, 202, 209, 210, 252, 279, 290, 999] {
        assert!(
            Error::is_retriable_code(code),
            "code {code} should be in the retriable set"
        );
    }
    for code in [47, 60, 62, 81, 161] {
        assert!(
            !Error::is_retriable_code(code),
            "code {code} should NOT be in the retriable set"
        );
    }
}
