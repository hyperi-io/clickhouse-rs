//! Mock-based tests for `Query::with_query_id` + `Client::kill_query`
//! . Validates the URL-level wiring; live KILL semantics are
//! out of scope here (covered by `system.processes` consultation in
//! the user's own integration suite).

#![cfg(feature = "test-util")]

use clickhouse::{Client, test};

use crate::SimpleRow;

#[tokio::test]
async fn with_query_id_appears_in_request_uri() {
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    // `record_with_uri` returns an empty body; we use `execute()`
    // here rather than `fetch_all` so the decoder doesn't try to
    // pull rows out of the empty response. The URL the client put
    // on the wire is what we want to verify.
    let recorder = mock.add(test::handlers::record_with_uri::<SimpleRow>());

    client
        .query("SELECT 1")
        .with_query_id("my-app/req-12345")
        .execute()
        .await
        .unwrap();

    let uri = recorder.collect_uri().await;
    assert!(
        uri.contains("query_id=my-app") && uri.contains("req-12345"),
        "query_id should flow into the URL; got: {uri}"
    );
}

#[tokio::test]
async fn kill_query_issues_kill_statement_with_query_id() {
    use clickhouse::test::handlers::record_ddl;

    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let recorder = mock.add(record_ddl());

    client.kill_query("abandoned-id-7").await.unwrap();

    // The DDL recorder captures the request body. KILL QUERY is a
    // DDL-shaped statement; the body should reflect the rendered SQL
    // with the bound id.
    let sql = recorder.query().await;
    assert!(
        sql.contains("KILL QUERY"),
        "kill_query should send a KILL QUERY statement; got: {sql}"
    );
    assert!(
        sql.contains("abandoned-id-7"),
        "bound id should appear in the rendered SQL; got: {sql}"
    );
    assert!(
        sql.contains("SYNC"),
        "kill_query should use SYNC mode; got: {sql}"
    );
}

// ---------------------------------------------------------------------------
// Auto-generated query_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn with_auto_query_id_generates_uuid_v7_per_query() {
    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_auto_query_id();
    let r1 = mock.add(test::handlers::record_with_uri::<SimpleRow>());
    let r2 = mock.add(test::handlers::record_with_uri::<SimpleRow>());

    client.query("SELECT 1").execute().await.unwrap();
    client.query("SELECT 2").execute().await.unwrap();

    let u1 = r1.collect_uri().await;
    let u2 = r2.collect_uri().await;

    // Both queries carry a query_id; the two are different.
    assert!(u1.contains("query_id="), "query 1 missing query_id; got: {u1}");
    assert!(u2.contains("query_id="), "query 2 missing query_id; got: {u2}");
    // Extract values; they should differ.
    fn extract_query_id(uri: &str) -> &str {
        let after = uri.split("query_id=").nth(1).unwrap_or("");
        after.split('&').next().unwrap_or("")
    }
    let id1 = extract_query_id(&u1);
    let id2 = extract_query_id(&u2);
    assert_ne!(id1, id2, "consecutive queries must have distinct ids");
    // UUIDv7 has a fixed 36-char form: 8-4-4-4-12 hex digits.
    assert_eq!(id1.len(), 36, "expected UUID-shaped id; got: {id1}");
}

#[tokio::test]
async fn with_query_id_overrides_auto() {
    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_auto_query_id();
    let recorder = mock.add(test::handlers::record_with_uri::<SimpleRow>());

    client
        .query("SELECT 1")
        .with_query_id("custom-id-42")
        .execute()
        .await
        .unwrap();

    let uri = recorder.collect_uri().await;
    assert!(
        uri.contains("query_id=custom-id-42"),
        "explicit with_query_id should override auto; got: {uri}"
    );
}

#[tokio::test]
async fn without_with_auto_query_id_no_query_id_is_set() {
    // Default Client (no with_auto_query_id call): no query_id
    // appears in the URL unless the caller explicitly sets one.
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let recorder = mock.add(test::handlers::record_with_uri::<SimpleRow>());

    client.query("SELECT 1").execute().await.unwrap();

    let uri = recorder.collect_uri().await;
    assert!(
        !uri.contains("query_id="),
        "default client should not auto-generate query_id; got: {uri}"
    );
}

// ---------------------------------------------------------------------------
// Drop-on-cursor auto KILL QUERY
// ---------------------------------------------------------------------------

#[tokio::test]
async fn kill_on_drop_issues_kill_when_cursor_abandoned() {
    use std::time::Duration;

    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_kill_on_drop()
        .with_validation(false);

    // Only handler is for the KILL QUERY DDL. The cursor's HTTP
    // request future is constructed by `fetch()` but never polled
    // (we drop immediately), so it never fires -- mock only sees
    // the KILL spawned by Drop.
    let kill_handler = mock.add(test::handlers::record_ddl());

    {
        let cursor = client
            .query("SELECT 1")
            .with_query_id("abandoned-cursor-id")
            .fetch::<SimpleRow>()
            .unwrap();
        drop(cursor);
    }

    let sql = tokio::time::timeout(Duration::from_secs(2), kill_handler.query())
        .await
        .expect("KILL QUERY should arrive within 2s of cursor drop");
    assert!(
        sql.contains("KILL QUERY"),
        "drop should have issued KILL QUERY; got: {sql}"
    );
    assert!(
        sql.contains("abandoned-cursor-id"),
        "KILL QUERY should target the abandoned query_id; got: {sql}"
    );
}

#[tokio::test]
async fn kill_on_drop_skipped_when_no_query_id() {
    // with_kill_on_drop is enabled but no query_id is set on the
    // query -- KILL is impossible without one. Drop is a no-op.
    // Verified via `non_exhaustive()`: if KILL fired, it'd hit an
    // unregistered handler and the mock would error.
    let mut mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_kill_on_drop()
        .with_validation(false);
    mock.non_exhaustive();

    let cursor = client.query("SELECT 1").fetch::<SimpleRow>().unwrap();
    drop(cursor);

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
}

#[tokio::test]
async fn kill_on_drop_off_by_default() {
    // Without with_kill_on_drop(), even with a query_id set, the
    // Drop is a no-op.
    let mut mock = test::Mock::new();
    let client = Client::default().with_mock(&mock).with_validation(false);
    mock.non_exhaustive();

    let cursor = client
        .query("SELECT 1")
        .with_query_id("some-id")
        .fetch::<SimpleRow>()
        .unwrap();
    drop(cursor);

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
}
