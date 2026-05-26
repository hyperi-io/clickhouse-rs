//! Tests for the small per-query / per-Client conveniences:
//!
//!   - `Query::with_role`
//!   - `Query::with_session_id`
//!   - `Query::async_insert(wait)`
//!   - `Client::ping()`
//!
//! Each verifies the URL-level wiring. Live semantics (does the
//! server actually honour the setting?) are out of scope here --
//! ClickHouse's behaviour is documented and stable for these
//! settings.

#![cfg(feature = "test-util")]

use clickhouse::{Client, test};

use crate::SimpleRow;

#[tokio::test]
async fn with_role_sets_role_url_param() {
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let recorder = mock.add(test::handlers::record_with_uri::<SimpleRow>());

    client
        .query("SELECT 1")
        .with_role("analyst")
        .execute()
        .await
        .unwrap();

    let uri = recorder.collect_uri().await;
    assert!(
        uri.contains("role=analyst"),
        "with_role should surface as role= URL param; got: {uri}"
    );
}

#[tokio::test]
async fn with_session_id_sets_session_id_url_param() {
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let recorder = mock.add(test::handlers::record_with_uri::<SimpleRow>());

    client
        .query("SELECT 1")
        .with_session_id("user-42/session-abc")
        .execute()
        .await
        .unwrap();

    let uri = recorder.collect_uri().await;
    assert!(
        uri.contains("session_id=user-42") && uri.contains("session-abc"),
        "with_session_id should surface as session_id= URL param; got: {uri}"
    );
}

#[tokio::test]
async fn async_insert_wait_sets_both_settings() {
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let recorder = mock.add(test::handlers::record_with_uri::<SimpleRow>());

    client
        .query("INSERT INTO t VALUES (1)")
        .async_insert(true)
        .execute()
        .await
        .unwrap();

    let uri = recorder.collect_uri().await;
    assert!(
        uri.contains("async_insert=1"),
        "async_insert=1 should be set; got: {uri}"
    );
    assert!(
        uri.contains("wait_for_async_insert=1"),
        "wait=true should set wait_for_async_insert=1; got: {uri}"
    );
}

#[tokio::test]
async fn async_insert_no_wait_sets_zero() {
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let recorder = mock.add(test::handlers::record_with_uri::<SimpleRow>());

    client
        .query("INSERT INTO t VALUES (1)")
        .async_insert(false)
        .execute()
        .await
        .unwrap();

    let uri = recorder.collect_uri().await;
    assert!(
        uri.contains("async_insert=1"),
        "async_insert=1 should be set; got: {uri}"
    );
    assert!(
        uri.contains("wait_for_async_insert=0"),
        "wait=false should set wait_for_async_insert=0; got: {uri}"
    );
}

#[tokio::test]
async fn ping_succeeds_against_responsive_server() {
    // `ping()` runs `SELECT 1` via `execute()`. clickhouse-rs sends
    // the SQL in the POST body (not the URL), so we don't assert on
    // URL content -- just that the request was made and the call
    // returned Ok. A non-responsive mock would block here.
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let _recorder = mock.add(test::handlers::record_ddl());

    client.ping().await.unwrap();
}

#[tokio::test]
async fn ping_propagates_server_error() {
    use hyper::StatusCode;

    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let _ = mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));

    let result = client.ping().await;
    assert!(
        result.is_err(),
        "ping should propagate server-side failures; got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// Pool knobs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pool_idle_timeout_is_accepted_and_client_still_works() {
    // Smoke test: setting the pool idle timeout doesn't break the
    // client. Actual pool-behaviour verification (does the conn
    // actually evict after N seconds?) requires longer-running
    // integration tests; mock-based coverage here just exercises
    // the rebuild path.
    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_pool_idle_timeout(std::time::Duration::from_secs(30));
    let _ = mock.add(test::handlers::record_ddl());

    client.query("SELECT 1").execute().await.unwrap();
}

#[tokio::test]
async fn pool_max_idle_per_host_is_accepted_and_client_still_works() {
    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_pool_max_idle_per_host(8);
    let _ = mock.add(test::handlers::record_ddl());

    client.query("SELECT 1").execute().await.unwrap();
}

#[tokio::test]
async fn tcp_keepalive_is_accepted_and_client_still_works() {
    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_tcp_keepalive(std::time::Duration::from_secs(120));
    let _ = mock.add(test::handlers::record_ddl());

    client.query("SELECT 1").execute().await.unwrap();
}

#[tokio::test]
async fn pool_knobs_chain_with_other_builders() {
    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_pool_idle_timeout(std::time::Duration::from_secs(15))
        .with_pool_max_idle_per_host(10)
        .with_tcp_keepalive(std::time::Duration::from_secs(45))
        .with_database("my_db");
    let _ = mock.add(test::handlers::record_ddl());

    client.query("SELECT 1").execute().await.unwrap();
}

// ---------------------------------------------------------------------------
// Distributed durability mode
// ---------------------------------------------------------------------------

#[tokio::test]
async fn durability_background_is_default_and_sets_no_extra_settings() {
    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_durability(clickhouse::Durability::Background);
    let recorder = mock.add(test::handlers::record_with_uri::<SimpleRow>());

    client.query("SELECT 1").execute().await.unwrap();

    let uri = recorder.collect_uri().await;
    assert!(
        !uri.contains("distributed_foreground_insert"),
        "Background mode should not set distributed_foreground_insert; got: {uri}"
    );
    assert!(
        !uri.contains("fsync_after_insert"),
        "Background mode should not set fsync_after_insert; got: {uri}"
    );
}

#[tokio::test]
async fn durability_foreground_sets_distributed_foreground_insert() {
    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_durability(clickhouse::Durability::Foreground);
    let recorder = mock.add(test::handlers::record_with_uri::<SimpleRow>());

    client.query("SELECT 1").execute().await.unwrap();

    let uri = recorder.collect_uri().await;
    assert!(
        uri.contains("distributed_foreground_insert=1"),
        "Foreground mode should set distributed_foreground_insert=1; got: {uri}"
    );
    assert!(
        !uri.contains("fsync_after_insert"),
        "Foreground mode should NOT set fsync_after_insert; got: {uri}"
    );
}

#[tokio::test]
async fn durability_foreground_fsynced_sets_all_three() {
    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_durability(clickhouse::Durability::ForegroundFsynced);
    let recorder = mock.add(test::handlers::record_with_uri::<SimpleRow>());

    client.query("SELECT 1").execute().await.unwrap();

    let uri = recorder.collect_uri().await;
    assert!(uri.contains("distributed_foreground_insert=1"));
    assert!(uri.contains("fsync_after_insert=1"));
    assert!(uri.contains("fsync_directories=1"));
}
