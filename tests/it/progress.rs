//! Mock-based tests for `Client::with_progress_callback` -- the
//! 11a-callbacks-api HTTP-side Progress callback surface.
//!
//! Live-CH coverage is intentionally out of scope here; the
//! server-side emission of `X-ClickHouse-Progress` headers depends
//! on the `send_progress_in_http_headers=1` setting, which is
//! straightforward to enable but adds CH-version coupling we
//! don't want to bake into the protocol-level tests.

#![cfg(feature = "test-util")]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use clickhouse::{Client, progress::Progress, test};

use crate::SimpleRow;

#[tokio::test]
async fn progress_callback_fires_for_each_header() {
    // Two progress headers in sequence -- typical for an in-flight
    // query that emits cumulative state. The callback should fire
    // twice with the parsed values.
    let mock = test::Mock::new();
    let captured = Arc::new(std::sync::Mutex::new(Vec::<Progress>::new()));
    let captured_for_cb = captured.clone();

    let client = Client::default()
        .with_mock(&mock)
        .with_progress_callback(move |p| {
            captured_for_cb.lock().unwrap().push(p.clone());
        });

    mock.add(test::handlers::provide_with_progress::<SimpleRow>(
        Vec::<SimpleRow>::new(),
        [
            r#"{"read_rows":"100","read_bytes":"2048","total_rows_to_read":"1000","written_rows":"0","written_bytes":"0","elapsed_ns":"500000"}"#,
            r#"{"read_rows":"1000","read_bytes":"20480","total_rows_to_read":"1000","written_rows":"0","written_bytes":"0","elapsed_ns":"1500000"}"#,
        ],
    ));

    let _rows: Vec<SimpleRow> = client
        .query("SELECT 1")
        .fetch_all::<SimpleRow>()
        .await
        .unwrap();

    let snapshot = captured.lock().unwrap().clone();
    assert_eq!(snapshot.len(), 2, "two progress headers -> two callbacks");
    assert_eq!(snapshot[0].read_rows, 100);
    assert_eq!(snapshot[0].total_rows_to_read, 1000);
    assert_eq!(snapshot[1].read_rows, 1000);
    assert_eq!(snapshot[1].elapsed_ns, 1500000);
}

#[tokio::test]
async fn no_callback_means_no_overhead_and_no_panic() {
    // When the client has no progress callback registered, the
    // header parser is skipped entirely. Mock emits headers anyway;
    // we verify the query completes successfully.
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);

    mock.add(test::handlers::provide_with_progress::<SimpleRow>(
        Vec::<SimpleRow>::new(),
        [r#"{"read_rows":"42"}"#],
    ));

    let _rows: Vec<SimpleRow> = client
        .query("SELECT 1")
        .fetch_all::<SimpleRow>()
        .await
        .unwrap();
    // No assertion needed beyond reaching this line without panic.
}

#[tokio::test]
async fn malformed_progress_header_is_silently_ignored() {
    // Server contract is that progress is best-effort. A malformed
    // header MUST NOT raise to the caller; the parser returns None
    // and the callback is simply not invoked for that header.
    let mock = test::Mock::new();
    let callback_count = Arc::new(AtomicU64::new(0));
    let count_for_cb = callback_count.clone();

    let client = Client::default()
        .with_mock(&mock)
        .with_progress_callback(move |_p| {
            count_for_cb.fetch_add(1, Ordering::Relaxed);
        });

    mock.add(test::handlers::provide_with_progress::<SimpleRow>(
        Vec::<SimpleRow>::new(),
        [
            "not json at all",                                // bad
            r#"{"read_rows":"7"}"#,                           // good
            r#"{"read_rows":"not_a_number"}"#,                // bad
        ],
    ));

    let _rows: Vec<SimpleRow> = client
        .query("SELECT 1")
        .fetch_all::<SimpleRow>()
        .await
        .unwrap();

    // Exactly one valid header -> callback fired once.
    assert_eq!(callback_count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn callback_runs_for_query_via_client_setting() {
    // Verify the full "register callback + observe headers" path
    // matches the rustdoc example. The mock doesn't emit headers
    // for the SELECT 1 itself unless we configure it to; we add
    // one explicit progress header and confirm the callback fires.
    let mock = test::Mock::new();
    let captured = Arc::new(std::sync::Mutex::new(None::<Progress>));
    let captured_for_cb = captured.clone();

    let client = Client::default()
        .with_mock(&mock)
        // Per the rustdoc, callers must set this; the mock will emit
        // a synthetic header regardless, but documenting the setting
        // matches what a real consumer would write.
        .with_setting("send_progress_in_http_headers", "1")
        .with_progress_callback(move |p| {
            *captured_for_cb.lock().unwrap() = Some(p.clone());
        });

    mock.add(test::handlers::provide_with_progress::<SimpleRow>(
        Vec::<SimpleRow>::new(),
        [r#"{"read_rows":"50","total_rows_to_read":"50","elapsed_ns":"123"}"#],
    ));

    let _rows: Vec<SimpleRow> = client
        .query("SELECT 1")
        .fetch_all::<SimpleRow>()
        .await
        .unwrap();

    let p = captured.lock().unwrap().clone().expect("callback fired");
    assert_eq!(p.read_rows, 50);
    assert_eq!(p.total_rows_to_read, 50);
    assert_eq!(p.elapsed_ns, 123);
}
