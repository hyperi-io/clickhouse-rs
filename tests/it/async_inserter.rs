//! Mock-based tests for `AsyncInserter` (the RFC #421 background-actor
//! Inserter shape). No live ClickHouse needed.

#![cfg(feature = "test-util")]

use std::time::Duration;

use clickhouse::{
    Client,
    async_inserter::{AsyncInserter, AsyncInserterConfig},
    test,
};

use crate::SimpleRow;

#[tokio::test]
async fn write_then_flush_delivers_rows() {
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let recorder = mock.add(test::handlers::record::<SimpleRow>());

    let inserter: AsyncInserter<SimpleRow> = AsyncInserter::new(
        &client,
        "t",
        AsyncInserterConfig::default()
            .without_period()
            .with_max_rows(u64::MAX)
            .with_max_bytes(u64::MAX),
    );

    inserter.write(SimpleRow::new(1, "a")).await.unwrap();
    inserter.write(SimpleRow::new(2, "b")).await.unwrap();

    // Nothing should be sent yet (no thresholds tripped, no period, no flush).
    // Force flush.
    let _q = inserter.flush().await.unwrap();

    let rows: Vec<SimpleRow> = recorder.collect().await;
    inserter.end().await.unwrap();

    assert_eq!(rows, vec![SimpleRow::new(1, "a"), SimpleRow::new(2, "b")]);
}

#[tokio::test]
async fn auto_flush_on_max_rows() {
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let recorder = mock.add(test::handlers::record::<SimpleRow>());

    let inserter: AsyncInserter<SimpleRow> = AsyncInserter::new(
        &client,
        "t",
        AsyncInserterConfig::default()
            .without_period()
            .with_max_rows(2),
    );

    // Two writes -> auto-flush on second commit (after threshold reached).
    inserter.write(SimpleRow::new(1, "a")).await.unwrap();
    inserter.write(SimpleRow::new(2, "b")).await.unwrap();

    let rows: Vec<SimpleRow> = recorder.collect().await;
    inserter.end().await.unwrap();

    assert_eq!(rows, vec![SimpleRow::new(1, "a"), SimpleRow::new(2, "b")]);
}

#[tokio::test]
async fn end_drains_remaining_rows() {
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let recorder = mock.add(test::handlers::record::<SimpleRow>());

    let inserter: AsyncInserter<SimpleRow> = AsyncInserter::new(
        &client,
        "t",
        AsyncInserterConfig::default().without_period(),
    );

    inserter.write(SimpleRow::new(7, "tail")).await.unwrap();
    // No flush -- end must drain.
    let _q = inserter.end().await.unwrap();

    let rows: Vec<SimpleRow> = recorder.collect().await;
    assert_eq!(rows, vec![SimpleRow::new(7, "tail")]);
}

#[tokio::test]
async fn handle_clones_can_write_concurrently() {
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let recorder = mock.add(test::handlers::record::<SimpleRow>());

    let inserter: AsyncInserter<SimpleRow> = AsyncInserter::new(
        &client,
        "t",
        AsyncInserterConfig::default().without_period(),
    );

    let h1 = inserter.handle();
    let h2 = inserter.handle();

    let t1 = tokio::spawn(async move { h1.write(SimpleRow::new(1, "a")).await });
    let t2 = tokio::spawn(async move { h2.write(SimpleRow::new(2, "b")).await });

    t1.await.unwrap().unwrap();
    t2.await.unwrap().unwrap();

    let _q = inserter.end().await.unwrap();
    let mut rows: Vec<SimpleRow> = recorder.collect().await;
    rows.sort_by_key(|r| r.id);
    assert_eq!(rows, vec![SimpleRow::new(1, "a"), SimpleRow::new(2, "b")]);
}

#[tokio::test]
async fn multi_table_write_to_routes_per_table() {
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let rec_a = mock.add(test::handlers::record::<SimpleRow>());
    let rec_b = mock.add(test::handlers::record::<SimpleRow>());

    let inserter: AsyncInserter<SimpleRow> = AsyncInserter::new_multi_table(
        &client,
        AsyncInserterConfig::default().without_period(),
    );

    // Mock handlers are matched in registration order; mock.add() FIFO
    // matches each handler to one HTTP request. Two tables -> two
    // separate INSERT requests.
    inserter.write_to("table_a", SimpleRow::new(1, "a")).await.unwrap();
    inserter.write_to("table_b", SimpleRow::new(2, "b")).await.unwrap();

    let _q = inserter.end().await.unwrap();

    let rows_a: Vec<SimpleRow> = rec_a.collect().await;
    let rows_b: Vec<SimpleRow> = rec_b.collect().await;
    assert_eq!(rows_a, vec![SimpleRow::new(1, "a")]);
    assert_eq!(rows_b, vec![SimpleRow::new(2, "b")]);
}

#[tokio::test]
async fn multi_table_write_default_is_rejected() {
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);

    let inserter: AsyncInserter<SimpleRow> = AsyncInserter::new_multi_table(
        &client,
        AsyncInserterConfig::default().without_period(),
    );

    // No default table -> write() must error.
    let result = inserter.write(SimpleRow::new(1, "a")).await;
    assert!(result.is_err(), "expected error from write() in multi-table mode");

    let _ = inserter.end().await;
}

#[tokio::test]
async fn multi_table_flush_aggregates_quantities() {
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let _rec_a = mock.add(test::handlers::record::<SimpleRow>());
    let _rec_b = mock.add(test::handlers::record::<SimpleRow>());

    let inserter: AsyncInserter<SimpleRow> = AsyncInserter::new_multi_table(
        &client,
        AsyncInserterConfig::default().without_period(),
    );

    inserter.write_to("table_a", SimpleRow::new(1, "a")).await.unwrap();
    inserter.write_to("table_b", SimpleRow::new(2, "b")).await.unwrap();

    let q = inserter.flush().await.unwrap();
    // Two rows total across both tables; the worker sums quantities.
    assert_eq!(q.rows, 2);

    let _ = inserter.end().await;
}

#[tokio::test]
async fn single_table_write_to_other_table_works() {
    // The single-table API still supports `write_to` for ad-hoc
    // routing (the worker just creates an additional buffer).
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let rec_default = mock.add(test::handlers::record::<SimpleRow>());
    let rec_other = mock.add(test::handlers::record::<SimpleRow>());

    let inserter: AsyncInserter<SimpleRow> = AsyncInserter::new(
        &client,
        "default_table",
        AsyncInserterConfig::default().without_period(),
    );

    inserter.write(SimpleRow::new(1, "default")).await.unwrap();
    inserter.write_to("other_table", SimpleRow::new(2, "other")).await.unwrap();

    let _ = inserter.end().await;

    let rows_default: Vec<SimpleRow> = rec_default.collect().await;
    let rows_other: Vec<SimpleRow> = rec_other.collect().await;
    assert_eq!(rows_default, vec![SimpleRow::new(1, "default")]);
    assert_eq!(rows_other, vec![SimpleRow::new(2, "other")]);
}

#[tokio::test]
async fn period_flush_fires_when_quiet() {
    tokio::time::pause();

    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let recorder = mock.add(test::handlers::record::<SimpleRow>());

    let inserter: AsyncInserter<SimpleRow> = AsyncInserter::new(
        &client,
        "t",
        AsyncInserterConfig::default()
            .with_max_period(Duration::from_millis(50))
            .with_max_rows(u64::MAX)
            .with_max_bytes(u64::MAX),
    );

    inserter.write(SimpleRow::new(42, "tick")).await.unwrap();
    // Advance past the period so the worker's on_idle fires and
    // commits. Awaiting `recorder.collect()` next is deterministic --
    // its underlying oneshot only fires once the mock handler has
    // processed the worker's HTTP request, which only happens after
    // the period-triggered flush. No `yield_now` hack needed.
    tokio::time::advance(Duration::from_millis(200)).await;

    let rows: Vec<SimpleRow> = recorder.collect().await;
    assert_eq!(rows, vec![SimpleRow::new(42, "tick")]);

    // Clean shutdown after the period-flush has been proved by the
    // recorder above.
    let _ = inserter.end().await;
}

// ---------------------------------------------------------------------------
// Failure semantics (architecture.md section 11)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multi_table_flush_first_error_aborts_atomically() {
    // Two tables: table_a's INSERT succeeds, table_b's returns 500.
    // The flush() must return Err -- caller learns the batch did not
    // fully land. Per architecture.md section 11.4 the rows for table_a may
    // already be in CH; replay safety relies on server-side dedup.
    use hyper::StatusCode;
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    // HTTP requests open in write order (table_a first, table_b
    // second), so the mock's FIFO handlers map cleanly:
    // request 1 = table_a -> record handler; request 2 = table_b
    // -> failure handler. The worker's flush iterates per-table
    // inserters in `BTreeMap` order (lexicographic), which agrees
    // with the write order here -- so table_a's commit succeeds and
    // then table_b's 500 aborts the flush.
    let _rec_a = mock.add(test::handlers::record::<SimpleRow>());
    let _ = mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));

    let inserter: AsyncInserter<SimpleRow> = AsyncInserter::new_multi_table(
        &client,
        AsyncInserterConfig::default().without_period(),
    );

    inserter
        .write_to("table_a", SimpleRow::new(1, "a"))
        .await
        .unwrap();
    inserter
        .write_to("table_b", SimpleRow::new(2, "b"))
        .await
        .unwrap();

    let result = inserter.flush().await;
    assert!(
        result.is_err(),
        "flush should fail atomically when any table's INSERT errors"
    );

    let _ = inserter.end().await;
}

#[tokio::test]
async fn auto_commit_error_propagates_to_write_caller() {
    // When a single-table insert exercises a max_rows threshold of 1,
    // every write triggers an auto-commit. If that auto-commit fails
    // the write's caller must see the error -- previously the result
    // was silently swallowed.
    use hyper::StatusCode;
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let _ = mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));

    let inserter: AsyncInserter<SimpleRow> = AsyncInserter::new(
        &client,
        "tiny",
        AsyncInserterConfig::default()
            .without_period()
            .with_max_rows(1)
            .with_max_bytes(u64::MAX),
    );

    // The single write triggers commit() (max_rows=1 reached), and
    // that commit hits the 500 mock -- the caller's write() must
    // surface the error.
    let result = inserter.write(SimpleRow::new(1, "boom")).await;
    assert!(
        result.is_err(),
        "auto-commit failure during write must propagate to caller"
    );

    // Drop without expecting end() success: the worker is still alive
    // but its inner Insert<T> was aborted; subsequent commands behave
    // sanely.
    let _ = inserter.end().await;
}

// ---------------------------------------------------------------------------
// Writer-id injection (mitigates ClickHouse#86651 flush poisoning)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn writer_id_auto_injects_log_comment_query_param() {
    // Default AsyncInserterConfig has WriterId::Auto, so every INSERT
    // should carry a log_comment=clickhouse-rs:async_inserter:* query
    // parameter. This is the per-writer queue-splitting mitigation for
    // async_insert flush poisoning (https://github.com/ClickHouse/ClickHouse/issues/86651).
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let recorder = mock.add(test::handlers::record_with_uri::<SimpleRow>());

    let inserter: AsyncInserter<SimpleRow> = AsyncInserter::new(
        &client,
        "t",
        AsyncInserterConfig::default().without_period(),
    );

    inserter.write(SimpleRow::new(1, "a")).await.unwrap();
    inserter.flush().await.unwrap();
    let (uri, _rows): (String, Vec<SimpleRow>) = recorder.collect().await;
    inserter.end().await.unwrap();

    assert!(
        uri.contains("log_comment=clickhouse-rs"),
        "auto-generated log_comment should appear in the request URI; got: {uri}"
    );
    assert!(
        uri.contains("async_inserter"),
        "auto-generated log_comment should carry the async_inserter tag; got: {uri}"
    );
}

#[tokio::test]
async fn writer_id_custom_value_appears_verbatim() {
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let recorder = mock.add(test::handlers::record_with_uri::<SimpleRow>());

    let inserter: AsyncInserter<SimpleRow> = AsyncInserter::new(
        &client,
        "t",
        AsyncInserterConfig::default()
            .without_period()
            .with_writer_id("my-app/shard-7"),
    );

    inserter.write(SimpleRow::new(1, "a")).await.unwrap();
    inserter.flush().await.unwrap();
    let (uri, _rows): (String, Vec<SimpleRow>) = recorder.collect().await;
    inserter.end().await.unwrap();

    // URL-encoded form of "my-app/shard-7" is "my-app%2Fshard-7".
    // Either appears -- depending on how the client encodes query params.
    assert!(
        uri.contains("log_comment=my-app") && uri.contains("shard-7"),
        "custom writer_id should round-trip into log_comment param; got: {uri}"
    );
}

#[tokio::test]
async fn writer_id_disabled_omits_log_comment() {
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let recorder = mock.add(test::handlers::record_with_uri::<SimpleRow>());

    let inserter: AsyncInserter<SimpleRow> = AsyncInserter::new(
        &client,
        "t",
        AsyncInserterConfig::default()
            .without_period()
            .without_writer_id(),
    );

    inserter.write(SimpleRow::new(1, "a")).await.unwrap();
    inserter.flush().await.unwrap();
    let (uri, _rows): (String, Vec<SimpleRow>) = recorder.collect().await;
    inserter.end().await.unwrap();

    assert!(
        !uri.contains("log_comment="),
        "without_writer_id() should not inject log_comment; got: {uri}"
    );
}

#[tokio::test]
async fn writer_id_shared_across_multi_table_inserters() {
    // All per-table inserters in one AsyncInserter share the same
    // writer_id, so the server-side flush queue gets one partition
    // per AsyncInserter instance (not per table).
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let rec_a = mock.add(test::handlers::record_with_uri::<SimpleRow>());
    let rec_b = mock.add(test::handlers::record_with_uri::<SimpleRow>());

    let inserter: AsyncInserter<SimpleRow> = AsyncInserter::new_multi_table(
        &client,
        AsyncInserterConfig::default()
            .without_period()
            .with_writer_id("shared-id"),
    );

    inserter
        .write_to("table_a", SimpleRow::new(1, "a"))
        .await
        .unwrap();
    inserter
        .write_to("table_b", SimpleRow::new(2, "b"))
        .await
        .unwrap();
    inserter.end().await.unwrap();

    let (uri_a, _): (String, Vec<SimpleRow>) = rec_a.collect().await;
    let (uri_b, _): (String, Vec<SimpleRow>) = rec_b.collect().await;
    assert!(uri_a.contains("log_comment=shared-id"));
    assert!(uri_b.contains("log_comment=shared-id"));
}

#[tokio::test]
async fn cross_table_watermark_triggers_flush_across_all_tables() {
    // Many small writes across multiple tables. No single table
    // trips its per-table threshold, but aggregate bytes exceed
    // the cross-table watermark -> all tables flush together.
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock).with_validation(false);
    let rec_a = mock.add(test::handlers::record::<SimpleRow>());
    let rec_b = mock.add(test::handlers::record::<SimpleRow>());

    // Per-table thresholds set absurdly high so they never trip.
    // Cross-table watermark intentionally tiny so a couple of writes
    // overflow it.
    let inserter: AsyncInserter<SimpleRow> = AsyncInserter::new_multi_table(
        &client,
        AsyncInserterConfig::default()
            .without_period()
            .with_max_rows(u64::MAX)
            .with_max_bytes(u64::MAX)
            .with_cross_table_max_bytes(1),
    );

    inserter
        .write_to("table_a", SimpleRow::new(1, "a"))
        .await
        .unwrap();
    inserter
        .write_to("table_b", SimpleRow::new(2, "b"))
        .await
        .unwrap();
    // No explicit flush; the watermark check inside write_one should
    // have already flushed both tables.
    let rows_a: Vec<SimpleRow> = rec_a.collect().await;
    let rows_b: Vec<SimpleRow> = rec_b.collect().await;
    inserter.end().await.unwrap();

    assert_eq!(rows_a, vec![SimpleRow::new(1, "a")]);
    assert_eq!(rows_b, vec![SimpleRow::new(2, "b")]);
}

// ---------------------------------------------------------------------------
// Per-commit observability callback
// ---------------------------------------------------------------------------

#[tokio::test]
async fn commit_callback_fires_per_flush_with_table_name_and_quantities() {
    use std::sync::Arc;
    use std::sync::Mutex;

    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let _rec = mock.add(test::handlers::record::<SimpleRow>());

    let log: Arc<Mutex<Vec<(String, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let log_for_cb = Arc::clone(&log);

    let inserter: AsyncInserter<SimpleRow> = AsyncInserter::new(
        &client,
        "t",
        AsyncInserterConfig::default()
            .without_period()
            .with_commit_callback(move |table, q| {
                log_for_cb
                    .lock()
                    .unwrap()
                    .push((table.to_string(), q.rows));
            }),
    );

    inserter.write(SimpleRow::new(1, "a")).await.unwrap();
    inserter.write(SimpleRow::new(2, "b")).await.unwrap();
    inserter.flush().await.unwrap();
    inserter.end().await.unwrap();

    let entries = log.lock().unwrap().clone();
    // Exactly one non-zero commit (the flush). end() on a freshly
    // flushed inserter has zero rows pending so it doesn't fire.
    assert!(
        !entries.is_empty(),
        "commit callback should fire at least once"
    );
    for (table, _rows) in &entries {
        assert_eq!(table, "t");
    }
    let total_rows: u64 = entries.iter().map(|(_, r)| r).sum();
    assert_eq!(total_rows, 2, "callback saw {entries:?}");
}

#[tokio::test]
async fn commit_callback_distinguishes_tables_in_multi_table_mode() {
    use std::sync::Arc;
    use std::sync::Mutex;

    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let _rec_a = mock.add(test::handlers::record::<SimpleRow>());
    let _rec_b = mock.add(test::handlers::record::<SimpleRow>());

    let by_table: Arc<Mutex<std::collections::HashMap<String, u64>>> =
        Arc::new(Mutex::new(std::collections::HashMap::new()));
    let by_table_for_cb = Arc::clone(&by_table);

    let inserter: AsyncInserter<SimpleRow> = AsyncInserter::new_multi_table(
        &client,
        AsyncInserterConfig::default()
            .without_period()
            .with_commit_callback(move |table, q| {
                *by_table_for_cb
                    .lock()
                    .unwrap()
                    .entry(table.to_string())
                    .or_insert(0) += q.rows;
            }),
    );

    inserter
        .write_to("table_a", SimpleRow::new(1, "a"))
        .await
        .unwrap();
    inserter
        .write_to("table_b", SimpleRow::new(2, "b"))
        .await
        .unwrap();
    inserter.flush().await.unwrap();
    inserter.end().await.unwrap();

    let final_state = by_table.lock().unwrap().clone();
    assert_eq!(final_state.get("table_a"), Some(&1));
    assert_eq!(final_state.get("table_b"), Some(&1));
}
