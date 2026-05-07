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
    // Mock handlers are matched FIFO. The order of HTTP requests
    // depends on `HashMap` iteration; we register both record + a
    // failure so whichever fires first records and whichever fires
    // second gets 500. Either way the flush observes one success +
    // one failure.
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
