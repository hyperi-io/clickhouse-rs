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
