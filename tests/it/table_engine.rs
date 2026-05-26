//! Mock-based tests for `Client::table_engine` and
//! `Client::is_distributed_table`.
//!
//! These methods consult `system.tables`. The mock framework can
//! return arbitrary row data; we configure it to return the engine
//! name for the next query.

#![cfg(feature = "test-util")]

use clickhouse::{Client, test};

#[tokio::test]
async fn table_engine_returns_engine_string() {
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);

    mock.add(test::handlers::provide(vec!["MergeTree".to_string()]));

    let engine = client.table_engine("events").await.unwrap();
    assert_eq!(engine, "MergeTree");
}

#[tokio::test]
async fn is_distributed_table_recognises_distributed_engine() {
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);

    mock.add(test::handlers::provide(vec!["Distributed".to_string()]));

    assert!(client.is_distributed_table("events_dist").await.unwrap());
}

#[tokio::test]
async fn is_distributed_table_returns_false_for_replicated_merge_tree() {
    // ReplicatedMergeTree fans out to replicas (within a shard) but
    // NOT to multiple shards -- row attribution is coordinator-batch-
    // local, NOT shard-local. So is_distributed_table = false.
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);

    mock.add(test::handlers::provide(
        vec!["ReplicatedMergeTree".to_string()],
    ));

    assert!(!client.is_distributed_table("events").await.unwrap());
}

#[tokio::test]
async fn is_distributed_table_returns_false_for_plain_merge_tree() {
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);

    mock.add(test::handlers::provide(vec!["MergeTree".to_string()]));

    assert!(!client.is_distributed_table("events").await.unwrap());
}

#[tokio::test]
async fn table_engine_accepts_qualified_table_name() {
    // `"db.name"` form should pull the database from the qualifier,
    // not the Client default.
    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_database("default_db");

    mock.add(test::handlers::provide(vec!["Distributed".to_string()]));

    let engine = client.table_engine("other_db.users").await.unwrap();
    assert_eq!(engine, "Distributed");
}
