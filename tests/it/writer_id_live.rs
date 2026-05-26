//! Live-ClickHouse integration test for the writer-id mitigation of
//! [ClickHouse#86651](https://github.com/ClickHouse/ClickHouse/issues/86651).
//!
//! The mock-based suite in `async_inserter.rs` (tests
//! `writer_id_auto_injects_log_comment_query_param`,
//! `writer_id_custom_value_appears_verbatim`,
//! `writer_id_disabled_omits_log_comment`) proves the client sends
//! `log_comment` as a URL parameter. This file proves the *server*
//! records it -- the partition key that ClickHouse uses to segregate
//! the async_insert flush queue is settings_hash, which is derived
//! from the parameters we send.
//!
//! Verified by reading back `system.query_log.log_comment` after each
//! INSERT and asserting it matches the [`WriterId`] strategy.
//!
//! Skipped (with a clear message) when `CLICKHOUSE_URL` is unset.

use std::env;
use std::time::Duration;

use clickhouse::async_inserter::{AsyncInserter, AsyncInserterConfig};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
struct LiveRow {
    id: u32,
}

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

/// Drop guard so the unique-name table doesn't accumulate across runs.
struct Cleanup<'a> {
    client: &'a Client,
    table: String,
}

impl<'a> std::ops::Drop for Cleanup<'a> {
    fn drop(&mut self) {
        let client = self.client.clone();
        let table = self.table.clone();
        tokio::spawn(async move {
            let _ = client
                .query(&format!("DROP TABLE IF EXISTS {table}"))
                .execute()
                .await;
        });
    }
}

/// Read back the `log_comment` value the server recorded for INSERTs
/// against `table` in `system.query_log`. Returns the most recent
/// non-empty value, or `Some("")` if a matching entry exists with an
/// empty `log_comment`, or `None` if no entry exists.
async fn read_log_comment(client: &Client, table: &str) -> Option<String> {
    // system.query_log is buffered server-side; force a flush so the
    // entry is visible immediately.
    let _ = client.query("SYSTEM FLUSH LOGS").execute().await;

    // type = 2 is QueryFinish; INSERT may also produce type = 1
    // (QueryStart) and type = 4 (ExceptionWhileProcessing). We pick
    // QueryFinish to ensure the query completed.
    let query = format!(
        "SELECT log_comment FROM system.query_log \
         WHERE query LIKE '%{table}%' AND type = 2 \
         ORDER BY event_time DESC LIMIT 1"
    );
    client.query(&query).fetch_one::<String>().await.ok()
}

async fn insert_one_row(inserter: AsyncInserter<LiveRow>) {
    inserter.write(LiveRow { id: 1 }).await.expect("write");
    inserter.end().await.expect("end");
}

#[tokio::test]
async fn writer_id_auto_records_default_prefix_in_query_log() {
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    let table = format!("_it_writer_id_auto_{}", Uuid::new_v4().simple());
    client
        .query(&format!(
            "CREATE TABLE {table} (id UInt32) ENGINE = MergeTree ORDER BY id"
        ))
        .execute()
        .await
        .expect("CREATE TABLE");
    let _cleanup = Cleanup {
        client: &client,
        table: table.clone(),
    };

    // Default config = WriterId::Auto.
    let inserter: AsyncInserter<LiveRow> = AsyncInserter::new(
        &client,
        &table,
        AsyncInserterConfig::default().without_period(),
    );
    insert_one_row(inserter).await;

    // system.query_log flush may lag the INSERT. Retry a few times
    // before giving up.
    let mut log_comment = None;
    for _ in 0..5 {
        log_comment = read_log_comment(&client, &table).await;
        if log_comment.as_deref().is_some_and(|s| !s.is_empty()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let value = log_comment.expect("query_log entry should exist");
    assert!(
        value.starts_with("clickhouse-rs:async_inserter:"),
        "expected auto writer_id prefix, got: {value:?}"
    );
}

#[tokio::test]
async fn writer_id_custom_records_exact_value_in_query_log() {
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    let table = format!("_it_writer_id_custom_{}", Uuid::new_v4().simple());
    client
        .query(&format!(
            "CREATE TABLE {table} (id UInt32) ENGINE = MergeTree ORDER BY id"
        ))
        .execute()
        .await
        .expect("CREATE TABLE");
    let _cleanup = Cleanup {
        client: &client,
        table: table.clone(),
    };

    let custom = format!("hyperi-shard-{}", Uuid::new_v4().simple());
    let inserter: AsyncInserter<LiveRow> = AsyncInserter::new(
        &client,
        &table,
        AsyncInserterConfig::default()
            .without_period()
            .with_writer_id(custom.clone()),
    );
    insert_one_row(inserter).await;

    let mut log_comment = None;
    for _ in 0..5 {
        log_comment = read_log_comment(&client, &table).await;
        if log_comment.as_deref().is_some_and(|s| !s.is_empty()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(
        log_comment.as_deref(),
        Some(custom.as_str()),
        "expected custom writer_id verbatim"
    );
}

#[tokio::test]
async fn writer_id_disabled_records_empty_log_comment() {
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    let table = format!("_it_writer_id_off_{}", Uuid::new_v4().simple());
    client
        .query(&format!(
            "CREATE TABLE {table} (id UInt32) ENGINE = MergeTree ORDER BY id"
        ))
        .execute()
        .await
        .expect("CREATE TABLE");
    let _cleanup = Cleanup {
        client: &client,
        table: table.clone(),
    };

    let inserter: AsyncInserter<LiveRow> = AsyncInserter::new(
        &client,
        &table,
        AsyncInserterConfig::default()
            .without_period()
            .without_writer_id(),
    );
    insert_one_row(inserter).await;

    // Allow query_log to flush. Empty log_comment is the EXPECTED
    // outcome here, so we wait the full retry window without breaking
    // on first read.
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let log_comment = read_log_comment(&client, &table).await;
    assert_eq!(
        log_comment.as_deref(),
        Some(""),
        "WriterId::Disabled should produce empty log_comment"
    );
}
