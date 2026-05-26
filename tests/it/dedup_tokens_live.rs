//! Live-ClickHouse integration test for the dedup-token
//! retry-safety mitigation
//! (`Client::insert_batch_with_isolation_with_token`).
//!
//! The mock-based suite in `batch_isolation.rs` proves the client
//! sends `insert_deduplication_token={token_base}/{start}-{end}` per
//! sub-batch. This file proves the *server* honours those tokens --
//! a retry of the same call with the same `token_base` and same
//! input rows produces zero duplicates server-side.
//!
//! Uses a `MergeTree` table with `non_replicated_deduplication_window`
//! enabled. This avoids requiring ZK/Keeper for ReplicatedMergeTree,
//! at the cost of needing CH 21.9+ for the non-Replicated dedup
//! window support.
//!
//! Skipped (with a clear message) when `CLICKHOUSE_URL` is unset.

use std::env;

use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
struct LiveRow {
    id: u32,
    payload: String,
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

async fn create_dedup_table(client: &Client, table: &str) {
    let ddl = format!(
        "CREATE TABLE {table} (id UInt32, payload String) \
         ENGINE = MergeTree ORDER BY id \
         SETTINGS non_replicated_deduplication_window = 100"
    );
    client.query(&ddl).execute().await.expect("CREATE TABLE");
}

async fn count_rows(client: &Client, table: &str) -> u64 {
    client
        .query(&format!("SELECT count() FROM {table}"))
        .fetch_one::<u64>()
        .await
        .expect("count")
}

fn make_rows(n: u32) -> Vec<LiveRow> {
    (1..=n)
        .map(|id| LiveRow {
            id,
            payload: format!("row-{id}"),
        })
        .collect()
}

#[tokio::test]
async fn replay_with_same_token_dedups_server_side() {
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    let table = format!("_it_dedup_tok_replay_{}", Uuid::new_v4().simple());
    create_dedup_table(&client, &table).await;
    let _cleanup = Cleanup {
        client: &client,
        table: table.clone(),
    };

    let token_base = format!("batch-{}", Uuid::new_v4().simple());
    let rows = make_rows(5);

    // First call lands all 5 rows. Single sub-batch (no failures),
    // so token = `{token_base}/0-4`.
    client
        .insert_batch_with_isolation_with_token(&table, rows.clone(), token_base.clone())
        .await
        .expect("first insert");
    assert_eq!(count_rows(&client, &table).await, 5);

    // Replay with same token_base + same rows. The server must
    // dedup based on the matching insert_deduplication_token, so
    // the row count must stay at 5.
    client
        .insert_batch_with_isolation_with_token(&table, rows.clone(), token_base.clone())
        .await
        .expect("replay insert");
    assert_eq!(
        count_rows(&client, &table).await,
        5,
        "replay with same token_base must produce zero new rows"
    );
}

#[tokio::test]
async fn different_token_base_produces_separate_inserts() {
    // Sanity check: dedup is keyed on the FULL token, not on the
    // row content. Two calls with different `token_base` but
    // identical rows must produce two distinct INSERT batches.
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    let table = format!("_it_dedup_tok_distinct_{}", Uuid::new_v4().simple());
    create_dedup_table(&client, &table).await;
    let _cleanup = Cleanup {
        client: &client,
        table: table.clone(),
    };

    let rows = make_rows(3);
    let token_a = format!("batch-a-{}", Uuid::new_v4().simple());
    let token_b = format!("batch-b-{}", Uuid::new_v4().simple());

    client
        .insert_batch_with_isolation_with_token(&table, rows.clone(), token_a)
        .await
        .expect("insert a");
    client
        .insert_batch_with_isolation_with_token(&table, rows.clone(), token_b)
        .await
        .expect("insert b");

    assert_eq!(
        count_rows(&client, &table).await,
        6,
        "different token bases must NOT dedup"
    );
}

#[tokio::test]
async fn variant_without_token_does_not_dedup_under_replay() {
    // Sanity check: the no-token variant produces no
    // insert_deduplication_token setting, so replays land duplicate
    // rows. Documents the at-most-once semantics of the default
    // `insert_batch_with_isolation` (without the `_with_token`
    // suffix) when callers retry.
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    let table = format!("_it_dedup_tok_notok_{}", Uuid::new_v4().simple());
    create_dedup_table(&client, &table).await;
    let _cleanup = Cleanup {
        client: &client,
        table: table.clone(),
    };

    let rows = make_rows(3);

    client
        .insert_batch_with_isolation(&table, rows.clone())
        .await
        .expect("first insert");
    client
        .insert_batch_with_isolation(&table, rows.clone())
        .await
        .expect("replay insert");

    assert_eq!(
        count_rows(&client, &table).await,
        6,
        "no-token variant must NOT dedup -- caller drives idempotency"
    );
}
