//! Live-ClickHouse integration test for Phase 2 Native-format wire
//! round-trip correctness.
//!
//! The mock-based suite in `insert_native.rs` verifies the request
//! body matches the Native format byte-for-byte. This file proves
//! that what we write the server can read back into the same Rust
//! struct -- end-to-end wire compatibility.
//!
//! Skipped (with a clear message) when `CLICKHOUSE_URL` is unset.

use std::env;

use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
struct ScalarRow {
    id: u64,
    name: String,
    value: i32,
    flag: bool,
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

async fn create_scalar_table(client: &Client, table: &str) {
    client
        .query(&format!(
            "CREATE TABLE {table} \
             (id UInt64, name String, value Int32, flag Bool) \
             ENGINE = MergeTree ORDER BY id"
        ))
        .execute()
        .await
        .expect("CREATE TABLE");
}

fn make_rows(n: u32) -> Vec<ScalarRow> {
    (0..n)
        .map(|i| ScalarRow {
            id: i as u64,
            name: format!("row-{i}"),
            value: i as i32 * 7 - 1000,
            flag: i % 2 == 0,
        })
        .collect()
}

#[tokio::test]
async fn insert_native_then_select_matches_byte_for_byte() {
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    let table = format!("_it_nr_scalar_{}", Uuid::new_v4().simple());
    create_scalar_table(&client, &table).await;
    let _cleanup = Cleanup {
        client: &client,
        table: table.clone(),
    };

    let rows = make_rows(5);

    // Write via Native format.
    let mut inserter = client
        .insert_native::<ScalarRow>(&table)
        .await
        .expect("insert_native init");
    for row in &rows {
        inserter.write(row).await.expect("native write");
    }
    inserter.end().await.expect("native end");

    // Read back via standard RowBinary fetch path.
    let read: Vec<ScalarRow> = client
        .query(&format!("SELECT ?fields FROM {table} ORDER BY id"))
        .fetch_all::<ScalarRow>()
        .await
        .expect("SELECT");

    assert_eq!(read, rows, "Native round-trip should be byte-perfect");
}

#[tokio::test]
async fn chunked_blocks_round_trip_intact() {
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    let table = format!("_it_nr_chunked_{}", Uuid::new_v4().simple());
    create_scalar_table(&client, &table).await;
    let _cleanup = Cleanup {
        client: &client,
        table: table.clone(),
    };

    // Force multi-block: 10 rows per block, 25 rows total -> 3 blocks
    // (10, 10, 5).
    let rows = make_rows(25);

    let mut inserter = client
        .insert_native::<ScalarRow>(&table)
        .await
        .expect("insert_native init")
        .with_max_rows_per_block(10);
    for row in &rows {
        inserter.write(row).await.expect("native write");
    }
    inserter.end().await.expect("native end");

    let read: Vec<ScalarRow> = client
        .query(&format!("SELECT ?fields FROM {table} ORDER BY id"))
        .fetch_all::<ScalarRow>()
        .await
        .expect("SELECT");

    assert_eq!(read.len(), 25, "all 25 rows landed");
    assert_eq!(read, rows, "chunked round-trip should be byte-perfect");
}

#[tokio::test]
async fn empty_native_insert_lands_zero_rows() {
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    let table = format!("_it_nr_empty_{}", Uuid::new_v4().simple());
    create_scalar_table(&client, &table).await;
    let _cleanup = Cleanup {
        client: &client,
        table: table.clone(),
    };

    let inserter = client
        .insert_native::<ScalarRow>(&table)
        .await
        .expect("insert_native init");
    // No writes -- just end.
    inserter.end().await.expect("end");

    let count: u64 = client
        .query(&format!("SELECT count() FROM {table}"))
        .fetch_one::<u64>()
        .await
        .expect("count");
    assert_eq!(count, 0, "empty insert should land zero rows");
}

#[tokio::test]
async fn insert_with_native_format_handoff_round_trips() {
    // Use the Insert<T>::with_native_format() path -- caller built
    // an `Insert<T>` first then switched to Native. The result should
    // be wire-equivalent to insert_native::<T>().
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    let table = format!("_it_nr_handoff_{}", Uuid::new_v4().simple());
    create_scalar_table(&client, &table).await;
    let _cleanup = Cleanup {
        client: &client,
        table: table.clone(),
    };

    let rows = make_rows(3);

    let insert = client
        .insert::<ScalarRow>(&table)
        .await
        .expect("Insert init");
    let mut inserter = insert
        .with_native_format()
        .expect("with_native_format handoff");
    for row in &rows {
        inserter.write(row).await.expect("native write");
    }
    inserter.end().await.expect("native end");

    let read: Vec<ScalarRow> = client
        .query(&format!("SELECT ?fields FROM {table} ORDER BY id"))
        .fetch_all::<ScalarRow>()
        .await
        .expect("SELECT");
    assert_eq!(read, rows, "handoff path must round-trip identically");
}
