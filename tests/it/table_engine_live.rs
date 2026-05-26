//! Live-ClickHouse integration test for table_engine + is_distributed_table
//! (`Client::table_engine`, `Client::is_distributed_table`).
//!
//! The mock-based suite in `table_engine.rs` proves the SQL we issue
//! against `system.tables` produces the expected URI shape. This
//! file proves the *server* returns the engine string we expect for
//! each engine type, and that `is_distributed_table` correctly
//! distinguishes Distributed from non-Distributed.
//!
//! Skipped (with a clear message) when `CLICKHOUSE_URL` is unset.

use std::env;

use clickhouse::Client;
use uuid::Uuid;

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

#[tokio::test]
async fn table_engine_returns_mergetree_for_mergetree() {
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    let table = format!("_it_te_mt_{}", Uuid::new_v4().simple());
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

    let engine = client.table_engine(&table).await.expect("table_engine");
    assert_eq!(engine, "MergeTree", "MergeTree table reports MergeTree");

    let is_dist = client
        .is_distributed_table(&table)
        .await
        .expect("is_distributed_table");
    assert!(!is_dist, "MergeTree is NOT Distributed");
}

#[tokio::test]
async fn table_engine_returns_memory_for_memory() {
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    let table = format!("_it_te_mem_{}", Uuid::new_v4().simple());
    client
        .query(&format!("CREATE TABLE {table} (id UInt32) ENGINE = Memory"))
        .execute()
        .await
        .expect("CREATE TABLE");
    let _cleanup = Cleanup {
        client: &client,
        table: table.clone(),
    };

    let engine = client.table_engine(&table).await.expect("table_engine");
    assert_eq!(engine, "Memory");
}

#[tokio::test]
async fn table_engine_errors_for_missing_table() {
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    let bogus = format!("_it_te_missing_{}", Uuid::new_v4().simple());
    let result = client.table_engine(&bogus).await;
    assert!(
        result.is_err(),
        "missing table should produce an error; got: {result:?}"
    );
}

#[tokio::test]
async fn table_engine_resolves_qualified_database_name() {
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    // Use the client's resolved database (or "default"). Then call
    // table_engine with the QUALIFIED name even though that's the
    // current database; the implementation must accept both forms.
    let database = env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".into());
    let table = format!("_it_te_qual_{}", Uuid::new_v4().simple());
    client
        .query(&format!(
            "CREATE TABLE {database}.{table} (id UInt32) ENGINE = MergeTree ORDER BY id"
        ))
        .execute()
        .await
        .expect("CREATE TABLE");
    let _cleanup = Cleanup {
        client: &client,
        table: format!("{database}.{table}"),
    };

    let engine = client
        .table_engine(&format!("{database}.{table}"))
        .await
        .expect("qualified table_engine");
    assert_eq!(engine, "MergeTree");
}

#[tokio::test]
async fn is_distributed_table_true_for_distributed_engine() {
    // This test requires a configured cluster on the server. If
    // `default_cluster` (the typical test config) is not available,
    // the CREATE TABLE fails -- in that case we still pass (the
    // check is best-effort live; the mock test asserts the SQL
    // shape unconditionally).
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    let database = env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".into());
    let local = format!("_it_te_dist_local_{}", Uuid::new_v4().simple());
    let dist = format!("_it_te_dist_d_{}", Uuid::new_v4().simple());

    client
        .query(&format!(
            "CREATE TABLE {local} (id UInt32) ENGINE = MergeTree ORDER BY id"
        ))
        .execute()
        .await
        .expect("CREATE TABLE local");
    let _cleanup_local = Cleanup {
        client: &client,
        table: local.clone(),
    };

    let dist_ddl = format!(
        "CREATE TABLE {dist} AS {local} \
         ENGINE = Distributed('default_cluster', {database}, {local})"
    );
    let create_dist = client.query(&dist_ddl).execute().await;
    if create_dist.is_err() {
        eprintln!(
            "skipping Distributed assertion: no `default_cluster` configured on server"
        );
        return;
    }
    let _cleanup_dist = Cleanup {
        client: &client,
        table: dist.clone(),
    };

    let is_dist = client
        .is_distributed_table(&dist)
        .await
        .expect("is_distributed_table on Distributed");
    assert!(is_dist, "Distributed table should report true");
}
