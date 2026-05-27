//! Live integration tests for the TCP-transport `Format::Native` INSERT.
//!
//! `#[ignore]`'d; run with:
//!
//! ```ignore
//! cargo test --features 'tcp test-util' --ignored insert_native_tcp_live
//! ```

#![cfg(all(feature = "tcp", feature = "test-util"))]

use clickhouse::{Client, Row, insert_native::InsertNative};
use serde::{Deserialize, Serialize};

#[derive(Row, Serialize, Deserialize, PartialEq, Debug)]
struct Tiny {
    id: u64,
    name: String,
}

fn columns() -> Vec<(String, String)> {
    vec![
        ("id".to_string(), "UInt64".to_string()),
        ("name".to_string(), "String".to_string()),
    ]
}

fn live_client() -> Client {
    // Construct via Client::tcp so the TCP pool exists; then layer the
    // HTTP URL for SELECT readback. with_tcp_addrs requires the pool
    // already exist.
    let tcp_url = std::env::var("CLICKHOUSE_TCP_URL")
        .unwrap_or_else(|_| "127.0.0.1:9000".to_string());
    let http_url = std::env::var("CLICKHOUSE_URL")
        .unwrap_or_else(|_| "http://localhost:8123".to_string());

    let mut client = Client::tcp(tcp_url).with_url(http_url);
    if let Ok(db) = std::env::var("CLICKHOUSE_DATABASE") {
        client = client.with_database(db);
    }
    if let Ok(user) = std::env::var("CLICKHOUSE_USER") {
        client = client.with_user(user);
    }
    if let Ok(pw) = std::env::var("CLICKHOUSE_PASSWORD") {
        client = client.with_password(pw);
    }
    client
}

#[tokio::test]
#[ignore = "live: requires CH 25.x reachable via CLICKHOUSE_URL + CLICKHOUSE_TCP_URL"]
async fn insert_native_with_columns_tcp_dynamic_roundtrip() {
    let client = live_client();
    let table = "tinies_with_columns_tcp_dynamic_roundtrip";
    let create_sql = format!(
        "CREATE OR REPLACE TABLE {table} (id UInt64, name String) ENGINE = MergeTree ORDER BY id"
    );
    client.query(&create_sql).execute().await.unwrap();

    let mut insert: InsertNative<Tiny> = client
        .insert_native_with_columns::<Tiny>(table, &columns())
        .await
        .unwrap();
    insert.write(&Tiny { id: 1, name: "a".into() }).await.unwrap();
    insert.write(&Tiny { id: 2, name: "b".into() }).await.unwrap();
    insert.write(&Tiny { id: 3, name: "c".into() }).await.unwrap();
    insert.end().await.unwrap();

    let select_sql = format!("SELECT ?fields FROM {table} ORDER BY id");
    let got: Vec<Tiny> = client
        .query(&select_sql)
        .fetch_all::<Tiny>()
        .await
        .unwrap();
    assert_eq!(
        got,
        vec![
            Tiny { id: 1, name: "a".into() },
            Tiny { id: 2, name: "b".into() },
            Tiny { id: 3, name: "c".into() },
        ],
    );

    let drop_sql = format!("DROP TABLE IF EXISTS {table}");
    client.query(&drop_sql).execute().await.unwrap();
}

#[tokio::test]
#[ignore = "live: requires CH 25.x reachable"]
async fn with_columns_tcp_rejects_mismatched_type() {
    let client = live_client();
    let table = "tinies_with_columns_tcp_type_mismatch";
    let create_sql = format!(
        "CREATE OR REPLACE TABLE {table} (id UInt64, name String) ENGINE = MergeTree ORDER BY id"
    );
    client.query(&create_sql).execute().await.unwrap();

    // Caller declares `id` as `UInt32` but the server has it as `UInt64`.
    // Strict-exact validation rejects with type-mismatch wording naming
    // both sides.
    let wrong = vec![
        ("id".to_string(), "UInt32".to_string()),
        ("name".to_string(), "String".to_string()),
    ];
    let err = match InsertNative::<Tiny>::with_columns_tcp(&client, table, &wrong).await {
        Ok(_) => panic!("mismatched column type must reject"),
        Err(e) => e,
    };
    let msg = format!("{err}");
    assert!(
        msg.contains("column 0") && msg.contains("UInt32") && msg.contains("UInt64"),
        "expected position-indexed type mismatch naming both sides, got: {msg}",
    );

    let drop_sql = format!("DROP TABLE IF EXISTS {table}");
    client.query(&drop_sql).execute().await.unwrap();
}

#[tokio::test]
#[ignore = "live: requires CH 25.x reachable"]
async fn typed_tcp_rejects_wrong_struct_names() {
    let client = live_client();
    let table = "tinies_typed_tcp_wrong_names";
    let create_sql = format!(
        "CREATE OR REPLACE TABLE {table} (id UInt64, name String) ENGINE = MergeTree ORDER BY id"
    );
    client.query(&create_sql).execute().await.unwrap();

    // Typed Row with a field name that disagrees with the table column.
    #[derive(Row, Serialize)]
    #[allow(dead_code)]
    struct WrongName {
        id: u64,
        foo: String,
    }

    let err = match client.insert_native::<WrongName>(table).await {
        Ok(_) => panic!("typed-path name mismatch must reject at construction"),
        Err(e) => e,
    };
    let msg = format!("{err}");
    assert!(
        msg.contains("position 1") && msg.contains("foo") && msg.contains("name"),
        "expected position-indexed typed name mismatch, got: {msg}",
    );

    let drop_sql = format!("DROP TABLE IF EXISTS {table}");
    client.query(&drop_sql).execute().await.unwrap();
}
