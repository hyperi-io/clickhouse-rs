//! Live-ClickHouse integration test for the atomic-batch-failure
//! premise that `Client::insert_batch_with_isolation` is built on.
//!
//! The mock-based suite in `batch_isolation.rs` proves the bisection
//! algorithm is correct given the assumed server behaviour. This file
//! proves the assumption itself: ClickHouse rejects a binary-format
//! INSERT *atomically* when any row violates a server-side constraint,
//! so bisection can isolate the failing rows.
//!
//! Skipped (with a clear message) when `CLICKHOUSE_URL` is unset, so
//! the suite stays green in environments without a live CH.

use std::env;

use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
struct Constrained {
    x: u32,
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

/// Atomic batch failure via a CHECK constraint.
///
/// Inserts 4 rows where row index 2 violates `x < 10`. Asserts:
///  - `succeeded_rows == 3` (the three good rows landed via bisection)
///  - `failed.len() == 1` (the bad row is isolated)
///  - `round_trips == 5` (matches the documented `one bad in 4` cost)
///  - the per-row error carries the server's constraint message
///
/// The table name is unique per run so concurrent invocations don't
/// race. The table is dropped on the way out (best-effort).
#[tokio::test]
async fn atomic_batch_failure_isolates_one_bad_row_via_check_constraint() {
    let Some(client) = live_client() else {
        eprintln!(
            "skipping: CLICKHOUSE_URL unset (live-CH test). Set \
             CLICKHOUSE_URL + USER/PASSWORD/DATABASE to run."
        );
        return;
    };

    let table = format!("_it_batch_iso_{}", Uuid::new_v4().simple());
    let create = format!(
        "CREATE TABLE IF NOT EXISTS {table} (x UInt32, CONSTRAINT x_lt_10 CHECK x < 10) \
         ENGINE = MergeTree ORDER BY tuple()"
    );
    client.query(&create).execute().await.expect("CREATE TABLE");

    // Best-effort cleanup; runs even on panic via Drop on the guard.
    struct Drop<'a> {
        client: &'a Client,
        table: String,
    }
    impl<'a> std::ops::Drop for Drop<'a> {
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
    let _cleanup = Drop {
        client: &client,
        table: table.clone(),
    };

    let rows = vec![
        Constrained { x: 1 },
        Constrained { x: 2 },
        Constrained { x: 100 }, // violates x < 10
        Constrained { x: 4 },
    ];

    let result = client
        .insert_batch_with_isolation::<Constrained>(&table, rows)
        .await
        .expect("insert_batch_with_isolation should not return Err for per-row failures");

    assert_eq!(result.succeeded_rows, 3, "three good rows should land");
    assert_eq!(result.failed.len(), 1, "the one bad row should be isolated");
    assert_eq!(
        result.failed[0].0.x, 100,
        "the failing row should carry x=100"
    );
    assert_eq!(
        result.round_trips, 5,
        "one-bad-in-four bisection trace is 5 round trips"
    );

    // Verify the per-row error came from the server's CHECK violation
    // rather than a transport error.
    let err = &result.failed[0].1;
    let msg = format!("{err}");
    assert!(
        msg.contains("violated") || msg.contains("Constraint"),
        "per-row error should reference the CHECK violation; got: {msg}"
    );

    // Cross-check: query the table and confirm the three landed rows
    // are the expected values.
    let landed: Vec<u32> = client
        .query(&format!("SELECT x FROM {table} ORDER BY x"))
        .fetch_all::<u32>()
        .await
        .expect("SELECT");
    assert_eq!(landed, vec![1, 2, 4]);
}
