//! Live-ClickHouse integration test for per-row serde isolation
//! along the `serialize_with_validation` path
//! (`Client::with_validation(true)`).
//!
//! Complements `row_serde_isolation.rs` (mock-based, covers the
//! plain `serialize_row_binary` path). With validation ON, the
//! inserter sends a DESCRIBE TABLE query before each INSERT and the
//! serialiser produces RowBinaryWithNamesAndTypes payloads matching
//! the server's column metadata. This test proves that
//! `Insert::do_write`'s buffer-truncate-on-serde-error behaviour
//! also holds on the validation path, not just the plain path.
//!
//! Skipped (with a clear message) when `CLICKHOUSE_URL` is unset.

use std::env;

use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize, Serializer, ser::SerializeStruct};
use uuid::Uuid;

/// A row whose `Serialize` impl returns an error when `field == u32::MAX`.
/// Mirrors the helper in `row_serde_isolation.rs`.
#[derive(Row, Deserialize, Debug, PartialEq, Clone)]
struct MaybeFailRow {
    id: u32,
    field: u32,
}

impl Serialize for MaybeFailRow {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        let mut st = ser.serialize_struct("MaybeFailRow", 2)?;
        st.serialize_field("id", &self.id)?;
        if self.field == u32::MAX {
            // After serialize_field("id", ...) above, the serialiser
            // has already written 4 bytes for `id`. The Err leaves
            // those bytes orphaned -- pre-fix that would corrupt the
            // wire format for prior rows; post-fix the buffer is
            // truncated back to its pre-write length.
            return Err(serde::ser::Error::custom(
                "simulated row-level serialise failure",
            ));
        }
        st.serialize_field("field", &self.field)?;
        st.end()
    }
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

#[tokio::test]
async fn bad_row_does_not_poison_prior_rows_under_validation() {
    let Some(client) = live_client() else {
        eprintln!(
            "skipping: CLICKHOUSE_URL unset (live-CH test). Set \
             CLICKHOUSE_URL + USER/PASSWORD/DATABASE to run."
        );
        return;
    };

    let client = client.with_validation(true);
    let table = format!("_it_row_serde_iso_live_{}", Uuid::new_v4().simple());
    let create = format!(
        "CREATE TABLE IF NOT EXISTS {table} (id UInt32, field UInt32) \
         ENGINE = MergeTree ORDER BY id"
    );
    client.query(&create).execute().await.expect("CREATE TABLE");

    // Best-effort cleanup.
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

    let mut insert = client
        .insert::<MaybeFailRow>(&table)
        .await
        .expect("insert init");

    // Row 1 -- good.
    insert
        .write(&MaybeFailRow { id: 1, field: 1 })
        .await
        .expect("row 1 should succeed");
    // Row 2 -- bad (serde Err). Must error WITHOUT poisoning row 1's
    // bytes already in the buffer.
    let result = insert
        .write(&MaybeFailRow {
            id: 2,
            field: u32::MAX,
        })
        .await;
    assert!(result.is_err(), "row 2 should fail serde");
    // Row 3 -- good. Must succeed despite row 2's failure.
    insert
        .write(&MaybeFailRow { id: 3, field: 3 })
        .await
        .expect("row 3 should succeed after row 2's failure");

    insert.end().await.expect("insert end");

    // Verify the server received rows 1 and 3 only (row 2 was
    // rejected during serialise; its orphaned bytes were truncated).
    let rows: Vec<MaybeFailRow> = client
        .query(&format!("SELECT ?fields FROM {table} ORDER BY id"))
        .fetch_all::<MaybeFailRow>()
        .await
        .expect("SELECT");
    assert_eq!(
        rows,
        vec![
            MaybeFailRow { id: 1, field: 1 },
            MaybeFailRow { id: 3, field: 3 },
        ]
    );
}
