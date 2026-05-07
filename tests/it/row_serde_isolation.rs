//! Tests for the per-row serialise-error isolation in
//! `Insert::do_write` -- proves that a serde failure on row N
//! does NOT poison rows 1..N-1 already in the buffer.
//!
//! Reference: `src/insert.rs::Insert::do_write` (this branch).

#![cfg(feature = "test-util")]

use clickhouse::{Client, Row, test};
use serde::{Deserialize, Serialize, Serializer, ser::SerializeStruct};

/// A row whose `Serialize` impl returns an error when `field == u32::MAX`.
/// `Deserialize` is normal so the recorder mock can decode what landed.
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
            // has already written 4 bytes for `id` to the buffer.
            // This Err leaves those 4 bytes orphaned -- pre-fix that
            // would be flushed as "row N partially serialised before
            // error", corrupting the wire format for prior rows.
            return Err(serde::ser::Error::custom(
                "simulated row-level serialise failure",
            ));
        }
        st.serialize_field("field", &self.field)?;
        st.end()
    }
}

#[tokio::test]
async fn bad_row_does_not_poison_prior_rows_in_buffer() {
    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_validation(false);
    let recorder = mock.add(test::handlers::record::<MaybeFailRow>());

    let mut insert = client.insert::<MaybeFailRow>("t").await.unwrap();

    // Row 1 -- good.
    insert.write(&MaybeFailRow { id: 1, field: 100 }).await.unwrap();
    // Row 2 -- triggers the custom serialiser to err mid-row.
    let bad_row_result = insert.write(&MaybeFailRow {
        id: 2,
        field: u32::MAX,
    }).await;
    assert!(
        bad_row_result.is_err(),
        "bad row's write() must return Err",
    );
    // Row 3 -- good. Must succeed even after the bad row's mid-write
    // failure. Pre-fix this would error because Insert::abort() had
    // killed the request.
    insert.write(&MaybeFailRow { id: 3, field: 300 }).await.unwrap();

    // Finalise the INSERT. Pre-fix this would also error because the
    // request was aborted on the bad-row write.
    insert.end().await.unwrap();

    // The mock should have received rows 1 and 3 (in that order),
    // and NOT row 2 (its bytes were truncated out of the buffer).
    let landed: Vec<MaybeFailRow> = recorder.collect().await;
    assert_eq!(
        landed,
        vec![
            MaybeFailRow { id: 1, field: 100 },
            MaybeFailRow { id: 3, field: 300 },
        ],
        "prior and subsequent rows must land; bad row's bytes must be truncated",
    );
}

#[tokio::test]
async fn bad_row_alone_finalises_to_empty_insert() {
    // Edge case: only one write, and it fails. The buffer truncates
    // back to empty; end() finalises a zero-row INSERT successfully.
    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_validation(false);
    let recorder = mock.add(test::handlers::record::<MaybeFailRow>());

    let mut insert = client.insert::<MaybeFailRow>("t").await.unwrap();
    let result = insert.write(&MaybeFailRow {
        id: 99,
        field: u32::MAX,
    }).await;
    assert!(result.is_err());

    insert.end().await.unwrap();

    let landed: Vec<MaybeFailRow> = recorder.collect().await;
    assert!(
        landed.is_empty(),
        "no rows should land; the only write was bad and got truncated",
    );
}

#[tokio::test]
async fn many_alternating_good_and_bad_rows() {
    // Stress: 5 alternating rows (good, bad, good, bad, good).
    // Only 3 good rows should land; the 2 bads should be silently
    // truncated out of the buffer at write time.
    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_validation(false);
    let recorder = mock.add(test::handlers::record::<MaybeFailRow>());

    let mut insert = client.insert::<MaybeFailRow>("t").await.unwrap();

    let inputs = [
        MaybeFailRow { id: 1, field: 1 },              // good
        MaybeFailRow { id: 2, field: u32::MAX },       // bad
        MaybeFailRow { id: 3, field: 3 },              // good
        MaybeFailRow { id: 4, field: u32::MAX },       // bad
        MaybeFailRow { id: 5, field: 5 },              // good
    ];

    for row in &inputs {
        let result = insert.write(row).await;
        if row.field == u32::MAX {
            assert!(result.is_err(), "row {} must error", row.id);
        } else {
            assert!(result.is_ok(), "row {} must succeed", row.id);
        }
    }

    insert.end().await.unwrap();

    let landed: Vec<MaybeFailRow> = recorder.collect().await;
    assert_eq!(
        landed,
        vec![
            MaybeFailRow { id: 1, field: 1 },
            MaybeFailRow { id: 3, field: 3 },
            MaybeFailRow { id: 5, field: 5 },
        ],
    );
}
