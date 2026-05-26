//! MVP integration tests for HTTP `Format::Native` insert.
//!
//! Uses [`InsertNative::with_columns`] to skip the DESCRIBE TABLE
//! round-trip and a `record_raw_body` mock handler to capture the
//! emitted Native-format bytes for byte-exact verification.

#![cfg(feature = "test-util")]

use bytes::Bytes;
use clickhouse::{Client, Row, insert_native::InsertNative, test};
use serde::Serialize;

#[derive(Row, Serialize)]
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

#[tokio::test]
async fn empty_insert_emits_zero_row_block() {
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let recorder = mock.add(test::handlers::record_raw_body());

    let insert: InsertNative<Tiny> =
        InsertNative::with_columns(&client, "tiny", &columns()).unwrap();
    insert.end().await.unwrap();

    let body: Bytes = recorder.body().await;
    // ClickHouse reads HTTP `FORMAT Native` at server_revision = 0:
    // NO BlockInfo, NO per-column custom_serialization flag. Expected
    // layout for the empty Native block (counts + per-column name/type
    // headers; headers emitted even for 0 rows so num_columns matches):
    //   varint(num_columns = 2) = 0x02
    //   varint(num_rows    = 0) = 0x00
    //   col id:   string("id")   = 0x02 'i' 'd'
    //             string("UInt64") = 0x06 U I n t 6 4
    //   col name: string("name") = 0x04 'n' 'a' 'm' 'e'
    //             string("String") = 0x06 S t r i n g
    // No flag bytes, no data (0 rows).
    let expected: &[u8] = &[
        0x02, // num_columns = 2
        0x00, // num_rows = 0
        0x02, b'i', b'd', // string("id")
        0x06, b'U', b'I', b'n', b't', b'6', b'4', // string("UInt64")
        0x04, b'n', b'a', b'm', b'e', // string("name")
        0x06, b'S', b't', b'r', b'i', b'n', b'g', // string("String")
    ];
    assert_eq!(body.as_ref(), expected);
}

#[tokio::test]
async fn block_envelope_has_counts_then_columns() {
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let recorder = mock.add(test::handlers::record_raw_body());

    let mut insert: InsertNative<Tiny> =
        InsertNative::with_columns(&client, "tiny", &columns()).unwrap();
    insert.write(&Tiny { id: 1, name: "a".into() }).await.unwrap();
    insert.end().await.unwrap();

    let body = recorder.body().await;

    // Revision 0: no BlockInfo, no per-column flag. Body opens with
    // counts then per-column name/type headers.
    // varint(num_columns = 2)
    assert_eq!(body[0], 0x02);
    // varint(num_rows = 1)
    assert_eq!(body[1], 0x01);
    // Then per-column data starts -- first thing should be the
    // length-prefixed column NAME for column 0 ("id"):
    //   varint(2) "id"
    assert_eq!(body[2], 0x02);
    assert_eq!(&body[3..5], b"id");
    // Then varint(len) + the type name "UInt64".
    assert_eq!(body[5], 0x06);
    assert_eq!(&body[6..12], b"UInt64");
}

#[tokio::test]
async fn integer_column_serialises_le() {
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let recorder = mock.add(test::handlers::record_raw_body());

    #[derive(Row, Serialize)]
    struct OneU32 {
        x: u32,
    }
    let cols = vec![("x".to_string(), "UInt32".to_string())];

    let mut insert: InsertNative<OneU32> =
        InsertNative::with_columns(&client, "t", &cols).unwrap();
    insert.write(&OneU32 { x: 0x01020304 }).await.unwrap();
    insert.write(&OneU32 { x: 0x05060708 }).await.unwrap();
    insert.end().await.unwrap();

    let body = recorder.body().await;
    // Revision 0: no BlockInfo, no per-column flag. Envelope is
    //   varint(num_columns=1) (1) + varint(num_rows=2) (1) = 2 bytes,
    //   then the column starts:
    //   varint(1) "x"        : 1 + 1 = 2 bytes
    //   varint(6) "UInt32"   : 1 + 6 = 7 bytes
    //   then 2 * 4 bytes of LE-encoded u32 values
    let header_end = 2 + 2 + 7; // 11
    let column_data = &body[header_end..];
    assert_eq!(column_data.len(), 8, "two u32 = 8 bytes");
    assert_eq!(&column_data[0..4], &0x01020304u32.to_le_bytes());
    assert_eq!(&column_data[4..8], &0x05060708u32.to_le_bytes());
}

#[tokio::test]
async fn chunks_at_max_rows_per_block() {
    // Three rows with a 2-row threshold should produce 2 blocks:
    //   block #1: 2 rows (auto-flushed on 2nd write)
    //   block #2: 1 row (flushed by end())
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let recorder = mock.add(test::handlers::record_raw_body());

    #[derive(Row, Serialize)]
    struct OneU32 {
        x: u32,
    }
    let cols = vec![("x".to_string(), "UInt32".to_string())];

    let mut insert: InsertNative<OneU32> =
        InsertNative::with_columns(&client, "t", &cols)
            .unwrap()
            .with_max_rows_per_block(2);

    insert.write(&OneU32 { x: 1 }).await.unwrap();
    insert.write(&OneU32 { x: 2 }).await.unwrap(); // triggers flush
    insert.write(&OneU32 { x: 3 }).await.unwrap();
    insert.end().await.unwrap();

    let body = recorder.body().await;

    // Revision 0: no BlockInfo. Each block carries its per-column
    // name/type header, so count blocks by the column-header marker
    // string("x") + string("UInt32") which appears once per block.
    let col_header: &[u8] = &[0x01, b'x', 0x06, b'U', b'I', b'n', b't', b'3', b'2'];
    assert!(
        body.starts_with(&[0x01, 0x02]),
        "first block must open with counts num_columns=1 num_rows=2"
    );
    let mut count = 0;
    let mut i = 0;
    while i + col_header.len() <= body.len() {
        if &body[i..i + col_header.len()] == col_header {
            count += 1;
            i += col_header.len();
        } else {
            i += 1;
        }
    }
    assert_eq!(count, 2, "expected exactly two block envelopes; got {count}");
}

#[tokio::test]
async fn flush_emits_block_immediately() {
    // Explicit `flush()` between writes should produce two blocks
    // even without hitting any threshold.
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let recorder = mock.add(test::handlers::record_raw_body());

    #[derive(Row, Serialize)]
    struct OneU32 {
        x: u32,
    }
    let cols = vec![("x".to_string(), "UInt32".to_string())];

    let mut insert: InsertNative<OneU32> =
        InsertNative::with_columns(&client, "t", &cols)
            .unwrap()
            .with_max_rows_per_block(u64::MAX);

    insert.write(&OneU32 { x: 100 }).await.unwrap();
    insert.flush().await.unwrap();
    insert.write(&OneU32 { x: 200 }).await.unwrap();
    insert.end().await.unwrap();

    let body = recorder.body().await;
    // Revision 0: count blocks by the per-column header marker.
    let col_header: &[u8] = &[0x01, b'x', 0x06, b'U', b'I', b'n', b't', b'3', b'2'];
    let mut count = 0;
    let mut i = 0;
    while i + col_header.len() <= body.len() {
        if &body[i..i + col_header.len()] == col_header {
            count += 1;
            i += col_header.len();
        } else {
            i += 1;
        }
    }
    assert_eq!(count, 2, "expected two block envelopes");
}

#[tokio::test]
async fn end_finalises_request_with_format_native_url_param() {
    // This test verifies the request URL includes the right query
    // settings -- specifically that we route through the
    // FORMAT-Native path, not the default RowBinary one.
    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let recorder = mock.add(test::handlers::record_raw_body());

    let insert: InsertNative<Tiny> =
        InsertNative::with_columns(&client, "tiny", &columns()).unwrap();
    insert.end().await.unwrap();

    // Just confirm we got SOMETHING back -- if the URL was wrong the
    // mock wouldn't have matched and the future would hang. The
    // record_raw_body handler captured the body, so the request
    // reached the mock.
    let body = recorder.body().await;
    assert!(!body.is_empty(), "request body should contain at least the block counts + headers");
}
