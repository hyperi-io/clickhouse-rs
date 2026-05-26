//! Mock-based tests for `Client::insert_batch_with_isolation`.
//!
//! `with_validation(false)` is used so the inserter doesn't fire a
//! DESCRIBE TABLE before each retry -- otherwise each batch insert
//! would consume an extra mock handler for the metadata query.
//! Schema validation isn't relevant for the bisection logic itself.

#![cfg(feature = "test-util")]

use clickhouse::{Client, test};
use hyper::StatusCode;

use crate::SimpleRow;

#[tokio::test]
async fn clean_batch_takes_one_round_trip() {
    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_validation(false);
    let recorder = mock.add(test::handlers::record::<SimpleRow>());

    let rows = vec![
        SimpleRow::new(1, "a"),
        SimpleRow::new(2, "b"),
        SimpleRow::new(3, "c"),
    ];
    let result = client
        .insert_batch_with_isolation::<SimpleRow>("t", rows.clone())
        .await
        .unwrap();

    assert_eq!(result.succeeded_rows, 3, "all 3 rows should land");
    assert!(result.failed.is_empty(), "no rows should fail");
    assert_eq!(result.round_trips, 1, "clean batch is one round trip");

    // Verify the mock got the expected rows (in the original order).
    let recorded: Vec<SimpleRow> = recorder.collect().await;
    assert_eq!(recorded, rows);
}

#[tokio::test]
async fn all_bad_batch_isolates_each_row() {
    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_validation(false);

    // For an all-fail batch of N=4 the bisection visits:
    //   [1,2,3,4] fail, [1,2] fail, [1] fail, [2] fail,
    //   [3,4] fail, [3] fail, [4] fail.
    // Total: 7 round trips, all failures.
    for _ in 0..7 {
        mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));
    }

    let rows: Vec<SimpleRow> = (1..=4)
        .map(|i| SimpleRow::new(i, format!("row{i}")))
        .collect();
    let result = client
        .insert_batch_with_isolation::<SimpleRow>("t", rows.clone())
        .await
        .unwrap();

    assert_eq!(result.succeeded_rows, 0, "no rows should land");
    assert_eq!(result.failed.len(), 4, "every row should be isolated as failed");
    assert_eq!(
        result.round_trips, 7,
        "expected 2N-1 = 7 round trips for N=4 all-fail"
    );

    // Each failed entry has the per-row error from the single-row
    // INSERT that confirmed it bad. The original row is preserved.
    let mut failed_ids: Vec<u64> = result.failed.iter().map(|(r, _)| r.id).collect();
    failed_ids.sort();
    assert_eq!(failed_ids, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn one_bad_row_isolated_three_others_landed() {
    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_validation(false);

    // Bisection trace for `[1,2,3,4]` where the first request to
    // [1,2,3,4] fails, and so does any sub-batch containing row 2:
    //
    //   1. [1,2,3,4] -> fail (FAILURE)
    //   2. [1,2]     -> fail (FAILURE)
    //   3. [1]       -> succeed (RECORD)
    //   4. [2]       -> fail   (FAILURE)
    //   5. [3,4]     -> succeed (RECORD)
    //
    // 5 round trips total. We register handlers in DFS-pop order
    // (left-then-right) since `try_insert_one_batch` pops the
    // left half first.
    mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));
    mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));
    let rec_left_one = mock.add(test::handlers::record::<SimpleRow>());
    mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));
    let rec_right = mock.add(test::handlers::record::<SimpleRow>());

    let rows: Vec<SimpleRow> = (1..=4)
        .map(|i| SimpleRow::new(i, format!("row{i}")))
        .collect();
    let result = client
        .insert_batch_with_isolation::<SimpleRow>("t", rows)
        .await
        .unwrap();

    assert_eq!(result.succeeded_rows, 3, "rows 1, 3, 4 should land");
    assert_eq!(result.failed.len(), 1, "row 2 should be the only failure");
    assert_eq!(result.failed[0].0.id, 2, "the isolated row is row 2");
    assert_eq!(result.round_trips, 5, "expected 5 round trips for one bad row in 4");

    let landed_left: Vec<SimpleRow> = rec_left_one.collect().await;
    let landed_right: Vec<SimpleRow> = rec_right.collect().await;
    assert_eq!(landed_left, vec![SimpleRow::new(1, "row1")]);
    assert_eq!(
        landed_right,
        vec![SimpleRow::new(3, "row3"), SimpleRow::new(4, "row4")]
    );
}

/// SG-13 (a): N=1 immediate-fail. A single-row batch that fails
/// shouldn't recurse; the row is isolated on the first try.
#[tokio::test]
async fn single_row_immediate_fail() {
    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_validation(false);
    mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));

    let result = client
        .insert_batch_with_isolation::<SimpleRow>("t", vec![SimpleRow::new(42, "x")])
        .await
        .unwrap();

    assert_eq!(result.succeeded_rows, 0);
    assert_eq!(result.failed.len(), 1);
    assert_eq!(result.failed[0].0.id, 42);
    assert_eq!(
        result.round_trips, 1,
        "single-row failing batch is exactly one HTTP request"
    );
}

/// SG-13 (b): bisection-then-success. The whole batch fails the first
/// HTTP request (transient 5xx); both halves succeed independently
/// when re-tried. Proves the algorithm doesn't keep descending past
/// the failure boundary -- on success it accepts the sub-batch and
/// moves on.
#[tokio::test]
async fn bisection_halts_when_subdivision_succeeds() {
    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_validation(false);
    // Whole batch fails once, then each half succeeds.
    mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));
    let rec_left = mock.add(test::handlers::record::<SimpleRow>());
    let rec_right = mock.add(test::handlers::record::<SimpleRow>());

    let rows: Vec<SimpleRow> = (1..=4)
        .map(|i| SimpleRow::new(i, format!("row{i}")))
        .collect();
    let result = client
        .insert_batch_with_isolation::<SimpleRow>("t", rows)
        .await
        .unwrap();

    assert_eq!(result.succeeded_rows, 4, "all four rows should land on retry");
    assert!(result.failed.is_empty(), "no rows should be unrecoverable");
    assert_eq!(
        result.round_trips, 3,
        "1 failed whole-batch + 2 successful halves = 3; \
         algorithm does not descend further on success"
    );

    let landed_left: Vec<SimpleRow> = rec_left.collect().await;
    let landed_right: Vec<SimpleRow> = rec_right.collect().await;
    assert_eq!(
        landed_left,
        vec![SimpleRow::new(1, "row1"), SimpleRow::new(2, "row2")]
    );
    assert_eq!(
        landed_right,
        vec![SimpleRow::new(3, "row3"), SimpleRow::new(4, "row4")]
    );
}

/// SG-13 (c): asymmetric (odd) N. `split_off(N/2)` on odd N gives
/// left = floor(N/2), right = ceil(N/2). For N=5: left = 2, right = 3.
#[tokio::test]
async fn odd_batch_size_bisection_trace() {
    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_validation(false);
    // [1..5] with row 3 (index 2) bad:
    //   [1..5] fail
    //     [1,2] OK
    //     [3,4,5] fail
    //       [3] fail (isolated bad)
    //       [4,5] OK
    // 5 round trips.
    mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));
    let rec_left = mock.add(test::handlers::record::<SimpleRow>());
    mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));
    mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));
    let rec_45 = mock.add(test::handlers::record::<SimpleRow>());

    let rows: Vec<SimpleRow> = (1..=5)
        .map(|i| SimpleRow::new(i, format!("row{i}")))
        .collect();
    let result = client
        .insert_batch_with_isolation::<SimpleRow>("t", rows)
        .await
        .unwrap();

    assert_eq!(result.succeeded_rows, 4);
    assert_eq!(result.failed.len(), 1);
    assert_eq!(result.failed[0].0.id, 3);
    assert_eq!(result.round_trips, 5);

    let landed_left: Vec<SimpleRow> = rec_left.collect().await;
    let landed_45: Vec<SimpleRow> = rec_45.collect().await;
    assert_eq!(
        landed_left,
        vec![SimpleRow::new(1, "row1"), SimpleRow::new(2, "row2")]
    );
    assert_eq!(
        landed_45,
        vec![SimpleRow::new(4, "row4"), SimpleRow::new(5, "row5")]
    );
}

/// SG-13 (d): two failures at non-adjacent positions. Verifies the
/// failed set contains BOTH bad rows in DFS-left isolation order
/// and the round-trip count matches the trace.
#[tokio::test]
async fn two_failures_at_non_adjacent_positions() {
    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_validation(false);
    // [1..8] bad at indices 2 and 5:
    //   [1..8] fail
    //     [1..4] fail
    //       [1,2] OK
    //       [3,4] fail -> [3] fail (bad), [4] OK
    //     [5..8] fail
    //       [5,6] fail -> [5] OK, [6] fail (bad)
    //       [7,8] OK
    // 11 round trips. Handler order (FIFO):
    //   [1..8] F, [1..4] F, [1,2] OK, [3,4] F, [3] F, [4] OK,
    //   [5..8] F, [5,6] F, [5] OK, [6] F, [7,8] OK.
    mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));
    mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));
    let _r12 = mock.add(test::handlers::record::<SimpleRow>());
    mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));
    mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));
    let _r4 = mock.add(test::handlers::record::<SimpleRow>());
    mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));
    mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));
    let _r5 = mock.add(test::handlers::record::<SimpleRow>());
    mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));
    let _r78 = mock.add(test::handlers::record::<SimpleRow>());

    let rows: Vec<SimpleRow> = (1..=8)
        .map(|i| SimpleRow::new(i, format!("row{i}")))
        .collect();
    let result = client
        .insert_batch_with_isolation::<SimpleRow>("t", rows)
        .await
        .unwrap();

    assert_eq!(result.succeeded_rows, 6);
    assert_eq!(result.failed.len(), 2);
    let failed_ids: Vec<u64> = result.failed.iter().map(|(r, _)| r.id).collect();
    assert_eq!(
        failed_ids,
        vec![3, 6],
        "DFS-left isolation order: row 3 first, then row 6"
    );
    assert_eq!(result.round_trips, 11);
}

#[tokio::test]
async fn empty_batch_is_zero_round_trips() {
    let mut mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_validation(false);
    // No mock handlers added -- if the implementation tried to send,
    // the test would fail with "non-exhaustive mock" at drop. The
    // empty-batch path should not emit any HTTP request.
    mock.non_exhaustive();

    let result = client
        .insert_batch_with_isolation::<SimpleRow>("t", vec![])
        .await
        .unwrap();

    assert_eq!(result.succeeded_rows, 0);
    assert!(result.failed.is_empty());
    assert_eq!(result.round_trips, 0, "empty batch sends no HTTP");
    assert!(result.is_clean());
}

// ---------------------------------------------------------------------------
// Client-driven dedup tokens (at-least-once retry safety)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn token_variant_injects_insert_deduplication_token_per_sub_batch() {
    use clickhouse::test::handlers::record_with_uri;

    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_validation(false);

    // Clean 1-round-trip path: one INSERT, one token derived from
    // (token_base, start=0, len=3).
    let recorder = mock.add(record_with_uri::<SimpleRow>());

    let rows = vec![
        SimpleRow::new(1, "a"),
        SimpleRow::new(2, "b"),
        SimpleRow::new(3, "c"),
    ];
    let result = client
        .insert_batch_with_isolation_with_token::<SimpleRow>(
            "t",
            rows,
            "logical_batch_42",
        )
        .await
        .unwrap();

    assert_eq!(result.succeeded_rows, 3);
    assert_eq!(result.round_trips, 1);
    assert!(result.is_clean());

    let (uri, _rows): (String, Vec<SimpleRow>) = recorder.collect().await;
    // Token format: `{base}/{start}-{end}` -- inclusive end.
    assert!(
        uri.contains("insert_deduplication_token=logical_batch_42")
            && uri.contains("0-2"),
        "expected token logical_batch_42/0-2 in URI; got: {uri}"
    );
}

#[tokio::test]
async fn token_variant_propagates_unique_tokens_through_bisection() {
    // [1..=4] with row 2 (index 1) bad. Bisection visits:
    //   [1..=4]   start=0 len=4 -> fail (token=base/0-3)
    //     [1,2]   start=0 len=2 -> fail (token=base/0-1)
    //       [1]   start=0 len=1 -> OK   (token=base/0-0)
    //       [2]   start=1 len=1 -> fail (token=base/1-1)
    //     [3,4]   start=2 len=2 -> OK   (token=base/2-3)
    // 5 round trips; row 2 isolated as the only failure.
    use clickhouse::test::handlers::record_with_uri;
    use hyper::StatusCode;

    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_validation(false);

    // Handler order (DFS-pop): whole, [1,2], [1], [2], [3,4].
    let _rec_whole = mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));
    let _rec_left = mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));
    let rec_lone1 = mock.add(record_with_uri::<SimpleRow>());
    let _rec_lone2 = mock.add(test::handlers::failure(StatusCode::INTERNAL_SERVER_ERROR));
    let rec_right = mock.add(record_with_uri::<SimpleRow>());

    let rows: Vec<SimpleRow> = (1..=4)
        .map(|i| SimpleRow::new(i, format!("row{i}")))
        .collect();
    let result = client
        .insert_batch_with_isolation_with_token::<SimpleRow>(
            "t",
            rows,
            "logical_batch_77",
        )
        .await
        .unwrap();

    assert_eq!(result.succeeded_rows, 3);
    assert_eq!(result.failed.len(), 1);
    assert_eq!(result.failed[0].0.id, 2);
    assert_eq!(result.round_trips, 5);

    // The two RECORDED inserts carry deterministic tokens:
    //   - [1] alone:  base/0-0
    //   - [3,4]:      base/2-3
    let (uri1, _): (String, Vec<SimpleRow>) = rec_lone1.collect().await;
    let (uri2, _): (String, Vec<SimpleRow>) = rec_right.collect().await;
    assert!(
        uri1.contains("insert_deduplication_token=logical_batch_77")
            && uri1.contains("0-0"),
        "[1] sub-batch should carry token logical_batch_77/0-0; got: {uri1}"
    );
    assert!(
        uri2.contains("insert_deduplication_token=logical_batch_77")
            && uri2.contains("2-3"),
        "[3,4] sub-batch should carry token logical_batch_77/2-3; got: {uri2}"
    );
}

#[tokio::test]
async fn default_variant_does_not_inject_dedup_token() {
    // Existing insert_batch_with_isolation (no _with_token suffix)
    // remains token-free for backward compatibility. Callers that
    // want client-driven dedup must use the explicit method.
    use clickhouse::test::handlers::record_with_uri;

    let mock = test::Mock::new();
    let client = Client::default()
        .with_mock(&mock)
        .with_validation(false);
    let recorder = mock.add(record_with_uri::<SimpleRow>());

    let rows = vec![SimpleRow::new(1, "a"), SimpleRow::new(2, "b")];
    client
        .insert_batch_with_isolation::<SimpleRow>("t", rows)
        .await
        .unwrap();

    let (uri, _rows): (String, Vec<SimpleRow>) = recorder.collect().await;
    assert!(
        !uri.contains("insert_deduplication_token"),
        "token-free variant should not inject dedup token; got: {uri}"
    );
}
