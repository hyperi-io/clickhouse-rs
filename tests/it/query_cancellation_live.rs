//! Live-ClickHouse integration test for the query-cancel
//! API (`Query::with_query_id`, `Client::kill_query`,
//! `Client::with_kill_on_drop`).
//!
//! The mock-based suite in `query_id.rs` proves the client sends the
//! `query_id` URL parameter and the `KILL QUERY` statement. This file
//! proves the *server* actually terminates the running query in
//! response and removes it from `system.processes`.
//!
//! Skipped (with a clear message) when `CLICKHOUSE_URL` is unset.

use std::env;
use std::time::Duration;

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

/// Count entries in `system.processes` matching `query_id`. The
/// server may take a moment to remove a killed query from the table,
/// so callers should poll with a short timeout.
async fn count_running(client: &Client, query_id: &str) -> u64 {
    client
        .query("SELECT count() FROM system.processes WHERE query_id = ?")
        .bind(query_id)
        .fetch_one::<u64>()
        .await
        .unwrap_or(0)
}

async fn wait_for_zero_running(client: &Client, query_id: &str, max_ms: u64) -> u64 {
    let start = std::time::Instant::now();
    let mut last = u64::MAX;
    while start.elapsed().as_millis() < max_ms as u128 {
        last = count_running(client, query_id).await;
        if last == 0 {
            return 0;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    last
}

#[tokio::test]
async fn kill_query_terminates_running_select() {
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    let qid = format!("kill-test-{}", Uuid::new_v4().simple());
    let qid_clone = qid.clone();
    let bg_client = client.clone();
    let started = std::sync::Arc::new(tokio::sync::Notify::new());
    let started_clone = started.clone();

    // Background task: run a query that sleeps for ~5 seconds.
    let handle = tokio::spawn(async move {
        started_clone.notify_one();
        bg_client
            .query("SELECT sleep(3), sleep(3)")
            .with_query_id(&qid_clone)
            .execute()
            .await
    });

    // Wait for the task to spawn, then give the server ~300ms to
    // begin the query and register in system.processes.
    started.notified().await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The query should be visible in system.processes by now.
    let pre_kill = count_running(&client, &qid).await;
    assert!(
        pre_kill >= 1,
        "query should be running before KILL; system.processes count = {pre_kill}"
    );

    // Kill it.
    client.kill_query(&qid).await.expect("kill_query");

    // Background task should error out shortly.
    let bg_result = tokio::time::timeout(Duration::from_secs(3), handle)
        .await
        .expect("background task did not return after KILL")
        .expect("join");
    assert!(
        bg_result.is_err(),
        "background query should fail after KILL; got: {bg_result:?}"
    );

    // system.processes should drop the entry shortly.
    let remaining = wait_for_zero_running(&client, &qid, 2000).await;
    assert_eq!(remaining, 0, "query still in system.processes after KILL");
}

#[tokio::test]
async fn kill_query_no_op_for_nonexistent_id() {
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    // KILL against a query that doesn't exist must NOT error -- the
    // KILL statement is a no-op when no matching query_id is found.
    let result = client
        .kill_query(&format!("nonexistent-{}", Uuid::new_v4().simple()))
        .await;
    assert!(
        result.is_ok(),
        "kill_query for nonexistent id should return Ok; got: {result:?}"
    );
}

#[tokio::test]
async fn kill_on_drop_terminates_abandoned_cursor() {
    let Some(client) = live_client() else {
        eprintln!("skipping: CLICKHOUSE_URL unset (live-CH test)");
        return;
    };

    let kill_client = client.clone().with_kill_on_drop();
    let qid = format!("dropkill-test-{}", Uuid::new_v4().simple());

    // Open a streaming cursor that returns rows over ~10 seconds
    // (20 rows * 0.5s sleep each). Read ONE row so the HTTP request
    // is in flight, then drop the cursor.
    let mut cursor = kill_client
        .query("SELECT number, sleep(0.5) FROM numbers(20)")
        .with_query_id(&qid)
        .fetch::<(u64, u8)>()
        .expect("fetch init");

    let _first = cursor.next().await.expect("first row");

    // Confirm the query is running before we drop.
    let pre_drop = count_running(&client, &qid).await;
    assert!(
        pre_drop >= 1,
        "query should be running before cursor drop"
    );

    drop(cursor);

    // Kill-on-drop spawns a background KILL; allow up to 3s for the
    // server to process it and drop the entry from system.processes.
    let remaining = wait_for_zero_running(&client, &qid, 3000).await;
    assert_eq!(
        remaining, 0,
        "query should be killed by kill_on_drop within 3s"
    );
}
