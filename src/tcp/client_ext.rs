//! TCP-side dispatch helpers: acquire a connection from the pool,
//! run a query / streaming SELECT / INSERT session, and poison the
//! handle on failure so the pool's recycle path drops the broken
//! connection.
//!
//! These are the glue between the public [`crate::Client`] surface and
//! the connection-actor primitives. They are deliberately small and
//! stateless -- all heavy lifting (cancel-on-drop, full-duplex
//! Exception detection, Cancel + drain on receiver-drop) already lives
//! inside [`crate::tcp::connection_actor`]; this module just routes the
//! pooled handles into it.
//!
//! # Why a separate module instead of inlining into `lib.rs`
//!
//! Keeps the TCP-specific dispatch logic out of the shared `Client`
//! file. `lib.rs` is already feature-gated on `tcp` for the few
//! routing points (`Client::query`, `Client::insert_native`); the
//! helpers themselves stay here so a reader of `lib.rs` does not need
//! to know about `NativePool`, `ConnectionHandle`, or the actor's
//! `execute_*` shape.
//!
//! # Block size for `insert_native_via_pool`
//!
//! INSERTs target roughly 1M rows per Native block, matching the
//! server's `max_insert_block_size` default. The choice mirrors the
//! HTTP `insert_native` path's row-count threshold and keeps the
//! server's merge pipeline running on its native batching cadence;
//! tiny blocks trigger more frequent part creation and merge churn,
//! oversized blocks risk hitting per-statement memory limits.

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::tcp::cursor::TcpRawCursor;
use crate::tcp::pool::NativePool;
use crate::tcp::retry::{RetryPolicy, run_with_retry};

/// Default rows per Native block on the TCP insert path. Matches
/// ClickHouse server's `max_insert_block_size` default; keeps the
/// merge pipeline's batching cadence aligned and avoids the tiny-
/// block merge churn that smaller batches would trigger.
pub(crate) const DEFAULT_TCP_INSERT_BLOCK_ROWS: u64 = 1_000_000;

/// Acquire a connection and run a query that does not stream rows
/// (DDL, `SET`, `INSERT ... VALUES`, etc.). Poisons the handle on
/// error so the pool's recycle path drops it on return.
///
/// `query_id` is forwarded into `ClientInfo.initial_query_id` so the
/// server's `system.query_log.initial_query_id` matches what the
/// caller uses for tracing and for `KILL QUERY WHERE query_id = ?`.
///
/// # Retry safety
///
/// `ExecuteQuery` covers arbitrary statements, many of which are NOT
/// safe to replay (a non-idempotent `INSERT ... VALUES`, a mutation).
/// Auto-retry is therefore OPT-IN: it is enabled only when the caller
/// asserts idempotency (`Query::idempotent()`, which `Client::ping`
/// sets for its `SELECT 1`). When `idempotent` is `false`, this runs a
/// single acquire pass regardless of `retry` -- endpoint failover still
/// happens inside the pool manager's `create`, but no statement is
/// replayed. When `true`, transient transport/connect failures are
/// retried per `retry`.
pub(crate) async fn execute_query_via_pool(
    pool: &Arc<NativePool>,
    query_id: &str,
    query: &str,
    settings: &[(String, String)],
    retry: Option<RetryPolicy>,
    idempotent: bool,
) -> Result<()> {
    // Only the idempotent path consults `retry`; a non-idempotent
    // statement gets exactly one pass (no replay) by passing `None`.
    let effective = if idempotent { retry } else { None };
    run_with_retry(pool, effective, |conn| async move {
        let result = conn
            .execute_query(query_id.to_string(), query.to_string(), settings.to_vec())
            .await;
        if result.is_err() {
            // Poison so the pool's recycle path drops this connection;
            // the retry loop's next pass acquires a fresh one.
            conn.poison();
        }
        result
    })
    .await
}

/// Acquire a connection and open a streaming SELECT.
///
/// Returns a [`TcpRawCursor`] that yields decoded blocks until the
/// server emits `EndOfStream`. The cursor holds the pool's
/// `Object<TcpConnectionManager>` (via the embedded receiver path):
/// dropping the cursor drops the receiver, which trips the actor's
/// `results.closed()` watch and triggers protocol Cancel + drain --
/// the connection then recycles back to the pool.
///
/// The acquired connection IS returned to the pool implicitly through
/// the actor's command-channel lifetime. The pool object is dropped
/// inside this function (after dispatching the streaming command);
/// the connection-actor's internal task keeps the socket alive until
/// the cursor is fully drained or dropped, because `ConnectionHandle`
/// is cheap-clone and the actor lives until every handle is gone.
///
/// # Retry safety
///
/// The retried region is the query *issue* only -- `pool.get()` plus
/// opening the cursor. No row is consumed inside it, so a transient
/// connect/issue failure (including a silently-dead pooled connection)
/// is replayed safely per `retry`. Once the cursor is returned, its
/// later `next_block()` failures happen AFTER the retried region and
/// surface to the caller WITHOUT auto-retry. A SELECT is always
/// retry-eligible (subject to the policy); the caller passes the
/// client's configured `retry`.
pub(crate) async fn execute_stream_via_pool(
    pool: &Arc<NativePool>,
    query_id: &str,
    query: &str,
    settings: &[(String, String)],
    retry: Option<RetryPolicy>,
) -> Result<TcpRawCursor> {
    // `op` is `Fn` (re-run per attempt), so clone the owned inputs
    // inside the closure rather than moving them out once.
    run_with_retry(pool, retry, |conn| {
        let query_id = query_id.to_string();
        let query = query.to_string();
        let settings = settings.to_vec();
        async move {
            let cursor = conn
                .execute_stream_cursor(query_id, query, settings)
                .await;
            match cursor {
                Ok(c) => Ok(c),
                Err(e) => {
                    conn.poison();
                    Err(e)
                }
            }
        }
    })
    .await
}

/// Pool-acquired INSERT session.
///
/// Holds the pooled [`deadpool::managed::Object`] for the duration of
/// the session so the connection-actor keeps the socket exclusive
/// for our `BeginInsert` -> N x `SendInsertBlock` -> `FinishInsert`
/// sequence. Dropping `TcpInsertSession` without calling `finish()`
/// drops the pool object which returns the connection to the pool
/// in `InsertActive` state -- the next acquirer will see an out-of-
/// state command and the actor will respond with an error. To avoid
/// that footgun, callers MUST call [`Self::finish`] OR [`Self::abort`]
/// (which poisons and drops); the InsertNative wrapper does this.
pub(crate) struct TcpInsertSession {
    handle: deadpool::managed::Object<crate::tcp::pool::TcpConnectionManager>,
    /// Column metadata the server echoed in its schema block.
    /// Forwarded so callers (e.g. the InsertNative<T> wrapper) can
    /// reconcile against their declared schema if needed.
    pub(crate) server_columns: Vec<(String, String)>,
    /// Negotiated server protocol revision from the handshake. The Native
    /// encoder uses THIS (not a hardcoded constant) so the per-column
    /// custom_serialization flag presence matches what the server expects
    /// for its revision -- a server below the custom-serialization revision
    /// must NOT receive the flag.
    pub(crate) server_revision: u64,
}

impl TcpInsertSession {
    /// Send one Native block. `column_bytes` is the pre-encoded
    /// payload from [`crate::native::encode_columns`]; the actor is
    /// purely a transport.
    pub(crate) async fn send_block(
        &self,
        column_bytes: Vec<u8>,
        num_columns: u64,
        num_rows: u64,
    ) -> Result<()> {
        let result = self
            .handle
            .send_insert_block(column_bytes, num_columns, num_rows)
            .await;
        if result.is_err() {
            self.handle.poison();
        }
        result
    }

    /// Terminate the INSERT session and return the connection to the
    /// pool. Drains response packets to EndOfStream; surfaces a
    /// server Exception if the server rejected the INSERT.
    pub(crate) async fn finish(self) -> Result<()> {
        let result = self.handle.finish_insert().await;
        if result.is_err() {
            self.handle.poison();
        }
        result
    }

    /// Poison the connection and drop the session. The pool's recycle
    /// path will refuse the handle and `Manager::create` opens a new
    /// one for the next caller.
    pub(crate) fn abort(self) {
        self.handle.poison();
    }
}

/// Acquire a connection and open an INSERT session against `table`
/// using the supplied SQL.
///
/// `sql` is the full INSERT statement, typically
/// `"INSERT INTO <table> (...) FORMAT Native"`. The actor sends the
/// Query packet, drains protocol chatter until the server's schema
/// block, and replies with the `(name, type_name)` column pairs the
/// schema block carried.
pub(crate) async fn insert_native_via_pool(
    pool: &Arc<NativePool>,
    query_id: &str,
    sql: &str,
    settings: &[(String, String)],
) -> Result<TcpInsertSession> {
    let conn = pool
        .get()
        .await
        .map_err(|e| Error::Custom(format!("tcp pool: {e}")))?;
    let server_columns = match conn
        .begin_insert(query_id.to_string(), sql.to_string(), settings.to_vec())
        .await
    {
        Ok(cols) => cols,
        Err(e) => {
            conn.poison();
            return Err(e);
        }
    };
    // Capture the negotiated revision before `conn` is moved into the
    // session, so the encoder can match the server's framing exactly.
    let server_revision = conn.server_hello().revision;
    Ok(TcpInsertSession {
        handle: conn,
        server_columns,
        server_revision,
    })
}
