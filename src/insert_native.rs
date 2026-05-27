//! HTTP `Format::Native` insert.
//!
//! Buffers RowBinary-serialised rows in memory, transposes them into
//! Native columnar blocks, and ships them through the existing
//! [`crate::insert_formatted::InsertFormatted`] HTTP
//! path with `format=Native`.
//!
//! # Block chunking
//!
//! ClickHouse's `format=Native` body is a sequence of independent
//! columnar blocks. `InsertNative` flushes a block when either the
//! row count or the buffered byte size crosses a configurable
//! threshold (defaults: 100k rows / 10 MiB), then keeps buffering
//! into the next block. [`end`][InsertNative::end] flushes any
//! remaining rows as a final block. The Native format primitive in
//! [`crate::native`] supports any number of blocks back-to-back, so
//! a million-row INSERT splits cleanly across many blocks without
//! holding everything in memory.
//!
//! # Example
//!
//! ```no_run
//! # async fn run() -> clickhouse::error::Result<()> {
//! use clickhouse::{Client, Row};
//! use serde::Serialize;
//!
//! #[derive(Row, Serialize)]
//! struct Event { id: u64, name: String }
//!
//! let client = Client::default().with_url("http://localhost:8123");
//! let mut insert = client.insert_native::<Event>("events").await?;
//! insert.write(&Event { id: 1, name: "a".into() }).await?;
//! insert.write(&Event { id: 2, name: "b".into() }).await?;
//! insert.end().await?;
//! # Ok(()) }
//! ```

use std::marker::PhantomData;

use crate::insert_formatted::InsertFormatted;
use crate::native::encode::{ColumnSchema, encode_columns};
use crate::native::io::ClickHouseBytesWrite;
use crate::row::{Row, RowWrite};
use crate::row_metadata::RowMetadata;
use crate::rowbinary::serialize_row_binary;
use crate::{Client, error::Result, formats};

/// Backing sink for the encoded Native blocks. `Http` wraps the
/// existing `InsertFormatted` request-body path; `Tcp` holds a
/// pool-acquired session that ships blocks via the TCP connection
/// actor.
///
/// The wrapper exists so `InsertNative<T>` keeps one public surface
/// across transports -- the encoder + block-chunking logic is shared.
enum NativeSink {
    Http(InsertFormatted),
    #[cfg(feature = "tcp")]
    Tcp(crate::tcp::client_ext::TcpInsertSession),
}

/// Server protocol revision the encoder reports as. The encoder uses
/// this only to decide whether to emit a per-column
/// `custom_serialization` flag byte (required from rev 54454 onwards
/// -- ClickHouse 24.x and newer). Default to a "modern" revision since
/// virtually all in-the-wild servers exceed it.
const DEFAULT_REVISION: u64 = 54454;

/// Default per-block row threshold. Blocks are flushed when buffered
/// rows reach this number.
const DEFAULT_MAX_ROWS_PER_BLOCK: u64 = 100_000;

/// Default per-block byte threshold (RowBinary-buffered size). Blocks
/// are flushed when buffered serialised bytes reach this number.
const DEFAULT_MAX_BYTES_PER_BLOCK: u64 = 10 * 1024 * 1024;

/// `Format::Native` insert backed by either the HTTP transport or
/// the TCP transport (depending on how the `Client` was constructed).
/// See module docs.
#[must_use]
pub struct InsertNative<T> {
    inner: NativeSink,
    column_schema: Vec<ColumnSchema>,
    rows: Vec<Vec<u8>>,
    /// Sum of `rows.iter().map(Vec::len).sum()` -- maintained
    /// incrementally so the threshold check is O(1).
    buffered_bytes: u64,
    revision: u64,
    max_rows_per_block: u64,
    max_bytes_per_block: u64,
    /// `true` once the first block has been shipped; affects empty-
    /// INSERT semantics in [`end`][InsertNative::end].
    sent_any_block: bool,
    _marker: PhantomData<fn() -> T>,
}

/// Build the `INSERT INTO ...(cols...) FORMAT Native` SQL for a
/// pre-escaped table identifier and a column list. Both
/// [`InsertNative::with_columns`] and [`InsertNative::new`] route
/// through this helper so the two paths share one SQL template.
fn compose_native_insert_sql(table_escaped: &str, headers: &[(String, String)]) -> String {
    let column_names = headers
        .iter()
        .map(|(n, _)| format!("`{}`", n.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "INSERT INTO {table_escaped}({column_names}) FORMAT {fmt}",
        fmt = formats::NATIVE,
    )
}

/// Strict-exact validation: caller's declared columns must match server's
/// echoed columns 1:1 by count, name (case-sensitive), and type (byte-for-byte
/// String equality). On mismatch returns a position-indexed error message that
/// names both sides for cheap diffing.
///
/// Server-echoed types come from `IDataType::getName()` (canonical form), so
/// callers are expected to supply canonical types (e.g. `Int64`, not `BIGINT`).
/// An alias mismatch surfaces here and teaches the caller the canonical name.
#[cfg(feature = "tcp")]
fn validate_caller_columns_vs_server(
    caller: &[(String, String)],
    server: &[(String, String)],
) -> Result<()> {
    if server.len() != caller.len() {
        return Err(crate::error::Error::Other(
            format!(
                "InsertNative::with_columns_tcp: caller declared {} columns but server-echoed schema has {}",
                caller.len(),
                server.len(),
            )
            .into(),
        ));
    }
    for (i, (caller_col, server_col)) in caller.iter().zip(server.iter()).enumerate() {
        if caller_col.0 != server_col.0 || caller_col.1 != server_col.1 {
            return Err(crate::error::Error::Other(
                format!(
                    "InsertNative::with_columns_tcp: column {i}: caller declared ({:?}, {:?}), server echoed ({:?}, {:?})",
                    caller_col.0, caller_col.1, server_col.0, server_col.1,
                )
                .into(),
            ));
        }
    }
    Ok(())
}

/// Typed-row validation: count + name + order against `T::COLUMN_NAMES`. For
/// `RowKind::Struct`, `COLUMN_NAMES` is populated and we enforce name + order;
/// for `RowKind::Tuple`/`Vec`/`Primitive`, `COLUMN_NAMES == &[]` so the zip is
/// empty and only the count check fires (correct: those kinds have no field
/// names to validate against).
///
/// No type check: `Row` exposes only names + count, not types. Type-strictness
/// lives on the dynamic path where the caller supplies types.
#[cfg(feature = "tcp")]
fn validate_typed_columns_vs_server<T: crate::Row>(
    server: &[(String, String)],
) -> Result<()> {
    if server.len() != T::COLUMN_COUNT {
        return Err(crate::error::Error::Other(
            format!(
                "TCP insert_native: Row<T> has {} columns but server schema reports {}",
                T::COLUMN_COUNT,
                server.len(),
            )
            .into(),
        ));
    }
    for (i, ((srv_name, _srv_type), expected_name)) in
        server.iter().zip(T::COLUMN_NAMES.iter()).enumerate()
    {
        if srv_name != *expected_name {
            return Err(crate::error::Error::Other(
                format!(
                    "TCP insert_native: column at position {i}: Row<T> field {:?} but server reports {:?}",
                    expected_name, srv_name,
                )
                .into(),
            ));
        }
    }
    Ok(())
}

impl<T: Row> InsertNative<T> {
    /// Construct an `InsertNative` directly from pre-resolved column
    /// names + types. Lets callers skip the [`Client::insert_native`]
    /// `DESCRIBE TABLE` round-trip when they already know the schema
    /// (e.g. cached locally, or in tests against a mock).
    ///
    /// `columns` is `(column_name, type_name)` pairs in the order the
    /// columns appear in the table; `type_name` is the canonical
    /// ClickHouse type string (e.g. `"UInt64"`, `"String"`,
    /// `"Array(UInt32)"`).
    ///
    /// # Errors
    ///
    /// `Err` if `table` cannot be escaped as a SQL identifier (rare;
    /// happens only for inputs the upstream `sql::escape::identifier`
    /// rejects) or if any `type_name` is not a recognised ClickHouse
    /// type.
    pub fn with_columns(
        client: &Client,
        table: &str,
        columns: &[(String, String)],
    ) -> Result<Self> {
        // Empty `columns` would compose to `INSERT INTO t() FORMAT Native`,
        // which the server rejects as a syntax error. Reject up-front so the
        // caller learns at construction time instead of after the round-trip.
        if columns.is_empty() {
            return Err(crate::error::Error::Other(
                "InsertNative::with_columns: columns is empty; INSERT needs at least one column"
                    .into(),
            ));
        }
        // Escape the table name BEFORE SQL composition. Callers MUST
        // not be trusted to pass an already-escaped identifier; we
        // mirror what Client::insert_native does so the two ctor
        // entry points have identical safety guarantees.
        let mut escaped_table = String::new();
        crate::sql::escape::identifier(table, &mut escaped_table)
            .map_err(|e| crate::error::Error::Other(
                format!("error escaping table name: {e:?}").into(),
            ))?;
        let column_schema = ColumnSchema::from_headers(columns)?;
        let sql = compose_native_insert_sql(&escaped_table, columns);
        Ok(Self {
            inner: NativeSink::Http(InsertFormatted::new(client, sql, Some(&escaped_table))),
            column_schema,
            rows: Vec::new(),
            buffered_bytes: 0,
            revision: DEFAULT_REVISION,
            max_rows_per_block: DEFAULT_MAX_ROWS_PER_BLOCK,
            max_bytes_per_block: DEFAULT_MAX_BYTES_PER_BLOCK,
            sent_any_block: false,
            _marker: PhantomData,
        })
    }

    pub(crate) fn new(client: &Client, table: &str, metadata: RowMetadata) -> Result<Self> {
        // Caller (Client::insert_native) supplies an already-escaped
        // table identifier. `with_columns` does its own escape.
        // Build (name, type_name) headers from the resolved metadata.
        let headers: Vec<(String, String)> = metadata
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.data_type.to_string()))
            .collect();
        let column_schema = ColumnSchema::from_headers(&headers)?;
        let sql = compose_native_insert_sql(table, &headers);
        Ok(Self {
            inner: NativeSink::Http(InsertFormatted::new(client, sql, Some(table))),
            column_schema,
            rows: Vec::new(),
            buffered_bytes: 0,
            revision: DEFAULT_REVISION,
            max_rows_per_block: DEFAULT_MAX_ROWS_PER_BLOCK,
            max_bytes_per_block: DEFAULT_MAX_BYTES_PER_BLOCK,
            sent_any_block: false,
            _marker: PhantomData,
        })
    }

    /// TCP-transport constructor.
    ///
    /// Skips the HTTP-path `DESCRIBE TABLE` round-trip and instead
    /// derives the column schema from the server's response to
    /// `BeginInsert` -- ClickHouse 24.x/25.x always echoes
    /// `(name, type_name)` pairs in the schema block that follows
    /// `INSERT INTO <table> FORMAT Native`.
    ///
    /// The INSERT is issued with no explicit column list (so the
    /// server uses the table's full column set in declaration order);
    /// the returned schema becomes the encoder's column layout.
    /// Block-size defaults differ from the HTTP path: the TCP path
    /// targets ~1M rows per Native block to match the server's
    /// `max_insert_block_size` default.
    #[cfg(feature = "tcp")]
    pub(crate) async fn new_tcp_with_server_schema(
        client: &Client,
        table_escaped: &str,
    ) -> Result<Self>
    where
        T: Row,
    {
        let pool = client
            .tcp_pool()
            .expect("new_tcp_with_server_schema: client has no TCP pool");
        let sql = format!(
            "INSERT INTO {table_escaped} FORMAT {fmt}",
            fmt = formats::NATIVE,
        );
        // Forward Client database + settings + roles via the shared TCP-settings
        // helper. Skips HTTP-only knobs (compression toggles, default_format).
        let tcp_settings = client.tcp_insert_settings();
        let session = crate::tcp::client_ext::insert_native_via_pool(
            pool,
            "", // query_id default; future TCP path can plumb auto_query_id
            &sql,
            &tcp_settings,
        )
        .await?;
        if session.server_columns.is_empty() {
            session.abort();
            return Err(crate::error::Error::Other(
                "TCP insert_native: server schema block carried no columns; \
                 cannot determine encoder layout. This usually indicates a \
                 protocol revision below 24.x -- supply columns explicitly \
                 via InsertNative::with_columns over a Client that exposes \
                 the TCP pool directly."
                    .into(),
            ));
        }
        // Wrap with abort: a bare `?` would drop the session mid-handshake,
        // returning the pool connection in InsertActive and poisoning the
        // next user. Same shape as the validate-then-abort below.
        let column_schema = match ColumnSchema::from_headers(&session.server_columns) {
            Ok(s) => s,
            Err(e) => {
                session.abort();
                return Err(e);
            }
        };
        // Validate Row<T> against the server-echoed schema: count + name +
        // order. Type check unavailable from T alone (Row trait exposes
        // only COLUMN_NAMES and COLUMN_COUNT, not types). Type-strictness
        // lives on the dynamic path (with_columns_tcp) where the caller
        // supplies types.
        if let Err(e) = validate_typed_columns_vs_server::<T>(&session.server_columns) {
            session.abort();
            return Err(e);
        }
        // Encode at the NEGOTIATED revision (not a hardcoded constant) so
        // the per-column custom_serialization flag matches the server's
        // expectation for its revision. Captured before `session` moves.
        let revision = session.server_revision;
        Ok(Self {
            inner: NativeSink::Tcp(session),
            column_schema,
            rows: Vec::new(),
            buffered_bytes: 0,
            revision,
            max_rows_per_block: crate::tcp::client_ext::DEFAULT_TCP_INSERT_BLOCK_ROWS,
            max_bytes_per_block: DEFAULT_MAX_BYTES_PER_BLOCK,
            sent_any_block: false,
            _marker: PhantomData,
        })
    }

    /// Open a TCP-transport `Format::Native` INSERT for a caller-supplied
    /// runtime column list. Sibling of [`Self::with_columns`] (HTTP).
    ///
    /// Columns are validated strictly against the server-echoed schema
    /// (count + name + type + order, byte-for-byte case-sensitive); on
    /// any mismatch the session is aborted before any rows ship.
    ///
    /// The composed SQL includes an explicit `(col, ...)` list so the
    /// server matches columns by name. Type strings must be canonical
    /// ClickHouse form (`Nullable(UInt64)`, not `BIGINT`).
    ///
    /// # Errors
    ///
    /// `Err` if `columns` is empty, if `table` cannot be SQL-escaped, if
    /// the TCP pool cannot acquire a connection, if the server rejects the
    /// INSERT, or if the server-echoed schema disagrees with `columns`.
    #[cfg(feature = "tcp")]
    pub async fn with_columns_tcp(
        client: &Client,
        table: &str,
        columns: &[(String, String)],
    ) -> Result<Self> {
        // Reject empty `columns` before any network activity. Mirrors the
        // HTTP `with_columns` guard.
        if columns.is_empty() {
            return Err(crate::error::Error::Other(
                "InsertNative::with_columns_tcp: columns is empty; INSERT needs at least one column"
                    .into(),
            ));
        }
        // Escape `table` BEFORE SQL composition (same wrapper as
        // `with_columns`). Caller MUST NOT be trusted to pass an
        // already-escaped identifier.
        let mut escaped_table = String::new();
        crate::sql::escape::identifier(table, &mut escaped_table).map_err(|e| {
            crate::error::Error::Other(format!("error escaping table name: {e:?}").into())
        })?;
        let pool = client.tcp_pool().ok_or_else(|| {
            crate::error::Error::Other(
                "InsertNative::with_columns_tcp: Client has no TCP pool; build with Client::tcp(addr) or with_tcp_addrs"
                    .into(),
            )
        })?;
        // Explicit column-list SQL: server matches by name and echoes back
        // exactly our column set. Same composer as the HTTP `with_columns`.
        let sql = compose_native_insert_sql(&escaped_table, columns);
        // Forward Client database + settings + roles via the shared TCP-settings
        // helper. Skips HTTP-only knobs (compression toggles, default_format).
        let tcp_settings = client.tcp_insert_settings();
        // query_id stays empty; auto-query-id on the TCP insert path is a
        // known TODO unrelated to this work.
        let session = crate::tcp::client_ext::insert_native_via_pool(
            pool,
            "",
            &sql,
            &tcp_settings,
        )
        .await?;
        if let Err(e) = validate_caller_columns_vs_server(columns, &session.server_columns) {
            session.abort();
            return Err(e);
        }
        // Caller's columns and server-echoed columns are identical (we just
        // validated). Use the caller's slice for the encoder schema. Wrap
        // with abort: bare `?` would leak the session in InsertActive.
        let column_schema = match ColumnSchema::from_headers(columns) {
            Ok(s) => s,
            Err(e) => {
                session.abort();
                return Err(e);
            }
        };
        // Encode at the NEGOTIATED revision (not a hardcoded constant) so
        // the per-column custom_serialization flag matches the server's
        // expectation for its revision. Captured before `session` moves.
        let revision = session.server_revision;
        Ok(Self {
            inner: NativeSink::Tcp(session),
            column_schema,
            rows: Vec::new(),
            buffered_bytes: 0,
            revision,
            max_rows_per_block: crate::tcp::client_ext::DEFAULT_TCP_INSERT_BLOCK_ROWS,
            max_bytes_per_block: DEFAULT_MAX_BYTES_PER_BLOCK,
            sent_any_block: false,
            _marker: PhantomData,
        })
    }

    /// Override the protocol revision the encoder reports as. The
    /// only effect is whether the per-column `custom_serialization`
    /// flag byte is emitted (rev >= 54454 = yes). Default is recent
    /// enough for any modern server; lower it for legacy ClickHouse.
    pub fn with_revision(mut self, revision: u64) -> Self {
        self.revision = revision;
        self
    }

    /// Per-block row threshold. When the number of buffered rows
    /// reaches this value, the next [`write`][Self::write] flushes
    /// the current block and starts the next one.
    ///
    /// Default: 100_000 rows. Set high to disable row-based
    /// chunking (combine with [`with_max_bytes_per_block`][Self::with_max_bytes_per_block] on
    /// `u64::MAX` to disable chunking entirely).
    pub fn with_max_rows_per_block(mut self, n: u64) -> Self {
        self.max_rows_per_block = n.max(1);
        self
    }

    /// Per-block byte threshold. When the buffered RowBinary-
    /// serialised bytes reach this value, the next
    /// [`write`][Self::write] flushes the current block.
    ///
    /// Default: 10 MiB. Note that this counts pre-transpose bytes
    /// (RowBinary input); the on-the-wire Native block is similar
    /// in size for typical workloads but can differ for sparse or
    /// LowCardinality columns.
    pub fn with_max_bytes_per_block(mut self, n: u64) -> Self {
        self.max_bytes_per_block = n.max(1);
        self
    }

    /// Buffer one row. The row is serialised via the existing
    /// RowBinary path; columns are produced by transposing at flush
    /// time. If the row crosses either configured per-block
    /// threshold, the buffered block is shipped before this method
    /// returns.
    pub async fn write(&mut self, row: &T::Value<'_>) -> Result<()>
    where
        T: RowWrite,
    {
        let mut buf: Vec<u8> = Vec::with_capacity(64);
        serialize_row_binary(&mut buf, row)?;
        self.buffered_bytes = self
            .buffered_bytes
            .saturating_add(buf.len() as u64);
        self.rows.push(buf);

        if self.rows.len() as u64 >= self.max_rows_per_block
            || self.buffered_bytes >= self.max_bytes_per_block
        {
            self.flush_block().await?;
        }
        Ok(())
    }

    /// Force-flush any buffered rows as a Native block. Useful for
    /// callers that want to emit blocks on a wall-clock interval
    /// rather than on threshold. No-op if the buffer is empty.
    pub async fn flush(&mut self) -> Result<()> {
        if !self.rows.is_empty() {
            self.flush_block().await?;
        }
        Ok(())
    }

    /// Internal: transpose buffered rows into a Native block and
    /// ship it. Resets the row buffer. Always sends -- callers must
    /// guard against empty buffers themselves where appropriate.
    async fn flush_block(&mut self) -> Result<()> {
        let n_rows = self.rows.len() as u64;
        let n_cols = self.column_schema.len() as u64;

        match &mut self.inner {
            NativeSink::Http(http) => {
                // HTTP `FORMAT Native` reads at server_revision = 0:
                // NO BlockInfo, NO per-column custom_serialization flag.
                // Envelope is just varint(num_columns) + varint(num_rows)
                // + column_bytes (per-column header + data). Headers are
                // emitted even for a 0-row block so num_columns matches
                // the stream.
                let column_bytes = encode_columns(&self.rows, &self.column_schema, 0)?;
                let mut body: Vec<u8> = Vec::with_capacity(column_bytes.len() + 16);
                body.put_var_uint(n_cols);
                body.put_var_uint(n_rows);
                body.extend_from_slice(&column_bytes);
                http.send(body.into()).await?;
            }
            #[cfg(feature = "tcp")]
            NativeSink::Tcp(session) => {
                // TCP uses the negotiated revision (self.revision,
                // >= 54454 on modern CH): the connection-actor writes its
                // own BlockInfo + num_columns + num_rows envelope inside
                // `send_data_block`, and `encode_columns` writes the
                // per-column custom_serialization flag. Must NOT encode at
                // revision 0 here (that omits the flag -> stream desync).
                let column_bytes =
                    encode_columns(&self.rows, &self.column_schema, self.revision)?;
                session.send_block(column_bytes, n_cols, n_rows).await?;
            }
        }
        self.rows.clear();
        self.buffered_bytes = 0;
        self.sent_any_block = true;
        Ok(())
    }

    /// Flush any remaining buffered rows as a final block, then
    /// finalise the INSERT. If no rows were ever written, sends a
    /// single empty block (the `format=Native` protocol requires at
    /// least one block).
    pub async fn end(mut self) -> Result<()> {
        // Flush iff there are buffered rows, OR no block has been sent
        // yet (the Native protocol requires at least one block per
        // INSERT -- emit a zero-row block).
        if !self.rows.is_empty() || !self.sent_any_block {
            self.flush_block().await?;
        }
        match self.inner {
            NativeSink::Http(http) => http.end().await,
            #[cfg(feature = "tcp")]
            NativeSink::Tcp(session) => session.finish().await,
        }
    }

    /// Drop pending rows without sending. Useful when you want to
    /// reset state and start a new INSERT.
    pub fn abort(self) {
        match self.inner {
            NativeSink::Http(_) => {
                // Inner `InsertFormatted` is dropped here; no request
                // was initiated yet (we hold all bytes until `end()`),
                // so there is nothing to roll back on the server.
            }
            #[cfg(feature = "tcp")]
            NativeSink::Tcp(session) => session.abort(),
        }
    }
}

#[cfg(test)]
mod sql_composition_tests {
    use super::*;

    #[test]
    fn compose_native_insert_sql_quotes_columns_and_escapes_backticks() {
        let sql = compose_native_insert_sql(
            "`db`.`t`",
            &[
                ("id".to_string(), "UInt64".to_string()),
                ("weird`name".to_string(), "String".to_string()),
            ],
        );
        assert_eq!(
            sql,
            "INSERT INTO `db`.`t`(`id`,`weird``name`) FORMAT Native"
        );
    }

    // Regression for the SQL-injection class (caller-supplied table name interpolated raw):
    // `InsertNative::with_columns` previously interpolated the caller-
    // supplied `table` raw into the SQL template. The fix routes
    // both ctors through `sql::escape::identifier`. We can't easily
    // build a Client in this unit test, but we can confirm the
    // helper itself treats whatever it's handed as an opaque
    // pre-escaped identifier -- the responsibility for escape lives
    // at the ctor boundary, NOT in compose_native_insert_sql.
    #[test]
    fn compose_native_insert_sql_does_not_re_quote_table() {
        // If the caller passed in `events; DROP TABLE x; --` AS THE
        // PRE-ESCAPED identifier (which is impossible via
        // sql::escape::identifier -- that function would quote it),
        // the helper interpolates it verbatim. This pins the
        // responsibility: composition trusts its first arg.
        let sql = compose_native_insert_sql("`evil`", &[]);
        assert_eq!(sql, "INSERT INTO `evil`() FORMAT Native");
    }

    #[test]
    fn with_columns_empty_columns_errors() {
        // Regression: caller-supplied empty `columns` previously composed to
        // `INSERT INTO t() FORMAT Native` and was punted to the server as a
        // syntax error. Guard at the ctor boundary so the caller learns at
        // construction time instead of at first network round-trip.
        use crate::{Client, Row};
        use serde::Serialize;

        #[derive(Row, Serialize)]
        #[clickhouse(crate = "crate")]
        #[allow(dead_code)]
        struct Tiny {
            id: u64,
        }

        let client = Client::default();
        let err = match InsertNative::<Tiny>::with_columns(&client, "tiny", &[]) {
            Ok(_) => panic!("empty columns must be rejected at ctor time"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("empty"),
            "error message should name the empty-columns problem, got: {msg}",
        );
    }
}

#[cfg(all(test, feature = "tcp"))]
mod validation_tests {
    use super::{validate_caller_columns_vs_server, validate_typed_columns_vs_server};

    fn pair(n: &str, t: &str) -> (String, String) {
        (n.to_string(), t.to_string())
    }

    #[test]
    fn caller_exact_match_accepts() {
        let caller = vec![pair("id", "UInt64"), pair("name", "String")];
        let server = vec![pair("id", "UInt64"), pair("name", "String")];
        validate_caller_columns_vs_server(&caller, &server).unwrap();
    }

    #[test]
    fn caller_count_mismatch_rejects() {
        let caller = vec![pair("id", "UInt64")];
        let server = vec![pair("id", "UInt64"), pair("name", "String")];
        let err = match validate_caller_columns_vs_server(&caller, &server) {
            Ok(_) => panic!("count mismatch must reject"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("caller declared 1") && msg.contains("has 2"),
            "expected count-mismatch wording naming both sides, got: {msg}",
        );
    }

    #[test]
    fn caller_name_mismatch_at_position_rejects() {
        let caller = vec![pair("id", "UInt64"), pair("wrong", "String")];
        let server = vec![pair("id", "UInt64"), pair("name", "String")];
        let err = match validate_caller_columns_vs_server(&caller, &server) {
            Ok(_) => panic!("name mismatch must reject"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("column 1") && msg.contains("wrong") && msg.contains("name"),
            "expected position-indexed name mismatch naming both sides, got: {msg}",
        );
    }

    #[test]
    fn caller_type_mismatch_at_position_rejects() {
        let caller = vec![pair("id", "UInt64"), pair("name", "FixedString(8)")];
        let server = vec![pair("id", "UInt64"), pair("name", "String")];
        let err = match validate_caller_columns_vs_server(&caller, &server) {
            Ok(_) => panic!("type mismatch must reject"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("column 1") && msg.contains("FixedString(8)") && msg.contains("String"),
            "expected position-indexed type mismatch naming both sides, got: {msg}",
        );
    }

    #[test]
    fn typed_struct_match_accepts() {
        use crate::Row;
        use serde::Serialize;
        #[derive(Row, Serialize)]
        #[clickhouse(crate = "crate")]
        #[allow(dead_code)]
        struct Two { id: u64, name: String }
        let server = vec![pair("id", "UInt64"), pair("name", "String")];
        validate_typed_columns_vs_server::<Two>(&server).unwrap();
    }

    #[test]
    fn typed_struct_count_mismatch_rejects() {
        use crate::Row;
        use serde::Serialize;
        #[derive(Row, Serialize)]
        #[clickhouse(crate = "crate")]
        #[allow(dead_code)]
        struct One { id: u64 }
        let server = vec![pair("id", "UInt64"), pair("name", "String")];
        let err = match validate_typed_columns_vs_server::<One>(&server) {
            Ok(_) => panic!("count mismatch must reject"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("has 1 columns") && msg.contains("reports 2"),
            "expected count-mismatch wording, got: {msg}",
        );
    }

    #[test]
    fn typed_struct_name_mismatch_at_position_rejects() {
        use crate::Row;
        use serde::Serialize;
        #[derive(Row, Serialize)]
        #[clickhouse(crate = "crate")]
        #[allow(dead_code)]
        struct WrongName { id: u64, foo: String }
        let server = vec![pair("id", "UInt64"), pair("name", "String")];
        let err = match validate_typed_columns_vs_server::<WrongName>(&server) {
            Ok(_) => panic!("name mismatch must reject"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("position 1") && msg.contains("foo") && msg.contains("name"),
            "expected position-indexed name mismatch naming both sides, got: {msg}",
        );
    }

    #[test]
    fn typed_tuple_kind_count_only_accepts() {
        // Tuple rows have COLUMN_NAMES == &[]; only the count check fires.
        let server = vec![pair("id", "UInt64"), pair("name", "String")];
        validate_typed_columns_vs_server::<(u64, String)>(&server).unwrap();
    }

    #[test]
    fn typed_tuple_kind_count_mismatch_rejects() {
        let server = vec![pair("id", "UInt64")];
        let err = match validate_typed_columns_vs_server::<(u64, String)>(&server) {
            Ok(_) => panic!("count mismatch must reject"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("has 2 columns") && msg.contains("reports 1"),
            "expected count-mismatch wording, got: {msg}",
        );
    }
}
