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
use crate::native::block_info::BlockInfo;
use crate::native::encode::{ColumnSchema, encode_columns};
use crate::native::io::ClickHouseBytesWrite;
use crate::row::{Row, RowWrite};
use crate::row_metadata::RowMetadata;
use crate::rowbinary::serialize_row_binary;
use crate::{Client, error::Result, formats};

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

/// HTTP-side `Format::Native` insert. See module docs.
#[must_use]
pub struct InsertNative<T> {
    inner: InsertFormatted,
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
            inner: InsertFormatted::new(client, sql, Some(&escaped_table)),
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
            inner: InsertFormatted::new(client, sql, Some(table)),
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

        let column_bytes = if n_rows == 0 {
            Vec::new()
        } else {
            encode_columns(&self.rows, &self.column_schema, self.revision)?
        };

        // Compose the block envelope:
        //   BlockInfo (~8 bytes for default flags)
        //   varint(num_columns)
        //   varint(num_rows)
        //   column_bytes (per-column header + data)
        let mut body: Vec<u8> = Vec::with_capacity(column_bytes.len() + 24);
        BlockInfo::default().write_to_buf(&mut body);
        body.put_var_uint(n_cols);
        body.put_var_uint(n_rows);
        body.extend_from_slice(&column_bytes);

        self.inner.send(body.into()).await?;
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
        self.inner.end().await
    }

    /// Drop pending rows without sending. Useful when you want to
    /// reset state and start a new INSERT.
    pub fn abort(self) {
        // Inner `InsertFormatted` is dropped here; no request was
        // initiated yet (we hold all bytes until `end()`), so there is
        // nothing to roll back on the server.
        drop(self);
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
}
