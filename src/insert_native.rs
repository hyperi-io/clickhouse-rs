//! HTTP `Format::Native` insert.
//!
//! Buffers RowBinary-serialised rows in memory and transposes them
//! into a single Native columnar block at [`end`][InsertNative::end]
//! time, then ships it through the existing
//! [`crate::insert_formatted::InsertFormatted`] HTTP
//! path with `format=Native`.
//!
//! This is the Wave 1 MVP path -- one block per `InsertNative`. Larger
//! workloads (millions of rows) will want chunked blocks; that is a
//! follow-up. The primitives in [`crate::native`] support chunking
//! already; only the buffering policy here needs revising.
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
//! insert.write(&Event { id: 1, name: "a".into() })?;
//! insert.write(&Event { id: 2, name: "b".into() })?;
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

/// Server protocol revision the encoder reports as. The encoder uses
/// this only to decide whether to emit a per-column
/// `custom_serialization` flag byte (required from rev 54454 onwards
/// -- ClickHouse 24.x and newer). Default to a "modern" revision since
/// virtually all in-the-wild servers exceed it.
const DEFAULT_REVISION: u64 = 54454;

/// HTTP-side `Format::Native` insert. See module docs.
#[must_use]
pub struct InsertNative<T> {
    inner: InsertFormatted,
    column_schema: Vec<ColumnSchema>,
    rows: Vec<Vec<u8>>,
    revision: u64,
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
            inner: InsertFormatted::new(client, sql, Some(&escaped_table)),
            column_schema,
            rows: Vec::new(),
            revision: DEFAULT_REVISION,
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
            revision: DEFAULT_REVISION,
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

    /// Buffer one row. The row is serialised via the existing
    /// RowBinary path; columns are produced by transposing at
    /// [`end`][Self::end] time.
    pub fn write(&mut self, row: &T::Value<'_>) -> Result<()>
    where
        T: RowWrite,
    {
        let mut buf: Vec<u8> = Vec::with_capacity(64);
        serialize_row_binary(&mut buf, row)?;
        self.rows.push(buf);
        Ok(())
    }

    /// Compose the full Native block (counts + column data), ship it
    /// as the request body, and finalise the INSERT.
    pub async fn end(mut self) -> Result<()> {
        let n_rows = self.rows.len() as u64;
        let n_cols = self.column_schema.len() as u64;

        // ClickHouse reads HTTP `FORMAT Native` at server_revision = 0:
        // NO BlockInfo, NO per-column custom_serialization flag. Emit
        // counts + per-column (name, type, data). Headers are emitted
        // even for a 0-row block so num_columns matches the stream
        // (the server still wants one empty block per INSERT so it
        // doesn't hang waiting for data).
        let column_bytes = encode_columns(&self.rows, &self.column_schema, 0)?;

        // Compose the block envelope:
        //   varint(num_columns)
        //   varint(num_rows)
        //   column_bytes (per-column header + data)
        let mut body: Vec<u8> = Vec::with_capacity(column_bytes.len() + 16);
        body.put_var_uint(n_cols);
        body.put_var_uint(n_rows);
        body.extend_from_slice(&column_bytes);

        self.inner.send(body.into()).await?;
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
