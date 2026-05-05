//! HTTP `Format::Native` insert (layer 05c).
//!
//! Buffers RowBinary-serialised rows in memory and transposes them
//! into a single Native columnar block at [`end`][InsertNative::end]
//! time, then ships it through the existing
//! [`InsertFormatted`][crate::insert_formatted::InsertFormatted] HTTP
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

/// HTTP-side `Format::Native` insert. See module docs.
#[must_use]
pub struct InsertNative<T> {
    inner: InsertFormatted,
    column_schema: Vec<ColumnSchema>,
    rows: Vec<Vec<u8>>,
    revision: u64,
    _marker: PhantomData<fn() -> T>,
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
    pub fn with_columns(
        client: &Client,
        table: &str,
        columns: &[(String, String)],
    ) -> Result<Self> {
        let column_schema = ColumnSchema::from_headers(columns)?;
        let column_names = columns
            .iter()
            .map(|(n, _)| format!("`{}`", n.replace('`', "``")))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "INSERT INTO {table}({column_names}) FORMAT {fmt}",
            fmt = formats::NATIVE,
        );
        Ok(Self {
            inner: InsertFormatted::new(client, sql, Some(table)),
            column_schema,
            rows: Vec::new(),
            revision: DEFAULT_REVISION,
            _marker: PhantomData,
        })
    }

    pub(crate) fn new(client: &Client, table: &str, metadata: RowMetadata) -> Result<Self> {
        // Build (name, type_name) headers from the resolved metadata.
        let headers: Vec<(String, String)> = metadata
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.data_type.to_string()))
            .collect();
        let column_schema = ColumnSchema::from_headers(&headers)?;

        let column_names = headers
            .iter()
            .map(|(n, _)| format!("`{}`", n.replace('`', "``")))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "INSERT INTO {table}({column_names}) FORMAT {fmt}",
            fmt = formats::NATIVE,
        );

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

    /// Compose the full Native block (BlockInfo + counts + column
    /// data), ship it as the request body, and finalise the INSERT.
    pub async fn end(mut self) -> Result<()> {
        let n_rows = self.rows.len() as u64;
        let n_cols = self.column_schema.len() as u64;

        // Empty INSERTs still need a single empty block per the
        // ClickHouse `format=Native` protocol so the server doesn't
        // hang waiting for data.
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
        let mut body: Vec<u8> =
            Vec::with_capacity(column_bytes.len() + 24);
        BlockInfo::default().write_to_buf(&mut body);
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
