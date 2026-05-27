use hyper::{Method, Request, header::CONTENT_LENGTH};
use serde::Serialize;
use std::fmt::Display;
use tracing::Instrument;
use url::Url;

use crate::{
    Client,
    error::{Error, Result},
    formats,
    headers::with_request_headers,
    request_body::RequestBody,
    response::Response,
    row::{Row, RowOwned, RowRead},
    sql::{Bind, SqlBuilder, ser},
};

pub use crate::cursors::{BytesCursor, RowCursor};
use crate::headers::with_authentication;
use crate::settings;

#[must_use]
#[derive(Clone)]
pub struct Query {
    client: Client,
    sql: SqlBuilder,
    /// Caller assertion that this statement is safe to replay. Only
    /// consulted on the TCP transport's `execute()` path, where it
    /// gates opt-in auto-retry of `ExecuteQuery`. Default `false` --
    /// `execute()` is never auto-retried unless the caller opts in.
    /// See [`Query::idempotent`].
    idempotent: bool,
}

impl Query {
    pub(crate) fn new(client: &Client, template: &str) -> Self {
        Self {
            client: client.clone(),
            sql: SqlBuilder::new(template),
            idempotent: false,
        }
    }

    /// Assert that this query is safe to replay, enabling opt-in
    /// bounded-backoff auto-retry of a transient transport/connect
    /// failure on the TCP transport's [`Self::execute`] path.
    ///
    /// `execute()` covers arbitrary statements -- some non-idempotent
    /// (a plain `INSERT ... VALUES`, a mutation) -- so retry is NOT
    /// automatic there: it activates only when the caller asserts the
    /// statement can be safely re-issued (a `SELECT`, a `CREATE TABLE
    /// IF NOT EXISTS`, an `INSERT` carrying an
    /// `insert_deduplication_token`, etc.). The retry bounds come from
    /// [`Client::with_tcp_retry`][crate::Client::with_tcp_retry]; with
    /// no policy set this is a no-op beyond the always-on endpoint
    /// failover.
    ///
    /// No effect on the HTTP transport or on streaming SELECTs via
    /// `fetch_native_blocks` (those are inherently replay-safe and
    /// retry independently of this flag).
    pub fn idempotent(mut self) -> Self {
        self.idempotent = true;
        self
    }

    /// Display SQL query as string.
    pub fn sql_display(&self) -> &impl Display {
        &self.sql
    }

    /// Binds `value` to the next `?` in the query.
    ///
    /// The `value`, which must either implement [`Serialize`] or be an
    /// [`Identifier`], will be appropriately escaped.
    ///
    /// All possible errors will be returned as [`Error::InvalidParams`]
    /// during query execution (`execute()`, `fetch()`, etc.).
    ///
    /// WARNING: This means that the query must not have any extra `?`, even if
    /// they are in a string literal! Use `??` to have plain `?` in query.
    ///
    /// [`Serialize`]: serde::Serialize
    /// [`Identifier`]: crate::sql::Identifier
    #[track_caller]
    pub fn bind(mut self, value: impl Bind) -> Self {
        self.sql.bind_arg(value);
        self
    }

    /// Executes the query.
    ///
    /// If the underlying [`Client`] was constructed via
    /// [`Client::tcp`][crate::Client::tcp], the query is dispatched
    /// over the TCP transport (acquires a pool connection, runs the
    /// ExecuteQuery command, returns once the server emits
    /// EndOfStream). Otherwise the HTTP transport path is used.
    pub async fn execute(self) -> Result<()> {
        // Enter the span for the `self.do_execute()` call
        let span = self.make_span(None);

        async {
            #[cfg(feature = "tcp")]
            if self.client.tcp_pool().is_some() {
                // Snapshot the pieces the TCP path needs BEFORE
                // consuming `self.sql` -- `SqlBuilder::finish` takes
                // `self` by value, so we cannot call `tcp_settings`
                // after it.
                let pool = self.client.tcp_pool().cloned().unwrap();
                let retry = self.client.tcp_retry();
                let idempotent = self.idempotent;
                let query_id = self
                    .client
                    .get_setting(settings::QUERY_ID)
                    .map(str::to_string)
                    .unwrap_or_default();
                let tcp_settings = self.tcp_settings();
                let sql = self.sql.finish()?;
                return crate::tcp::client_ext::execute_query_via_pool(
                    &pool,
                    &query_id,
                    &sql,
                    &tcp_settings,
                    retry,
                    idempotent,
                )
                .await
                .inspect_err(|e| e.record_in_current_span("error executing tcp query"));
            }

            let mut response = self
                .do_execute(None)
                .inspect_err(|e| e.record_in_current_span("error executing query"))?;

            response
                .finish()
                .await
                .inspect_err(|e| e.record_in_current_span("response error"))
        }
        .instrument(span)
        .await
    }

    /// Run a streaming SELECT over the TCP transport and return a
    /// raw-block cursor.
    ///
    /// The cursor yields one
    /// [`crate::native::decode::DecodedBlock`] per call until the
    /// server emits `EndOfStream`. v1 only -- a per-row cursor that
    /// bridges to the `Row` trait lands in a follow-up.
    ///
    /// # Errors
    ///
    /// `Err(Error::Custom)` if this `Client` was not constructed via
    /// [`Client::tcp`][crate::Client::tcp]. The HTTP transport has no
    /// equivalent "decoded blocks" surface today -- it returns row-
    /// oriented `RowBinary` payloads; callers wanting whole-block
    /// iteration over HTTP can use `fetch_bytes("Native")` and decode
    /// themselves.
    #[cfg(feature = "tcp")]
    pub async fn fetch_native_blocks(self) -> Result<crate::tcp::cursor::TcpRawCursor> {
        let Some(pool) = self.client.tcp_pool().cloned() else {
            return Err(Error::Custom(
                "fetch_native_blocks requires a TCP-transport Client (Client::tcp)".into(),
            ));
        };
        let retry = self.client.tcp_retry();
        let query_id = self
            .client
            .get_setting(settings::QUERY_ID)
            .map(str::to_string)
            .unwrap_or_default();
        let tcp_settings = self.tcp_settings();
        let sql = self.sql.finish()?;
        crate::tcp::client_ext::execute_stream_via_pool(&pool, &query_id, &sql, &tcp_settings, retry)
            .await
    }

    /// Compose the `(name, value)` settings list to forward into a
    /// TCP Query packet. Mirrors what `do_execute` puts on the HTTP
    /// URL (database, plain settings, roles); omits the
    /// HTTP-transport-only knobs (compress / decompress /
    /// enable_http_compression / default_format).
    #[cfg(feature = "tcp")]
    pub(crate) fn tcp_settings(&self) -> Vec<(String, String)> {
        let mut out = Vec::new();
        if let Some(db) = &self.client.database {
            out.push((settings::DATABASE.to_string(), db.clone()));
        }
        for (k, v) in &self.client.settings {
            out.push((k.clone(), v.clone()));
        }
        for role in &self.client.roles {
            out.push((settings::ROLE.to_string(), role.clone()));
        }
        out
    }

    /// Executes the query, returning a [`RowCursor`] to obtain results.
    ///
    /// # Example
    ///
    /// ```
    /// # async fn example() -> clickhouse::error::Result<()> {
    /// #[derive(clickhouse::Row, serde::Deserialize)]
    /// struct MyRow<'a> {
    ///     no: u32,
    ///     name: &'a str,
    /// }
    ///
    /// let mut cursor = clickhouse::Client::default()
    ///     .query("SELECT ?fields FROM some WHERE no BETWEEN 0 AND 1")
    ///     .fetch::<MyRow<'_>>()?;
    ///
    /// while let Some(MyRow { name, no }) = cursor.next().await? {
    ///     println!("{name}: {no}");
    /// }
    /// # Ok(()) }
    /// ```
    pub fn fetch<T: Row>(mut self) -> Result<RowCursor<T>> {
        let validation = self.client.get_validation();
        let format = if validation {
            formats::ROW_BINARY_WITH_NAMES_AND_TYPES
        } else {
            formats::ROW_BINARY
        };

        let span = self.make_span(Some(format)).entered();

        self.sql.bind_fields::<T>();

        // Snapshot the KILL-on-drop handle BEFORE consuming
        // `self.client` in `do_execute`. Capture the runtime
        // Handle now so RowCursor::drop can spawn the kill on the
        // same runtime even if Drop runs after the TLS current-
        // runtime has been torn down (e.g. block_on scope-exit
        // after SIGTERM).
        let kill_on_drop = self
            .client
            .kill_on_drop_handle()
            .map(|(client, query_id)| crate::cursors::row::KillOnDropHandle {
                client,
                query_id,
                runtime: tokio::runtime::Handle::current(),
            });

        let response = self
            .do_execute(Some(format))
            .inspect_err(|e| e.record_in_current_span("error executing fetch"))?;

        Ok(RowCursor::new(
            response,
            validation,
            span.exit(),
            kill_on_drop,
        ))
    }

    /// Executes the query and returns just a single row.
    ///
    /// Note that `T` must be owned.
    pub async fn fetch_one<T>(self) -> Result<T>
    where
        T: RowOwned + RowRead,
    {
        match self.fetch::<T>()?.next().await {
            Ok(Some(row)) => Ok(row),
            Ok(None) => Err(Error::RowNotFound),
            Err(err) => Err(err),
        }
    }

    /// Executes the query and returns at most one row.
    ///
    /// Note that `T` must be owned.
    pub async fn fetch_optional<T>(self) -> Result<Option<T>>
    where
        T: RowOwned + RowRead,
    {
        self.fetch::<T>()?.next().await
    }

    /// Executes the query and returns all the generated results,
    /// collected into a Vec.
    ///
    /// Note that `T` must be owned.
    pub async fn fetch_all<T>(self) -> Result<Vec<T>>
    where
        T: RowOwned + RowRead,
    {
        let mut result = Vec::new();
        let mut cursor = self.fetch::<T>()?;

        while let Some(row) = cursor.next().await? {
            result.push(row);
        }

        Ok(result)
    }

    /// Executes the query, returning a [`BytesCursor`] to obtain results as raw
    /// bytes containing data in the [provided format].
    ///
    /// [provided format]: https://clickhouse.com/docs/en/interfaces/formats
    pub fn fetch_bytes(self, format: impl AsRef<str>) -> Result<BytesCursor> {
        let format = format.as_ref();

        let span = self.make_span(Some(format)).entered();

        let response = self.do_execute(Some(format))?;
        Ok(BytesCursor::new(response, span.exit()))
    }

    pub(crate) fn make_span(&self, response_format: Option<&str>) -> tracing::Span {
        // https://opentelemetry.io/docs/specs/semconv/db/sql/
        // TODO: write our own Semantic Conventions for ClickHouse
        tracing::info_span!(
            "clickhouse.query",
            // OTel conventional fields
            // Note that `Empty` or `Option::None` fields are not reported,
            // so we can avoid adding noise to logs when the `opentelemetry` feature is disabled.
            otel.status_code = tracing::field::Empty,
            otel.kind = cfg!(feature = "opentelemetry").then_some("client"),
            error.type = tracing::field::Empty,
            db.system.name = cfg!(feature = "opentelemetry").then_some("clickhouse"),
            // Only log full query text at TRACE level
            // Important that this is taken before client-side parameters are populated
            // FIXME: we can't use `enabled!` due to https://github.com/tokio-rs/tracing/issues/2448
            // but we don't want to log the full query at all verbosity levels.
            // db.query.text = tracing::enabled!(tracing::Level::TRACE).then(|| self.sql.to_string()),
            // TODO: generate summary
            db.query.summary = tracing::field::Empty,
            db.response.status_code = tracing::field::Empty,
            db.response.returned_rows = tracing::field::Empty,
            // ClickHouse-specific extension fields
            clickhouse.request.session_id = self.client.get_setting(settings::SESSION_ID),
            clickhouse.request.query_id = self.client.get_setting(settings::QUERY_ID),
            clickhouse.response.received_bytes = tracing::field::Empty,
            clickhouse.response.decoded_bytes = tracing::field::Empty,
            clickhouse.response.format = response_format,
        )
    }

    pub(crate) fn do_execute(self, default_format: Option<&str>) -> Result<Response> {
        let query = self.sql.finish()?;

        let mut url =
            Url::parse(&self.client.url).map_err(|err| Error::InvalidParams(Box::new(err)))?;
        let mut pairs = url.query_pairs_mut();
        pairs.clear();

        if let Some(format) = default_format {
            pairs.append_pair(settings::DEFAULT_FORMAT, format);
        }

        if let Some(database) = &self.client.database {
            pairs.append_pair(settings::DATABASE, database);
        }

        if self.client.compression.is_enabled() {
            #[cfg(feature = "zstd")]
            if matches!(self.client.compression, crate::Compression::Zstd(_)) {
                pairs.append_pair(settings::ENABLE_HTTP_COMPRESSION, "1");
            } else {
                pairs.append_pair(settings::COMPRESS, "1");
            }

            #[cfg(not(feature = "zstd"))]
            pairs.append_pair(settings::COMPRESS, "1");
        }

        for (name, value) in &self.client.settings {
            pairs.append_pair(name, value);
        }

        pairs.extend_pairs(self.client.roles.iter().map(|role| (settings::ROLE, role)));

        drop(pairs);

        let mut builder = Request::builder().method(Method::POST).uri(url.as_str());
        builder = with_request_headers(builder, &self.client.headers, &self.client.products_info);
        builder = with_authentication(builder, &self.client.authentication);

        #[cfg(feature = "zstd")]
        if matches!(self.client.compression, crate::Compression::Zstd(_)) {
            builder = builder.header("Accept-Encoding", "zstd");
        }

        let content_length = query.len();
        builder = builder.header(CONTENT_LENGTH, content_length.to_string());

        let request = builder.body(RequestBody::full(query)).map_err(|err| {
            let err = Error::InvalidParams(Box::new(err));
            err.record_in_current_span("invalid params in query");
            err
        })?;

        let future = self.client.http.request(request);
        Ok(Response::new(
            future,
            self.client.compression,
            self.client.progress_callback(),
        ))
    }

    /// Configure the [roles] to use when executing this query.
    ///
    /// Overrides any roles previously set by this method, [`Query::with_setting`],
    /// [`Client::with_roles`] or [`Client::with_setting`].
    ///
    /// An empty iterator may be passed to clear the set roles.
    ///
    /// [roles]: https://clickhouse.com/docs/operations/access-rights#role-management
    pub fn with_roles(self, roles: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            client: self.client.with_roles(roles),
            ..self
        }
    }

    /// Configure a single role for this query. Thin convenience over
    /// [`with_roles`][Self::with_roles] for the common single-role
    /// case; saves the `[role]` iterator construction at the call
    /// site. Same override semantics as `with_roles`.
    ///
    /// [roles]: https://clickhouse.com/docs/operations/access-rights#role-management
    pub fn with_role(self, role: impl Into<String>) -> Self {
        self.with_roles(std::iter::once(role))
    }

    /// Set the server-side `session_id` for this query.
    ///
    /// Avoids `SET role` / `SET session ...` leakage across users on
    /// a pooled HTTP/1.1 keep-alive connection: ClickHouse natively
    /// supports `session_id` as a URL parameter, scoping the session
    /// to this request. The id is opaque to the server -- callers
    /// pick the value (UUID or app-prefixed string).
    pub fn with_session_id(self, id: impl Into<String>) -> Self {
        self.with_setting(crate::settings::SESSION_ID, id)
    }

    /// Toggle server-side `async_insert` for this query.
    ///
    /// Wraps the two underlying settings:
    ///   - `async_insert=1`
    ///   - `wait_for_async_insert={wait as int}`
    ///
    /// `wait=true` (recommended): client awaits the server-side
    /// flush. Atomicity per-query is preserved, but see
    /// [ClickHouse#86651](https://github.com/ClickHouse/ClickHouse/issues/86651)
    /// for the flush-poisoning hazard. `wait=false` is fire-and-
    /// forget: the server acknowledges the queue insert immediately
    /// and applies the row at the next flush. No atomicity, no
    /// row-level errors; rows may be lost if the server crashes
    /// before flush.
    ///
    /// Server-side async_insert is orthogonal to
    /// [`AsyncInserter<T>`][crate::async_inserter::AsyncInserter]:
    /// client-side batching produces large blocks for the server to
    /// then queue. Combining both is the typical PB/hr ingest
    /// pattern.
    pub fn async_insert(self, wait: bool) -> Self {
        self.with_setting("async_insert", "1")
            .with_setting("wait_for_async_insert", if wait { "1" } else { "0" })
    }

    /// Clear any explicit [roles] previously set on this `Query` or inherited from [`Client`].
    ///
    /// Overrides any roles previously set by [`Query::with_roles`], [`Query::with_setting`],
    /// [`Client::with_roles`] or [`Client::with_setting`].
    ///
    /// [roles]: https://clickhouse.com/docs/operations/access-rights#role-management
    pub fn with_default_roles(self) -> Self {
        Self {
            client: self.client.with_default_roles(),
            ..self
        }
    }

    /// Similar to [`Client::with_option`], but for this particular query only.
    #[deprecated(since = "0.14.3", note = "please use `with_setting` instead")]
    pub fn with_option(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.client.set_setting(name, value);
        self
    }

    /// Similar to [`Client::with_setting`], but for this particular query only.
    pub fn with_setting(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.client.set_setting(name, value);
        self
    }

    /// Set the server-side `query_id` for this query.
    ///
    /// The id is sent as a URL parameter and appears in
    /// `system.query_log` / `system.processes`. Pair with
    /// [`Client::kill_query`][crate::Client::kill_query] to cancel
    /// in-flight queries when the consumer abandons the cursor
    /// before draining the full result -- otherwise the server
    /// keeps processing until it tries to write to a dead socket.
    ///
    /// Convention: use a UUID (v7 preferred for k-sortability) or
    /// an app-prefixed identifier (`"my-app/req-12345"`).
    pub fn with_query_id(self, id: impl Into<String>) -> Self {
        self.with_setting(crate::settings::QUERY_ID, id)
    }

    /// Specify server side parameter for query.
    ///
    /// In queries, you can reference params as {name: type} e.g. {val: Int32}.
    pub fn param(mut self, name: &str, value: impl Serialize) -> Self {
        let mut param = String::from("");
        if let Err(err) = ser::write_param(&mut param, &value) {
            self.sql = SqlBuilder::Failed(format!("invalid param: {err}"));
            self
        } else {
            self.with_setting(format!("param_{name}"), param)
        }
    }
}
