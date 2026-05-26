#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub use self::{
    compression::Compression,
    query_summary::QuerySummary,
    row::{Row, RowOwned, RowRead, RowWrite},
};
use self::{error::Result, http_client::HttpClient};
use crate::row_metadata::{AccessType, ColumnDefaultKind, InsertMetadata, RowMetadata};

#[doc = include_str!("row_derive.md")]
pub use clickhouse_macros::Row;
use clickhouse_types::{Column, DataTypeNode};

use crate::error::Error;
use std::collections::HashSet;
use std::time::Duration;
use std::{collections::HashMap, fmt::Display, sync::Arc};
use tokio::sync::RwLock;

#[cfg(feature = "inserter")]
pub mod async_inserter;
pub mod batch_isolation;
pub mod error;
pub mod insert;
pub mod insert_formatted;
pub mod insert_native;
#[cfg(feature = "inserter")]
pub mod inserter;
pub mod progress;
pub mod query;
pub mod recovery;
pub mod serde;
pub mod sql;
#[cfg(feature = "test-util")]
pub mod test;
// Internal background-worker primitive (CommandWorker trait + spawn).
// `pub(crate)` until a second consumer arrives -- currently only
// `async_inserter` uses it. Promote to `pub mod` when the TCP transport
// or another long-lived actor lands.
pub(crate) mod worker;

pub mod types;

mod bytes_ext;
mod compression;
pub mod native;
#[cfg(feature = "tcp")]
pub(crate) mod tcp;
mod cursors;
mod headers;
mod http_client;
mod query_summary;
mod request_body;
mod response;
mod row;
mod row_metadata;
mod rowbinary;
#[cfg(feature = "inserter")]
mod ticks;
// Shared TLS trust (HTTP + TCP). Gated to whenever any rustls path is
// active; the `native-tls-rustls` arm matches downstream where it is
// defined.
#[cfg(any(
    feature = "rustls-tls-aws-lc",
    feature = "rustls-tls-ring",
    feature = "native-tls-rustls"
))]
pub(crate) mod tls;

/// A client containing HTTP pool.
///
/// ### Cloning behavior
/// Clones share the same HTTP transport but store their own configurations.
/// Any `with_*` configuration method (e.g., [`Client::with_setting`]) applies
/// only to future clones, because [`Client::clone`] creates a deep copy
/// of the [`Client`] configuration, except the transport.
#[derive(Clone)]
pub struct Client {
    http: Arc<dyn HttpClient>,

    url: String,
    database: Option<String>,
    authentication: Authentication,
    compression: Compression,
    roles: HashSet<String>,
    settings: HashMap<String, String>,
    headers: HashMap<String, String>,
    products_info: Vec<ProductInfo>,
    validation: bool,
    insert_metadata_cache: Arc<InsertMetadataCache>,

    /// Optional callback invoked for each `X-ClickHouse-Progress`
    /// response header. See [`crate::progress`] for the
    /// shape and server-side enablement (`send_progress_in_http_headers=1`).
    progress_callback: Option<progress::ProgressCallback>,

    /// When true, [`query`][Self::query] auto-generates a UUIDv7
    /// `query_id` for any query that doesn't already have one.
    /// Off by default; opt in via
    /// [`with_auto_query_id`][Self::with_auto_query_id].
    /// Only meaningful with the `uuid` feature.
    #[cfg(feature = "uuid")]
    auto_query_id: bool,

    /// When true AND a query has a `query_id` (manually set or
    /// auto-generated), `RowCursor::drop` spawns a background task
    /// that issues `KILL QUERY` for the still-in-flight query.
    /// Opt in via [`with_kill_on_drop`][Self::with_kill_on_drop].
    kill_on_drop: bool,

    /// Pool configuration baked into `self.http`. Stored so
    /// `with_pool_*` builder methods can rebuild the http client
    /// with the updated value. `Default::default()` matches the
    /// hardcoded behaviour of prior versions
    /// (idle_timeout=2s, no per-host cap, keepalive=60s).
    pool_config: http_client::PoolConfig,

    /// Declarative or explicit TLS trust source (None = default
    /// per-transport behaviour, happy path untouched).
    #[cfg(any(
        feature = "rustls-tls-aws-lc",
        feature = "rustls-tls-ring",
        feature = "native-tls-rustls"
    ))]
    tls_source: Option<tls::TlsConfigSource>,
    /// Cached resolved config, kept in sync with `tls_source` by the
    /// TLS builder methods so non-TLS rebuilds reuse it infallibly.
    #[cfg(any(
        feature = "rustls-tls-aws-lc",
        feature = "rustls-tls-ring",
        feature = "native-tls-rustls"
    ))]
    tls_resolved: Option<std::sync::Arc<rustls::ClientConfig>>,

    #[cfg(feature = "test-util")]
    mocked: bool,
}

#[derive(Clone)]
struct ProductInfo {
    name: String,
    version: String,
}

impl Display for ProductInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.name, self.version)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Authentication {
    Credentials {
        user: Option<String>,
        password: Option<String>,
    },
    Jwt {
        access_token: String,
    },
}

impl Default for Authentication {
    fn default() -> Self {
        Self::Credentials {
            user: None,
            password: None,
        }
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::with_http_client(http_client::default())
    }
}

/// Cache for [`RowMetadata`] to avoid allocating it for the same struct more than once
/// during the application lifecycle. Key: fully qualified table name (e.g. `database.table`).
#[derive(Default)]
pub(crate) struct InsertMetadataCache(RwLock<HashMap<String, Arc<InsertMetadata>>>);

/// Durability mode for INSERTs into Distributed-engine tables.
///
/// Distributed targets fan out writes from a coordinator node to
/// shard nodes. The default mode acks the client as soon as the
/// coordinator has buffered the data locally; if the coordinator
/// crashes before forwarding, data is lost. The other two modes
/// trade throughput for stronger durability.
///
/// Maps to ClickHouse session settings. No effect on non-Distributed
/// tables (the settings simply do nothing).
///
/// Background on the coordinator-crash window:
/// [ClickHouse#17380](https://github.com/ClickHouse/ClickHouse/issues/17380).
///
/// # Interaction with `Query::async_insert` and dedup tokens
///
/// `Durability`, `Query::async_insert(wait)`, and
/// `Client::insert_batch_with_isolation_with_token` are three
/// independent durability dimensions and combine sensibly:
///
/// | Durability         | async_insert(true)         | async_insert(false)        | dedup-token retry         |
/// |--------------------|----------------------------|----------------------------|---------------------------|
/// | Background         | server queues; client waits | server queues; fire-and-forget | safe (caller retries on transport failure) |
/// | Foreground         | server queues; client waits for shard ack | server queues; client returns when queued | shard-local dedup; non-Replicated under Distributed gives no cross-shard safety |
/// | ForegroundFsynced  | as Foreground + fsync gate | as Foreground + fsync gate | as Foreground |
///
/// The combinations are not mutually exclusive; CH evaluates the
/// settings independently. Caveats worth knowing:
///
/// - `async_insert(false)` with `Foreground` puts the fsync inside
///   the async-flush worker on the server, NOT on the request path
///   (the request returns once the row is queued for flush). This
///   weakens the durability guarantee from "data is on disk before
///   ack" to "data is queued for the flush worker before ack".
/// - Dedup tokens forwarded by Distributed tables get evaluated
///   per-shard; non-Replicated MergeTree under Distributed gives
///   no cross-node dedup. Replicated*MergeTree is required for
///   retry-safe at-least-once with dedup tokens under Distributed.
/// - Combining Durability + dedup tokens does not require
///   `async_insert`; the three layer cleanly.
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub enum Durability {
    /// Coordinator acks the client as soon as data is buffered on
    /// its local disk; forwards to shards lazily. Highest throughput;
    /// crash window between ack and shard delivery loses data.
    /// Default: matches CH server's default
    /// (`distributed_foreground_insert=0`).
    #[default]
    Background,
    /// Coordinator blocks the ack until shards have received the
    /// data. No crash window for delivery. Throughput cost depends
    /// on shard count and network latency.
    /// Maps to `distributed_foreground_insert=1`.
    Foreground,
    /// Foreground + fsync at both the file and directory level on
    /// the coordinator. Maximum durability available; for use when
    /// the coordinator's local disk is a real durability boundary
    /// (typically NVMe with battery-backed write cache).
    /// Maps to `distributed_foreground_insert=1,
    /// fsync_after_insert=1, fsync_directories=1`.
    ForegroundFsynced,
}

impl Client {
    /// Creates a new client with a specified underlying HTTP client.
    ///
    /// See `HttpClient` for details.
    pub fn with_http_client(client: impl HttpClient) -> Self {
        Self {
            http: Arc::new(client),
            url: String::new(),
            database: None,
            authentication: Authentication::default(),
            compression: Compression::default(),
            roles: HashSet::new(),
            settings: HashMap::new(),
            headers: HashMap::new(),
            products_info: Vec::default(),
            validation: true,
            insert_metadata_cache: Arc::new(InsertMetadataCache::default()),
            progress_callback: None,
            #[cfg(feature = "uuid")]
            auto_query_id: false,
            kill_on_drop: false,
            pool_config: http_client::PoolConfig::default(),
            #[cfg(any(
                feature = "rustls-tls-aws-lc",
                feature = "rustls-tls-ring",
                feature = "native-tls-rustls"
            ))]
            tls_source: None,
            #[cfg(any(
                feature = "rustls-tls-aws-lc",
                feature = "rustls-tls-ring",
                feature = "native-tls-rustls"
            ))]
            tls_resolved: None,
            #[cfg(feature = "test-util")]
            mocked: false,
        }
    }

    /// Idle-connection timeout for the default HTTP client's pool.
    /// Default: 2 seconds (matches ClickHouse server's keep-alive
    /// default). For steady-state high-throughput ingest, raise this
    /// (e.g. 30 s) so the pool doesn't churn connections between
    /// bursts.
    ///
    /// **Constructor-time only.** Rebuilds the underlying HTTP
    /// client. Call before issuing any requests; calling mid-flight
    /// drops `self.http`'s `Arc` but in-flight requests keep their
    /// own handle, so two pools run in parallel until the in-flight
    /// requests drain. Repeated mid-flight calls (e.g. from a
    /// config-watcher) compound this. No effect if the client was
    /// constructed via
    /// [`with_http_client`][Self::with_http_client] (caller-supplied
    /// HTTP clients aren't rebuilt).
    pub fn with_pool_idle_timeout(mut self, d: Duration) -> Self {
        self.pool_config.idle_timeout = d;
        self.rebuild_http_client();
        self
    }

    /// Maximum number of idle pooled connections per host. Default
    /// (unset) lets hyper use its built-in default (currently
    /// unbounded). `clickhouse-go` defaults to 5; raise based on
    /// concurrent-query peak per host.
    ///
    /// Rebuilds the underlying HTTP client. See
    /// [`with_pool_idle_timeout`][Self::with_pool_idle_timeout] for
    /// the rebuild caveat.
    pub fn with_pool_max_idle_per_host(mut self, n: usize) -> Self {
        self.pool_config.max_idle_per_host = Some(n);
        self.rebuild_http_client();
        self
    }

    /// TCP keep-alive interval for outbound connections. Default:
    /// 60 s. Drives the OS-level KEEPALIVE probes; orthogonal to
    /// the pool's idle-timeout (which evicts conns the client
    /// hasn't reused). Rebuilds the HTTP client.
    pub fn with_tcp_keepalive(mut self, d: Duration) -> Self {
        self.pool_config.tcp_keepalive = d;
        self.rebuild_http_client();
        self
    }

    /// Resolve the TLS config for an HTTP-client rebuild, fail-closed.
    ///
    /// When a trust was configured (`tls_source` is Some) but did not
    /// resolve (`tls_resolved` is None), return an empty-roots config
    /// that rejects every handshake -- never the default webpki path
    /// (broad trust). Used by EVERY http rebuild so a non-TLS rebuild
    /// (e.g. `with_pool_idle_timeout`) can't silently revert a
    /// configured-but-unresolved trust to fail-open.
    #[cfg(any(feature = "rustls-tls-aws-lc", feature = "rustls-tls-ring"))]
    fn http_tls_config(&self) -> Option<std::sync::Arc<rustls::ClientConfig>> {
        match (&self.tls_source, &self.tls_resolved) {
            #[cfg(not(feature = "native-tls"))]
            (Some(_), None) => Some(tls::build_failclosed_config()),
            (_, resolved) => resolved.clone(),
        }
    }

    fn rebuild_http_client(&mut self) {
        // Only meaningful when using the default HTTP client; a
        // caller-supplied implementation has its own pool config.
        // We can't distinguish at runtime which case we're in -- if
        // the caller passed a custom HttpClient via `with_http_client`,
        // this rebuild silently replaces it with the default. Callers
        // mixing `with_http_client` + `with_pool_*` should treat the
        // last call as authoritative.
        self.http = Arc::new(http_client::with_pool_config(
            self.pool_config.clone(),
            #[cfg(any(feature = "rustls-tls-aws-lc", feature = "rustls-tls-ring"))]
            self.http_tls_config(),
        ));
    }

    /// Specifies ClickHouse's url. Should point to HTTP endpoint.
    ///
    /// Automatically [clears the metadata cache][Self::clear_cached_metadata]
    /// for this instance only.
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_url("http://localhost:8123");
    /// ```
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();

        // `with_mock()` didn't exist previously, so to not break existing usages,
        // we need to be able to detect a mocked server using nothing but the URL.
        #[cfg(feature = "test-util")]
        if let Some(url) = test::Mock::mocked_url_to_real(&self.url) {
            self.url = url;
            self.mocked = true;
        }

        // Assume our cached metadata is invalid.
        self.insert_metadata_cache = Default::default();

        self
    }

    /// Specifies a database name.
    ///
    /// Automatically [clears the metadata cache][Self::clear_cached_metadata]
    /// for this instance only.
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_database("test");
    /// ```
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());

        // Assume our cached metadata is invalid.
        self.insert_metadata_cache = Default::default();

        self
    }

    /// Specifies a user.
    ///
    /// # Panics
    /// If called after [`Client::with_access_token`].
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_user("test");
    /// ```
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        match self.authentication {
            Authentication::Jwt { .. } => {
                panic!("`user` cannot be set together with `access_token`");
            }
            Authentication::Credentials { password, .. } => {
                self.authentication = Authentication::Credentials {
                    user: Some(user.into()),
                    password,
                };
            }
        }
        self
    }

    /// Specifies a password.
    ///
    /// # Panics
    /// If called after [`Client::with_access_token`].
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_password("secret");
    /// ```
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        match self.authentication {
            Authentication::Jwt { .. } => {
                panic!("`password` cannot be set together with `access_token`");
            }
            Authentication::Credentials { user, .. } => {
                self.authentication = Authentication::Credentials {
                    user,
                    password: Some(password.into()),
                };
            }
        }
        self
    }

    /// Configure the [roles] to use when executing statements with this `Client` instance.
    ///
    /// Overrides any roles previously set by this method or [`Client::with_setting`].
    ///
    /// Call [`Client::with_default_roles`] to clear any explicitly set roles.
    ///
    /// This setting is copied into cloned clients.
    ///
    /// [roles]: https://clickhouse.com/docs/operations/access-rights#role-management
    ///
    /// # Examples
    ///
    /// ```
    /// # use clickhouse::Client;
    ///
    /// // Single role
    /// let client = Client::default().with_roles(["foo"]);
    ///
    /// // Multiple roles
    /// let client = Client::default().with_roles(["foo", "bar", "baz"]);
    /// ```
    pub fn with_roles(mut self, roles: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.set_roles(roles);
        self
    }

    /// Clear any explicitly set [roles] from this `Client` instance.
    ///
    /// Overrides any roles previously set by [`Client::with_roles`] or [`Client::with_setting`].
    ///
    /// [roles]: https://clickhouse.com/docs/operations/access-rights#role-management
    pub fn with_default_roles(mut self) -> Self {
        self.clear_roles();
        self
    }

    /// A JWT access token to authenticate with ClickHouse.
    /// JWT token authentication is supported in ClickHouse Cloud only.
    /// Should not be called after [`Client::with_user`] or
    /// [`Client::with_password`].
    ///
    /// # Panics
    /// If called after [`Client::with_user`] or [`Client::with_password`].
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_access_token("jwt");
    /// ```
    pub fn with_access_token(mut self, access_token: impl Into<String>) -> Self {
        match self.authentication {
            Authentication::Credentials { user, password }
                if user.is_some() || password.is_some() =>
            {
                panic!("`access_token` cannot be set together with `user` or `password`");
            }
            _ => {
                self.authentication = Authentication::Jwt {
                    access_token: access_token.into(),
                }
            }
        }
        self
    }

    /// Specifies a compression mode. See [`Compression`] for details.
    /// By default, `Lz4` is used if the `lz4` feature is enabled.
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::{Client, Compression};
    /// # #[cfg(feature = "lz4")]
    /// let client = Client::default().with_compression(Compression::Lz4);
    /// # #[cfg(feature = "zstd")]
    /// let client = Client::default().with_compression(Compression::zstd());
    /// ```
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Provide a fully-built rustls `ClientConfig` used for BOTH the
    /// HTTP and TCP transports (the rustls analog of clickhouse-go's
    /// `Options.TLS`). Overrides any accumulated `with_tls_*` trust.
    #[cfg(any(
        feature = "rustls-tls-aws-lc",
        feature = "rustls-tls-ring",
        feature = "native-tls-rustls"
    ))]
    pub fn with_tls_config(mut self, config: rustls::ClientConfig) -> Self {
        let arc = std::sync::Arc::new(config);
        self.tls_source = Some(tls::TlsConfigSource::Explicit(arc.clone()));
        self.tls_resolved = Some(arc);
        self.rebuild_http_for_tls();
        self
    }

    /// Toggle OS native-root trust (default on once any with_tls_* is
    /// used). Applies to whichever transport the client uses.
    #[cfg(any(
        feature = "rustls-tls-aws-lc",
        feature = "rustls-tls-ring",
        feature = "native-tls-rustls"
    ))]
    pub fn with_tls_native_roots(mut self, enabled: bool) -> Self {
        self.mutate_trust(|t| t.native_roots = enabled);
        self
    }

    /// Toggle the compiled-in webpki bundle.
    #[cfg(any(
        feature = "rustls-tls-aws-lc",
        feature = "rustls-tls-ring",
        feature = "native-tls-rustls"
    ))]
    pub fn with_tls_webpki_roots(mut self, enabled: bool) -> Self {
        self.mutate_trust(|t| t.webpki_roots = enabled);
        self
    }

    /// Trust ONLY the explicit CA files; ignore native + webpki.
    /// Requires at least one `try_with_tls_root_ca` / intermediate.
    #[cfg(any(
        feature = "rustls-tls-aws-lc",
        feature = "rustls-tls-ring",
        feature = "native-tls-rustls"
    ))]
    pub fn with_tls_roots_exclusive(mut self) -> Self {
        self.mutate_trust(|t| t.exclusive = true);
        self
    }

    /// Add a root CA PEM file (may bundle many certs; all are loaded).
    /// Fallible: reads the file now to surface bad paths / empty files.
    #[cfg(any(
        feature = "rustls-tls-aws-lc",
        feature = "rustls-tls-ring",
        feature = "native-tls-rustls"
    ))]
    pub fn try_with_tls_root_ca(mut self, pem_path: impl AsRef<std::path::Path>) -> Result<Self> {
        let p = pem_path.as_ref().to_path_buf();
        self.try_mutate_trust(|t| t.extra_roots.push(p))?;
        Ok(self)
    }

    /// Add an intermediate CA PEM file (added as anchors too, for
    /// servers that do not present their chain / multi-tier PKI).
    #[cfg(any(
        feature = "rustls-tls-aws-lc",
        feature = "rustls-tls-ring",
        feature = "native-tls-rustls"
    ))]
    pub fn try_with_tls_intermediate_certs(
        mut self,
        pem_path: impl AsRef<std::path::Path>,
    ) -> Result<Self> {
        let p = pem_path.as_ref().to_path_buf();
        self.try_mutate_trust(|t| t.extra_intermediates.push(p))?;
        Ok(self)
    }

    // --- internal TLS helpers ---

    #[cfg(any(
        feature = "rustls-tls-aws-lc",
        feature = "rustls-tls-ring",
        feature = "native-tls-rustls"
    ))]
    fn current_trust(&self) -> tls::TlsTrust {
        match &self.tls_source {
            Some(tls::TlsConfigSource::Trust(t)) => t.clone(),
            _ => tls::TlsTrust::default(),
        }
    }

    /// Infallible flag mutation. Re-resolves and records the resolve
    /// OUTCOME: on a resolve error `tls_resolved` becomes None while
    /// `tls_source` still records the configured intent. Fail-closed --
    /// transports MUST NOT fall back to default/broad trust when a trust
    /// was configured (tls_source is Some) -- see the build sites.
    #[cfg(any(
        feature = "rustls-tls-aws-lc",
        feature = "rustls-tls-ring",
        feature = "native-tls-rustls"
    ))]
    fn mutate_trust(&mut self, f: impl FnOnce(&mut tls::TlsTrust)) {
        let mut trust = self.current_trust();
        f(&mut trust);
        let src = tls::TlsConfigSource::Trust(trust);
        self.tls_resolved = tls::build_client_config(&src).ok();
        self.tls_source = Some(src);
        self.rebuild_http_for_tls();
    }

    /// Fallible mutation (reads CA files). Surfaces resolve errors.
    #[cfg(any(
        feature = "rustls-tls-aws-lc",
        feature = "rustls-tls-ring",
        feature = "native-tls-rustls"
    ))]
    fn try_mutate_trust(&mut self, f: impl FnOnce(&mut tls::TlsTrust)) -> Result<()> {
        let mut trust = self.current_trust();
        f(&mut trust);
        let src = tls::TlsConfigSource::Trust(trust);
        let cfg = tls::build_client_config(&src)?;
        self.tls_resolved = Some(cfg);
        self.tls_source = Some(src);
        self.rebuild_http_for_tls();
        Ok(())
    }

    /// Rebuild the HTTP client so a TLS change takes effect on the HTTP
    /// transport. (TCP reads tls_resolved at pool-build time.)
    #[cfg(all(
        any(feature = "rustls-tls-aws-lc", feature = "rustls-tls-ring"),
        not(feature = "native-tls")
    ))]
    fn rebuild_http_for_tls(&mut self) {
        // Shares the single fail-closed-aware rebuild path so a TLS
        // change and a non-TLS pool rebuild can't diverge. See
        // `http_tls_config` for the fail-closed substitution.
        self.rebuild_http_client();
    }

    /// No-op when HTTP rustls is not the active TLS path (e.g. native-tls
    /// feature, or TCP-only TLS build).
    #[cfg(all(
        not(all(
            any(feature = "rustls-tls-aws-lc", feature = "rustls-tls-ring"),
            not(feature = "native-tls")
        )),
        any(
            feature = "rustls-tls-aws-lc",
            feature = "rustls-tls-ring",
            feature = "native-tls-rustls"
        )
    ))]
    fn rebuild_http_for_tls(&mut self) {}

    /// Used to specify settings that will be passed to all queries.
    ///
    /// # Example
    /// ```
    /// # use clickhouse::Client;
    /// Client::default().with_option("allow_nondeterministic_mutations", "1");
    /// ```
    #[deprecated(since = "0.14.3", note = "please use `with_setting` instead")]
    pub fn with_option(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.settings.insert(name.into(), value.into());
        self
    }

    /// Used to specify settings that will be passed to all queries.
    ///
    /// # Example
    /// ```
    /// # use clickhouse::Client;
    /// Client::default().with_setting("allow_nondeterministic_mutations", "1");
    /// ```
    pub fn with_setting(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.settings.insert(name.into(), value.into());
        self
    }

    /// Used to specify a header that will be passed to all queries.
    ///
    /// # Example
    /// ```
    /// # use clickhouse::Client;
    /// Client::default().with_header("Cookie", "A=1");
    /// ```
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    /// Specifies the product name and version that will be included
    /// in the default User-Agent header. Multiple products are supported.
    /// This could be useful for the applications built on top of this client.
    ///
    /// # Examples
    ///
    /// Sample default User-Agent header:
    ///
    /// ```plaintext
    /// clickhouse-rs/0.12.2 (lv:rust/1.67.0, os:macos)
    /// ```
    ///
    /// Sample User-Agent with a single product information:
    ///
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_product_info("MyDataSource", "v1.0.0");
    /// ```
    ///
    /// ```plaintext
    /// MyDataSource/v1.0.0 clickhouse-rs/0.12.2 (lv:rust/1.67.0, os:macos)
    /// ```
    ///
    /// Sample User-Agent with multiple products information
    /// (NB: the products are added in the reverse order of
    /// [`Client::with_product_info`] calls, which could be useful to add
    /// higher abstraction layers first):
    ///
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default()
    ///     .with_product_info("MyDataSource", "v1.0.0")
    ///     .with_product_info("MyApp", "0.0.1");
    /// ```
    ///
    /// ```plaintext
    /// MyApp/0.0.1 MyDataSource/v1.0.0 clickhouse-rs/0.12.2 (lv:rust/1.67.0, os:macos)
    /// ```
    pub fn with_product_info(
        mut self,
        product_name: impl Into<String>,
        product_version: impl Into<String>,
    ) -> Self {
        self.products_info.push(ProductInfo {
            name: product_name.into(),
            version: product_version.into(),
        });
        self
    }

    /// Set a setting on this instance of [`Client`].
    ///
    /// Returns the previous value for the setting, if one was set.
    #[deprecated(since = "0.14.3", note = "please use `set_setting` instead")]
    pub fn set_option(
        &mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Option<String> {
        self.settings.insert(name.into(), value.into())
    }

    /// Set a setting on this instance of [`Client`].
    ///
    /// Returns the previous value for the setting, if one was set.
    pub fn set_setting(
        &mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Option<String> {
        self.settings.insert(name.into(), value.into())
    }

    /// Get a setting that was previously set on this `Client`.
    #[deprecated(since = "0.14.3", note = "please use `get_setting` instead")]
    pub fn get_option(&self, name: impl AsRef<str>) -> Option<&str> {
        self.settings.get(name.as_ref()).map(String::as_str)
    }

    /// Get a setting that was previously set on this `Client`.
    pub fn get_setting(&self, name: impl AsRef<str>) -> Option<&str> {
        self.settings.get(name.as_ref()).map(String::as_str)
    }

    /// Starts a new INSERT statement.
    ///
    /// The table name will be escaped as a single identifier. To pass a fully qualified name,
    /// use [`Client::insert_unescaped()`] instead, or override the database name for this statement
    /// using `client.clone().with_database("<db name>")`.
    ///
    /// # Validation
    ///
    /// If validation is enabled (default), `RowBinaryWithNamesAndTypes` input format is used.
    /// When [`Client::insert`] method is called for this `table` for the first time,
    /// it will fetch the table schema from the server, allowing to validate the serialized rows,
    /// as well as write the names and types of the columns in the request header.
    ///
    /// Fetching the schema will happen only once per `table`,
    /// as the schema is cached by the client internally.
    ///
    /// With disabled validation, the schema is not fetched,
    /// and the rows serialized with `RowBinary` input format.
    ///
    /// # Panics
    ///
    /// If `T` has unnamed fields, e.g. tuples.
    pub async fn insert<T: Row>(&self, table: &str) -> Result<insert::Insert<T>> {
        let mut escaped_table_name = String::new();
        sql::escape::identifier(table, &mut escaped_table_name)
            // In practice this should not error, as writing to a `String` should be infallible.
            .map_err(|e| Error::Other(format!("error escaping table name: {e:?}").into()))?;

        self.insert_unescaped(&escaped_table_name).await
    }

    /// Start a new `INSERT` statement using an unescaped table name.
    ///
    /// See [`Client::insert()`] for details.
    pub async fn insert_unescaped<T: Row>(
        &self,
        raw_table_name: &str,
    ) -> Result<insert::Insert<T>> {
        if self.get_validation() {
            let metadata = self.get_insert_metadata(raw_table_name).await?;
            let row = metadata.to_row::<T>()?;
            return Ok(insert::Insert::new(self, raw_table_name, Some(row)));
        }
        Ok(insert::Insert::new(self, raw_table_name, None))
    }

    /// Creates an inserter to perform multiple INSERT statements.
    #[cfg(feature = "inserter")]
    pub fn inserter<T: Row>(&self, table: &str) -> inserter::Inserter<T> {
        inserter::Inserter::new(self, table)
    }

    /// Start a new `INSERT` statement that will ship rows in the
    /// ClickHouse `Native` (columnar) format over HTTP.
    ///
    /// Like [`Client::insert`], this resolves the column schema from
    /// the server (one DESCRIBE TABLE per call, cached). The
    /// returned [`InsertNative<T>`][insert_native::InsertNative]
    /// chunks the input into Native blocks bounded by row count
    /// (default 100k rows) and serialised byte size (default
    /// 10 MiB); both ceilings can be overridden via
    /// [`with_max_rows_per_block`][insert_native::InsertNative::with_max_rows_per_block]
    /// and
    /// [`with_max_bytes_per_block`][insert_native::InsertNative::with_max_bytes_per_block].
    /// `end()` ships the trailing partial block.
    pub async fn insert_native<T: Row>(
        &self,
        table: &str,
    ) -> Result<insert_native::InsertNative<T>> {
        let mut escaped_table_name = String::new();
        sql::escape::identifier(table, &mut escaped_table_name)
            .map_err(|e| Error::Other(format!("error escaping table name: {e:?}").into()))?;

        let metadata = self.get_insert_metadata(&escaped_table_name).await?;
        let row = metadata.to_row::<T>()?;
        insert_native::InsertNative::new(self, &escaped_table_name, row)
    }

    /// Start an `INSERT` statement sending pre-formatted data.
    ///
    /// `sql` should be an `INSERT INTO ... FORMAT <format name>` statement.
    /// Any other type of statement may produce incorrect results.
    ///
    /// The statement is not issued until the first call to
    /// [`.send()`][insert_formatted::InsertFormatted::send].
    ///
    /// # Note: Not Validated
    /// Unlike [`Insert`][insert::Insert] and [`Inserter`][inserter::Inserter],
    /// this does not perform any validation on the submitted data.
    ///
    /// Only the use of self-describing formats (e.g. CSV, TabSeparated, JSON) is recommended.
    ///
    /// See the [list of supported formats](https://clickhouse.com/docs/interfaces/formats)
    /// for details.
    pub fn insert_formatted_with(
        &self,
        sql: impl Into<String>,
    ) -> insert_formatted::InsertFormatted {
        // TODO: extract collection name from query
        insert_formatted::InsertFormatted::new(self, sql.into(), None)
    }

    /// Starts a new SELECT/DDL query.
    ///
    /// If [`with_auto_query_id`][Self::with_auto_query_id] was
    /// called, AND no `query_id` is set on the Client, the returned
    /// `Query` gets a freshly-generated UUIDv7 as its `query_id`.
    /// Callers can override with
    /// [`Query::with_query_id`][crate::query::Query::with_query_id].
    pub fn query(&self, query: &str) -> query::Query {
        let q = query::Query::new(self, query);
        #[cfg(feature = "uuid")]
        if self.auto_query_id && !self.settings.contains_key(settings::QUERY_ID) {
            return q.with_query_id(uuid::Uuid::now_v7().to_string());
        }
        q
    }

    /// Enable Drop-on-cursor `KILL QUERY`. When a `RowCursor` is
    /// dropped BEFORE the body stream finishes (consumer abandoned
    /// before draining), AND its query had a `query_id` set, a
    /// background task issues `KILL QUERY WHERE query_id = ? SYNC`.
    /// Recovers server-side resources that would otherwise tie up
    /// CPU + RAM until the next socket write fails.
    ///
    /// Pairs with [`with_auto_query_id`][Self::with_auto_query_id]
    /// for the "every query is auto-cancellable on drop" pattern.
    /// Off by default; opt in.
    pub fn with_kill_on_drop(mut self) -> Self {
        self.kill_on_drop = true;
        self
    }

    /// Read-side accessor for Query::fetch to construct a
    /// KillOnDropHandle. Returns the Client clone + query_id only
    /// when `kill_on_drop` is enabled AND `query_id` is set.
    pub(crate) fn kill_on_drop_handle(&self) -> Option<(Self, String)> {
        if !self.kill_on_drop {
            return None;
        }
        let qid = self.settings.get(settings::QUERY_ID).cloned()?;
        Some((self.clone(), qid))
    }

    /// Auto-generate a UUIDv7 `query_id` for every query that
    /// doesn't already have one set.
    ///
    /// Pairs with [`kill_query`][Self::kill_query] for safe
    /// consumer cancellation -- without an id, there's no way to
    /// reach a running query. UUIDv7 is k-sortable: the timestamp
    /// prefix means query ids cluster by time in the server's
    /// `system.query_log`, easier to grep by submission window.
    ///
    /// Off by default (zero overhead for callers who don't need it).
    /// Available only with the `uuid` feature.
    #[cfg(feature = "uuid")]
    pub fn with_auto_query_id(mut self) -> Self {
        self.auto_query_id = true;
        self
    }

    /// Send `KILL QUERY WHERE query_id = ? SYNC` to cancel a running
    /// query. `SYNC` waits for the server to acknowledge cancellation
    /// (vs the default async, which returns immediately).
    ///
    /// Pair with [`Query::with_query_id`][crate::query::Query::with_query_id]
    /// for the consumer pattern: register an id at query start, call
    /// this on the same `Client` if the consumer abandons the result
    /// cursor before draining. Without it, the server keeps
    /// processing until it tries to write to a dead socket; for long
    /// queries that's wasted server resources and held-open server-
    /// side state.
    ///
    /// Returns `Ok(())` even if no query matches the id (the KILL
    /// statement is a no-op in that case). `Err` only on transport
    /// failure or insufficient privileges.
    pub async fn kill_query(&self, query_id: &str) -> Result<()> {
        self.query("KILL QUERY WHERE query_id = ? SYNC")
            .bind(query_id)
            .execute()
            .await
    }

    /// Look up the storage engine of `table` from `system.tables`.
    ///
    /// `table` may be qualified (`"db.name"`) or unqualified
    /// (`"name"`). Unqualified names resolve against the client's
    /// configured database (set via
    /// [`with_database`][Self::with_database]); if none is set,
    /// falls back to ClickHouse's `"default"`.
    ///
    /// One round-trip per call. No caching -- callers driving high-
    /// frequency lookups should memoise themselves.
    ///
    /// # Errors
    ///
    /// `Err` if `system.tables` is inaccessible (rare; typically a
    /// permissions issue) or the table doesn't exist.
    pub async fn table_engine(&self, table: &str) -> Result<String> {
        let (database, name) = if let Some((d, n)) = table.split_once('.') {
            (d.to_string(), n.to_string())
        } else if let Some(d) = &self.database {
            (d.clone(), table.to_string())
        } else {
            ("default".to_string(), table.to_string())
        };
        self.query(
            "SELECT engine FROM system.tables WHERE database = ? AND name = ? LIMIT 1",
        )
        .bind(database)
        .bind(name)
        .fetch_one::<String>()
        .await
    }

    /// Set the durability mode for INSERTs into Distributed-engine
    /// tables. See [`Durability`] for the per-mode setting map and
    /// the throughput vs durability tradeoff.
    ///
    /// No effect on non-Distributed tables. Maps to ClickHouse
    /// session settings via [`with_setting`][Self::with_setting];
    /// equivalent to setting the underlying settings directly.
    pub fn with_durability(self, mode: Durability) -> Self {
        match mode {
            Durability::Background => self, // CH defaults
            Durability::Foreground => {
                self.with_setting("distributed_foreground_insert", "1")
            }
            Durability::ForegroundFsynced => self
                .with_setting("distributed_foreground_insert", "1")
                .with_setting("fsync_after_insert", "1")
                .with_setting("fsync_directories", "1"),
        }
    }

    /// Health check: run `SELECT 1` against the server.
    ///
    /// Verifies that the configured URL is reachable, TLS handshake
    /// works (if applicable), credentials are accepted, and the
    /// server is processing queries. Returns `Ok(())` on success.
    ///
    /// One round-trip per call. Consumers driving high-frequency
    /// health checks should cap their interval; the CH server's
    /// dedicated `GET /ping` endpoint is lighter but we don't go
    /// through that path -- `SELECT 1` exercises the full query
    /// pipeline.
    pub async fn ping(&self) -> Result<()> {
        self.query("SELECT 1").execute().await
    }


    /// `true` if `table` is backed by the `Distributed` engine.
    ///
    /// Use this BEFORE trusting row positions in
    /// [`crate::recovery::FailureLocation`] --
    /// Distributed tables fan out to shards and the server-reported
    /// row index is shard-local, NOT coordinator-batch-local, so it
    /// won't map back to the client batch position. See
    /// [`crate::recovery::failing_row_from_error`]
    /// for the caveat.
    ///
    /// Thin wrapper over [`table_engine`][Self::table_engine].
    ///
    /// # Errors
    ///
    /// Same as [`table_engine`][Self::table_engine].
    pub async fn is_distributed_table(&self, table: &str) -> Result<bool> {
        let engine = self.table_engine(table).await?;
        Ok(engine == "Distributed")
    }

    /// Enables or disables [`Row`] data types validation against the database schema
    /// at the cost of performance. Validation is enabled by default, and in this mode,
    /// the client will use `RowBinaryWithNamesAndTypes` format.
    ///
    /// If you are looking to maximize performance, you could disable validation using this method.
    /// When validation is disabled, the client switches to `RowBinary` format usage instead.
    ///
    /// The downside with plain `RowBinary` is that instead of clearer error messages,
    /// a mismatch between [`Row`] and database schema will result
    /// in a [`error::Error::NotEnoughData`] error without specific details.
    ///
    /// However, depending on the dataset, there might be x1.1 to x3 performance improvement,
    /// but that highly depends on the shape and volume of the dataset.
    ///
    /// It is always recommended to measure the performance impact of validation
    /// in your specific use case. Additionally, writing smoke tests to ensure that
    /// the row types match the ClickHouse schema is highly recommended,
    /// if you plan to disable validation in your application.
    ///
    /// # Note: Mocking
    /// When using [`test::Mock`] with the `test-util` feature, validation is forced off.
    ///
    /// This applies either when using [`Client::with_mock()`], or [`Client::with_url()`]
    /// with a URL from [`test::Mock::url()`].
    ///
    /// As of writing, the mocking facilities are unable to generate the `RowBinaryWithNamesAndTypes`
    /// header required for validation to function.
    pub fn with_validation(mut self, enabled: bool) -> Self {
        self.validation = enabled;
        self
    }

    /// Register a callback for `X-ClickHouse-Progress` headers.
    /// Requires `send_progress_in_http_headers=1` on the session
    /// (not auto-set; pair with [`with_setting`][Self::with_setting]).
    /// See [`crate::progress`] for the hyper limitation.
    /// Synchronous -- do not block.
    ///
    /// ```no_run
    /// # use clickhouse::Client;
    /// let client = Client::default()
    ///     .with_url("http://localhost:8123")
    ///     .with_setting("send_progress_in_http_headers", "1")
    ///     .with_progress_callback(|p| {
    ///         tracing::info!(
    ///             read_rows = p.read_rows,
    ///             total = p.total_rows_to_read,
    ///             "progress"
    ///         );
    ///     });
    /// ```
    pub fn with_progress_callback(
        mut self,
        callback: impl Fn(&progress::Progress) + Send + Sync + 'static,
    ) -> Self {
        self.progress_callback = Some(std::sync::Arc::new(callback));
        self
    }

    pub(crate) fn progress_callback(&self) -> Option<progress::ProgressCallback> {
        self.progress_callback.clone()
    }

    /// Clear table metadata that was previously received and cached.
    ///
    /// [`Insert`][crate::insert::Insert] uses cached metadata when sending data with validation.
    /// If the table schema changes, this metadata needs to re-fetched.
    ///
    /// This method clears the metadata cache, causing future insert queries to re-fetch metadata.
    /// This applies to all cloned instances of this `Client` (using the same URL and database)
    /// as well.
    ///
    /// This may need to wait to acquire a lock if a query is concurrently writing into the cache.
    ///
    /// Cancel-safe.
    pub async fn clear_cached_metadata(&self) {
        self.insert_metadata_cache.0.write().await.clear();
    }

    /// Used internally to check if the validation mode is enabled,
    /// as it takes into account the `test-util` feature flag.
    #[inline]
    pub(crate) fn get_validation(&self) -> bool {
        #[cfg(feature = "test-util")]
        if self.mocked {
            return false;
        }
        self.validation
    }

    pub(crate) fn set_roles(&mut self, roles: impl IntoIterator<Item = impl Into<String>>) {
        self.clear_roles();
        self.roles.extend(roles.into_iter().map(Into::into));
    }

    #[inline]
    pub(crate) fn clear_roles(&mut self) {
        // Make sure we overwrite any role manually set by the user via `with_setting()`.
        self.settings.remove(settings::ROLE);
        self.roles.clear();
    }

    /// Use a mock server for testing purposes.
    ///
    /// # Note
    ///
    /// The client will always use `RowBinary` format instead of `RowBinaryWithNamesAndTypes`,
    /// as otherwise it'd be required to provide RBWNAT header in the mocks,
    /// which is pointless in that kind of tests.
    #[cfg(feature = "test-util")]
    pub fn with_mock(mut self, mock: &test::Mock) -> Self {
        self.url = mock.real_url().to_string();
        self.mocked = true;
        self
    }

    async fn get_insert_metadata(&self, raw_table_name: &str) -> Result<Arc<InsertMetadata>> {
        #[derive(::serde::Deserialize, clickhouse_macros::Row)]
        #[clickhouse(crate = "self")]
        // `Row` derive doesn't allow omitting columns
        #[expect(dead_code)]
        struct DescribeColumn {
            name: String,
            r#type: String,
            default_type: String,
            default_expression: String,
            comment: String,
            codec_expression: String,
            ttl_expression: String,
        }

        {
            let read_lock = self.insert_metadata_cache.0.read().await;

            if let Some(metadata) = read_lock.get(raw_table_name) {
                return Ok(metadata.clone());
            }
        }

        // TODO: should it be moved to a cold function?
        let mut write_lock = self.insert_metadata_cache.0.write().await;

        let mut columns_cursor = self
            .query(&_priv::row_insert_metadata_query(raw_table_name))
            .with_setting("describe_include_subcolumns", "0")
            .fetch::<DescribeColumn>()?;

        let mut columns = Vec::new();
        let mut column_default_kinds = Vec::new();
        let mut column_lookup = HashMap::new();

        while let Some(column) = columns_cursor.next().await? {
            let data_type = DataTypeNode::new(&column.r#type)?;
            let default_kind = column.default_type.parse::<ColumnDefaultKind>()?;

            column_lookup.insert(column.name.clone(), columns.len());

            columns.push(Column {
                name: column.name,
                data_type,
            });

            column_default_kinds.push(default_kind);
        }

        let metadata = Arc::new(InsertMetadata {
            row_metadata: RowMetadata {
                columns,
                access_type: AccessType::WithSeqAccess, // ignored on insert
            },
            column_default_kinds,
            column_lookup,
        });

        write_lock.insert(raw_table_name.to_string(), metadata.clone());
        Ok(metadata)
    }
}

mod formats {
    pub(crate) const ROW_BINARY: &str = "RowBinary";
    pub(crate) const ROW_BINARY_WITH_NAMES_AND_TYPES: &str = "RowBinaryWithNamesAndTypes";
    /// ClickHouse Native columnar block format
    /// (<https://clickhouse.com/docs/interfaces/formats#native>).
    /// Used by [`insert::Insert::with_native_format`].
    pub(crate) const NATIVE: &str = "Native";
}

mod settings {
    pub(crate) const DATABASE: &str = "database";
    pub(crate) const DEFAULT_FORMAT: &str = "default_format";
    pub(crate) const COMPRESS: &str = "compress";
    pub(crate) const DECOMPRESS: &str = "decompress";
    #[cfg(feature = "zstd")]
    pub(crate) const ENABLE_HTTP_COMPRESSION: &str = "enable_http_compression";
    pub(crate) const ROLE: &str = "role";
    pub(crate) const QUERY: &str = "query";
    pub(crate) const QUERY_ID: &str = "query_id";
    pub(crate) const SESSION_ID: &str = "session_id";
}

/// This is a private API exported only for internal purposes.
/// Do not use it in your code directly, it doesn't follow semver.
#[doc(hidden)]
pub mod _priv {
    pub use crate::row::RowKind;

    #[cfg(feature = "lz4")]
    pub fn lz4_compress(uncompressed: &[u8]) -> super::Result<bytes::Bytes> {
        crate::compression::lz4::compress(uncompressed)
    }

    #[cfg(feature = "zstd")]
    pub fn zstd_compress(uncompressed: &[u8]) -> super::Result<bytes::Bytes> {
        crate::compression::zstd::compress(uncompressed, None)
    }

    // Also needed by `it::insert::cache_row_metadata()`
    pub fn row_insert_metadata_query(raw_table: &str) -> String {
        format!("DESCRIBE TABLE {raw_table}")
    }
}

#[cfg(test)]
mod client_tests {
    use crate::_priv::RowKind;
    use crate::row_metadata::{AccessType, RowMetadata};
    use crate::{Authentication, Client, Row};
    use clickhouse_types::{Column, DataTypeNode};

    #[test]
    fn it_can_use_credentials_auth() {
        assert_eq!(
            Client::default()
                .with_user("bob")
                .with_password("secret")
                .authentication,
            Authentication::Credentials {
                user: Some("bob".into()),
                password: Some("secret".into()),
            }
        );
    }

    #[test]
    fn it_can_use_credentials_auth_user_only() {
        assert_eq!(
            Client::default().with_user("alice").authentication,
            Authentication::Credentials {
                user: Some("alice".into()),
                password: None,
            }
        );
    }

    #[test]
    fn it_can_use_credentials_auth_password_only() {
        assert_eq!(
            Client::default().with_password("secret").authentication,
            Authentication::Credentials {
                user: None,
                password: Some("secret".into()),
            }
        );
    }

    #[test]
    fn it_can_override_credentials_auth() {
        assert_eq!(
            Client::default()
                .with_user("bob")
                .with_password("secret")
                .with_user("alice")
                .with_password("something_else")
                .authentication,
            Authentication::Credentials {
                user: Some("alice".into()),
                password: Some("something_else".into()),
            }
        );
    }

    #[test]
    fn it_can_use_jwt_auth() {
        assert_eq!(
            Client::default().with_access_token("my_jwt").authentication,
            Authentication::Jwt {
                access_token: "my_jwt".into(),
            }
        );
    }

    #[test]
    fn it_can_override_jwt_auth() {
        assert_eq!(
            Client::default()
                .with_access_token("my_jwt")
                .with_access_token("my_jwt_2")
                .authentication,
            Authentication::Jwt {
                access_token: "my_jwt_2".into(),
            }
        );
    }

    #[test]
    #[should_panic(expected = "`access_token` cannot be set together with `user` or `password`")]
    fn it_cannot_use_jwt_after_with_user() {
        let _ = Client::default()
            .with_user("bob")
            .with_access_token("my_jwt");
    }

    #[test]
    #[should_panic(expected = "`access_token` cannot be set together with `user` or `password`")]
    fn it_cannot_use_jwt_after_with_password() {
        let _ = Client::default()
            .with_password("secret")
            .with_access_token("my_jwt");
    }

    #[test]
    #[should_panic(expected = "`access_token` cannot be set together with `user` or `password`")]
    fn it_cannot_use_jwt_after_both_with_user_and_with_password() {
        let _ = Client::default()
            .with_user("alice")
            .with_password("secret")
            .with_access_token("my_jwt");
    }

    #[test]
    #[should_panic(expected = "`user` cannot be set together with `access_token`")]
    fn it_cannot_use_with_user_after_jwt() {
        let _ = Client::default()
            .with_access_token("my_jwt")
            .with_user("alice");
    }

    #[test]
    #[should_panic(expected = "`password` cannot be set together with `access_token`")]
    fn it_cannot_use_with_password_after_jwt() {
        let _ = Client::default()
            .with_access_token("my_jwt")
            .with_password("secret");
    }

    #[test]
    fn it_sets_validation_mode() {
        let client = Client::default();
        assert!(client.validation);
        let client = client.with_validation(false);
        assert!(!client.validation);
        let client = client.with_validation(true);
        assert!(client.validation);
    }

    #[derive(Debug, Clone, PartialEq)]
    struct SystemRolesRow {
        name: String,
        id: uuid::Uuid,
        storage: String,
    }

    impl SystemRolesRow {
        fn columns() -> Vec<Column> {
            vec![
                Column::new("name".to_string(), DataTypeNode::String),
                Column::new("id".to_string(), DataTypeNode::UUID),
                Column::new("storage".to_string(), DataTypeNode::String),
            ]
        }
    }

    impl Row for SystemRolesRow {
        const NAME: &'static str = "SystemRolesRow";
        const KIND: RowKind = RowKind::Struct;
        const COLUMN_COUNT: usize = 3;
        const COLUMN_NAMES: &'static [&'static str] = &["name", "id", "storage"];
        type Value<'a> = SystemRolesRow;
    }

    #[test]
    fn get_row_metadata() {
        let metadata =
            RowMetadata::new_for_cursor::<SystemRolesRow>(SystemRolesRow::columns()).unwrap();
        assert_eq!(metadata.columns, SystemRolesRow::columns());
        assert_eq!(metadata.access_type, AccessType::WithSeqAccess);

        // the order is shuffled => map access
        let columns = vec![
            Column::new("id".to_string(), DataTypeNode::UUID),
            Column::new("storage".to_string(), DataTypeNode::String),
            Column::new("name".to_string(), DataTypeNode::String),
        ];
        let metadata = RowMetadata::new_for_cursor::<SystemRolesRow>(columns.clone()).unwrap();
        assert_eq!(metadata.columns, columns);
        assert_eq!(
            metadata.access_type,
            AccessType::WithMapAccess(vec![1, 2, 0]) // see COLUMN_NAMES above
        );
    }

    #[test]
    fn it_does_follow_previous_configuration() {
        let client = Client::default().with_setting("async_insert", "1");
        assert_eq!(client.settings, client.clone().settings,);
    }

    #[test]
    fn it_does_not_follow_future_configuration() {
        let client = Client::default();
        let client_clone = client.clone();
        let client = client.with_setting("async_insert", "1");
        assert_ne!(client.settings, client_clone.settings,);
    }

    #[test]
    fn it_gets_and_sets_settings() {
        let mut client = Client::default();

        assert_eq!(client.set_setting("foo", "foo"), None);
        assert_eq!(client.set_setting("bar", "bar"), None);

        assert_eq!(client.get_setting("foo"), Some("foo"));
        assert_eq!(client.get_setting("bar"), Some("bar"));
        assert_eq!(client.get_setting("baz"), None);

        assert_eq!(client.set_setting("foo", "foo_2"), Some("foo".to_string()));
        assert_eq!(client.set_setting("bar", "bar_2"), Some("bar".to_string()));
    }
}

#[cfg(all(test, any(feature = "rustls-tls-aws-lc", feature = "rustls-tls-ring")))]
mod tls_builder_tests {
    use super::*;

    #[test]
    fn default_client_has_no_tls_source() {
        let c = Client::default();
        assert!(c.tls_resolved.is_none());
    }

    #[test]
    fn native_roots_toggle_sets_resolved() {
        let c = Client::default().with_tls_native_roots(true);
        assert!(c.tls_resolved.is_some(), "toggle must resolve a config");
    }

    #[test]
    fn root_ca_missing_path_errors() {
        let r = Client::default().try_with_tls_root_ca("/no/such/ca.pem");
        let err = match r {
            Ok(_) => panic!("missing CA path must error"),
            Err(e) => e,
        };
        assert!(format!("{err}").contains("cannot read CA file"));
    }

    #[test]
    fn exclusive_without_ca_records_intent_but_no_resolved() {
        // Exclusive trust with no explicit CA cannot resolve a config.
        // Fail-closed: tls_resolved stays None, but tls_source records the
        // configured intent so transports refuse to fall back to broad
        // default trust.
        let c = Client::default().with_tls_roots_exclusive();
        assert!(
            c.tls_resolved.is_none(),
            "exclusive trust with no CA must not resolve a config"
        );
        assert!(
            c.tls_source.is_some(),
            "configured-trust intent must be recorded even when resolve fails"
        );
    }

    // Fail-closed must survive a NON-TLS pool rebuild. Before the
    // shared `http_tls_config` helper, `with_pool_idle_timeout` (and
    // the other `with_pool_*` setters) passed `tls_resolved` raw, so
    // the (Some, None) state reverted to the default webpki path
    // (fail-open). The helper substitutes an empty-roots config for
    // EVERY http rebuild; assert it returns Some for that state.
    #[cfg(not(feature = "native-tls"))]
    #[test]
    fn failclosed_survives_non_tls_pool_rebuild() {
        use std::time::Duration;
        let c = Client::default()
            .with_tls_roots_exclusive()
            .with_pool_idle_timeout(Duration::from_secs(1));
        assert!(
            c.tls_source.is_some(),
            "configured-trust intent must survive a pool rebuild"
        );
        assert!(
            c.tls_resolved.is_none(),
            "exclusive trust with no CA stays unresolved"
        );
        assert!(
            c.http_tls_config().is_some(),
            "fail-closed: (Some, None) must yield an empty-roots config, \
             never the default webpki path"
        );
    }
}
