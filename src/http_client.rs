use std::time::Duration;

use hyper::Request;
use hyper_util::{
    client::legacy::{
        Client, Client as HyperClient, ResponseFuture,
        connect::{Connect, HttpConnector},
    },
    rt::TokioExecutor,
};

use crate::request_body::RequestBody;

/// A trait for underlying HTTP client.
///
/// Firstly, now it is implemented only for
/// `hyper_util::client::legacy::Client`, it's impossible to use another HTTP
/// client.
///
/// Secondly, although it's stable in terms of semver, it will be changed in the
/// future (e.g. to support more runtimes, not only tokio). Thus, prefer to open
/// a feature request instead of implementing this trait manually.
pub trait HttpClient: sealed::Sealed + Send + Sync + 'static {
    fn request(&self, req: Request<RequestBody>) -> ResponseFuture;
}

impl<C> HttpClient for Client<C, RequestBody>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    fn request(&self, req: Request<RequestBody>) -> ResponseFuture {
        self.request(req)
    }
}

impl<C> sealed::Sealed for Client<C, RequestBody> {}

// === Default ===

const TCP_KEEPALIVE: Duration = Duration::from_secs(60);

// ClickHouse uses 3s by default.
// See https://github.com/ClickHouse/ClickHouse/blob/368cb74b4d222dc5472a7f2177f6bb154ebae07a/programs/server/config.xml#L201
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

/// Connection-pool configuration for the default HTTP client.
/// Set on `Client` via [`Client::with_pool_idle_timeout`][crate::Client::with_pool_idle_timeout]
/// and [`Client::with_pool_max_idle_per_host`][crate::Client::with_pool_max_idle_per_host];
/// stored on the Client and applied when the http_client is
/// (re)built.
#[derive(Debug, Clone)]
pub(crate) struct PoolConfig {
    pub idle_timeout: Duration,
    /// `None` means hyper's default (currently unbounded, but
    /// callers should set this in production -- Go uses 5).
    pub max_idle_per_host: Option<usize>,
    pub tcp_keepalive: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            idle_timeout: POOL_IDLE_TIMEOUT,
            max_idle_per_host: None,
            tcp_keepalive: TCP_KEEPALIVE,
        }
    }
}

pub(crate) fn default() -> impl HttpClient {
    with_pool_config(PoolConfig::default())
}

pub(crate) fn with_pool_config(config: PoolConfig) -> impl HttpClient {
    let mut connector = HttpConnector::new();

    connector.set_keepalive(Some(config.tcp_keepalive));

    connector.enforce_http(!cfg!(any(
        feature = "native-tls",
        feature = "rustls-tls-aws-lc",
        feature = "rustls-tls-ring",
    )));

    #[cfg(feature = "native-tls")]
    let connector = hyper_tls::HttpsConnector::new_with_connector(connector);

    #[cfg(all(feature = "rustls-tls-aws-lc", not(feature = "native-tls")))]
    let connector =
        prepare_hyper_rustls_connector(connector, rustls::crypto::aws_lc_rs::default_provider());

    #[cfg(all(
        feature = "rustls-tls-ring",
        not(feature = "rustls-tls-aws-lc"),
        not(feature = "native-tls"),
    ))]
    let connector =
        prepare_hyper_rustls_connector(connector, rustls::crypto::ring::default_provider());

    let mut builder = HyperClient::builder(TokioExecutor::new());
    builder.pool_idle_timeout(config.idle_timeout);
    if let Some(n) = config.max_idle_per_host {
        builder.pool_max_idle_per_host(n);
    }
    builder.build(connector)
}

#[cfg(not(feature = "native-tls"))]
#[cfg(any(feature = "rustls-tls-aws-lc", feature = "rustls-tls-ring"))]
fn prepare_hyper_rustls_connector(
    connector: HttpConnector,
    provider: rustls::crypto::CryptoProvider,
) -> hyper_rustls::HttpsConnector<HttpConnector> {
    #[cfg(not(feature = "rustls-tls-webpki-roots"))]
    #[cfg(not(feature = "rustls-tls-native-roots"))]
    compile_error!(
        "`rustls-tls-aws-lc` and `rustls-tls-ring` features require either \
         `rustls-tls-webpki-roots` or `rustls-tls-native-roots` feature to be enabled"
    );

    #[cfg(feature = "rustls-tls-native-roots")]
    let builder = hyper_rustls::HttpsConnectorBuilder::new()
        .with_provider_and_native_roots(provider)
        .unwrap();

    #[cfg(all(
        feature = "rustls-tls-webpki-roots",
        not(feature = "rustls-tls-native-roots")
    ))]
    let builder = hyper_rustls::HttpsConnectorBuilder::new()
        .with_provider_and_webpki_roots(provider)
        .unwrap();

    builder
        .https_or_http()
        .enable_http1()
        .wrap_connector(connector)
}

mod sealed {
    pub trait Sealed {}
}
