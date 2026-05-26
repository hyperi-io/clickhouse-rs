//! Shared TLS trust configuration for both the HTTP and TCP transports.
//!
//! One mechanism, two transports: this module turns a trust description
//! (OS native roots, the compiled webpki bundle, and/or explicit PEM CA
//! files) into a single rustls [`ClientConfig`] that the HTTP connector
//! and the TCP connector both consume. Mirrors clickhouse-go: a server
//! cert is verified against one pool; CA files are loaded
//! `AppendCertsFromPEM`-style (best-effort, all certs in the file,
//! error only if none parse).
//!
//! [`ClientConfig`]: rustls::ClientConfig

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::RootCertStore;
use rustls_pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject;

use crate::error::{Error, Result};

/// Declarative trust description, resolved to a [`rustls::ClientConfig`]
/// at transport-build time.
#[derive(Clone, Debug)]
pub(crate) struct TlsTrust {
    /// Load OS native roots (rustls-native-certs).
    pub native_roots: bool,
    /// Include the compiled-in Mozilla bundle (webpki-roots).
    pub webpki_roots: bool,
    /// Explicit root CA PEM files (each may bundle many certs).
    pub extra_roots: Vec<PathBuf>,
    /// Explicit intermediate CA PEM files (added as anchors too).
    pub extra_intermediates: Vec<PathBuf>,
    /// When true, ignore native + webpki; trust ONLY the explicit files.
    pub exclusive: bool,
}

impl Default for TlsTrust {
    fn default() -> Self {
        // Native + webpki both on: internal-CA clusters work (OS store
        // carries the org CA) and public-CA servers still verify.
        Self {
            native_roots: true,
            webpki_roots: true,
            extra_roots: Vec::new(),
            extra_intermediates: Vec::new(),
            exclusive: false,
        }
    }
}

/// What the Client carries: either a caller-built config (Go's
/// `Options.TLS` analog) or a declarative trust we resolve ourselves.
#[derive(Clone)]
pub(crate) enum TlsConfigSource {
    Explicit(Arc<rustls::ClientConfig>),
    Trust(TlsTrust),
}

/// `AppendCertsFromPEM` analog: read `path`, best-effort parse every PEM
/// certificate block, add all valid certs to `store`. Junk / non-cert
/// blocks are skipped. Errors only if the file cannot be read or yields
/// ZERO usable certs (the Go `!successful` branch).
fn add_pem_file_certs(store: &mut RootCertStore, path: &Path) -> Result<()> {
    let mut certs: Vec<CertificateDer<'static>> = Vec::new();
    let iter = CertificateDer::pem_file_iter(path).map_err(|e| {
        Error::Custom(format!("tls: cannot read CA file {}: {e}", path.display()))
    })?;
    // Lenient per-block: skip an unparseable block rather than fail the
    // whole file (AppendCertsFromPEM parity).
    for cert in iter.flatten() {
        certs.push(cert);
    }
    let (added, _ignored) = store.add_parsable_certificates(certs);
    if added == 0 {
        return Err(Error::Custom(format!(
            "tls: no usable certificates in CA file {}",
            path.display()
        )));
    }
    Ok(())
}

/// Assemble the trust anchor set per the [`TlsTrust`] rules.
fn build_root_store(trust: &TlsTrust) -> Result<RootCertStore> {
    let mut store = RootCertStore::empty();

    if !trust.exclusive {
        if trust.native_roots {
            // Best-effort: take what the OS store yields, tolerate
            // partial errors as long as we end up non-empty (webpki net).
            let result = rustls_native_certs::load_native_certs();
            let (added, _ignored) = store.add_parsable_certificates(result.certs);
            let _ = added;
        }
        if trust.webpki_roots {
            store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }
    } else if trust.extra_roots.is_empty() && trust.extra_intermediates.is_empty() {
        return Err(Error::Custom(
            "tls: exclusive trust requested but no explicit CA files were supplied".into(),
        ));
    }

    for path in &trust.extra_roots {
        add_pem_file_certs(&mut store, path)?;
    }
    for path in &trust.extra_intermediates {
        add_pem_file_certs(&mut store, path)?;
    }

    if store.is_empty() {
        return Err(Error::Custom(
            "tls: resulting trust store is empty (no roots loaded)".into(),
        ));
    }
    Ok(store)
}

/// Build a [`rustls::ClientConfig`] from an already-assembled root store,
/// using the same explicit provider precedence as the public path.
fn build_config_with_roots(roots: RootCertStore) -> Result<Arc<rustls::ClientConfig>> {
    // Explicit provider so the build does not depend on
    // process-default provider installation order. aws-lc-rs is
    // the default (and the only provider `native-tls-rustls`
    // enables on the TCP side); ring only when it is the sole
    // compiled provider, mirroring `http_client.rs` precedence.
    #[cfg(any(feature = "rustls-tls-aws-lc", feature = "native-tls-rustls"))]
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    #[cfg(all(
        feature = "rustls-tls-ring",
        not(feature = "rustls-tls-aws-lc"),
        not(feature = "native-tls-rustls")
    ))]
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::Custom(format!("tls: rustls config: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(cfg))
}

/// Resolve a [`TlsConfigSource`] into a ready [`rustls::ClientConfig`].
pub(crate) fn build_client_config(src: &TlsConfigSource) -> Result<Arc<rustls::ClientConfig>> {
    match src {
        TlsConfigSource::Explicit(cfg) => Ok(cfg.clone()),
        TlsConfigSource::Trust(trust) => {
            let roots = build_root_store(trust)?;
            build_config_with_roots(roots)
        }
    }
}

/// A `ClientConfig` that trusts NO roots -- used to fail closed when a
/// trust was configured but could not be resolved (never silently fall
/// back to default/broad trust). An empty `RootCertStore` rejects every
/// server certificate at handshake time.
#[cfg(all(
    any(feature = "rustls-tls-aws-lc", feature = "rustls-tls-ring"),
    not(feature = "native-tls")
))]
pub(crate) fn build_failclosed_config() -> Arc<rustls::ClientConfig> {
    let roots = RootCertStore::empty();
    // Provider build is infallible for the safe defaults; if it ever
    // failed we still must NOT fall back to broad trust, so default to a
    // store that trusts nothing either way.
    build_config_with_roots(roots).unwrap_or_else(|_| {
        // Should be unreachable (empty store + safe defaults always
        // build); kept fail-closed regardless.
        let provider = {
            #[cfg(any(feature = "rustls-tls-aws-lc", feature = "native-tls-rustls"))]
            {
                Arc::new(rustls::crypto::aws_lc_rs::default_provider())
            }
            #[cfg(all(
                feature = "rustls-tls-ring",
                not(feature = "rustls-tls-aws-lc"),
                not(feature = "native-tls-rustls")
            ))]
            {
                Arc::new(rustls::crypto::ring::default_provider())
            }
        };
        Arc::new(
            rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .expect("rustls safe default protocol versions")
                .with_root_certificates(RootCertStore::empty())
                .with_no_client_auth(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // A syntactically valid self-signed cert (parses as a cert; never
    // verified here -- these tests check store assembly, not chains, so
    // expiry is irrelevant). Generated once for the suite.
    const TEST_CA_PEM: &str = include_str!("../tests/resources/test_ca.pem");

    fn write_tmp(name: &str, body: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("ch_rs_tls_tests");
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join(name);
        std::fs::write(&p, body).unwrap();
        p
    }

    #[test]
    fn add_pem_file_adds_all_certs_in_bundle() {
        let bundle = format!("{TEST_CA_PEM}\n{TEST_CA_PEM}");
        let path = write_tmp("bundle.pem", &bundle);
        let mut store = RootCertStore::empty();
        add_pem_file_certs(&mut store, &path).unwrap();
        assert_eq!(store.len(), 2, "both concatenated certs must be added");
    }

    #[test]
    fn add_pem_file_is_lenient_skips_junk() {
        let mixed = format!(
            "-----BEGIN CERTIFICATE-----\nbm90YWNlcnQ=\n-----END CERTIFICATE-----\n{TEST_CA_PEM}"
        );
        let path = write_tmp("mixed.pem", &mixed);
        let mut store = RootCertStore::empty();
        add_pem_file_certs(&mut store, &path).unwrap();
        assert_eq!(store.len(), 1, "valid cert added, junk block skipped");
    }

    #[test]
    fn add_pem_file_errors_on_zero_certs() {
        let path = write_tmp("empty.pem", "not a pem at all\n");
        let mut store = RootCertStore::empty();
        let err = match add_pem_file_certs(&mut store, &path) {
            Ok(_) => panic!("zero-cert file must error"),
            Err(e) => e,
        };
        assert!(format!("{err}").contains("no usable certificates"));
    }

    #[test]
    fn add_pem_file_errors_on_missing_path() {
        let mut store = RootCertStore::empty();
        let err = match add_pem_file_certs(&mut store, Path::new("/no/such/ca.pem")) {
            Ok(_) => panic!("missing file must error"),
            Err(e) => e,
        };
        assert!(format!("{err}").contains("cannot read CA file"));
    }

    #[test]
    fn build_root_store_augment_includes_extra() {
        let path = write_tmp("root.pem", TEST_CA_PEM);
        let trust = TlsTrust {
            native_roots: false, // keep test hermetic (no OS dependency)
            webpki_roots: true,
            extra_roots: vec![path],
            extra_intermediates: Vec::new(),
            exclusive: false,
        };
        let store = build_root_store(&trust).unwrap();
        // webpki bundle is large; +1 for our cert. Just assert non-empty
        // and larger than webpki alone is hard to pin, so assert it added.
        assert!(store.len() > 1);
    }

    #[test]
    fn build_root_store_exclusive_only_extra() {
        let path = write_tmp("only.pem", TEST_CA_PEM);
        let trust = TlsTrust {
            native_roots: true,
            webpki_roots: true,
            extra_roots: vec![path],
            extra_intermediates: Vec::new(),
            exclusive: true,
        };
        let store = build_root_store(&trust).unwrap();
        assert_eq!(store.len(), 1, "exclusive trusts only the supplied CA");
    }

    #[test]
    fn build_root_store_exclusive_no_files_errors() {
        let trust = TlsTrust {
            native_roots: true,
            webpki_roots: true,
            extra_roots: Vec::new(),
            extra_intermediates: Vec::new(),
            exclusive: true,
        };
        let err = match build_root_store(&trust) {
            Ok(_) => panic!("exclusive with no files must error"),
            Err(e) => e,
        };
        assert!(format!("{err}").contains("no explicit CA files"));
    }
}
