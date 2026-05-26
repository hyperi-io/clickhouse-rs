//! Contains [`Error`] and corresponding [`Result`].

use serde::{de, ser};
use std::{error::Error as StdError, fmt, io, result, str::Utf8Error};

/// A result with a specified [`Error`] type.
pub type Result<T, E = Error> = result::Result<T, E>;

type BoxedError = Box<dyn StdError + Send + Sync>;

/// Represents all possible errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum Error {
    #[error("invalid params: {0}")]
    InvalidParams(#[source] BoxedError),
    #[error("network error: {0}")]
    Network(#[source] BoxedError),
    #[error("compression error: {0}")]
    Compression(#[source] BoxedError),
    #[error("decompression error: {0}")]
    Decompression(#[source] BoxedError),
    #[error("no rows returned by a query that expected to return at least one row")]
    RowNotFound,
    #[error("sequences must have a known size ahead of time")]
    SequenceMustHaveLength,
    #[error("`deserialize_any` is not supported")]
    DeserializeAnyNotSupported,
    #[error("not enough data, probably a row type mismatches a database schema")]
    NotEnoughData,
    #[error("string is not valid utf8")]
    InvalidUtf8Encoding(#[from] Utf8Error),
    #[error("tag for enum is not valid")]
    InvalidTagEncoding(usize),
    #[error("max number of types in the Variant data type is 255, got {0}")]
    VariantDiscriminatorIsOutOfBound(usize),
    #[error("a custom error message from serde: {0}")]
    Custom(String),
    /// Background worker task exited; the inserter is no longer
    /// accepting commands. Terminal: construct a fresh inserter,
    /// don't retry the same handle.
    #[error("background worker task has exited; the inserter is no longer accepting commands")]
    WorkerExited,
    /// AsyncInserter state-machine misuse from the caller.
    /// Example: calling `write` (default-table form) on an inserter
    /// constructed via `new_multi_table`. `method` is the API the
    /// caller invoked; `hint` is the corrective action.
    #[error("AsyncInserter API misuse: `{method}` -- {hint}")]
    AsyncInserterApiMisuse {
        method: &'static str,
        hint: &'static str,
    },
    /// Structured server-side exception parsed from the response.
    /// Preferred over [`Error::BadResponse`] when the server returned
    /// a recognisable `Code: NNN. DB::Exception: ...` body. Callers
    /// can match on `code` for typed handling and use
    /// [`Error::is_retriable`] for conservative retry classification.
    #[error("server error code {code}: {message}")]
    ServerException {
        /// `X-ClickHouse-Exception-Code` from the response header.
        code: u32,
        /// Exception name extracted from `(UPPERCASE_NAME)` near
        /// the end of the message. `None` when the parser couldn't
        /// recognise it (older CH versions, custom error paths).
        name: Option<String>,
        /// Cleaned message body, stripped of the `Code: N.`,
        /// `DB::Exception:` prefix, exception-name parens, and
        /// `(version ...)` suffix.
        message: String,
        /// Server-side stack trace if `Stack trace:` was present in
        /// the body.
        stack_trace: Option<String>,
    },
    #[error("bad response: {0}")]
    BadResponse(String),
    #[error("timeout expired")]
    TimedOut,
    #[error("error while parsing columns header from the response: {0}")]
    InvalidColumnsHeader(#[source] BoxedError),
    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("{0}")]
    Other(BoxedError),
}

impl From<clickhouse_types::error::TypesError> for Error {
    fn from(err: clickhouse_types::error::TypesError) -> Self {
        Self::InvalidColumnsHeader(Box::new(err))
    }
}

impl From<hyper::Error> for Error {
    fn from(error: hyper::Error) -> Self {
        Self::Network(Box::new(error))
    }
}

impl From<hyper_util::client::legacy::Error> for Error {
    fn from(error: hyper_util::client::legacy::Error) -> Self {
        #[cfg(not(any(feature = "rustls-tls", feature = "native-tls")))]
        if error.is_connect() {
            static SCHEME_IS_NOT_HTTP: &str = "invalid URL, scheme is not http";

            let src = error.source().unwrap();
            // Unfortunately, this seems to be the only way, as `INVALID_NOT_HTTP` is not public.
            // See https://github.com/hyperium/hyper-util/blob/v0.1.14/src/client/legacy/connect/http.rs#L491-L495
            if src.to_string() == SCHEME_IS_NOT_HTTP {
                return Self::Unsupported(format!(
                    "{SCHEME_IS_NOT_HTTP}; if you are trying to connect via HTTPS, \
                    consider enabling `native-tls` or `rustls-tls` feature"
                ));
            }
        }
        Self::Network(Box::new(error))
    }
}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self::Custom(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self::Custom(msg.to_string())
    }
}

impl From<Error> for io::Error {
    fn from(error: Error) -> Self {
        io::Error::other(error)
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        // TODO: after MSRV 1.79 replace with `io::Error::downcast`.
        if error.get_ref().is_some_and(|r| r.is::<Error>()) {
            *error.into_inner().unwrap().downcast::<Error>().unwrap()
        } else {
            Self::Other(error.into())
        }
    }
}

impl Error {
    /// Method sugar over
    /// [`recovery::failing_row_from_error`][crate::recovery::failing_row_from_error].
    /// See [`recovery`][crate::recovery] for parsed patterns.
    #[must_use]
    pub fn failing_row(&self) -> Option<crate::recovery::FailureLocation> {
        crate::recovery::failing_row_from_error(self)
    }

    /// Conservative retriability classification.
    ///
    /// Returns `true` for errors that *might* succeed on retry --
    /// transport-level (`Network`, `TimedOut`) and a known set of
    /// transient server-side codes (timeouts, simultaneous-query
    /// limits, parts-count overruns, Keeper hiccups).
    ///
    /// Returns `false` for everything else, including unknown
    /// server codes. The classification is intentionally
    /// conservative -- callers wanting more aggressive retry should
    /// match on the underlying variant. The full mapping lives in
    /// [`is_retriable_code`][Self::is_retriable_code]; PRs welcome
    /// to extend it as production experience shows what's actually
    /// transient.
    #[must_use]
    pub fn is_retriable(&self) -> bool {
        match self {
            Self::Network(_) | Self::TimedOut => true,
            Self::ServerException { code, .. } => Self::is_retriable_code(*code),
            _ => false,
        }
    }

    /// ClickHouse error codes considered transient for retry. See
    /// `src/Common/ErrorCodes.cpp` upstream for the canonical list.
    /// Conservative -- false negatives are expected; false positives
    /// should be rare. Source-of-truth comments in this table use
    /// the upstream macro name. The table is production-experience
    /// driven; PRs extending it with new operationally-transient
    /// codes are welcome.
    #[must_use]
    pub fn is_retriable_code(code: u32) -> bool {
        matches!(
            code,
            // TIMEOUT_EXCEEDED
            159 |
            // TOO_SLOW (merge queue lag; usually transient)
            160 |
            // TOO_MANY_SIMULTANEOUS_QUERIES
            202 |
            // SOCKET_TIMEOUT
            209 |
            // NETWORK_ERROR
            210 |
            // ABORTED (operator KILL or shutdown -- replayable on
            // another node)
            236 |
            // MEMORY_LIMIT_EXCEEDED (often transient under brief
            // memory pressure)
            241 |
            // TOO_MANY_PARTS (transient under heavy ingest)
            252 |
            // ALL_CONNECTION_TRIES_FAILED
            279 |
            // LIMIT_EXCEEDED (transient quota)
            290 |
            // UNKNOWN_STATUS_OF_INSERT (the "did my INSERT land?"
            // ambiguity code -- documented mitigation is retry with
            // an insert_deduplication_token, which the
            // batch_isolation token variant ships)
            319 |
            // CANNOT_SCHEDULE_TASK (task scheduler full)
            439 |
            // KEEPER_EXCEPTION (replicated-metadata hiccup)
            999
        )
    }

    /// https://opentelemetry.io/docs/specs/semconv/registry/attributes/error/#error-type
    #[cfg(feature = "opentelemetry")]
    pub(crate) fn error_type(&self) -> &str {
        match self {
            Error::InvalidParams(_) => "InvalidParams",
            Error::Network(_) => "Network",
            Error::Compression(_) => "Compression",
            Error::Decompression(_) => "Decompression",
            Error::RowNotFound => "RowNotFound",
            Error::SequenceMustHaveLength => "SequenceMustHaveLength",
            Error::DeserializeAnyNotSupported => "DeserializeAnyNotSupported",
            Error::NotEnoughData => "NotEnoughData",
            Error::InvalidUtf8Encoding(_) => "InvalidUtf8Encoding",
            Error::InvalidTagEncoding(_) => "InvalidTagEncoding",
            Error::VariantDiscriminatorIsOutOfBound(_) => "VariantDiscriminatorIsOutOfBound",
            Error::Custom(_) => "Custom",
            Error::WorkerExited => "WorkerExited",
            Error::AsyncInserterApiMisuse { .. } => "AsyncInserterApiMisuse",
            Error::ServerException { .. } => "ServerException",
            Error::BadResponse(_) => "BadResponse",
            Error::TimedOut => "TimedOut",
            Error::InvalidColumnsHeader(_) => "InvalidColumnsHeader",
            Error::SchemaMismatch(_) => "SchemaMismatch",
            Error::Unsupported(_) => "Unsupported",
            Error::Other(_) => "Other",
        }
    }

    /// Record this `Error` in the context of the current `tracing::Span`,
    /// setting the OpenTelemetry conventional fields if the `opentelemetry` feature is enabled.
    pub(crate) fn record_in_current_span(&self, msg: &str) {
        // Span fields that remain unpopulated are not reported,
        // so we can avoid adding noise to logs if the user isn't utilizing this feature.
        #[cfg(feature = "opentelemetry")]
        tracing::record_all!(
            tracing::Span::current(),
            otel.status_code = "Error",
            otel.status_description = format!("{msg}: {self}"),
            error.type = self.error_type(),
        );

        tracing::debug!(error=%self, "{msg}");
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error;
    use std::io;

    #[test]
    fn roundtrip_io_error() {
        let orig = Error::NotEnoughData;

        // Error -> io::Error
        let orig_str = orig.to_string();
        let io = io::Error::from(orig);
        assert_eq!(io.kind(), io::ErrorKind::Other);
        assert_eq!(io.to_string(), orig_str);

        // io::Error -> Error
        let orig = Error::from(io);
        assert!(matches!(orig, Error::NotEnoughData));
    }

    #[test]
    fn error_traits() {
        fn assert_traits<T: std::error::Error + Send + Sync>() {}

        assert_traits::<Error>();
    }

    #[test]
    fn is_retriable_classifies_known_transient_codes() {
        let make = |code: u32| Error::ServerException {
            code,
            name: None,
            message: String::new(),
            stack_trace: None,
        };

        // Sampled from the retriable table -- known transient codes.
        for &code in &[159u32, 160, 202, 209, 210, 236, 241, 252, 279, 290, 319, 439, 999] {
            assert!(
                make(code).is_retriable(),
                "code {code} should be retriable"
            );
        }

        // Sampled known-not-retriable codes (auth, schema, parse).
        for &code in &[36u32, 60, 117, 192, 195, 469] {
            assert!(
                !make(code).is_retriable(),
                "code {code} should NOT be retriable"
            );
        }
    }

    #[test]
    fn is_retriable_for_transport_errors() {
        assert!(Error::TimedOut.is_retriable());
        // Network variant needs a BoxedError construction; use a
        // simple io::Error round-trip to build one.
        let net_err: Error =
            io::Error::new(io::ErrorKind::ConnectionReset, "reset").into();
        // io::Error -> Error::Custom path; not a Network variant.
        // Just verify Custom is NOT retriable to lock in the
        // conservative classification.
        assert!(!net_err.is_retriable());
    }

    #[test]
    fn is_retriable_code_defaults_unknown_to_not_retriable() {
        // Conservative default: anything outside the documented
        // retriable set is NOT retriable. New ClickHouse versions
        // can ship new codes; until our table is updated, callers
        // see them as terminal -- safer than masking real failures
        // under a retry loop.
        for &code in &[0u32, 1, 12345, 99999, u32::MAX] {
            assert!(
                !Error::is_retriable_code(code),
                "unknown code {code} should default to NOT retriable"
            );
        }
    }
}
