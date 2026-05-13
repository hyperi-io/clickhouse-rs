//! `X-ClickHouse-Progress` response headers -> [`ProgressCallback`].
//!
//! Sibling of [`crate::query_summary::QuerySummary`]: summary is
//! one-shot end-of-query; progress is the in-flight stream.
//!
//! Requires `send_progress_in_http_headers=1` on the server. The
//! client deliberately does NOT auto-set it -- callers who set `0`
//! shouldn't be silently overridden.
//!
//! ```no_run
//! # use clickhouse::Client;
//! let client = Client::default()
//!     .with_url("http://localhost:8123")
//!     .with_setting("send_progress_in_http_headers", "1")
//!     .with_progress_callback(|p| {
//!         tracing::info!(
//!             read_rows = p.read_rows,
//!             total = p.total_rows_to_read,
//!             "progress"
//!         );
//!     });
//! ```
//!
//! Limitation: hyper exposes only response-init headers, not mid-
//! response. The callback fires per header present at init -- fine
//! for fast queries, partial for streaming SELECTs.

use std::sync::Arc;

/// Cumulative counters from one `X-ClickHouse-Progress` header.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Progress {
    pub read_rows: u64,
    pub read_bytes: u64,
    /// Server's estimate; `0` until known.
    pub total_rows_to_read: u64,
    /// INSERT-only; `0` for SELECT.
    pub written_rows: u64,
    /// INSERT-only.
    pub written_bytes: u64,
    pub elapsed_ns: u64,
}

impl Progress {
    /// Parse one header value. `None` on malformed; progress is
    /// best-effort observability, never raise.
    ///
    /// Hand-rolled to avoid `serde_json` allocation on a hot path;
    /// six known numeric fields, format stable.
    #[must_use]
    pub fn from_header_value(value: &str) -> Option<Self> {
        // Format (Server/HTTPHandler.cpp::onProgress):
        //   {"read_rows":"123","read_bytes":"45678",...}
        // Numbers are quoted to dodge JSON's 53-bit safe-int range.
        let trimmed = value.trim();
        if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
            return None;
        }
        let inner = trimmed[1..trimmed.len() - 1].trim();

        let mut p = Progress::default();
        if inner.is_empty() {
            return Some(p);
        }
        for entry in inner.split(',') {
            let (key, val) = entry.split_once(':')?;
            let key = strip_quotes(key.trim())?;
            let val = strip_quotes(val.trim())?;
            let n: u64 = val.parse().ok()?;
            match key {
                "read_rows" => p.read_rows = n,
                "read_bytes" => p.read_bytes = n,
                "total_rows_to_read" => p.total_rows_to_read = n,
                "written_rows" => p.written_rows = n,
                "written_bytes" => p.written_bytes = n,
                "elapsed_ns" => p.elapsed_ns = n,
                // Ignore unknown keys so the server can add fields.
                _ => {}
            }
        }
        Some(p)
    }
}

fn strip_quotes(s: &str) -> Option<&str> {
    let s = s.strip_prefix('"')?;
    s.strip_suffix('"')
}

/// Synchronous progress notification. Must not block -- spawn a
/// task or use a channel for real work. `Arc` so client clones
/// share cheaply.
pub type ProgressCallback = Arc<dyn Fn(&Progress) + Send + Sync + 'static>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_select_progress_with_total() {
        let header = r#"{"read_rows":"100","read_bytes":"2048",\
            "total_rows_to_read":"1000","written_rows":"0",\
            "written_bytes":"0","elapsed_ns":"500000"}"#
            .replace("\\\n            ", "");
        let p = Progress::from_header_value(&header).expect("parses");
        assert_eq!(
            p,
            Progress {
                read_rows: 100,
                read_bytes: 2048,
                total_rows_to_read: 1000,
                written_rows: 0,
                written_bytes: 0,
                elapsed_ns: 500000,
            }
        );
    }

    #[test]
    fn parses_insert_progress() {
        let header = r#"{"read_rows":"0","read_bytes":"0",\
            "total_rows_to_read":"0","written_rows":"500",\
            "written_bytes":"4096","elapsed_ns":"100000"}"#
            .replace("\\\n            ", "");
        let p = Progress::from_header_value(&header).expect("parses");
        assert_eq!(p.written_rows, 500);
        assert_eq!(p.written_bytes, 4096);
        assert_eq!(p.read_rows, 0);
    }

    #[test]
    fn ignores_unknown_keys() {
        let header = r#"{"read_rows":"5","unknown_future_key":"42","elapsed_ns":"1"}"#;
        let p = Progress::from_header_value(header).expect("parses");
        assert_eq!(p.read_rows, 5);
        assert_eq!(p.elapsed_ns, 1);
    }

    #[test]
    fn empty_object_parses_to_default() {
        let p = Progress::from_header_value("{}").expect("parses");
        assert_eq!(p, Progress::default());
    }

    #[test]
    fn malformed_returns_none() {
        assert!(Progress::from_header_value("").is_none());
        assert!(Progress::from_header_value("not json").is_none());
        assert!(Progress::from_header_value(r#"{"read_rows":no_quotes}"#).is_none());
        assert!(Progress::from_header_value(r#"{"read_rows":"not_a_number"}"#).is_none());
    }
}
