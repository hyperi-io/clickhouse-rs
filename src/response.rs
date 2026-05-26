use bstr::ByteSlice;
use bytes::{BufMut, Bytes};
use futures_util::stream::{self, Stream, TryStreamExt};
use http_body_util::BodyExt as _;
use hyper::{
    StatusCode,
    body::{Body as _, Incoming},
};
use hyper_util::client::legacy::ResponseFuture as HyperResponseFuture;
use std::{
    future::{self, Future},
    pin::{Pin, pin},
    task::{Context, Poll},
};

#[cfg(feature = "lz4")]
use crate::compression::lz4::Lz4Decoder;
#[cfg(feature = "zstd")]
use crate::compression::zstd::ZstdHttpDecoder;
use crate::{
    compression::Compression,
    error::{Error, Result},
    progress::{ProgressCallback, dispatch_progress},
    query_summary::QuerySummary,
};
use tracing::Instrument;

// === Response ===

pub(crate) enum Response {
    // Headers haven't been received yet.
    // `Box<_>` improves performance by reducing the size of the whole future.
    Waiting(ResponseFuture),
    // Headers have been received, streaming the body.
    Loading(Chunks),
}

pub(crate) type ResponseFuture =
    Pin<Box<dyn Future<Output = Result<(Chunks, Option<Box<QuerySummary>>)>> + Send>>;

impl Response {
    pub(crate) fn new(
        response: HyperResponseFuture,
        compression: Compression,
        progress_callback: Option<ProgressCallback>,
    ) -> Self {
        let span = tracing::info_span!(
            "response",
            otel.status_code = tracing::field::Empty,
            otel.status_description = tracing::field::Empty,
            error.type = tracing::field::Empty,
            db.response_code = tracing::field::Empty,
        );

        Self::Waiting(Box::pin(
            collect_response(response, compression, progress_callback).instrument(span),
        ))
    }

    pub(crate) fn into_future(self) -> ResponseFuture {
        match self {
            Self::Waiting(future) => future,
            Self::Loading(_) => panic!("response is already streaming"),
        }
    }

    pub(crate) async fn finish(&mut self) -> Result<()> {
        let chunks = loop {
            match self {
                Self::Waiting(future) => {
                    let (chunks, _summary) = future.await?;
                    *self = Self::Loading(chunks);
                }
                Self::Loading(chunks) => break chunks,
            }
        };

        while chunks.try_next().await?.is_some() {}
        Ok(())
    }
}

async fn collect_response(
    response: HyperResponseFuture,
    compression: Compression,
    progress_callback: Option<ProgressCallback>,
) -> Result<(Chunks, Option<Box<QuerySummary>>)> {
    let response = response.await?;

    let status = response.status();
    let exception_code = response.headers().get("X-ClickHouse-Exception-Code");

    // X-ClickHouse-Progress at response init only; trailers needed
    // for mid-response (see progress module docs).
    if let Some(cb) = progress_callback.as_ref() {
        dispatch_progress(cb, response.headers().get_all("X-ClickHouse-Progress"));
    }

    tracing::record_all!(
        tracing::Span::current(),
        // Note: not supposed to set `otel.status_code` unless an error occurs
        db.response.status_code = status.as_u16(),
    );

    if status == StatusCode::OK && exception_code.is_none() {
        let tag = response
            .headers()
            .get("X-ClickHouse-Exception-Tag")
            .map(|value| value.as_bytes().into());

        let summary = response
            .headers()
            .get("X-ClickHouse-Summary")
            .and_then(|v| v.to_str().ok())
            .and_then(QuerySummary::from_header)
            .map(Box::new); // More likely to be successful, start streaming.
        // It still can fail, but we'll handle it in `DetectDbException`.
        // Progress callback is passed to `Chunks` so trailer frames
        // (mid-response progress) are dispatched as bytes are consumed.
        // The response-init dispatch above covers headers buffered
        // before the first body byte.
        Ok((
            Chunks::new(response.into_body(), compression, tag, progress_callback),
            summary,
        ))
    } else {
        // An instantly failed request.
        let error = collect_bad_response(
            status,
            exception_code
                .and_then(|value| value.to_str().ok())
                .map(|code| format!("Code: {code}")),
            response.into_body(),
            compression,
        )
        .await;

        error.record_in_current_span("response error");

        Err(error)
    }
}

/// Cap on the error-response body we will collect into memory.
/// ClickHouse exception messages are at most a few KB even with
/// the full stack trace; an upstream / proxy / MITM returning a
/// multi-megabyte error page should not be allowed to grow our
/// per-request memory unbounded. Truncating beyond the cap loses
/// stack-trace detail; the surfaced Error::BadResponse names the
/// truncation so operators can investigate.
const BAD_RESPONSE_BODY_CAP: usize = 1 << 20; // 1 MiB

#[cold]
#[inline(never)]
async fn collect_bad_response(
    status: StatusCode,
    exception_code: Option<String>,
    body: Incoming,
    compression: Compression,
) -> Error {
    // Collect the whole body into one contiguous buffer to simplify
    // handling, capped at BAD_RESPONSE_BODY_CAP so a malicious /
    // misconfigured peer can't blow our memory budget.
    let raw_bytes = match body.collect().await {
        Ok(collected) => {
            let bytes = collected.to_bytes();
            if bytes.len() > BAD_RESPONSE_BODY_CAP {
                tracing::warn!(
                    target: "clickhouse::response",
                    body_len = bytes.len(),
                    cap = BAD_RESPONSE_BODY_CAP,
                    "error-response body exceeded the truncation cap; \
                     surfacing the prefix only"
                );
                bytes.slice(..BAD_RESPONSE_BODY_CAP)
            } else {
                bytes
            }
        }
        // If we can't collect the body, return standardised reason for the status code.
        Err(_) => return Error::BadResponse(reason(status, exception_code)),
    };
    if raw_bytes.is_empty() {
        return Error::BadResponse(reason(status, exception_code));
    }

    // Try to decompress the body, because CH uses compression even for errors.
    let stream = stream::once(future::ready(Result::<_>::Ok(raw_bytes.slice(..))));
    let stream = Decompress::new(stream, compression).map_ok(|chunk| chunk.data);

    // We're collecting already fetched chunks, thus only decompression errors can
    // be here. If decompression is failed, we should try the raw body because
    // it can be sent without any compression if some proxy is used, which
    // typically know nothing about CH params.
    let bytes = collect_bytes(stream).await.unwrap_or(raw_bytes);

    let reason = String::from_utf8(bytes.into())
        .map(|reason| reason.trim().into())
        // If we have a unreadable response, return standardised reason for the status code.
        .unwrap_or_else(|_| reason(status, exception_code.clone()));

    // Try the structured parse first; fall back to the stringly-typed
    // BadResponse when the body doesn't look like a ClickHouse
    // exception (proxies, non-CH servers, very old CH versions).
    parse_server_exception(&reason, exception_code.as_deref())
        .unwrap_or(Error::BadResponse(reason))
}

/// Best-effort structured parser for ClickHouse error responses.
/// Returns `None` if the body doesn't have the expected shape;
/// callers should fall back to [`Error::BadResponse`].
///
/// Recognises bodies of the form:
/// ```text
/// Code: 469. DB::Exception: <message body>: While executing X.
///   (VIOLATED_CONSTRAINT) (version 26.2.4.23 (official build))
/// ```
/// plus optional `Stack trace:` suffix.
#[cold]
#[inline(never)]
fn parse_server_exception(body: &str, header_code: Option<&str>) -> Option<Error> {
    // Code from header (preferred) or from "Code: N." prefix in body.
    let code: i32 = header_code
        .and_then(|s| s.trim().strip_prefix("Code: ").unwrap_or(s).parse().ok())
        .or_else(|| {
            body.strip_prefix("Code: ")
                .and_then(|s| s.split_once('.').map(|(c, _)| c.trim()))
                .and_then(|c| c.parse().ok())
        })?;

    // Everything after the first `Exception: ` marker is the message
    // body. CH emits variants like `DB::Exception:`, `DB::NetException:`,
    // `DB::ParsingException:`, `DB::ErrnoException:` -- all share the
    // common `Exception: ` suffix, so splitting on that handles every
    // form.
    let after_prefix = body
        .split_once("Exception: ")
        .map(|(_, rest)| rest.trim())
        .unwrap_or(body.trim());

    // Strip the trailing `(version X.Y.Z ...)` chunk if present.
    let without_version = match after_prefix.rfind("(version ") {
        Some(i) => after_prefix[..i].trim_end_matches([' ', '.', ',']),
        None => after_prefix,
    };

    // Extract the trailing `(UPPERCASE_NAME)` exception tag. Allow
    // letters, digits and underscores; require the parens to come at
    // the very end (after trimming the version suffix).
    let (without_name, name) = if without_version.ends_with(')') {
        if let Some(open) = without_version.rfind('(') {
            let candidate = &without_version[open + 1..without_version.len() - 1];
            let looks_like_name = !candidate.is_empty()
                && candidate
                    .chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_');
            if looks_like_name {
                let prefix = without_version[..open].trim_end_matches([':', ' ', '.', ',']);
                (prefix, Some(candidate.to_string()))
            } else {
                (without_version, None)
            }
        } else {
            (without_version, None)
        }
    } else {
        (without_version, None)
    };

    // Separate `Stack trace:` suffix into its own field if present.
    let (message, stack_trace) = match without_name.find("Stack trace:") {
        Some(idx) => {
            let msg = without_name[..idx].trim_end().to_string();
            let st = without_name[idx + "Stack trace:".len()..].trim().to_string();
            (msg, Some(st))
        }
        None => (without_name.trim().to_string(), None),
    };

    if message.is_empty() {
        return None;
    }

    Some(Error::ServerException {
        code,
        name,
        message,
        stack_trace,
    })
}

async fn collect_bytes(stream: impl Stream<Item = Result<Bytes>>) -> Result<Bytes> {
    let mut stream = pin!(stream);

    let mut bytes = Vec::new();

    // TODO: avoid extra copying if there is only one chunk in the stream.
    while let Some(chunk) = stream.try_next().await? {
        bytes.put(chunk);
    }

    Ok(bytes.into())
}

fn reason(status: StatusCode, exception_code: Option<String>) -> String {
    exception_code.unwrap_or_else(|| {
        format!(
            "{} {}",
            status.as_str(),
            status.canonical_reason().unwrap_or("<unknown>"),
        )
    })
}

// === Chunks ===

pub(crate) struct Chunk {
    pub(crate) data: Bytes,
    pub(crate) net_size: usize,
}

// * Uses `Option<_>` to make this stream fused.
// * Uses `Box<_>` in order to reduce the size of cursors.
pub(crate) struct Chunks {
    inner: Option<Box<DetectDbException<Decompress<IncomingStream>>>>,
}

impl Chunks {
    fn new(
        stream: Incoming,
        compression: Compression,
        exception_tag: Option<Box<[u8]>>,
        progress_callback: Option<ProgressCallback>,
    ) -> Self {
        let stream = IncomingStream {
            body: stream,
            progress_callback,
        };
        let stream = Decompress::new(stream, compression);
        let stream = DetectDbException {
            stream,
            exception_tag,
        };
        Self {
            inner: Some(Box::new(stream)),
        }
    }

    pub(crate) fn empty() -> Self {
        Self { inner: None }
    }

    #[cfg(feature = "futures03")]
    pub(crate) fn is_terminated(&self) -> bool {
        self.inner.is_none()
    }
}

impl Stream for Chunks {
    type Item = Result<Chunk>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // We use `take()` to make the stream fused, including the case of panics.
        if let Some(mut stream) = self.inner.take() {
            let res = Pin::new(&mut stream).poll_next(cx);

            if matches!(res, Poll::Pending | Poll::Ready(Some(Ok(_)))) {
                self.inner = Some(stream);
            }

            res
        } else {
            Poll::Ready(None)
        }
    }

    // `size_hint()` is unimplemented because unused.
}

// === IncomingStream ===

// * Produces bytes from incoming data frames.
// * Dispatches `X-ClickHouse-Progress` from HTTP trailer frames to
//   the registered progress callback (mid-response progress; the
//   initial-headers set is handled at `collect_response`).
// * Converts hyper errors to our own.
struct IncomingStream {
    body: Incoming,
    progress_callback: Option<ProgressCallback>,
}

impl Stream for IncomingStream {
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut incoming = Pin::new(&mut this.body);

        loop {
            break match incoming.as_mut().poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(bytes) => Poll::Ready(Some(Ok(bytes))),
                    // Non-data frame; could be trailers. Best-effort:
                    // extract trailers and dispatch any progress
                    // headers we recognise. Silently ignored if it's
                    // not a trailers frame.
                    //
                    // ClickHouse 24.x/25.x emits progress as RESPONSE
                    // HEADERS (sent before the body, all in one block),
                    // not as trailers -- that path is handled in
                    // `collect_response`. The trailer path here is
                    // defensive: future CH versions or alternative
                    // endpoints may switch to trailer-based progress.
                    // Note: HTTP/1.1 chunked-trailer parsers (hyper
                    // included) deduplicate duplicate trailer keys, so
                    // at most one X-ClickHouse-Progress trailer per
                    // response is observable here.
                    Err(frame) => {
                        if let Some(cb) = this.progress_callback.as_ref()
                            && let Ok(trailers) = frame.into_trailers()
                        {
                            dispatch_progress(cb, trailers.get_all("X-ClickHouse-Progress"));
                        }
                        continue;
                    }
                },
                Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err.into()))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            };
        }
    }
}

// === Decompress ===

enum Decompress<S> {
    Plain(S),
    #[cfg(feature = "lz4")]
    Lz4(Lz4Decoder<S>),
    #[cfg(feature = "zstd")]
    Zstd(ZstdHttpDecoder<S>),
}

impl<S> Decompress<S> {
    fn new(stream: S, compression: Compression) -> Self {
        match compression {
            Compression::None => Self::Plain(stream),
            #[cfg(feature = "lz4")]
            #[allow(deprecated)]
            Compression::Lz4 | Compression::Lz4Hc(_) => Self::Lz4(Lz4Decoder::new(stream)),
            #[cfg(feature = "zstd")]
            Compression::Zstd(_) => Self::Zstd(ZstdHttpDecoder::new(stream)),
        }
    }
}

impl<S> Stream for Decompress<S>
where
    S: Stream<Item = Result<Bytes>> + Unpin,
{
    type Item = Result<Chunk>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream)
                .poll_next(cx)
                .map_ok(|bytes| Chunk {
                    net_size: bytes.len(),
                    data: bytes,
                })
                .map_err(Into::into),
            #[cfg(feature = "lz4")]
            Self::Lz4(stream) => Pin::new(stream).poll_next(cx),
            #[cfg(feature = "zstd")]
            Self::Zstd(stream) => Pin::new(stream).poll_next(cx),
        }
    }
}

// === DetectDbException ===

struct DetectDbException<S> {
    stream: S,
    exception_tag: Option<Box<[u8]>>,
}

impl<S> Stream for DetectDbException<S>
where
    S: Stream<Item = Result<Chunk>> + Unpin,
{
    type Item = Result<Chunk>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let res = Pin::new(&mut self.stream).poll_next(cx);

        if let Poll::Ready(Some(Ok(chunk))) = &res
            && let Some(err) = extract_exception(&chunk.data, self.exception_tag.as_deref())
        {
            err.record_in_current_span("response error");
            return Poll::Ready(Some(Err(err)));
        }

        res
    }
}

fn extract_exception(chunk: &[u8], tag: Option<&[u8]>) -> Option<Error> {
    // 25.11 introduced a new exception tagging format that's incompatible with the previous
    // https://github.com/ClickHouse/clickhouse-rs/issues/359
    if let Some(tag) = tag
        && chunk.ends_with(b"__exception__\r\n")
    {
        extract_exception_new(chunk, tag)
    } else if chunk.ends_with(b"))\n") {
        // `))\n` is very rare in real data, so it's fast dirty check.
        // In random data, it occurs with a probability of ~6*10^-8 only.
        extract_exception_old(chunk)
    } else {
        None
    }
}

// Format:
// ```
//   <data>Code: <code>. DB::Exception: <desc> (version <version> (official build))\n
// ```
#[cold]
#[inline(never)]
fn extract_exception_old(chunk: &[u8]) -> Option<Error> {
    let index = chunk.rfind(b"Code:")?;

    if !(chunk[index..].contains_str(b"DB::") && chunk[index..].contains_str(b"Exception:")) {
        return None;
    }

    let exception = String::from_utf8_lossy(&chunk[index..chunk.len() - 1]);
    // Prefer the structured ServerException variant; fall back to
    // stringly BadResponse if the body doesn't parse (proxies / non-
    // CH responders / very old CH versions).
    Some(
        parse_server_exception(&exception, None)
            .unwrap_or_else(|| Error::BadResponse(exception.into())),
    )
}

// https://github.com/ClickHouse/ClickHouse/blob/4eaa92852bac117e95f28abe61237b0257d939d6/src/Server/HTTP/WriteBufferFromHTTPServerResponse.cpp#L347-L357
#[cold]
#[inline(never)]
fn extract_exception_new(chunk: &[u8], tag: &[u8]) -> Option<Error> {
    // Strip the chunk backwards until we get to the `<message length>`
    let rem = chunk
        .strip_suffix(b"\r\n__exception__\r\n")?
        .strip_suffix(tag)?
        .strip_suffix(b" ")?;

    // `<message length>` is *NOT* 8 bytes, because it's actually an integer formatted as text:
    // https://github.com/ClickHouse/ClickHouse/blob/4eaa92852bac117e95f28abe61237b0257d939d6/src/Server/HTTP/WriteBufferFromHTTPServerResponse.cpp#L376
    //
    // This means we actually need to search for the `\n` that's added to terminate the message:
    // https://github.com/ClickHouse/ClickHouse/blob/4eaa92852bac117e95f28abe61237b0257d939d6/src/Server/HTTP/WriteBufferFromHTTPServerResponse.cpp#L373-L374
    let msg_len_start = rem.rfind(b"\n")? + 1;

    // `msg_len_start` should always be either in-bounds or just past the end
    let msg_len = match parse_msg_len(&rem[msg_len_start..]) {
        Ok(msg_len) => msg_len,
        // At this point we can be fairly certain we've found the exception tag,
        // so it's better to fail with an error than continue.
        Err(e) => return Some(e),
    };

    // Note: checked operations in case `msg_len` is incorrect
    let Some(msg) = msg_len_start
        .checked_sub(msg_len)
        .and_then(|msg_start| rem.get(msg_start..msg_len_start))
    else {
        return Some(Error::Other(
            format!("found exception tag in response but message length was invalid: {msg_len} (chunk len: {})", chunk.len())
                .into(),
        ));
    };

    // We shouldn't discard the exception message if it fails to validate as UTF-8
    let exception: String = String::from_utf8_lossy(msg).trim().into();
    Some(
        parse_server_exception(&exception, None)
            .unwrap_or(Error::BadResponse(exception)),
    )
}

// FIXME: this can be replaced with `usize::from_ascii()` when stable
// https://github.com/rust-lang/rust/issues/134821
fn parse_msg_len(len_bytes: &[u8]) -> Result<usize, Error> {
    let len_utf8 = str::from_utf8(len_bytes).map_err(|e| {
        Error::Other(
            format!("found exception tag in response but failed to parse message length: {e}")
                .into(),
        )
    })?;

    len_utf8.parse().map_err(|e| {
        Error::Other(
            format!("found exception tag in response but failed to parse message length {len_utf8:?}: {e}")
                .into(),
        )
    })
}

#[test]
fn it_extracts_exception_old() {
    let cases: [(&str, i32, &str, &str); 2] = [
        (
            "Code: 159. DB::Exception: Timeout exceeded: elapsed 1.2 seconds, maximum: 0.1. (TIMEOUT_EXCEEDED) (version 24.10.1.2812 (official build))",
            159,
            "TIMEOUT_EXCEEDED",
            "Timeout exceeded: elapsed 1.2 seconds, maximum: 0.1",
        ),
        (
            "Code: 210. DB::NetException: I/O error: Broken pipe, while writing to socket (127.0.0.1:9000 -> 127.0.0.1:54646). (NETWORK_ERROR) (version 23.8.8.20 (official build))",
            210,
            "NETWORK_ERROR",
            "I/O error: Broken pipe, while writing to socket (127.0.0.1:9000 -> 127.0.0.1:54646)",
        ),
    ];

    for (raw, expect_code, expect_name, expect_msg) in cases {
        let chunk = format!("{raw}\n");
        let err = extract_exception(chunk.as_bytes(), None).expect("failed to extract exception");
        match err {
            Error::ServerException { code, name, message, stack_trace } => {
                assert_eq!(code, expect_code);
                assert_eq!(name.as_deref(), Some(expect_name));
                assert_eq!(message, expect_msg);
                assert!(stack_trace.is_none(), "no Stack trace: section in input");
            }
            other => panic!("expected ServerException, got: {other:?}"),
        }
    }
}

#[test]
fn it_extracts_exception_new() {
    let tag = b"rnywyenlaeqynhmu";
    let chunk = b"\r\n__exception__\r\nrnywyenlaeqynhmu\r\nCode: 159. DB::Exception: Timeout exceeded: elapsed 126.147987 ms, maximum: 100 ms. (TIMEOUT_EXCEEDED) (version 25.12.1.649 (official build))\n142 rnywyenlaeqynhmu\r\n__exception__\r\n";

    let err = extract_exception(chunk, Some(tag)).expect("failed to extract exception");
    match err {
        Error::ServerException { code, name, message, stack_trace } => {
            assert_eq!(code, 159);
            assert_eq!(name.as_deref(), Some("TIMEOUT_EXCEEDED"));
            assert_eq!(message, "Timeout exceeded: elapsed 126.147987 ms, maximum: 100 ms");
            assert!(stack_trace.is_none());
        }
        other => panic!("expected ServerException, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// parse_server_exception unit tests
// ---------------------------------------------------------------------------

#[test]
fn parse_server_exception_constraint_violation() {
    let body = "Code: 469. DB::Exception: Constraint `x_lt_10` for table benchmark.t is violated at row 5000. Expression: (x < 10). Column values: x = 100: While executing WaitForAsyncInsert. (VIOLATED_CONSTRAINT) (version 26.2.4.23 (official build))";
    let err = parse_server_exception(body, None).expect("should parse");
    match err {
        Error::ServerException { code, name, message, stack_trace } => {
            assert_eq!(code, 469);
            assert_eq!(name.as_deref(), Some("VIOLATED_CONSTRAINT"));
            assert!(message.contains("Constraint `x_lt_10`"));
            assert!(message.contains("violated at row 5000"));
            assert!(stack_trace.is_none());
        }
        other => panic!("expected ServerException, got {other:?}"),
    }
}

#[test]
fn parse_server_exception_with_stack_trace() {
    let body = "Code: 36. DB::Exception: Bad arguments. Stack trace:\n0. ./Common/Exception.cpp:99\n1. ./Functions/foo.cpp:42\n (BAD_ARGUMENTS) (version 26.2.4.23 (official build))";
    let err = parse_server_exception(body, None).expect("should parse");
    match err {
        Error::ServerException { code, name, stack_trace, .. } => {
            assert_eq!(code, 36);
            assert_eq!(name.as_deref(), Some("BAD_ARGUMENTS"));
            let st = stack_trace.expect("stack trace present");
            assert!(st.contains("Exception.cpp:99"));
            assert!(st.contains("foo.cpp:42"));
        }
        other => panic!("expected ServerException, got {other:?}"),
    }
}

#[test]
fn parse_server_exception_code_from_header_preferred() {
    // Body has no "Code: N." prefix; rely on header.
    let body = "DB::Exception: Something happened (UNKNOWN_TABLE) (version 26.2.4.23 (official build))";
    let err = parse_server_exception(body, Some("60")).expect("should parse");
    match err {
        Error::ServerException { code, name, .. } => {
            assert_eq!(code, 60);
            assert_eq!(name.as_deref(), Some("UNKNOWN_TABLE"));
        }
        other => panic!("expected ServerException, got {other:?}"),
    }
}

#[test]
fn parse_server_exception_unparseable_returns_none() {
    // Garbage body, no code anywhere.
    assert!(parse_server_exception("404 Not Found", None).is_none());
    assert!(parse_server_exception("", None).is_none());
    // Has DB::Exception but no code at all.
    assert!(
        parse_server_exception("DB::Exception: oops", None).is_none(),
        "no code in header or body should yield None"
    );
}

#[test]
fn parse_server_exception_without_name_tag() {
    // Some older / proxied responses omit the (NAME) tag.
    let body = "Code: 999. DB::Exception: Generic problem (version 26.2.4.23 (official build))";
    let err = parse_server_exception(body, None).expect("should parse");
    match err {
        Error::ServerException { code, name, message, .. } => {
            assert_eq!(code, 999);
            assert!(name.is_none(), "no (NAME) tag in input");
            assert_eq!(message, "Generic problem");
        }
        other => panic!("expected ServerException, got {other:?}"),
    }
}
