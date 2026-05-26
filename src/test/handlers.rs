use std::marker::PhantomData;

use bytes::Bytes;
use futures_channel::oneshot;
use hyper::{Request, Response, StatusCode};
use serde::Serialize;

use super::{Handler, HandlerFn};
use crate::{Row, RowOwned, RowRead, rowbinary};

const BUFFER_INITIAL_CAPACITY: usize = 1024;

// === Thunk ===

struct Thunk(Response<Bytes>);

impl super::sealed::Sealed for Thunk {}

impl super::Handler for Thunk {
    type Control = ();

    fn make(self) -> (HandlerFn, Self::Control) {
        (Box::new(|_| self.0), ())
    }
}

// === failure ===

#[track_caller]
pub fn failure(status: StatusCode) -> impl Handler {
    let reason = status.canonical_reason().unwrap_or("<unknown status code>");

    Response::builder()
        .status(status)
        .body(Bytes::from(reason))
        .map(Thunk)
        .expect("invalid builder")
}

#[track_caller]
pub fn exception(code: u8) -> impl Handler {
    Response::builder()
        .status(StatusCode::OK)
        .header("X-ClickHouse-Exception-Code", code.to_string())
        .body(Bytes::new())
        .map(Thunk)
        .expect("invalid builder")
}
// === provide ===

#[track_caller]
pub fn provide<T>(rows: impl IntoIterator<Item = T>) -> impl Handler
where
    T: Serialize + Row,
{
    let mut buffer = Vec::with_capacity(BUFFER_INITIAL_CAPACITY);
    for row in rows {
        rowbinary::serialize_row_binary(&mut buffer, &row).expect("failed to serialize");
    }
    Thunk(Response::new(buffer.into()))
}

// === provide_with_summary ===

/// Like [`provide`], but includes an `X-ClickHouse-Summary` response header.
#[track_caller]
pub fn provide_with_summary<T>(rows: impl IntoIterator<Item = T>, summary: &str) -> impl Handler
where
    T: Serialize + Row,
{
    let mut buffer = Vec::with_capacity(BUFFER_INITIAL_CAPACITY);
    for row in rows {
        rowbinary::serialize_row_binary(&mut buffer, &row).expect("failed to serialize");
    }
    Thunk(
        Response::builder()
            .header("X-ClickHouse-Summary", summary)
            .body(Bytes::from(buffer))
            .expect("invalid builder"),
    )
}

// === provide_with_progress ===

/// Like [`provide`], but emits one or more `X-ClickHouse-Progress`
/// response headers (one per element in `progress_headers`). The
/// raw header values are passed through verbatim; callers
/// constructing them should match the server's format
/// (a JSON object with quoted-numeric fields per
/// [`crate::progress::Progress::from_header_value`]).
///
/// Used by 11a-callbacks-api tests to verify the progress callback
/// fires correctly when the server emits headers.
#[track_caller]
pub fn provide_with_progress<T>(
    rows: impl IntoIterator<Item = T>,
    progress_headers: impl IntoIterator<Item: AsRef<str>>,
) -> impl Handler
where
    T: Serialize + Row,
{
    let mut buffer = Vec::with_capacity(BUFFER_INITIAL_CAPACITY);
    for row in rows {
        rowbinary::serialize_row_binary(&mut buffer, &row).expect("failed to serialize");
    }
    let mut builder = Response::builder();
    for header in progress_headers {
        builder = builder.header("X-ClickHouse-Progress", header.as_ref());
    }
    Thunk(
        builder
            .body(Bytes::from(buffer))
            .expect("invalid builder"),
    )
}

// === record ===

struct RecordHandler<T>(PhantomData<T>);

impl<T> super::sealed::Sealed for RecordHandler<T> {}

impl<T> super::Handler for RecordHandler<T> {
    type Control = RecordControl<T>;

    #[doc(hidden)]
    fn make(self) -> (HandlerFn, Self::Control) {
        let (tx, rx) = oneshot::channel();
        let marker = PhantomData;
        let control = RecordControl { rx, marker };

        let h = Box::new(move |request: Request<Bytes>| -> Response<Bytes> {
            let body = request.into_body();
            let _ = tx.send(body);
            Response::new(<_>::default())
        });

        (h, control)
    }
}

pub struct RecordControl<T> {
    rx: oneshot::Receiver<Bytes>,
    marker: PhantomData<T>,
}

impl<T> RecordControl<T>
where
    T: RowOwned + RowRead,
{
    pub async fn collect<C>(self) -> C
    where
        C: Default + Extend<T>,
    {
        let bytes = self.rx.await.expect("query canceled");
        let slice = &mut (&bytes[..]);
        let mut result = C::default();

        while !slice.is_empty() {
            let res = rowbinary::deserialize_row(slice, None);
            let row: T = res.expect("failed to deserialize");
            result.extend(std::iter::once(row));
        }

        result
    }
}

#[track_caller]
pub fn record<T>() -> impl Handler<Control = RecordControl<T>> {
    RecordHandler(PhantomData)
}

// === record_with_uri ===

/// Like [`record`] but also captures the request's URI string so
/// tests can assert on query-string params (e.g. `log_comment=...`).
/// Used by 11b-writer-id tests; useful elsewhere when verifying
/// per-INSERT settings made it onto the wire.
struct RecordWithUriHandler<T>(PhantomData<T>);

impl<T> super::sealed::Sealed for RecordWithUriHandler<T> {}

impl<T> super::Handler for RecordWithUriHandler<T> {
    type Control = RecordWithUriControl<T>;

    #[doc(hidden)]
    fn make(self) -> (HandlerFn, Self::Control) {
        let (tx, rx) = oneshot::channel();
        let marker = PhantomData;
        let control = RecordWithUriControl { rx, marker };

        let h = Box::new(move |request: Request<Bytes>| -> Response<Bytes> {
            let uri = request.uri().to_string();
            let body = request.into_body();
            let _ = tx.send((uri, body));
            Response::new(<_>::default())
        });

        (h, control)
    }
}

pub struct RecordWithUriControl<T> {
    rx: oneshot::Receiver<(String, Bytes)>,
    marker: PhantomData<T>,
}

impl<T> RecordWithUriControl<T>
where
    T: RowOwned + RowRead,
{
    /// Wait for the request and return `(uri, rows)`. `uri` is the
    /// full request URI string (path + query); `rows` are
    /// RowBinary-decoded from the request body.
    pub async fn collect<C>(self) -> (String, C)
    where
        C: Default + Extend<T>,
    {
        let (uri, bytes) = self.rx.await.expect("query canceled");
        let slice = &mut (&bytes[..]);
        let mut rows = C::default();
        while !slice.is_empty() {
            let res = rowbinary::deserialize_row(slice, None);
            let row: T = res.expect("failed to deserialize");
            rows.extend(std::iter::once(row));
        }
        (uri, rows)
    }

    /// Wait for the request and return just the URI. Useful for
    /// tests where the body shape doesn't decode as `T` (e.g.
    /// SELECTs where the body carries the SQL text, not row data).
    pub async fn collect_uri(self) -> String {
        let (uri, _bytes) = self.rx.await.expect("query canceled");
        uri
    }
}

#[track_caller]
pub fn record_with_uri<T>() -> impl Handler<Control = RecordWithUriControl<T>> {
    RecordWithUriHandler(PhantomData)
}

// === record_ddl ===

struct RecordDdlHandler;

impl super::sealed::Sealed for RecordDdlHandler {}

impl super::Handler for RecordDdlHandler {
    type Control = RecordDdlControl;

    #[doc(hidden)]
    fn make(self) -> (HandlerFn, Self::Control) {
        let (tx, rx) = oneshot::channel();
        let control = RecordDdlControl(rx);

        let h = Box::new(move |request: Request<Bytes>| -> Response<Bytes> {
            let body = request.into_body();
            let _ = tx.send(body);
            Response::new(<_>::default())
        });

        (h, control)
    }
}

pub struct RecordDdlControl(oneshot::Receiver<Bytes>);

impl RecordDdlControl {
    pub async fn query(self) -> String {
        let buffer = self.0.await.expect("query canceled");
        String::from_utf8(buffer.to_vec()).expect("query is not DDL")
    }
}

pub fn record_ddl() -> impl Handler<Control = RecordDdlControl> {
    RecordDdlHandler
}
