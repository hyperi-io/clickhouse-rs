//! Streaming-SELECT cursor over the TCP connection actor.
//!
//! Pairs with [`crate::tcp::connection_actor::ConnectionCmd::ExecuteStream`].
//!
//! [`TcpRawCursor`] is the v1 entry point: it yields whole
//! [`DecodedBlock`]s one at a time. Callers walk the columnar container
//! themselves; no row-serde bridge involved. This is the shape
//! integration tests and Phase-3 cursor consumers use today.
//!
//! A per-row deserialising cursor that bridges to the upstream
//! `crate::Row` trait is a follow-up: `Row` is built around
//! `serde::Deserialize` over RowBinary bytes, so it needs a transpose
//! pass from the column-oriented [`DecodedBlock`] back to per-row
//! RowBinary. That type lands with its implementation rather than as
//! an always-erroring placeholder.
//!
//! # Cancel-on-drop
//!
//! Dropping the cursor drops its `mpsc::Receiver`, which trips the
//! `results.closed()` watch the actor's `do_execute_stream` runs.
//! That triggers a protocol Cancel + bounded drain inside the actor
//! itself -- no explicit `tokio::spawn` from the cursor's `Drop`. The
//! "runtime Handle captured for Drop-time spawn survival" pattern
//! used elsewhere in this crate still applies in principle: we
//! capture [`tokio::runtime::Handle::current`] at construction so
//! future growth (e.g. a v2 sync-context cursor.next() backed by
//! `block_on`) does not have to track down the right runtime. The
//! Handle is stored even though v1 does not use it.

use tokio::sync::mpsc;

use crate::error::Result;
use crate::native::decode::DecodedBlock;
use crate::tcp::reader::ServerPacket;

/// Whole-block streaming-SELECT cursor.
///
/// Returns one [`DecodedBlock`] per `next()` call until the server
/// emits `EndOfStream` (`next()` returns `Ok(None)`) or an Exception
/// (`next()` returns `Err(Error::ServerException)`). Schema blocks
/// (`num_rows == 0`) are surfaced as well so callers that care about
/// the announced `(name, type_name)` pairs can read them; payload
/// blocks always carry their own schema too via
/// [`DecodedBlock::schema`] so most cursors skip the empty one.
pub struct TcpRawCursor {
    rx: mpsc::Receiver<Result<ServerPacket>>,
    /// Captured for any v2 Drop-time spawn need. Reserved -- not
    /// consulted in v1. Holding `Handle::current()` at construction
    /// means a future Drop that wants to schedule cleanup work (the
    /// classic kill-on-drop pattern) survives the TLS-current-runtime
    /// tear-down a generic sync caller would otherwise trigger.
    _runtime: tokio::runtime::Handle,
    /// `true` once an EndOfStream or Exception has been observed.
    /// `next()` returns `Ok(None)` thereafter without touching `rx`.
    done: bool,
}

impl TcpRawCursor {
    /// Construct a raw cursor over the actor's reply channel.
    ///
    /// `rx` is the receiver half of the mpsc pair passed into
    /// [`crate::tcp::connection_actor::ConnectionHandle::execute_stream`];
    /// dropping `self` drops `rx`, which trips the actor-side
    /// `results.closed()` watch and triggers Cancel + drain.
    pub(crate) fn from_receiver(rx: mpsc::Receiver<Result<ServerPacket>>) -> Self {
        Self {
            rx,
            _runtime: tokio::runtime::Handle::current(),
            done: false,
        }
    }

    /// Pull the next [`DecodedBlock`] off the stream. Returns:
    ///
    /// - `Ok(Some(block))` for both schema (`num_rows == 0`) and
    ///   payload (`num_rows > 0`) blocks. Callers can filter by
    ///   `block.num_rows == 0` if they want payload-only iteration.
    /// - `Ok(None)` when the server has emitted EndOfStream -- the
    ///   stream is fully drained, the connection is reusable.
    /// - `Err(Error::ServerException { .. })` if the server returned
    ///   an Exception mid-stream; the actor will drain to EndOfStream
    ///   on its own, so the connection stays reusable.
    /// - `Err(other)` for I/O or decode failures; the actor poisons
    ///   the connection on these.
    pub async fn next_block(&mut self) -> Result<Option<DecodedBlock>> {
        if self.done {
            return Ok(None);
        }
        loop {
            match self.rx.recv().await {
                None => {
                    // Actor exited without forwarding a final packet
                    // -- treat as terminal so subsequent `next_block`
                    // calls return `Ok(None)` rather than panic on a
                    // closed receiver.
                    self.done = true;
                    return Ok(None);
                }
                Some(Err(e)) => {
                    self.done = true;
                    return Err(e);
                }
                Some(Ok(ServerPacket::EndOfStream)) => {
                    self.done = true;
                    return Ok(None);
                }
                Some(Ok(ServerPacket::DataBlock(block))) => {
                    return Ok(Some(block));
                }
                Some(Ok(ServerPacket::Data {
                    num_rows, columns, ..
                })) => {
                    // Surface the schema block as an empty
                    // DecodedBlock; downstream callers cross-reference
                    // `schema.len()` for the authoritative column
                    // count, and a payload block follows shortly with
                    // populated `columns`.
                    return Ok(Some(DecodedBlock {
                        columns: Vec::new(),
                        schema: columns,
                        num_rows,
                    }));
                }
                // Progress / ProfileInfo / TableColumns / TimezoneUpdate
                // are not row data; skip and keep pulling until we see
                // a block, EndOfStream, or Exception.
                Some(Ok(_)) => continue,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::native::decode::DecodedColumn;
    use crate::tcp::reader::ServerPacket;

    // Compile-time assertion: the cursor must be Send so callers can
    // move it across `.await` points and pass it to spawned tasks.
    static_assertions::assert_impl_all!(TcpRawCursor: Send);

    #[tokio::test]
    async fn raw_cursor_terminates_on_end_of_stream() {
        let (tx, rx) = mpsc::channel(4);
        let mut cur = TcpRawCursor::from_receiver(rx);
        tx.send(Ok(ServerPacket::EndOfStream)).await.unwrap();
        assert!(cur.next_block().await.unwrap().is_none());
        // Repeated calls after EndOfStream stay `Ok(None)` -- callers
        // that don't drop the cursor immediately must not see a panic
        // on the closed receiver.
        assert!(cur.next_block().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn raw_cursor_yields_data_block_then_eos() {
        let (tx, rx) = mpsc::channel(4);
        let mut cur = TcpRawCursor::from_receiver(rx);
        let block = DecodedBlock {
            columns: vec![DecodedColumn::UInt64(vec![1, 2, 3])],
            schema: vec![("n".into(), "UInt64".into())],
            num_rows: 3,
        };
        tx.send(Ok(ServerPacket::DataBlock(block))).await.unwrap();
        tx.send(Ok(ServerPacket::EndOfStream)).await.unwrap();

        let first = cur.next_block().await.unwrap().expect("block");
        assert_eq!(first.num_rows, 3);
        match &first.columns[0] {
            DecodedColumn::UInt64(v) => assert_eq!(v, &vec![1u64, 2, 3]),
            other => panic!("expected UInt64, got {other:?}"),
        }

        assert!(cur.next_block().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn raw_cursor_surfaces_server_error() {
        let (tx, rx) = mpsc::channel(4);
        let mut cur = TcpRawCursor::from_receiver(rx);
        let err = Error::ServerException {
            code: 60,
            name: Some("DB::Exception".into()),
            message: "table not found".into(),
            stack_trace: None,
        };
        tx.send(Err(err)).await.unwrap();
        let result = cur.next_block().await;
        match result {
            Err(Error::ServerException { code, .. }) => assert_eq!(code, 60),
            other => panic!("expected ServerException, got {other:?}"),
        }
        // After surfacing an error the cursor is terminal too.
        assert!(cur.next_block().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn raw_cursor_skips_protocol_chatter() {
        use crate::tcp::protocol::{ProfileInfo, Progress};
        let (tx, rx) = mpsc::channel(8);
        let mut cur = TcpRawCursor::from_receiver(rx);
        tx.send(Ok(ServerPacket::Progress(Progress {
            rows_read: 1,
            bytes_read: 8,
            total_rows_to_read: 0,
            written_rows: 0,
            written_bytes: 0,
        })))
        .await
        .unwrap();
        tx.send(Ok(ServerPacket::ProfileInfo(ProfileInfo {
            rows: 1,
            blocks: 1,
            bytes: 8,
            applied_limit: false,
            rows_before_limit: 0,
        })))
        .await
        .unwrap();
        tx.send(Ok(ServerPacket::EndOfStream)).await.unwrap();
        assert!(cur.next_block().await.unwrap().is_none());
    }
}
