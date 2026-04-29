//! Row cursor for native protocol query results.
//!
//! Reads native data blocks, transposes columnar data to RowBinary format,
//! and deserializes rows using the existing `rowbinary::deserialize_row` machinery.

use std::collections::VecDeque;
use std::marker::PhantomData;

use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::native::callbacks::QueryCallbacks;
use crate::native::client::NativeClient;
use crate::native::pool::PooledConnection;
use crate::native::reader::ServerPacket;
use crate::row::{RowOwned, RowRead};
use crate::rowbinary;

/// Per-cursor packet buffer capacity. Bigger = more read-ahead;
/// smaller = tighter backpressure. 64 covers typical SELECT
/// Data/Progress interleaving without stalling.
const STREAM_CAPACITY: usize = 64;

/// A cursor that emits owned deserialized rows from a native TCP query.
///
/// `T` must be [`RowOwned`] -- i.e., the deserialized value must not borrow from
/// the network buffer.  This covers the vast majority of use cases.
pub struct NativeRowCursor<T: RowOwned + RowRead> {
    client: NativeClient,
    sql: String,
    /// Query ID to send in the query packet (`""` -> server generates one).
    query_id: String,
    /// Merged settings (client-level + per-query overrides) for this cursor.
    settings: Vec<(String, String)>,
    /// Observability callbacks invoked as packets arrive from the server.
    callbacks: QueryCallbacks,
    /// Buffered row bytes from already-received blocks.
    row_buf: VecDeque<Vec<u8>>,
    state: CursorState,
    _marker: PhantomData<fn() -> T>,
}

enum CursorState {
    /// Initial state -- connection not yet acquired from pool.
    NotStarted,
    /// Stream open. The mpsc receiver is fed by the connection
    /// actor's read loop. The PooledConnection is held here too so
    /// the pool ownership stays correct for the cursor's lifetime —
    /// dropping the cursor drops the receiver (actor sends Cancel +
    /// drains) AND the connection (returns to pool clean).
    Reading {
        rx: mpsc::Receiver<Result<ServerPacket>>,
        // Held for RAII pool return; not read after construction.
        _conn: Box<PooledConnection>,
    },
    /// EndOfStream received -- no more data.
    Done,
}

// No explicit Drop needed any more — the actor's cancel-on-drop
// semantics handle cleanup. When CursorState::Reading drops:
// 1. The mpsc Receiver drops -> actor sees results.is_closed() ->
//    sends protocol Cancel + drains to EndOfStream.
// 2. The PooledConnection drops -> deadpool returns it to the pool.
// 3. Pool's recycle hook checks is_alive() -> still true -> reused.
//
// This replaces the previous "discard the connection on partial read"
// pattern with cleanup that keeps the connection alive for the next
// caller.

impl<T: RowOwned + RowRead> NativeRowCursor<T> {
    pub(crate) fn new(
        client: NativeClient,
        sql: String,
        query_id: String,
        settings: Vec<(String, String)>,
        callbacks: QueryCallbacks,
    ) -> Self {
        Self {
            client,
            sql,
            query_id,
            settings,
            callbacks,
            row_buf: VecDeque::new(),
            state: CursorState::NotStarted,
            _marker: PhantomData,
        }
    }

    /// Consume all remaining packets until `EndOfStream`, allowing the
    /// underlying connection to be returned to the pool in a clean state.
    ///
    /// With the actor-backed cursor this is no longer strictly required
    /// — dropping the cursor mid-stream triggers actor-side Cancel + drain
    /// automatically. `drain` is kept for callers that want to know the
    /// stream finished cleanly (and for `fetch_one` which prefers
    /// explicit drain to avoid the Cancel round-trip).
    pub(crate) async fn drain(&mut self) -> Result<()> {
        loop {
            match &mut self.state {
                CursorState::Done | CursorState::NotStarted => return Ok(()),
                CursorState::Reading { rx, .. } => match rx.recv().await {
                    Some(Ok(ServerPacket::EndOfStream)) | None => {
                        self.state = CursorState::Done;
                        return Ok(());
                    }
                    Some(Ok(ServerPacket::Exception(err))) => {
                        self.state = CursorState::Done;
                        return Err(Error::BadResponse(err.to_string()));
                    }
                    Some(Ok(_)) => continue, // discard Data / Progress / ProfileInfo
                    Some(Err(e)) => {
                        self.state = CursorState::Done;
                        return Err(e);
                    }
                },
            }
        }
    }

    /// Return the next deserialized row, or `None` at end of stream.
    ///
    /// `T` must be [`RowOwned`], meaning the result does not borrow from
    /// the network buffer. This is required for correctness with async streaming.
    pub async fn next(&mut self) -> Result<Option<T>> {
        loop {
            // Return a buffered row if available.
            if let Some(row_bytes) = self.row_buf.pop_front() {
                let mut slice: &[u8] = &row_bytes;
                let value = rowbinary::deserialize_row::<T>(&mut slice, None)?;
                return Ok(Some(value));
            }

            match &mut self.state {
                CursorState::Done => return Ok(None),

                CursorState::NotStarted => {
                    let mut conn = self.client.acquire().await?;
                    let rx = conn
                        .execute_stream(&self.query_id, &self.sql, &self.settings, STREAM_CAPACITY)
                        .await?;
                    self.state = CursorState::Reading {
                        rx,
                        _conn: Box::new(conn),
                    };
                }

                CursorState::Reading { rx, .. } => {
                    match rx.recv().await {
                        Some(Ok(ServerPacket::EndOfStream)) => {
                            self.state = CursorState::Done;
                            // Connection auto-returns to pool when state drops.
                        }
                        Some(Ok(ServerPacket::Data(block))) => {
                            if block.num_rows > 0 {
                                self.row_buf.extend(block.row_data);
                            }
                        }
                        Some(Ok(ServerPacket::Exception(err))) => {
                            // Actor has already drained the stream on its side;
                            // connection stays alive in the pool.
                            self.state = CursorState::Done;
                            return Err(Error::BadResponse(err.to_string()));
                        }
                        Some(Ok(ServerPacket::Progress(p))) => {
                            if let Some(cb) = &self.callbacks.on_progress {
                                cb(&p);
                            }
                        }
                        Some(Ok(ServerPacket::ProfileInfo(pi))) => {
                            if let Some(cb) = &self.callbacks.on_profile_info {
                                cb(&pi);
                            }
                        }
                        Some(Ok(_)) => {} // ignore other packet types
                        Some(Err(e)) => {
                            // I/O error from the actor's read loop —
                            // surface and end the cursor.
                            self.state = CursorState::Done;
                            return Err(e);
                        }
                        None => {
                            // Channel closed unexpectedly (actor exited).
                            self.state = CursorState::Done;
                            return Err(Error::Custom("connection actor closed mid-stream".into()));
                        }
                    }
                }
            }
        }
    }
}
