//! Connection facade for the native TCP transport.
//!
//! `NativeConnection` is a thin compatibility wrapper around
//! [`crate::native::connection_actor::OwnedConnection`] / [`ConnectionHandle`]:
//! all I/O lives in the actor's background task, and `NativeConnection`'s
//! public(crate) methods delegate via the handle.
//!
//! The wrapper exists so the rest of the crate (pool, insert, cursor,
//! client, query) keeps the existing `&mut NativeConnection` API
//! surface — but every protocol operation now goes through the actor,
//! which gives us:
//! - protocol-level Cancel on caller-future drop
//! - full-duplex INSERT exception detection
//! - cancel-safe writes (caller's timeout doesn't strand the socket)
//! - reliable EOF / liveness via the actor's read loop (no more
//!   `noop_waker` / `poll_read` poll-trick)
//!
//! The `writer_mut()` / `reader_mut()` raw accessors of the previous
//! design are intentionally removed — they would let callers bypass
//! the actor's serialisation. Cursor and `fetch_string_pairs` now use
//! [`ConnectionHandle::execute_stream`] for streaming results.

use std::net::SocketAddr;

use crate::error::Result;
use crate::native::connection_actor::{ConnectionActor, ConnectionHandle, OwnedConnection};
use crate::native::protocol::{NativeCompressionMethod, ServerHello};
use crate::native::reader::ServerPacket;
use crate::native::tcp::MaybeTlsStream;
use tokio::sync::mpsc;

/// TLS configuration for native connections.
///
/// When `None`, a plain TCP connection is used (port 9000 default).
/// When `Some`, the connection is wrapped in TLS (port 9440 default).
#[cfg(feature = "native-tls-rustls")]
pub(crate) type TlsConfig = Option<(
    std::sync::Arc<rustls::ClientConfig>,
    rustls::pki_types::ServerName<'static>,
)>;

#[cfg(not(feature = "native-tls-rustls"))]
pub(crate) type TlsConfig = ();

// Suppress an "unused" warning for MaybeTlsStream when the file is
// compiled without native-tls-rustls (the actor uses it directly via
// the tcp module).
#[allow(dead_code)]
type _SuppressUnusedMaybeTlsStream = MaybeTlsStream;

/// A single native TCP connection to ClickHouse, backed by the
/// background-task connection actor. All protocol operations delegate
/// to the actor via its [`ConnectionHandle`].
pub(crate) struct NativeConnection {
    /// RAII owner of the actor + reader sub-task. Drops abort cleanly
    /// when this NativeConnection is dropped.
    owned: OwnedConnection,
    /// Cheap-clone handle for the protocol method delegations.
    handle: ConnectionHandle,
    /// Cached at open time so `compression()` doesn't need to round-trip
    /// to the actor.
    compression: NativeCompressionMethod,
}

impl NativeConnection {
    /// Connect, perform the handshake, and spawn the background actor.
    pub(crate) async fn open(
        addr: &SocketAddr,
        database: &str,
        username: &str,
        password: &str,
        compression: NativeCompressionMethod,
        settings: Vec<(String, String)>,
        tls: &TlsConfig,
    ) -> Result<Self> {
        let owned = ConnectionActor::open(
            addr,
            database,
            username,
            password,
            compression,
            settings,
            tls,
        )
        .await?;
        let handle = owned.handle();
        Ok(Self {
            owned,
            handle,
            compression,
        })
    }

    /// True when the underlying actor has been marked broken or has exited.
    /// Used by the pool's recycle hook.
    #[allow(dead_code)] // Pool recycler uses check_alive(); this is the symmetric query
    pub(crate) fn is_poisoned(&self) -> bool {
        !self.handle.is_alive()
    }

    /// Pool-recycle liveness check.
    ///
    /// Returns `true` when the actor is still running and the
    /// connection has not been poisoned. Replaces the previous
    /// `noop_waker` + `poll_read` poll-trick — the actor's read loop
    /// is the canonical source of truth for socket health.
    pub(crate) fn check_alive(&mut self) -> bool {
        self.handle.is_alive()
    }

    /// Get the server hello info (immutable after handshake).
    #[allow(unused)]
    pub(crate) fn server_hello(&self) -> &ServerHello {
        self.handle.server_hello()
    }

    /// Negotiated server revision.
    pub(crate) fn server_revision(&self) -> u64 {
        self.handle.server_revision()
    }

    /// Compression method in use.
    pub(crate) fn compression(&self) -> NativeCompressionMethod {
        self.compression
    }

    /// Mark this connection as broken so the pool drops it on recycle.
    /// Idempotent.
    pub(crate) fn poison(&mut self) {
        self.handle.poison();
    }

    /// Activate ClickHouse roles for this session via `SET ROLE`.
    ///
    /// Called once per new connection by the pool manager, immediately
    /// after the handshake. Role names are backtick-escaped to prevent
    /// injection.
    pub(crate) async fn set_roles(&mut self, roles: &[String]) -> Result<()> {
        debug_assert!(!roles.is_empty(), "set_roles called with empty slice");
        let mut sql = String::from("SET ROLE ");
        for (i, role) in roles.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            crate::sql::escape::identifier(role, &mut sql)
                .expect("fmt::Write on String is infallible");
        }
        self.execute_query(&sql).await
    }

    /// Execute a query and read all response packets until EndOfStream.
    #[allow(dead_code)] // Convenience wrapper over execute_query_with
    pub(crate) async fn execute_query(&mut self, query: &str) -> Result<()> {
        self.handle.execute_query("", query, &[]).await
    }

    /// Execute a query with an explicit query ID and per-query settings.
    ///
    /// `query_id` is sent verbatim in the query packet header; pass `""`
    /// to let the server generate its own ID. `extra_settings` are
    /// merged over the connection-level settings.
    pub(crate) async fn execute_query_with(
        &mut self,
        query_id: &str,
        query: &str,
        extra_settings: &[(String, String)],
    ) -> Result<()> {
        self.handle
            .execute_query(query_id, query, extra_settings)
            .await
    }

    /// Begin an INSERT operation. Returns the column headers
    /// `(name, type_name)` declared by the server.
    pub(crate) async fn begin_insert(&mut self, query: &str) -> Result<Vec<(String, String)>> {
        self.handle.begin_insert(query).await
    }

    /// Send one data block during an INSERT.
    ///
    /// `column_bytes` must be produced by [`crate::native::encode::encode_columns`].
    pub(crate) async fn send_insert_block(
        &mut self,
        column_bytes: &[u8],
        num_columns: usize,
        num_rows: usize,
    ) -> Result<()> {
        // The actor's command takes owned bytes (Send across mpsc).
        // For now we copy; future enhancement: take Bytes / Arc<[u8]>
        // through the channel to avoid this allocation in the hot path.
        self.handle
            .send_insert_block(column_bytes.to_vec(), num_columns, num_rows)
            .await
    }

    /// Finish an INSERT: send the empty terminator, drain to EndOfStream.
    pub(crate) async fn finish_insert(&mut self) -> Result<()> {
        self.handle.finish_insert().await
    }

    /// Send ping and wait for pong.
    pub(crate) async fn ping(&mut self) -> Result<()> {
        self.handle.ping().await
    }

    /// Begin a streaming SELECT. Returns the receive end of an mpsc
    /// channel that the actor will push every received `ServerPacket`
    /// into until EndOfStream or Exception.
    ///
    /// **Cancel-on-drop:** drop the returned receiver to abort the
    /// stream — the actor sends the protocol Cancel packet, drains to
    /// EndOfStream, and the connection stays usable in the pool.
    pub(crate) async fn execute_stream(
        &mut self,
        query_id: &str,
        query: &str,
        extra_settings: &[(String, String)],
        capacity: usize,
    ) -> Result<mpsc::Receiver<Result<ServerPacket>>> {
        self.handle
            .execute_stream(query_id, query, extra_settings, capacity)
            .await
    }

    /// Take ownership of the underlying [`OwnedConnection`] for
    /// explicit shutdown. Consumes the wrapper.
    ///
    /// Currently only used by tests; production code lets the wrapper
    /// drop and the actor exit naturally.
    #[allow(dead_code)]
    pub(crate) fn into_owned(self) -> OwnedConnection {
        self.owned
    }
}
