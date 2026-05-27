//! Native columnar block decoder.
//!
//! Inverse of [`crate::native::encode_columns`]. Reads a Native-format
//! data block off any [`ClickHouseRead`] source and returns a
//! [`DecodedBlock`] -- one [`DecodedColumn`] per declared column, with
//! typed value buffers for every scalar variant and recursive child
//! columns for Nullable / Array / Tuple / Map / LowCardinality.
//!
//! # Why a runtime-typed container?
//!
//! Phase 2's [`crate::native::columns::read_column`] already decodes
//! every column type, but it returns per-row RowBinary bytes -- a
//! shape optimised for piping into upstream's row-oriented
//! `rowbinary::deserialize_row` machinery. The streaming-SELECT cursor
//! over TCP wants the inverse: a columnar container that callers can
//! iterate row by row WITHOUT a second transpose pass. `DecodedBlock`
//! is that container. It coexists with `read_column`; both reach the
//! same bytes on the wire but bridge to different downstream shapes.
//!
//! # Mirror of `NativeReader.cpp`
//!
//! The bytes-on-wire follow [`NativeReader.cpp`] on `upstream/master`
//! and Phase 2's [`crate::native::encode::encode_columns`]. Round-trip
//! property tests (one per supported type) sit next to this module
//! and pin the bytes against the encoder.
//!
//! # Scope of v1
//!
//! Decoded types:
//!
//! - Numerics: `UInt8/16/32/64/128/256`, `Int8/16/32/64/128/256`,
//!   `Float32/64`, `Bool` (decoded as `UInt8`).
//! - `Decimal32/64/128/256` (backing little-endian integer + the
//!   `precision`/`scale` lifted from the type name).
//! - String / FixedString(N).
//! - Date / Date32 / DateTime / DateTime64 (`precision` + optional
//!   `timezone` for DateTime64).
//! - UUID, IPv4, IPv6.
//! - Nullable(T), Array(T), Tuple(T1, ..., Tn), Map(K, V), LowCardinality(T).
//!
//! Out of v1 scope (encoded as `DecodedColumn::Unsupported(type_name)`):
//! Variant, Dynamic, JSON, Time / Time64, BFloat16, geo types. These
//! survive the block-skip path without misaligning the stream pointer
//! because [`crate::native::columns::read_column`] consumes their wire
//! bytes; the decoder tags them Unsupported so the cursor surfaces a
//! clean error if a caller tries to read a value out.
//!
//! [`NativeReader.cpp`]: https://github.com/ClickHouse/ClickHouse/blob/master/src/Formats/NativeReader.cpp

use std::pin::Pin;

use tokio::io::AsyncReadExt;

use crate::error::{Error, Result};
use crate::native::columns::{self, ColumnType};
use crate::native::io::ClickHouseRead;

/// Upper bound on composite-type nesting the decoder will descend.
/// Bounds the recursive `decode_column` so a hostile server announcing
/// `Array(Array(...))` thousands deep cannot drive unbounded
/// (heap-allocated, boxed-future) recursion. This backstops the
/// parse-time depth guard in [`crate::native::columns::ColumnType`]:
/// since types only reach `decode_column` via that parser, the cap is
/// rarely exercised, but it keeps the decoder robust if a deep type is
/// constructed by any other path.
const MAX_DECODE_DEPTH: usize = 32;

/// Fixed-width little-endian scalar that can be bulk-decoded. The blanket
/// numeric column decode reads the whole column's bytes in one
/// `read_exact` and converts them in a single tight loop, instead of
/// one `.await` per element -- the per-element shape forced N await
/// points (and N bounds checks) per N-row column on the streaming-SELECT
/// hot path. On little-endian targets the conversion loop lowers to a
/// `memcpy`; on big-endian it is a vectorisable byte-swap.
trait LeScalar: Sized + Copy {
    const WIDTH: usize;
    fn from_le_slice(bytes: &[u8]) -> Self;
}

macro_rules! impl_le_scalar {
    ($($t:ty),* $(,)?) => {
        $(
            impl LeScalar for $t {
                const WIDTH: usize = std::mem::size_of::<$t>();
                #[inline]
                fn from_le_slice(bytes: &[u8]) -> Self {
                    // `chunks_exact(WIDTH)` guarantees an exact-width slice.
                    <$t>::from_le_bytes(bytes.try_into().expect("chunks_exact yields WIDTH bytes"))
                }
            }
        )*
    };
}

impl_le_scalar!(u16, i16, u32, i32, u64, i64, u128, i128, f32, f64);

/// Read `n` little-endian `T` values in bulk: one `read_exact` of the
/// whole column followed by a single conversion pass.
async fn read_le_column<R: ClickHouseRead, T: LeScalar>(r: &mut R, n: usize) -> Result<Vec<T>> {
    let total = n.checked_mul(T::WIDTH).ok_or_else(|| {
        Error::BadResponse("tcp: numeric column byte length overflows usize".into())
    })?;
    let mut raw = vec![0u8; total];
    r.read_exact(&mut raw).await?;
    Ok(raw.chunks_exact(T::WIDTH).map(T::from_le_slice).collect())
}

/// Read `n` raw 32-byte little-endian values in bulk (the backing store
/// for `Int256`/`UInt256`/`Decimal256`, which have no native Rust
/// scalar). One `read_exact` + a chunked copy.
async fn read_u256_column<R: ClickHouseRead>(r: &mut R, n: usize) -> Result<Vec<[u8; 32]>> {
    let total = n.checked_mul(32).ok_or_else(|| {
        Error::BadResponse("tcp: 256-bit column byte length overflows usize".into())
    })?;
    let mut raw = vec![0u8; total];
    r.read_exact(&mut raw).await?;
    Ok(raw
        .chunks_exact(32)
        .map(|c| {
            let mut a = [0u8; 32];
            a.copy_from_slice(c);
            a
        })
        .collect())
}

/// One column of a decoded Native block, runtime-typed.
///
/// Composite variants (Nullable / Array / Tuple / Map / LowCardinality)
/// hold their child columns boxed so the enum stays a small, recursive
/// owned value -- the same shape `ColumnType` uses upstream of it.
///
/// Numeric variants hold contiguous `Vec<T>` buffers ready for
/// per-row indexing; this is the simplest shape that preserves the
/// columnar layout the wire produced. Specialised SIMD or pooled
/// allocators are deferred follow-ups; the layout is amenable.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum DecodedColumn {
    // Numeric scalars.
    UInt8(Vec<u8>),
    UInt16(Vec<u16>),
    UInt32(Vec<u32>),
    UInt64(Vec<u64>),
    Int8(Vec<i8>),
    Int16(Vec<i16>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Int128(Vec<i128>),
    UInt128(Vec<u128>),
    /// 256-bit integers carried as raw little-endian 32-byte values
    /// (Rust has no native i256/u256). Callers interpret as needed.
    Int256(Vec<[u8; 32]>),
    UInt256(Vec<[u8; 32]>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),

    /// `Decimal(P, S)` decoded as its backing little-endian integer
    /// (32/64/128/256-bit by precision). `precision` (total digits) and
    /// `scale` (fractional digits) are carried from the column type --
    /// the value is `backing / 10^scale`. They are NOT on the wire; the
    /// decoder lifts them from the parsed `ColumnType`.
    Decimal32 { precision: u8, scale: u8, values: Vec<i32> },
    Decimal64 { precision: u8, scale: u8, values: Vec<i64> },
    Decimal128 { precision: u8, scale: u8, values: Vec<i128> },
    Decimal256 { precision: u8, scale: u8, values: Vec<[u8; 32]> },

    // Variable-length scalars.
    String(Vec<Vec<u8>>),
    FixedString { width: usize, bytes: Vec<u8> },

    // Date / time.
    Date(Vec<u16>),
    /// `Date32`: signed days since the Unix epoch.
    Date32(Vec<i32>),
    DateTime(Vec<u32>),
    /// `DateTime64`: Int64 ticks at `precision` sub-second digits, with the
    /// optional IANA `timezone` from the type name. Both come from the
    /// parsed `ColumnType`, not the wire.
    DateTime64 {
        precision: u8,
        timezone: Option<String>,
        values: Vec<i64>,
    },

    // Network.
    Uuid(Vec<[u8; 16]>),
    Ipv4(Vec<u32>),
    Ipv6(Vec<[u8; 16]>),

    // Composites.
    /// `mask[i] == 1` => row `i` is null; child[i] still exists with a
    /// placeholder value (matching the Native wire shape).
    Nullable {
        mask: Vec<u8>,
        child: Box<DecodedColumn>,
    },
    /// Cumulative end-offsets; row `i` spans `child[offsets[i-1]..offsets[i]]`.
    Array {
        offsets: Vec<u64>,
        child: Box<DecodedColumn>,
    },
    /// Per-block dictionary + per-row indices. Resolved at access time;
    /// the cursor narrows to `child[indices[row]]` when iterating.
    LowCardinality {
        dict: Box<DecodedColumn>,
        indices: Box<DecodedColumn>,
        /// True when the LC inner type is `Nullable(T)`. Dictionary
        /// index 0 then represents NULL.
        is_nullable_inner: bool,
    },
    Tuple(Vec<DecodedColumn>),
    Map {
        offsets: Vec<u64>,
        keys: Box<DecodedColumn>,
        values: Box<DecodedColumn>,
    },

    /// Type recognised by the parser but not decoded by v1. The wire
    /// bytes have been consumed (so the stream pointer is correct);
    /// per-row access on this variant returns an error.
    Unsupported(String),
}

impl DecodedColumn {
    /// Number of logical rows this column carries.
    ///
    /// For composite variants the count is the OUTER row count, not
    /// the cumulative child-element count.
    #[must_use]
    pub fn row_count(&self) -> usize {
        match self {
            Self::UInt8(v) => v.len(),
            Self::UInt16(v) => v.len(),
            Self::UInt32(v) => v.len(),
            Self::UInt64(v) => v.len(),
            Self::Int8(v) => v.len(),
            Self::Int16(v) => v.len(),
            Self::Int32(v) => v.len(),
            Self::Int64(v) => v.len(),
            Self::Int128(v) => v.len(),
            Self::UInt128(v) => v.len(),
            Self::Int256(v) => v.len(),
            Self::UInt256(v) => v.len(),
            Self::Float32(v) => v.len(),
            Self::Float64(v) => v.len(),
            Self::Decimal32 { values, .. } => values.len(),
            Self::Decimal64 { values, .. } => values.len(),
            Self::Decimal128 { values, .. } => values.len(),
            Self::Decimal256 { values, .. } => values.len(),
            Self::String(v) => v.len(),
            Self::FixedString { width, bytes } => {
                if *width == 0 {
                    0
                } else {
                    bytes.len() / *width
                }
            }
            Self::Date(v) => v.len(),
            Self::Date32(v) => v.len(),
            Self::DateTime(v) => v.len(),
            Self::DateTime64 { values, .. } => values.len(),
            Self::Uuid(v) => v.len(),
            Self::Ipv4(v) => v.len(),
            Self::Ipv6(v) => v.len(),
            Self::Nullable { mask, .. } => mask.len(),
            Self::Array { offsets, .. } => offsets.len(),
            Self::LowCardinality { indices, .. } => indices.row_count(),
            Self::Tuple(fields) => fields.first().map_or(0, Self::row_count),
            Self::Map { offsets, .. } => offsets.len(),
            // Unsupported columns surface as zero rows so callers that
            // ignore the variant don't loop over uninitialised state.
            Self::Unsupported(_) => 0,
        }
    }
}

/// A decoded Native data block: ordered list of columns plus the
/// `(name, type_name)` schema the server announced.
///
/// The schema is copied per-block so a single cursor can survive the
/// rare server-side schema renegotiation that mid-stream INSERT-SELECT
/// can emit. Cost is small (handful of strings per block); we re-tag
/// every block rather than thread a per-cursor schema slot.
#[derive(Debug, Clone)]
pub struct DecodedBlock {
    pub columns: Vec<DecodedColumn>,
    pub schema: Vec<(String, String)>,
    pub num_rows: u64,
}

/// Read one Native-format data block body off the wire.
///
/// Stream pointer position on entry MUST be immediately after the
/// `num_columns` + `num_rows` varuint pair the caller already consumed
/// from the Data packet header. The decoder consumes exactly the
/// column-payload bytes for `num_columns` columns at `num_rows` rows;
/// on return the stream pointer is aligned for the next packet ID.
///
/// `server_revision` decides whether the per-column custom-serialization
/// flag byte is on the wire -- 25.x servers are always above the gate.
///
/// # Errors
///
/// - [`Error::BadResponse`] if a column header is malformed (varuint
///   overflow, length cap exceeded) or a composite-column offset list
///   is non-monotonic.
/// - I/O errors from the underlying reader propagate untouched.
pub(crate) async fn decode_block<R: ClickHouseRead>(
    r: &mut R,
    num_columns: u64,
    num_rows: u64,
    server_revision: u64,
) -> Result<DecodedBlock> {
    let has_custom_ser = server_revision
        >= crate::native::encode::DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION;

    let mut schema = Vec::with_capacity(usize::try_from(num_columns).unwrap_or(0));
    let mut columns = Vec::with_capacity(usize::try_from(num_columns).unwrap_or(0));

    for _ in 0..num_columns {
        let name = r.read_utf8_string().await?;
        let type_name = r.read_utf8_string().await?;
        if has_custom_ser {
            // Custom-serialization flag byte (0 = normal serialisation).
            // We always treat columns as normal-serialised; a non-zero
            // flag from a future server build would silently misalign
            // the body bytes. Surface a clean error rather than risk
            // a wedged stream pointer.
            let flag = r.read_u8().await?;
            if flag != 0 {
                return Err(Error::BadResponse(format!(
                    "tcp: column '{name}' uses custom serialization flag {flag} \
                     -- only normal (0) is supported"
                )));
            }
        }

        let col = match ColumnType::parse(&type_name) {
            Some(ct) => decode_column(r, &ct, num_rows, server_revision, 0).await?,
            None => {
                // Unknown type -- we can't advance the stream pointer
                // past it without knowing its size. Treat as a protocol
                // disagreement and surface so the actor poisons the
                // connection. Letting it slide would misalign every
                // subsequent packet.
                return Err(Error::BadResponse(format!(
                    "tcp: server announced unknown column type '{type_name}' for column '{name}'"
                )));
            }
        };

        schema.push((name, type_name));
        columns.push(col);
    }

    Ok(DecodedBlock {
        columns,
        schema,
        num_rows,
    })
}

/// Decode one column's values into a [`DecodedColumn`].
///
/// Recursive over composite types; the recursive call sites use
/// [`Box::pin`] to break the async-fn-cycle Rust would otherwise
/// reject for infinite future size (same pattern Phase 2's
/// [`crate::native::columns::read_column`] uses). `depth` bounds that
/// recursion at [`MAX_DECODE_DEPTH`] against hostile deep nesting.
fn decode_column<'a, R: ClickHouseRead + 'a>(
    r: &'a mut R,
    col_type: &'a ColumnType,
    num_rows: u64,
    server_revision: u64,
    depth: usize,
) -> Pin<Box<dyn std::future::Future<Output = Result<DecodedColumn>> + Send + 'a>> {
    Box::pin(async move {
        if depth > MAX_DECODE_DEPTH {
            return Err(Error::BadResponse(format!(
                "tcp: column type nesting exceeds {MAX_DECODE_DEPTH} levels"
            )));
        }

        let n = usize::try_from(num_rows).map_err(|_| {
            Error::BadResponse(format!(
                "tcp: row count {num_rows} exceeds platform usize"
            ))
        })?;

        match col_type {
            ColumnType::UInt8 | ColumnType::Enum8 => {
                let mut buf = vec![0u8; n];
                r.read_exact(&mut buf).await?;
                Ok(DecodedColumn::UInt8(buf))
            }
            ColumnType::Int8 => {
                let mut raw = vec![0u8; n];
                r.read_exact(&mut raw).await?;
                // Reinterpret as i8 without an extra copy: i8 and u8
                // have identical layout, the cast is lossless and the
                // Vec capacity matches.
                let buf: Vec<i8> = raw.into_iter().map(|b| b as i8).collect();
                Ok(DecodedColumn::Int8(buf))
            }
            ColumnType::UInt16 | ColumnType::Enum16 => {
                Ok(DecodedColumn::UInt16(read_le_column::<_, u16>(r, n).await?))
            }
            ColumnType::Int16 => {
                Ok(DecodedColumn::Int16(read_le_column::<_, i16>(r, n).await?))
            }
            ColumnType::UInt32 => {
                Ok(DecodedColumn::UInt32(read_le_column::<_, u32>(r, n).await?))
            }
            ColumnType::Int32 => {
                Ok(DecodedColumn::Int32(read_le_column::<_, i32>(r, n).await?))
            }
            ColumnType::UInt64 => {
                Ok(DecodedColumn::UInt64(read_le_column::<_, u64>(r, n).await?))
            }
            ColumnType::Int64 => {
                Ok(DecodedColumn::Int64(read_le_column::<_, i64>(r, n).await?))
            }
            ColumnType::Int128 => {
                Ok(DecodedColumn::Int128(read_le_column::<_, i128>(r, n).await?))
            }
            ColumnType::UInt128 => {
                Ok(DecodedColumn::UInt128(read_le_column::<_, u128>(r, n).await?))
            }
            ColumnType::Int256 => Ok(DecodedColumn::Int256(read_u256_column(r, n).await?)),
            ColumnType::UInt256 => Ok(DecodedColumn::UInt256(read_u256_column(r, n).await?)),
            ColumnType::Float32 => {
                Ok(DecodedColumn::Float32(read_le_column::<_, f32>(r, n).await?))
            }
            ColumnType::Float64 => {
                Ok(DecodedColumn::Float64(read_le_column::<_, f64>(r, n).await?))
            }
            // Decimal(P, S) wire format is its backing little-endian
            // integer (32/64/128/256-bit by precision); P + S are carried
            // on the parsed ColumnType (lifted from the type name), not the
            // wire bytes -- surface them so callers recover the rational
            // value as `backing / 10^scale` without re-parsing the schema.
            ColumnType::Decimal32 { precision, scale } => Ok(DecodedColumn::Decimal32 {
                precision: *precision,
                scale: *scale,
                values: read_le_column::<_, i32>(r, n).await?,
            }),
            ColumnType::Decimal64 { precision, scale } => Ok(DecodedColumn::Decimal64 {
                precision: *precision,
                scale: *scale,
                values: read_le_column::<_, i64>(r, n).await?,
            }),
            ColumnType::Decimal128 { precision, scale } => Ok(DecodedColumn::Decimal128 {
                precision: *precision,
                scale: *scale,
                values: read_le_column::<_, i128>(r, n).await?,
            }),
            ColumnType::Decimal256 { precision, scale } => Ok(DecodedColumn::Decimal256 {
                precision: *precision,
                scale: *scale,
                values: read_u256_column(r, n).await?,
            }),
            ColumnType::String | ColumnType::Json => {
                let mut buf = Vec::with_capacity(n);
                for _ in 0..n {
                    // read_string applies the MAX_STRING_SIZE cap.
                    buf.push(r.read_string().await?);
                }
                Ok(DecodedColumn::String(buf))
            }
            ColumnType::FixedString(width) => {
                let total = width
                    .checked_mul(n)
                    .ok_or_else(|| Error::BadResponse("tcp: FixedString block overflow".into()))?;
                let mut bytes = vec![0u8; total];
                r.read_exact(&mut bytes).await?;
                Ok(DecodedColumn::FixedString {
                    width: *width,
                    bytes,
                })
            }
            ColumnType::Date => Ok(DecodedColumn::Date(read_le_column::<_, u16>(r, n).await?)),
            ColumnType::Date32 => {
                Ok(DecodedColumn::Date32(read_le_column::<_, i32>(r, n).await?))
            }
            ColumnType::DateTime => {
                Ok(DecodedColumn::DateTime(read_le_column::<_, u32>(r, n).await?))
            }
            ColumnType::DateTime64 {
                precision,
                timezone,
            } => {
                // Wire format is identical to Int64 (raw ticks at the
                // configured precision). Precision + timezone ride on the
                // parsed ColumnType (lifted from the type name), not the
                // payload bytes -- surface both so callers can format
                // ticks without re-parsing the schema string.
                Ok(DecodedColumn::DateTime64 {
                    precision: *precision,
                    timezone: timezone.clone(),
                    values: read_le_column::<_, i64>(r, n).await?,
                })
            }
            ColumnType::Uuid => {
                let mut buf = Vec::with_capacity(n);
                for _ in 0..n {
                    let mut slot = [0u8; 16];
                    r.read_exact(&mut slot).await?;
                    buf.push(slot);
                }
                Ok(DecodedColumn::Uuid(buf))
            }
            ColumnType::IPv4 => Ok(DecodedColumn::Ipv4(read_le_column::<_, u32>(r, n).await?)),
            ColumnType::IPv6 => {
                let mut buf = Vec::with_capacity(n);
                for _ in 0..n {
                    let mut slot = [0u8; 16];
                    r.read_exact(&mut slot).await?;
                    buf.push(slot);
                }
                Ok(DecodedColumn::Ipv6(buf))
            }
            ColumnType::Nullable(inner) => {
                let mut mask = vec![0u8; n];
                r.read_exact(&mut mask).await?;
                let child = decode_column(r, inner, num_rows, server_revision, depth + 1).await?;
                Ok(DecodedColumn::Nullable {
                    mask,
                    child: Box::new(child),
                })
            }
            ColumnType::Array(inner) => {
                let mut offsets = Vec::with_capacity(n);
                let mut prev: u64 = 0;
                for _ in 0..n {
                    let end = r.read_u64_le().await?;
                    if end < prev {
                        return Err(Error::BadResponse(
                            "tcp: Array column offsets are not monotonically increasing".into(),
                        ));
                    }
                    prev = end;
                    offsets.push(end);
                }
                let total = offsets.last().copied().unwrap_or(0);
                let child = decode_column(r, inner, total, server_revision, depth + 1).await?;
                Ok(DecodedColumn::Array {
                    offsets,
                    child: Box::new(child),
                })
            }
            ColumnType::Tuple(fields) => {
                let mut decoded_fields = Vec::with_capacity(fields.len());
                for field in fields {
                    decoded_fields
                        .push(decode_column(r, field, num_rows, server_revision, depth + 1).await?);
                }
                Ok(DecodedColumn::Tuple(decoded_fields))
            }
            ColumnType::Map(key_type, val_type) => {
                let mut offsets = Vec::with_capacity(n);
                let mut prev: u64 = 0;
                for _ in 0..n {
                    let end = r.read_u64_le().await?;
                    if end < prev {
                        return Err(Error::BadResponse(
                            "tcp: Map column offsets are not monotonically increasing".into(),
                        ));
                    }
                    prev = end;
                    offsets.push(end);
                }
                let total = offsets.last().copied().unwrap_or(0);
                let keys = decode_column(r, key_type, total, server_revision, depth + 1).await?;
                let values = decode_column(r, val_type, total, server_revision, depth + 1).await?;
                Ok(DecodedColumn::Map {
                    offsets,
                    keys: Box::new(keys),
                    values: Box::new(values),
                })
            }
            ColumnType::LowCardinality(inner) => {
                decode_low_cardinality(r, inner, num_rows, server_revision, depth + 1).await
            }
            ColumnType::SimpleAggregateFunction(inner) => {
                // Wire-compatible with the inner type T.
                decode_column(r, inner, num_rows, server_revision, depth + 1).await
            }
            // Everything else falls back to the Phase 2 read_column
            // path so the wire bytes are consumed (the stream pointer
            // stays aligned), but the rows aren't materialised into a
            // typed `DecodedColumn` variant yet. Accessing rows on the
            // resulting `Unsupported` variant surfaces an error rather
            // than silently returning a placeholder.
            other => {
                let _consumed = columns::read_column(r, other, num_rows).await?;
                Ok(DecodedColumn::Unsupported(format!("{other:?}")))
            }
        }
    })
}

/// LowCardinality column wire shape -- per-block dictionary + indices.
/// Mirrors Phase 2's encoder layout (which itself matches what the
/// server emits) byte-for-byte.
///
/// ```text
/// u64    version (== 1)
/// u64    flags  (bit 0-1: index size code; bit 8: NEED_GLOBAL_DICTIONARY;
///                bit 9: HAS_ADDITIONAL_KEYS)
/// optional u64 global_dict_size + values
/// optional u64 additional_keys_size + values
/// u64    num_indices  (== num_rows)
/// num_rows x index_byte_width  (1/2/4/8 depending on bits 0-1)
/// ```
async fn decode_low_cardinality<R: ClickHouseRead>(
    r: &mut R,
    inner: &ColumnType,
    num_rows: u64,
    server_revision: u64,
    depth: usize,
) -> Result<DecodedColumn> {
    let n = usize::try_from(num_rows).map_err(|_| {
        Error::BadResponse(format!("tcp: row count {num_rows} exceeds platform usize"))
    })?;

    let _version = r.read_u64_le().await?;
    let flags = r.read_u64_le().await?;
    let index_type = (flags & 0x03) as u8;
    let has_global_dict = (flags & 0x100) != 0;
    let has_additional_keys = (flags & 0x200) != 0;
    // Bit 10 (0x400, NeedUpdateDictionary) signals that the client
    // should discard any dictionary cached across blocks. This decoder
    // materialises the dictionary per block (it keeps no cross-block
    // state), so the bit is a no-op here and is intentionally not
    // consulted. A future cross-block dictionary-reuse optimisation
    // would have to honour it.

    let (dict_type, is_nullable_inner) = if let ColumnType::Nullable(t) = inner {
        (t.as_ref(), true)
    } else {
        (inner, false)
    };

    // Client-server Native LowCardinality payloads carry per-block
    // additional keys only: the server sets HAS_ADDITIONAL_KEYS and does
    // NOT send a shared global dictionary (global dicts are an internal
    // merge-tree concern). cpp-client and clickhouse-go both reject a
    // global dictionary here and require the additional-keys bit; the
    // Phase-2 `crate::native::columns` reader applies the same rule.
    // Accept only the additional-keys shape and fail loud on anything
    // else (a global dict, or neither flag), rather than mis-decode a
    // payload no current server emits.
    if has_global_dict {
        return Err(Error::BadResponse(
            "tcp: LowCardinality global dictionary is not supported on the client-server \
             path (only per-block additional keys)"
                .into(),
        ));
    }
    if !has_additional_keys {
        return Err(Error::BadResponse(
            "tcp: LowCardinality block set neither the additional-keys nor the \
             global-dictionary flag"
                .into(),
        ));
    }
    let additional_keys_size = r.read_u64_le().await?;
    let combined = decode_column(r, dict_type, additional_keys_size, server_revision, depth).await?;

    let num_indices = r.read_u64_le().await?;
    if num_indices != num_rows {
        return Err(Error::BadResponse(format!(
            "tcp: LowCardinality index count {num_indices} != row count {num_rows}"
        )));
    }

    let indices = match index_type {
        0 => {
            let mut buf = vec![0u8; n];
            r.read_exact(&mut buf).await?;
            DecodedColumn::UInt8(buf)
        }
        1 => DecodedColumn::UInt16(read_le_column::<_, u16>(r, n).await?),
        2 => DecodedColumn::UInt32(read_le_column::<_, u32>(r, n).await?),
        3 => DecodedColumn::UInt64(read_le_column::<_, u64>(r, n).await?),
        other => {
            return Err(Error::BadResponse(format!(
                "tcp: LowCardinality index type {other} is not valid (expected 0..=3)"
            )));
        }
    };

    Ok(DecodedColumn::LowCardinality {
        dict: Box::new(combined),
        indices: Box::new(indices),
        is_nullable_inner,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::columns::ColumnType;
    use crate::native::encode::{ColumnSchema, encode_columns};
    use crate::native::io::ClickHouseWrite;
    use std::io::Cursor;

    // Inlined to keep these decoder tests buildable in default (no-`tcp`)
    // feature builds. Mirrors `crate::tcp::protocol::DBMS_TCP_PROTOCOL_VERSION`
    // (= DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS); the decode path here is
    // revision-insensitive for the cases exercised, so the exact value is
    // not load-bearing -- it only stands in for "a current TCP revision".
    const REV: u64 = 54459;

    /// Encode a single-column block via Phase 2's `encode_columns`,
    /// wrap it as a Native block body (num_columns + num_rows +
    /// payload), and return the bytes ready to feed into `decode_block`.
    async fn encode_one_column_block(
        name: &str,
        type_name: &str,
        rows: &[Vec<u8>],
    ) -> Vec<u8> {
        let schema = ColumnSchema::from_headers(&[(name.to_string(), type_name.to_string())])
            .expect("schema parses");
        // The block-body layout the actor's reader sees AFTER the
        // Data packet header has been consumed is just the column-
        // payload bytes; the (num_columns, num_rows) varuint pair
        // lives in the header and decode_block takes those as
        // parameters.
        encode_columns(rows, &schema, REV).expect("encode succeeds")
    }

    async fn decode_via_cursor(bytes: Vec<u8>, num_rows: u64) -> DecodedBlock {
        let mut cur = Cursor::new(bytes);
        decode_block(&mut cur, 1, num_rows, REV).await.unwrap()
    }

    #[tokio::test]
    async fn roundtrip_uint64() {
        let rows: Vec<Vec<u8>> = (0u64..5).map(|v| v.to_le_bytes().to_vec()).collect();
        let bytes = encode_one_column_block("n", "UInt64", &rows).await;
        let block = decode_via_cursor(bytes, 5).await;
        match &block.columns[0] {
            DecodedColumn::UInt64(values) => assert_eq!(values, &vec![0u64, 1, 2, 3, 4]),
            other => panic!("expected UInt64, got {other:?}"),
        }
        assert_eq!(block.schema[0], ("n".to_string(), "UInt64".to_string()));
        assert_eq!(block.num_rows, 5);
    }

    #[tokio::test]
    async fn roundtrip_int32_signed() {
        let rows: Vec<Vec<u8>> = [-3i32, -1, 0, 7, 99]
            .iter()
            .map(|v| v.to_le_bytes().to_vec())
            .collect();
        let bytes = encode_one_column_block("v", "Int32", &rows).await;
        let block = decode_via_cursor(bytes, 5).await;
        match &block.columns[0] {
            DecodedColumn::Int32(values) => assert_eq!(values, &vec![-3i32, -1, 0, 7, 99]),
            other => panic!("expected Int32, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_string() {
        // RowBinary for String: varuint(len) + bytes.
        let strings = ["", "hello", "wide \u{1F600}", "trailing"];
        let rows: Vec<Vec<u8>> = strings
            .iter()
            .map(|s| {
                let mut v = Vec::new();
                let mut len = s.len() as u64;
                loop {
                    let byte = (len & 0x7F) as u8;
                    len >>= 7;
                    if len == 0 {
                        v.push(byte);
                        break;
                    }
                    v.push(byte | 0x80);
                }
                v.extend_from_slice(s.as_bytes());
                v
            })
            .collect();
        let bytes = encode_one_column_block("s", "String", &rows).await;
        let block = decode_via_cursor(bytes, strings.len() as u64).await;
        match &block.columns[0] {
            DecodedColumn::String(values) => {
                let decoded: Vec<&str> = values
                    .iter()
                    .map(|v| std::str::from_utf8(v).unwrap())
                    .collect();
                assert_eq!(decoded, strings);
            }
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_fixed_string() {
        let raw = b"abcdefghij"; // 10 bytes, two rows of width 5
        let rows: Vec<Vec<u8>> = raw.chunks(5).map(<[u8]>::to_vec).collect();
        let bytes = encode_one_column_block("f", "FixedString(5)", &rows).await;
        let block = decode_via_cursor(bytes, 2).await;
        match &block.columns[0] {
            DecodedColumn::FixedString { width, bytes } => {
                assert_eq!(*width, 5);
                assert_eq!(bytes, raw);
            }
            other => panic!("expected FixedString, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_nullable_uint8() {
        // RowBinary Nullable: flag byte (0=value, 1=null) + value ONLY when
        // not null. A null row is the flag byte alone -- no value follows
        // (the encoder's trailing-byte guard rejects a spurious value
        // byte after a null flag). The decoder still zero-fills the null
        // slot, so the child column reads back [42, 0, 7].
        let rows: Vec<Vec<u8>> = vec![
            vec![0, 42], // value 42
            vec![1],     // null (flag only, canonical RowBinary)
            vec![0, 7],  // value 7
        ];
        let bytes = encode_one_column_block("n", "Nullable(UInt8)", &rows).await;
        let block = decode_via_cursor(bytes, 3).await;
        match &block.columns[0] {
            DecodedColumn::Nullable { mask, child } => {
                assert_eq!(mask, &vec![0u8, 1, 0]);
                match child.as_ref() {
                    DecodedColumn::UInt8(values) => {
                        // The middle slot's value is the default (0)
                        // because the encoder zero-fills null slots.
                        assert_eq!(values, &vec![42u8, 0, 7]);
                    }
                    other => panic!("expected UInt8 child, got {other:?}"),
                }
            }
            other => panic!("expected Nullable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_array_uint64() {
        // RowBinary Array<UInt64>: varuint(count) + count x u64 LE.
        let varuint = |mut v: u64, out: &mut Vec<u8>| loop {
            let b = (v & 0x7F) as u8;
            v >>= 7;
            if v == 0 {
                out.push(b);
                break;
            }
            out.push(b | 0x80);
        };
        let arrays: [&[u64]; 3] = [&[1, 2, 3], &[], &[42]];
        let rows: Vec<Vec<u8>> = arrays
            .iter()
            .map(|arr| {
                let mut v = Vec::new();
                varuint(arr.len() as u64, &mut v);
                for x in *arr {
                    v.extend_from_slice(&x.to_le_bytes());
                }
                v
            })
            .collect();
        let bytes = encode_one_column_block("a", "Array(UInt64)", &rows).await;
        let block = decode_via_cursor(bytes, 3).await;
        match &block.columns[0] {
            DecodedColumn::Array { offsets, child } => {
                assert_eq!(offsets, &vec![3u64, 3, 4]);
                match child.as_ref() {
                    DecodedColumn::UInt64(values) => {
                        assert_eq!(values, &vec![1u64, 2, 3, 42]);
                    }
                    other => panic!("expected UInt64 child, got {other:?}"),
                }
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_lowcardinality_string() {
        // RowBinary for LC(String) is just String RowBinary -- the
        // LC dictionary lives only on the Native wire side.
        let varuint = |mut v: u64, out: &mut Vec<u8>| loop {
            let b = (v & 0x7F) as u8;
            v >>= 7;
            if v == 0 {
                out.push(b);
                break;
            }
            out.push(b | 0x80);
        };
        let strings = ["foo", "bar", "foo", "baz", "bar"];
        let rows: Vec<Vec<u8>> = strings
            .iter()
            .map(|s| {
                let mut v = Vec::new();
                varuint(s.len() as u64, &mut v);
                v.extend_from_slice(s.as_bytes());
                v
            })
            .collect();
        let bytes = encode_one_column_block("lc", "LowCardinality(String)", &rows).await;
        let block = decode_via_cursor(bytes, 5).await;
        match &block.columns[0] {
            DecodedColumn::LowCardinality {
                dict,
                indices,
                is_nullable_inner,
            } => {
                assert!(!*is_nullable_inner);
                match dict.as_ref() {
                    DecodedColumn::String(dict_strings) => {
                        // The dictionary order is encoder-implementation-
                        // defined; assert membership rather than order.
                        let mut set: std::collections::HashSet<&[u8]> =
                            dict_strings.iter().map(Vec::as_slice).collect();
                        assert!(set.remove(b"foo".as_slice()));
                        assert!(set.remove(b"bar".as_slice()));
                        assert!(set.remove(b"baz".as_slice()));
                    }
                    other => panic!("expected String dict, got {other:?}"),
                }
                // Cross-check by walking the index column and
                // reconstructing the row strings.
                let reconstructed = lc_to_strings(dict.as_ref(), indices.as_ref());
                assert_eq!(reconstructed, strings);
            }
            other => panic!("expected LowCardinality, got {other:?}"),
        }
    }

    fn lc_to_strings(dict: &DecodedColumn, indices: &DecodedColumn) -> Vec<String> {
        let dict_strings = match dict {
            DecodedColumn::String(s) => s,
            _ => panic!("dict must be String"),
        };
        let idxs: Vec<usize> = match indices {
            DecodedColumn::UInt8(v) => v.iter().map(|&x| x as usize).collect(),
            DecodedColumn::UInt16(v) => v.iter().map(|&x| x as usize).collect(),
            DecodedColumn::UInt32(v) => v.iter().map(|&x| x as usize).collect(),
            DecodedColumn::UInt64(v) => v.iter().map(|&x| x as usize).collect(),
            _ => panic!("indices must be unsigned int"),
        };
        idxs.into_iter()
            .map(|i| String::from_utf8(dict_strings[i].clone()).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn rejects_unknown_column_type() {
        // Hand-craft a single-column block body whose type_name is
        // not a recognised ColumnType.
        let mut bytes = Vec::new();
        bytes.write_string(b"x").await.unwrap();
        bytes.write_string(b"DefinitelyNotAType").await.unwrap();
        // Custom-serialisation flag byte for modern revisions.
        bytes.push(0u8);
        let mut cur = Cursor::new(bytes);
        let err = decode_block(&mut cur, 1, 0, REV).await.unwrap_err();
        match err {
            Error::BadResponse(msg) => {
                assert!(msg.contains("unknown column type"), "got: {msg}");
            }
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn rejects_nonzero_custom_serialization_flag() {
        let mut bytes = Vec::new();
        bytes.write_string(b"x").await.unwrap();
        bytes.write_string(b"UInt8").await.unwrap();
        // Non-zero flag -- not normal serialisation.
        bytes.push(7u8);
        let mut cur = Cursor::new(bytes);
        let err = decode_block(&mut cur, 1, 0, REV).await.unwrap_err();
        match err {
            Error::BadResponse(msg) => {
                assert!(msg.contains("custom serialization flag"), "got: {msg}");
            }
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn row_count_matches_inner_buffers() {
        let rows: Vec<Vec<u8>> = (0u32..8).map(|v| v.to_le_bytes().to_vec()).collect();
        let bytes = encode_one_column_block("v", "UInt32", &rows).await;
        let block = decode_via_cursor(bytes, 8).await;
        assert_eq!(block.columns[0].row_count(), 8);
    }

    #[test]
    fn parse_column_type_via_phase2() {
        // Sanity-check the parser we're reusing: ColumnType::parse is
        // visible from this module and recognises every type we list
        // as v1-supported.
        for ty in [
            "UInt8",
            "UInt64",
            "Int32",
            "String",
            "FixedString(8)",
            "Nullable(UInt32)",
            "Array(UInt64)",
            "Tuple(UInt8, String)",
            "Map(String, UInt64)",
            "LowCardinality(String)",
            "LowCardinality(Nullable(String))",
            "DateTime",
            "DateTime64(3)",
            "UUID",
            "IPv4",
            "IPv6",
        ] {
            assert!(ColumnType::parse(ty).is_some(), "{ty}");
        }
    }

    #[tokio::test]
    async fn decode_column_rejects_excessive_nesting() {
        // Hand-build Array(Array(...Array(UInt8)...)) deeper than the
        // cap, bypassing the parser (which caps separately). decode_column
        // must reject it before recursing without bound. Zero rows means
        // each Array level reads no offset bytes, so an empty reader is
        // enough to drive the recursion to the depth guard.
        let mut ct = ColumnType::UInt8;
        for _ in 0..(MAX_DECODE_DEPTH + 5) {
            ct = ColumnType::Array(Box::new(ct));
        }
        let mut cur = Cursor::new(Vec::new());
        let err = decode_column(&mut cur, &ct, 0, REV, 0).await.unwrap_err();
        match err {
            Error::BadResponse(msg) => assert!(msg.contains("nesting"), "got: {msg}"),
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn lowcardinality_rejects_zero_flags() {
        // A LowCardinality payload with neither the global-dictionary
        // nor additional-keys flag set is a shape no current server
        // emits; the decoder rejects it rather than guessing a layout.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_le_bytes()); // version
        bytes.extend_from_slice(&0u64.to_le_bytes()); // flags = 0
        let ct = ColumnType::LowCardinality(Box::new(ColumnType::String));
        let mut cur = Cursor::new(bytes);
        let err = decode_column(&mut cur, &ct, 1, REV, 0).await.unwrap_err();
        match err {
            Error::BadResponse(msg) => assert!(msg.contains("neither"), "got: {msg}"),
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }

    /// Build a one-column Native block body (name "c", the given type,
    /// the custom-serialization flag byte, then `payload`) and decode
    /// it. REV is above the custom-serialization gate, so the flag byte
    /// is present on the wire.
    async fn decode_one_typed(type_name: &str, num_rows: u64, payload: &[u8]) -> DecodedColumn {
        let mut buf = Vec::new();
        buf.write_string(b"c").await.unwrap();
        buf.write_string(type_name.as_bytes()).await.unwrap();
        buf.push(0u8); // custom-serialization flag
        buf.extend_from_slice(payload);
        let mut cur = Cursor::new(buf);
        let mut block = decode_block(&mut cur, 1, num_rows, REV).await.unwrap();
        block.columns.pop().unwrap()
    }

    fn le_bytes_i128(vals: &[i128]) -> Vec<u8> {
        vals.iter().flat_map(|v| v.to_le_bytes()).collect()
    }

    #[tokio::test]
    async fn roundtrip_int128() {
        let vals = [1i128, -2, i128::MAX, i128::MIN];
        match decode_one_typed("Int128", vals.len() as u64, &le_bytes_i128(&vals)).await {
            DecodedColumn::Int128(v) => assert_eq!(v, vals),
            other => panic!("expected Int128, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_uint128() {
        let vals = [0u128, 1, u128::MAX];
        let payload: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        match decode_one_typed("UInt128", vals.len() as u64, &payload).await {
            DecodedColumn::UInt128(v) => assert_eq!(v, vals),
            other => panic!("expected UInt128, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_int256_raw_le() {
        // Two raw 32-byte LE values: 1, and a value with the top byte set.
        let mut a = [0u8; 32];
        a[0] = 1;
        let mut b = [0u8; 32];
        b[31] = 0x80;
        let payload: Vec<u8> = a.iter().chain(b.iter()).copied().collect();
        match decode_one_typed("Int256", 2, &payload).await {
            DecodedColumn::Int256(v) => assert_eq!(v, vec![a, b]),
            other => panic!("expected Int256, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_decimal64_as_backing_int() {
        // Decimal(18, 4) decodes to its backing i64 (12345 == 1.2345) and
        // surfaces precision=18 + scale=4 from the type name.
        let vals = [12345i64, -67890, 0];
        let payload: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        match decode_one_typed("Decimal(18, 4)", vals.len() as u64, &payload).await {
            DecodedColumn::Decimal64 {
                precision,
                scale,
                values,
            } => {
                assert_eq!(values, vals);
                assert_eq!(precision, 18);
                assert_eq!(scale, 4);
            }
            other => panic!("expected Decimal64, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn decimal_sized_form_implies_precision() {
        // The sized form Decimal64(4) carries only scale on the wire-type
        // name; precision is implied by the backing width (64-bit -> 18).
        let vals = [1i64, 2, 3];
        let payload: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        match decode_one_typed("Decimal64(4)", vals.len() as u64, &payload).await {
            DecodedColumn::Decimal64 {
                precision, scale, ..
            } => {
                assert_eq!(precision, 18, "Decimal64 implies precision 18");
                assert_eq!(scale, 4);
            }
            other => panic!("expected Decimal64, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn datetime64_surfaces_precision_and_timezone() {
        // DateTime64(3, 'UTC') decodes Int64 ticks and surfaces both the
        // sub-second precision (3) and the IANA timezone ('UTC').
        let vals = [1_700_000_000_000i64, 0, -1];
        let payload: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        match decode_one_typed("DateTime64(3, 'UTC')", vals.len() as u64, &payload).await {
            DecodedColumn::DateTime64 {
                precision,
                timezone,
                values,
            } => {
                assert_eq!(values, vals);
                assert_eq!(precision, 3);
                assert_eq!(timezone.as_deref(), Some("UTC"));
            }
            other => panic!("expected DateTime64, got {other:?}"),
        }
        // Without a timezone arg the field is None.
        match decode_one_typed("DateTime64(9)", 0, &[]).await {
            DecodedColumn::DateTime64 {
                precision,
                timezone,
                ..
            } => {
                assert_eq!(precision, 9);
                assert!(timezone.is_none());
            }
            other => panic!("expected DateTime64, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_date32_signed_days() {
        let vals = [0i32, 19_000, -1]; // epoch, ~2022, one day pre-epoch
        let payload: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        match decode_one_typed("Date32", vals.len() as u64, &payload).await {
            DecodedColumn::Date32(v) => assert_eq!(v, vals),
            other => panic!("expected Date32, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_float64_nan_inf() {
        let vals = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1.5];
        let payload: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        match decode_one_typed("Float64", vals.len() as u64, &payload).await {
            DecodedColumn::Float64(v) => {
                assert!(v[0].is_nan());
                assert_eq!(v[1], f64::INFINITY);
                assert_eq!(v[2], f64::NEG_INFINITY);
                assert_eq!(v[3], 1.5);
            }
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    /// Hand-build a `LowCardinality(String)` column body with an
    /// explicit index-size code (0=u8, 1=u16, 2=u32, 3=u64), so we can
    /// exercise all four index widths without needing a dictionary
    /// large enough to force the wider codes naturally. `dict` are the
    /// additional-key strings; `idx` are the per-row dictionary
    /// positions. Mirrors the wire shape `decode_low_cardinality` reads:
    /// version, flags (HAS_ADDITIONAL_KEYS | code), additional-keys
    /// count + strings, index count, then indices at the chosen width.
    fn lc_string_payload(index_code: u8, dict: &[&str], idx: &[u64]) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&1u64.to_le_bytes()); // version
        let flags = 0x200u64 | u64::from(index_code); // HAS_ADDITIONAL_KEYS | code
        p.extend_from_slice(&flags.to_le_bytes());
        // Additional keys: count, then each String as varuint(len)+bytes
        // (all test dict entries are < 128 bytes -> single length byte).
        p.extend_from_slice(&(dict.len() as u64).to_le_bytes());
        for s in dict {
            assert!(s.len() < 128, "test dict entries stay single-byte-len");
            p.push(s.len() as u8);
            p.extend_from_slice(s.as_bytes());
        }
        // Index count, then indices at the chosen width.
        p.extend_from_slice(&(idx.len() as u64).to_le_bytes());
        for &i in idx {
            match index_code {
                0 => p.push(i as u8),
                1 => p.extend_from_slice(&(i as u16).to_le_bytes()),
                2 => p.extend_from_slice(&(i as u32).to_le_bytes()),
                3 => p.extend_from_slice(&i.to_le_bytes()),
                other => panic!("bad index code {other}"),
            }
        }
        p
    }

    #[tokio::test]
    async fn lowcardinality_decodes_all_index_widths() {
        // The same logical column ["a","b","a","c"] encoded with each
        // of the four index-size codes; the decoder must select the
        // matching width and reconstruct identical row strings every
        // time. Catches an index-width misread (a silent-misalignment
        // edge -- a u16 column read as u8 desyncs the rest of the block).
        let dict = ["a", "b", "c"];
        let idx = [0u64, 1, 0, 2];
        for code in 0u8..=3 {
            let payload = lc_string_payload(code, &dict, &idx);
            let col =
                decode_one_typed("LowCardinality(String)", idx.len() as u64, &payload).await;
            match col {
                DecodedColumn::LowCardinality {
                    dict,
                    indices,
                    is_nullable_inner,
                } => {
                    assert!(!is_nullable_inner, "code {code}");
                    let got = lc_to_strings(dict.as_ref(), indices.as_ref());
                    assert_eq!(got, vec!["a", "b", "a", "c"], "index code {code}");
                    let width_ok = matches!(
                        (code, indices.as_ref()),
                        (0, DecodedColumn::UInt8(_))
                            | (1, DecodedColumn::UInt16(_))
                            | (2, DecodedColumn::UInt32(_))
                            | (3, DecodedColumn::UInt64(_))
                    );
                    assert!(width_ok, "wrong index variant for code {code}");
                }
                other => panic!("expected LowCardinality, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn lowcardinality_nullable_index_zero_is_null() {
        // LC(Nullable(String)) reserves dictionary slot 0 as the NULL
        // sentinel; rows referencing index 0 are NULL. The decoder
        // flags is_nullable_inner=true and leaves the null reading to
        // the caller (index 0 == null).
        let dict = ["", "x", "y"]; // slot 0 = null placeholder
        let idx = [0u64, 1, 0, 2];
        let payload = lc_string_payload(0, &dict, &idx);
        let col = decode_one_typed(
            "LowCardinality(Nullable(String))",
            idx.len() as u64,
            &payload,
        )
        .await;
        match col {
            DecodedColumn::LowCardinality {
                dict,
                indices,
                is_nullable_inner,
            } => {
                assert!(is_nullable_inner, "Nullable inner must be flagged");
                let idxs = match indices.as_ref() {
                    DecodedColumn::UInt8(v) => v.clone(),
                    other => panic!("expected UInt8 indices, got {other:?}"),
                };
                let nulls: Vec<bool> = idxs.iter().map(|&i| i == 0).collect();
                assert_eq!(nulls, vec![true, false, true, false]);
                match dict.as_ref() {
                    DecodedColumn::String(d) => assert_eq!(d.len(), 3),
                    other => panic!("expected String dict, got {other:?}"),
                }
            }
            other => panic!("expected LowCardinality, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_nested_array_array_uint64() {
        // Array(Array(UInt64)) over two rows: [[1,2],[3]] and [[4,5,6]].
        // Outer offsets count inner arrays (cumulative); inner offsets
        // count u64s (cumulative). Exercises the recursive Array path
        // and the offset bookkeeping across two nesting levels.
        let mut payload = Vec::new();
        for v in [2u64, 3] {
            payload.extend_from_slice(&v.to_le_bytes()); // outer offsets
        }
        for v in [2u64, 3, 6] {
            payload.extend_from_slice(&v.to_le_bytes()); // inner offsets
        }
        for v in [1u64, 2, 3, 4, 5, 6] {
            payload.extend_from_slice(&v.to_le_bytes()); // u64 values
        }
        let col = decode_one_typed("Array(Array(UInt64))", 2, &payload).await;
        match col {
            DecodedColumn::Array { offsets, child } => {
                assert_eq!(offsets, vec![2u64, 3]);
                match child.as_ref() {
                    DecodedColumn::Array {
                        offsets: inner_off,
                        child: inner_child,
                    } => {
                        assert_eq!(inner_off, &vec![2u64, 3, 6]);
                        match inner_child.as_ref() {
                            DecodedColumn::UInt64(v) => {
                                assert_eq!(v, &vec![1u64, 2, 3, 4, 5, 6]);
                            }
                            other => panic!("expected UInt64, got {other:?}"),
                        }
                    }
                    other => panic!("expected inner Array, got {other:?}"),
                }
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_map_string_uint64() {
        // Map(String, UInt64) over two rows: {"a":1,"b":2} and {"c":3}.
        // Wire shape: row offsets (cumulative pair count), then the keys
        // column, then the values column -- both flat over total pairs.
        let mut payload = Vec::new();
        for v in [2u64, 3] {
            payload.extend_from_slice(&v.to_le_bytes()); // offsets
        }
        for s in ["a", "b", "c"] {
            payload.push(s.len() as u8); // varuint len (< 128)
            payload.extend_from_slice(s.as_bytes());
        }
        for v in [1u64, 2, 3] {
            payload.extend_from_slice(&v.to_le_bytes()); // values
        }
        let col = decode_one_typed("Map(String, UInt64)", 2, &payload).await;
        match col {
            DecodedColumn::Map {
                offsets,
                keys,
                values,
            } => {
                assert_eq!(offsets, vec![2u64, 3]);
                match keys.as_ref() {
                    DecodedColumn::String(k) => {
                        let ks: Vec<&str> =
                            k.iter().map(|b| std::str::from_utf8(b).unwrap()).collect();
                        assert_eq!(ks, vec!["a", "b", "c"]);
                    }
                    other => panic!("expected String keys, got {other:?}"),
                }
                match values.as_ref() {
                    DecodedColumn::UInt64(v) => assert_eq!(v, &vec![1u64, 2, 3]),
                    other => panic!("expected UInt64 values, got {other:?}"),
                }
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn nullable_all_null_uint8() {
        // Native Nullable shape: n mask bytes (1 = null) then the child
        // column. An all-null mask over a zero-filled child -- the
        // companion to roundtrip_nullable_uint8's interleaved case.
        let payload = vec![1u8, 1, 1, /* child */ 0, 0, 0];
        let col = decode_one_typed("Nullable(UInt8)", 3, &payload).await;
        match col {
            DecodedColumn::Nullable { mask, child } => {
                assert_eq!(mask, vec![1u8, 1, 1]);
                match child.as_ref() {
                    DecodedColumn::UInt8(v) => assert_eq!(v, &vec![0u8, 0, 0]),
                    other => panic!("expected UInt8 child, got {other:?}"),
                }
            }
            other => panic!("expected Nullable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fixed_string_preserves_nul_and_padding() {
        // FixedString(4) is raw fixed-width bytes: embedded NULs and
        // trailing zero-padding are data, never terminators. Row 0 is
        // "a\0b\0" (embedded + trailing NUL), row 1 is "ab\0\0"
        // (zero-padded short value). Both must survive byte-for-byte.
        let raw = [b'a', 0, b'b', 0, b'a', b'b', 0, 0];
        let col = decode_one_typed("FixedString(4)", 2, &raw).await;
        match col {
            DecodedColumn::FixedString { width, bytes } => {
                assert_eq!(width, 4);
                assert_eq!(bytes, raw);
            }
            other => panic!("expected FixedString, got {other:?}"),
        }
    }
}
