//! SQL value types for database-agnostic data transfer.
//!
//! This module provides efficient value representations optimized for high-throughput
//! data migration between different database systems.

use std::borrow::Cow;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use uuid::Uuid;

/// Minimum text length to consider for compression (64 bytes).
/// Smaller strings have too much overhead from compression headers.
pub const COMPRESSION_THRESHOLD: usize = 64;

/// Type hint for NULL values to ensure correct target database encoding.
///
/// When encoding NULL values in binary protocols (like PostgreSQL COPY),
/// we need to know the expected column type to emit the correct wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlNullType {
    Bool,
    I16,
    I32,
    I64,
    F32,
    F64,
    String,
    Bytes,
    Uuid,
    Decimal,
    DateTime,
    DateTimeOffset,
    Date,
    Time,
}

/// SQL value enum for type-safe row handling with efficient memory usage.
///
/// Uses `Cow` for string and byte data to enable zero-copy transfers when possible,
/// reducing allocation overhead during high-throughput migrations.
///
/// # Lifetime
///
/// The `'a` lifetime allows borrowing from source buffers during read operations.
/// For owned data that outlives the source buffer, use `.into_owned()`.
///
/// # Example
///
/// ```rust
/// use std::borrow::Cow;
/// use mssql_pg_migrate::core::SqlValue;
///
/// // Zero-copy from source buffer
/// let borrowed: SqlValue<'_> = SqlValue::Text(Cow::Borrowed("hello"));
///
/// // Convert to owned for storage/transfer
/// let owned: SqlValue<'static> = borrowed.into_owned();
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue<'a> {
    /// NULL with type hint for correct wire format encoding.
    Null(SqlNullType),

    /// Boolean value.
    Bool(bool),

    /// 16-bit signed integer (smallint).
    I16(i16),

    /// 32-bit signed integer (int).
    I32(i32),

    /// 64-bit signed integer (bigint).
    I64(i64),

    /// 32-bit floating point (real/float4).
    F32(f32),

    /// 64-bit floating point (double precision/float8).
    F64(f64),

    /// Text/string data with zero-copy support.
    ///
    /// Uses `Cow` to avoid allocation when borrowing from source buffers.
    Text(Cow<'a, str>),

    /// LZ4-compressed text data for large strings.
    ///
    /// Stores the original length and compressed bytes to reduce memory
    /// usage in the transfer pipeline for tables with large text columns.
    CompressedText {
        /// Original uncompressed length in bytes.
        original_len: usize,
        /// LZ4-compressed data.
        compressed: Vec<u8>,
    },

    /// Binary data with zero-copy support.
    ///
    /// Uses `Cow` to avoid allocation when borrowing from source buffers.
    Bytes(Cow<'a, [u8]>),

    /// UUID/GUID value.
    Uuid(Uuid),

    /// Decimal value with arbitrary precision.
    Decimal(Decimal),

    /// Timestamp without timezone.
    DateTime(NaiveDateTime),

    /// Timestamp with timezone offset.
    DateTimeOffset(DateTime<FixedOffset>),

    /// Date without time component.
    Date(NaiveDate),

    /// Time without date component.
    Time(NaiveTime),
}

impl<'a> SqlValue<'a> {
    /// Convert to a fully owned value with `'static` lifetime.
    ///
    /// This clones any borrowed data, making the value independent of
    /// the original source buffer.
    #[must_use]
    pub fn into_owned(self) -> SqlValue<'static> {
        match self {
            SqlValue::Null(t) => SqlValue::Null(t),
            SqlValue::Bool(v) => SqlValue::Bool(v),
            SqlValue::I16(v) => SqlValue::I16(v),
            SqlValue::I32(v) => SqlValue::I32(v),
            SqlValue::I64(v) => SqlValue::I64(v),
            SqlValue::F32(v) => SqlValue::F32(v),
            SqlValue::F64(v) => SqlValue::F64(v),
            SqlValue::Text(v) => SqlValue::Text(Cow::Owned(v.into_owned())),
            SqlValue::CompressedText {
                original_len,
                compressed,
            } => SqlValue::CompressedText {
                original_len,
                compressed,
            },
            SqlValue::Bytes(v) => SqlValue::Bytes(Cow::Owned(v.into_owned())),
            SqlValue::Uuid(v) => SqlValue::Uuid(v),
            SqlValue::Decimal(v) => SqlValue::Decimal(v),
            SqlValue::DateTime(v) => SqlValue::DateTime(v),
            SqlValue::DateTimeOffset(v) => SqlValue::DateTimeOffset(v),
            SqlValue::Date(v) => SqlValue::Date(v),
            SqlValue::Time(v) => SqlValue::Time(v),
        }
    }

    /// Check if this value is NULL.
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, SqlValue::Null(_))
    }

    /// Get the SqlNullType for this value (for type-aware NULL encoding).
    #[must_use]
    pub fn null_type(&self) -> SqlNullType {
        match self {
            SqlValue::Null(t) => *t,
            SqlValue::Bool(_) => SqlNullType::Bool,
            SqlValue::I16(_) => SqlNullType::I16,
            SqlValue::I32(_) => SqlNullType::I32,
            SqlValue::I64(_) => SqlNullType::I64,
            SqlValue::F32(_) => SqlNullType::F32,
            SqlValue::F64(_) => SqlNullType::F64,
            SqlValue::Text(_) => SqlNullType::String,
            SqlValue::CompressedText { .. } => SqlNullType::String,
            SqlValue::Bytes(_) => SqlNullType::Bytes,
            SqlValue::Uuid(_) => SqlNullType::Uuid,
            SqlValue::Decimal(_) => SqlNullType::Decimal,
            SqlValue::DateTime(_) => SqlNullType::DateTime,
            SqlValue::DateTimeOffset(_) => SqlNullType::DateTimeOffset,
            SqlValue::Date(_) => SqlNullType::Date,
            SqlValue::Time(_) => SqlNullType::Time,
        }
    }
}

// Convenience constructors for common cases
impl<'a> SqlValue<'a> {
    /// Create a text value from a borrowed string slice.
    #[must_use]
    pub fn text_borrowed(s: &'a str) -> Self {
        SqlValue::Text(Cow::Borrowed(s))
    }

    /// Create a text value from an owned String.
    #[must_use]
    pub fn text_owned(s: String) -> SqlValue<'static> {
        SqlValue::Text(Cow::Owned(s))
    }

    /// Create a bytes value from a borrowed byte slice.
    #[must_use]
    pub fn bytes_borrowed(b: &'a [u8]) -> Self {
        SqlValue::Bytes(Cow::Borrowed(b))
    }

    /// Create a bytes value from an owned Vec<u8>.
    #[must_use]
    pub fn bytes_owned(b: Vec<u8>) -> SqlValue<'static> {
        SqlValue::Bytes(Cow::Owned(b))
    }

    /// Compress a text value if it exceeds the threshold.
    ///
    /// Returns a `CompressedText` variant for large strings, or the original
    /// `Text` variant for small strings (below `COMPRESSION_THRESHOLD`).
    #[must_use]
    pub fn compress_text(s: String) -> SqlValue<'static> {
        if s.len() < COMPRESSION_THRESHOLD {
            return SqlValue::Text(Cow::Owned(s));
        }

        let compressed = lz4_flex::compress_prepend_size(s.as_bytes());

        // Only use compression if it actually saves space
        if compressed.len() < s.len() {
            SqlValue::CompressedText {
                original_len: s.len(),
                compressed,
            }
        } else {
            SqlValue::Text(Cow::Owned(s))
        }
    }

    /// Decompress a text value, returning the string.
    ///
    /// For `Text` variants, returns the string directly.
    /// For `CompressedText` variants, decompresses and returns the string.
    /// Returns `None` for non-text variants.
    #[must_use]
    pub fn decompress_text(&self) -> Option<Cow<'_, str>> {
        match self {
            SqlValue::Text(s) => Some(Cow::Borrowed(s.as_ref())),
            SqlValue::CompressedText {
                original_len: _,
                compressed,
            } => {
                // Decompress - lz4_flex stores the size in the first 4 bytes
                match lz4_flex::decompress_size_prepended(compressed) {
                    Ok(decompressed) => {
                        // Convert bytes to string
                        String::from_utf8(decompressed)
                            .ok()
                            .map(Cow::Owned)
                    }
                    Err(_) => None,
                }
            }
            _ => None,
        }
    }

    /// Check if this is a text or compressed text value.
    #[must_use]
    pub fn is_text(&self) -> bool {
        matches!(self, SqlValue::Text(_) | SqlValue::CompressedText { .. })
    }
}

// From implementations for common types
impl From<bool> for SqlValue<'static> {
    fn from(v: bool) -> Self {
        SqlValue::Bool(v)
    }
}

impl From<i16> for SqlValue<'static> {
    fn from(v: i16) -> Self {
        SqlValue::I16(v)
    }
}

impl From<i32> for SqlValue<'static> {
    fn from(v: i32) -> Self {
        SqlValue::I32(v)
    }
}

impl From<i64> for SqlValue<'static> {
    fn from(v: i64) -> Self {
        SqlValue::I64(v)
    }
}

impl From<f32> for SqlValue<'static> {
    fn from(v: f32) -> Self {
        SqlValue::F32(v)
    }
}

impl From<f64> for SqlValue<'static> {
    fn from(v: f64) -> Self {
        SqlValue::F64(v)
    }
}

impl From<String> for SqlValue<'static> {
    fn from(v: String) -> Self {
        SqlValue::Text(Cow::Owned(v))
    }
}

impl<'a> From<&'a str> for SqlValue<'a> {
    fn from(v: &'a str) -> Self {
        SqlValue::Text(Cow::Borrowed(v))
    }
}

impl From<Vec<u8>> for SqlValue<'static> {
    fn from(v: Vec<u8>) -> Self {
        SqlValue::Bytes(Cow::Owned(v))
    }
}

impl<'a> From<&'a [u8]> for SqlValue<'a> {
    fn from(v: &'a [u8]) -> Self {
        SqlValue::Bytes(Cow::Borrowed(v))
    }
}

impl From<Uuid> for SqlValue<'static> {
    fn from(v: Uuid) -> Self {
        SqlValue::Uuid(v)
    }
}

impl From<Decimal> for SqlValue<'static> {
    fn from(v: Decimal) -> Self {
        SqlValue::Decimal(v)
    }
}

impl From<NaiveDateTime> for SqlValue<'static> {
    fn from(v: NaiveDateTime) -> Self {
        SqlValue::DateTime(v)
    }
}

impl From<DateTime<FixedOffset>> for SqlValue<'static> {
    fn from(v: DateTime<FixedOffset>) -> Self {
        SqlValue::DateTimeOffset(v)
    }
}

impl From<NaiveDate> for SqlValue<'static> {
    fn from(v: NaiveDate) -> Self {
        SqlValue::Date(v)
    }
}

impl From<NaiveTime> for SqlValue<'static> {
    fn from(v: NaiveTime) -> Self {
        SqlValue::Time(v)
    }
}

/// A batch of rows for streaming transfer.
///
/// Used by the transfer pipeline to efficiently move data between databases
/// while supporting backpressure through bounded channels.
#[derive(Debug)]
pub struct Batch {
    /// Rows in this batch (owned for channel transfer).
    pub rows: Vec<Vec<SqlValue<'static>>>,

    /// Last primary key value for keyset pagination continuity.
    pub last_key: Option<SqlValue<'static>>,

    /// Whether this is the final batch for the current partition/table.
    pub is_last: bool,
}

impl Batch {
    /// Create a new batch with the given rows.
    pub fn new(rows: Vec<Vec<SqlValue<'static>>>) -> Self {
        Self {
            rows,
            last_key: None,
            is_last: false,
        }
    }

    /// Create an empty final batch.
    pub fn empty_final() -> Self {
        Self {
            rows: Vec::new(),
            last_key: None,
            is_last: true,
        }
    }

    /// Set the last key for keyset pagination.
    pub fn with_last_key(mut self, key: SqlValue<'static>) -> Self {
        self.last_key = Some(key);
        self
    }

    /// Mark this as the final batch.
    pub fn mark_final(mut self) -> Self {
        self.is_last = true;
        self
    }

    /// Get the number of rows in this batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Check if the batch is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Compress all text values in the batch using LZ4.
    ///
    /// This reduces memory usage for batches containing large text columns.
    /// Text values smaller than `COMPRESSION_THRESHOLD` are left uncompressed.
    pub fn compress_text_values(&mut self) {
        for row in &mut self.rows {
            for value in row {
                if let SqlValue::Text(text) = value {
                    if text.len() >= COMPRESSION_THRESHOLD {
                        let s = std::mem::take(text);
                        *value = SqlValue::compress_text(s.into_owned());
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_value_into_owned() {
        let borrowed: SqlValue<'_> = SqlValue::Text(Cow::Borrowed("hello"));
        let owned: SqlValue<'static> = borrowed.into_owned();
        assert_eq!(owned, SqlValue::Text(Cow::Owned("hello".to_string())));
    }

    #[test]
    fn test_sql_value_is_null() {
        assert!(SqlValue::<'static>::Null(SqlNullType::String).is_null());
        assert!(!SqlValue::I32(42).is_null());
    }

    #[test]
    fn test_batch_operations() {
        let batch = Batch::new(vec![
            vec![SqlValue::I32(1), SqlValue::text_owned("a".to_string())],
            vec![SqlValue::I32(2), SqlValue::text_owned("b".to_string())],
        ]);

        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
        assert!(!batch.is_last);

        let final_batch = batch.mark_final();
        assert!(final_batch.is_last);
    }

    #[test]
    fn test_from_implementations() {
        let v: SqlValue<'static> = 42i32.into();
        assert_eq!(v, SqlValue::I32(42));

        let v: SqlValue<'static> = "hello".to_string().into();
        assert_eq!(v, SqlValue::Text(Cow::Owned("hello".to_string())));
    }

    // ============== LZ4 Compression Tests ==============

    #[test]
    fn test_compress_text_small_string_not_compressed() {
        // Strings below COMPRESSION_THRESHOLD (64 bytes) should not be compressed
        let small = "hello world".to_string();
        assert!(small.len() < COMPRESSION_THRESHOLD);

        let result = SqlValue::compress_text(small.clone());
        assert!(matches!(result, SqlValue::Text(_)));
        if let SqlValue::Text(cow) = result {
            assert_eq!(cow.as_ref(), "hello world");
        }
    }

    #[test]
    fn test_compress_text_large_string_compressed() {
        // Large repetitive string should compress well
        let large = "a".repeat(200);
        assert!(large.len() >= COMPRESSION_THRESHOLD);

        let result = SqlValue::compress_text(large.clone());
        // Repetitive data compresses well, so it should be compressed
        assert!(
            matches!(result, SqlValue::CompressedText { .. }),
            "Expected CompressedText for large repetitive string"
        );

        if let SqlValue::CompressedText {
            original_len,
            compressed,
        } = &result
        {
            assert_eq!(*original_len, 200);
            assert!(compressed.len() < 200, "Compressed size should be smaller");
        }
    }

    #[test]
    fn test_compress_text_incompressible_data() {
        // Random-looking data that doesn't compress well
        // Should remain as Text if compression doesn't save space
        let random_looking: String = (0..100)
            .map(|i| char::from_u32(((i * 7 + 13) % 94 + 33) as u32).unwrap())
            .collect();
        assert!(random_looking.len() >= COMPRESSION_THRESHOLD);

        let result = SqlValue::compress_text(random_looking.clone());
        // The result could be either Text or CompressedText depending on actual compression ratio
        // The key is that decompress_text should return the original string
        let decompressed = result.decompress_text();
        assert!(decompressed.is_some());
        assert_eq!(decompressed.unwrap().as_ref(), random_looking);
    }

    #[test]
    fn test_decompress_text_from_text_variant() {
        let text = SqlValue::Text(Cow::Owned("hello".to_string()));
        let result = text.decompress_text();
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_ref(), "hello");
    }

    #[test]
    fn test_decompress_text_from_compressed_variant() {
        let original = "a".repeat(200);
        let compressed = SqlValue::compress_text(original.clone());

        let result = compressed.decompress_text();
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_ref(), original);
    }

    #[test]
    fn test_decompress_text_returns_none_for_non_text() {
        assert!(SqlValue::I32(42).decompress_text().is_none());
        assert!(SqlValue::Bool(true).decompress_text().is_none());
        assert!(SqlValue::Null(SqlNullType::String).decompress_text().is_none());
        assert!(SqlValue::bytes_owned(vec![1, 2, 3]).decompress_text().is_none());
    }

    #[test]
    fn test_compressed_text_into_owned() {
        let original = "a".repeat(200);
        let compressed = SqlValue::compress_text(original.clone());

        if let SqlValue::CompressedText {
            original_len,
            compressed: data,
        } = compressed
        {
            let owned: SqlValue<'static> = SqlValue::CompressedText {
                original_len,
                compressed: data,
            }
            .into_owned();

            // Verify it's still CompressedText after into_owned
            assert!(matches!(owned, SqlValue::CompressedText { .. }));

            // Verify decompression still works
            let decompressed = owned.decompress_text();
            assert!(decompressed.is_some());
            assert_eq!(decompressed.unwrap().as_ref(), original);
        }
    }

    #[test]
    fn test_compressed_text_is_not_null() {
        let compressed = SqlValue::compress_text("a".repeat(200));
        assert!(!compressed.is_null());
    }

    #[test]
    fn test_compressed_text_null_type() {
        let compressed = SqlValue::compress_text("a".repeat(200));
        assert_eq!(compressed.null_type(), SqlNullType::String);
    }

    #[test]
    fn test_is_text_includes_compressed_text() {
        let text = SqlValue::text_owned("hello".to_string());
        let compressed = SqlValue::compress_text("a".repeat(200));

        assert!(text.is_text());
        assert!(compressed.is_text());
        assert!(!SqlValue::I32(42).is_text());
        assert!(!SqlValue::bytes_owned(vec![1, 2, 3]).is_text());
    }

    #[test]
    fn test_batch_compress_text_values() {
        let mut batch = Batch::new(vec![
            vec![
                SqlValue::I32(1),
                SqlValue::text_owned("short".to_string()),
                SqlValue::text_owned("a".repeat(200)),
            ],
            vec![
                SqlValue::I32(2),
                SqlValue::text_owned("b".repeat(300)),
                SqlValue::Bool(true),
            ],
        ]);

        batch.compress_text_values();

        // Row 0: I32 unchanged, short text unchanged, large text compressed
        assert!(matches!(batch.rows[0][0], SqlValue::I32(1)));
        assert!(matches!(batch.rows[0][1], SqlValue::Text(_))); // Too short to compress
        assert!(
            matches!(batch.rows[0][2], SqlValue::CompressedText { .. }),
            "Large text should be compressed"
        );

        // Row 1: I32 unchanged, large text compressed, bool unchanged
        assert!(matches!(batch.rows[1][0], SqlValue::I32(2)));
        assert!(
            matches!(batch.rows[1][1], SqlValue::CompressedText { .. }),
            "Large text should be compressed"
        );
        assert!(matches!(batch.rows[1][2], SqlValue::Bool(true)));

        // Verify decompression works
        let decompressed_0_2 = batch.rows[0][2].decompress_text();
        assert!(decompressed_0_2.is_some());
        assert_eq!(decompressed_0_2.unwrap().as_ref(), "a".repeat(200));

        let decompressed_1_1 = batch.rows[1][1].decompress_text();
        assert!(decompressed_1_1.is_some());
        assert_eq!(decompressed_1_1.unwrap().as_ref(), "b".repeat(300));
    }

    #[test]
    fn test_compression_threshold_boundary() {
        // Test at exactly the threshold
        let at_threshold = "x".repeat(COMPRESSION_THRESHOLD);
        let below_threshold = "x".repeat(COMPRESSION_THRESHOLD - 1);

        let result_at = SqlValue::compress_text(at_threshold.clone());
        let result_below = SqlValue::compress_text(below_threshold.clone());

        // Below threshold should never be compressed
        assert!(
            matches!(result_below, SqlValue::Text(_)),
            "Below threshold should not be compressed"
        );

        // At threshold, compression is attempted (result depends on compression ratio)
        // Both should decompress correctly
        assert_eq!(
            result_at.decompress_text().unwrap().as_ref(),
            at_threshold
        );
        assert_eq!(
            result_below.decompress_text().unwrap().as_ref(),
            below_threshold
        );
    }
}
