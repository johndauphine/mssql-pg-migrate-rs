//! SQL value types for database-agnostic data transfer.
//!
//! This module provides efficient value representations optimized for high-throughput
//! data migration between different database systems.

use std::borrow::Cow;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use uuid::Uuid;

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
}
