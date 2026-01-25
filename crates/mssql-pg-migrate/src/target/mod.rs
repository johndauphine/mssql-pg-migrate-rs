//! Target database value types and traits.
//!
//! This module provides core types for target database operations:
//! - [`SqlValue`] - Type-safe representation of database values
//! - [`SqlNullType`] - Type hints for NULL values
//! - [`UpsertWriter`] - Trait for stateful upsert operations
//!
//! # Migration Notice
//!
//! The `TargetPool` trait and legacy pool implementations have been replaced by
//! the new plugin architecture in [`crate::core`] and [`crate::drivers`]:
//!
//! - Use [`crate::core::traits::TargetWriter`] instead of the old `TargetPool` trait
//! - Use [`crate::drivers::TargetWriterImpl`] for enum-based dispatch
//! - Use [`crate::drivers::PostgresWriter`] instead of the old `PgPool`
//! - Use [`crate::drivers::MssqlWriter`] instead of the old `MssqlTargetPool`

use crate::error::{MigrateError, Result};
use async_trait::async_trait;

/// Trait for a stateful upsert writer that holds a connection.
#[async_trait]
pub trait UpsertWriter: Send {
    /// Upsert a chunk of rows using the held connection.
    async fn upsert_chunk(
        &mut self,
        cols: &[String],
        pk_cols: &[String],
        rows: Vec<Vec<SqlValue>>,
    ) -> Result<u64>;

    /// Upsert pre-encoded COPY binary data directly.
    ///
    /// This is the high-performance path that bypasses SqlValue encoding.
    /// The `copy_data` must be valid PostgreSQL COPY binary format.
    /// Default implementation falls back to upsert_chunk (for MSSQL target).
    async fn upsert_chunk_direct(
        &mut self,
        _cols: &[String],
        _pk_cols: &[String],
        _copy_data: bytes::Bytes,
        _row_count: usize,
    ) -> Result<u64> {
        // Default: not supported, caller should use upsert_chunk
        Err(MigrateError::Transfer {
            table: "unknown".to_string(),
            message: "Direct copy not supported for this target".to_string(),
        })
    }

    /// Returns true if this writer supports direct copy.
    fn supports_direct_copy(&self) -> bool {
        false
    }
}

/// SQL value enum for type-safe row handling.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null(SqlNullType),
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    String(String),
    /// LZ4-compressed text data for large strings.
    CompressedText {
        /// Original uncompressed length in bytes.
        original_len: usize,
        /// LZ4-compressed data.
        compressed: Vec<u8>,
    },
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
    Decimal(rust_decimal::Decimal),
    DateTime(chrono::NaiveDateTime),
    DateTimeOffset(chrono::DateTime<chrono::FixedOffset>),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
}

/// Type hint for NULL values to ensure correct PostgreSQL encoding.
#[derive(Debug, Clone, Copy, PartialEq)]
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
