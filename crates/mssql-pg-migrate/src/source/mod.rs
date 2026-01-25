//! Source database schema types (re-exports for backward compatibility).
//!
//! # Migration Notice
//!
//! This module now re-exports types from [`crate::core::schema`] for backward
//! compatibility. The legacy `SourcePool` trait and implementations have been
//! replaced by the new plugin architecture:
//!
//! - Use [`crate::core::traits::SourceReader`] instead of the old `SourcePool` trait
//! - Use [`crate::drivers::SourceReaderImpl`] for enum-based dispatch
//! - Use [`crate::drivers::MssqlReader`] instead of the old `MssqlPool`
//! - Use [`crate::drivers::PostgresReader`] instead of the old `PgSourcePool`

// Re-export schema types from core module for backward compatibility
pub use crate::core::schema::{
    CheckConstraint, Column, ForeignKey, Index, Partition, PkValue, Table,
};

// Re-export utility types from drivers::common
pub use crate::drivers::common::{BinaryColumnType, BinaryRowParser};
