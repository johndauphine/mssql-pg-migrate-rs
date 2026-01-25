//! Core abstractions for database-agnostic migration.
//!
//! This module provides the foundational types and traits used throughout
//! the migration system:
//!
//! - [`schema`]: Table, column, and constraint metadata types
//! - [`value`]: SQL value representation with efficient memory usage
//! - [`traits`]: Core traits for readers, writers, dialects, and type mappers
//! - [`catalog`]: Driver registry for dependency injection
//!
//! # Architecture
//!
//! The core module defines database-agnostic abstractions that are implemented
//! by driver modules (`drivers/mssql`, `drivers/postgres`, etc.). This separation
//! enables:
//!
//! - **Extensibility**: New databases can be added without modifying core code
//! - **Testability**: Core logic can be tested with mock implementations
//! - **Maintainability**: Clear boundaries between generic and database-specific code
//!
//! # Design Patterns
//!
//! - **Abstract Factory**: `DriverCatalog` creates families of related objects
//! - **Strategy**: `Dialect` and `TypeMapper` provide interchangeable algorithms
//! - **Template Method**: Default trait method implementations define algorithm skeletons

pub mod catalog;
pub mod identifier;
pub mod schema;
pub mod traits;
pub mod value;

// Re-export commonly used types for convenience
pub use catalog::DriverCatalog;
pub use schema::{CheckConstraint, Column, ForeignKey, Index, Partition, PkValue, Table};
pub use traits::{
    ColumnMapping, Dialect, ReadOptions, SelectQueryOptions, SourceReader, TargetWriter,
    TypeMapper, TypeMapping,
};
pub use value::{Batch, SqlNullType, SqlValue};
