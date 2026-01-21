//! PostgreSQL driver.
//!
//! This module provides PostgreSQL-specific implementations:
//!
//! - [`PostgresDialect`]: SQL syntax strategy for PostgreSQL
//!
//! # Note
//!
//! The full `PostgresReader` and `PostgresWriter` implementations will be extracted
//! from `source/postgres.rs` and `target/mod.rs` in subsequent commits.

mod dialect;

pub use dialect::PostgresDialect;
