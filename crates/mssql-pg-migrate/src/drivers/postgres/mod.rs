//! PostgreSQL driver.
//!
//! This module provides PostgreSQL-specific implementations:
//!
//! - [`PostgresDialect`]: SQL syntax strategy for PostgreSQL
//! - [`PostgresReader`]: Source reader for PostgreSQL databases
//! - [`PostgresWriter`]: Target writer for PostgreSQL databases

mod dialect;
mod reader;
mod writer;

pub use dialect::PostgresDialect;
pub use reader::PostgresReader;
pub use writer::PostgresWriter;
