//! Microsoft SQL Server driver.
//!
//! This module provides MSSQL-specific implementations:
//!
//! - [`MssqlDialect`]: SQL syntax strategy for MSSQL
//!
//! # Note
//!
//! The full `MssqlReader` and `MssqlWriter` implementations will be extracted
//! from `source/mod.rs` and `target/mssql.rs` in subsequent commits.

mod dialect;

pub use dialect::MssqlDialect;
