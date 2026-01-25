//! Microsoft SQL Server driver.
//!
//! This module provides MSSQL-specific implementations:
//!
//! - [`MssqlDialect`]: SQL syntax strategy for MSSQL
//! - [`MssqlReader`]: Source reader for MSSQL databases
//! - [`MssqlWriter`]: Target writer for MSSQL databases

mod dialect;
mod reader;
mod writer;

pub use dialect::MssqlDialect;
pub use reader::MssqlReader;
pub use writer::{MssqlWriter, TiberiusConnectionManager};
