//! MySQL/MariaDB database driver.
//!
//! This module provides MySQL-specific implementations for:
//! - [`MysqlDialect`]: SQL syntax strategy
//! - [`MysqlReader`]: Source database reader
//! - [`MysqlWriter`]: Target database writer
//!
//! # Feature Flag
//!
//! This module is only available when the `mysql` feature is enabled:
//!
//! ```toml
//! [dependencies]
//! mssql-pg-migrate = { version = "1.1", features = ["mysql"] }
//! ```
//!
//! # Supported Versions
//!
//! - MySQL 5.7+, 8.0+
//! - MariaDB 10.2+
//!
//! # Connection String
//!
//! Uses SQLx connection format:
//! ```text
//! mysql://user:password@host:port/database
//! ```

mod dialect;
mod reader;
mod writer;

pub use dialect::MysqlDialect;
pub use reader::MysqlReader;
pub use writer::MysqlWriter;
