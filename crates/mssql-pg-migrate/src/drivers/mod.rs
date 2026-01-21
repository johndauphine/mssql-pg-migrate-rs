//! Database driver implementations.
//!
//! This module provides database-specific implementations of the core traits:
//!
//! - [`mssql`]: Microsoft SQL Server driver
//! - [`postgres`]: PostgreSQL driver
//! - [`common`]: Shared utilities (TLS, connection helpers)
//!
//! # Architecture
//!
//! Each driver module implements:
//! - `Dialect`: SQL syntax strategy for the database engine
//!
//! Future additions (to be extracted from source/target modules):
//! - `SourceReader`: For reading schema and data from the database
//! - `TargetWriter`: For writing schema and data to the database
//!
//! # enum_dispatch
//!
//! This module uses `enum_dispatch` for zero-cost polymorphism. Instead of
//! dynamic dispatch via `Box<dyn Trait>`, we use enum variants that implement
//! traits directly, achieving the same abstraction with static dispatch.
//!
//! # Adding New Databases
//!
//! To add support for a new database:
//!
//! 1. Create a new module under `drivers/` (e.g., `drivers/mysql/`)
//! 2. Implement `Dialect` trait (and eventually `SourceReader`, `TargetWriter`)
//! 3. Add enum variant to `DialectImpl` (and other enums when implemented)
//! 4. Register type mappers in `DriverCatalog::with_builtins()`
//! 5. Gate the driver with a feature flag in `Cargo.toml`

pub mod common;
pub mod mssql;
pub mod postgres;

// Re-export common utilities
pub use common::{SslMode, TlsBuilder};

// Re-export driver types
pub use mssql::MssqlDialect;
pub use postgres::PostgresDialect;

use crate::core::traits::{Dialect, SelectQueryOptions};

/// Enum-based static dispatch for dialects.
///
/// This provides zero-cost polymorphism - the compiler generates
/// a match statement instead of using vtable dispatch.
///
/// Note: We use manual impl instead of enum_dispatch macro due to
/// cross-module trait complexities. The performance is identical.
#[derive(Debug, Clone)]
pub enum DialectImpl {
    Mssql(MssqlDialect),
    Postgres(PostgresDialect),
}

impl Dialect for DialectImpl {
    fn name(&self) -> &str {
        match self {
            DialectImpl::Mssql(d) => d.name(),
            DialectImpl::Postgres(d) => d.name(),
        }
    }

    fn quote_ident(&self, name: &str) -> String {
        match self {
            DialectImpl::Mssql(d) => d.quote_ident(name),
            DialectImpl::Postgres(d) => d.quote_ident(name),
        }
    }

    fn build_select_query(&self, opts: &SelectQueryOptions) -> String {
        match self {
            DialectImpl::Mssql(d) => d.build_select_query(opts),
            DialectImpl::Postgres(d) => d.build_select_query(opts),
        }
    }

    fn build_upsert_query(
        &self,
        target_table: &str,
        staging_table: &str,
        columns: &[String],
        pk_columns: &[String],
    ) -> String {
        match self {
            DialectImpl::Mssql(d) => {
                d.build_upsert_query(target_table, staging_table, columns, pk_columns)
            }
            DialectImpl::Postgres(d) => {
                d.build_upsert_query(target_table, staging_table, columns, pk_columns)
            }
        }
    }

    fn param_placeholder(&self, index: usize) -> String {
        match self {
            DialectImpl::Mssql(d) => d.param_placeholder(index),
            DialectImpl::Postgres(d) => d.param_placeholder(index),
        }
    }

    fn build_keyset_where(&self, pk_col: &str, last_pk: i64) -> String {
        match self {
            DialectImpl::Mssql(d) => d.build_keyset_where(pk_col, last_pk),
            DialectImpl::Postgres(d) => d.build_keyset_where(pk_col, last_pk),
        }
    }

    fn build_row_number_query(
        &self,
        inner_query: &str,
        pk_col: &str,
        start_row: i64,
        end_row: i64,
    ) -> String {
        match self {
            DialectImpl::Mssql(d) => {
                d.build_row_number_query(inner_query, pk_col, start_row, end_row)
            }
            DialectImpl::Postgres(d) => {
                d.build_row_number_query(inner_query, pk_col, start_row, end_row)
            }
        }
    }
}

// Note: SourceReaderImpl and TargetWriterImpl will be added once
// the reader/writer implementations are extracted from source/ and target/.
// For now, the existing SourcePoolImpl/TargetPoolImpl in orchestrator/pools.rs
// continue to function until migration is complete.

impl DialectImpl {
    /// Create a dialect implementation from a database type string.
    ///
    /// # Errors
    ///
    /// Returns an error if the database type is not recognized.
    pub fn from_db_type(db_type: &str) -> crate::error::Result<Self> {
        match db_type.to_lowercase().as_str() {
            "mssql" | "sqlserver" | "sql_server" => Ok(DialectImpl::Mssql(MssqlDialect::new())),
            "postgres" | "postgresql" | "pg" => Ok(DialectImpl::Postgres(PostgresDialect::new())),
            other => Err(crate::error::MigrateError::Config(format!(
                "Unknown database type: '{}'. Supported types: mssql, postgres",
                other
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Dialect trait must be in scope to call its methods on DialectImpl
    use crate::core::Dialect;

    #[test]
    fn test_dialect_impl_from_db_type() {
        let mssql = DialectImpl::from_db_type("mssql").unwrap();
        assert_eq!(mssql.name(), "mssql");

        let postgres = DialectImpl::from_db_type("postgres").unwrap();
        assert_eq!(postgres.name(), "postgres");

        // Alternative names
        assert!(DialectImpl::from_db_type("sqlserver").is_ok());
        assert!(DialectImpl::from_db_type("postgresql").is_ok());
        assert!(DialectImpl::from_db_type("pg").is_ok());

        // Unknown should error
        assert!(DialectImpl::from_db_type("unknown").is_err());
    }

    #[test]
    fn test_dialect_impl_enum_dispatch() {
        // Test that enum_dispatch properly delegates trait methods
        let dialect: DialectImpl = DialectImpl::Postgres(PostgresDialect::new());

        // These calls go through enum_dispatch, not vtable
        assert_eq!(dialect.name(), "postgres");
        assert_eq!(dialect.quote_ident("table"), "\"table\"");
        assert_eq!(dialect.param_placeholder(1), "$1");
    }

    #[test]
    fn test_dialect_impl_mssql() {
        let dialect: DialectImpl = DialectImpl::Mssql(MssqlDialect::new());

        assert_eq!(dialect.name(), "mssql");
        assert_eq!(dialect.quote_ident("table"), "[table]");
        assert_eq!(dialect.param_placeholder(1), "@P1");
    }
}
