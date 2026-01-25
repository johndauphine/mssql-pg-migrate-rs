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
//! 2. Implement `Dialect`, `SourceReader`, and `TargetWriter` traits
//! 3. Add enum variant to `DialectImpl`, `SourceReaderImpl`, and `TargetWriterImpl`
//! 4. Register type mappers in `DriverCatalog::with_builtins()`
//! 5. Gate the driver with a feature flag in `Cargo.toml`

pub mod common;
pub mod mssql;
#[cfg(feature = "mysql")]
pub mod mysql;
pub mod postgres;

// Re-export common utilities
pub use common::{SslMode, TlsBuilder};

// Re-export driver types
pub use mssql::{MssqlDialect, MssqlReader, MssqlWriter, TiberiusConnectionManager};
#[cfg(feature = "mysql")]
pub use mysql::{MysqlDialect, MysqlReader, MysqlWriter};
pub use postgres::{PostgresDialect, PostgresReader, PostgresWriter};

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::core::schema::{CheckConstraint, ForeignKey, Index, Partition, Table};
use crate::core::traits::{Dialect, ReadOptions, SelectQueryOptions, SourceReader, TargetWriter};
use crate::core::value::Batch;
use crate::error::Result;

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
    #[cfg(feature = "mysql")]
    Mysql(MysqlDialect),
}

impl Dialect for DialectImpl {
    fn name(&self) -> &str {
        match self {
            DialectImpl::Mssql(d) => d.name(),
            DialectImpl::Postgres(d) => d.name(),
            #[cfg(feature = "mysql")]
            DialectImpl::Mysql(d) => d.name(),
        }
    }

    fn quote_ident(&self, name: &str) -> String {
        match self {
            DialectImpl::Mssql(d) => d.quote_ident(name),
            DialectImpl::Postgres(d) => d.quote_ident(name),
            #[cfg(feature = "mysql")]
            DialectImpl::Mysql(d) => d.quote_ident(name),
        }
    }

    fn build_select_query(&self, opts: &SelectQueryOptions) -> String {
        match self {
            DialectImpl::Mssql(d) => d.build_select_query(opts),
            DialectImpl::Postgres(d) => d.build_select_query(opts),
            #[cfg(feature = "mysql")]
            DialectImpl::Mysql(d) => d.build_select_query(opts),
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
            #[cfg(feature = "mysql")]
            DialectImpl::Mysql(d) => {
                d.build_upsert_query(target_table, staging_table, columns, pk_columns)
            }
        }
    }

    fn param_placeholder(&self, index: usize) -> String {
        match self {
            DialectImpl::Mssql(d) => d.param_placeholder(index),
            DialectImpl::Postgres(d) => d.param_placeholder(index),
            #[cfg(feature = "mysql")]
            DialectImpl::Mysql(d) => d.param_placeholder(index),
        }
    }

    fn build_keyset_where(&self, pk_col: &str, last_pk: i64) -> String {
        match self {
            DialectImpl::Mssql(d) => d.build_keyset_where(pk_col, last_pk),
            DialectImpl::Postgres(d) => d.build_keyset_where(pk_col, last_pk),
            #[cfg(feature = "mysql")]
            DialectImpl::Mysql(d) => d.build_keyset_where(pk_col, last_pk),
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
            #[cfg(feature = "mysql")]
            DialectImpl::Mysql(d) => {
                d.build_row_number_query(inner_query, pk_col, start_row, end_row)
            }
        }
    }
}

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
            #[cfg(feature = "mysql")]
            "mysql" | "mariadb" => Ok(DialectImpl::Mysql(MysqlDialect::new())),
            other => {
                #[cfg(feature = "mysql")]
                let supported = "mssql, postgres, mysql";
                #[cfg(not(feature = "mysql"))]
                let supported = "mssql, postgres (enable 'mysql' feature for MySQL support)";
                Err(crate::error::MigrateError::Config(format!(
                    "Unknown database type: '{}'. Supported types: {}",
                    other, supported
                )))
            }
        }
    }
}

// =============================================================================
// SourceReaderImpl - Enum-based dispatch for source readers
// =============================================================================

/// Enum-based static dispatch for source readers.
///
/// Provides zero-cost polymorphism for reading data from different
/// source database engines. Uses Arc internally for cheap cloning.
#[derive(Clone)]
pub enum SourceReaderImpl {
    /// Microsoft SQL Server source
    Mssql(Arc<MssqlReader>),
    /// PostgreSQL source
    Postgres(Arc<PostgresReader>),
    /// MySQL/MariaDB source
    #[cfg(feature = "mysql")]
    Mysql(Arc<MysqlReader>),
}

#[async_trait]
impl SourceReader for SourceReaderImpl {
    async fn extract_schema(&self, schema: &str) -> Result<Vec<Table>> {
        match self {
            SourceReaderImpl::Mssql(r) => r.extract_schema(schema).await,
            SourceReaderImpl::Postgres(r) => r.extract_schema(schema).await,
            #[cfg(feature = "mysql")]
            SourceReaderImpl::Mysql(r) => r.extract_schema(schema).await,
        }
    }

    async fn load_indexes(&self, table: &mut Table) -> Result<()> {
        match self {
            SourceReaderImpl::Mssql(r) => r.load_indexes(table).await,
            SourceReaderImpl::Postgres(r) => r.load_indexes(table).await,
            #[cfg(feature = "mysql")]
            SourceReaderImpl::Mysql(r) => r.load_indexes(table).await,
        }
    }

    async fn load_foreign_keys(&self, table: &mut Table) -> Result<()> {
        match self {
            SourceReaderImpl::Mssql(r) => r.load_foreign_keys(table).await,
            SourceReaderImpl::Postgres(r) => r.load_foreign_keys(table).await,
            #[cfg(feature = "mysql")]
            SourceReaderImpl::Mysql(r) => r.load_foreign_keys(table).await,
        }
    }

    async fn load_check_constraints(&self, table: &mut Table) -> Result<()> {
        match self {
            SourceReaderImpl::Mssql(r) => r.load_check_constraints(table).await,
            SourceReaderImpl::Postgres(r) => r.load_check_constraints(table).await,
            #[cfg(feature = "mysql")]
            SourceReaderImpl::Mysql(r) => r.load_check_constraints(table).await,
        }
    }

    fn read_table(&self, opts: ReadOptions) -> mpsc::Receiver<Result<Batch>> {
        match self {
            SourceReaderImpl::Mssql(r) => r.read_table(opts),
            SourceReaderImpl::Postgres(r) => r.read_table(opts),
            #[cfg(feature = "mysql")]
            SourceReaderImpl::Mysql(r) => r.read_table(opts),
        }
    }

    async fn get_partition_boundaries(
        &self,
        table: &Table,
        num_partitions: usize,
    ) -> Result<Vec<Partition>> {
        match self {
            SourceReaderImpl::Mssql(r) => r.get_partition_boundaries(table, num_partitions).await,
            SourceReaderImpl::Postgres(r) => {
                r.get_partition_boundaries(table, num_partitions).await
            }
            #[cfg(feature = "mysql")]
            SourceReaderImpl::Mysql(r) => r.get_partition_boundaries(table, num_partitions).await,
        }
    }

    async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        match self {
            SourceReaderImpl::Mssql(r) => r.get_row_count(schema, table).await,
            SourceReaderImpl::Postgres(r) => r.get_row_count(schema, table).await,
            #[cfg(feature = "mysql")]
            SourceReaderImpl::Mysql(r) => r.get_row_count(schema, table).await,
        }
    }

    async fn get_max_pk(&self, schema: &str, table: &str, pk_col: &str) -> Result<i64> {
        match self {
            SourceReaderImpl::Mssql(r) => r.get_max_pk(schema, table, pk_col).await,
            SourceReaderImpl::Postgres(r) => r.get_max_pk(schema, table, pk_col).await,
            #[cfg(feature = "mysql")]
            SourceReaderImpl::Mysql(r) => r.get_max_pk(schema, table, pk_col).await,
        }
    }

    fn db_type(&self) -> &str {
        match self {
            SourceReaderImpl::Mssql(r) => r.db_type(),
            SourceReaderImpl::Postgres(r) => r.db_type(),
            #[cfg(feature = "mysql")]
            SourceReaderImpl::Mysql(r) => r.db_type(),
        }
    }

    async fn close(&self) {
        match self {
            SourceReaderImpl::Mssql(r) => r.close().await,
            SourceReaderImpl::Postgres(r) => r.close().await,
            #[cfg(feature = "mysql")]
            SourceReaderImpl::Mysql(r) => r.close().await,
        }
    }
}

impl SourceReaderImpl {
    /// Get the database type string.
    pub fn db_type_str(&self) -> &'static str {
        match self {
            SourceReaderImpl::Mssql(_) => "mssql",
            SourceReaderImpl::Postgres(_) => "postgres",
            #[cfg(feature = "mysql")]
            SourceReaderImpl::Mysql(_) => "mysql",
        }
    }

    /// Returns true if this source supports COPY TO BINARY streaming.
    pub fn supports_copy_binary(&self) -> bool {
        matches!(self, SourceReaderImpl::Postgres(_))
    }
}

// =============================================================================
// TargetWriterImpl - Enum-based dispatch for target writers
// =============================================================================

/// Enum-based static dispatch for target writers.
///
/// Provides zero-cost polymorphism for writing data to different
/// target database engines. Uses Arc internally for cheap cloning.
#[derive(Clone)]
pub enum TargetWriterImpl {
    /// Microsoft SQL Server target
    Mssql(Arc<MssqlWriter>),
    /// PostgreSQL target
    Postgres(Arc<PostgresWriter>),
    /// MySQL/MariaDB target
    #[cfg(feature = "mysql")]
    Mysql(Arc<MysqlWriter>),
}

#[async_trait]
impl TargetWriter for TargetWriterImpl {
    // ===== Schema Operations =====

    async fn create_schema(&self, schema: &str) -> Result<()> {
        match self {
            TargetWriterImpl::Mssql(w) => w.create_schema(schema).await,
            TargetWriterImpl::Postgres(w) => w.create_schema(schema).await,
            #[cfg(feature = "mysql")]
            TargetWriterImpl::Mysql(w) => w.create_schema(schema).await,
        }
    }

    async fn create_table(&self, table: &Table, target_schema: &str) -> Result<()> {
        match self {
            TargetWriterImpl::Mssql(w) => w.create_table(table, target_schema).await,
            TargetWriterImpl::Postgres(w) => w.create_table(table, target_schema).await,
            #[cfg(feature = "mysql")]
            TargetWriterImpl::Mysql(w) => w.create_table(table, target_schema).await,
        }
    }

    async fn create_table_unlogged(&self, table: &Table, target_schema: &str) -> Result<()> {
        match self {
            TargetWriterImpl::Mssql(w) => w.create_table_unlogged(table, target_schema).await,
            TargetWriterImpl::Postgres(w) => w.create_table_unlogged(table, target_schema).await,
            #[cfg(feature = "mysql")]
            TargetWriterImpl::Mysql(w) => w.create_table_unlogged(table, target_schema).await,
        }
    }

    async fn drop_table(&self, schema: &str, table: &str) -> Result<()> {
        match self {
            TargetWriterImpl::Mssql(w) => w.drop_table(schema, table).await,
            TargetWriterImpl::Postgres(w) => w.drop_table(schema, table).await,
            #[cfg(feature = "mysql")]
            TargetWriterImpl::Mysql(w) => w.drop_table(schema, table).await,
        }
    }

    async fn table_exists(&self, schema: &str, table: &str) -> Result<bool> {
        match self {
            TargetWriterImpl::Mssql(w) => w.table_exists(schema, table).await,
            TargetWriterImpl::Postgres(w) => w.table_exists(schema, table).await,
            #[cfg(feature = "mysql")]
            TargetWriterImpl::Mysql(w) => w.table_exists(schema, table).await,
        }
    }

    // ===== Constraint Operations =====

    async fn create_primary_key(&self, table: &Table, target_schema: &str) -> Result<()> {
        match self {
            TargetWriterImpl::Mssql(w) => w.create_primary_key(table, target_schema).await,
            TargetWriterImpl::Postgres(w) => w.create_primary_key(table, target_schema).await,
            #[cfg(feature = "mysql")]
            TargetWriterImpl::Mysql(w) => w.create_primary_key(table, target_schema).await,
        }
    }

    async fn create_index(&self, table: &Table, idx: &Index, target_schema: &str) -> Result<()> {
        match self {
            TargetWriterImpl::Mssql(w) => w.create_index(table, idx, target_schema).await,
            TargetWriterImpl::Postgres(w) => w.create_index(table, idx, target_schema).await,
            #[cfg(feature = "mysql")]
            TargetWriterImpl::Mysql(w) => w.create_index(table, idx, target_schema).await,
        }
    }

    async fn drop_non_pk_indexes(&self, schema: &str, table: &str) -> Result<Vec<String>> {
        match self {
            TargetWriterImpl::Mssql(w) => w.drop_non_pk_indexes(schema, table).await,
            TargetWriterImpl::Postgres(w) => w.drop_non_pk_indexes(schema, table).await,
            #[cfg(feature = "mysql")]
            TargetWriterImpl::Mysql(w) => w.drop_non_pk_indexes(schema, table).await,
        }
    }

    async fn create_foreign_key(
        &self,
        table: &Table,
        fk: &ForeignKey,
        target_schema: &str,
    ) -> Result<()> {
        match self {
            TargetWriterImpl::Mssql(w) => w.create_foreign_key(table, fk, target_schema).await,
            TargetWriterImpl::Postgres(w) => w.create_foreign_key(table, fk, target_schema).await,
            #[cfg(feature = "mysql")]
            TargetWriterImpl::Mysql(w) => w.create_foreign_key(table, fk, target_schema).await,
        }
    }

    async fn create_check_constraint(
        &self,
        table: &Table,
        chk: &CheckConstraint,
        target_schema: &str,
    ) -> Result<()> {
        match self {
            TargetWriterImpl::Mssql(w) => {
                w.create_check_constraint(table, chk, target_schema).await
            }
            TargetWriterImpl::Postgres(w) => {
                w.create_check_constraint(table, chk, target_schema).await
            }
            #[cfg(feature = "mysql")]
            TargetWriterImpl::Mysql(w) => {
                w.create_check_constraint(table, chk, target_schema).await
            }
        }
    }

    // ===== Data Operations =====

    async fn write_batch(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        batch: Batch,
    ) -> Result<u64> {
        match self {
            TargetWriterImpl::Mssql(w) => w.write_batch(schema, table, cols, batch).await,
            TargetWriterImpl::Postgres(w) => w.write_batch(schema, table, cols, batch).await,
            #[cfg(feature = "mysql")]
            TargetWriterImpl::Mysql(w) => w.write_batch(schema, table, cols, batch).await,
        }
    }

    async fn upsert_batch(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        pk_cols: &[String],
        batch: Batch,
        writer_id: usize,
        partition_id: Option<i32>,
    ) -> Result<u64> {
        match self {
            TargetWriterImpl::Mssql(w) => {
                w.upsert_batch(schema, table, cols, pk_cols, batch, writer_id, partition_id)
                    .await
            }
            TargetWriterImpl::Postgres(w) => {
                w.upsert_batch(schema, table, cols, pk_cols, batch, writer_id, partition_id)
                    .await
            }
            #[cfg(feature = "mysql")]
            TargetWriterImpl::Mysql(w) => {
                w.upsert_batch(schema, table, cols, pk_cols, batch, writer_id, partition_id)
                    .await
            }
        }
    }

    // ===== Utility Operations =====

    async fn has_primary_key(&self, schema: &str, table: &str) -> Result<bool> {
        match self {
            TargetWriterImpl::Mssql(w) => w.has_primary_key(schema, table).await,
            TargetWriterImpl::Postgres(w) => w.has_primary_key(schema, table).await,
            #[cfg(feature = "mysql")]
            TargetWriterImpl::Mysql(w) => w.has_primary_key(schema, table).await,
        }
    }

    async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        match self {
            TargetWriterImpl::Mssql(w) => w.get_row_count(schema, table).await,
            TargetWriterImpl::Postgres(w) => w.get_row_count(schema, table).await,
            #[cfg(feature = "mysql")]
            TargetWriterImpl::Mysql(w) => w.get_row_count(schema, table).await,
        }
    }

    async fn reset_sequence(&self, schema: &str, table: &Table) -> Result<()> {
        match self {
            TargetWriterImpl::Mssql(w) => w.reset_sequence(schema, table).await,
            TargetWriterImpl::Postgres(w) => w.reset_sequence(schema, table).await,
            #[cfg(feature = "mysql")]
            TargetWriterImpl::Mysql(w) => w.reset_sequence(schema, table).await,
        }
    }

    async fn set_table_logged(&self, schema: &str, table: &str) -> Result<()> {
        match self {
            TargetWriterImpl::Mssql(w) => w.set_table_logged(schema, table).await,
            TargetWriterImpl::Postgres(w) => w.set_table_logged(schema, table).await,
            #[cfg(feature = "mysql")]
            TargetWriterImpl::Mysql(w) => w.set_table_logged(schema, table).await,
        }
    }

    async fn set_table_unlogged(&self, schema: &str, table: &str) -> Result<()> {
        match self {
            TargetWriterImpl::Mssql(w) => w.set_table_unlogged(schema, table).await,
            TargetWriterImpl::Postgres(w) => w.set_table_unlogged(schema, table).await,
            #[cfg(feature = "mysql")]
            TargetWriterImpl::Mysql(w) => w.set_table_unlogged(schema, table).await,
        }
    }

    fn db_type(&self) -> &str {
        match self {
            TargetWriterImpl::Mssql(w) => w.db_type(),
            TargetWriterImpl::Postgres(w) => w.db_type(),
            #[cfg(feature = "mysql")]
            TargetWriterImpl::Mysql(w) => w.db_type(),
        }
    }

    async fn close(&self) {
        match self {
            TargetWriterImpl::Mssql(w) => w.close().await,
            TargetWriterImpl::Postgres(w) => w.close().await,
            #[cfg(feature = "mysql")]
            TargetWriterImpl::Mysql(w) => w.close().await,
        }
    }
}

impl TargetWriterImpl {
    /// Get the database type string.
    pub fn db_type_str(&self) -> &'static str {
        match self {
            TargetWriterImpl::Mssql(_) => "mssql",
            TargetWriterImpl::Postgres(_) => "postgres",
            #[cfg(feature = "mysql")]
            TargetWriterImpl::Mysql(_) => "mysql",
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
