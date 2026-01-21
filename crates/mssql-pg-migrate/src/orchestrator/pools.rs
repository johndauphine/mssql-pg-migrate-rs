//! Pool wrapper enums for bidirectional migration support.
//!
//! These enums provide static dispatch for source and target pool implementations,
//! avoiding trait objects while supporting all 4 migration directions:
//! - MSSQL → PostgreSQL
//! - PostgreSQL → MSSQL
//! - MSSQL → MSSQL
//! - PostgreSQL → PostgreSQL

use crate::config::AuthMethod;
use crate::config::Config;
#[cfg(not(feature = "kerberos"))]
use crate::error::MigrateError;
use crate::error::Result;
use crate::source::{
    CheckConstraint, ForeignKey, Index, MssqlPool, Partition, PgSourcePool, SourcePool, Table,
};
use crate::state::{DbStateBackend, MssqlStateBackend, StateBackend};
use crate::target::{MssqlTargetPool, PgPool, SqlValue, TargetPool, UpsertWriter};
use std::sync::Arc;
use tokio::sync::mpsc;

#[cfg(feature = "kerberos")]
use tracing::info;

/// Enum wrapper for source pool implementations.
pub enum SourcePoolImpl {
    Mssql(Arc<MssqlPool>),
    Postgres(Arc<PgSourcePool>),
}

impl SourcePoolImpl {
    /// Create source pool from configuration.
    pub async fn from_config(config: &Config) -> Result<Self> {
        let pool_size = config.migration.get_max_mssql_connections() as u32;
        let source_type = config.source.r#type.to_lowercase();

        if source_type == "postgres" || source_type == "postgresql" {
            // PostgreSQL source - native tokio-postgres only
            // Note: PostgreSQL Kerberos is not supported (tokio-postgres lacks GSSAPI)
            let pool = PgSourcePool::new(&config.source, pool_size as usize).await?;
            Ok(Self::Postgres(Arc::new(pool)))
        } else {
            // MSSQL source - uses native Tiberius driver
            if config.source.auth == AuthMethod::Kerberos {
                #[cfg(feature = "kerberos")]
                {
                    info!(
                        "Using Kerberos authentication via Tiberius GSSAPI for MSSQL source. \
                         Ensure you have a valid Kerberos ticket (kinit user@REALM)."
                    );
                }
                #[cfg(not(feature = "kerberos"))]
                {
                    return Err(MigrateError::Config(
                        "Kerberos authentication for MSSQL requires the 'kerberos' feature.\n\n\
                         Rebuild with: cargo build --features kerberos\n\n\
                         System requirements:\n\
                         - Linux: apt install libgssapi-krb5-2\n\
                         - macOS: Uses GSS.framework (built-in on macOS 10.14+)\n\
                         - Windows: Uses SSPI (built-in)\n\n\
                         Obtain a ticket before running: kinit user@REALM"
                            .into(),
                    ));
                }
            }
            // Use native Tiberius for all MSSQL connections
            let pool = MssqlPool::with_max_connections(config.source.clone(), pool_size).await?;
            Ok(Self::Mssql(Arc::new(pool)))
        }
    }

    /// Get the database type.
    pub fn db_type(&self) -> &str {
        match self {
            Self::Mssql(p) => p.db_type(),
            Self::Postgres(p) => p.db_type(),
        }
    }

    /// Extract schema information.
    pub async fn extract_schema(&self, schema: &str) -> Result<Vec<Table>> {
        match self {
            Self::Mssql(p) => p.extract_schema(schema).await,
            Self::Postgres(p) => p.extract_schema(schema).await,
        }
    }

    /// Get partition boundaries for parallel reads.
    pub async fn get_partition_boundaries(
        &self,
        table: &Table,
        num_partitions: usize,
    ) -> Result<Vec<Partition>> {
        match self {
            Self::Mssql(p) => p.get_partition_boundaries(table, num_partitions).await,
            Self::Postgres(p) => p.get_partition_boundaries(table, num_partitions).await,
        }
    }

    /// Load index metadata for a table.
    pub async fn load_indexes(&self, table: &mut Table) -> Result<()> {
        match self {
            Self::Mssql(p) => p.load_indexes(table).await,
            Self::Postgres(p) => p.load_indexes(table).await,
        }
    }

    /// Load foreign key metadata for a table.
    pub async fn load_foreign_keys(&self, table: &mut Table) -> Result<()> {
        match self {
            Self::Mssql(p) => p.load_foreign_keys(table).await,
            Self::Postgres(p) => p.load_foreign_keys(table).await,
        }
    }

    /// Load check constraint metadata for a table.
    pub async fn load_check_constraints(&self, table: &mut Table) -> Result<()> {
        match self {
            Self::Mssql(p) => p.load_check_constraints(table).await,
            Self::Postgres(p) => p.load_check_constraints(table).await,
        }
    }

    /// Get the row count for a table.
    pub async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        match self {
            Self::Mssql(p) => p.get_row_count(schema, table).await,
            Self::Postgres(p) => p.get_row_count(schema, table).await,
        }
    }

    /// Test the connection.
    pub async fn test_connection(&self) -> Result<()> {
        match self {
            Self::Mssql(p) => p.test_connection().await,
            Self::Postgres(p) => p.test_connection().await,
        }
    }

    /// Close all connections.
    pub async fn close(&self) {
        match self {
            Self::Mssql(p) => p.close().await,
            Self::Postgres(p) => p.close().await,
        }
    }

    // === Transfer methods ===

    /// Query rows with pre-computed column types.
    pub async fn query_rows_fast(
        &self,
        sql: &str,
        columns: &[String],
        col_types: &[String],
    ) -> Result<Vec<Vec<SqlValue>>> {
        match self {
            Self::Mssql(p) => p.query_rows_fast(sql, columns, col_types).await,
            Self::Postgres(p) => p.query_rows_fast(sql, columns, col_types).await,
        }
    }

    /// Query rows and encode directly to PostgreSQL COPY binary format.
    ///
    /// This is the high-performance path for MSSQL->PG upsert that bypasses
    /// SqlValue entirely. Returns (encoded_bytes, first_pk, last_pk, row_count).
    ///
    /// Only supported for MSSQL sources - returns None for PostgreSQL.
    pub async fn query_rows_direct_copy(
        &self,
        sql: &str,
        col_types: &[String],
        pk_idx: Option<usize>,
    ) -> Result<Option<(bytes::Bytes, Option<i64>, Option<i64>, usize)>> {
        match self {
            Self::Mssql(p) => {
                let result = p.query_rows_direct_copy(sql, col_types, pk_idx).await?;
                Ok(Some(result))
            }
            // Direct copy is only optimized for MSSQL sources
            Self::Postgres(_) => Ok(None),
        }
    }

    /// Returns true if this source supports direct copy encoding.
    pub fn supports_direct_copy(&self) -> bool {
        matches!(self, Self::Mssql(_))
    }

    /// Stream rows using PostgreSQL COPY TO BINARY protocol.
    /// Returns the total number of rows read.
    /// Only supported for PostgreSQL sources - returns an error for other sources.
    #[allow(clippy::too_many_arguments)]
    pub async fn copy_rows_binary(
        &self,
        schema: &str,
        table: &str,
        columns: &[String],
        col_types: &[String],
        pk_col: Option<&str>,
        min_pk: Option<i64>,
        max_pk: Option<i64>,
        tx: mpsc::Sender<Vec<Vec<SqlValue>>>,
        batch_size: usize,
    ) -> Result<i64> {
        match self {
            Self::Postgres(p) => {
                p.copy_rows_binary(
                    schema, table, columns, col_types, pk_col, min_pk, max_pk, tx, batch_size,
                )
                .await
            }
            // COPY TO BINARY is only supported for PostgreSQL sources
            _ => Err(crate::error::MigrateError::Config(
                "COPY TO BINARY is only supported for PostgreSQL sources".to_string(),
            )),
        }
    }

    /// Check if this source supports COPY TO BINARY streaming.
    pub fn supports_copy_binary(&self) -> bool {
        matches!(self, Self::Postgres(_))
    }

    /// Get the maximum value of a primary key column.
    pub async fn get_max_pk(&self, schema: &str, table: &str, pk_col: &str) -> Result<i64> {
        match self {
            Self::Mssql(p) => p.get_max_pk(schema, table, pk_col).await,
            Self::Postgres(p) => p.get_max_pk(schema, table, pk_col).await,
        }
    }

    /// Get the underlying MSSQL pool (for transfer engine compatibility).
    pub fn as_mssql(&self) -> Option<Arc<MssqlPool>> {
        match self {
            Self::Mssql(p) => Some(p.clone()),
            Self::Postgres(_) => None,
        }
    }

    /// Get the underlying PostgreSQL source pool.
    pub fn as_postgres(&self) -> Option<Arc<PgSourcePool>> {
        match self {
            Self::Mssql(_) => None,
            Self::Postgres(p) => Some(p.clone()),
        }
    }

    /// Check if this is an ODBC-based source.
    /// Always returns false - ODBC support has been removed.
    pub fn is_odbc(&self) -> bool {
        false
    }
}

/// Enum wrapper for target pool implementations.
pub enum TargetPoolImpl {
    Mssql(Arc<MssqlTargetPool>),
    Postgres(Arc<PgPool>),
}

impl TargetPoolImpl {
    /// Create target pool from configuration.
    pub async fn from_config(config: &Config) -> Result<Self> {
        let max_conns = config.migration.get_max_pg_connections();
        let target_type = config.target.r#type.to_lowercase();

        if target_type == "mssql" {
            // MSSQL target - uses native Tiberius driver
            if config.target.auth == AuthMethod::Kerberos {
                #[cfg(feature = "kerberos")]
                {
                    info!(
                        "Using Kerberos authentication via Tiberius GSSAPI for MSSQL target. \
                         Ensure you have a valid Kerberos ticket (kinit user@REALM)."
                    );
                }
                #[cfg(not(feature = "kerberos"))]
                {
                    return Err(MigrateError::Config(
                        "Kerberos authentication for MSSQL requires the 'kerberos' feature.\n\n\
                         Rebuild with: cargo build --features kerberos\n\n\
                         System requirements:\n\
                         - Linux: apt install libgssapi-krb5-2\n\
                         - macOS: Uses GSS.framework (built-in on macOS 10.14+)\n\
                         - Windows: Uses SSPI (built-in)\n\n\
                         Obtain a ticket before running: kinit user@REALM"
                            .into(),
                    ));
                }
            }
            let pool = MssqlTargetPool::new(&config.target, max_conns as u32).await?;
            Ok(Self::Mssql(Arc::new(pool)))
        } else {
            // PostgreSQL target - native tokio-postgres only
            // Note: PostgreSQL Kerberos is not supported (tokio-postgres lacks GSSAPI)
            let pool = PgPool::new(
                &config.target,
                max_conns,
                config.migration.get_copy_buffer_rows(),
                config.migration.use_binary_copy,
            )
            .await?;
            Ok(Self::Postgres(Arc::new(pool)))
        }
    }

    /// Get the database type.
    pub fn db_type(&self) -> &str {
        match self {
            Self::Mssql(p) => p.db_type(),
            Self::Postgres(p) => p.db_type(),
        }
    }

    /// Create a state backend appropriate for this target database type.
    pub fn create_state_backend(&self) -> Result<StateBackend> {
        match self {
            Self::Postgres(p) => Ok(StateBackend::Postgres(DbStateBackend::new(p.pool().clone()))),
            Self::Mssql(p) => Ok(StateBackend::Mssql(MssqlStateBackend::new(Arc::clone(p)))),
        }
    }

    /// Create a schema if it doesn't exist.
    pub async fn create_schema(&self, schema: &str) -> Result<()> {
        match self {
            Self::Mssql(p) => p.create_schema(schema).await,
            Self::Postgres(p) => p.create_schema(schema).await,
        }
    }

    /// Create a table from metadata.
    pub async fn create_table(&self, table: &Table, target_schema: &str) -> Result<()> {
        match self {
            Self::Mssql(p) => p.create_table(table, target_schema).await,
            Self::Postgres(p) => p.create_table(table, target_schema).await,
        }
    }

    /// Create a table with optional UNLOGGED.
    pub async fn create_table_unlogged(&self, table: &Table, target_schema: &str) -> Result<()> {
        match self {
            Self::Mssql(p) => p.create_table_unlogged(table, target_schema).await,
            Self::Postgres(p) => p.create_table_unlogged(table, target_schema).await,
        }
    }

    /// Drop a table if it exists.
    pub async fn drop_table(&self, schema: &str, table: &str) -> Result<()> {
        match self {
            Self::Mssql(p) => p.drop_table(schema, table).await,
            Self::Postgres(p) => p.drop_table(schema, table).await,
        }
    }

    /// Truncate a table.
    pub async fn truncate_table(&self, schema: &str, table: &str) -> Result<()> {
        match self {
            Self::Mssql(p) => p.truncate_table(schema, table).await,
            Self::Postgres(p) => p.truncate_table(schema, table).await,
        }
    }

    /// Check if a table exists.
    pub async fn table_exists(&self, schema: &str, table: &str) -> Result<bool> {
        match self {
            Self::Mssql(p) => p.table_exists(schema, table).await,
            Self::Postgres(p) => p.table_exists(schema, table).await,
        }
    }

    /// Create a primary key constraint.
    pub async fn create_primary_key(&self, table: &Table, target_schema: &str) -> Result<()> {
        match self {
            Self::Mssql(p) => p.create_primary_key(table, target_schema).await,
            Self::Postgres(p) => p.create_primary_key(table, target_schema).await,
        }
    }

    /// Create an index.
    pub async fn create_index(
        &self,
        table: &Table,
        idx: &Index,
        target_schema: &str,
    ) -> Result<()> {
        match self {
            Self::Mssql(p) => p.create_index(table, idx, target_schema).await,
            Self::Postgres(p) => p.create_index(table, idx, target_schema).await,
        }
    }

    /// Drop all non-PK indexes on a table.
    pub async fn drop_non_pk_indexes(&self, schema: &str, table: &str) -> Result<Vec<String>> {
        match self {
            Self::Mssql(p) => p.drop_non_pk_indexes(schema, table).await,
            Self::Postgres(p) => p.drop_non_pk_indexes(schema, table).await,
        }
    }

    /// Create a foreign key constraint.
    pub async fn create_foreign_key(
        &self,
        table: &Table,
        fk: &ForeignKey,
        target_schema: &str,
    ) -> Result<()> {
        match self {
            Self::Mssql(p) => p.create_foreign_key(table, fk, target_schema).await,
            Self::Postgres(p) => p.create_foreign_key(table, fk, target_schema).await,
        }
    }

    /// Create a check constraint.
    pub async fn create_check_constraint(
        &self,
        table: &Table,
        chk: &CheckConstraint,
        target_schema: &str,
    ) -> Result<()> {
        match self {
            Self::Mssql(p) => p.create_check_constraint(table, chk, target_schema).await,
            Self::Postgres(p) => p.create_check_constraint(table, chk, target_schema).await,
        }
    }

    /// Check if a table has a primary key.
    pub async fn has_primary_key(&self, schema: &str, table: &str) -> Result<bool> {
        match self {
            Self::Mssql(p) => p.has_primary_key(schema, table).await,
            Self::Postgres(p) => p.has_primary_key(schema, table).await,
        }
    }

    /// Get the row count for a table.
    pub async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        match self {
            Self::Mssql(p) => p.get_row_count(schema, table).await,
            Self::Postgres(p) => p.get_row_count(schema, table).await,
        }
    }

    /// Reset sequence to max value.
    pub async fn reset_sequence(&self, schema: &str, table: &Table) -> Result<()> {
        match self {
            Self::Mssql(p) => p.reset_sequence(schema, table).await,
            Self::Postgres(p) => p.reset_sequence(schema, table).await,
        }
    }

    /// Set table to LOGGED mode.
    pub async fn set_table_logged(&self, schema: &str, table: &str) -> Result<()> {
        match self {
            Self::Mssql(p) => p.set_table_logged(schema, table).await,
            Self::Postgres(p) => p.set_table_logged(schema, table).await,
        }
    }

    /// Set table to UNLOGGED mode.
    pub async fn set_table_unlogged(&self, schema: &str, table: &str) -> Result<()> {
        match self {
            Self::Mssql(p) => p.set_table_unlogged(schema, table).await,
            Self::Postgres(p) => p.set_table_unlogged(schema, table).await,
        }
    }

    /// Write a chunk of rows.
    pub async fn write_chunk(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        rows: Vec<Vec<SqlValue>>,
    ) -> Result<u64> {
        match self {
            Self::Mssql(p) => p.write_chunk(schema, table, cols, rows).await,
            Self::Postgres(p) => p.write_chunk(schema, table, cols, rows).await,
        }
    }

    /// Upsert a chunk of rows.
    pub async fn upsert_chunk(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        pk_cols: &[String],
        rows: Vec<Vec<SqlValue>>,
        writer_id: usize,
        partition_id: Option<i32>,
    ) -> Result<u64> {
        match self {
            Self::Mssql(p) => {
                p.upsert_chunk(schema, table, cols, pk_cols, rows, writer_id, partition_id)
                    .await
            }
            Self::Postgres(p) => {
                p.upsert_chunk(schema, table, cols, pk_cols, rows, writer_id, partition_id)
                    .await
            }
        }
    }

    /// Get a dedicated upsert writer for a specific table/partition.
    pub async fn get_upsert_writer(
        &self,
        schema: &str,
        table: &str,
        writer_id: usize,
        partition_id: Option<i32>,
    ) -> Result<Box<dyn UpsertWriter>> {
        match self {
            Self::Mssql(p) => {
                p.get_upsert_writer(schema, table, writer_id, partition_id)
                    .await
            }
            Self::Postgres(p) => {
                p.get_upsert_writer(schema, table, writer_id, partition_id)
                    .await
            }
        }
    }

    /// Test the connection.
    pub async fn test_connection(&self) -> Result<()> {
        match self {
            Self::Mssql(_) => Ok(()),
            Self::Postgres(p) => p.test_connection().await,
        }
    }

    /// Close all connections.
    pub async fn close(&self) {
        match self {
            Self::Mssql(p) => p.close().await,
            Self::Postgres(p) => p.close().await,
        }
    }

    /// Get the underlying PostgreSQL pool.
    pub fn as_postgres(&self) -> Option<Arc<PgPool>> {
        match self {
            Self::Mssql(_) => None,
            Self::Postgres(p) => Some(p.clone()),
        }
    }

    /// Get the underlying MSSQL target pool.
    pub fn as_mssql(&self) -> Option<Arc<MssqlTargetPool>> {
        match self {
            Self::Mssql(p) => Some(p.clone()),
            Self::Postgres(_) => None,
        }
    }

    /// Check if this is an ODBC-based target.
    /// Always returns false - ODBC support has been removed.
    pub fn is_odbc(&self) -> bool {
        false
    }
}

impl Clone for SourcePoolImpl {
    fn clone(&self) -> Self {
        match self {
            Self::Mssql(p) => Self::Mssql(p.clone()),
            Self::Postgres(p) => Self::Postgres(p.clone()),
        }
    }
}

impl Clone for TargetPoolImpl {
    fn clone(&self) -> Self {
        match self {
            Self::Mssql(p) => Self::Mssql(p.clone()),
            Self::Postgres(p) => Self::Postgres(p.clone()),
        }
    }
}
