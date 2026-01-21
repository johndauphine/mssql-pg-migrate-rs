//! Pool wrapper enums for bidirectional migration support.
//!
//! These enums provide static dispatch for source and target pool implementations,
//! avoiding trait objects while supporting all migration directions:
//! - MSSQL → PostgreSQL
//! - PostgreSQL → MSSQL
//! - MSSQL → MSSQL
//! - PostgreSQL → PostgreSQL
//! - MySQL → PostgreSQL (requires `mysql` feature)
//! - MySQL → MSSQL (requires `mysql` feature)
//! - MSSQL → MySQL (requires `mysql` feature)
//! - PostgreSQL → MySQL (requires `mysql` feature)

use crate::config::AuthMethod;
use crate::config::Config;
#[cfg(not(feature = "kerberos"))]
use crate::error::MigrateError;
use crate::error::Result;
use crate::source::{
    CheckConstraint, ForeignKey, Index, MssqlPool, Partition, PgSourcePool, SourcePool, Table,
};
use crate::state::{DbStateBackend, MssqlStateBackend, NoOpStateBackend, StateBackendEnum};
use crate::target::{MssqlTargetPool, PgPool, SqlValue, TargetPool, UpsertWriter};
use std::sync::Arc;
use tokio::sync::mpsc;

#[cfg(feature = "mysql")]
use crate::drivers::{MysqlReader, MysqlWriter};
#[cfg(feature = "mysql")]
use crate::core::traits::{SourceReader as NewSourceReader, TargetWriter as NewTargetWriter};
#[cfg(feature = "mysql")]
use crate::core::value::{Batch, SqlValue as NewSqlValue, SqlNullType as NewSqlNullType};
#[cfg(feature = "mysql")]
use std::borrow::Cow;

#[cfg(feature = "kerberos")]
use tracing::info;
#[cfg(feature = "mysql")]
use tracing::info as mysql_info;

/// Convert old SqlValue to new SqlValue<'static> for MySQL compatibility.
#[cfg(feature = "mysql")]
fn convert_sql_value(old: SqlValue) -> NewSqlValue<'static> {
    use crate::target::SqlNullType as OldSqlNullType;

    match old {
        SqlValue::Null(nt) => {
            let new_nt = match nt {
                OldSqlNullType::Bool => NewSqlNullType::Bool,
                OldSqlNullType::I16 => NewSqlNullType::I16,
                OldSqlNullType::I32 => NewSqlNullType::I32,
                OldSqlNullType::I64 => NewSqlNullType::I64,
                OldSqlNullType::F32 => NewSqlNullType::F32,
                OldSqlNullType::F64 => NewSqlNullType::F64,
                OldSqlNullType::String => NewSqlNullType::String,
                OldSqlNullType::Bytes => NewSqlNullType::Bytes,
                OldSqlNullType::Uuid => NewSqlNullType::Uuid,
                OldSqlNullType::Decimal => NewSqlNullType::Decimal,
                OldSqlNullType::DateTime => NewSqlNullType::DateTime,
                OldSqlNullType::DateTimeOffset => NewSqlNullType::DateTimeOffset,
                OldSqlNullType::Date => NewSqlNullType::Date,
                OldSqlNullType::Time => NewSqlNullType::Time,
            };
            NewSqlValue::Null(new_nt)
        }
        SqlValue::Bool(v) => NewSqlValue::Bool(v),
        SqlValue::I16(v) => NewSqlValue::I16(v),
        SqlValue::I32(v) => NewSqlValue::I32(v),
        SqlValue::I64(v) => NewSqlValue::I64(v),
        SqlValue::F32(v) => NewSqlValue::F32(v),
        SqlValue::F64(v) => NewSqlValue::F64(v),
        SqlValue::String(v) => NewSqlValue::Text(Cow::Owned(v)),
        SqlValue::Bytes(v) => NewSqlValue::Bytes(Cow::Owned(v)),
        SqlValue::Uuid(v) => NewSqlValue::Uuid(v),
        SqlValue::Decimal(v) => NewSqlValue::Decimal(v),
        SqlValue::DateTime(v) => NewSqlValue::DateTime(v),
        SqlValue::DateTimeOffset(v) => NewSqlValue::DateTimeOffset(v),
        SqlValue::Date(v) => NewSqlValue::Date(v),
        SqlValue::Time(v) => NewSqlValue::Time(v),
    }
}

/// Enum wrapper for source pool implementations.
pub enum SourcePoolImpl {
    Mssql(Arc<MssqlPool>),
    Postgres(Arc<PgSourcePool>),
    #[cfg(feature = "mysql")]
    Mysql(Arc<MysqlReader>),
}

impl SourcePoolImpl {
    /// Create source pool from configuration.
    pub async fn from_config(config: &Config) -> Result<Self> {
        let pool_size = config.migration.get_max_mssql_connections() as u32;
        let source_type = config.source.r#type.to_lowercase();

        if source_type == "postgres" || source_type == "postgresql" || source_type == "pg" {
            // PostgreSQL source - native tokio-postgres only
            // Note: PostgreSQL Kerberos is not supported (tokio-postgres lacks GSSAPI)
            let pool = PgSourcePool::new(&config.source, pool_size as usize).await?;
            Ok(Self::Postgres(Arc::new(pool)))
        } else if source_type == "mysql" || source_type == "mariadb" {
            // MySQL/MariaDB source - uses SQLx
            #[cfg(feature = "mysql")]
            {
                mysql_info!("Creating MySQL source connection pool");
                let reader = MysqlReader::new(&config.source, pool_size as usize).await?;
                Ok(Self::Mysql(Arc::new(reader)))
            }
            #[cfg(not(feature = "mysql"))]
            {
                Err(MigrateError::Config(
                    "MySQL source requires the 'mysql' feature.\n\n\
                     Rebuild with: cargo build --features mysql"
                        .into(),
                ))
            }
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
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => p.db_type(),
        }
    }

    /// Extract schema information.
    pub async fn extract_schema(&self, schema: &str) -> Result<Vec<Table>> {
        match self {
            Self::Mssql(p) => p.extract_schema(schema).await,
            Self::Postgres(p) => p.extract_schema(schema).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewSourceReader::extract_schema(p.as_ref(), schema).await,
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
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewSourceReader::get_partition_boundaries(p.as_ref(), table, num_partitions).await,
        }
    }

    /// Load index metadata for a table.
    pub async fn load_indexes(&self, table: &mut Table) -> Result<()> {
        match self {
            Self::Mssql(p) => p.load_indexes(table).await,
            Self::Postgres(p) => p.load_indexes(table).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewSourceReader::load_indexes(p.as_ref(), table).await,
        }
    }

    /// Load foreign key metadata for a table.
    pub async fn load_foreign_keys(&self, table: &mut Table) -> Result<()> {
        match self {
            Self::Mssql(p) => p.load_foreign_keys(table).await,
            Self::Postgres(p) => p.load_foreign_keys(table).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewSourceReader::load_foreign_keys(p.as_ref(), table).await,
        }
    }

    /// Load check constraint metadata for a table.
    pub async fn load_check_constraints(&self, table: &mut Table) -> Result<()> {
        match self {
            Self::Mssql(p) => p.load_check_constraints(table).await,
            Self::Postgres(p) => p.load_check_constraints(table).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewSourceReader::load_check_constraints(p.as_ref(), table).await,
        }
    }

    /// Get the row count for a table.
    pub async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        match self {
            Self::Mssql(p) => p.get_row_count(schema, table).await,
            Self::Postgres(p) => p.get_row_count(schema, table).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewSourceReader::get_row_count(p.as_ref(), schema, table).await,
        }
    }

    /// Test the connection.
    pub async fn test_connection(&self) -> Result<()> {
        match self {
            Self::Mssql(p) => p.test_connection().await,
            Self::Postgres(p) => p.test_connection().await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => p.test_connection().await,
        }
    }

    /// Close all connections.
    pub async fn close(&self) {
        match self {
            Self::Mssql(p) => p.close().await,
            Self::Postgres(p) => p.close().await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewSourceReader::close(p.as_ref()).await,
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
            #[cfg(feature = "mysql")]
            Self::Mysql(_) => {
                // MySQL uses the new trait-based API which doesn't have query_rows_fast
                // This path shouldn't be called for MySQL sources using the new transfer engine
                Err(crate::error::MigrateError::Config(
                    "MySQL source does not support query_rows_fast. Use the new SourceReader API.".to_string(),
                ))
            }
        }
    }

    /// Query rows and encode directly to PostgreSQL COPY binary format.
    ///
    /// This is the high-performance path for MSSQL->PG upsert that bypasses
    /// SqlValue entirely. Returns (encoded_bytes, first_pk, last_pk, row_count).
    ///
    /// Only supported for MSSQL sources - returns None for other sources.
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
            #[cfg(feature = "mysql")]
            Self::Mysql(_) => Ok(None),
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
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewSourceReader::get_max_pk(p.as_ref(), schema, table, pk_col).await,
        }
    }

    /// Get the underlying MSSQL pool (for transfer engine compatibility).
    pub fn as_mssql(&self) -> Option<Arc<MssqlPool>> {
        match self {
            Self::Mssql(p) => Some(p.clone()),
            Self::Postgres(_) => None,
            #[cfg(feature = "mysql")]
            Self::Mysql(_) => None,
        }
    }

    /// Get the underlying PostgreSQL source pool.
    pub fn as_postgres(&self) -> Option<Arc<PgSourcePool>> {
        match self {
            Self::Mssql(_) => None,
            Self::Postgres(p) => Some(p.clone()),
            #[cfg(feature = "mysql")]
            Self::Mysql(_) => None,
        }
    }

    /// Get the underlying MySQL reader.
    #[cfg(feature = "mysql")]
    pub fn as_mysql(&self) -> Option<Arc<MysqlReader>> {
        match self {
            Self::Mssql(_) => None,
            Self::Postgres(_) => None,
            Self::Mysql(p) => Some(p.clone()),
        }
    }

    /// Check if this is a MySQL source.
    #[cfg(feature = "mysql")]
    pub fn is_mysql(&self) -> bool {
        matches!(self, Self::Mysql(_))
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
    #[cfg(feature = "mysql")]
    Mysql(Arc<MysqlWriter>),
}

impl TargetPoolImpl {
    /// Create target pool from configuration.
    pub async fn from_config(config: &Config) -> Result<Self> {
        let max_conns = config.migration.get_max_pg_connections();
        let target_type = config.target.r#type.to_lowercase();

        if target_type == "mssql" || target_type == "sqlserver" || target_type == "sql_server" {
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
        } else if target_type == "mysql" || target_type == "mariadb" {
            // MySQL/MariaDB target - uses SQLx
            #[cfg(feature = "mysql")]
            {
                mysql_info!("Creating MySQL target connection pool");
                let writer = MysqlWriter::new(&config.target, max_conns).await?;
                Ok(Self::Mysql(Arc::new(writer)))
            }
            #[cfg(not(feature = "mysql"))]
            {
                Err(MigrateError::Config(
                    "MySQL target requires the 'mysql' feature.\n\n\
                     Rebuild with: cargo build --features mysql"
                        .into(),
                ))
            }
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
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => p.db_type(),
        }
    }

    /// Create a state backend appropriate for this target database type.
    pub fn create_state_backend(&self) -> Result<StateBackendEnum> {
        match self {
            Self::Postgres(p) => Ok(StateBackendEnum::Postgres(DbStateBackend::new(
                p.pool().clone(),
            ))),
            Self::Mssql(p) => Ok(StateBackendEnum::Mssql(MssqlStateBackend::new(Arc::clone(
                p,
            )))),
            #[cfg(feature = "mysql")]
            Self::Mysql(_) => {
                // MySQL state backend not yet implemented - use no-op backend
                // Resume capability not available for MySQL targets
                Ok(StateBackendEnum::NoOp(NoOpStateBackend::new()))
            }
        }
    }

    /// Create a schema if it doesn't exist.
    pub async fn create_schema(&self, schema: &str) -> Result<()> {
        match self {
            Self::Mssql(p) => p.create_schema(schema).await,
            Self::Postgres(p) => p.create_schema(schema).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewTargetWriter::create_schema(p.as_ref(), schema).await,
        }
    }

    /// Create a table from metadata.
    pub async fn create_table(&self, table: &Table, target_schema: &str) -> Result<()> {
        match self {
            Self::Mssql(p) => p.create_table(table, target_schema).await,
            Self::Postgres(p) => p.create_table(table, target_schema).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewTargetWriter::create_table(p.as_ref(), table, target_schema).await,
        }
    }

    /// Create a table with optional UNLOGGED.
    pub async fn create_table_unlogged(&self, table: &Table, target_schema: &str) -> Result<()> {
        match self {
            Self::Mssql(p) => p.create_table_unlogged(table, target_schema).await,
            Self::Postgres(p) => p.create_table_unlogged(table, target_schema).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewTargetWriter::create_table_unlogged(p.as_ref(), table, target_schema).await,
        }
    }

    /// Drop a table if it exists.
    pub async fn drop_table(&self, schema: &str, table: &str) -> Result<()> {
        match self {
            Self::Mssql(p) => p.drop_table(schema, table).await,
            Self::Postgres(p) => p.drop_table(schema, table).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewTargetWriter::drop_table(p.as_ref(), schema, table).await,
        }
    }

    /// Truncate a table.
    pub async fn truncate_table(&self, schema: &str, table: &str) -> Result<()> {
        match self {
            Self::Mssql(p) => p.truncate_table(schema, table).await,
            Self::Postgres(p) => p.truncate_table(schema, table).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewTargetWriter::truncate_table(p.as_ref(), schema, table).await,
        }
    }

    /// Check if a table exists.
    pub async fn table_exists(&self, schema: &str, table: &str) -> Result<bool> {
        match self {
            Self::Mssql(p) => p.table_exists(schema, table).await,
            Self::Postgres(p) => p.table_exists(schema, table).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewTargetWriter::table_exists(p.as_ref(), schema, table).await,
        }
    }

    /// Create a primary key constraint.
    pub async fn create_primary_key(&self, table: &Table, target_schema: &str) -> Result<()> {
        match self {
            Self::Mssql(p) => p.create_primary_key(table, target_schema).await,
            Self::Postgres(p) => p.create_primary_key(table, target_schema).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewTargetWriter::create_primary_key(p.as_ref(), table, target_schema).await,
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
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewTargetWriter::create_index(p.as_ref(), table, idx, target_schema).await,
        }
    }

    /// Drop all non-PK indexes on a table.
    pub async fn drop_non_pk_indexes(&self, schema: &str, table: &str) -> Result<Vec<String>> {
        match self {
            Self::Mssql(p) => p.drop_non_pk_indexes(schema, table).await,
            Self::Postgres(p) => p.drop_non_pk_indexes(schema, table).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewTargetWriter::drop_non_pk_indexes(p.as_ref(), schema, table).await,
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
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewTargetWriter::create_foreign_key(p.as_ref(), table, fk, target_schema).await,
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
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewTargetWriter::create_check_constraint(p.as_ref(), table, chk, target_schema).await,
        }
    }

    /// Check if a table has a primary key.
    pub async fn has_primary_key(&self, schema: &str, table: &str) -> Result<bool> {
        match self {
            Self::Mssql(p) => p.has_primary_key(schema, table).await,
            Self::Postgres(p) => p.has_primary_key(schema, table).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewTargetWriter::has_primary_key(p.as_ref(), schema, table).await,
        }
    }

    /// Get the row count for a table.
    pub async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        match self {
            Self::Mssql(p) => p.get_row_count(schema, table).await,
            Self::Postgres(p) => p.get_row_count(schema, table).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewTargetWriter::get_row_count(p.as_ref(), schema, table).await,
        }
    }

    /// Reset sequence to max value.
    pub async fn reset_sequence(&self, schema: &str, table: &Table) -> Result<()> {
        match self {
            Self::Mssql(p) => p.reset_sequence(schema, table).await,
            Self::Postgres(p) => p.reset_sequence(schema, table).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewTargetWriter::reset_sequence(p.as_ref(), schema, table).await,
        }
    }

    /// Set table to LOGGED mode.
    pub async fn set_table_logged(&self, schema: &str, table: &str) -> Result<()> {
        match self {
            Self::Mssql(p) => p.set_table_logged(schema, table).await,
            Self::Postgres(p) => p.set_table_logged(schema, table).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewTargetWriter::set_table_logged(p.as_ref(), schema, table).await,
        }
    }

    /// Set table to UNLOGGED mode.
    pub async fn set_table_unlogged(&self, schema: &str, table: &str) -> Result<()> {
        match self {
            Self::Mssql(p) => p.set_table_unlogged(schema, table).await,
            Self::Postgres(p) => p.set_table_unlogged(schema, table).await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewTargetWriter::set_table_unlogged(p.as_ref(), schema, table).await,
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
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => {
                // Convert old SqlValue to new SqlValue<'static> for MySQL
                let new_rows: Vec<Vec<NewSqlValue<'static>>> = rows
                    .into_iter()
                    .map(|row| row.into_iter().map(convert_sql_value).collect())
                    .collect();
                let batch = Batch {
                    rows: new_rows,
                    last_key: None,
                    is_last: true,
                };
                NewTargetWriter::write_batch(p.as_ref(), schema, table, cols, batch).await
            }
        }
    }

    /// Upsert a chunk of rows.
    #[allow(clippy::too_many_arguments)]
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
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => {
                // Convert old SqlValue to new SqlValue<'static> for MySQL
                let new_rows: Vec<Vec<NewSqlValue<'static>>> = rows
                    .into_iter()
                    .map(|row| row.into_iter().map(convert_sql_value).collect())
                    .collect();
                let batch = Batch {
                    rows: new_rows,
                    last_key: None,
                    is_last: true,
                };
                NewTargetWriter::upsert_batch(p.as_ref(), schema, table, cols, pk_cols, batch, writer_id, partition_id).await
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
            #[cfg(feature = "mysql")]
            Self::Mysql(_) => {
                // MySQL uses the new trait-based API
                Err(crate::error::MigrateError::Config(
                    "MySQL target does not support get_upsert_writer. \
                     Use the new TargetWriter API with upsert_batch."
                        .to_string(),
                ))
            }
        }
    }

    /// Test the connection.
    pub async fn test_connection(&self) -> Result<()> {
        match self {
            Self::Mssql(_) => Ok(()),
            Self::Postgres(p) => p.test_connection().await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => p.test_connection().await,
        }
    }

    /// Close all connections.
    pub async fn close(&self) {
        match self {
            Self::Mssql(p) => p.close().await,
            Self::Postgres(p) => p.close().await,
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => NewTargetWriter::close(p.as_ref()).await,
        }
    }

    /// Get the underlying PostgreSQL pool.
    pub fn as_postgres(&self) -> Option<Arc<PgPool>> {
        match self {
            Self::Mssql(_) => None,
            Self::Postgres(p) => Some(p.clone()),
            #[cfg(feature = "mysql")]
            Self::Mysql(_) => None,
        }
    }

    /// Get the underlying MSSQL target pool.
    pub fn as_mssql(&self) -> Option<Arc<MssqlTargetPool>> {
        match self {
            Self::Mssql(p) => Some(p.clone()),
            Self::Postgres(_) => None,
            #[cfg(feature = "mysql")]
            Self::Mysql(_) => None,
        }
    }

    /// Get the underlying MySQL writer.
    #[cfg(feature = "mysql")]
    pub fn as_mysql(&self) -> Option<Arc<MysqlWriter>> {
        match self {
            Self::Mssql(_) => None,
            Self::Postgres(_) => None,
            Self::Mysql(p) => Some(p.clone()),
        }
    }

    /// Check if this is a MySQL target.
    #[cfg(feature = "mysql")]
    pub fn is_mysql(&self) -> bool {
        matches!(self, Self::Mysql(_))
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
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => Self::Mysql(p.clone()),
        }
    }
}

impl Clone for TargetPoolImpl {
    fn clone(&self) -> Self {
        match self {
            Self::Mssql(p) => Self::Mssql(p.clone()),
            Self::Postgres(p) => Self::Postgres(p.clone()),
            #[cfg(feature = "mysql")]
            Self::Mysql(p) => Self::Mysql(p.clone()),
        }
    }
}
