//! Pool wrapper enums for bidirectional migration support.
//!
//! These enums provide static dispatch for source and target pool implementations,
//! avoiding trait objects while supporting all 4 migration directions:
//! - MSSQL → PostgreSQL
//! - PostgreSQL → MSSQL
//! - MSSQL → MSSQL
//! - PostgreSQL → PostgreSQL

use crate::config::Config;
use crate::error::Result;
use crate::source::{
    CheckConstraint, ForeignKey, Index, MssqlPool, Partition, PgSourcePool, SourcePool, Table,
};
use crate::target::{MssqlTargetPool, PgPool, SqlValue, TargetPool};
use crate::verify::{CompositePk, CompositeRowHashMap, RowRange};
use std::sync::Arc;

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
            let pool = PgSourcePool::new(&config.source, pool_size as usize).await?;
            Ok(Self::Postgres(Arc::new(pool)))
        } else {
            // Default to MSSQL
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

    // === Verification methods ===

    /// Get total row count for a table (verification).
    pub async fn get_total_row_count(&self, query: &str) -> Result<i64> {
        match self {
            Self::Mssql(p) => p.get_total_row_count(query).await,
            Self::Postgres(p) => p.get_total_row_count(query).await,
        }
    }

    /// Execute NTILE partition query with hash.
    ///
    /// Returns (partition_id, row_count, partition_hash) tuples for Tier 1.
    /// The partition_hash enables detection of updates even when row counts match.
    pub async fn execute_ntile_partition_query(&self, query: &str) -> Result<Vec<(i64, i64, i64)>> {
        match self {
            Self::Mssql(p) => p.execute_ntile_partition_query(query).await,
            Self::Postgres(p) => p.execute_ntile_partition_query(query).await,
        }
    }

    /// Execute count query with rownum range and hash.
    ///
    /// Returns (row_count, range_hash) for Tier 2 update detection.
    pub async fn execute_count_query_with_rownum(
        &self,
        query: &str,
        range: &RowRange,
    ) -> Result<(i64, i64)> {
        match self {
            Self::Mssql(p) => p.execute_count_query_with_rownum(query, range).await,
            Self::Postgres(p) => p.execute_count_query_with_rownum(query, range).await,
        }
    }

    /// Fetch row hashes with rownum range.
    pub async fn fetch_row_hashes_with_rownum(
        &self,
        query: &str,
        range: &RowRange,
        pk_column_count: usize,
    ) -> Result<CompositeRowHashMap> {
        match self {
            Self::Mssql(p) => p.fetch_row_hashes_with_rownum(query, range, pk_column_count).await,
            Self::Postgres(p) => p.fetch_row_hashes_with_rownum(query, range, pk_column_count).await,
        }
    }

    /// Fetch rows for a PK range.
    pub async fn fetch_rows_for_range(
        &self,
        table: &Table,
        pk_column: &str,
        min_pk: i64,
        max_pk: i64,
    ) -> Result<Vec<Vec<SqlValue>>> {
        match self {
            Self::Mssql(p) => p.fetch_rows_for_range(table, pk_column, min_pk, max_pk).await,
            Self::Postgres(p) => p.fetch_rows_for_range(table, pk_column, min_pk, max_pk).await,
        }
    }

    /// Fetch specific rows by composite primary key values.
    pub async fn fetch_rows_by_composite_pks(
        &self,
        table: &Table,
        pks: &[CompositePk],
    ) -> Result<Vec<Vec<SqlValue>>> {
        match self {
            Self::Mssql(p) => p.fetch_rows_by_composite_pks(table, pks).await,
            Self::Postgres(p) => p.fetch_rows_by_composite_pks(table, pks).await,
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

    /// Get the maximum value of a primary key column.
    pub async fn get_max_pk(&self, schema: &str, table: &str, pk_col: &str) -> Result<i64> {
        match self {
            Self::Mssql(p) => p.get_max_pk(schema, table, pk_col).await,
            Self::Postgres(p) => p.get_max_pk(schema, table, pk_col).await,
        }
    }

    /// Get the underlying MSSQL pool (for transfer engine compatibility).
    /// Returns None if not MSSQL.
    pub fn as_mssql(&self) -> Option<Arc<MssqlPool>> {
        match self {
            Self::Mssql(p) => Some(p.clone()),
            Self::Postgres(_) => None,
        }
    }

    /// Get the underlying PostgreSQL source pool.
    /// Returns None if not PostgreSQL.
    pub fn as_postgres(&self) -> Option<Arc<PgSourcePool>> {
        match self {
            Self::Mssql(_) => None,
            Self::Postgres(p) => Some(p.clone()),
        }
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
            let pool = MssqlTargetPool::new(&config.target, max_conns as u32).await?;
            Ok(Self::Mssql(Arc::new(pool)))
        } else {
            // Default to PostgreSQL
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

    /// Create a table with row hash column for change detection.
    pub async fn create_table_with_hash(
        &self,
        table: &Table,
        target_schema: &str,
        row_hash_column: &str,
    ) -> Result<()> {
        match self {
            Self::Mssql(p) => p.create_table_with_hash(table, target_schema, row_hash_column).await,
            Self::Postgres(p) => p.create_table_with_hash(table, target_schema, row_hash_column).await,
        }
    }

    /// Ensure the row hash column exists on the table.
    pub async fn ensure_row_hash_column(
        &self,
        schema: &str,
        table: &str,
        row_hash_column: &str,
    ) -> Result<bool> {
        match self {
            Self::Mssql(p) => p.ensure_row_hash_column(schema, table, row_hash_column).await,
            Self::Postgres(p) => p.ensure_row_hash_column(schema, table, row_hash_column).await,
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
    pub async fn create_index(&self, table: &Table, idx: &Index, target_schema: &str) -> Result<()> {
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
        row_hash_column: Option<&str>,
    ) -> Result<u64> {
        match self {
            Self::Mssql(p) => {
                p.upsert_chunk(schema, table, cols, pk_cols, rows, writer_id, row_hash_column)
                    .await
            }
            Self::Postgres(p) => {
                p.upsert_chunk(schema, table, cols, pk_cols, rows, writer_id, row_hash_column)
                    .await
            }
        }
    }

    /// Upsert a chunk with hash-based change detection.
    pub async fn upsert_chunk_with_hash(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        pk_cols: &[String],
        rows: Vec<Vec<SqlValue>>,
        writer_id: usize,
        row_hash_column: Option<&str>,
    ) -> Result<u64> {
        match self {
            Self::Mssql(p) => {
                p.upsert_chunk_with_hash(schema, table, cols, pk_cols, rows, writer_id, row_hash_column)
                    .await
            }
            Self::Postgres(p) => {
                p.upsert_chunk_with_hash(schema, table, cols, pk_cols, rows, writer_id, row_hash_column)
                    .await
            }
        }
    }

    /// Fetch row hashes for hash-based change detection.
    pub async fn fetch_row_hashes(
        &self,
        schema: &str,
        table: &str,
        pk_cols: &[String],
        row_hash_col: &str,
        min_pk: Option<i64>,
        max_pk: Option<i64>,
    ) -> Result<std::collections::HashMap<String, String>> {
        match self {
            Self::Mssql(p) => p.fetch_row_hashes(schema, table, pk_cols, row_hash_col, min_pk, max_pk).await,
            Self::Postgres(p) => p.fetch_row_hashes(schema, table, pk_cols, row_hash_col, min_pk, max_pk).await,
        }
    }

    /// Get total row count from a query (for verification).
    pub async fn get_total_row_count(&self, query: &str) -> Result<i64> {
        match self {
            Self::Mssql(p) => p.get_total_row_count(query).await,
            Self::Postgres(p) => p.get_total_row_count(query).await,
        }
    }

    /// Execute NTILE partition query with hash (for verification).
    ///
    /// Returns (partition_id, row_count, partition_hash) tuples for Tier 1.
    /// The partition_hash enables detection of updates even when row counts match.
    pub async fn execute_ntile_partition_query(&self, query: &str) -> Result<Vec<(i64, i64, i64)>> {
        match self {
            Self::Mssql(p) => p.execute_ntile_partition_query(query).await,
            Self::Postgres(p) => p.execute_ntile_partition_query(query).await,
        }
    }

    /// Execute count query with ROW_NUMBER range and hash (for verification).
    ///
    /// Returns (row_count, range_hash) for Tier 2 update detection.
    pub async fn execute_count_query_with_rownum(
        &self,
        query: &str,
        range: &crate::verify::RowRange,
    ) -> Result<(i64, i64)> {
        match self {
            Self::Mssql(p) => p.execute_count_query_with_rownum(query, range).await,
            Self::Postgres(p) => p.execute_count_query_with_rownum(query, range).await,
        }
    }

    /// Fetch row hashes with ROW_NUMBER range (for verification).
    pub async fn fetch_row_hashes_with_rownum(
        &self,
        query: &str,
        range: &crate::verify::RowRange,
        pk_column_count: usize,
    ) -> Result<crate::verify::CompositeRowHashMap> {
        match self {
            Self::Mssql(p) => p.fetch_row_hashes_with_rownum(query, range, pk_column_count).await,
            Self::Postgres(p) => p.fetch_row_hashes_with_rownum(query, range, pk_column_count).await,
        }
    }

    /// Test the connection.
    ///
    /// For PostgreSQL, this delegates to `PgPool::test_connection` which actively
    /// validates the connection. For MSSQL, `MssqlTargetPool` currently does not
    /// expose a lightweight, dedicated connection-test API; connection issues are
    /// surfaced when regular operations are executed. To keep the interface
    /// consistent across targets without performing redundant or invasive checks,
    /// this is intentionally a no-op for MSSQL and simply returns `Ok(())`.
    pub async fn test_connection(&self) -> Result<()> {
        match self {
            Self::Mssql(_) => {
                // MSSQL connections are validated as part of normal operations.
                // This method is kept for API symmetry and intentionally does not
                // perform an additional probe here.
                Ok(())
            }
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

    /// Get the underlying PostgreSQL pool (for transfer engine compatibility).
    /// Returns None if not PostgreSQL.
    pub fn as_postgres(&self) -> Option<Arc<PgPool>> {
        match self {
            Self::Mssql(_) => None,
            Self::Postgres(p) => Some(p.clone()),
        }
    }

    /// Get the underlying MSSQL target pool.
    /// Returns None if not MSSQL.
    pub fn as_mssql(&self) -> Option<Arc<MssqlTargetPool>> {
        match self {
            Self::Mssql(p) => Some(p.clone()),
            Self::Postgres(_) => None,
        }
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
