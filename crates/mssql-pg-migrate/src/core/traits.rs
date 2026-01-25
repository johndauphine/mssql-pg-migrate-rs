//! Core traits for database-agnostic data migration.
//!
//! This module defines the primary abstractions used by the migration engine:
//!
//! - [`SourceReader`]: Reads schema and data from source databases
//! - [`TargetWriter`]: Writes schema and data to target databases
//! - [`Dialect`]: SQL syntax strategy for different database engines
//! - [`TypeMapper`]: Maps types between source and target dialects
//!
//! # Design Patterns
//!
//! - **Abstract Factory**: Drivers create families of related objects (Reader, Writer, Dialect)
//! - **Strategy**: Dialect and TypeMapper provide interchangeable algorithms
//! - **Template Method**: Default implementations in traits define algorithm skeletons

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::error::Result;

use super::schema::{CheckConstraint, Column, ForeignKey, Index, Partition, Table};
use super::value::Batch;

/// Options for reading rows from a table.
#[derive(Debug, Clone)]
pub struct ReadOptions {
    /// Schema name.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Columns to read.
    pub columns: Vec<String>,
    /// Column type strings for encoding.
    pub col_types: Vec<String>,
    /// Primary key column index (for keyset pagination).
    pub pk_idx: Option<usize>,
    /// Minimum PK value (exclusive, for keyset pagination).
    pub min_pk: Option<i64>,
    /// Maximum PK value (inclusive, for keyset pagination).
    pub max_pk: Option<i64>,
    /// Start row (for ROW_NUMBER pagination).
    pub start_row: Option<i64>,
    /// End row (for ROW_NUMBER pagination).
    pub end_row: Option<i64>,
    /// Number of rows per batch.
    pub batch_size: usize,
    /// Optional WHERE clause for incremental sync.
    pub where_clause: Option<String>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            schema: String::new(),
            table: String::new(),
            columns: Vec::new(),
            col_types: Vec::new(),
            pk_idx: None,
            min_pk: None,
            max_pk: None,
            start_row: None,
            end_row: None,
            batch_size: 10_000,
            where_clause: None,
        }
    }
}

/// Read data from a source database.
///
/// Implementations provide schema extraction and row streaming for different
/// database engines (MSSQL, PostgreSQL, etc.).
///
/// # Streaming
///
/// The [`read_table`] method returns a channel receiver for streaming batches,
/// enabling backpressure and memory-efficient transfers of large tables.
#[async_trait]
pub trait SourceReader: Send + Sync {
    /// Extract table metadata from the source schema.
    ///
    /// Returns a list of tables with column definitions but without
    /// index/FK/constraint metadata (use [`load_table_metadata`] for that).
    async fn extract_schema(&self, schema: &str) -> Result<Vec<Table>>;

    /// Load full metadata for a table (indexes, FKs, check constraints).
    ///
    /// This is a template method with a default implementation that calls
    /// the individual metadata loading methods.
    async fn load_table_metadata(&self, table: &mut Table) -> Result<()> {
        self.load_indexes(table).await?;
        self.load_foreign_keys(table).await?;
        self.load_check_constraints(table).await?;
        Ok(())
    }

    /// Load index metadata for a table.
    async fn load_indexes(&self, table: &mut Table) -> Result<()>;

    /// Load foreign key metadata for a table.
    async fn load_foreign_keys(&self, table: &mut Table) -> Result<()>;

    /// Load check constraint metadata for a table.
    async fn load_check_constraints(&self, table: &mut Table) -> Result<()>;

    /// Start streaming rows from a table.
    ///
    /// Returns a channel receiver that yields batches of rows. The reader
    /// spawns a background task that populates the channel, enabling
    /// backpressure when the channel fills up.
    ///
    /// # Arguments
    ///
    /// * `opts` - Read options including schema, table, columns, and pagination
    ///
    /// # Returns
    ///
    /// A receiver that yields `Result<Batch>` until the table is exhausted.
    fn read_table(&self, opts: ReadOptions) -> mpsc::Receiver<Result<Batch>>;

    /// Get partition boundaries for parallel reading.
    ///
    /// Divides a table into `n` partitions based on primary key ranges
    /// or row number ranges, depending on what the table supports.
    async fn get_partition_boundaries(
        &self,
        table: &Table,
        num_partitions: usize,
    ) -> Result<Vec<Partition>>;

    /// Get the row count for a table.
    async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64>;

    /// Get the maximum primary key value for keyset pagination.
    async fn get_max_pk(&self, schema: &str, table: &str, pk_col: &str) -> Result<i64>;

    /// Get the database type identifier (e.g., "mssql", "postgres").
    fn db_type(&self) -> &str;

    /// Close the connection pool.
    async fn close(&self);
}

/// Write schema and data to a target database.
///
/// Implementations handle schema creation, data insertion, and upserts
/// for different database engines.
#[async_trait]
pub trait TargetWriter: Send + Sync {
    // ===== Schema Operations =====

    /// Create a schema if it doesn't exist.
    async fn create_schema(&self, schema: &str) -> Result<()>;

    /// Create a table in the target database.
    async fn create_table(&self, table: &Table, target_schema: &str) -> Result<()>;

    /// Create a table as UNLOGGED (PostgreSQL) for faster initial loads.
    async fn create_table_unlogged(&self, table: &Table, target_schema: &str) -> Result<()>;

    /// Drop a table if it exists.
    async fn drop_table(&self, schema: &str, table: &str) -> Result<()>;

    /// Check if a table exists.
    async fn table_exists(&self, schema: &str, table: &str) -> Result<bool>;

    // ===== Constraint Operations =====

    /// Create the primary key constraint for a table.
    async fn create_primary_key(&self, table: &Table, target_schema: &str) -> Result<()>;

    /// Create an index on a table.
    async fn create_index(&self, table: &Table, idx: &Index, target_schema: &str) -> Result<()>;

    /// Drop all non-primary-key indexes on a table.
    ///
    /// Returns the names of dropped indexes for potential recreation.
    async fn drop_non_pk_indexes(&self, schema: &str, table: &str) -> Result<Vec<String>>;

    /// Create a foreign key constraint.
    async fn create_foreign_key(
        &self,
        table: &Table,
        fk: &ForeignKey,
        target_schema: &str,
    ) -> Result<()>;

    /// Create a check constraint.
    async fn create_check_constraint(
        &self,
        table: &Table,
        chk: &CheckConstraint,
        target_schema: &str,
    ) -> Result<()>;

    // ===== Data Operations =====

    /// Write a batch of rows to the target table.
    ///
    /// For bulk inserts without conflict handling.
    async fn write_batch(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        batch: Batch,
    ) -> Result<u64>;

    /// Upsert a batch of rows (insert or update on conflict).
    ///
    /// Uses staging table pattern for efficiency.
    #[allow(clippy::too_many_arguments)]
    async fn upsert_batch(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        pk_cols: &[String],
        batch: Batch,
        writer_id: usize,
        partition_id: Option<i32>,
    ) -> Result<u64>;

    // ===== Utility Operations =====

    /// Check if a table has a primary key.
    async fn has_primary_key(&self, schema: &str, table: &str) -> Result<bool>;

    /// Get the row count for a table.
    async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64>;

    /// Reset sequence values for identity columns after data load.
    async fn reset_sequence(&self, schema: &str, table: &Table) -> Result<()>;

    /// Convert a table from UNLOGGED to LOGGED (PostgreSQL).
    async fn set_table_logged(&self, schema: &str, table: &str) -> Result<()>;

    /// Convert a table from LOGGED to UNLOGGED (PostgreSQL).
    async fn set_table_unlogged(&self, schema: &str, table: &str) -> Result<()>;

    /// Get the database type identifier (e.g., "mssql", "postgres").
    fn db_type(&self) -> &str;

    /// Close the connection pool.
    async fn close(&self);
}

/// SQL syntax strategy for different database engines.
///
/// Provides database-specific SQL generation while keeping the
/// core migration logic database-agnostic.
///
/// # Design Pattern
///
/// This is a **Strategy** pattern - different implementations provide
/// interchangeable SQL syntax rules.
///
/// # enum_dispatch
///
/// This trait is used with `enum_dispatch` to enable zero-cost
/// polymorphism via the `DialectImpl` enum in the `drivers` module.
pub trait Dialect: Send + Sync {
    /// Get the dialect identifier (e.g., "mssql", "postgres").
    fn name(&self) -> &str;

    /// Quote an identifier (table name, column name, etc.).
    ///
    /// - MSSQL: `[identifier]`
    /// - PostgreSQL: `"identifier"`
    fn quote_ident(&self, name: &str) -> String;

    /// Build a SELECT query for reading rows.
    fn build_select_query(&self, opts: &SelectQueryOptions) -> String;

    /// Build an upsert query (INSERT ... ON CONFLICT for PG, MERGE for MSSQL).
    fn build_upsert_query(
        &self,
        target_table: &str,
        staging_table: &str,
        columns: &[String],
        pk_columns: &[String],
    ) -> String;

    /// Get a parameter placeholder for the given 1-based index.
    ///
    /// - MSSQL: `@p1`, `@p2`, etc.
    /// - PostgreSQL: `$1`, `$2`, etc.
    fn param_placeholder(&self, index: usize) -> String;

    /// Build a keyset pagination WHERE clause.
    ///
    /// - `pk_col > {last_pk}` for ascending order
    fn build_keyset_where(&self, pk_col: &str, last_pk: i64) -> String;

    /// Build a ROW_NUMBER pagination query wrapper.
    fn build_row_number_query(
        &self,
        inner_query: &str,
        pk_col: &str,
        start_row: i64,
        end_row: i64,
    ) -> String;
}

/// Options for building a SELECT query.
#[derive(Debug, Clone)]
pub struct SelectQueryOptions {
    /// Schema name.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Columns to select.
    pub columns: Vec<String>,
    /// Primary key column (for ordering).
    pub pk_col: Option<String>,
    /// Minimum PK value (exclusive).
    pub min_pk: Option<i64>,
    /// Maximum PK value (inclusive).
    pub max_pk: Option<i64>,
    /// Additional WHERE clause.
    pub where_clause: Option<String>,
    /// Row limit (for batch reads).
    pub limit: Option<usize>,
}

/// Maps data types between source and target database dialects.
///
/// # Design Pattern
///
/// TypeMapper uses a **(source, target) pair keying** approach to solve
/// the N² complexity problem. Instead of mapping directly between all
/// database pairs, mappers are registered for specific source→target
/// combinations.
#[async_trait]
pub trait TypeMapper: Send + Sync {
    /// Get the source dialect name.
    fn source_dialect(&self) -> &str;

    /// Get the target dialect name.
    fn target_dialect(&self) -> &str;

    /// Map a column definition from source to target.
    ///
    /// Returns a [`ColumnMapping`] with the target type string and
    /// any warnings about lossy conversions.
    fn map_column(&self, col: &Column) -> ColumnMapping;

    /// Map a source type string to target type string.
    ///
    /// Lower-level method used by `map_column`.
    fn map_type(&self, data_type: &str, max_length: i32, precision: i32, scale: i32)
        -> TypeMapping;
}

/// Result of mapping a column from source to target.
#[derive(Debug, Clone)]
pub struct ColumnMapping {
    /// Target column name (usually same as source).
    pub name: String,
    /// Target data type string.
    pub target_type: String,
    /// Whether the column is nullable.
    pub is_nullable: bool,
    /// Warning message if the mapping is lossy.
    pub warning: Option<String>,
}

/// Result of mapping a type from source to target.
#[derive(Debug, Clone)]
pub struct TypeMapping {
    /// Target type string (e.g., "varchar(255)", "bigint").
    pub target_type: String,
    /// Whether this mapping loses data or precision.
    pub is_lossy: bool,
    /// Warning message for lossy mappings.
    pub warning: Option<String>,
}

impl TypeMapping {
    /// Create a lossless type mapping.
    pub fn lossless(target_type: impl Into<String>) -> Self {
        Self {
            target_type: target_type.into(),
            is_lossy: false,
            warning: None,
        }
    }

    /// Create a lossy type mapping with a warning.
    pub fn lossy(target_type: impl Into<String>, warning: impl Into<String>) -> Self {
        Self {
            target_type: target_type.into(),
            is_lossy: true,
            warning: Some(warning.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_mapping_lossless() {
        let mapping = TypeMapping::lossless("bigint");
        assert_eq!(mapping.target_type, "bigint");
        assert!(!mapping.is_lossy);
        assert!(mapping.warning.is_none());
    }

    #[test]
    fn test_type_mapping_lossy() {
        let mapping = TypeMapping::lossy("text", "Array types stored as JSON");
        assert_eq!(mapping.target_type, "text");
        assert!(mapping.is_lossy);
        assert_eq!(
            mapping.warning.as_deref(),
            Some("Array types stored as JSON")
        );
    }

    #[test]
    fn test_read_options_default() {
        let opts = ReadOptions::default();
        assert_eq!(opts.batch_size, 10_000);
        assert!(opts.columns.is_empty());
    }
}
