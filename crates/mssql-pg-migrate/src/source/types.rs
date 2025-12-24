//! Schema and metadata types.

use serde::{Deserialize, Serialize};

/// Table metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    /// Schema name.
    pub schema: String,

    /// Table name.
    pub name: String,

    /// Column definitions.
    pub columns: Vec<Column>,

    /// Primary key column names.
    pub primary_key: Vec<String>,

    /// Full primary key column definitions.
    pub pk_columns: Vec<Column>,

    /// Approximate row count.
    pub row_count: i64,

    /// Estimated average row size in bytes.
    pub estimated_row_size: i64,

    /// Non-primary key indexes.
    pub indexes: Vec<Index>,

    /// Foreign key constraints.
    pub foreign_keys: Vec<ForeignKey>,

    /// Check constraints.
    pub check_constraints: Vec<CheckConstraint>,
}

impl Table {
    /// Get the fully qualified table name.
    pub fn full_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }

    /// Check if the table is large (exceeds threshold).
    pub fn is_large(&self, threshold: i64) -> bool {
        self.row_count > threshold
    }

    /// Check if the table has a primary key.
    pub fn has_pk(&self) -> bool {
        !self.primary_key.is_empty()
    }

    /// Check if the table has a single-column primary key.
    pub fn has_single_pk(&self) -> bool {
        self.primary_key.len() == 1
    }

    /// Check if the table supports keyset pagination.
    pub fn supports_keyset_pagination(&self) -> bool {
        if self.pk_columns.len() != 1 {
            return false;
        }
        let pk_type = self.pk_columns[0].data_type.to_lowercase();
        matches!(
            pk_type.as_str(),
            "int" | "bigint" | "smallint" | "tinyint"
        )
    }
}

/// Column metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    /// Column name.
    pub name: String,

    /// Data type (e.g., "int", "varchar", "datetime2").
    pub data_type: String,

    /// Maximum length for string/binary types (-1 for max).
    pub max_length: i32,

    /// Numeric precision.
    pub precision: i32,

    /// Numeric scale.
    pub scale: i32,

    /// Whether the column allows NULL.
    pub is_nullable: bool,

    /// Whether the column is an identity column.
    pub is_identity: bool,

    /// Ordinal position (1-based).
    pub ordinal_pos: i32,
}

/// Partition for parallel transfer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partition {
    /// Table name.
    pub table_name: String,

    /// Partition ID (0-based).
    pub partition_id: i32,

    /// Minimum primary key value (for keyset pagination).
    pub min_pk: Option<i64>,

    /// Maximum primary key value (for keyset pagination).
    pub max_pk: Option<i64>,

    /// Start row number (for ROW_NUMBER pagination).
    pub start_row: i64,

    /// End row number (for ROW_NUMBER pagination).
    pub end_row: i64,

    /// Row count in this partition.
    pub row_count: i64,
}

/// Index metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    /// Index name.
    pub name: String,

    /// Indexed column names.
    pub columns: Vec<String>,

    /// Whether the index is unique.
    pub is_unique: bool,

    /// Whether the index is clustered.
    pub is_clustered: bool,

    /// Included columns (non-key).
    pub include_cols: Vec<String>,
}

/// Foreign key metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKey {
    /// Constraint name.
    pub name: String,

    /// Source column names.
    pub columns: Vec<String>,

    /// Referenced table name.
    pub ref_table: String,

    /// Referenced schema name.
    pub ref_schema: String,

    /// Referenced column names.
    pub ref_columns: Vec<String>,

    /// ON DELETE action.
    pub on_delete: String,

    /// ON UPDATE action.
    pub on_update: String,
}

/// Check constraint metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckConstraint {
    /// Constraint name.
    pub name: String,

    /// Constraint definition (SQL expression).
    pub definition: String,
}
