//! Schema and metadata types for database tables, columns, indexes, and constraints.
//!
//! These types provide a database-agnostic representation of schema metadata
//! used throughout the migration process.

use serde::{Deserialize, Serialize};
use std::hash::Hash;
use uuid::Uuid;

/// Represents a primary key value of various types.
///
/// This enum allows handling different PK types uniformly for verification
/// and sync operations.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PkValue {
    /// Integer primary key (covers int, bigint, smallint, tinyint).
    Int(i64),
    /// UUID/GUID primary key.
    Uuid(Uuid),
    /// String primary key (varchar, nvarchar, char, nchar).
    String(String),
}

impl PkValue {
    /// Convert to a SQL literal string for use in queries.
    ///
    /// # Security Note
    ///
    /// This method performs basic SQL escaping (single quotes doubled) which is
    /// sufficient for typical primary key values (integers, UUIDs, short identifiers).
    /// For untrusted input or complex strings, prefer parameterized queries.
    /// This is used internally for building WHERE clauses when fetching rows by PK.
    pub fn to_sql_literal(&self) -> String {
        match self {
            PkValue::Int(v) => v.to_string(),
            PkValue::Uuid(v) => format!("'{}'", v),
            PkValue::String(v) => format!("'{}'", v.replace('\'', "''")),
        }
    }

    /// Convert to a SQL literal for MSSQL (with N prefix for Unicode strings).
    ///
    /// # Security Note
    ///
    /// This method performs basic SQL escaping (single quotes doubled) which is
    /// sufficient for typical primary key values (integers, UUIDs, short identifiers).
    /// For untrusted input or complex strings, prefer parameterized queries.
    /// This is used internally for building WHERE clauses when fetching rows by PK.
    pub fn to_mssql_literal(&self) -> String {
        match self {
            PkValue::Int(v) => v.to_string(),
            PkValue::Uuid(v) => format!("'{}'", v),
            PkValue::String(v) => format!("N'{}'", v.replace('\'', "''")),
        }
    }
}

impl From<i64> for PkValue {
    fn from(v: i64) -> Self {
        PkValue::Int(v)
    }
}

impl From<i32> for PkValue {
    fn from(v: i32) -> Self {
        PkValue::Int(v as i64)
    }
}

impl From<Uuid> for PkValue {
    fn from(v: Uuid) -> Self {
        PkValue::Uuid(v)
    }
}

impl From<String> for PkValue {
    fn from(v: String) -> Self {
        PkValue::String(v)
    }
}

impl From<&str> for PkValue {
    fn from(v: &str) -> Self {
        PkValue::String(v.to_string())
    }
}

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
        matches!(pk_type.as_str(), "int" | "bigint" | "smallint" | "tinyint")
    }

    /// Find the first date/timestamp column matching the candidate names.
    /// Used for date-based incremental sync (watermark).
    /// Returns (column_name, column_type) if found.
    ///
    /// # Timezone Warning
    ///
    /// For timezone-naive types (datetime, datetime2, timestamp without time zone),
    /// logs a warning since mismatched timezones can cause data loss.
    pub fn find_date_column(&self, candidate_names: &[String]) -> Option<(String, String)> {
        for name in candidate_names {
            if let Some(col) = self
                .columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(name))
            {
                let data_type = col.data_type.to_lowercase();
                // Check if it's a date/time type
                if is_date_type(&data_type) {
                    // SECURITY: Warn about timezone-naive types
                    if is_timezone_naive(&data_type) {
                        tracing::warn!(
                            "Table {}.{}: Using timezone-naive column '{}' ({}) for incremental sync. \
                             Ensure database timezone matches UTC or use datetimeoffset/timestamptz to avoid data loss.",
                            self.schema, self.name, col.name, col.data_type
                        );
                    }
                    return Some((col.name.clone(), col.data_type.clone()));
                }
            }
        }
        None
    }
}

/// Check if a data type is a date/time type suitable for watermark filtering.
fn is_date_type(data_type: &str) -> bool {
    matches!(
        data_type,
        "datetime"
            | "datetime2"
            | "smalldatetime"
            | "date"
            | "datetimeoffset"
            | "timestamp"
            | "timestamptz"
            | "timestamp with time zone"
            | "timestamp without time zone"
    )
}

/// Check if a timestamp type is timezone-naive (lacks timezone info).
///
/// # Security
///
/// Timezone-naive types can cause data loss if database timezone differs from UTC.
/// Returns true for types that should generate warnings.
fn is_timezone_naive(data_type: &str) -> bool {
    matches!(
        data_type,
        "datetime" | "datetime2" | "smalldatetime" | "timestamp" | "timestamp without time zone"
    )
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_column(name: &str, data_type: &str) -> Column {
        Column {
            name: name.to_string(),
            data_type: data_type.to_string(),
            max_length: 0,
            precision: 0,
            scale: 0,
            is_nullable: true,
            is_identity: false,
            ordinal_pos: 1,
        }
    }

    fn make_test_table(columns: Vec<Column>) -> Table {
        Table {
            schema: "dbo".to_string(),
            name: "TestTable".to_string(),
            columns,
            primary_key: vec![],
            pk_columns: vec![],
            row_count: 1000,
            estimated_row_size: 100,
            indexes: vec![],
            foreign_keys: vec![],
            check_constraints: vec![],
        }
    }

    #[test]
    fn test_pk_value_literals() {
        let int_pk = PkValue::Int(42);
        assert_eq!(int_pk.to_sql_literal(), "42");
        assert_eq!(int_pk.to_mssql_literal(), "42");

        let uuid_pk = PkValue::Uuid(Uuid::nil());
        assert_eq!(
            uuid_pk.to_sql_literal(),
            "'00000000-0000-0000-0000-000000000000'"
        );

        let string_pk = PkValue::String("O'Brien".to_string());
        assert_eq!(string_pk.to_sql_literal(), "'O''Brien'");
        assert_eq!(string_pk.to_mssql_literal(), "N'O''Brien'");
    }

    #[test]
    fn test_table_full_name() {
        let table = make_test_table(vec![]);
        assert_eq!(table.full_name(), "dbo.TestTable");
    }

    #[test]
    fn test_table_is_large() {
        let table = make_test_table(vec![]);
        assert!(table.is_large(500));
        assert!(!table.is_large(1000));
        assert!(!table.is_large(2000));
    }

    #[test]
    fn test_find_date_column_found() {
        let table = make_test_table(vec![
            make_test_column("Id", "int"),
            make_test_column("LastActivityDate", "datetime2"),
            make_test_column("CreationDate", "datetime"),
        ]);

        let candidates = vec![
            "LastActivityDate".to_string(),
            "ModifiedDate".to_string(),
            "CreationDate".to_string(),
        ];

        let result = table.find_date_column(&candidates);
        assert!(result.is_some());
        let (col_name, col_type) = result.unwrap();
        assert_eq!(col_name, "LastActivityDate");
        assert_eq!(col_type, "datetime2");
    }

    #[test]
    fn test_find_date_column_case_insensitive() {
        let table = make_test_table(vec![make_test_column("UpdatedAt", "timestamp")]);

        let candidates = vec!["updatedat".to_string()];
        let result = table.find_date_column(&candidates);
        assert!(result.is_some());
        let (col_name, _) = result.unwrap();
        assert_eq!(col_name, "UpdatedAt");
    }

    #[test]
    fn test_find_date_column_not_found() {
        let table = make_test_table(vec![make_test_column("Id", "int")]);

        let candidates = vec!["LastActivityDate".to_string()];
        let result = table.find_date_column(&candidates);
        assert!(result.is_none());
    }

    #[test]
    fn test_find_date_column_wrong_type() {
        let table = make_test_table(vec![make_test_column("LastActivityDate", "varchar")]);

        let candidates = vec!["LastActivityDate".to_string()];
        let result = table.find_date_column(&candidates);
        assert!(result.is_none());
    }

    #[test]
    fn test_supports_keyset_pagination() {
        let mut table = make_test_table(vec![make_test_column("Id", "int")]);
        table.pk_columns = vec![make_test_column("Id", "int")];
        assert!(table.supports_keyset_pagination());

        table.pk_columns = vec![make_test_column("Id", "bigint")];
        assert!(table.supports_keyset_pagination());

        table.pk_columns = vec![make_test_column("Id", "varchar")];
        assert!(!table.supports_keyset_pagination());

        // Multiple PK columns
        table.pk_columns = vec![
            make_test_column("Id1", "int"),
            make_test_column("Id2", "int"),
        ];
        assert!(!table.supports_keyset_pagination());
    }

    #[test]
    fn test_is_date_type() {
        assert!(is_date_type("datetime"));
        assert!(is_date_type("datetime2"));
        assert!(is_date_type("smalldatetime"));
        assert!(is_date_type("date"));
        assert!(is_date_type("datetimeoffset"));
        assert!(is_date_type("timestamp"));
        assert!(is_date_type("timestamptz"));
        assert!(is_date_type("timestamp with time zone"));
        assert!(is_date_type("timestamp without time zone"));

        assert!(!is_date_type("varchar"));
        assert!(!is_date_type("int"));
    }
}
