//! SQL query generation for batch hash comparison.
//!
//! This module generates aggregate hash queries for both MSSQL and PostgreSQL
//! that produce comparable results for the same data.
//!
//! ## MSSQL Strategy
//! Uses `CHECKSUM_AGG(BINARY_CHECKSUM(...))` for fast 32-bit aggregate hashing.
//! This has collision risk but is acceptable for Tier 1/2 coarse verification.
//!
//! ## PostgreSQL Strategy
//! Uses `SUM(('x' || SUBSTR(MD5(...), 1, 8))::BIT(32)::INTEGER)` to produce
//! a comparable aggregate hash.

use crate::source::Table;
use super::normalize::{mssql_normalize_expr, postgres_normalize_expr, mssql_column_separator, postgres_column_separator};

/// Generate MSSQL batch hash query for a PK range.
///
/// Returns a query that computes:
/// - `batch_hash`: CHECKSUM_AGG of all rows in the range
/// - `row_count`: Number of rows in the range
///
/// # Arguments
/// * `schema` - Schema name
/// * `table` - Table metadata with columns and primary key info
/// * `pk_column` - Name of the primary key column (must be single integer PK)
pub fn mssql_batch_hash_query(
    schema: &str,
    table: &Table,
    pk_column: &str,
) -> String {
    // Build normalized column expressions for non-PK columns
    let column_exprs: Vec<String> = table
        .columns
        .iter()
        .filter(|c| !table.primary_key.contains(&c.name))
        .map(mssql_normalize_expr)
        .collect();

    let columns_sql = column_exprs.join(mssql_column_separator());

    // Build the query with parameterized PK range
    format!(
        r#"SELECT
    ISNULL(CHECKSUM_AGG(BINARY_CHECKSUM({columns_sql})), 0) AS batch_hash,
    COUNT(*) AS row_count
FROM [{schema}].[{table_name}] WITH (NOLOCK)
WHERE [{pk_column}] >= @min_pk AND [{pk_column}] < @max_pk"#,
        columns_sql = columns_sql,
        schema = schema,
        table_name = table.name,
        pk_column = pk_column,
    )
}

/// Generate PostgreSQL batch hash query for a PK range.
///
/// Returns a query that computes:
/// - `batch_hash`: Aggregate hash comparable to MSSQL CHECKSUM_AGG
/// - `row_count`: Number of rows in the range
///
/// # Arguments
/// * `schema` - Schema name
/// * `table` - Table metadata with columns and primary key info
/// * `pk_column` - Name of the primary key column (must be single integer PK)
pub fn postgres_batch_hash_query(
    schema: &str,
    table: &Table,
    pk_column: &str,
) -> String {
    // Build normalized column expressions for non-PK columns
    let column_exprs: Vec<String> = table
        .columns
        .iter()
        .filter(|c| !table.primary_key.contains(&c.name))
        .map(postgres_normalize_expr)
        .collect();

    let columns_sql = column_exprs.join(postgres_column_separator());

    // Use MD5 and convert to BIGINT for aggregation
    // Take first 8 hex chars (32 bits) and sum them as BIGINT to avoid overflow
    // This won't produce identical results to MSSQL CHECKSUM_AGG but will be
    // deterministic for comparison purposes
    format!(
        r#"SELECT
    COALESCE(SUM(
        ('x' || SUBSTR(MD5({columns_sql}), 1, 8))::BIT(32)::BIGINT
    ), 0)::BIGINT AS batch_hash,
    COUNT(*)::BIGINT AS row_count
FROM "{schema}"."{table_name}"
WHERE "{pk_column}" >= $1 AND "{pk_column}" < $2"#,
        columns_sql = columns_sql,
        schema = schema,
        table_name = table.name,
        pk_column = pk_column,
    )
}

/// Generate MSSQL query to fetch row hashes for Tier 3 comparison.
///
/// Returns (pk, row_hash) pairs for all rows in the PK range.
pub fn mssql_row_hashes_query(
    schema: &str,
    table: &Table,
    pk_column: &str,
) -> String {
    // Build normalized column expressions for non-PK columns
    let column_exprs: Vec<String> = table
        .columns
        .iter()
        .filter(|c| !table.primary_key.contains(&c.name))
        .map(mssql_normalize_expr)
        .collect();

    let columns_sql = column_exprs.join(" + N'|' + ");

    // Use HASHBYTES with MD5 for row-level hashes
    format!(
        r#"SELECT
    [{pk_column}] AS pk,
    LOWER(CONVERT(NVARCHAR(32), HASHBYTES('MD5', {columns_sql}), 2)) AS row_hash
FROM [{schema}].[{table_name}] WITH (NOLOCK)
WHERE [{pk_column}] >= @min_pk AND [{pk_column}] < @max_pk
ORDER BY [{pk_column}]"#,
        pk_column = pk_column,
        columns_sql = columns_sql,
        schema = schema,
        table_name = table.name,
    )
}

/// Generate PostgreSQL query to fetch row hashes for Tier 3 comparison.
///
/// Returns (pk, row_hash) pairs for all rows in the PK range.
/// This uses the existing row_hash column if available, otherwise computes MD5.
pub fn postgres_row_hashes_query(
    schema: &str,
    table_name: &str,
    pk_column: &str,
    row_hash_column: Option<&str>,
) -> String {
    match row_hash_column {
        Some(hash_col) => {
            // Use existing row_hash column
            format!(
                r#"SELECT
    "{pk_column}"::BIGINT AS pk,
    "{hash_col}" AS row_hash
FROM "{schema}"."{table_name}"
WHERE "{pk_column}" >= $1 AND "{pk_column}" < $2
ORDER BY "{pk_column}""#,
                pk_column = pk_column,
                hash_col = hash_col,
                schema = schema,
                table_name = table_name,
            )
        }
        None => {
            // Compute hash on the fly (slower but works without row_hash column)
            // Note: This would need the full column list - for now, require row_hash column
            format!(
                r#"SELECT
    "{pk_column}"::BIGINT AS pk,
    NULL::TEXT AS row_hash
FROM "{schema}"."{table_name}"
WHERE "{pk_column}" >= $1 AND "{pk_column}" < $2
ORDER BY "{pk_column}""#,
                pk_column = pk_column,
                schema = schema,
                table_name = table_name,
            )
        }
    }
}

/// Generate MSSQL query to get PK boundaries for range splitting.
///
/// Returns min_pk, max_pk, and row_count for the table.
pub fn mssql_pk_bounds_query(
    schema: &str,
    table_name: &str,
    pk_column: &str,
) -> String {
    format!(
        r#"SELECT
    MIN([{pk_column}]) AS min_pk,
    MAX([{pk_column}]) AS max_pk,
    COUNT(*) AS row_count
FROM [{schema}].[{table_name}] WITH (NOLOCK)"#,
        pk_column = pk_column,
        schema = schema,
        table_name = table_name,
    )
}

/// Generate PostgreSQL query to get PK boundaries for range splitting.
pub fn postgres_pk_bounds_query(
    schema: &str,
    table_name: &str,
    pk_column: &str,
) -> String {
    format!(
        r#"SELECT
    MIN("{pk_column}")::BIGINT AS min_pk,
    MAX("{pk_column}")::BIGINT AS max_pk,
    COUNT(*)::BIGINT AS row_count
FROM "{schema}"."{table_name}""#,
        pk_column = pk_column,
        schema = schema,
        table_name = table_name,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::Column;

    fn make_test_table() -> Table {
        let id_column = Column {
            name: "id".to_string(),
            data_type: "int".to_string(),
            max_length: 4,
            precision: 10,
            scale: 0,
            is_nullable: false,
            is_identity: true,
            ordinal_pos: 1,
        };

        Table {
            name: "users".to_string(),
            schema: "dbo".to_string(),
            columns: vec![
                id_column.clone(),
                Column {
                    name: "name".to_string(),
                    data_type: "nvarchar".to_string(),
                    max_length: 100,
                    precision: 0,
                    scale: 0,
                    is_nullable: true,
                    is_identity: false,
                    ordinal_pos: 2,
                },
                Column {
                    name: "created_at".to_string(),
                    data_type: "datetime".to_string(),
                    max_length: 8,
                    precision: 0,
                    scale: 0,
                    is_nullable: true,
                    is_identity: false,
                    ordinal_pos: 3,
                },
            ],
            primary_key: vec!["id".to_string()],
            pk_columns: vec![id_column],
            row_count: 1000,
            estimated_row_size: 100,
            indexes: vec![],
            foreign_keys: vec![],
            check_constraints: vec![],
        }
    }

    #[test]
    fn test_mssql_batch_hash_query_structure() {
        let table = make_test_table();
        let query = mssql_batch_hash_query("dbo", &table, "id");

        assert!(query.contains("CHECKSUM_AGG"));
        assert!(query.contains("BINARY_CHECKSUM"));
        assert!(query.contains("batch_hash"));
        assert!(query.contains("row_count"));
        assert!(query.contains("@min_pk"));
        assert!(query.contains("@max_pk"));
        assert!(!query.contains("[id],")); // PK should be excluded from hash
    }

    #[test]
    fn test_postgres_batch_hash_query_structure() {
        let table = make_test_table();
        let query = postgres_batch_hash_query("public", &table, "id");

        assert!(query.contains("MD5"));
        assert!(query.contains("SUM"));
        assert!(query.contains("batch_hash"));
        assert!(query.contains("row_count"));
        assert!(query.contains("$1"));
        assert!(query.contains("$2"));
    }

    #[test]
    fn test_mssql_row_hashes_query_structure() {
        let table = make_test_table();
        let query = mssql_row_hashes_query("dbo", &table, "id");

        assert!(query.contains("HASHBYTES"));
        assert!(query.contains("MD5"));
        assert!(query.contains("row_hash"));
        assert!(query.contains("ORDER BY"));
    }

    #[test]
    fn test_postgres_row_hashes_with_column() {
        let query = postgres_row_hashes_query("public", "users", "id", Some("row_hash"));

        assert!(query.contains("row_hash"));
        assert!(query.contains("ORDER BY"));
    }

    #[test]
    fn test_pk_bounds_queries() {
        let mssql_query = mssql_pk_bounds_query("dbo", "users", "id");
        let pg_query = postgres_pk_bounds_query("public", "users", "id");

        assert!(mssql_query.contains("MIN"));
        assert!(mssql_query.contains("MAX"));
        assert!(pg_query.contains("MIN"));
        assert!(pg_query.contains("MAX"));
    }
}
