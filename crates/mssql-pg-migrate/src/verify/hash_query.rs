//! SQL query generation for batch verification.
//!
//! This module generates queries for efficient multi-tier verification:
//! - Tier 1/2: Count queries for quick mismatch detection
//! - Tier 3: Row hash queries for detailed comparison
//!
//! MSSQL computes hashes server-side using HASHBYTES to match Rust's format.
//! PostgreSQL uses the existing row_hash column computed during migration.

use crate::source::Table;
use super::normalize::mssql_row_hash_expr;

/// Generate MSSQL query to get row count for a PK range.
///
/// Used for Tier 1/2 quick verification.
pub fn mssql_count_query(schema: &str, table_name: &str, pk_column: &str) -> String {
    format!(
        r#"SELECT COUNT(*) AS row_count
FROM [{schema}].[{table_name}] WITH (NOLOCK)
WHERE [{pk_column}] >= @P1 AND [{pk_column}] < @P2"#,
        schema = schema.replace(']', "]]"),
        table_name = table_name.replace(']', "]]"),
        pk_column = pk_column.replace(']', "]]"),
    )
}

/// Generate PostgreSQL query to get row count for a PK range.
///
/// Used for Tier 1/2 quick verification.
pub fn postgres_count_query(schema: &str, table_name: &str, pk_column: &str) -> String {
    format!(
        r#"SELECT COUNT(*) AS row_count
FROM "{schema}"."{table_name}"
WHERE "{pk_column}"::BIGINT >= $1 AND "{pk_column}"::BIGINT < $2"#,
        schema = schema.replace('"', "\"\""),
        table_name = table_name.replace('"', "\"\""),
        pk_column = pk_column.replace('"', "\"\""),
    )
}

/// Generate MSSQL query to fetch (pk, row_hash) pairs for a PK range.
///
/// Computes hashes server-side using HASHBYTES('MD5', ...) to match Rust's format.
/// Used for Tier 3 row-level comparison.
pub fn mssql_row_hashes_query(schema: &str, table: &Table, pk_column: &str) -> String {
    let pk_columns: Vec<String> = table.primary_key.clone();
    let row_hash_expr = mssql_row_hash_expr(&table.columns, &pk_columns);

    format!(
        r#"SELECT [{pk_column}], {row_hash_expr} AS row_hash
FROM [{schema}].[{table_name}] WITH (NOLOCK)
WHERE [{pk_column}] >= @P1 AND [{pk_column}] < @P2
ORDER BY [{pk_column}]"#,
        pk_column = pk_column.replace(']', "]]"),
        row_hash_expr = row_hash_expr,
        schema = schema.replace(']', "]]"),
        table_name = table.name.replace(']', "]]"),
    )
}

/// Generate PostgreSQL query to fetch (pk, row_hash) pairs for a PK range.
///
/// Uses the existing row_hash column computed during migration.
/// Used for Tier 3 row-level comparison.
pub fn postgres_row_hashes_query(
    schema: &str,
    table_name: &str,
    pk_column: &str,
    row_hash_column: &str,
) -> String {
    format!(
        r#"SELECT "{pk_column}"::BIGINT, "{row_hash_column}"
FROM "{schema}"."{table_name}"
WHERE "{pk_column}"::BIGINT >= $1 AND "{pk_column}"::BIGINT < $2
ORDER BY "{pk_column}""#,
        pk_column = pk_column.replace('"', "\"\""),
        row_hash_column = row_hash_column.replace('"', "\"\""),
        schema = schema.replace('"', "\"\""),
        table_name = table_name.replace('"', "\"\""),
    )
}

/// Generate MSSQL query to get PK boundaries for range splitting.
///
/// Returns min_pk, max_pk, and row_count for the table.
pub fn mssql_pk_bounds_query(schema: &str, table_name: &str, pk_column: &str) -> String {
    format!(
        r#"SELECT
    CAST(MIN([{pk_column}]) AS BIGINT) AS min_pk,
    CAST(MAX([{pk_column}]) AS BIGINT) AS max_pk,
    CAST(COUNT(*) AS BIGINT) AS row_count
FROM [{schema}].[{table_name}] WITH (NOLOCK)"#,
        pk_column = pk_column.replace(']', "]]"),
        schema = schema.replace(']', "]]"),
        table_name = table_name.replace(']', "]]"),
    )
}

/// Generate PostgreSQL query to get PK boundaries for range splitting.
pub fn postgres_pk_bounds_query(schema: &str, table_name: &str, pk_column: &str) -> String {
    format!(
        r#"SELECT
    MIN("{pk_column}")::BIGINT AS min_pk,
    MAX("{pk_column}")::BIGINT AS max_pk,
    COUNT(*)::BIGINT AS row_count
FROM "{schema}"."{table_name}""#,
        pk_column = pk_column.replace('"', "\"\""),
        schema = schema.replace('"', "\"\""),
        table_name = table_name.replace('"', "\"\""),
    )
}

// ============================================================================
// Universal PK Support: NTILE and ROW_NUMBER based queries
// ============================================================================

/// Build ORDER BY clause for composite primary keys (MSSQL).
pub fn mssql_pk_order_by(pk_columns: &[String]) -> String {
    pk_columns
        .iter()
        .map(|col| format!("[{}]", col.replace(']', "]]")))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Build ORDER BY clause for composite primary keys (PostgreSQL).
pub fn postgres_pk_order_by(pk_columns: &[String]) -> String {
    pk_columns
        .iter()
        .map(|col| format!("\"{}\"", col.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Build SELECT clause for PK columns (MSSQL).
pub fn mssql_pk_select(pk_columns: &[String]) -> String {
    pk_columns
        .iter()
        .map(|col| format!("[{}]", col.replace(']', "]]")))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Build SELECT clause for PK columns (PostgreSQL).
pub fn postgres_pk_select(pk_columns: &[String]) -> String {
    pk_columns
        .iter()
        .map(|col| format!("\"{}\"", col.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Generate MSSQL query to partition table using NTILE for Tier 1.
///
/// Returns partition_id and row_count for each partition.
/// This is a one-time query to divide the table into N equal partitions.
pub fn mssql_ntile_partition_query(
    schema: &str,
    table_name: &str,
    pk_columns: &[String],
    num_partitions: i64,
) -> String {
    let pk_order_by = mssql_pk_order_by(pk_columns);
    format!(
        r#"WITH partitioned AS (
    SELECT NTILE({num_partitions}) OVER (ORDER BY {pk_order_by}) AS partition_id
    FROM [{schema}].[{table_name}] WITH (NOLOCK)
)
SELECT
    partition_id,
    CAST(COUNT(*) AS BIGINT) AS row_count
FROM partitioned
GROUP BY partition_id
ORDER BY partition_id"#,
        num_partitions = num_partitions,
        pk_order_by = pk_order_by,
        schema = schema.replace(']', "]]"),
        table_name = table_name.replace(']', "]]"),
    )
}

/// Generate PostgreSQL query to partition table using NTILE for Tier 1.
pub fn postgres_ntile_partition_query(
    schema: &str,
    table_name: &str,
    pk_columns: &[String],
    num_partitions: i64,
) -> String {
    let pk_order_by = postgres_pk_order_by(pk_columns);
    format!(
        r#"WITH partitioned AS (
    SELECT NTILE({num_partitions}) OVER (ORDER BY {pk_order_by}) AS partition_id
    FROM "{schema}"."{table_name}"
)
SELECT
    partition_id,
    COUNT(*)::BIGINT AS row_count
FROM partitioned
GROUP BY partition_id
ORDER BY partition_id"#,
        num_partitions = num_partitions,
        pk_order_by = pk_order_by,
        schema = schema.replace('"', "\"\""),
        table_name = table_name.replace('"', "\"\""),
    )
}

/// Generate MSSQL query to count rows using ROW_NUMBER range.
///
/// Used for Tier 2 verification with any PK type.
pub fn mssql_row_count_with_rownum_query(
    schema: &str,
    table_name: &str,
    pk_columns: &[String],
) -> String {
    let pk_order_by = mssql_pk_order_by(pk_columns);
    format!(
        r#"WITH numbered AS (
    SELECT ROW_NUMBER() OVER (ORDER BY {pk_order_by}) AS row_num
    FROM [{schema}].[{table_name}] WITH (NOLOCK)
)
SELECT COUNT(*) AS row_count
FROM numbered
WHERE row_num >= @P1 AND row_num < @P2"#,
        pk_order_by = pk_order_by,
        schema = schema.replace(']', "]]"),
        table_name = table_name.replace(']', "]]"),
    )
}

/// Generate PostgreSQL query to count rows using ROW_NUMBER range.
pub fn postgres_row_count_with_rownum_query(
    schema: &str,
    table_name: &str,
    pk_columns: &[String],
) -> String {
    let pk_order_by = postgres_pk_order_by(pk_columns);
    format!(
        r#"WITH numbered AS (
    SELECT ROW_NUMBER() OVER (ORDER BY {pk_order_by}) AS row_num
    FROM "{schema}"."{table_name}"
)
SELECT COUNT(*) AS row_count
FROM numbered
WHERE row_num >= $1 AND row_num < $2"#,
        pk_order_by = pk_order_by,
        schema = schema.replace('"', "\"\""),
        table_name = table_name.replace('"', "\"\""),
    )
}

/// Generate MSSQL query to fetch (pk_cols..., row_hash) using ROW_NUMBER range.
///
/// Returns all PK columns plus computed row_hash for Tier 3 comparison.
/// Works with any PK type including composite keys.
pub fn mssql_row_hashes_with_rownum_query(
    schema: &str,
    table: &Table,
) -> String {
    let pk_columns = &table.primary_key;
    let pk_order_by = mssql_pk_order_by(pk_columns);
    let pk_select = mssql_pk_select(pk_columns);
    let row_hash_expr = mssql_row_hash_expr(&table.columns, pk_columns);

    format!(
        r#"WITH numbered AS (
    SELECT {pk_select}, {row_hash_expr} AS row_hash,
           ROW_NUMBER() OVER (ORDER BY {pk_order_by}) AS row_num
    FROM [{schema}].[{table_name}] WITH (NOLOCK)
)
SELECT {pk_select}, row_hash
FROM numbered
WHERE row_num >= @P1 AND row_num < @P2
ORDER BY row_num"#,
        pk_select = pk_select,
        row_hash_expr = row_hash_expr,
        pk_order_by = pk_order_by,
        schema = schema.replace(']', "]]"),
        table_name = table.name.replace(']', "]]"),
    )
}

/// Generate PostgreSQL query to fetch (pk_cols..., row_hash) using ROW_NUMBER range.
///
/// Returns all PK columns plus existing row_hash column for Tier 3 comparison.
pub fn postgres_row_hashes_with_rownum_query(
    schema: &str,
    table_name: &str,
    pk_columns: &[String],
    row_hash_column: &str,
) -> String {
    let pk_order_by = postgres_pk_order_by(pk_columns);
    let pk_select = postgres_pk_select(pk_columns);
    let row_hash_col = row_hash_column.replace('"', "\"\"");

    format!(
        r#"WITH numbered AS (
    SELECT {pk_select}, "{row_hash_col}" AS row_hash,
           ROW_NUMBER() OVER (ORDER BY {pk_order_by}) AS row_num
    FROM "{schema}"."{table_name}"
)
SELECT {pk_select}, row_hash
FROM numbered
WHERE row_num >= $1 AND row_num < $2
ORDER BY row_num"#,
        pk_select = pk_select,
        row_hash_col = row_hash_col,
        pk_order_by = pk_order_by,
        schema = schema.replace('"', "\"\""),
        table_name = table_name.replace('"', "\"\""),
    )
}

/// Generate MSSQL query to get total row count for a table.
pub fn mssql_total_row_count_query(schema: &str, table_name: &str) -> String {
    format!(
        r#"SELECT CAST(COUNT(*) AS BIGINT) AS row_count
FROM [{schema}].[{table_name}] WITH (NOLOCK)"#,
        schema = schema.replace(']', "]]"),
        table_name = table_name.replace(']', "]]"),
    )
}

/// Generate PostgreSQL query to get total row count for a table.
pub fn postgres_total_row_count_query(schema: &str, table_name: &str) -> String {
    format!(
        r#"SELECT COUNT(*)::BIGINT AS row_count
FROM "{schema}"."{table_name}""#,
        schema = schema.replace('"', "\"\""),
        table_name = table_name.replace('"', "\"\""),
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
    fn test_mssql_count_query() {
        let query = mssql_count_query("dbo", "users", "id");
        assert!(query.contains("COUNT(*)"));
        assert!(query.contains("@P1"));
        assert!(query.contains("@P2"));
    }

    #[test]
    fn test_postgres_count_query() {
        let query = postgres_count_query("public", "users", "id");
        assert!(query.contains("COUNT(*)"));
        assert!(query.contains("$1"));
        assert!(query.contains("$2"));
        assert!(query.contains("::BIGINT"));
    }

    #[test]
    fn test_mssql_row_hashes_query() {
        let table = make_test_table();
        let query = mssql_row_hashes_query("dbo", &table, "id");

        assert!(query.contains("HASHBYTES"));
        assert!(query.contains("MD5"));
        assert!(query.contains("[name]"));
        assert!(query.contains("[created_at]"));
        assert!(query.contains("ORDER BY"));
    }

    #[test]
    fn test_postgres_row_hashes_query() {
        let query = postgres_row_hashes_query("public", "users", "id", "row_hash");

        assert!(query.contains("\"row_hash\""));
        assert!(query.contains("$1"));
        assert!(query.contains("$2"));
        assert!(query.contains("ORDER BY"));
    }

    #[test]
    fn test_pk_bounds_queries() {
        let mssql_query = mssql_pk_bounds_query("dbo", "users", "id");
        let pg_query = postgres_pk_bounds_query("public", "users", "id");

        assert!(mssql_query.contains("MIN"));
        assert!(mssql_query.contains("MAX"));
        assert!(mssql_query.contains("BIGINT"));
        assert!(pg_query.contains("MIN"));
        assert!(pg_query.contains("MAX"));
        assert!(pg_query.contains("BIGINT"));
    }
}
