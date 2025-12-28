//! SQL query generation for batch verification.
//!
//! This module generates queries for efficient multi-tier verification:
//! - Tier 1: NTILE partitioning for quick mismatch detection (count + hash)
//! - Tier 2: ROW_NUMBER ranges for drilling down into mismatched partitions
//! - Tier 3: Row hash queries for detailed comparison
//!
//! MSSQL computes hashes server-side using HASHBYTES to match Rust's format.
//! PostgreSQL uses the existing row_hash column computed during migration.
//!
//! ## Partition Hash Algorithm
//!
//! To detect updates (not just inserts/deletes), Tier 1 and Tier 2 include
//! an XOR aggregate of row hashes:
//!
//! - **MSSQL**: `CHECKSUM_AGG(CHECKSUM(CONVERT(VARBINARY(8), row_hash, 2)))`
//!   where `row_hash` is the MD5 hash as hex string converted to varbinary
//! - **PostgreSQL**: `BIT_XOR(('x' || LEFT(row_hash, 16))::bit(64)::bigint)`
//!   on the stored row_hash column
//!
//! **Note**: MSSQL CHECKSUM returns 32-bit while PostgreSQL uses 64-bit hashes.
//! This means partition hashes will differ between source and target even for
//! identical data. The system handles this correctly by drilling down to Tier 3
//! for row-level comparison when hashes mismatch.
//!
//! When partition hashes differ, the partition is marked for Tier 2/3 drill-down
//! even if row counts match (indicating updates rather than inserts/deletes).

use crate::source::Table;
use super::normalize::mssql_row_hash_expr;
use super::normalize_pg::pg_row_hash_expr;

// ============================================================================
// Helper functions for building SQL clauses
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

// ============================================================================
// Tier 1: NTILE partitioning queries
// ============================================================================

/// Generate MSSQL query to partition table using NTILE for Tier 1.
///
/// Returns partition_id, row_count, and partition_hash for each partition.
/// The partition_hash is an XOR aggregate of row hashes, enabling detection
/// of updates (not just inserts/deletes) when row counts match.
///
/// This is a one-time query to divide the table into N approximately equal partitions
/// (some partitions may contain at most one more row than others when row count
/// doesn't divide evenly).
pub fn mssql_ntile_partition_query_with_hash(
    schema: &str,
    table: &Table,
    num_partitions: i64,
) -> String {
    let pk_columns = &table.primary_key;
    let pk_order_by = mssql_pk_order_by(pk_columns);
    let row_hash_expr = mssql_row_hash_expr(&table.columns, pk_columns);

    // XOR aggregate: convert first 16 hex chars of MD5 to bigint, then CHECKSUM_AGG
    // CHECKSUM_AGG performs XOR of checksums
    format!(
        r#"WITH partitioned AS (
    SELECT NTILE({num_partitions}) OVER (ORDER BY {pk_order_by}) AS partition_id,
           {row_hash_expr} AS row_hash
    FROM [{schema}].[{table_name}] WITH (NOLOCK)
)
SELECT
    partition_id,
    CAST(COUNT(*) AS BIGINT) AS row_count,
    CAST(ISNULL(CHECKSUM_AGG(CHECKSUM(CONVERT(VARBINARY(8), row_hash, 2))), 0) AS BIGINT) AS partition_hash
FROM partitioned
GROUP BY partition_id
ORDER BY partition_id"#,
        num_partitions = num_partitions,
        pk_order_by = pk_order_by,
        row_hash_expr = row_hash_expr,
        schema = schema.replace(']', "]]"),
        table_name = table.name.replace(']', "]]"),
    )
}

/// Generate MSSQL query to partition table using NTILE for Tier 1 (count only).
///
/// Returns partition_id and row_count for each partition.
/// Use `mssql_ntile_partition_query_with_hash` for update detection.
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
///
/// Returns partition_id, row_count, and partition_hash for each partition.
/// Uses BIT_XOR aggregate on the stored row_hash column for update detection.
///
/// The row_hash column should contain MD5 hashes computed during migration
/// using the same algorithm as MSSQL, ensuring hash compatibility.
pub fn postgres_ntile_partition_query_with_hash(
    schema: &str,
    table_name: &str,
    pk_columns: &[String],
    num_partitions: i64,
    row_hash_column: &str,
) -> String {
    let pk_order_by = postgres_pk_order_by(pk_columns);
    let row_hash_col = row_hash_column.replace('"', "\"\"");

    // BIT_XOR aggregate: convert first 16 hex chars of row_hash to bigint
    // COALESCE handles NULL row_hash (returns 0, triggering mismatch)
    format!(
        r#"WITH partitioned AS (
    SELECT NTILE({num_partitions}) OVER (ORDER BY {pk_order_by}) AS partition_id,
           "{row_hash_col}" AS row_hash
    FROM "{schema}"."{table_name}"
)
SELECT
    partition_id,
    COUNT(*)::BIGINT AS row_count,
    COALESCE(BIT_XOR(('x' || COALESCE(LEFT(row_hash, 16), '0000000000000000'))::bit(64)::bigint), 0)::BIGINT AS partition_hash
FROM partitioned
GROUP BY partition_id
ORDER BY partition_id"#,
        num_partitions = num_partitions,
        pk_order_by = pk_order_by,
        row_hash_col = row_hash_col,
        schema = schema.replace('"', "\"\""),
        table_name = table_name.replace('"', "\"\""),
    )
}

/// Generate PostgreSQL query to partition table using NTILE for Tier 1.
///
/// Returns partition_id, row_count, and partition_hash for each partition.
/// Computes row hashes on-the-fly for PostgreSQL as SOURCE (no stored row_hash).
pub fn postgres_ntile_partition_query_computed(
    schema: &str,
    table: &Table,
    num_partitions: i64,
) -> String {
    let pk_columns = &table.primary_key;
    let pk_order_by = postgres_pk_order_by(pk_columns);
    let row_hash_expr = pg_row_hash_expr(&table.columns, pk_columns);

    // BIT_XOR aggregate on computed MD5 hashes
    format!(
        r#"WITH partitioned AS (
    SELECT NTILE({num_partitions}) OVER (ORDER BY {pk_order_by}) AS partition_id,
           {row_hash_expr} AS row_hash
    FROM "{schema}"."{table_name}"
)
SELECT
    partition_id,
    COUNT(*)::BIGINT AS row_count,
    COALESCE(BIT_XOR(('x' || COALESCE(LEFT(row_hash, 16), '0000000000000000'))::bit(64)::bigint), 0)::BIGINT AS partition_hash
FROM partitioned
GROUP BY partition_id
ORDER BY partition_id"#,
        num_partitions = num_partitions,
        pk_order_by = pk_order_by,
        row_hash_expr = row_hash_expr,
        schema = schema.replace('"', "\"\""),
        table_name = table.name.replace('"', "\"\""),
    )
}

/// Generate PostgreSQL query to partition table using NTILE for Tier 1 (count only).
///
/// Returns partition_id and row_count for each partition. NTILE divides into
/// approximately equal partitions (some may have one more row than others).
/// Use `postgres_ntile_partition_query_with_hash` for update detection.
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

// ============================================================================
// Tier 2: ROW_NUMBER range count queries
// ============================================================================

/// Generate MSSQL query to count rows and compute range hash using ROW_NUMBER range.
///
/// Returns row_count and range_hash for Tier 2 verification with update detection.
pub fn mssql_row_count_with_rownum_query_with_hash(
    schema: &str,
    table: &Table,
) -> String {
    let pk_columns = &table.primary_key;
    let pk_order_by = mssql_pk_order_by(pk_columns);
    let row_hash_expr = mssql_row_hash_expr(&table.columns, pk_columns);

    format!(
        r#"WITH numbered AS (
    SELECT ROW_NUMBER() OVER (ORDER BY {pk_order_by}) AS row_num,
           {row_hash_expr} AS row_hash
    FROM [{schema}].[{table_name}] WITH (NOLOCK)
)
SELECT
    COUNT(*) AS row_count,
    CAST(ISNULL(CHECKSUM_AGG(CHECKSUM(CONVERT(VARBINARY(8), row_hash, 2))), 0) AS BIGINT) AS range_hash
FROM numbered
WHERE row_num >= @P1 AND row_num < @P2"#,
        pk_order_by = pk_order_by,
        row_hash_expr = row_hash_expr,
        schema = schema.replace(']', "]]"),
        table_name = table.name.replace(']', "]]"),
    )
}

/// Generate MSSQL query to count rows using ROW_NUMBER range (count only).
///
/// Used for Tier 2 verification with any PK type.
/// Use `mssql_row_count_with_rownum_query_with_hash` for update detection.
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

/// Generate PostgreSQL query to count rows and compute range hash using ROW_NUMBER range.
///
/// Returns row_count and range_hash for Tier 2 verification with update detection.
/// Uses stored row_hash column (for PostgreSQL as target).
pub fn postgres_row_count_with_rownum_query_with_hash(
    schema: &str,
    table_name: &str,
    pk_columns: &[String],
    row_hash_column: &str,
) -> String {
    let pk_order_by = postgres_pk_order_by(pk_columns);
    let row_hash_col = row_hash_column.replace('"', "\"\"");

    format!(
        r#"WITH numbered AS (
    SELECT ROW_NUMBER() OVER (ORDER BY {pk_order_by}) AS row_num,
           "{row_hash_col}" AS row_hash
    FROM "{schema}"."{table_name}"
)
SELECT
    COUNT(*) AS row_count,
    COALESCE(BIT_XOR(('x' || COALESCE(LEFT(row_hash, 16), '0000000000000000'))::bit(64)::bigint), 0)::BIGINT AS range_hash
FROM numbered
WHERE row_num >= $1 AND row_num < $2"#,
        pk_order_by = pk_order_by,
        row_hash_col = row_hash_col,
        schema = schema.replace('"', "\"\""),
        table_name = table_name.replace('"', "\"\""),
    )
}

/// Generate PostgreSQL query to count rows and compute range hash using ROW_NUMBER range.
///
/// Returns row_count and range_hash for Tier 2 verification with update detection.
/// Computes row hashes on-the-fly (for PostgreSQL as source).
pub fn postgres_row_count_with_rownum_query_computed(
    schema: &str,
    table: &Table,
) -> String {
    let pk_columns = &table.primary_key;
    let pk_order_by = postgres_pk_order_by(pk_columns);
    let row_hash_expr = pg_row_hash_expr(&table.columns, pk_columns);

    format!(
        r#"WITH numbered AS (
    SELECT ROW_NUMBER() OVER (ORDER BY {pk_order_by}) AS row_num,
           {row_hash_expr} AS row_hash
    FROM "{schema}"."{table_name}"
)
SELECT
    COUNT(*) AS row_count,
    COALESCE(BIT_XOR(('x' || COALESCE(LEFT(row_hash, 16), '0000000000000000'))::bit(64)::bigint), 0)::BIGINT AS range_hash
FROM numbered
WHERE row_num >= $1 AND row_num < $2"#,
        pk_order_by = pk_order_by,
        row_hash_expr = row_hash_expr,
        schema = schema.replace('"', "\"\""),
        table_name = table.name.replace('"', "\"\""),
    )
}

/// Generate PostgreSQL query to count rows using ROW_NUMBER range (count only).
///
/// Use `postgres_row_count_with_rownum_query_with_hash` for update detection.
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

// ============================================================================
// Tier 3: Row hash queries with ROW_NUMBER
// ============================================================================

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

/// Generate PostgreSQL query to fetch (pk_cols..., row_hash) using ROW_NUMBER range.
///
/// This variant *computes* the row hash server-side using MD5 via [`pg_row_hash_expr`],
/// and is intended for scenarios where PostgreSQL is the **source** and there is no
/// precomputed `row_hash` column on the table.
///
/// In contrast, [`postgres_row_hashes_with_rownum_query`] reads an existing `row_hash`
/// column (typically on the migrated/target PostgreSQL table) instead of recomputing it.
/// Both functions return the same logical shape: `(pk_cols..., row_hash)` ordered by PK.
///
/// Works with any PK type including composite keys.
pub fn postgres_row_hashes_with_rownum_query_computed(
    schema: &str,
    table: &Table,
) -> String {
    let pk_columns = &table.primary_key;
    let pk_order_by = postgres_pk_order_by(pk_columns);
    let pk_select = postgres_pk_select(pk_columns);
    let row_hash_expr = pg_row_hash_expr(&table.columns, pk_columns);

    format!(
        r#"WITH numbered AS (
    SELECT {pk_select}, {row_hash_expr} AS row_hash,
           ROW_NUMBER() OVER (ORDER BY {pk_order_by}) AS row_num
    FROM "{schema}"."{table_name}"
)
SELECT {pk_select}, row_hash
FROM numbered
WHERE row_num >= $1 AND row_num < $2
ORDER BY row_num"#,
        pk_select = pk_select,
        row_hash_expr = row_hash_expr,
        pk_order_by = pk_order_by,
        schema = schema.replace('"', "\"\""),
        table_name = table.name.replace('"', "\"\""),
    )
}

// ============================================================================
// Total row count queries
// ============================================================================

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
    fn test_mssql_ntile_partition_query() {
        let pk_columns = vec!["id".to_string()];
        let query = mssql_ntile_partition_query("dbo", "users", &pk_columns, 10);
        assert!(query.contains("NTILE(10)"));
        assert!(query.contains("ORDER BY [id]"));
        assert!(query.contains("partition_id"));
    }

    #[test]
    fn test_postgres_ntile_partition_query() {
        let pk_columns = vec!["id".to_string()];
        let query = postgres_ntile_partition_query("public", "users", &pk_columns, 10);
        assert!(query.contains("NTILE(10)"));
        assert!(query.contains("ORDER BY \"id\""));
        assert!(query.contains("partition_id"));
    }

    #[test]
    fn test_mssql_row_count_with_rownum_query() {
        let pk_columns = vec!["id".to_string()];
        let query = mssql_row_count_with_rownum_query("dbo", "users", &pk_columns);
        assert!(query.contains("ROW_NUMBER()"));
        assert!(query.contains("@P1"));
        assert!(query.contains("@P2"));
    }

    #[test]
    fn test_postgres_row_count_with_rownum_query() {
        let pk_columns = vec!["id".to_string()];
        let query = postgres_row_count_with_rownum_query("public", "users", &pk_columns);
        assert!(query.contains("ROW_NUMBER()"));
        assert!(query.contains("$1"));
        assert!(query.contains("$2"));
    }

    #[test]
    fn test_mssql_row_hashes_with_rownum_query() {
        let table = make_test_table();
        let query = mssql_row_hashes_with_rownum_query("dbo", &table);

        assert!(query.contains("HASHBYTES"));
        assert!(query.contains("MD5"));
        assert!(query.contains("[name]"));
        assert!(query.contains("[created_at]"));
        assert!(query.contains("ROW_NUMBER()"));
    }

    #[test]
    fn test_postgres_row_hashes_with_rownum_query() {
        let pk_columns = vec!["id".to_string()];
        let query = postgres_row_hashes_with_rownum_query("public", "users", &pk_columns, "row_hash");

        assert!(query.contains("\"row_hash\""));
        assert!(query.contains("$1"));
        assert!(query.contains("$2"));
        assert!(query.contains("ROW_NUMBER()"));
    }

    #[test]
    fn test_postgres_row_hashes_with_rownum_query_computed() {
        let table = make_test_table();
        let query = postgres_row_hashes_with_rownum_query_computed("public", &table);

        assert!(query.contains("md5"));
        assert!(query.contains("\"name\""));
        assert!(query.contains("\"created_at\""));
        assert!(query.contains("$1"));
        assert!(query.contains("$2"));
        assert!(query.contains("ROW_NUMBER()"));
    }

    #[test]
    fn test_total_row_count_queries() {
        let mssql_query = mssql_total_row_count_query("dbo", "users");
        let pg_query = postgres_total_row_count_query("public", "users");

        assert!(mssql_query.contains("COUNT(*)"));
        assert!(mssql_query.contains("BIGINT"));
        assert!(pg_query.contains("COUNT(*)"));
        assert!(pg_query.contains("BIGINT"));
    }

    #[test]
    fn test_composite_pk_order_by() {
        let pk_columns = vec!["tenant_id".to_string(), "order_id".to_string()];

        let mssql_order = mssql_pk_order_by(&pk_columns);
        assert_eq!(mssql_order, "[tenant_id], [order_id]");

        let pg_order = postgres_pk_order_by(&pk_columns);
        assert_eq!(pg_order, "\"tenant_id\", \"order_id\"");
    }

    // =========================================================================
    // Tests for hash-enabled queries (update detection)
    // =========================================================================

    #[test]
    fn test_mssql_ntile_partition_query_with_hash() {
        let table = make_test_table();
        let query = mssql_ntile_partition_query_with_hash("dbo", &table, 10);

        assert!(query.contains("NTILE(10)"));
        assert!(query.contains("partition_id"));
        assert!(query.contains("row_count"));
        assert!(query.contains("partition_hash"));
        assert!(query.contains("CHECKSUM_AGG"));
        assert!(query.contains("HASHBYTES"));
    }

    #[test]
    fn test_postgres_ntile_partition_query_with_hash() {
        let pk_columns = vec!["id".to_string()];
        let query = postgres_ntile_partition_query_with_hash(
            "public", "users", &pk_columns, 10, "row_hash"
        );

        assert!(query.contains("NTILE(10)"));
        assert!(query.contains("partition_id"));
        assert!(query.contains("row_count"));
        assert!(query.contains("partition_hash"));
        assert!(query.contains("BIT_XOR"));
        assert!(query.contains("row_hash"));
    }

    #[test]
    fn test_postgres_ntile_partition_query_computed() {
        let table = make_test_table();
        let query = postgres_ntile_partition_query_computed("public", &table, 10);

        assert!(query.contains("NTILE(10)"));
        assert!(query.contains("partition_id"));
        assert!(query.contains("row_count"));
        assert!(query.contains("partition_hash"));
        assert!(query.contains("BIT_XOR"));
        assert!(query.contains("md5")); // computed hash
    }

    #[test]
    fn test_mssql_row_count_with_rownum_query_with_hash() {
        let table = make_test_table();
        let query = mssql_row_count_with_rownum_query_with_hash("dbo", &table);

        assert!(query.contains("ROW_NUMBER()"));
        assert!(query.contains("row_count"));
        assert!(query.contains("range_hash"));
        assert!(query.contains("CHECKSUM_AGG"));
        assert!(query.contains("@P1"));
        assert!(query.contains("@P2"));
    }

    #[test]
    fn test_postgres_row_count_with_rownum_query_with_hash() {
        let pk_columns = vec!["id".to_string()];
        let query = postgres_row_count_with_rownum_query_with_hash(
            "public", "users", &pk_columns, "row_hash"
        );

        assert!(query.contains("ROW_NUMBER()"));
        assert!(query.contains("row_count"));
        assert!(query.contains("range_hash"));
        assert!(query.contains("BIT_XOR"));
        assert!(query.contains("$1"));
        assert!(query.contains("$2"));
    }

    #[test]
    fn test_postgres_row_count_with_rownum_query_computed() {
        let table = make_test_table();
        let query = postgres_row_count_with_rownum_query_computed("public", &table);

        assert!(query.contains("ROW_NUMBER()"));
        assert!(query.contains("row_count"));
        assert!(query.contains("range_hash"));
        assert!(query.contains("BIT_XOR"));
        assert!(query.contains("md5")); // computed hash
    }
}
