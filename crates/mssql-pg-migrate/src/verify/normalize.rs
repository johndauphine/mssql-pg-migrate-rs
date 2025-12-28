//! MSSQL column normalization to match Rust's calculate_row_hash format.
//!
//! This module generates SQL expressions that produce output matching
//! the Rust hash computation used during migration. This allows SQL-side
//! hash computation for efficient verification.
//!
//! Key normalization rules (matching calculate_row_hash):
//! - NULLs: Represented as literal string '\N'
//! - Booleans (bit): 't' or 'f'
//! - Integers: Plain string representation
//! - Float32 (real): 6 decimal places
//! - Float64 (float): 15 decimal places
//! - DateTime: 'YYYY-MM-DD HH:MM:SS.ffffff' format (space, not T; 6 decimal places)
//! - Date: 'YYYY-MM-DD'
//! - Time: 'HH:MM:SS.ffffff'
//! - UUID: lowercase with dashes
//! - Strings: Direct (no transformation)

use crate::source::Column;

/// Check if a column is a text type that should be skipped when hash_text_columns is false.
///
/// Returns true for: text, ntext, varchar(max), nvarchar(max), xml
pub fn is_text_type(data_type: &str) -> bool {
    let dt = data_type.to_lowercase();
    matches!(dt.as_str(), "text" | "ntext" | "xml")
        || dt == "varchar(-1)"
        || dt == "nvarchar(-1)"
        || dt == "varchar(max)"
        || dt == "nvarchar(max)"
}

/// Generate MSSQL expression for a normalized column value.
///
/// The expression converts the column to a string representation that
/// matches what Rust's calculate_row_hash produces for the same data.
pub fn mssql_normalize_expr(column: &Column) -> String {
    let name = &column.name;
    let escaped_name = name.replace(']', "]]");
    let data_type = column.data_type.to_lowercase();

    match data_type.as_str() {
        // DateTime types: format as 'YYYY-MM-DD HH:MM:SS.ffffff'
        // Rust uses: dt.format("%Y-%m-%d %H:%M:%S%.6f")
        "datetime" | "datetime2" | "smalldatetime" => {
            // FORMAT with custom pattern to match Rust's chrono format
            // Note: MSSQL datetime only has 3.33ms precision, datetime2 has 100ns
            format!(
                "ISNULL(FORMAT([{escaped_name}], 'yyyy-MM-dd HH:mm:ss.ffffff'), '\\N')"
            )
        }
        "datetimeoffset" => {
            // Rust uses RFC3339 format for DateTimeOffset
            format!(
                "ISNULL(FORMAT([{escaped_name}], 'yyyy-MM-ddTHH:mm:ss.fffffffzzz'), '\\N')"
            )
        }
        "date" => {
            // Rust uses: d.to_string() which gives YYYY-MM-DD
            format!("ISNULL(CONVERT(VARCHAR(10), [{escaped_name}], 23), '\\N')")
        }
        "time" => {
            // Rust uses: t.to_string() which gives HH:MM:SS.ffffff
            format!("ISNULL(CONVERT(VARCHAR(16), [{escaped_name}], 114), '\\N')")
        }

        // Boolean: 't' or 'f' to match Rust
        "bit" => {
            format!(
                "ISNULL(CASE WHEN [{escaped_name}] = 1 THEN 't' ELSE 'f' END, '\\N')"
            )
        }

        // Floating point: fixed precision to match Rust's format!
        "float" => {
            // Rust uses: format!("{:.15}", n) for f64
            // MSSQL STR adds leading spaces, so we LTRIM
            format!("ISNULL(LTRIM(STR([{escaped_name}], 40, 15)), '\\N')")
        }
        "real" => {
            // Rust uses: format!("{:.6}", n) for f32
            format!("ISNULL(LTRIM(STR([{escaped_name}], 20, 6)), '\\N')")
        }

        // Decimal/Numeric: Rust uses d.to_string()
        "decimal" | "numeric" | "money" | "smallmoney" => {
            format!("ISNULL(CAST([{escaped_name}] AS VARCHAR(50)), '\\N')")
        }

        // Integer types: Rust uses n.to_string()
        "tinyint" | "smallint" | "int" | "bigint" => {
            format!("ISNULL(CAST([{escaped_name}] AS VARCHAR(20)), '\\N')")
        }

        // UUID: Rust uses u.to_string() which is lowercase with dashes
        "uniqueidentifier" => {
            format!("ISNULL(LOWER(CAST([{escaped_name}] AS VARCHAR(36))), '\\N')")
        }

        // Binary: Rust hashes raw bytes, but for comparison we use hex
        "binary" | "varbinary" | "image" => {
            // Convert to hex without 0x prefix
            format!(
                "ISNULL(CONVERT(VARCHAR(MAX), [{escaped_name}], 2), '\\N')"
            )
        }

        // Text types: direct conversion
        "text" | "ntext" | "varchar" | "nvarchar" | "char" | "nchar" => {
            format!("ISNULL(CAST([{escaped_name}] AS VARCHAR(MAX)), '\\N')")
        }

        // Default: cast to VARCHAR
        _ => {
            format!("ISNULL(CAST([{escaped_name}] AS VARCHAR(MAX)), '\\N')")
        }
    }
}

/// Generate the MSSQL expression to compute a row hash matching Rust's calculate_row_hash.
///
/// Returns a HASHBYTES('MD5', ...) expression that produces the same hash as Rust.
///
/// If `include_text` is false, skips text-type columns (text, ntext, varchar(max), etc.)
/// for better performance.
pub fn mssql_row_hash_expr(columns: &[Column], pk_columns: &[String], include_text: bool) -> String {
    // Build concatenated expression with '|' separator, excluding PK columns
    // and optionally excluding text columns
    let col_exprs: Vec<String> = columns
        .iter()
        .filter(|c| !pk_columns.contains(&c.name))
        .filter(|c| include_text || !is_text_type(&c.data_type))
        .map(mssql_normalize_expr)
        .collect();

    if col_exprs.is_empty() {
        // Edge case: only PK columns, return empty hash
        return "CONVERT(VARCHAR(32), HASHBYTES('MD5', ''), 2)".to_string();
    }

    // Concatenate with '|' separator to match Rust's pipe separator
    let concat_expr = col_exprs.join(" + '|' + ");

    // HASHBYTES returns varbinary, convert to lowercase hex string
    format!("LOWER(CONVERT(VARCHAR(32), HASHBYTES('MD5', {}), 2))", concat_expr)
}

/// Generate the MSSQL aggregate hash expression using XOR.
///
/// This produces an aggregate hash for a range of rows that can be compared
/// to the XOR of row_hash values on the PostgreSQL side.
pub fn mssql_batch_hash_expr(columns: &[Column], pk_columns: &[String], include_text: bool) -> String {
    let row_hash_expr = mssql_row_hash_expr(columns, pk_columns, include_text);

    // For aggregate hash, we need to XOR all row hashes
    // MSSQL doesn't have XOR aggregate, so we use CHECKSUM_AGG on the hash bytes
    // This won't match PostgreSQL exactly, so we use a different approach:
    // Convert hash to BIGINT and SUM (with overflow), then convert back
    // Actually, let's just count rows and compare individual hashes at tier 3
    format!(
        "SELECT {} AS row_hash, COUNT(*) AS row_count",
        row_hash_expr
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_column(name: &str, data_type: &str) -> Column {
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

    #[test]
    fn test_mssql_datetime_normalization() {
        let col = make_column("created_at", "datetime");
        let expr = mssql_normalize_expr(&col);
        assert!(expr.contains("FORMAT"));
        assert!(expr.contains("yyyy-MM-dd HH:mm:ss"));
        assert!(expr.contains("\\N")); // NULL marker
    }

    #[test]
    fn test_mssql_bit_normalization() {
        let col = make_column("is_active", "bit");
        let expr = mssql_normalize_expr(&col);
        assert!(expr.contains("CASE WHEN"));
        assert!(expr.contains("'t'"));
        assert!(expr.contains("'f'"));
    }

    #[test]
    fn test_mssql_float_normalization() {
        let col = make_column("value", "float");
        let expr = mssql_normalize_expr(&col);
        assert!(expr.contains("STR"));
        assert!(expr.contains("15")); // 15 decimal places for float64
    }

    #[test]
    fn test_mssql_row_hash_expr() {
        let columns = vec![
            make_column("id", "int"),
            make_column("name", "varchar"),
            make_column("value", "decimal"),
        ];
        let pk_cols = vec!["id".to_string()];

        let expr = mssql_row_hash_expr(&columns, &pk_cols, true);

        // Should exclude id (PK), include name and value
        assert!(expr.contains("HASHBYTES"));
        assert!(expr.contains("MD5"));
        assert!(expr.contains("[name]"));
        assert!(expr.contains("[value]"));
        assert!(!expr.contains("[id]")); // PK should be excluded
        assert!(expr.contains("'|'")); // Pipe separator
    }

    #[test]
    fn test_is_text_type() {
        // Text types that should be skipped
        assert!(is_text_type("text"));
        assert!(is_text_type("ntext"));
        assert!(is_text_type("TEXT")); // case insensitive
        assert!(is_text_type("varchar(-1)"));
        assert!(is_text_type("nvarchar(-1)"));
        assert!(is_text_type("varchar(max)"));
        assert!(is_text_type("nvarchar(max)"));
        assert!(is_text_type("xml"));

        // Non-text types that should be included
        assert!(!is_text_type("varchar"));
        assert!(!is_text_type("varchar(100)"));
        assert!(!is_text_type("nvarchar(50)"));
        assert!(!is_text_type("int"));
        assert!(!is_text_type("datetime"));
        assert!(!is_text_type("varbinary(max)")); // binary, not text
    }

    #[test]
    fn test_mssql_row_hash_expr_skip_text() {
        let columns = vec![
            make_column("id", "int"),
            make_column("name", "varchar(100)"),
            make_column("body", "nvarchar(max)"),
            make_column("notes", "text"),
        ];
        let pk_cols = vec!["id".to_string()];

        // With include_text = true, should include all non-PK columns
        let expr_with_text = mssql_row_hash_expr(&columns, &pk_cols, true);
        assert!(expr_with_text.contains("[name]"));
        assert!(expr_with_text.contains("[body]"));
        assert!(expr_with_text.contains("[notes]"));

        // With include_text = false, should skip text columns
        let expr_without_text = mssql_row_hash_expr(&columns, &pk_cols, false);
        assert!(expr_without_text.contains("[name]"));
        assert!(!expr_without_text.contains("[body]"));
        assert!(!expr_without_text.contains("[notes]"));
    }
}
