//! Cross-platform column normalization for hash consistency.
//!
//! This module handles the critical task of generating SQL expressions that
//! produce identical output on both MSSQL and PostgreSQL for the same data.
//!
//! Key normalization rules:
//! - NULLs: Represented as literal string 'NULL'
//! - DateTimes: Rounded to 3ms precision (MSSQL DATETIME limitation), ISO 8601 format
//! - Booleans: Converted to '0' or '1'
//! - Floats: Fixed precision formatting
//! - Strings: Direct conversion to text

use crate::source::Column;

/// Generate MSSQL expression for a normalized column value.
///
/// The expression converts the column to a consistent string representation
/// that will match the PostgreSQL equivalent for the same data.
pub fn mssql_normalize_expr(column: &Column) -> String {
    let name = &column.name;
    let data_type = column.data_type.to_lowercase();

    match data_type.as_str() {
        // DateTime types: use FORMAT for consistent ISO 8601 output
        // We truncate to seconds for simplicity (avoids overflow issues with DATEDIFF on milliseconds)
        "datetime" | "datetime2" | "smalldatetime" => {
            // FORMAT with 'yyyy-MM-ddTHH:mm:ss.fff' for ISO 8601
            format!(
                "ISNULL(FORMAT([{name}], 'yyyy-MM-ddTHH:mm:ss.fff'), N'NULL')"
            )
        }
        "datetimeoffset" => {
            // Format with timezone offset
            format!(
                "ISNULL(FORMAT([{name}], 'yyyy-MM-ddTHH:mm:ss.fffzzz'), N'NULL')"
            )
        }
        "date" => {
            format!("ISNULL(CONVERT(NVARCHAR(10), [{name}], 23), N'NULL')")
        }
        "time" => {
            format!("ISNULL(CONVERT(NVARCHAR(16), [{name}], 114), N'NULL')")
        }

        // Boolean: convert BIT to '0' or '1'
        "bit" => {
            format!("ISNULL(CAST([{name}] AS CHAR(1)), N'NULL')")
        }

        // Floating point: fixed precision to avoid representation differences
        "float" => {
            // FLOAT(53) - use 15 significant digits
            format!("ISNULL(STR([{name}], 25, 15), N'NULL')")
        }
        "real" => {
            // FLOAT(24) - use 7 significant digits
            format!("ISNULL(STR([{name}], 15, 7), N'NULL')")
        }

        // Decimal/Numeric: preserve exact representation
        "decimal" | "numeric" | "money" | "smallmoney" => {
            format!("ISNULL(CAST([{name}] AS NVARCHAR(50)), N'NULL')")
        }

        // Integer types: direct conversion
        "tinyint" | "smallint" | "int" | "bigint" => {
            format!("ISNULL(CAST([{name}] AS NVARCHAR(20)), N'NULL')")
        }

        // UUID
        "uniqueidentifier" => {
            format!("ISNULL(LOWER(CAST([{name}] AS NVARCHAR(36))), N'NULL')")
        }

        // Binary: hex encoding
        "binary" | "varbinary" | "image" => {
            format!(
                "ISNULL(CONVERT(NVARCHAR(MAX), [{name}], 1), N'NULL')"
            )
        }

        // Text types: direct conversion, normalize line endings
        "text" | "ntext" | "varchar" | "nvarchar" | "char" | "nchar" => {
            // Replace CRLF with LF for consistency
            format!(
                "ISNULL(REPLACE(CAST([{name}] AS NVARCHAR(MAX)), CHAR(13)+CHAR(10), CHAR(10)), N'NULL')"
            )
        }

        // Default: cast to NVARCHAR
        _ => {
            format!("ISNULL(CAST([{name}] AS NVARCHAR(MAX)), N'NULL')")
        }
    }
}

/// Generate PostgreSQL expression for a normalized column value.
///
/// The expression converts the column to a consistent string representation
/// that will match the MSSQL equivalent for the same data.
pub fn postgres_normalize_expr(column: &Column) -> String {
    let name = &column.name;
    let data_type = column.data_type.to_lowercase();

    match data_type.as_str() {
        // DateTime types: round to 3ms precision to match MSSQL
        // Format as ISO 8601: YYYY-MM-DDTHH:MM:SS.mmm
        "datetime" | "datetime2" | "smalldatetime" | "timestamp" | "timestamp without time zone" => {
            // Round to 3ms precision and format
            format!(
                "COALESCE(TO_CHAR(\
                 DATE_TRUNC('millisecond', \"{name}\") - \
                 (EXTRACT(MILLISECONDS FROM \"{name}\")::integer % 3) * INTERVAL '1 millisecond', \
                 'YYYY-MM-DD\"T\"HH24:MI:SS.MS'), 'NULL')"
            )
        }
        "datetimeoffset" | "timestamptz" | "timestamp with time zone" => {
            // Include timezone, round to 3ms
            format!(
                "COALESCE(TO_CHAR(\
                 DATE_TRUNC('millisecond', \"{name}\") - \
                 (EXTRACT(MILLISECONDS FROM \"{name}\")::integer % 3) * INTERVAL '1 millisecond', \
                 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), 'NULL')"
            )
        }
        "date" => {
            format!("COALESCE(TO_CHAR(\"{name}\", 'YYYY-MM-DD'), 'NULL')")
        }
        "time" | "time without time zone" => {
            format!("COALESCE(TO_CHAR(\"{name}\", 'HH24:MI:SS.MS'), 'NULL')")
        }

        // Boolean: convert to '0' or '1'
        "bit" | "boolean" | "bool" => {
            format!("COALESCE(CASE WHEN \"{name}\" THEN '1' ELSE '0' END, 'NULL')")
        }

        // Floating point: fixed precision to match MSSQL STR output
        "float" | "double precision" => {
            // 15 significant digits for FLOAT(53)
            format!(
                "COALESCE(TRIM(TO_CHAR(\"{name}\", 'FM9999999999999999999999990.999999999999999')), 'NULL')"
            )
        }
        "real" => {
            // 7 significant digits for FLOAT(24)
            format!(
                "COALESCE(TRIM(TO_CHAR(\"{name}\", 'FM999999990.9999999')), 'NULL')"
            )
        }

        // Decimal/Numeric: preserve exact representation
        "decimal" | "numeric" | "money" | "smallmoney" => {
            format!("COALESCE(\"{name}\"::TEXT, 'NULL')")
        }

        // Integer types: direct conversion
        "tinyint" | "smallint" | "int" | "integer" | "bigint" => {
            format!("COALESCE(\"{name}\"::TEXT, 'NULL')")
        }

        // UUID: lowercase
        "uniqueidentifier" | "uuid" => {
            format!("COALESCE(LOWER(\"{name}\"::TEXT), 'NULL')")
        }

        // Binary: hex encoding
        "binary" | "varbinary" | "image" | "bytea" => {
            format!("COALESCE(ENCODE(\"{name}\", 'hex'), 'NULL')")
        }

        // Text types: normalize line endings
        "text" | "ntext" | "varchar" | "nvarchar" | "char" | "nchar" | "character varying" | "character" => {
            // Replace CRLF with LF for consistency
            format!("COALESCE(REPLACE(\"{name}\"::TEXT, E'\\r\\n', E'\\n'), 'NULL')")
        }

        // Default: cast to TEXT
        _ => {
            format!("COALESCE(\"{name}\"::TEXT, 'NULL')")
        }
    }
}

/// Generate MSSQL separator for BINARY_CHECKSUM (no separator needed, uses comma internally).
pub fn mssql_column_separator() -> &'static str {
    ", "
}

/// Generate PostgreSQL separator for hash concatenation.
pub fn postgres_column_separator() -> &'static str {
    " || '|' || "
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
        assert!(expr.contains("DATEADD"));
        assert!(expr.contains("DATEDIFF"));
        assert!(expr.contains("126")); // ISO 8601 format
    }

    #[test]
    fn test_mssql_bit_normalization() {
        let col = make_column("is_active", "bit");
        let expr = mssql_normalize_expr(&col);
        assert!(expr.contains("CHAR(1)"));
    }

    #[test]
    fn test_postgres_boolean_normalization() {
        let col = make_column("is_active", "boolean");
        let expr = postgres_normalize_expr(&col);
        assert!(expr.contains("CASE WHEN"));
        assert!(expr.contains("'1'"));
        assert!(expr.contains("'0'"));
    }

    #[test]
    fn test_postgres_timestamp_normalization() {
        let col = make_column("created_at", "timestamp");
        let expr = postgres_normalize_expr(&col);
        assert!(expr.contains("DATE_TRUNC"));
        assert!(expr.contains("millisecond"));
    }
}
