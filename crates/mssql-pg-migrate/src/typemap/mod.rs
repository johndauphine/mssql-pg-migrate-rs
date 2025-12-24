//! Type mapping between MSSQL and PostgreSQL.

/// Map an MSSQL data type to PostgreSQL.
pub fn mssql_to_postgres(mssql_type: &str, max_length: i32, precision: i32, scale: i32) -> String {
    match mssql_type.to_lowercase().as_str() {
        // Boolean
        "bit" => "boolean".to_string(),

        // Integer types
        "tinyint" => "smallint".to_string(),
        "smallint" => "smallint".to_string(),
        "int" => "integer".to_string(),
        "bigint" => "bigint".to_string(),

        // Decimal/numeric
        "decimal" | "numeric" => {
            if precision > 0 {
                format!("numeric({},{})", precision, scale)
            } else {
                "numeric".to_string()
            }
        }
        "money" => "numeric(19,4)".to_string(),
        "smallmoney" => "numeric(10,4)".to_string(),

        // Floating point
        "float" => "double precision".to_string(),
        "real" => "real".to_string(),

        // String types
        "char" | "nchar" => {
            if max_length > 0 && max_length <= 10485760 {
                format!("char({})", max_length)
            } else {
                "text".to_string()
            }
        }
        "varchar" | "nvarchar" => {
            if max_length == -1 {
                "text".to_string()
            } else if max_length > 0 && max_length <= 10485760 {
                format!("varchar({})", max_length)
            } else {
                "text".to_string()
            }
        }
        "text" | "ntext" => "text".to_string(),

        // Binary types
        "binary" | "varbinary" | "image" => "bytea".to_string(),

        // Date/time types
        "date" => "date".to_string(),
        "time" => "time".to_string(),
        "datetime" | "datetime2" | "smalldatetime" => "timestamp".to_string(),
        "datetimeoffset" => "timestamptz".to_string(),

        // GUID
        "uniqueidentifier" => "uuid".to_string(),

        // XML
        "xml" => "xml".to_string(),

        // Spatial types (convert to text)
        "geometry" | "geography" => "text".to_string(),

        // Default fallback
        _ => "text".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_types() {
        assert_eq!(mssql_to_postgres("int", 0, 0, 0), "integer");
        assert_eq!(mssql_to_postgres("bigint", 0, 0, 0), "bigint");
        assert_eq!(mssql_to_postgres("smallint", 0, 0, 0), "smallint");
        assert_eq!(mssql_to_postgres("tinyint", 0, 0, 0), "smallint");
    }

    #[test]
    fn test_string_types() {
        assert_eq!(mssql_to_postgres("varchar", 100, 0, 0), "varchar(100)");
        assert_eq!(mssql_to_postgres("varchar", -1, 0, 0), "text");
        assert_eq!(mssql_to_postgres("nvarchar", 255, 0, 0), "varchar(255)");
        assert_eq!(mssql_to_postgres("text", 0, 0, 0), "text");
    }

    #[test]
    fn test_decimal_types() {
        assert_eq!(mssql_to_postgres("decimal", 0, 18, 2), "numeric(18,2)");
        assert_eq!(mssql_to_postgres("money", 0, 0, 0), "numeric(19,4)");
    }

    #[test]
    fn test_datetime_types() {
        assert_eq!(mssql_to_postgres("datetime", 0, 0, 0), "timestamp");
        assert_eq!(mssql_to_postgres("datetime2", 0, 0, 0), "timestamp");
        assert_eq!(mssql_to_postgres("datetimeoffset", 0, 0, 0), "timestamptz");
        assert_eq!(mssql_to_postgres("date", 0, 0, 0), "date");
    }

    #[test]
    fn test_special_types() {
        assert_eq!(mssql_to_postgres("uniqueidentifier", 0, 0, 0), "uuid");
        assert_eq!(mssql_to_postgres("bit", 0, 0, 0), "boolean");
        assert_eq!(mssql_to_postgres("varbinary", 0, 0, 0), "bytea");
    }
}
