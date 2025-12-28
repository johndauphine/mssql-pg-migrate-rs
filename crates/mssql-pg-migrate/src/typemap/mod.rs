//! Bidirectional type mapping between MSSQL and PostgreSQL.

use crate::config::DatabaseType;

/// Result of a type mapping operation.
#[derive(Debug, Clone)]
pub struct TypeMapping {
    /// The target database type.
    pub target_type: String,
    /// Whether the mapping is lossy (may lose data or functionality).
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

/// Map a type from source database to target database.
pub fn map_type(
    source_type: DatabaseType,
    target_type: DatabaseType,
    data_type: &str,
    max_length: i32,
    precision: i32,
    scale: i32,
) -> TypeMapping {
    match (source_type, target_type) {
        (DatabaseType::Mssql, DatabaseType::Postgres) => {
            TypeMapping::lossless(mssql_to_postgres(data_type, max_length, precision, scale))
        }
        (DatabaseType::Postgres, DatabaseType::Mssql) => {
            postgres_to_mssql(data_type, max_length, precision, scale)
        }
        (DatabaseType::Mssql, DatabaseType::Mssql) => {
            // Same database type - preserve original
            TypeMapping::lossless(data_type.to_string())
        }
        (DatabaseType::Postgres, DatabaseType::Postgres) => {
            // Same database type - preserve original
            TypeMapping::lossless(data_type.to_string())
        }
    }
}

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

/// Map a PostgreSQL data type to MSSQL.
/// Returns a TypeMapping with lossy flag set for types that may lose functionality.
pub fn postgres_to_mssql(pg_type: &str, max_length: i32, precision: i32, scale: i32) -> TypeMapping {
    let pg_lower = pg_type.to_lowercase();

    // Handle array types (lossy - stored as JSON)
    if pg_lower.ends_with("[]") || pg_lower.starts_with("_") {
        return TypeMapping::lossy(
            "nvarchar(max)".to_string(),
            format!("Array type '{}' stored as JSON string. Array operations unavailable.", pg_type),
        );
    }

    match pg_lower.as_str() {
        // Boolean
        "bool" | "boolean" => TypeMapping::lossless("bit"),

        // Integer types
        "int2" | "smallint" => TypeMapping::lossless("smallint"),
        "int4" | "integer" | "int" => TypeMapping::lossless("int"),
        "int8" | "bigint" => TypeMapping::lossless("bigint"),
        "serial" => TypeMapping::lossless("int"),
        "bigserial" => TypeMapping::lossless("bigint"),
        "smallserial" => TypeMapping::lossless("smallint"),

        // Decimal/numeric
        "numeric" | "decimal" => {
            if precision > 0 {
                TypeMapping::lossless(format!("decimal({},{})", precision, scale))
            } else {
                TypeMapping::lossless("decimal(38,10)")
            }
        }
        "money" => TypeMapping::lossless("money"),

        // Floating point
        "float4" | "real" => TypeMapping::lossless("real"),
        "float8" | "double precision" => TypeMapping::lossless("float"),

        // String types
        "char" | "character" | "bpchar" => {
            if max_length > 0 && max_length <= 4000 {
                TypeMapping::lossless(format!("nchar({})", max_length))
            } else {
                TypeMapping::lossless("nvarchar(max)")
            }
        }
        "varchar" | "character varying" => {
            if max_length > 0 && max_length <= 4000 {
                TypeMapping::lossless(format!("nvarchar({})", max_length))
            } else {
                TypeMapping::lossless("nvarchar(max)")
            }
        }
        "text" => TypeMapping::lossless("nvarchar(max)"),
        "name" => TypeMapping::lossless("nvarchar(128)"),

        // Binary types
        "bytea" => TypeMapping::lossless("varbinary(max)"),

        // Date/time types
        "date" => TypeMapping::lossless("date"),
        "time" | "time without time zone" => TypeMapping::lossless("time"),
        "timetz" | "time with time zone" => TypeMapping::lossy(
            "datetimeoffset",
            "time with time zone converted to datetimeoffset. Date portion set to 1900-01-01.",
        ),
        "timestamp" | "timestamp without time zone" => TypeMapping::lossless("datetime2"),
        "timestamptz" | "timestamp with time zone" => TypeMapping::lossless("datetimeoffset"),
        "interval" => TypeMapping::lossy(
            "nvarchar(100)",
            "PostgreSQL interval stored as string. Interval arithmetic unavailable.",
        ),

        // UUID
        "uuid" => TypeMapping::lossless("uniqueidentifier"),

        // JSON types (lossy - stored as string)
        "json" => TypeMapping::lossy(
            "nvarchar(max)",
            "JSON stored as string. JSON functions unavailable until SQL Server 2016+.",
        ),
        "jsonb" => TypeMapping::lossy(
            "nvarchar(max)",
            "JSONB stored as string. Binary JSON features and indexing unavailable.",
        ),

        // XML
        "xml" => TypeMapping::lossless("xml"),

        // Network types (lossy)
        "inet" | "cidr" => TypeMapping::lossy(
            "varchar(50)",
            format!("Network type '{}' stored as string. Network operations unavailable.", pg_type),
        ),
        "macaddr" | "macaddr8" => TypeMapping::lossy(
            "char(17)",
            "MAC address stored as string.",
        ),

        // Geometric types (lossy)
        "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" => {
            TypeMapping::lossy(
                "nvarchar(max)",
                format!("Geometric type '{}' stored as string. Geometric operations unavailable.", pg_type),
            )
        }

        // Full-text search types (lossy)
        "tsvector" | "tsquery" => TypeMapping::lossy(
            "nvarchar(max)",
            format!("Full-text type '{}' stored as string. Use SQL Server full-text search instead.", pg_type),
        ),

        // Range types (lossy)
        "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange" => {
            TypeMapping::lossy(
                "nvarchar(100)",
                format!("Range type '{}' stored as string. Range operations unavailable.", pg_type),
            )
        }

        // Bit strings
        "bit" => {
            if max_length == 1 {
                TypeMapping::lossless("bit")
            } else {
                TypeMapping::lossless(format!("binary({})", (max_length + 7) / 8))
            }
        }
        "varbit" | "bit varying" => TypeMapping::lossless("varbinary(max)"),

        // OID types
        "oid" => TypeMapping::lossless("bigint"),

        // Default fallback
        _ => TypeMapping::lossy(
            "nvarchar(max)",
            format!("Unknown PostgreSQL type '{}' stored as string.", pg_type),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mssql_to_postgres_integer_types() {
        assert_eq!(mssql_to_postgres("int", 0, 0, 0), "integer");
        assert_eq!(mssql_to_postgres("bigint", 0, 0, 0), "bigint");
        assert_eq!(mssql_to_postgres("smallint", 0, 0, 0), "smallint");
        assert_eq!(mssql_to_postgres("tinyint", 0, 0, 0), "smallint");
    }

    #[test]
    fn test_mssql_to_postgres_string_types() {
        assert_eq!(mssql_to_postgres("varchar", 100, 0, 0), "varchar(100)");
        assert_eq!(mssql_to_postgres("varchar", -1, 0, 0), "text");
        assert_eq!(mssql_to_postgres("nvarchar", 255, 0, 0), "varchar(255)");
        assert_eq!(mssql_to_postgres("text", 0, 0, 0), "text");
    }

    #[test]
    fn test_mssql_to_postgres_decimal_types() {
        assert_eq!(mssql_to_postgres("decimal", 0, 18, 2), "numeric(18,2)");
        assert_eq!(mssql_to_postgres("money", 0, 0, 0), "numeric(19,4)");
    }

    #[test]
    fn test_mssql_to_postgres_datetime_types() {
        assert_eq!(mssql_to_postgres("datetime", 0, 0, 0), "timestamp");
        assert_eq!(mssql_to_postgres("datetime2", 0, 0, 0), "timestamp");
        assert_eq!(mssql_to_postgres("datetimeoffset", 0, 0, 0), "timestamptz");
        assert_eq!(mssql_to_postgres("date", 0, 0, 0), "date");
    }

    #[test]
    fn test_mssql_to_postgres_special_types() {
        assert_eq!(mssql_to_postgres("uniqueidentifier", 0, 0, 0), "uuid");
        assert_eq!(mssql_to_postgres("bit", 0, 0, 0), "boolean");
        assert_eq!(mssql_to_postgres("varbinary", 0, 0, 0), "bytea");
    }

    #[test]
    fn test_postgres_to_mssql_lossless() {
        let mapping = postgres_to_mssql("integer", 0, 0, 0);
        assert_eq!(mapping.target_type, "int");
        assert!(!mapping.is_lossy);
        assert!(mapping.warning.is_none());

        let mapping = postgres_to_mssql("uuid", 0, 0, 0);
        assert_eq!(mapping.target_type, "uniqueidentifier");
        assert!(!mapping.is_lossy);

        let mapping = postgres_to_mssql("timestamp", 0, 0, 0);
        assert_eq!(mapping.target_type, "datetime2");
        assert!(!mapping.is_lossy);
    }

    #[test]
    fn test_postgres_to_mssql_lossy_json() {
        let mapping = postgres_to_mssql("json", 0, 0, 0);
        assert_eq!(mapping.target_type, "nvarchar(max)");
        assert!(mapping.is_lossy);
        assert!(mapping.warning.is_some());
        assert!(mapping.warning.unwrap().contains("JSON"));

        let mapping = postgres_to_mssql("jsonb", 0, 0, 0);
        assert_eq!(mapping.target_type, "nvarchar(max)");
        assert!(mapping.is_lossy);
    }

    #[test]
    fn test_postgres_to_mssql_lossy_arrays() {
        let mapping = postgres_to_mssql("integer[]", 0, 0, 0);
        assert_eq!(mapping.target_type, "nvarchar(max)");
        assert!(mapping.is_lossy);
        assert!(mapping.warning.unwrap().contains("Array"));

        let mapping = postgres_to_mssql("text[]", 0, 0, 0);
        assert!(mapping.is_lossy);
    }

    #[test]
    fn test_postgres_to_mssql_lossy_geometric() {
        let mapping = postgres_to_mssql("point", 0, 0, 0);
        assert_eq!(mapping.target_type, "nvarchar(max)");
        assert!(mapping.is_lossy);
        assert!(mapping.warning.unwrap().contains("Geometric"));
    }

    #[test]
    fn test_map_type_mssql_to_postgres() {
        let mapping = map_type(DatabaseType::Mssql, DatabaseType::Postgres, "int", 0, 0, 0);
        assert_eq!(mapping.target_type, "integer");
        assert!(!mapping.is_lossy);
    }

    #[test]
    fn test_map_type_postgres_to_mssql() {
        let mapping = map_type(DatabaseType::Postgres, DatabaseType::Mssql, "integer", 0, 0, 0);
        assert_eq!(mapping.target_type, "int");
        assert!(!mapping.is_lossy);
    }

    #[test]
    fn test_map_type_same_database() {
        let mapping = map_type(DatabaseType::Mssql, DatabaseType::Mssql, "nvarchar", 100, 0, 0);
        assert_eq!(mapping.target_type, "nvarchar");
        assert!(!mapping.is_lossy);

        let mapping = map_type(DatabaseType::Postgres, DatabaseType::Postgres, "jsonb", 0, 0, 0);
        assert_eq!(mapping.target_type, "jsonb");
        assert!(!mapping.is_lossy);
    }
}
