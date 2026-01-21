//! Type mapping implementations with (source, target) pair keying.
//!
//! This module provides TypeMapper implementations for database type conversions.
//! Each mapper handles a specific source→target dialect pair, solving the N²
//! complexity problem by only implementing actually-used combinations.

use crate::core::schema::Column;
use crate::core::traits::{ColumnMapping, TypeMapper, TypeMapping};

/// MSSQL → PostgreSQL type mapper.
///
/// Handles type conversions when migrating from Microsoft SQL Server to PostgreSQL.
/// All mappings are lossless (PostgreSQL can represent all MSSQL types).
#[derive(Debug, Clone, Default)]
pub struct MssqlToPostgresMapper;

impl MssqlToPostgresMapper {
    /// Create a new MSSQL to PostgreSQL mapper.
    pub fn new() -> Self {
        Self
    }
}

impl TypeMapper for MssqlToPostgresMapper {
    fn source_dialect(&self) -> &str {
        "mssql"
    }

    fn target_dialect(&self) -> &str {
        "postgres"
    }

    fn map_column(&self, col: &Column) -> ColumnMapping {
        let type_mapping = self.map_type(&col.data_type, col.max_length, col.precision, col.scale);

        ColumnMapping {
            name: col.name.clone(),
            target_type: type_mapping.target_type,
            is_nullable: col.is_nullable,
            warning: type_mapping.warning,
        }
    }

    fn map_type(
        &self,
        data_type: &str,
        max_length: i32,
        precision: i32,
        scale: i32,
    ) -> TypeMapping {
        let target = mssql_to_postgres(data_type, max_length, precision, scale);
        TypeMapping::lossless(target)
    }
}

/// PostgreSQL → MSSQL type mapper.
///
/// Handles type conversions when migrating from PostgreSQL to Microsoft SQL Server.
/// Some mappings are lossy (MSSQL cannot fully represent all PostgreSQL types).
#[derive(Debug, Clone, Default)]
pub struct PostgresToMssqlMapper;

impl PostgresToMssqlMapper {
    /// Create a new PostgreSQL to MSSQL mapper.
    pub fn new() -> Self {
        Self
    }
}

impl TypeMapper for PostgresToMssqlMapper {
    fn source_dialect(&self) -> &str {
        "postgres"
    }

    fn target_dialect(&self) -> &str {
        "mssql"
    }

    fn map_column(&self, col: &Column) -> ColumnMapping {
        let type_mapping = self.map_type(&col.data_type, col.max_length, col.precision, col.scale);

        ColumnMapping {
            name: col.name.clone(),
            target_type: type_mapping.target_type,
            is_nullable: col.is_nullable,
            warning: type_mapping.warning,
        }
    }

    fn map_type(
        &self,
        data_type: &str,
        max_length: i32,
        precision: i32,
        scale: i32,
    ) -> TypeMapping {
        postgres_to_mssql(data_type, max_length, precision, scale)
    }
}

/// Identity type mapper for same-dialect transfers.
///
/// Used when source and target are the same database type.
/// Preserves types unchanged.
#[derive(Debug, Clone)]
pub struct IdentityMapper {
    dialect: String,
}

impl IdentityMapper {
    /// Create a new identity mapper for the given dialect.
    pub fn new(dialect: impl Into<String>) -> Self {
        Self {
            dialect: dialect.into(),
        }
    }
}

impl TypeMapper for IdentityMapper {
    fn source_dialect(&self) -> &str {
        &self.dialect
    }

    fn target_dialect(&self) -> &str {
        &self.dialect
    }

    fn map_column(&self, col: &Column) -> ColumnMapping {
        ColumnMapping {
            name: col.name.clone(),
            target_type: col.data_type.clone(),
            is_nullable: col.is_nullable,
            warning: None,
        }
    }

    fn map_type(
        &self,
        data_type: &str,
        _max_length: i32,
        _precision: i32,
        _scale: i32,
    ) -> TypeMapping {
        // Identity mapping - preserve type as-is
        TypeMapping::lossless(data_type.to_string())
    }
}

/// Map an MSSQL data type to PostgreSQL.
///
/// All MSSQL→PostgreSQL mappings are lossless.
fn mssql_to_postgres(mssql_type: &str, max_length: i32, precision: i32, scale: i32) -> String {
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
///
/// Some mappings are lossy - the warning field indicates lost functionality.
fn postgres_to_mssql(pg_type: &str, max_length: i32, precision: i32, scale: i32) -> TypeMapping {
    let pg_lower = pg_type.to_lowercase();

    // Handle array types (lossy - stored as JSON)
    if pg_lower.ends_with("[]") || pg_lower.starts_with('_') {
        return TypeMapping::lossy(
            "nvarchar(max)".to_string(),
            format!(
                "Array type '{}' stored as JSON string. Array operations unavailable.",
                pg_type
            ),
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
        "date" => TypeMapping::lossy(
            "datetime2",
            "Date stored as datetime2 with midnight time component.",
        ),
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
            format!(
                "Network type '{}' stored as string. Network operations unavailable.",
                pg_type
            ),
        ),
        "macaddr" | "macaddr8" => TypeMapping::lossy("char(17)", "MAC address stored as string."),

        // Geometric types (lossy)
        "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" => TypeMapping::lossy(
            "nvarchar(max)",
            format!(
                "Geometric type '{}' stored as string. Geometric operations unavailable.",
                pg_type
            ),
        ),

        // Full-text search types (lossy)
        "tsvector" | "tsquery" => TypeMapping::lossy(
            "nvarchar(max)",
            format!(
                "Full-text type '{}' stored as string. Use SQL Server full-text search instead.",
                pg_type
            ),
        ),

        // Range types (lossy)
        "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange" => {
            TypeMapping::lossy(
                "nvarchar(100)",
                format!(
                    "Range type '{}' stored as string. Range operations unavailable.",
                    pg_type
                ),
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

    fn test_column(
        name: &str,
        data_type: &str,
        max_length: i32,
        precision: i32,
        scale: i32,
    ) -> Column {
        Column {
            name: name.to_string(),
            data_type: data_type.to_string(),
            max_length,
            precision,
            scale,
            is_nullable: true,
            is_identity: false,
            ordinal_pos: 1,
        }
    }

    #[test]
    fn test_mssql_to_postgres_mapper() {
        let mapper = MssqlToPostgresMapper::new();
        assert_eq!(mapper.source_dialect(), "mssql");
        assert_eq!(mapper.target_dialect(), "postgres");

        let col = test_column("id", "int", 0, 0, 0);
        let mapping = mapper.map_column(&col);
        assert_eq!(mapping.target_type, "integer");
        assert!(mapping.warning.is_none());
    }

    #[test]
    fn test_postgres_to_mssql_mapper_lossless() {
        let mapper = PostgresToMssqlMapper::new();
        assert_eq!(mapper.source_dialect(), "postgres");
        assert_eq!(mapper.target_dialect(), "mssql");

        let col = test_column("id", "integer", 0, 0, 0);
        let mapping = mapper.map_column(&col);
        assert_eq!(mapping.target_type, "int");
        assert!(mapping.warning.is_none());
    }

    #[test]
    fn test_postgres_to_mssql_mapper_lossy() {
        let mapper = PostgresToMssqlMapper::new();

        let col = test_column("data", "jsonb", 0, 0, 0);
        let mapping = mapper.map_column(&col);
        assert_eq!(mapping.target_type, "nvarchar(max)");
        assert!(mapping.warning.is_some());
        assert!(mapping.warning.unwrap().contains("JSONB"));
    }

    #[test]
    fn test_identity_mapper() {
        let mapper = IdentityMapper::new("postgres");
        assert_eq!(mapper.source_dialect(), "postgres");
        assert_eq!(mapper.target_dialect(), "postgres");

        let col = test_column("data", "jsonb", 0, 0, 0);
        let mapping = mapper.map_column(&col);
        assert_eq!(mapping.target_type, "jsonb");
        assert!(mapping.warning.is_none());
    }

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
    fn test_postgres_to_mssql_arrays() {
        let mapping = postgres_to_mssql("integer[]", 0, 0, 0);
        assert_eq!(mapping.target_type, "nvarchar(max)");
        assert!(mapping.is_lossy);
        assert!(mapping.warning.as_ref().unwrap().contains("Array"));
    }
}
