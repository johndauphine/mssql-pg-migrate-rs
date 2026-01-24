//! Type mapping implementations with (source, target) pair keying.
//!
//! This module provides TypeMapper implementations for database type conversions.
//! Each mapper handles a specific source→target dialect pair, solving the N²
//! complexity problem by only implementing actually-used combinations.
//!
//! ## Canonical Type System
//!
//! This module also provides the hub-and-spoke canonical type system via
//! `ToCanonical` and `FromCanonical` trait implementations for each dialect.
//! See [`super::canonical`] for details.

use crate::core::schema::Column;
use crate::core::traits::{ColumnMapping, TypeMapper, TypeMapping};

use super::canonical::{CanonicalType, CanonicalTypeInfo, FromCanonical, ToCanonical};

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

/// MySQL → PostgreSQL type mapper.
///
/// Handles type conversions when migrating from MySQL/MariaDB to PostgreSQL.
/// Most mappings are lossless as PostgreSQL can represent all MySQL types.
#[derive(Debug, Clone, Default)]
pub struct MysqlToPostgresMapper;

impl MysqlToPostgresMapper {
    /// Create a new MySQL to PostgreSQL mapper.
    pub fn new() -> Self {
        Self
    }
}

impl TypeMapper for MysqlToPostgresMapper {
    fn source_dialect(&self) -> &str {
        "mysql"
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
        mysql_to_postgres(data_type, max_length, precision, scale)
    }
}

/// PostgreSQL → MySQL type mapper.
///
/// Handles type conversions when migrating from PostgreSQL to MySQL/MariaDB.
/// Some mappings are lossy as MySQL cannot fully represent all PostgreSQL types.
#[derive(Debug, Clone, Default)]
pub struct PostgresToMysqlMapper;

impl PostgresToMysqlMapper {
    /// Create a new PostgreSQL to MySQL mapper.
    pub fn new() -> Self {
        Self
    }
}

impl TypeMapper for PostgresToMysqlMapper {
    fn source_dialect(&self) -> &str {
        "postgres"
    }

    fn target_dialect(&self) -> &str {
        "mysql"
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
        postgres_to_mysql(data_type, max_length, precision, scale)
    }
}

/// MySQL → MSSQL type mapper.
///
/// Handles type conversions when migrating from MySQL/MariaDB to Microsoft SQL Server.
#[derive(Debug, Clone, Default)]
pub struct MysqlToMssqlMapper;

impl MysqlToMssqlMapper {
    /// Create a new MySQL to MSSQL mapper.
    pub fn new() -> Self {
        Self
    }
}

impl TypeMapper for MysqlToMssqlMapper {
    fn source_dialect(&self) -> &str {
        "mysql"
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
        mysql_to_mssql(data_type, max_length, precision, scale)
    }
}

/// MSSQL → MySQL type mapper.
///
/// Handles type conversions when migrating from Microsoft SQL Server to MySQL/MariaDB.
#[derive(Debug, Clone, Default)]
pub struct MssqlToMysqlMapper;

impl MssqlToMysqlMapper {
    /// Create a new MSSQL to MySQL mapper.
    pub fn new() -> Self {
        Self
    }
}

impl TypeMapper for MssqlToMysqlMapper {
    fn source_dialect(&self) -> &str {
        "mssql"
    }

    fn target_dialect(&self) -> &str {
        "mysql"
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
        mssql_to_mysql(data_type, max_length, precision, scale)
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

/// Map a MySQL data type to PostgreSQL.
///
/// Most mappings are lossless - PostgreSQL can represent nearly all MySQL types.
fn mysql_to_postgres(mysql_type: &str, max_length: i32, precision: i32, scale: i32) -> TypeMapping {
    let mysql_lower = mysql_type.to_lowercase();

    match mysql_lower.as_str() {
        // Boolean (MySQL uses TINYINT(1) for bool)
        "tinyint" if max_length == 1 => TypeMapping::lossless("boolean"),
        "bool" | "boolean" => TypeMapping::lossless("boolean"),

        // Integer types
        "tinyint" => TypeMapping::lossless("smallint"),
        "smallint" => TypeMapping::lossless("smallint"),
        "mediumint" => TypeMapping::lossless("integer"),
        "int" | "integer" => TypeMapping::lossless("integer"),
        "bigint" => TypeMapping::lossless("bigint"),

        // Decimal/numeric
        "decimal" | "numeric" | "dec" | "fixed" => {
            if precision > 0 {
                TypeMapping::lossless(format!("numeric({},{})", precision, scale))
            } else {
                TypeMapping::lossless("numeric")
            }
        }

        // Floating point
        "float" => TypeMapping::lossless("real"),
        "double" | "double precision" | "real" => TypeMapping::lossless("double precision"),

        // String types
        "char" => {
            if max_length > 0 && max_length <= 10485760 {
                TypeMapping::lossless(format!("char({})", max_length))
            } else {
                TypeMapping::lossless("text")
            }
        }
        "varchar" => {
            if max_length > 0 && max_length <= 10485760 {
                TypeMapping::lossless(format!("varchar({})", max_length))
            } else {
                TypeMapping::lossless("text")
            }
        }
        "tinytext" | "text" | "mediumtext" | "longtext" => TypeMapping::lossless("text"),

        // Binary types
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => {
            TypeMapping::lossless("bytea")
        }

        // Date/time types
        "date" => TypeMapping::lossless("date"),
        "time" => TypeMapping::lossless("time"),
        "datetime" | "timestamp" => TypeMapping::lossless("timestamp"),
        "year" => TypeMapping::lossless("smallint"),

        // JSON
        "json" => TypeMapping::lossless("jsonb"),

        // Enum (lossy - stored as text)
        "enum" => TypeMapping::lossy(
            "text",
            "MySQL ENUM stored as text. Consider creating a CHECK constraint or PostgreSQL ENUM.",
        ),

        // Set (lossy - stored as text)
        "set" => TypeMapping::lossy(
            "text",
            "MySQL SET stored as text. Consider using an array type or separate table.",
        ),

        // Bit
        "bit" => {
            if max_length == 1 {
                TypeMapping::lossless("boolean")
            } else if max_length <= 64 {
                TypeMapping::lossless("bit varying")
            } else {
                TypeMapping::lossless("bytea")
            }
        }

        // Spatial types (lossy)
        "geometry" | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring"
        | "multipolygon" | "geometrycollection" => TypeMapping::lossy(
            "text",
            format!(
                "MySQL spatial type '{}' stored as text. Consider PostGIS for spatial operations.",
                mysql_type
            ),
        ),

        // Default fallback
        _ => TypeMapping::lossy(
            "text",
            format!("Unknown MySQL type '{}' stored as text.", mysql_type),
        ),
    }
}

/// Map a PostgreSQL data type to MySQL.
///
/// Some mappings are lossy - MySQL cannot fully represent all PostgreSQL types.
fn postgres_to_mysql(pg_type: &str, max_length: i32, precision: i32, scale: i32) -> TypeMapping {
    let pg_lower = pg_type.to_lowercase();

    // Handle array types (lossy - stored as JSON)
    if pg_lower.ends_with("[]") || pg_lower.starts_with('_') {
        return TypeMapping::lossy(
            "JSON".to_string(),
            format!(
                "Array type '{}' stored as JSON. Array operations unavailable.",
                pg_type
            ),
        );
    }

    match pg_lower.as_str() {
        // Boolean
        "bool" | "boolean" => TypeMapping::lossless("TINYINT(1)"),

        // Integer types
        "int2" | "smallint" => TypeMapping::lossless("SMALLINT"),
        "int4" | "integer" | "int" => TypeMapping::lossless("INT"),
        "int8" | "bigint" => TypeMapping::lossless("BIGINT"),
        "serial" => TypeMapping::lossless("INT AUTO_INCREMENT"),
        "bigserial" => TypeMapping::lossless("BIGINT AUTO_INCREMENT"),
        "smallserial" => TypeMapping::lossless("SMALLINT AUTO_INCREMENT"),

        // Decimal/numeric
        "numeric" | "decimal" => {
            if precision > 0 && precision <= 65 {
                TypeMapping::lossless(format!("DECIMAL({},{})", precision, scale))
            } else if precision > 65 {
                TypeMapping::lossy(
                    format!("DECIMAL(65,{})", scale.min(30)),
                    format!(
                        "Precision {} exceeds MySQL max of 65. Truncated.",
                        precision
                    ),
                )
            } else {
                TypeMapping::lossless("DECIMAL(10,0)")
            }
        }
        "money" => TypeMapping::lossless("DECIMAL(19,4)"),

        // Floating point
        "float4" | "real" => TypeMapping::lossless("FLOAT"),
        "float8" | "double precision" => TypeMapping::lossless("DOUBLE"),

        // String types
        "char" | "character" | "bpchar" => {
            if max_length > 0 && max_length <= 255 {
                TypeMapping::lossless(format!("CHAR({})", max_length))
            } else {
                TypeMapping::lossless("TEXT")
            }
        }
        "varchar" | "character varying" => {
            if max_length > 0 && max_length <= 65535 {
                TypeMapping::lossless(format!("VARCHAR({})", max_length))
            } else {
                TypeMapping::lossless("LONGTEXT")
            }
        }
        "text" => TypeMapping::lossless("LONGTEXT"),
        "name" => TypeMapping::lossless("VARCHAR(63)"),

        // Binary types
        "bytea" => TypeMapping::lossless("LONGBLOB"),

        // Date/time types
        "date" => TypeMapping::lossless("DATE"),
        "time" | "time without time zone" => TypeMapping::lossless("TIME"),
        "timetz" | "time with time zone" => {
            TypeMapping::lossy("TIME", "time with time zone loses timezone info in MySQL.")
        }
        "timestamp" | "timestamp without time zone" => TypeMapping::lossless("DATETIME"),
        "timestamptz" | "timestamp with time zone" => TypeMapping::lossy(
            "DATETIME",
            "timestamp with time zone loses timezone info in MySQL. Consider storing as UTC.",
        ),
        "interval" => TypeMapping::lossy(
            "VARCHAR(100)",
            "PostgreSQL interval stored as string. Interval arithmetic unavailable.",
        ),

        // UUID (stored as VARCHAR(36) in MySQL)
        "uuid" => TypeMapping::lossy(
            "VARCHAR(36)",
            "UUID stored as VARCHAR(36). UUID functions unavailable.",
        ),

        // JSON types
        "json" | "jsonb" => TypeMapping::lossless("JSON"),

        // XML
        "xml" => TypeMapping::lossy("LONGTEXT", "XML stored as text. XML functions unavailable."),

        // Network types (lossy)
        "inet" | "cidr" => TypeMapping::lossy(
            "VARCHAR(50)",
            format!(
                "Network type '{}' stored as string. Network operations unavailable.",
                pg_type
            ),
        ),
        "macaddr" | "macaddr8" => TypeMapping::lossy("CHAR(17)", "MAC address stored as string."),

        // Geometric types (lossy)
        "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" => TypeMapping::lossy(
            "TEXT",
            format!(
                "Geometric type '{}' stored as string. Consider MySQL spatial types.",
                pg_type
            ),
        ),

        // Full-text search types (lossy)
        "tsvector" | "tsquery" => TypeMapping::lossy(
            "LONGTEXT",
            format!(
                "Full-text type '{}' stored as string. Use MySQL FULLTEXT instead.",
                pg_type
            ),
        ),

        // Range types (lossy)
        "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange" => {
            TypeMapping::lossy(
                "VARCHAR(100)",
                format!(
                    "Range type '{}' stored as string. Range operations unavailable.",
                    pg_type
                ),
            )
        }

        // Bit strings
        "bit" => {
            if max_length == 1 {
                TypeMapping::lossless("BIT(1)")
            } else if max_length <= 64 {
                TypeMapping::lossless(format!("BIT({})", max_length))
            } else {
                TypeMapping::lossless("BLOB")
            }
        }
        "varbit" | "bit varying" => TypeMapping::lossless("BLOB"),

        // OID types
        "oid" => TypeMapping::lossless("BIGINT UNSIGNED"),

        // Default fallback
        _ => TypeMapping::lossy(
            "LONGTEXT",
            format!("Unknown PostgreSQL type '{}' stored as text.", pg_type),
        ),
    }
}

/// Map PostgreSQL type to MySQL type (returns just the target type string).
///
/// Convenience function that returns just the target type without the full TypeMapping.
pub fn postgres_to_mysql_basic(
    pg_type: &str,
    max_length: i32,
    precision: i32,
    scale: i32,
) -> String {
    postgres_to_mysql(pg_type, max_length, precision, scale).target_type
}

/// Map a MySQL data type to MSSQL.
fn mysql_to_mssql(mysql_type: &str, max_length: i32, precision: i32, scale: i32) -> TypeMapping {
    let mysql_lower = mysql_type.to_lowercase();

    match mysql_lower.as_str() {
        // Boolean (MySQL uses TINYINT(1) for bool)
        "tinyint" if max_length == 1 => TypeMapping::lossless("bit"),
        "bool" | "boolean" => TypeMapping::lossless("bit"),

        // Integer types
        "tinyint" => TypeMapping::lossless("tinyint"),
        "smallint" => TypeMapping::lossless("smallint"),
        "mediumint" => TypeMapping::lossless("int"),
        "int" | "integer" => TypeMapping::lossless("int"),
        "bigint" => TypeMapping::lossless("bigint"),

        // Decimal/numeric
        "decimal" | "numeric" | "dec" | "fixed" => {
            if precision > 0 && precision <= 38 {
                TypeMapping::lossless(format!("decimal({},{})", precision, scale))
            } else if precision > 38 {
                TypeMapping::lossy(
                    format!("decimal(38,{})", scale.min(38)),
                    format!(
                        "Precision {} exceeds MSSQL max of 38. Truncated.",
                        precision
                    ),
                )
            } else {
                TypeMapping::lossless("decimal(18,0)")
            }
        }

        // Floating point
        "float" => TypeMapping::lossless("real"),
        "double" | "double precision" | "real" => TypeMapping::lossless("float"),

        // String types
        "char" => {
            if max_length > 0 && max_length <= 4000 {
                TypeMapping::lossless(format!("nchar({})", max_length))
            } else {
                TypeMapping::lossless("nvarchar(max)")
            }
        }
        "varchar" => {
            if max_length > 0 && max_length <= 4000 {
                TypeMapping::lossless(format!("nvarchar({})", max_length))
            } else {
                TypeMapping::lossless("nvarchar(max)")
            }
        }
        "tinytext" | "text" | "mediumtext" | "longtext" => TypeMapping::lossless("nvarchar(max)"),

        // Binary types
        "binary" | "varbinary" => {
            if max_length > 0 && max_length <= 8000 {
                TypeMapping::lossless(format!("varbinary({})", max_length))
            } else {
                TypeMapping::lossless("varbinary(max)")
            }
        }
        "tinyblob" | "blob" | "mediumblob" | "longblob" => TypeMapping::lossless("varbinary(max)"),

        // Date/time types
        "date" => TypeMapping::lossless("date"),
        "time" => TypeMapping::lossless("time"),
        "datetime" | "timestamp" => TypeMapping::lossless("datetime2"),
        "year" => TypeMapping::lossless("smallint"),

        // JSON (stored as nvarchar)
        "json" => TypeMapping::lossy(
            "nvarchar(max)",
            "JSON stored as string. Use SQL Server 2016+ JSON functions.",
        ),

        // Enum (lossy - stored as text)
        "enum" => TypeMapping::lossy(
            "nvarchar(255)",
            "MySQL ENUM stored as string. Consider CHECK constraint.",
        ),

        // Set (lossy - stored as text)
        "set" => TypeMapping::lossy(
            "nvarchar(max)",
            "MySQL SET stored as string. Consider separate table.",
        ),

        // Bit
        "bit" => {
            if max_length == 1 {
                TypeMapping::lossless("bit")
            } else {
                TypeMapping::lossless(format!("binary({})", (max_length + 7) / 8))
            }
        }

        // Spatial types (lossy)
        "geometry" | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring"
        | "multipolygon" | "geometrycollection" => TypeMapping::lossy(
            "nvarchar(max)",
            format!(
                "MySQL spatial type '{}' stored as text. Consider MSSQL spatial types.",
                mysql_type
            ),
        ),

        // Default fallback
        _ => TypeMapping::lossy(
            "nvarchar(max)",
            format!("Unknown MySQL type '{}' stored as string.", mysql_type),
        ),
    }
}

/// Map an MSSQL data type to MySQL.
/// Map MSSQL type to MySQL type.
///
/// This is a public helper function for type mapping.
pub fn mssql_to_mysql(
    mssql_type: &str,
    max_length: i32,
    precision: i32,
    scale: i32,
) -> TypeMapping {
    let mssql_lower = mssql_type.to_lowercase();

    match mssql_lower.as_str() {
        // Boolean
        "bit" => TypeMapping::lossless("TINYINT(1)"),

        // Integer types
        "tinyint" => TypeMapping::lossless("TINYINT UNSIGNED"),
        "smallint" => TypeMapping::lossless("SMALLINT"),
        "int" => TypeMapping::lossless("INT"),
        "bigint" => TypeMapping::lossless("BIGINT"),

        // Decimal/numeric
        "decimal" | "numeric" => {
            if precision > 0 && precision <= 65 {
                TypeMapping::lossless(format!("DECIMAL({},{})", precision, scale))
            } else if precision > 65 {
                TypeMapping::lossy(
                    format!("DECIMAL(65,{})", scale.min(30)),
                    format!(
                        "Precision {} exceeds MySQL max of 65. Truncated.",
                        precision
                    ),
                )
            } else {
                TypeMapping::lossless("DECIMAL(10,0)")
            }
        }
        "money" => TypeMapping::lossless("DECIMAL(19,4)"),
        "smallmoney" => TypeMapping::lossless("DECIMAL(10,4)"),

        // Floating point
        "float" => TypeMapping::lossless("DOUBLE"),
        "real" => TypeMapping::lossless("FLOAT"),

        // String types
        "char" | "nchar" => {
            if max_length > 0 && max_length <= 255 {
                TypeMapping::lossless(format!("CHAR({})", max_length))
            } else {
                TypeMapping::lossless("TEXT")
            }
        }
        "varchar" | "nvarchar" => {
            if max_length == -1 {
                TypeMapping::lossless("LONGTEXT")
            } else if max_length > 0 && max_length <= 65535 {
                TypeMapping::lossless(format!("VARCHAR({})", max_length))
            } else {
                TypeMapping::lossless("LONGTEXT")
            }
        }
        "text" | "ntext" => TypeMapping::lossless("LONGTEXT"),

        // Binary types
        "binary" | "varbinary" | "image" => {
            if max_length == -1 {
                TypeMapping::lossless("LONGBLOB")
            } else if max_length > 0 && max_length <= 65535 {
                TypeMapping::lossless(format!("VARBINARY({})", max_length))
            } else {
                TypeMapping::lossless("LONGBLOB")
            }
        }

        // Date/time types
        "date" => TypeMapping::lossless("DATE"),
        "time" => TypeMapping::lossless("TIME"),
        "datetime" | "datetime2" | "smalldatetime" => TypeMapping::lossless("DATETIME"),
        "datetimeoffset" => TypeMapping::lossy(
            "DATETIME",
            "datetimeoffset loses timezone info in MySQL. Consider storing as UTC.",
        ),

        // GUID (stored as VARCHAR(36))
        "uniqueidentifier" => TypeMapping::lossy(
            "VARCHAR(36)",
            "uniqueidentifier stored as VARCHAR(36). UUID functions unavailable.",
        ),

        // XML
        "xml" => TypeMapping::lossy("LONGTEXT", "XML stored as text. XML functions unavailable."),

        // Spatial types
        "geometry" | "geography" => TypeMapping::lossy(
            "LONGTEXT",
            format!(
                "MSSQL spatial type '{}' stored as text. Consider MySQL spatial types.",
                mssql_type
            ),
        ),

        // Default fallback
        _ => TypeMapping::lossy(
            "LONGTEXT",
            format!("Unknown MSSQL type '{}' stored as text.", mssql_type),
        ),
    }
}

/// Map MSSQL type to MySQL type (returns just the target type string).
///
/// Convenience function that returns just the target type without the full TypeMapping.
pub fn mssql_to_mysql_basic(
    mssql_type: &str,
    max_length: i32,
    precision: i32,
    scale: i32,
) -> String {
    mssql_to_mysql(mssql_type, max_length, precision, scale).target_type
}

// =============================================================================
// Hub-and-Spoke Canonical Type Converters
// =============================================================================

/// MSSQL to canonical type converter.
#[derive(Debug, Clone, Default)]
pub struct MssqlToCanonical;

impl MssqlToCanonical {
    /// Create a new MSSQL to canonical converter.
    pub fn new() -> Self {
        Self
    }
}

impl ToCanonical for MssqlToCanonical {
    fn dialect_name(&self) -> &str {
        "mssql"
    }

    fn to_canonical(
        &self,
        data_type: &str,
        max_length: i32,
        precision: i32,
        scale: i32,
    ) -> CanonicalTypeInfo {
        let dt_lower = data_type.to_lowercase();

        match dt_lower.as_str() {
            // Boolean
            "bit" => CanonicalTypeInfo::lossless(CanonicalType::Boolean),

            // Integer types
            "tinyint" => CanonicalTypeInfo::lossless(CanonicalType::UInt8),
            "smallint" => CanonicalTypeInfo::lossless(CanonicalType::Int16),
            "int" => CanonicalTypeInfo::lossless(CanonicalType::Int32),
            "bigint" => CanonicalTypeInfo::lossless(CanonicalType::Int64),

            // Decimal/numeric
            "decimal" | "numeric" => {
                let p = if precision > 0 { precision as u16 } else { 18 };
                let s = scale as u16;
                CanonicalTypeInfo::lossless(CanonicalType::Decimal {
                    precision: p,
                    scale: s,
                })
            }
            "money" => CanonicalTypeInfo::lossless(CanonicalType::Money),
            "smallmoney" => CanonicalTypeInfo::lossless(CanonicalType::SmallMoney),

            // Floating point
            "float" => CanonicalTypeInfo::lossless(CanonicalType::Float64),
            "real" => CanonicalTypeInfo::lossless(CanonicalType::Float32),

            // String types
            "char" | "nchar" => {
                let len = if max_length > 0 { max_length as u32 } else { 1 };
                CanonicalTypeInfo::lossless(CanonicalType::Char(len))
            }
            "varchar" | "nvarchar" => {
                if max_length == -1 {
                    CanonicalTypeInfo::lossless(CanonicalType::Text)
                } else {
                    let len = if max_length > 0 {
                        max_length as u32
                    } else {
                        255
                    };
                    CanonicalTypeInfo::lossless(CanonicalType::Varchar(len))
                }
            }
            "text" | "ntext" => CanonicalTypeInfo::lossless(CanonicalType::Text),

            // Binary types
            "binary" => {
                let len = if max_length > 0 { max_length as u32 } else { 1 };
                CanonicalTypeInfo::lossless(CanonicalType::Binary(len))
            }
            "varbinary" => {
                if max_length == -1 {
                    CanonicalTypeInfo::lossless(CanonicalType::Blob)
                } else {
                    let len = if max_length > 0 {
                        max_length as u32
                    } else {
                        255
                    };
                    CanonicalTypeInfo::lossless(CanonicalType::Varbinary(len))
                }
            }
            "image" => CanonicalTypeInfo::lossless(CanonicalType::Blob),

            // Date/time types
            "date" => CanonicalTypeInfo::lossless(CanonicalType::Date),
            "time" => CanonicalTypeInfo::lossless(CanonicalType::Time),
            "datetime" | "datetime2" | "smalldatetime" => {
                CanonicalTypeInfo::lossless(CanonicalType::DateTime)
            }
            "datetimeoffset" => CanonicalTypeInfo::lossless(CanonicalType::DateTimeTz),

            // GUID
            "uniqueidentifier" => CanonicalTypeInfo::lossless(CanonicalType::Uuid),

            // XML
            "xml" => CanonicalTypeInfo::lossless(CanonicalType::Xml),

            // Spatial types (lossy - no direct canonical representation)
            "geometry" => CanonicalTypeInfo::lossy(
                CanonicalType::Geometry,
                "MSSQL geometry may lose precision in conversion.",
            ),
            "geography" => CanonicalTypeInfo::lossy(
                CanonicalType::Geography,
                "MSSQL geography may lose precision in conversion.",
            ),

            // Default fallback
            _ => CanonicalTypeInfo::lossy(
                CanonicalType::Unknown(data_type.to_string()),
                format!("Unknown MSSQL type '{}'.", data_type),
            ),
        }
    }
}

/// Canonical type to MSSQL converter.
#[derive(Debug, Clone, Default)]
pub struct MssqlFromCanonical;

impl MssqlFromCanonical {
    /// Create a new canonical to MSSQL converter.
    pub fn new() -> Self {
        Self
    }
}

impl FromCanonical for MssqlFromCanonical {
    fn dialect_name(&self) -> &str {
        "mssql"
    }

    fn from_canonical(&self, canonical: &CanonicalType) -> TypeMapping {
        match canonical {
            // Boolean
            CanonicalType::Boolean => TypeMapping::lossless("bit"),

            // Integer types
            CanonicalType::UInt8 => TypeMapping::lossless("tinyint"),
            CanonicalType::Int16 => TypeMapping::lossless("smallint"),
            CanonicalType::Int32 => TypeMapping::lossless("int"),
            CanonicalType::Int64 => TypeMapping::lossless("bigint"),

            // Floating point
            CanonicalType::Float32 => TypeMapping::lossless("real"),
            CanonicalType::Float64 => TypeMapping::lossless("float"),

            // Decimal/numeric
            CanonicalType::Decimal { precision, scale } => {
                let p = (*precision).min(38);
                let s = (*scale).min(p);
                if *precision > 38 {
                    TypeMapping::lossy(
                        format!("decimal({},{})", p, s),
                        format!("Precision {} exceeds MSSQL max of 38.", precision),
                    )
                } else {
                    TypeMapping::lossless(format!("decimal({},{})", p, s))
                }
            }
            CanonicalType::Money => TypeMapping::lossless("money"),
            CanonicalType::SmallMoney => TypeMapping::lossless("smallmoney"),

            // String types
            CanonicalType::Char(len) => {
                if *len <= 4000 {
                    TypeMapping::lossless(format!("nchar({})", len))
                } else {
                    TypeMapping::lossless("nvarchar(max)")
                }
            }
            CanonicalType::Varchar(len) => {
                if *len == 0 || *len > 4000 {
                    TypeMapping::lossless("nvarchar(max)")
                } else {
                    TypeMapping::lossless(format!("nvarchar({})", len))
                }
            }
            CanonicalType::Text => TypeMapping::lossless("nvarchar(max)"),

            // Binary types
            CanonicalType::Binary(len) => {
                if *len <= 8000 {
                    TypeMapping::lossless(format!("binary({})", len))
                } else {
                    TypeMapping::lossless("varbinary(max)")
                }
            }
            CanonicalType::Varbinary(len) => {
                if *len == 0 || *len > 8000 {
                    TypeMapping::lossless("varbinary(max)")
                } else {
                    TypeMapping::lossless(format!("varbinary({})", len))
                }
            }
            CanonicalType::Blob => TypeMapping::lossless("varbinary(max)"),

            // Date/time types
            CanonicalType::Date => TypeMapping::lossless("date"),
            CanonicalType::Time => TypeMapping::lossless("time"),
            CanonicalType::DateTime => TypeMapping::lossless("datetime2"),
            CanonicalType::DateTimeTz => TypeMapping::lossless("datetimeoffset"),
            CanonicalType::Interval => TypeMapping::lossy(
                "nvarchar(100)",
                "Interval stored as string. Interval arithmetic unavailable.",
            ),
            CanonicalType::Year => TypeMapping::lossless("smallint"),

            // Special types
            CanonicalType::Uuid => TypeMapping::lossless("uniqueidentifier"),
            CanonicalType::Json | CanonicalType::JsonBinary => TypeMapping::lossy(
                "nvarchar(max)",
                "JSON stored as string. Use SQL Server 2016+ JSON functions.",
            ),
            CanonicalType::Xml => TypeMapping::lossless("xml"),

            // Bit strings
            CanonicalType::Bit(len) => {
                if *len == 1 {
                    TypeMapping::lossless("bit")
                } else {
                    TypeMapping::lossless(format!("binary({})", (*len).div_ceil(8)))
                }
            }
            CanonicalType::VarBit(_) => TypeMapping::lossless("varbinary(max)"),

            // Network types (lossy)
            CanonicalType::InetAddr | CanonicalType::CidrAddr => {
                TypeMapping::lossy("varchar(50)", "Network type stored as string.")
            }
            CanonicalType::MacAddr => {
                TypeMapping::lossy("char(17)", "MAC address stored as string.")
            }

            // Geometric types (lossy)
            CanonicalType::Point
            | CanonicalType::Line
            | CanonicalType::Polygon
            | CanonicalType::Geometry => TypeMapping::lossy(
                "nvarchar(max)",
                "Geometric type stored as string. Consider MSSQL geometry type.",
            ),
            CanonicalType::Geography => TypeMapping::lossy(
                "nvarchar(max)",
                "Geographic type stored as string. Consider MSSQL geography type.",
            ),

            // Full-text search (lossy)
            CanonicalType::TsVector | CanonicalType::TsQuery => TypeMapping::lossy(
                "nvarchar(max)",
                "Full-text type stored as string. Use SQL Server full-text search.",
            ),

            // Range types (lossy)
            CanonicalType::Int4Range
            | CanonicalType::Int8Range
            | CanonicalType::NumRange
            | CanonicalType::TsRange
            | CanonicalType::TsTzRange
            | CanonicalType::DateRange => {
                TypeMapping::lossy("nvarchar(100)", "Range type stored as string.")
            }

            // Array types (lossy)
            CanonicalType::Array(_) => TypeMapping::lossy(
                "nvarchar(max)",
                "Array stored as JSON string. Array operations unavailable.",
            ),

            // MySQL-specific types (lossy)
            CanonicalType::Enum(_) => TypeMapping::lossy(
                "nvarchar(255)",
                "ENUM stored as string. Consider CHECK constraint.",
            ),
            CanonicalType::Set(_) => TypeMapping::lossy("nvarchar(max)", "SET stored as string."),

            // Fallback
            CanonicalType::Unknown(name) => TypeMapping::lossy(
                "nvarchar(max)",
                format!("Unknown type '{}' stored as string.", name),
            ),
        }
    }
}

/// Canonical to PostgreSQL type converter.
#[derive(Debug, Clone, Default)]
pub struct PostgresFromCanonical;

impl PostgresFromCanonical {
    /// Create a new canonical to PostgreSQL converter.
    pub fn new() -> Self {
        Self
    }
}

impl FromCanonical for PostgresFromCanonical {
    fn dialect_name(&self) -> &str {
        "postgres"
    }

    fn from_canonical(&self, canonical: &CanonicalType) -> TypeMapping {
        match canonical {
            // Boolean
            CanonicalType::Boolean => TypeMapping::lossless("boolean"),

            // Integer types
            CanonicalType::UInt8 => TypeMapping::lossless("smallint"), // PG has no unsigned types
            CanonicalType::Int16 => TypeMapping::lossless("smallint"),
            CanonicalType::Int32 => TypeMapping::lossless("integer"),
            CanonicalType::Int64 => TypeMapping::lossless("bigint"),

            // Floating point
            CanonicalType::Float32 => TypeMapping::lossless("real"),
            CanonicalType::Float64 => TypeMapping::lossless("double precision"),

            // Decimal/numeric
            CanonicalType::Decimal { precision, scale } => {
                TypeMapping::lossless(format!("numeric({},{})", precision, scale))
            }
            CanonicalType::Money => TypeMapping::lossless("numeric(19,4)"),
            CanonicalType::SmallMoney => TypeMapping::lossless("numeric(10,4)"),

            // String types
            CanonicalType::Char(len) => {
                if *len <= 10485760 {
                    TypeMapping::lossless(format!("char({})", len))
                } else {
                    TypeMapping::lossless("text")
                }
            }
            CanonicalType::Varchar(len) => {
                if *len == 0 || *len > 10485760 {
                    TypeMapping::lossless("text")
                } else {
                    TypeMapping::lossless(format!("varchar({})", len))
                }
            }
            CanonicalType::Text => TypeMapping::lossless("text"),

            // Binary types
            CanonicalType::Binary(_) | CanonicalType::Varbinary(_) | CanonicalType::Blob => {
                TypeMapping::lossless("bytea")
            }

            // Date/time types
            CanonicalType::Date => TypeMapping::lossless("date"),
            CanonicalType::Time => TypeMapping::lossless("time"),
            CanonicalType::DateTime => TypeMapping::lossless("timestamp"),
            CanonicalType::DateTimeTz => TypeMapping::lossless("timestamptz"),
            CanonicalType::Interval => TypeMapping::lossless("interval"),
            CanonicalType::Year => TypeMapping::lossless("smallint"),

            // Special types
            CanonicalType::Uuid => TypeMapping::lossless("uuid"),
            CanonicalType::Json => TypeMapping::lossless("json"),
            CanonicalType::JsonBinary => TypeMapping::lossless("jsonb"),
            CanonicalType::Xml => TypeMapping::lossless("xml"),

            // Bit strings
            CanonicalType::Bit(len) => {
                if *len == 1 {
                    TypeMapping::lossless("boolean")
                } else {
                    TypeMapping::lossless(format!("bit({})", len))
                }
            }
            CanonicalType::VarBit(len) => {
                if *len == 0 {
                    TypeMapping::lossless("bit varying")
                } else {
                    TypeMapping::lossless(format!("bit varying({})", len))
                }
            }

            // Network types
            CanonicalType::InetAddr => TypeMapping::lossless("inet"),
            CanonicalType::CidrAddr => TypeMapping::lossless("cidr"),
            CanonicalType::MacAddr => TypeMapping::lossless("macaddr"),

            // Geometric types
            CanonicalType::Point => TypeMapping::lossless("point"),
            CanonicalType::Line => TypeMapping::lossless("line"),
            CanonicalType::Polygon => TypeMapping::lossless("polygon"),
            CanonicalType::Geometry | CanonicalType::Geography => TypeMapping::lossy(
                "text",
                "Spatial type stored as text. Consider PostGIS for spatial operations.",
            ),

            // Full-text search
            CanonicalType::TsVector => TypeMapping::lossless("tsvector"),
            CanonicalType::TsQuery => TypeMapping::lossless("tsquery"),

            // Range types
            CanonicalType::Int4Range => TypeMapping::lossless("int4range"),
            CanonicalType::Int8Range => TypeMapping::lossless("int8range"),
            CanonicalType::NumRange => TypeMapping::lossless("numrange"),
            CanonicalType::TsRange => TypeMapping::lossless("tsrange"),
            CanonicalType::TsTzRange => TypeMapping::lossless("tstzrange"),
            CanonicalType::DateRange => TypeMapping::lossless("daterange"),

            // Array types
            CanonicalType::Array(inner) => {
                let inner_mapping = self.from_canonical(inner);
                TypeMapping {
                    target_type: format!("{}[]", inner_mapping.target_type),
                    is_lossy: inner_mapping.is_lossy,
                    warning: inner_mapping.warning,
                }
            }

            // MySQL-specific types (lossy)
            CanonicalType::Enum(_) => TypeMapping::lossy(
                "text",
                "ENUM stored as text. Consider PostgreSQL ENUM or CHECK constraint.",
            ),
            CanonicalType::Set(_) => TypeMapping::lossy(
                "text",
                "SET stored as text. Consider array type or separate table.",
            ),

            // Fallback
            CanonicalType::Unknown(name) => {
                TypeMapping::lossy("text", format!("Unknown type '{}' stored as text.", name))
            }
        }
    }
}

/// PostgreSQL to canonical type converter.
#[derive(Debug, Clone, Default)]
pub struct PostgresToCanonical;

impl PostgresToCanonical {
    /// Create a new PostgreSQL to canonical converter.
    pub fn new() -> Self {
        Self
    }
}

impl ToCanonical for PostgresToCanonical {
    fn dialect_name(&self) -> &str {
        "postgres"
    }

    fn to_canonical(
        &self,
        data_type: &str,
        max_length: i32,
        precision: i32,
        scale: i32,
    ) -> CanonicalTypeInfo {
        let pg_lower = data_type.to_lowercase();

        // Handle array types
        if pg_lower.ends_with("[]") {
            let base_type = &pg_lower[..pg_lower.len() - 2];
            let inner = self.to_canonical(base_type, max_length, precision, scale);
            return CanonicalTypeInfo {
                canonical_type: CanonicalType::Array(Box::new(inner.canonical_type)),
                is_lossy: inner.is_lossy,
                warning: inner.warning,
            };
        }
        // Handle underscore-prefixed array notation
        if let Some(base_type) = pg_lower.strip_prefix('_') {
            let inner = self.to_canonical(base_type, max_length, precision, scale);
            return CanonicalTypeInfo {
                canonical_type: CanonicalType::Array(Box::new(inner.canonical_type)),
                is_lossy: inner.is_lossy,
                warning: inner.warning,
            };
        }

        match pg_lower.as_str() {
            // Boolean
            "bool" | "boolean" => CanonicalTypeInfo::lossless(CanonicalType::Boolean),

            // Integer types
            "int2" | "smallint" => CanonicalTypeInfo::lossless(CanonicalType::Int16),
            "int4" | "integer" | "int" => CanonicalTypeInfo::lossless(CanonicalType::Int32),
            "int8" | "bigint" => CanonicalTypeInfo::lossless(CanonicalType::Int64),
            "serial" => CanonicalTypeInfo::lossless(CanonicalType::Int32),
            "bigserial" => CanonicalTypeInfo::lossless(CanonicalType::Int64),
            "smallserial" => CanonicalTypeInfo::lossless(CanonicalType::Int16),

            // Floating point
            "float4" | "real" => CanonicalTypeInfo::lossless(CanonicalType::Float32),
            "float8" | "double precision" => CanonicalTypeInfo::lossless(CanonicalType::Float64),

            // Decimal/numeric
            "numeric" | "decimal" => {
                let p = if precision > 0 { precision as u16 } else { 38 };
                let s = scale as u16;
                CanonicalTypeInfo::lossless(CanonicalType::Decimal {
                    precision: p,
                    scale: s,
                })
            }
            "money" => CanonicalTypeInfo::lossless(CanonicalType::Money),

            // String types
            "char" | "character" | "bpchar" => {
                let len = if max_length > 0 { max_length as u32 } else { 1 };
                CanonicalTypeInfo::lossless(CanonicalType::Char(len))
            }
            "varchar" | "character varying" => {
                let len = if max_length > 0 { max_length as u32 } else { 0 }; // 0 = unlimited
                CanonicalTypeInfo::lossless(CanonicalType::Varchar(len))
            }
            "text" => CanonicalTypeInfo::lossless(CanonicalType::Text),
            "name" => CanonicalTypeInfo::lossless(CanonicalType::Varchar(63)),

            // Binary types
            "bytea" => CanonicalTypeInfo::lossless(CanonicalType::Blob),

            // Date/time types
            "date" => CanonicalTypeInfo::lossless(CanonicalType::Date),
            "time" | "time without time zone" => CanonicalTypeInfo::lossless(CanonicalType::Time),
            "timetz" | "time with time zone" => CanonicalTypeInfo::lossy(
                CanonicalType::Time,
                "Time with timezone loses timezone info in some targets.",
            ),
            "timestamp" | "timestamp without time zone" => {
                CanonicalTypeInfo::lossless(CanonicalType::DateTime)
            }
            "timestamptz" | "timestamp with time zone" => {
                CanonicalTypeInfo::lossless(CanonicalType::DateTimeTz)
            }
            "interval" => CanonicalTypeInfo::lossless(CanonicalType::Interval),

            // Special types
            "uuid" => CanonicalTypeInfo::lossless(CanonicalType::Uuid),
            "json" => CanonicalTypeInfo::lossless(CanonicalType::Json),
            "jsonb" => CanonicalTypeInfo::lossy(
                CanonicalType::JsonBinary,
                "JSONB binary features may be unavailable in target.",
            ),
            "xml" => CanonicalTypeInfo::lossless(CanonicalType::Xml),

            // Bit strings
            "bit" => {
                let len = if max_length > 0 { max_length as u32 } else { 1 };
                CanonicalTypeInfo::lossless(CanonicalType::Bit(len))
            }
            "varbit" | "bit varying" => {
                let len = if max_length > 0 { max_length as u32 } else { 0 };
                CanonicalTypeInfo::lossless(CanonicalType::VarBit(len))
            }

            // Network types
            "inet" => CanonicalTypeInfo::lossless(CanonicalType::InetAddr),
            "cidr" => CanonicalTypeInfo::lossless(CanonicalType::CidrAddr),
            "macaddr" | "macaddr8" => CanonicalTypeInfo::lossless(CanonicalType::MacAddr),

            // Geometric types
            "point" => CanonicalTypeInfo::lossless(CanonicalType::Point),
            "line" | "lseg" => CanonicalTypeInfo::lossless(CanonicalType::Line),
            "box" | "path" | "polygon" | "circle" => {
                CanonicalTypeInfo::lossless(CanonicalType::Polygon)
            }

            // Full-text search
            "tsvector" => CanonicalTypeInfo::lossless(CanonicalType::TsVector),
            "tsquery" => CanonicalTypeInfo::lossless(CanonicalType::TsQuery),

            // Range types
            "int4range" => CanonicalTypeInfo::lossless(CanonicalType::Int4Range),
            "int8range" => CanonicalTypeInfo::lossless(CanonicalType::Int8Range),
            "numrange" => CanonicalTypeInfo::lossless(CanonicalType::NumRange),
            "tsrange" => CanonicalTypeInfo::lossless(CanonicalType::TsRange),
            "tstzrange" => CanonicalTypeInfo::lossless(CanonicalType::TsTzRange),
            "daterange" => CanonicalTypeInfo::lossless(CanonicalType::DateRange),

            // OID types
            "oid" => CanonicalTypeInfo::lossless(CanonicalType::Int64),

            // Default fallback
            _ => CanonicalTypeInfo::lossy(
                CanonicalType::Unknown(data_type.to_string()),
                format!("Unknown PostgreSQL type '{}'.", data_type),
            ),
        }
    }
}

/// MySQL to canonical type converter.
#[derive(Debug, Clone, Default)]
pub struct MysqlToCanonical;

impl MysqlToCanonical {
    /// Create a new MySQL to canonical converter.
    pub fn new() -> Self {
        Self
    }
}

impl ToCanonical for MysqlToCanonical {
    fn dialect_name(&self) -> &str {
        "mysql"
    }

    fn to_canonical(
        &self,
        data_type: &str,
        max_length: i32,
        precision: i32,
        scale: i32,
    ) -> CanonicalTypeInfo {
        let mysql_lower = data_type.to_lowercase();

        // Detect MySQL boolean types.
        //
        // MySQL represents BOOLEAN as TINYINT(1). Depending on how columns are
        // introspected, we may see either:
        //   - DATA_TYPE = 'tinyint' with max_length coming from CHARACTER_MAXIMUM_LENGTH
        //   - COLUMN_TYPE like 'tinyint(1)' (possibly with modifiers)
        //
        // We treat a column as boolean if:
        //   * it is declared as BOOL/BOOLEAN, or
        //   * it is a TINYINT whose display width is 1, as indicated either by
        //     max_length == 1 or by '(1)' appearing in the type string.
        let is_tinyint_bool = mysql_lower.starts_with("tinyint")
            && (max_length == 1 || mysql_lower == "tinyint(1)" || mysql_lower.contains("(1)"));

        match mysql_lower.as_str() {
            // Boolean (MySQL uses TINYINT(1) for bool)
            "bool" | "boolean" => CanonicalTypeInfo::lossless(CanonicalType::Boolean),
            "tinyint" if is_tinyint_bool => CanonicalTypeInfo::lossless(CanonicalType::Boolean),
            t if t.starts_with("tinyint") && is_tinyint_bool => {
                CanonicalTypeInfo::lossless(CanonicalType::Boolean)
            }

            // Integer types
            "tinyint" => CanonicalTypeInfo::lossless(CanonicalType::UInt8),
            "smallint" => CanonicalTypeInfo::lossless(CanonicalType::Int16),
            "mediumint" => CanonicalTypeInfo::lossless(CanonicalType::Int32),
            "int" | "integer" => CanonicalTypeInfo::lossless(CanonicalType::Int32),
            "bigint" => CanonicalTypeInfo::lossless(CanonicalType::Int64),

            // Floating point
            "float" => CanonicalTypeInfo::lossless(CanonicalType::Float32),
            "double" | "double precision" | "real" => {
                CanonicalTypeInfo::lossless(CanonicalType::Float64)
            }

            // Decimal/numeric
            "decimal" | "numeric" | "dec" | "fixed" => {
                let p = if precision > 0 { precision as u16 } else { 10 };
                let s = scale as u16;
                CanonicalTypeInfo::lossless(CanonicalType::Decimal {
                    precision: p,
                    scale: s,
                })
            }

            // String types
            "char" => {
                let len = if max_length > 0 { max_length as u32 } else { 1 };
                CanonicalTypeInfo::lossless(CanonicalType::Char(len))
            }
            "varchar" => {
                let len = if max_length > 0 {
                    max_length as u32
                } else {
                    255
                };
                CanonicalTypeInfo::lossless(CanonicalType::Varchar(len))
            }
            "tinytext" | "text" | "mediumtext" | "longtext" => {
                CanonicalTypeInfo::lossless(CanonicalType::Text)
            }

            // Binary types
            "binary" => {
                let len = if max_length > 0 { max_length as u32 } else { 1 };
                CanonicalTypeInfo::lossless(CanonicalType::Binary(len))
            }
            "varbinary" => {
                let len = if max_length > 0 {
                    max_length as u32
                } else {
                    255
                };
                CanonicalTypeInfo::lossless(CanonicalType::Varbinary(len))
            }
            "tinyblob" | "blob" | "mediumblob" | "longblob" => {
                CanonicalTypeInfo::lossless(CanonicalType::Blob)
            }

            // Date/time types
            "date" => CanonicalTypeInfo::lossless(CanonicalType::Date),
            "time" => CanonicalTypeInfo::lossless(CanonicalType::Time),
            "datetime" | "timestamp" => CanonicalTypeInfo::lossless(CanonicalType::DateTime),
            "year" => CanonicalTypeInfo::lossless(CanonicalType::Year),

            // JSON
            "json" => CanonicalTypeInfo::lossless(CanonicalType::Json),

            // Bit
            "bit" => {
                let len = if max_length > 0 { max_length as u32 } else { 1 };
                if len == 1 {
                    CanonicalTypeInfo::lossless(CanonicalType::Boolean)
                } else {
                    CanonicalTypeInfo::lossless(CanonicalType::Bit(len))
                }
            }

            // ENUM and SET
            "enum" => CanonicalTypeInfo::lossy(
                CanonicalType::Enum(Vec::new()),
                "ENUM values not preserved in canonical form.",
            ),
            "set" => CanonicalTypeInfo::lossy(
                CanonicalType::Set(Vec::new()),
                "SET values not preserved in canonical form.",
            ),

            // Spatial types (lossy)
            "geometry" | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring"
            | "multipolygon" | "geometrycollection" => CanonicalTypeInfo::lossy(
                CanonicalType::Geometry,
                format!(
                    "MySQL spatial type '{}' may lose precision in conversion.",
                    data_type
                ),
            ),

            // Default fallback
            _ => CanonicalTypeInfo::lossy(
                CanonicalType::Unknown(data_type.to_string()),
                format!("Unknown MySQL type '{}'.", data_type),
            ),
        }
    }
}

/// Canonical type to MySQL converter.
#[derive(Debug, Clone, Default)]
pub struct MysqlFromCanonical;

impl MysqlFromCanonical {
    /// Create a new canonical to MySQL converter.
    pub fn new() -> Self {
        Self
    }
}

impl FromCanonical for MysqlFromCanonical {
    fn dialect_name(&self) -> &str {
        "mysql"
    }

    fn from_canonical(&self, canonical: &CanonicalType) -> TypeMapping {
        match canonical {
            // Boolean
            CanonicalType::Boolean => TypeMapping::lossless("TINYINT(1)"),

            // Integer types
            CanonicalType::UInt8 => TypeMapping::lossless("TINYINT UNSIGNED"),
            CanonicalType::Int16 => TypeMapping::lossless("SMALLINT"),
            CanonicalType::Int32 => TypeMapping::lossless("INT"),
            CanonicalType::Int64 => TypeMapping::lossless("BIGINT"),

            // Floating point
            CanonicalType::Float32 => TypeMapping::lossless("FLOAT"),
            CanonicalType::Float64 => TypeMapping::lossless("DOUBLE"),

            // Decimal/numeric
            CanonicalType::Decimal { precision, scale } => {
                let p = (*precision).min(65);
                let s = (*scale).min(30).min(p);
                if *precision > 65 {
                    TypeMapping::lossy(
                        format!("DECIMAL({},{})", p, s),
                        format!("Precision {} exceeds MySQL max of 65.", precision),
                    )
                } else {
                    TypeMapping::lossless(format!("DECIMAL({},{})", p, s))
                }
            }
            CanonicalType::Money => TypeMapping::lossless("DECIMAL(19,4)"),
            CanonicalType::SmallMoney => TypeMapping::lossless("DECIMAL(10,4)"),

            // String types
            CanonicalType::Char(len) => {
                if *len <= 255 {
                    TypeMapping::lossless(format!("CHAR({})", len))
                } else {
                    TypeMapping::lossless("TEXT")
                }
            }
            CanonicalType::Varchar(len) => {
                if *len == 0 || *len > 65535 {
                    TypeMapping::lossless("LONGTEXT")
                } else {
                    TypeMapping::lossless(format!("VARCHAR({})", len))
                }
            }
            CanonicalType::Text => TypeMapping::lossless("LONGTEXT"),

            // Binary types
            CanonicalType::Binary(len) => {
                if *len <= 255 {
                    TypeMapping::lossless(format!("BINARY({})", len))
                } else {
                    TypeMapping::lossless("BLOB")
                }
            }
            CanonicalType::Varbinary(len) => {
                if *len == 0 || *len > 65535 {
                    TypeMapping::lossless("LONGBLOB")
                } else {
                    TypeMapping::lossless(format!("VARBINARY({})", len))
                }
            }
            CanonicalType::Blob => TypeMapping::lossless("LONGBLOB"),

            // Date/time types
            CanonicalType::Date => TypeMapping::lossless("DATE"),
            CanonicalType::Time => TypeMapping::lossless("TIME"),
            CanonicalType::DateTime => TypeMapping::lossless("DATETIME"),
            CanonicalType::DateTimeTz => TypeMapping::lossy(
                "DATETIME",
                "Timestamp with timezone loses timezone info in MySQL. Consider storing as UTC.",
            ),
            CanonicalType::Interval => TypeMapping::lossy(
                "VARCHAR(100)",
                "Interval stored as string. Interval arithmetic unavailable.",
            ),
            CanonicalType::Year => TypeMapping::lossless("YEAR"),

            // Special types
            CanonicalType::Uuid => TypeMapping::lossy("VARCHAR(36)", "UUID stored as VARCHAR(36)."),
            CanonicalType::Json | CanonicalType::JsonBinary => TypeMapping::lossless("JSON"),
            CanonicalType::Xml => {
                TypeMapping::lossy("LONGTEXT", "XML stored as text. XML functions unavailable.")
            }

            // Bit strings
            CanonicalType::Bit(len) => {
                if *len <= 64 {
                    TypeMapping::lossless(format!("BIT({})", len))
                } else {
                    TypeMapping::lossless("BLOB")
                }
            }
            CanonicalType::VarBit(_) => TypeMapping::lossless("BLOB"),

            // Network types (lossy)
            CanonicalType::InetAddr | CanonicalType::CidrAddr => TypeMapping::lossy(
                "VARCHAR(50)",
                "Network type stored as string. Network operations unavailable.",
            ),
            CanonicalType::MacAddr => {
                TypeMapping::lossy("CHAR(17)", "MAC address stored as string.")
            }

            // Geometric types (lossy - MySQL has spatial types but different semantics)
            CanonicalType::Point => {
                TypeMapping::lossy("TEXT", "Point stored as text. Consider MySQL POINT type.")
            }
            CanonicalType::Line => TypeMapping::lossy(
                "TEXT",
                "Line stored as text. Consider MySQL LINESTRING type.",
            ),
            CanonicalType::Polygon => TypeMapping::lossy(
                "TEXT",
                "Polygon stored as text. Consider MySQL POLYGON type.",
            ),
            CanonicalType::Geometry | CanonicalType::Geography => TypeMapping::lossy(
                "TEXT",
                "Spatial type stored as text. Consider MySQL spatial types.",
            ),

            // Full-text search (lossy)
            CanonicalType::TsVector | CanonicalType::TsQuery => TypeMapping::lossy(
                "LONGTEXT",
                "Full-text type stored as text. Use MySQL FULLTEXT instead.",
            ),

            // Range types (lossy)
            CanonicalType::Int4Range
            | CanonicalType::Int8Range
            | CanonicalType::NumRange
            | CanonicalType::TsRange
            | CanonicalType::TsTzRange
            | CanonicalType::DateRange => TypeMapping::lossy(
                "VARCHAR(100)",
                "Range type stored as string. Range operations unavailable.",
            ),

            // Array types (lossy)
            CanonicalType::Array(_) => TypeMapping::lossy(
                "JSON",
                "Array stored as JSON. Array operations unavailable.",
            ),

            // MySQL-specific types
            CanonicalType::Enum(values) => {
                if values.is_empty() {
                    TypeMapping::lossy("TEXT", "ENUM values not available; stored as TEXT.")
                } else {
                    let vals = values
                        .iter()
                        .map(|v| format!("'{}'", v.replace('\'', "''")))
                        .collect::<Vec<_>>()
                        .join(",");
                    TypeMapping::lossless(format!("ENUM({})", vals))
                }
            }
            CanonicalType::Set(values) => {
                if values.is_empty() {
                    TypeMapping::lossy("TEXT", "SET values not available; stored as TEXT.")
                } else {
                    let vals = values
                        .iter()
                        .map(|v| format!("'{}'", v.replace('\'', "''")))
                        .collect::<Vec<_>>()
                        .join(",");
                    TypeMapping::lossless(format!("SET({})", vals))
                }
            }

            // Fallback
            CanonicalType::Unknown(name) => TypeMapping::lossy(
                "LONGTEXT",
                format!("Unknown type '{}' stored as text.", name),
            ),
        }
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

    // MySQL type mapper tests

    #[test]
    fn test_mysql_to_postgres_mapper() {
        let mapper = MysqlToPostgresMapper::new();
        assert_eq!(mapper.source_dialect(), "mysql");
        assert_eq!(mapper.target_dialect(), "postgres");

        let col = test_column("id", "int", 0, 0, 0);
        let mapping = mapper.map_column(&col);
        assert_eq!(mapping.target_type, "integer");
        assert!(mapping.warning.is_none());
    }

    #[test]
    fn test_postgres_to_mysql_mapper() {
        let mapper = PostgresToMysqlMapper::new();
        assert_eq!(mapper.source_dialect(), "postgres");
        assert_eq!(mapper.target_dialect(), "mysql");

        let col = test_column("id", "integer", 0, 0, 0);
        let mapping = mapper.map_column(&col);
        assert_eq!(mapping.target_type, "INT");
        assert!(mapping.warning.is_none());
    }

    #[test]
    fn test_mysql_to_mssql_mapper() {
        let mapper = MysqlToMssqlMapper::new();
        assert_eq!(mapper.source_dialect(), "mysql");
        assert_eq!(mapper.target_dialect(), "mssql");

        let col = test_column("id", "int", 0, 0, 0);
        let mapping = mapper.map_column(&col);
        assert_eq!(mapping.target_type, "int");
        assert!(mapping.warning.is_none());
    }

    #[test]
    fn test_mssql_to_mysql_mapper() {
        let mapper = MssqlToMysqlMapper::new();
        assert_eq!(mapper.source_dialect(), "mssql");
        assert_eq!(mapper.target_dialect(), "mysql");

        let col = test_column("id", "int", 0, 0, 0);
        let mapping = mapper.map_column(&col);
        assert_eq!(mapping.target_type, "INT");
        assert!(mapping.warning.is_none());
    }

    #[test]
    fn test_mysql_to_postgres_integer_types() {
        let m = mysql_to_postgres("int", 0, 0, 0);
        assert_eq!(m.target_type, "integer");
        let m = mysql_to_postgres("bigint", 0, 0, 0);
        assert_eq!(m.target_type, "bigint");
        let m = mysql_to_postgres("smallint", 0, 0, 0);
        assert_eq!(m.target_type, "smallint");
        let m = mysql_to_postgres("mediumint", 0, 0, 0);
        assert_eq!(m.target_type, "integer");
    }

    #[test]
    fn test_mysql_to_postgres_tinyint_bool() {
        // TINYINT(1) should be boolean
        let m = mysql_to_postgres("tinyint", 1, 0, 0);
        assert_eq!(m.target_type, "boolean");
        // TINYINT without (1) should be smallint
        let m = mysql_to_postgres("tinyint", 0, 0, 0);
        assert_eq!(m.target_type, "smallint");
    }

    #[test]
    fn test_mysql_to_postgres_string_types() {
        let m = mysql_to_postgres("varchar", 100, 0, 0);
        assert_eq!(m.target_type, "varchar(100)");
        let m = mysql_to_postgres("text", 0, 0, 0);
        assert_eq!(m.target_type, "text");
        let m = mysql_to_postgres("longtext", 0, 0, 0);
        assert_eq!(m.target_type, "text");
    }

    #[test]
    fn test_mysql_to_postgres_json() {
        let m = mysql_to_postgres("json", 0, 0, 0);
        assert_eq!(m.target_type, "jsonb");
        assert!(!m.is_lossy);
    }

    #[test]
    fn test_mysql_to_postgres_enum_lossy() {
        let m = mysql_to_postgres("enum", 0, 0, 0);
        assert_eq!(m.target_type, "text");
        assert!(m.is_lossy);
        assert!(m.warning.as_ref().unwrap().contains("ENUM"));
    }

    #[test]
    fn test_postgres_to_mysql_uuid_lossy() {
        let m = postgres_to_mysql("uuid", 0, 0, 0);
        assert_eq!(m.target_type, "VARCHAR(36)");
        assert!(m.is_lossy);
    }

    #[test]
    fn test_postgres_to_mysql_arrays_lossy() {
        let m = postgres_to_mysql("integer[]", 0, 0, 0);
        assert_eq!(m.target_type, "JSON");
        assert!(m.is_lossy);
        assert!(m.warning.as_ref().unwrap().contains("Array"));
    }

    #[test]
    fn test_mssql_to_mysql_types() {
        let m = mssql_to_mysql("bit", 0, 0, 0);
        assert_eq!(m.target_type, "TINYINT(1)");
        let m = mssql_to_mysql("nvarchar", 255, 0, 0);
        assert_eq!(m.target_type, "VARCHAR(255)");
        let m = mssql_to_mysql("nvarchar", -1, 0, 0);
        assert_eq!(m.target_type, "LONGTEXT");
    }

    #[test]
    fn test_mssql_to_mysql_uniqueidentifier_lossy() {
        let m = mssql_to_mysql("uniqueidentifier", 0, 0, 0);
        assert_eq!(m.target_type, "VARCHAR(36)");
        assert!(m.is_lossy);
    }

    #[test]
    fn test_mysql_to_mssql_types() {
        let m = mysql_to_mssql("int", 0, 0, 0);
        assert_eq!(m.target_type, "int");
        let m = mysql_to_mssql("varchar", 100, 0, 0);
        assert_eq!(m.target_type, "nvarchar(100)");
        let m = mysql_to_mssql("longtext", 0, 0, 0);
        assert_eq!(m.target_type, "nvarchar(max)");
    }

    #[test]
    fn test_mysql_to_mssql_json_lossy() {
        let m = mysql_to_mssql("json", 0, 0, 0);
        assert_eq!(m.target_type, "nvarchar(max)");
        assert!(m.is_lossy);
    }

    // ==========================================================================
    // Canonical Type System Tests
    // ==========================================================================

    // --- MssqlToCanonical Tests ---

    #[test]
    fn test_mssql_to_canonical_integers() {
        let conv = MssqlToCanonical::new();
        assert_eq!(conv.dialect_name(), "mssql");

        let info = conv.to_canonical("bit", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Boolean);
        assert!(!info.is_lossy);

        let info = conv.to_canonical("tinyint", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::UInt8);

        let info = conv.to_canonical("smallint", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Int16);

        let info = conv.to_canonical("int", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Int32);

        let info = conv.to_canonical("bigint", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Int64);
    }

    #[test]
    fn test_mssql_to_canonical_decimals() {
        let conv = MssqlToCanonical::new();

        let info = conv.to_canonical("decimal", 0, 10, 2);
        assert_eq!(
            info.canonical_type,
            CanonicalType::Decimal {
                precision: 10,
                scale: 2
            }
        );

        let info = conv.to_canonical("money", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Money);

        let info = conv.to_canonical("smallmoney", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::SmallMoney);
    }

    #[test]
    fn test_mssql_to_canonical_strings() {
        let conv = MssqlToCanonical::new();

        let info = conv.to_canonical("varchar", 100, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Varchar(100));

        let info = conv.to_canonical("varchar", -1, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Text);

        let info = conv.to_canonical("nvarchar", 255, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Varchar(255));

        let info = conv.to_canonical("char", 10, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Char(10));

        let info = conv.to_canonical("text", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Text);
    }

    #[test]
    fn test_mssql_to_canonical_datetime() {
        let conv = MssqlToCanonical::new();

        let info = conv.to_canonical("date", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Date);

        let info = conv.to_canonical("time", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Time);

        let info = conv.to_canonical("datetime2", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::DateTime);

        let info = conv.to_canonical("datetimeoffset", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::DateTimeTz);
    }

    #[test]
    fn test_mssql_to_canonical_special_types() {
        let conv = MssqlToCanonical::new();

        let info = conv.to_canonical("uniqueidentifier", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Uuid);

        let info = conv.to_canonical("xml", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Xml);

        let info = conv.to_canonical("varbinary", -1, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Blob);

        let info = conv.to_canonical("image", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Blob);
    }

    #[test]
    fn test_mssql_to_canonical_unknown_type() {
        let conv = MssqlToCanonical::new();

        let info = conv.to_canonical("unknowntype", 0, 0, 0);
        assert!(matches!(info.canonical_type, CanonicalType::Unknown(_)));
        assert!(info.is_lossy);
    }

    // --- PostgresToCanonical Tests ---

    #[test]
    fn test_postgres_to_canonical_integers() {
        let conv = PostgresToCanonical::new();
        assert_eq!(conv.dialect_name(), "postgres");

        let info = conv.to_canonical("boolean", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Boolean);

        let info = conv.to_canonical("int2", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Int16);

        let info = conv.to_canonical("int4", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Int32);

        let info = conv.to_canonical("integer", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Int32);

        let info = conv.to_canonical("int8", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Int64);

        let info = conv.to_canonical("bigint", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Int64);
    }

    #[test]
    fn test_postgres_to_canonical_floats() {
        let conv = PostgresToCanonical::new();

        let info = conv.to_canonical("real", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Float32);

        let info = conv.to_canonical("float4", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Float32);

        let info = conv.to_canonical("double precision", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Float64);

        let info = conv.to_canonical("float8", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Float64);
    }

    #[test]
    fn test_postgres_to_canonical_strings() {
        let conv = PostgresToCanonical::new();

        let info = conv.to_canonical("varchar", 100, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Varchar(100));

        let info = conv.to_canonical("character varying", 255, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Varchar(255));

        let info = conv.to_canonical("text", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Text);

        let info = conv.to_canonical("char", 10, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Char(10));
    }

    #[test]
    fn test_postgres_to_canonical_json() {
        let conv = PostgresToCanonical::new();

        let info = conv.to_canonical("json", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Json);
        assert!(!info.is_lossy);

        let info = conv.to_canonical("jsonb", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::JsonBinary);
        assert!(info.is_lossy); // JSONB has binary features that may not be available
    }

    #[test]
    fn test_postgres_to_canonical_arrays() {
        let conv = PostgresToCanonical::new();

        let info = conv.to_canonical("integer[]", 0, 0, 0);
        assert!(matches!(info.canonical_type, CanonicalType::Array(_)));
        if let CanonicalType::Array(inner) = info.canonical_type {
            assert_eq!(*inner, CanonicalType::Int32);
        }

        // Underscore notation
        let info = conv.to_canonical("_int4", 0, 0, 0);
        assert!(matches!(info.canonical_type, CanonicalType::Array(_)));
    }

    #[test]
    fn test_postgres_to_canonical_network_types() {
        let conv = PostgresToCanonical::new();

        let info = conv.to_canonical("inet", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::InetAddr);

        let info = conv.to_canonical("cidr", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::CidrAddr);

        let info = conv.to_canonical("macaddr", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::MacAddr);
    }

    #[test]
    fn test_postgres_to_canonical_range_types() {
        let conv = PostgresToCanonical::new();

        let info = conv.to_canonical("int4range", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Int4Range);

        let info = conv.to_canonical("int8range", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Int8Range);

        let info = conv.to_canonical("tsrange", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::TsRange);

        let info = conv.to_canonical("daterange", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::DateRange);
    }

    // --- MysqlToCanonical Tests ---

    #[test]
    fn test_mysql_to_canonical_integers() {
        let conv = MysqlToCanonical::new();
        assert_eq!(conv.dialect_name(), "mysql");

        // TINYINT(1) is boolean
        let info = conv.to_canonical("tinyint", 1, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Boolean);

        // TINYINT without (1) is UInt8
        let info = conv.to_canonical("tinyint", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::UInt8);

        let info = conv.to_canonical("smallint", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Int16);

        let info = conv.to_canonical("mediumint", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Int32);

        let info = conv.to_canonical("int", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Int32);

        let info = conv.to_canonical("bigint", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Int64);
    }

    #[test]
    fn test_mysql_to_canonical_strings() {
        let conv = MysqlToCanonical::new();

        let info = conv.to_canonical("varchar", 100, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Varchar(100));

        let info = conv.to_canonical("text", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Text);

        let info = conv.to_canonical("longtext", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Text);

        let info = conv.to_canonical("char", 50, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Char(50));
    }

    #[test]
    fn test_mysql_to_canonical_datetime() {
        let conv = MysqlToCanonical::new();

        let info = conv.to_canonical("date", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Date);

        let info = conv.to_canonical("datetime", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::DateTime);

        let info = conv.to_canonical("timestamp", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::DateTime);

        let info = conv.to_canonical("year", 0, 0, 0);
        assert_eq!(info.canonical_type, CanonicalType::Year);
    }

    #[test]
    fn test_mysql_to_canonical_enum_set() {
        let conv = MysqlToCanonical::new();

        let info = conv.to_canonical("enum", 0, 0, 0);
        assert!(matches!(info.canonical_type, CanonicalType::Enum(_)));
        assert!(info.is_lossy);

        let info = conv.to_canonical("set", 0, 0, 0);
        assert!(matches!(info.canonical_type, CanonicalType::Set(_)));
        assert!(info.is_lossy);
    }

    // --- MssqlFromCanonical Tests ---

    #[test]
    fn test_mssql_from_canonical_integers() {
        let conv = MssqlFromCanonical::new();
        assert_eq!(conv.dialect_name(), "mssql");

        let m = conv.from_canonical(&CanonicalType::Boolean);
        assert_eq!(m.target_type, "bit");

        let m = conv.from_canonical(&CanonicalType::Int16);
        assert_eq!(m.target_type, "smallint");

        let m = conv.from_canonical(&CanonicalType::Int32);
        assert_eq!(m.target_type, "int");

        let m = conv.from_canonical(&CanonicalType::Int64);
        assert_eq!(m.target_type, "bigint");
    }

    #[test]
    fn test_mssql_from_canonical_strings() {
        let conv = MssqlFromCanonical::new();

        let m = conv.from_canonical(&CanonicalType::Varchar(100));
        assert_eq!(m.target_type, "nvarchar(100)");

        let m = conv.from_canonical(&CanonicalType::Varchar(0));
        assert_eq!(m.target_type, "nvarchar(max)");

        let m = conv.from_canonical(&CanonicalType::Text);
        assert_eq!(m.target_type, "nvarchar(max)");
    }

    #[test]
    fn test_mssql_from_canonical_lossy_types() {
        let conv = MssqlFromCanonical::new();

        let m = conv.from_canonical(&CanonicalType::Json);
        assert_eq!(m.target_type, "nvarchar(max)");
        assert!(m.is_lossy);

        let m = conv.from_canonical(&CanonicalType::Array(Box::new(CanonicalType::Int32)));
        assert_eq!(m.target_type, "nvarchar(max)");
        assert!(m.is_lossy);

        let m = conv.from_canonical(&CanonicalType::InetAddr);
        assert_eq!(m.target_type, "varchar(50)");
        assert!(m.is_lossy);
    }

    // --- PostgresFromCanonical Tests ---

    #[test]
    fn test_postgres_from_canonical_integers() {
        let conv = PostgresFromCanonical::new();
        assert_eq!(conv.dialect_name(), "postgres");

        let m = conv.from_canonical(&CanonicalType::Boolean);
        assert_eq!(m.target_type, "boolean");

        let m = conv.from_canonical(&CanonicalType::Int16);
        assert_eq!(m.target_type, "smallint");

        let m = conv.from_canonical(&CanonicalType::Int32);
        assert_eq!(m.target_type, "integer");

        let m = conv.from_canonical(&CanonicalType::Int64);
        assert_eq!(m.target_type, "bigint");

        // UInt8 maps to smallint (PG has no unsigned types)
        let m = conv.from_canonical(&CanonicalType::UInt8);
        assert_eq!(m.target_type, "smallint");
    }

    #[test]
    fn test_postgres_from_canonical_native_types() {
        let conv = PostgresFromCanonical::new();

        // PostgreSQL native types should be lossless
        let m = conv.from_canonical(&CanonicalType::InetAddr);
        assert_eq!(m.target_type, "inet");
        assert!(!m.is_lossy);

        let m = conv.from_canonical(&CanonicalType::JsonBinary);
        assert_eq!(m.target_type, "jsonb");
        assert!(!m.is_lossy);

        let m = conv.from_canonical(&CanonicalType::TsVector);
        assert_eq!(m.target_type, "tsvector");
        assert!(!m.is_lossy);

        let m = conv.from_canonical(&CanonicalType::Int4Range);
        assert_eq!(m.target_type, "int4range");
        assert!(!m.is_lossy);
    }

    #[test]
    fn test_postgres_from_canonical_arrays() {
        let conv = PostgresFromCanonical::new();

        let m = conv.from_canonical(&CanonicalType::Array(Box::new(CanonicalType::Int32)));
        assert_eq!(m.target_type, "integer[]");
        assert!(!m.is_lossy);

        let m = conv.from_canonical(&CanonicalType::Array(Box::new(CanonicalType::Text)));
        assert_eq!(m.target_type, "text[]");
        assert!(!m.is_lossy);
    }

    // --- MysqlFromCanonical Tests ---

    #[test]
    fn test_mysql_from_canonical_integers() {
        let conv = MysqlFromCanonical::new();
        assert_eq!(conv.dialect_name(), "mysql");

        let m = conv.from_canonical(&CanonicalType::Boolean);
        assert_eq!(m.target_type, "TINYINT(1)");

        let m = conv.from_canonical(&CanonicalType::Int16);
        assert_eq!(m.target_type, "SMALLINT");

        let m = conv.from_canonical(&CanonicalType::Int32);
        assert_eq!(m.target_type, "INT");

        let m = conv.from_canonical(&CanonicalType::Int64);
        assert_eq!(m.target_type, "BIGINT");
    }

    #[test]
    fn test_mysql_from_canonical_strings() {
        let conv = MysqlFromCanonical::new();

        let m = conv.from_canonical(&CanonicalType::Varchar(100));
        assert_eq!(m.target_type, "VARCHAR(100)");

        let m = conv.from_canonical(&CanonicalType::Varchar(0));
        assert_eq!(m.target_type, "LONGTEXT");

        let m = conv.from_canonical(&CanonicalType::Text);
        assert_eq!(m.target_type, "LONGTEXT");

        let m = conv.from_canonical(&CanonicalType::Char(50));
        assert_eq!(m.target_type, "CHAR(50)");
    }

    #[test]
    fn test_mysql_from_canonical_lossy_types() {
        let conv = MysqlFromCanonical::new();

        let m = conv.from_canonical(&CanonicalType::Uuid);
        assert_eq!(m.target_type, "VARCHAR(36)");
        assert!(m.is_lossy);

        let m = conv.from_canonical(&CanonicalType::DateTimeTz);
        assert_eq!(m.target_type, "DATETIME");
        assert!(m.is_lossy); // MySQL doesn't have timezone support

        let m = conv.from_canonical(&CanonicalType::Interval);
        assert_eq!(m.target_type, "VARCHAR(100)");
        assert!(m.is_lossy);
    }

    // --- ComposedMapper Integration Tests ---

    #[test]
    fn test_composed_mapper_mssql_to_postgres() {
        use std::sync::Arc;
        let mapper = super::super::canonical::ComposedMapper::new(
            Arc::new(MssqlToCanonical::new()),
            Arc::new(PostgresFromCanonical::new()),
        );

        assert_eq!(mapper.source_dialect(), "mssql");
        assert_eq!(mapper.target_dialect(), "postgres");

        // Integer mapping
        let m = mapper.map_type("int", 0, 0, 0);
        assert_eq!(m.target_type, "integer");
        assert!(!m.is_lossy);

        // String mapping
        let m = mapper.map_type("nvarchar", 255, 0, 0);
        assert_eq!(m.target_type, "varchar(255)");

        // UUID mapping
        let m = mapper.map_type("uniqueidentifier", 0, 0, 0);
        assert_eq!(m.target_type, "uuid");
        assert!(!m.is_lossy);
    }

    #[test]
    fn test_composed_mapper_postgres_to_mysql() {
        use std::sync::Arc;
        let mapper = super::super::canonical::ComposedMapper::new(
            Arc::new(PostgresToCanonical::new()),
            Arc::new(MysqlFromCanonical::new()),
        );

        assert_eq!(mapper.source_dialect(), "postgres");
        assert_eq!(mapper.target_dialect(), "mysql");

        // Integer mapping
        let m = mapper.map_type("integer", 0, 0, 0);
        assert_eq!(m.target_type, "INT");

        // UUID is lossy
        let m = mapper.map_type("uuid", 0, 0, 0);
        assert_eq!(m.target_type, "VARCHAR(36)");
        assert!(m.is_lossy);

        // JSON is lossless
        let m = mapper.map_type("json", 0, 0, 0);
        assert_eq!(m.target_type, "JSON");
        assert!(!m.is_lossy);
    }

    #[test]
    fn test_composed_mapper_mysql_to_mssql() {
        use std::sync::Arc;
        let mapper = super::super::canonical::ComposedMapper::new(
            Arc::new(MysqlToCanonical::new()),
            Arc::new(MssqlFromCanonical::new()),
        );

        assert_eq!(mapper.source_dialect(), "mysql");
        assert_eq!(mapper.target_dialect(), "mssql");

        // Integer mapping
        let m = mapper.map_type("int", 0, 0, 0);
        assert_eq!(m.target_type, "int");

        // TINYINT(1) -> boolean -> bit
        let m = mapper.map_type("tinyint", 1, 0, 0);
        assert_eq!(m.target_type, "bit");

        // JSON is lossy in MSSQL
        let m = mapper.map_type("json", 0, 0, 0);
        assert_eq!(m.target_type, "nvarchar(max)");
        assert!(m.is_lossy);
    }

    #[test]
    fn test_composed_mapper_column_mapping() {
        use std::sync::Arc;
        let mapper = super::super::canonical::ComposedMapper::new(
            Arc::new(MssqlToCanonical::new()),
            Arc::new(PostgresFromCanonical::new()),
        );

        let col = test_column("user_id", "uniqueidentifier", 0, 0, 0);
        let mapping = mapper.map_column(&col);

        assert_eq!(mapping.name, "user_id");
        assert_eq!(mapping.target_type, "uuid");
        assert!(mapping.is_nullable);
    }

    // --- Cross-dialect Round-trip Tests ---

    #[test]
    fn test_canonical_roundtrip_integers() {
        // Test that common types survive round-trips through canonical
        let types = [
            ("boolean", CanonicalType::Boolean, "boolean"),
            ("smallint", CanonicalType::Int16, "smallint"),
            ("integer", CanonicalType::Int32, "integer"),
            ("bigint", CanonicalType::Int64, "bigint"),
        ];

        let pg_to = PostgresToCanonical::new();
        let pg_from = PostgresFromCanonical::new();

        for (pg_type, expected_canonical, expected_back) in types {
            let info = pg_to.to_canonical(pg_type, 0, 0, 0);
            assert_eq!(
                info.canonical_type, expected_canonical,
                "Failed for type: {}",
                pg_type
            );

            let mapping = pg_from.from_canonical(&info.canonical_type);
            assert_eq!(
                mapping.target_type, expected_back,
                "Round-trip failed for type: {}",
                pg_type
            );
        }
    }

    #[test]
    fn test_canonical_all_dialects_handle_unknown() {
        let unknown_type = "totally_made_up_type";

        let mssql_to = MssqlToCanonical::new();
        let info = mssql_to.to_canonical(unknown_type, 0, 0, 0);
        assert!(matches!(info.canonical_type, CanonicalType::Unknown(_)));
        assert!(info.is_lossy);

        let pg_to = PostgresToCanonical::new();
        let info = pg_to.to_canonical(unknown_type, 0, 0, 0);
        assert!(matches!(info.canonical_type, CanonicalType::Unknown(_)));
        assert!(info.is_lossy);

        let mysql_to = MysqlToCanonical::new();
        let info = mysql_to.to_canonical(unknown_type, 0, 0, 0);
        assert!(matches!(info.canonical_type, CanonicalType::Unknown(_)));
        assert!(info.is_lossy);
    }

    #[test]
    fn test_canonical_precision_preservation() {
        let mssql_to = MssqlToCanonical::new();
        let pg_from = PostgresFromCanonical::new();

        // High precision decimal
        let info = mssql_to.to_canonical("decimal", 0, 28, 10);
        if let CanonicalType::Decimal { precision, scale } = info.canonical_type {
            assert_eq!(precision, 28);
            assert_eq!(scale, 10);

            let mapping = pg_from.from_canonical(&CanonicalType::Decimal { precision, scale });
            assert_eq!(mapping.target_type, "numeric(28,10)");
        } else {
            panic!("Expected Decimal canonical type");
        }
    }

    #[test]
    fn test_composed_mapper_lossy_warning_propagation() {
        use std::sync::Arc;
        let mapper = super::super::canonical::ComposedMapper::new(
            Arc::new(PostgresToCanonical::new()),
            Arc::new(MssqlFromCanonical::new()),
        );

        // JSONB is lossy in both directions
        let m = mapper.map_type("jsonb", 0, 0, 0);
        assert!(m.is_lossy);
        assert!(m.warning.is_some());
        // Warning should mention JSONB features
        assert!(
            m.warning.as_ref().unwrap().contains("JSONB")
                || m.warning.as_ref().unwrap().contains("JSON")
        );
    }
}
