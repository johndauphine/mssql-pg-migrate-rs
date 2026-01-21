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
        "timetz" | "time with time zone" => TypeMapping::lossy(
            "TIME",
            "time with time zone loses timezone info in MySQL.",
        ),
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
                    format!("Precision {} exceeds MSSQL max of 38. Truncated.", precision),
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
fn mssql_to_mysql(mssql_type: &str, max_length: i32, precision: i32, scale: i32) -> TypeMapping {
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
                    format!("Precision {} exceeds MySQL max of 65. Truncated.", precision),
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
}
