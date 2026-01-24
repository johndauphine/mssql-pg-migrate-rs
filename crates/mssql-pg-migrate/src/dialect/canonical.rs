//! Hub-and-spoke canonical type system for database type mapping.
//!
//! This module provides a canonical (intermediate) type representation that enables
//! efficient type mapping between multiple databases. Instead of requiring n*(n-1)
//! direct mappers for n databases, this approach only needs 2n implementations:
//! - `ToCanonical`: Convert native type → canonical type
//! - `FromCanonical`: Convert canonical type → native type
//!
//! # Architecture
//!
//! ```text
//! Source DB  →  CanonicalType  →  Target DB
//!   MSSQL    →     Int32       →   MySQL
//!   PostgreSQL →   Int32       →   MySQL
//! ```
//!
//! # Benefits
//!
//! - **Scalability**: Adding a new database requires only 2 implementations
//! - **Maintainability**: Type mappings are centralized per database
//! - **Consistency**: Same canonical type always maps the same way
//! - **Testability**: Each direction can be tested independently

use std::sync::Arc;

use crate::core::schema::Column;
use crate::core::traits::{ColumnMapping, TypeMapper, TypeMapping};

/// Canonical type representation for cross-database type mapping.
///
/// These types represent a database-agnostic intermediate form that
/// can be mapped to/from any supported database dialect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonicalType {
    // ===== Boolean =====
    /// Boolean/bit type.
    Boolean,

    // ===== Integer Types =====
    /// 8-bit unsigned integer (0-255). Maps to TINYINT in MSSQL/MySQL.
    UInt8,
    /// 16-bit signed integer. Maps to SMALLINT.
    Int16,
    /// 32-bit signed integer. Maps to INT/INTEGER.
    Int32,
    /// 64-bit signed integer. Maps to BIGINT.
    Int64,

    // ===== Floating Point =====
    /// 32-bit floating point. Maps to REAL/FLOAT.
    Float32,
    /// 64-bit floating point. Maps to DOUBLE PRECISION/FLOAT.
    Float64,

    // ===== Decimal/Numeric =====
    /// Exact decimal with precision and scale.
    /// Precision is total digits, scale is digits after decimal point.
    Decimal {
        /// Total number of digits (1-65 for MySQL, 1-38 for MSSQL, unlimited for PG).
        precision: u8,
        /// Number of digits after decimal point.
        scale: u8,
    },
    /// Money type with fixed precision (19,4).
    Money,
    /// Small money type with fixed precision (10,4).
    SmallMoney,

    // ===== String Types =====
    /// Fixed-length character string.
    Char(u32),
    /// Variable-length character string with max length.
    /// 0 means unlimited/max.
    Varchar(u32),
    /// Unlimited text.
    Text,

    // ===== Binary Types =====
    /// Fixed-length binary data.
    Binary(u32),
    /// Variable-length binary data with max length.
    /// 0 means unlimited/max.
    Varbinary(u32),
    /// Unlimited binary data.
    Blob,

    // ===== Date/Time Types =====
    /// Date only (year, month, day).
    Date,
    /// Time only (hour, minute, second, fraction).
    Time,
    /// Date and time without timezone.
    DateTime,
    /// Date and time with timezone.
    DateTimeTz,
    /// Time interval/duration.
    Interval,
    /// Year (MySQL specific, stored as small integer).
    Year,

    // ===== Special Types =====
    /// UUID/GUID (128-bit identifier).
    Uuid,
    /// JSON data (text-based).
    Json,
    /// JSON data (binary, PostgreSQL JSONB).
    JsonBinary,
    /// XML data.
    Xml,

    // ===== Bit Strings =====
    /// Fixed-length bit string.
    Bit(u32),
    /// Variable-length bit string.
    VarBit(u32),

    // ===== Network Types =====
    /// IP address (IPv4 or IPv6).
    InetAddr,
    /// CIDR network address.
    CidrAddr,
    /// MAC address.
    MacAddr,

    // ===== Geometric Types =====
    /// Point coordinates.
    Point,
    /// Line or line segment.
    Line,
    /// Polygon.
    Polygon,
    /// Generic geometry.
    Geometry,
    /// Geographic data.
    Geography,

    // ===== Full-Text Search =====
    /// Text search vector (PostgreSQL tsvector).
    TsVector,
    /// Text search query (PostgreSQL tsquery).
    TsQuery,

    // ===== Range Types =====
    /// Integer range.
    Int4Range,
    /// Bigint range.
    Int8Range,
    /// Numeric range.
    NumRange,
    /// Timestamp range.
    TsRange,
    /// Timestamp with timezone range.
    TsTzRange,
    /// Date range.
    DateRange,

    // ===== Array Types =====
    /// Array of another type.
    Array(Box<CanonicalType>),

    // ===== MySQL-Specific =====
    /// MySQL ENUM type with values.
    Enum(Vec<String>),
    /// MySQL SET type with values.
    Set(Vec<String>),

    // ===== Fallback =====
    /// Unknown type that couldn't be mapped.
    /// Contains the original type name for error messages.
    Unknown(String),
}

impl std::fmt::Display for CanonicalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CanonicalType::Boolean => write!(f, "Boolean"),
            CanonicalType::UInt8 => write!(f, "UInt8"),
            CanonicalType::Int16 => write!(f, "Int16"),
            CanonicalType::Int32 => write!(f, "Int32"),
            CanonicalType::Int64 => write!(f, "Int64"),
            CanonicalType::Float32 => write!(f, "Float32"),
            CanonicalType::Float64 => write!(f, "Float64"),
            CanonicalType::Decimal { precision, scale } => {
                write!(f, "Decimal({},{})", precision, scale)
            }
            CanonicalType::Money => write!(f, "Money"),
            CanonicalType::SmallMoney => write!(f, "SmallMoney"),
            CanonicalType::Char(n) => write!(f, "Char({})", n),
            CanonicalType::Varchar(n) => write!(f, "Varchar({})", n),
            CanonicalType::Text => write!(f, "Text"),
            CanonicalType::Binary(n) => write!(f, "Binary({})", n),
            CanonicalType::Varbinary(n) => write!(f, "Varbinary({})", n),
            CanonicalType::Blob => write!(f, "Blob"),
            CanonicalType::Date => write!(f, "Date"),
            CanonicalType::Time => write!(f, "Time"),
            CanonicalType::DateTime => write!(f, "DateTime"),
            CanonicalType::DateTimeTz => write!(f, "DateTimeTz"),
            CanonicalType::Interval => write!(f, "Interval"),
            CanonicalType::Year => write!(f, "Year"),
            CanonicalType::Uuid => write!(f, "Uuid"),
            CanonicalType::Json => write!(f, "Json"),
            CanonicalType::JsonBinary => write!(f, "JsonBinary"),
            CanonicalType::Xml => write!(f, "Xml"),
            CanonicalType::Bit(n) => write!(f, "Bit({})", n),
            CanonicalType::VarBit(n) => write!(f, "VarBit({})", n),
            CanonicalType::InetAddr => write!(f, "InetAddr"),
            CanonicalType::CidrAddr => write!(f, "CidrAddr"),
            CanonicalType::MacAddr => write!(f, "MacAddr"),
            CanonicalType::Point => write!(f, "Point"),
            CanonicalType::Line => write!(f, "Line"),
            CanonicalType::Polygon => write!(f, "Polygon"),
            CanonicalType::Geometry => write!(f, "Geometry"),
            CanonicalType::Geography => write!(f, "Geography"),
            CanonicalType::TsVector => write!(f, "TsVector"),
            CanonicalType::TsQuery => write!(f, "TsQuery"),
            CanonicalType::Int4Range => write!(f, "Int4Range"),
            CanonicalType::Int8Range => write!(f, "Int8Range"),
            CanonicalType::NumRange => write!(f, "NumRange"),
            CanonicalType::TsRange => write!(f, "TsRange"),
            CanonicalType::TsTzRange => write!(f, "TsTzRange"),
            CanonicalType::DateRange => write!(f, "DateRange"),
            CanonicalType::Array(inner) => write!(f, "Array({})", inner),
            CanonicalType::Enum(vals) => write!(f, "Enum({:?})", vals),
            CanonicalType::Set(vals) => write!(f, "Set({:?})", vals),
            CanonicalType::Unknown(name) => write!(f, "Unknown({})", name),
        }
    }
}

/// Result of converting a native type to canonical form.
#[derive(Debug, Clone)]
pub struct CanonicalTypeInfo {
    /// The canonical type representation.
    pub canonical_type: CanonicalType,
    /// Whether information was lost in the conversion to canonical.
    pub is_lossy: bool,
    /// Warning message if the conversion is lossy.
    pub warning: Option<String>,
}

impl CanonicalTypeInfo {
    /// Create a lossless canonical type conversion.
    pub fn lossless(canonical_type: CanonicalType) -> Self {
        Self {
            canonical_type,
            is_lossy: false,
            warning: None,
        }
    }

    /// Create a lossy canonical type conversion with a warning.
    pub fn lossy(canonical_type: CanonicalType, warning: impl Into<String>) -> Self {
        Self {
            canonical_type,
            is_lossy: true,
            warning: Some(warning.into()),
        }
    }
}

/// Convert native database types to canonical types.
///
/// Each database dialect implements this trait to convert its native
/// types to the canonical intermediate representation.
pub trait ToCanonical: Send + Sync {
    /// Get the dialect name (e.g., "mssql", "postgres", "mysql").
    fn dialect_name(&self) -> &str;

    /// Convert a native type to canonical form.
    ///
    /// # Arguments
    ///
    /// * `data_type` - The native type name (e.g., "varchar", "int4")
    /// * `max_length` - Maximum length for string/binary types (-1 for MAX)
    /// * `precision` - Precision for decimal types
    /// * `scale` - Scale for decimal types
    ///
    /// # Returns
    ///
    /// A `CanonicalTypeInfo` containing the canonical type and any warnings.
    fn to_canonical(
        &self,
        data_type: &str,
        max_length: i32,
        precision: i32,
        scale: i32,
    ) -> CanonicalTypeInfo;
}

/// Convert canonical types to native database types.
///
/// Each database dialect implements this trait to convert canonical
/// types to its native type representation.
#[allow(clippy::wrong_self_convention)]
pub trait FromCanonical: Send + Sync {
    /// Get the dialect name (e.g., "mssql", "postgres", "mysql").
    fn dialect_name(&self) -> &str;

    /// Convert a canonical type to native form.
    ///
    /// # Arguments
    ///
    /// * `canonical` - The canonical type to convert
    ///
    /// # Returns
    ///
    /// A `TypeMapping` containing the native type string and any warnings.
    fn from_canonical(&self, canonical: &CanonicalType) -> TypeMapping;
}

/// Composed type mapper that chains ToCanonical and FromCanonical conversions.
///
/// This is the main type mapper used by the migration engine. It converts
/// source types to canonical form, then from canonical to target form.
///
/// # Example
///
/// ```rust,ignore
/// let mapper = ComposedMapper::new(
///     Arc::new(MssqlToCanonical),
///     Arc::new(MysqlFromCanonical),
/// );
/// let mapping = mapper.map_type("uniqueidentifier", 0, 0, 0);
/// assert_eq!(mapping.target_type, "VARCHAR(36)");
/// ```
pub struct ComposedMapper {
    /// Converts source types to canonical.
    source_converter: Arc<dyn ToCanonical>,
    /// Converts canonical types to target.
    target_converter: Arc<dyn FromCanonical>,
}

impl ComposedMapper {
    /// Create a new composed mapper.
    ///
    /// # Arguments
    ///
    /// * `source_converter` - Converts source dialect types to canonical
    /// * `target_converter` - Converts canonical types to target dialect
    pub fn new(
        source_converter: Arc<dyn ToCanonical>,
        target_converter: Arc<dyn FromCanonical>,
    ) -> Self {
        Self {
            source_converter,
            target_converter,
        }
    }

    /// Get the source dialect name.
    pub fn source_dialect_name(&self) -> &str {
        self.source_converter.dialect_name()
    }

    /// Get the target dialect name.
    pub fn target_dialect_name(&self) -> &str {
        self.target_converter.dialect_name()
    }
}

impl std::fmt::Debug for ComposedMapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComposedMapper")
            .field("source", &self.source_converter.dialect_name())
            .field("target", &self.target_converter.dialect_name())
            .finish()
    }
}

impl TypeMapper for ComposedMapper {
    fn source_dialect(&self) -> &str {
        self.source_converter.dialect_name()
    }

    fn target_dialect(&self) -> &str {
        self.target_converter.dialect_name()
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
        // Step 1: Source type → Canonical
        let canonical_info = self
            .source_converter
            .to_canonical(data_type, max_length, precision, scale);

        // Step 2: Canonical → Target type
        let mut target_mapping = self
            .target_converter
            .from_canonical(&canonical_info.canonical_type);

        // Combine lossy flags and warnings from both steps
        if canonical_info.is_lossy {
            target_mapping.is_lossy = true;
            // Merge warnings
            match (&canonical_info.warning, &target_mapping.warning) {
                (Some(src_warn), Some(tgt_warn)) => {
                    target_mapping.warning = Some(format!("{} {}", src_warn, tgt_warn));
                }
                (Some(src_warn), None) => {
                    target_mapping.warning = Some(src_warn.clone());
                }
                (None, _) => {
                    // Keep target warning as-is
                }
            }
        }

        target_mapping
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canonical_type_display() {
        assert_eq!(format!("{}", CanonicalType::Int32), "Int32");
        assert_eq!(
            format!(
                "{}",
                CanonicalType::Decimal {
                    precision: 10,
                    scale: 2
                }
            ),
            "Decimal(10,2)"
        );
        assert_eq!(format!("{}", CanonicalType::Varchar(255)), "Varchar(255)");
        assert_eq!(
            format!("{}", CanonicalType::Array(Box::new(CanonicalType::Int32))),
            "Array(Int32)"
        );
    }

    #[test]
    fn test_canonical_type_info_lossless() {
        let info = CanonicalTypeInfo::lossless(CanonicalType::Int32);
        assert_eq!(info.canonical_type, CanonicalType::Int32);
        assert!(!info.is_lossy);
        assert!(info.warning.is_none());
    }

    #[test]
    fn test_canonical_type_info_lossy() {
        let info =
            CanonicalTypeInfo::lossy(CanonicalType::Json, "Binary JSON features unavailable");
        assert_eq!(info.canonical_type, CanonicalType::Json);
        assert!(info.is_lossy);
        assert_eq!(
            info.warning.as_deref(),
            Some("Binary JSON features unavailable")
        );
    }

    // Mock converters for testing ComposedMapper
    struct MockToCanonical;
    impl ToCanonical for MockToCanonical {
        fn dialect_name(&self) -> &str {
            "mock_source"
        }

        fn to_canonical(
            &self,
            data_type: &str,
            _max_length: i32,
            _precision: i32,
            _scale: i32,
        ) -> CanonicalTypeInfo {
            match data_type.to_lowercase().as_str() {
                "int" => CanonicalTypeInfo::lossless(CanonicalType::Int32),
                "jsonb" => CanonicalTypeInfo::lossy(
                    CanonicalType::Json,
                    "JSONB binary features unavailable.",
                ),
                _ => CanonicalTypeInfo::lossless(CanonicalType::Text),
            }
        }
    }

    struct MockFromCanonical;
    impl FromCanonical for MockFromCanonical {
        fn dialect_name(&self) -> &str {
            "mock_target"
        }

        fn from_canonical(&self, canonical: &CanonicalType) -> TypeMapping {
            match canonical {
                CanonicalType::Int32 => TypeMapping::lossless("INTEGER"),
                CanonicalType::Json => TypeMapping::lossless("JSON"),
                CanonicalType::Text => TypeMapping::lossless("TEXT"),
                _ => TypeMapping::lossy("TEXT", "Unsupported type"),
            }
        }
    }

    #[test]
    fn test_composed_mapper_lossless() {
        let mapper = ComposedMapper::new(Arc::new(MockToCanonical), Arc::new(MockFromCanonical));

        assert_eq!(mapper.source_dialect(), "mock_source");
        assert_eq!(mapper.target_dialect(), "mock_target");

        let mapping = mapper.map_type("int", 0, 0, 0);
        assert_eq!(mapping.target_type, "INTEGER");
        assert!(!mapping.is_lossy);
        assert!(mapping.warning.is_none());
    }

    #[test]
    fn test_composed_mapper_lossy_from_source() {
        let mapper = ComposedMapper::new(Arc::new(MockToCanonical), Arc::new(MockFromCanonical));

        let mapping = mapper.map_type("jsonb", 0, 0, 0);
        assert_eq!(mapping.target_type, "JSON");
        assert!(mapping.is_lossy);
        assert!(mapping.warning.is_some());
        assert!(mapping.warning.unwrap().contains("JSONB"));
    }

    #[test]
    fn test_composed_mapper_map_column() {
        let mapper = ComposedMapper::new(Arc::new(MockToCanonical), Arc::new(MockFromCanonical));

        let col = Column {
            name: "id".to_string(),
            data_type: "int".to_string(),
            max_length: 0,
            precision: 0,
            scale: 0,
            is_nullable: false,
            is_identity: true,
            ordinal_pos: 1,
        };

        let mapping = mapper.map_column(&col);
        assert_eq!(mapping.name, "id");
        assert_eq!(mapping.target_type, "INTEGER");
        assert!(!mapping.is_nullable);
        assert!(mapping.warning.is_none());
    }
}
