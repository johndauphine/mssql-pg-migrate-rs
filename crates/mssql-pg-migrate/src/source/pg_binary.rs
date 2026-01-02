//! PostgreSQL COPY BINARY format parser.
//!
//! Parses PostgreSQL's binary COPY format for high-performance data reads.
//! Binary format is significantly faster than text format because:
//! - No string parsing overhead for numeric types
//! - No character encoding validation for non-text data
//! - Fixed-size types are read directly without parsing
//!
//! Binary format specification:
//! https://www.postgresql.org/docs/current/sql-copy.html#id-1.9.3.55.9.4.5
//!
//! Header: PGCOPY\n\xff\r\n\0 (11 bytes) + flags (4 bytes) + ext_len (4 bytes)
//! Each row: field_count (2 bytes) + [field_len (4 bytes) + data]*
//! Trailer: -1 (2 bytes as field_count)

use bytes::{Buf, BytesMut};

use crate::error::{MigrateError, Result};
use crate::target::{SqlNullType, SqlValue};

/// PostgreSQL COPY binary header signature.
const PG_COPY_SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";

/// Minimum header size: signature (11) + flags (4) + extension length (4).
const HEADER_SIZE: usize = 19;

/// Column type for binary parsing.
///
/// Maps PostgreSQL OIDs to parsing strategies.
#[derive(Clone, Debug, PartialEq)]
pub enum BinaryColumnType {
    Bool,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Numeric,
    Uuid,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Text, // Also covers varchar, char, etc.
    Bytea,
    Json,
    Jsonb,
}

impl BinaryColumnType {
    /// Convert PostgreSQL type name to BinaryColumnType.
    pub fn from_pg_type(type_name: &str) -> Self {
        let lower = type_name.to_lowercase();
        match lower.as_str() {
            "bool" | "boolean" => BinaryColumnType::Bool,
            "int2" | "smallint" => BinaryColumnType::Int16,
            "int4" | "integer" | "int" => BinaryColumnType::Int32,
            "int8" | "bigint" => BinaryColumnType::Int64,
            "float4" | "real" => BinaryColumnType::Float32,
            "float8" | "double precision" => BinaryColumnType::Float64,
            "numeric" | "decimal" => BinaryColumnType::Numeric,
            "uuid" => BinaryColumnType::Uuid,
            "date" => BinaryColumnType::Date,
            "time" | "time without time zone" => BinaryColumnType::Time,
            "timestamp" | "timestamp without time zone" => BinaryColumnType::Timestamp,
            "timestamptz" | "timestamp with time zone" => BinaryColumnType::TimestampTz,
            "bytea" => BinaryColumnType::Bytea,
            "json" => BinaryColumnType::Json,
            "jsonb" => BinaryColumnType::Jsonb,
            // Default to text for varchar, char, text, etc.
            _ => BinaryColumnType::Text,
        }
    }

    /// Get the corresponding SqlNullType for this column type.
    fn to_null_type(&self) -> SqlNullType {
        match self {
            BinaryColumnType::Bool => SqlNullType::Bool,
            BinaryColumnType::Int16 => SqlNullType::I16,
            BinaryColumnType::Int32 => SqlNullType::I32,
            BinaryColumnType::Int64 => SqlNullType::I64,
            BinaryColumnType::Float32 => SqlNullType::F32,
            BinaryColumnType::Float64 => SqlNullType::F64,
            BinaryColumnType::Numeric => SqlNullType::Decimal,
            BinaryColumnType::Uuid => SqlNullType::Uuid,
            BinaryColumnType::Date => SqlNullType::Date,
            BinaryColumnType::Time => SqlNullType::Time,
            BinaryColumnType::Timestamp => SqlNullType::DateTime,
            BinaryColumnType::TimestampTz => SqlNullType::DateTimeOffset,
            BinaryColumnType::Text | BinaryColumnType::Json | BinaryColumnType::Jsonb => {
                SqlNullType::String
            }
            BinaryColumnType::Bytea => SqlNullType::Bytes,
        }
    }
}

/// PostgreSQL COPY BINARY format parser.
///
/// This parser is designed to work with streaming data from `tokio-postgres::copy_out`.
/// It accumulates bytes and parses complete rows as they become available.
pub struct BinaryRowParser {
    buffer: BytesMut,
    column_types: Vec<BinaryColumnType>,
    header_parsed: bool,
    complete: bool,
}

impl BinaryRowParser {
    /// Create a new binary row parser with the expected column types.
    pub fn new(column_types: Vec<BinaryColumnType>) -> Self {
        Self {
            buffer: BytesMut::with_capacity(64 * 1024), // 64KB initial buffer
            column_types,
            header_parsed: false,
            complete: false,
        }
    }

    /// Create from PostgreSQL type names (convenience constructor).
    pub fn from_type_names(type_names: &[String]) -> Self {
        let column_types = type_names
            .iter()
            .map(|t| BinaryColumnType::from_pg_type(t))
            .collect();
        Self::new(column_types)
    }

    /// Append more data from the COPY stream.
    pub fn extend(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Number of bytes currently buffered.
    pub fn buffered_len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if we've reached the end of the COPY stream.
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    /// Try to parse the next row from the buffer.
    ///
    /// Returns:
    /// - `Ok(Some(row))` if a complete row was parsed
    /// - `Ok(None)` if more data is needed
    /// - `Err(...)` if parsing failed
    pub fn next_row(&mut self) -> Result<Option<Vec<SqlValue>>> {
        // Parse header first if not yet done
        if !self.header_parsed {
            if !self.try_parse_header()? {
                return Ok(None); // Need more data for header
            }
        }

        // Check for trailer (end of data)
        if self.buffer.len() >= 2 {
            // Peek at field count without consuming
            let field_count = i16::from_be_bytes([self.buffer[0], self.buffer[1]]);
            if field_count == -1 {
                self.complete = true;
                self.buffer.advance(2);
                return Ok(None);
            }
        } else {
            return Ok(None); // Need more data
        }

        // Try to parse a complete row
        self.try_parse_row()
    }

    /// Try to parse the header. Returns true if successful.
    fn try_parse_header(&mut self) -> Result<bool> {
        if self.buffer.len() < HEADER_SIZE {
            return Ok(false);
        }

        // Verify signature
        if &self.buffer[..11] != PG_COPY_SIGNATURE {
            return Err(MigrateError::SchemaExtraction(
                "Invalid PostgreSQL COPY binary signature".into(),
            ));
        }

        // Skip signature
        self.buffer.advance(11);

        // Read flags (4 bytes) - we don't use these but must consume them
        let _flags = self.buffer.get_u32();

        // Read extension length
        let ext_len = self.buffer.get_u32() as usize;

        // Skip extension data if present
        if self.buffer.len() < ext_len {
            return Err(MigrateError::SchemaExtraction(format!(
                "Incomplete COPY header extension: need {} bytes, have {}",
                ext_len,
                self.buffer.len()
            )));
        }
        self.buffer.advance(ext_len);

        self.header_parsed = true;
        Ok(true)
    }

    /// Try to parse a complete row. Returns None if more data needed.
    fn try_parse_row(&mut self) -> Result<Option<Vec<SqlValue>>> {
        // We need to peek ahead to see if we have a complete row
        // without consuming data until we're sure.
        let required_len = self.calculate_row_length()?;
        match required_len {
            Some(len) if self.buffer.len() >= len => {
                // We have enough data - parse the row
                Ok(Some(self.parse_row_data()?))
            }
            Some(_) => Ok(None), // Not enough data yet
            None => Ok(None),    // Can't determine length yet
        }
    }

    /// Calculate the length of the next row, or None if we can't determine yet.
    fn calculate_row_length(&self) -> Result<Option<usize>> {
        if self.buffer.len() < 2 {
            return Ok(None);
        }

        let field_count = i16::from_be_bytes([self.buffer[0], self.buffer[1]]);

        if field_count == -1 {
            return Ok(Some(2)); // Trailer
        }

        if field_count as usize != self.column_types.len() {
            return Err(MigrateError::SchemaExtraction(format!(
                "Column count mismatch: expected {}, got {}",
                self.column_types.len(),
                field_count
            )));
        }

        // Calculate total length: 2 (field count) + for each field: 4 (length) + data
        let mut offset = 2usize;
        for _ in 0..field_count {
            if self.buffer.len() < offset + 4 {
                return Ok(None); // Need more data to read field length
            }

            let field_len = i32::from_be_bytes([
                self.buffer[offset],
                self.buffer[offset + 1],
                self.buffer[offset + 2],
                self.buffer[offset + 3],
            ]);
            offset += 4;

            if field_len >= 0 {
                offset += field_len as usize;
            }
            // -1 means NULL, no data follows
        }

        Ok(Some(offset))
    }

    /// Parse a row after we've verified we have enough data.
    fn parse_row_data(&mut self) -> Result<Vec<SqlValue>> {
        let field_count = self.buffer.get_i16() as usize;

        let mut values = Vec::with_capacity(field_count);

        // Use index-based iteration to avoid borrowing self.column_types while also borrowing self.buffer
        for i in 0..self.column_types.len() {
            let field_len = self.buffer.get_i32();
            let col_type = self.column_types[i].clone();

            let value = if field_len == -1 {
                SqlValue::Null(col_type.to_null_type())
            } else {
                self.parse_field(&col_type, field_len as usize)?
            };

            values.push(value);
        }

        Ok(values)
    }

    /// Parse a single field value based on its type.
    fn parse_field(&mut self, col_type: &BinaryColumnType, len: usize) -> Result<SqlValue> {
        let value = match col_type {
            BinaryColumnType::Bool => {
                let b = self.buffer.get_u8();
                SqlValue::Bool(b != 0)
            }

            BinaryColumnType::Int16 => {
                let n = self.buffer.get_i16();
                SqlValue::I16(n)
            }

            BinaryColumnType::Int32 => {
                let n = self.buffer.get_i32();
                SqlValue::I32(n)
            }

            BinaryColumnType::Int64 => {
                let n = self.buffer.get_i64();
                SqlValue::I64(n)
            }

            BinaryColumnType::Float32 => {
                let n = self.buffer.get_f32();
                SqlValue::F32(n)
            }

            BinaryColumnType::Float64 => {
                let n = self.buffer.get_f64();
                SqlValue::F64(n)
            }

            BinaryColumnType::Uuid => {
                let mut uuid_bytes = [0u8; 16];
                self.buffer.copy_to_slice(&mut uuid_bytes);
                SqlValue::Uuid(uuid::Uuid::from_bytes(uuid_bytes))
            }

            BinaryColumnType::Text | BinaryColumnType::Json | BinaryColumnType::Jsonb => {
                let bytes = self.buffer.split_to(len);
                // Note: JSONB has a version byte prefix (0x01), but we treat it as text
                // since SqlValue::String handles both JSON and JSONB as strings.
                let s = String::from_utf8_lossy(&bytes).into_owned();
                SqlValue::String(s)
            }

            BinaryColumnType::Bytea => {
                let bytes = self.buffer.split_to(len);
                SqlValue::Bytes(bytes.to_vec())
            }

            BinaryColumnType::Date => {
                // PostgreSQL date: days since 2000-01-01
                let days = self.buffer.get_i32();
                let epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
                let date = epoch + chrono::Duration::days(days as i64);
                SqlValue::Date(date)
            }

            BinaryColumnType::Time => {
                // PostgreSQL time: microseconds since midnight
                let micros = self.buffer.get_i64();
                let secs = (micros / 1_000_000) as u32;
                let nanos = ((micros % 1_000_000) * 1000) as u32;
                let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
                    .unwrap_or_else(chrono::NaiveTime::default);
                SqlValue::Time(time)
            }

            BinaryColumnType::Timestamp => {
                // PostgreSQL timestamp: microseconds since 2000-01-01 00:00:00
                let micros = self.buffer.get_i64();
                const PG_EPOCH_MICROS: i64 = 946_684_800_000_000; // 2000-01-01 in Unix micros
                let unix_micros = micros + PG_EPOCH_MICROS;
                let secs = unix_micros / 1_000_000;
                let nsecs = ((unix_micros % 1_000_000) * 1000) as u32;
                let dt =
                    chrono::DateTime::from_timestamp(secs, nsecs).map(|dt| dt.naive_utc());
                SqlValue::DateTime(dt.unwrap_or_default())
            }

            BinaryColumnType::TimestampTz => {
                // PostgreSQL timestamptz: also microseconds since 2000-01-01 00:00:00 UTC
                let micros = self.buffer.get_i64();
                const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;
                let unix_micros = micros + PG_EPOCH_MICROS;
                let secs = unix_micros / 1_000_000;
                let nsecs = ((unix_micros % 1_000_000) * 1000) as u32;
                let dt = chrono::DateTime::from_timestamp(secs, nsecs)
                    .map(|dt| dt.fixed_offset());
                SqlValue::DateTimeOffset(dt.unwrap_or_else(|| {
                    chrono::DateTime::<chrono::FixedOffset>::from(
                        chrono::DateTime::UNIX_EPOCH
                    )
                }))
            }

            BinaryColumnType::Numeric => {
                self.parse_numeric(len)?
            }
        };

        Ok(value)
    }

    /// Parse PostgreSQL NUMERIC binary format.
    ///
    /// Format:
    /// - int16 ndigits: number of base-10000 digits
    /// - int16 weight: position of first digit (exponent in base-10000)
    /// - int16 sign: 0x0000 = positive, 0x4000 = negative, 0xC000 = NaN
    /// - int16 dscale: display scale (digits after decimal point)
    /// - int16[] digits: base-10000 digits, most significant first
    fn parse_numeric(&mut self, len: usize) -> Result<SqlValue> {
        if len < 8 {
            // Minimum: ndigits(2) + weight(2) + sign(2) + dscale(2)
            return Err(MigrateError::SchemaExtraction(format!(
                "Invalid NUMERIC length: {} (minimum 8)",
                len
            )));
        }

        let ndigits = self.buffer.get_i16() as usize;
        let weight = self.buffer.get_i16();
        let sign = self.buffer.get_u16();
        let dscale = self.buffer.get_i16();

        const NUMERIC_NEG: u16 = 0x4000;
        const NUMERIC_NAN: u16 = 0xC000;

        if sign == NUMERIC_NAN {
            // Return as zero for NaN (rust_decimal doesn't support NaN)
            return Ok(SqlValue::Decimal(rust_decimal::Decimal::ZERO));
        }

        if ndigits == 0 {
            // Zero value
            return Ok(SqlValue::Decimal(rust_decimal::Decimal::ZERO));
        }

        // Read base-10000 digits
        let mut digits = Vec::with_capacity(ndigits);
        for _ in 0..ndigits {
            digits.push(self.buffer.get_i16());
        }

        // Convert base-10000 to decimal string
        // Weight indicates position of first digit: weight=0 means 0.xxxx, weight=1 means xxxx.xxxx
        let mut result = String::new();

        // Calculate where decimal point goes
        // weight = n means first digit is 10000^n
        // So for weight=0, first digit is ones place (0-9999)
        // For weight=1, first digit is 10000s place
        // For weight=-1, first digit is 0.0001-0.9999

        let total_base10000_positions = weight + 1; // How many base-10000 positions before decimal

        // Build integer part
        let int_positions = total_base10000_positions.max(0) as usize;
        for i in 0..int_positions {
            let digit = if i < ndigits { digits[i] } else { 0 };
            if i == 0 {
                // First digit: no leading zeros
                result.push_str(&digit.to_string());
            } else {
                // Subsequent digits: pad to 4 digits
                result.push_str(&format!("{:04}", digit));
            }
        }

        if result.is_empty() {
            result.push('0');
        }

        // Build fractional part if we have scale
        if dscale > 0 {
            result.push('.');

            // How many leading zeros before our digits?
            let leading_zero_groups = if weight < -1 { (-weight - 1) as usize } else { 0 };

            for _ in 0..leading_zero_groups {
                result.push_str("0000");
            }

            // Add remaining digits
            let frac_start = int_positions;
            for i in frac_start..ndigits {
                result.push_str(&format!("{:04}", digits[i]));
            }

            // Trim to dscale
            let decimal_pos = result.find('.').unwrap();
            let desired_len = decimal_pos + 1 + dscale as usize;
            if result.len() > desired_len {
                result.truncate(desired_len);
            } else {
                // Pad with zeros if needed
                while result.len() < desired_len {
                    result.push('0');
                }
            }
        }

        // Parse as Decimal
        let mut decimal: rust_decimal::Decimal = result
            .parse()
            .map_err(|e| MigrateError::SchemaExtraction(format!("Invalid NUMERIC: {}", e)))?;

        if sign == NUMERIC_NEG {
            decimal.set_sign_negative(true);
        }

        Ok(SqlValue::Decimal(decimal))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal COPY header for testing.
    fn build_test_header() -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(PG_COPY_SIGNATURE);
        buf.extend_from_slice(&0u32.to_be_bytes()); // flags
        buf.extend_from_slice(&0u32.to_be_bytes()); // extension length
        buf
    }

    /// Build a row with the given field values.
    fn build_test_row(fields: &[Option<&[u8]>]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(fields.len() as i16).to_be_bytes()); // field count

        for field in fields {
            match field {
                Some(data) => {
                    buf.extend_from_slice(&(data.len() as i32).to_be_bytes());
                    buf.extend_from_slice(data);
                }
                None => {
                    buf.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
                }
            }
        }

        buf
    }

    /// Build the trailer.
    fn build_trailer() -> Vec<u8> {
        vec![0xFF, 0xFF] // -1 as i16
    }

    #[test]
    fn test_parse_header() {
        let col_types = vec![BinaryColumnType::Int32];
        let mut parser = BinaryRowParser::new(col_types);

        let header = build_test_header();
        parser.extend(&header);

        // Should parse header successfully
        let result = parser.next_row().unwrap();
        assert!(result.is_none()); // No row yet, but header parsed

        // Extend with trailer
        parser.extend(&build_trailer());
        let result = parser.next_row().unwrap();
        assert!(result.is_none()); // Trailer, no more rows
        assert!(parser.is_complete());
    }

    #[test]
    fn test_parse_int32() {
        let col_types = vec![BinaryColumnType::Int32];
        let mut parser = BinaryRowParser::new(col_types);

        let mut data = build_test_header();
        data.extend(build_test_row(&[Some(&42i32.to_be_bytes())]));
        data.extend(build_trailer());

        parser.extend(&data);

        let row = parser.next_row().unwrap().unwrap();
        assert_eq!(row.len(), 1);
        assert_eq!(row[0], SqlValue::I32(42));

        assert!(parser.next_row().unwrap().is_none());
        assert!(parser.is_complete());
    }

    #[test]
    fn test_parse_null() {
        let col_types = vec![BinaryColumnType::Int32, BinaryColumnType::Text];
        let mut parser = BinaryRowParser::new(col_types);

        let mut data = build_test_header();
        data.extend(build_test_row(&[None, Some(b"hello")]));
        data.extend(build_trailer());

        parser.extend(&data);

        let row = parser.next_row().unwrap().unwrap();
        assert_eq!(row.len(), 2);
        assert_eq!(row[0], SqlValue::Null(SqlNullType::I32));
        assert_eq!(row[1], SqlValue::String("hello".into()));
    }

    #[test]
    fn test_parse_multiple_rows() {
        let col_types = vec![BinaryColumnType::Int64, BinaryColumnType::Bool];
        let mut parser = BinaryRowParser::new(col_types);

        let mut data = build_test_header();
        data.extend(build_test_row(&[
            Some(&100i64.to_be_bytes()),
            Some(&[1u8]),
        ]));
        data.extend(build_test_row(&[
            Some(&200i64.to_be_bytes()),
            Some(&[0u8]),
        ]));
        data.extend(build_trailer());

        parser.extend(&data);

        let row1 = parser.next_row().unwrap().unwrap();
        assert_eq!(row1[0], SqlValue::I64(100));
        assert_eq!(row1[1], SqlValue::Bool(true));

        let row2 = parser.next_row().unwrap().unwrap();
        assert_eq!(row2[0], SqlValue::I64(200));
        assert_eq!(row2[1], SqlValue::Bool(false));

        assert!(parser.next_row().unwrap().is_none());
        assert!(parser.is_complete());
    }

    #[test]
    fn test_parse_uuid() {
        let col_types = vec![BinaryColumnType::Uuid];
        let mut parser = BinaryRowParser::new(col_types);

        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();

        let mut data = build_test_header();
        data.extend(build_test_row(&[Some(uuid.as_bytes())]));
        data.extend(build_trailer());

        parser.extend(&data);

        let row = parser.next_row().unwrap().unwrap();
        assert_eq!(row[0], SqlValue::Uuid(uuid));
    }

    #[test]
    fn test_parse_date() {
        let col_types = vec![BinaryColumnType::Date];
        let mut parser = BinaryRowParser::new(col_types);

        // Days since 2000-01-01: e.g., 1 = 2000-01-02
        let days: i32 = 1;

        let mut data = build_test_header();
        data.extend(build_test_row(&[Some(&days.to_be_bytes())]));
        data.extend(build_trailer());

        parser.extend(&data);

        let row = parser.next_row().unwrap().unwrap();
        let expected = chrono::NaiveDate::from_ymd_opt(2000, 1, 2).unwrap();
        assert_eq!(row[0], SqlValue::Date(expected));
    }

    #[test]
    fn test_incremental_parsing() {
        // Test that parser correctly handles data arriving in chunks
        let col_types = vec![BinaryColumnType::Int32];
        let mut parser = BinaryRowParser::new(col_types);

        let mut data = build_test_header();
        data.extend(build_test_row(&[Some(&12345i32.to_be_bytes())]));
        data.extend(build_trailer());

        // Feed data byte by byte
        for byte in data {
            parser.extend(&[byte]);
            // Keep trying to parse - should only succeed when we have enough data
            while let Some(row) = parser.next_row().unwrap() {
                assert_eq!(row[0], SqlValue::I32(12345));
            }
        }

        assert!(parser.is_complete());
    }

    #[test]
    fn test_from_type_names() {
        let type_names = vec![
            "int4".to_string(),
            "text".to_string(),
            "boolean".to_string(),
            "uuid".to_string(),
        ];

        let parser = BinaryRowParser::from_type_names(&type_names);

        assert_eq!(parser.column_types.len(), 4);
        assert_eq!(parser.column_types[0], BinaryColumnType::Int32);
        assert_eq!(parser.column_types[1], BinaryColumnType::Text);
        assert_eq!(parser.column_types[2], BinaryColumnType::Bool);
        assert_eq!(parser.column_types[3], BinaryColumnType::Uuid);
    }
}
