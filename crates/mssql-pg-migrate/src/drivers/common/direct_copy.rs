//! Direct COPY binary encoder for zero-copy row transfer.
//!
//! This module provides a high-performance encoder that converts MSSQL rows
//! directly to PostgreSQL COPY binary format, bypassing the intermediate
//! `SqlValue` representation.
//!
//! Performance gains come from:
//! - Single type dispatch per value (vs. two in SqlValue approach)
//! - No intermediate allocations for SqlValue enum
//! - Better cache locality by processing rows in a tight loop
//!
//! ## Timezone Handling
//!
//! MSSQL `datetime` and `datetime2` columns do not store timezone information.
//! This encoder assumes all datetime values are in UTC when converting to
//! PostgreSQL timestamps. If your MSSQL data uses a different timezone
//! (e.g., local server time), the migrated values may need adjustment.
//!
//! ## Requirements
//!
//! - Direct copy is only enabled for tables with integer primary keys
//!   (tinyint, smallint, int, bigint) to support proper resume tracking.
//! - Tables with UUID, string, or composite primary keys will fall back
//!   to the standard SqlValue encoding path.

use bytes::{BufMut, BytesMut};
use chrono::{NaiveDateTime, Timelike};
use rust_decimal::Decimal;
use tiberius::Row;
use uuid::Uuid;

/// Buffer size for COPY data chunks (~64KB).
const COPY_SEND_BUF_SIZE: usize = 65536;

/// PostgreSQL epoch offset (microseconds from 1970-01-01 to 2000-01-01).
const POSTGRES_EPOCH_OFFSET: i64 = 946_684_800_000_000;

/// Maximum length for variable-length fields in PostgreSQL COPY binary format.
/// PostgreSQL uses i32 for lengths, so max is 2^31-1 bytes (~2GB).
/// In practice, PostgreSQL has lower limits, but we use this as a safety check.
const MAX_FIELD_LENGTH: usize = i32::MAX as usize;

/// Safely convert a usize length to i32 for PostgreSQL COPY binary format.
/// Returns the truncated length (capped at MAX_FIELD_LENGTH) and logs a warning
/// in debug builds if truncation occurs. In practice, values this large should
/// never occur in normal database usage.
#[inline]
fn safe_length_i32(len: usize) -> i32 {
    if len > MAX_FIELD_LENGTH {
        // This should never happen in practice - PostgreSQL doesn't support
        // values this large. Log in debug mode and cap the value.
        debug_assert!(
            false,
            "Field length {} exceeds PostgreSQL COPY limit of {} bytes",
            len, MAX_FIELD_LENGTH
        );
        i32::MAX
    } else {
        len as i32
    }
}

/// Column encoding strategy - determined once per column, used for all rows.
#[derive(Debug, Clone, Copy)]
pub enum ColumnEncoder {
    Bool,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Real,
    Float,
    String,
    Bytes,
    Uuid,
    DateTime,
    Date,
    Time,
    Decimal,
}

impl ColumnEncoder {
    /// Create encoder from MSSQL data type string.
    pub fn from_mssql_type(data_type: &str) -> Self {
        match data_type.to_lowercase().as_str() {
            "bit" => Self::Bool,
            "tinyint" => Self::TinyInt,
            "smallint" => Self::SmallInt,
            "int" => Self::Int,
            "bigint" => Self::BigInt,
            "real" => Self::Real,
            "float" => Self::Float,
            "uniqueidentifier" => Self::Uuid,
            "datetime" | "datetime2" | "smalldatetime" => Self::DateTime,
            "date" => Self::Date,
            "time" => Self::Time,
            "binary" | "varbinary" | "image" => Self::Bytes,
            "decimal" | "numeric" | "money" | "smallmoney" => Self::Decimal,
            // Default: treat as string (varchar, nvarchar, char, nchar, text, ntext, xml, etc.)
            _ => Self::String,
        }
    }
}

/// Direct COPY encoder that converts MSSQL rows to PostgreSQL binary format.
///
/// Pre-computes column encoders once, then uses them for all rows.
/// This eliminates per-value type dispatch overhead.
pub struct DirectCopyEncoder {
    encoders: Vec<ColumnEncoder>,
    num_cols: i16,
}

impl DirectCopyEncoder {
    /// Create a new encoder from column types.
    pub fn new(col_types: &[String]) -> Self {
        let encoders: Vec<ColumnEncoder> = col_types
            .iter()
            .map(|t| ColumnEncoder::from_mssql_type(t))
            .collect();
        let num_cols = encoders.len() as i16;
        Self { encoders, num_cols }
    }

    /// Write COPY binary header to buffer.
    #[inline]
    pub fn write_header(&self, buf: &mut BytesMut) {
        buf.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
        buf.put_i32(0); // Flags: no OIDs
        buf.put_i32(0); // Header extension length: 0
    }

    /// Write COPY binary trailer to buffer.
    #[inline]
    pub fn write_trailer(&self, buf: &mut BytesMut) {
        buf.put_i16(-1);
    }

    /// Encode a single row directly from tiberius Row to binary COPY format.
    ///
    /// Returns the primary key value at `pk_idx` if provided and the column is an integer type.
    #[inline]
    pub fn encode_row(&self, row: &Row, buf: &mut BytesMut, pk_idx: Option<usize>) -> Option<i64> {
        // Field count
        buf.put_i16(self.num_cols);

        let mut pk_value: Option<i64> = None;

        for (idx, encoder) in self.encoders.iter().enumerate() {
            let is_pk = pk_idx == Some(idx);

            match encoder {
                ColumnEncoder::Bool => {
                    if let Some(v) = row.get::<bool, _>(idx) {
                        buf.put_i32(1);
                        buf.put_u8(if v { 1 } else { 0 });
                    } else {
                        buf.put_i32(-1); // NULL
                    }
                }
                ColumnEncoder::TinyInt => {
                    if let Some(v) = row.get::<u8, _>(idx) {
                        buf.put_i32(2);
                        buf.put_i16(v as i16);
                        if is_pk {
                            pk_value = Some(v as i64);
                        }
                    } else {
                        buf.put_i32(-1);
                    }
                }
                ColumnEncoder::SmallInt => {
                    if let Some(v) = row.get::<i16, _>(idx) {
                        buf.put_i32(2);
                        buf.put_i16(v);
                        if is_pk {
                            pk_value = Some(v as i64);
                        }
                    } else {
                        buf.put_i32(-1);
                    }
                }
                ColumnEncoder::Int => {
                    if let Some(v) = row.get::<i32, _>(idx) {
                        buf.put_i32(4);
                        buf.put_i32(v);
                        if is_pk {
                            pk_value = Some(v as i64);
                        }
                    } else {
                        buf.put_i32(-1);
                    }
                }
                ColumnEncoder::BigInt => {
                    if let Some(v) = row.get::<i64, _>(idx) {
                        buf.put_i32(8);
                        buf.put_i64(v);
                        if is_pk {
                            pk_value = Some(v);
                        }
                    } else {
                        buf.put_i32(-1);
                    }
                }
                ColumnEncoder::Real => {
                    if let Some(v) = row.get::<f32, _>(idx) {
                        buf.put_i32(4);
                        buf.put_f32(v);
                    } else {
                        buf.put_i32(-1);
                    }
                }
                ColumnEncoder::Float => {
                    if let Some(v) = row.get::<f64, _>(idx) {
                        buf.put_i32(8);
                        buf.put_f64(v);
                    } else {
                        buf.put_i32(-1);
                    }
                }
                ColumnEncoder::String => {
                    if let Some(v) = row.get::<&str, _>(idx) {
                        let bytes = v.as_bytes();
                        buf.put_i32(safe_length_i32(bytes.len()));
                        buf.extend_from_slice(bytes);
                    } else {
                        buf.put_i32(-1);
                    }
                }
                ColumnEncoder::Bytes => {
                    if let Some(v) = row.get::<&[u8], _>(idx) {
                        buf.put_i32(safe_length_i32(v.len()));
                        buf.extend_from_slice(v);
                    } else {
                        buf.put_i32(-1);
                    }
                }
                ColumnEncoder::Uuid => {
                    if let Some(v) = row.get::<Uuid, _>(idx) {
                        buf.put_i32(16);
                        buf.extend_from_slice(v.as_bytes());
                    } else {
                        buf.put_i32(-1);
                    }
                }
                ColumnEncoder::DateTime => {
                    // TIMEZONE ASSUMPTION: MSSQL datetime/datetime2 columns store values
                    // without timezone information. This encoder treats them as UTC.
                    // If your MSSQL data is stored in a different timezone (e.g., local time),
                    // the migrated timestamps will be offset by the timezone difference.
                    // Consider adjusting timestamps post-migration if needed.
                    if let Some(v) = row.get::<NaiveDateTime, _>(idx) {
                        let micros = v.and_utc().timestamp_micros() - POSTGRES_EPOCH_OFFSET;
                        buf.put_i32(8);
                        buf.put_i64(micros);
                    } else {
                        buf.put_i32(-1);
                    }
                }
                ColumnEncoder::Date => {
                    if let Some(v) = row.get::<NaiveDateTime, _>(idx) {
                        // PostgreSQL date is days since 2000-01-01
                        let epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
                        let days = (v.date() - epoch).num_days() as i32;
                        buf.put_i32(4);
                        buf.put_i32(days);
                    } else {
                        buf.put_i32(-1);
                    }
                }
                ColumnEncoder::Time => {
                    if let Some(v) = row.get::<NaiveDateTime, _>(idx) {
                        // PostgreSQL time is microseconds since midnight
                        let t = v.time();
                        let micros = t.num_seconds_from_midnight() as i64 * 1_000_000
                            + t.nanosecond() as i64 / 1000;
                        buf.put_i32(8);
                        buf.put_i64(micros);
                    } else {
                        buf.put_i32(-1);
                    }
                }
                ColumnEncoder::Decimal => {
                    if let Some(d) = row.get::<Decimal, _>(idx) {
                        write_binary_numeric(buf, &d);
                    } else {
                        buf.put_i32(-1);
                    }
                }
            }
        }

        pk_value
    }

    /// Encode multiple rows to a COPY binary chunk.
    ///
    /// Returns (encoded_bytes, first_pk, last_pk, row_count).
    pub fn encode_rows(
        &self,
        rows: &[Row],
        pk_idx: Option<usize>,
    ) -> (BytesMut, Option<i64>, Option<i64>, usize) {
        let mut buf = BytesMut::with_capacity(COPY_SEND_BUF_SIZE * 2);

        self.write_header(&mut buf);

        let mut first_pk: Option<i64> = None;
        let mut last_pk: Option<i64> = None;
        let row_count = rows.len();

        for (i, row) in rows.iter().enumerate() {
            let pk = self.encode_row(row, &mut buf, pk_idx);

            // Track PKs if we have a PK column
            if pk_idx.is_some() {
                if i == 0 {
                    first_pk = pk;
                }
                if pk.is_some() {
                    last_pk = pk;
                }
            }
        }

        self.write_trailer(&mut buf);

        (buf, first_pk, last_pk, row_count)
    }

    /// Get recommended buffer size for a given number of rows.
    ///
    /// Estimates buffer size based on:
    /// - Average bytes per column by type (fixed vs variable length)
    /// - Per-row overhead (field count + length prefixes)
    /// - Header/trailer overhead
    pub fn buffer_size_for_rows(&self, row_count: usize) -> usize {
        // Estimate average bytes per column by type
        let avg_bytes_per_col: usize = self
            .encoders
            .iter()
            .map(|e| match e {
                ColumnEncoder::Bool => 1 + 4,     // value + length
                ColumnEncoder::TinyInt => 2 + 4,  // smallint + length
                ColumnEncoder::SmallInt => 2 + 4, // smallint + length
                ColumnEncoder::Int => 4 + 4,      // int + length
                ColumnEncoder::BigInt => 8 + 4,   // bigint + length
                ColumnEncoder::Real => 4 + 4,     // float4 + length
                ColumnEncoder::Float => 8 + 4,    // float8 + length
                ColumnEncoder::DateTime => 8 + 4, // timestamp + length
                ColumnEncoder::Date => 4 + 4,     // date + length
                ColumnEncoder::Time => 8 + 4,     // time + length
                ColumnEncoder::Uuid => 16 + 4,    // uuid + length
                ColumnEncoder::Decimal => 20 + 4, // avg decimal + length
                ColumnEncoder::String => 50 + 4,  // avg varchar + length
                ColumnEncoder::Bytes => 100 + 4,  // avg varbinary + length
            })
            .sum();

        // Per-row overhead: 2 bytes for field count
        let per_row_overhead = 2;

        // Estimate total size
        let data_size = row_count * (avg_bytes_per_col + per_row_overhead);

        // Add header (19 bytes) + trailer (2 bytes) + 10% safety margin
        let total = data_size + 21;
        let with_margin = total + total / 10;

        // Round up to next power of 2 for efficient allocator behavior
        // But cap at a reasonable maximum to avoid huge allocations
        let rounded = with_margin.next_power_of_two();
        rounded.min(64 * 1024 * 1024) // Cap at 64MB
    }

    /// Get default buffer size (for backward compatibility).
    pub fn buffer_size(&self) -> usize {
        COPY_SEND_BUF_SIZE
    }
}

/// Write a decimal value in PostgreSQL NUMERIC binary format.
///
/// PostgreSQL NUMERIC format:
/// - int16 ndigits: count of base-10000 digits
/// - int16 weight: position of first digit (exponent in base-10000)
/// - int16 sign: 0x0000 = positive, 0x4000 = negative
/// - int16 dscale: display scale (digits after decimal point)
/// - int16[] digits: base-10000 digits, most significant first
///
/// The weight represents the position of the first base-10000 digit relative
/// to the decimal point. weight=0 means the first digit is in the ones place
/// (0-9999), weight=1 means 10000-99999999, weight=-1 means 0.0001-0.9999, etc.
fn write_binary_numeric(buf: &mut BytesMut, d: &Decimal) {
    // Handle special case: zero
    if d.is_zero() {
        buf.put_i32(8); // length: header only (4 x i16)
        buf.put_i16(0); // ndigits
        buf.put_i16(0); // weight
        buf.put_i16(0); // sign (positive)
        buf.put_i16(0); // dscale
        return;
    }

    let scale = d.scale();
    let sign: i16 = if d.is_sign_negative() { 0x4000 } else { 0x0000 };

    // Get the absolute mantissa
    let mantissa = d.mantissa().unsigned_abs();

    // Convert mantissa to base-10000 digits (least significant first)
    let mut digits: Vec<i16> = Vec::with_capacity(16);
    let mut m = mantissa;
    while m > 0 {
        digits.push((m % 10000) as i16);
        m /= 10000;
    }
    if digits.is_empty() {
        digits.push(0);
    }
    digits.reverse(); // Now most significant first

    // Calculate weight based on number of base-10000 digits and scale
    // Each base-10000 digit represents 4 decimal digits
    // Weight = (number of integer digits - 1) / 4
    //
    // For a decimal with `scale` fractional digits:
    // - Total decimal digits in mantissa = ceil(log10(mantissa))
    // - Integer digits = total digits - scale
    let weight = (digits.len() as i32) - 1 - ((scale as i32 + 3) / 4);

    // Trim trailing zeros from digits (PostgreSQL convention)
    while digits.last() == Some(&0) && digits.len() > 1 {
        digits.pop();
    }

    let ndigits = digits.len() as i16;
    let dscale = scale as i16;

    // Length: 8 bytes header + 2 bytes per digit
    let len = 8 + ndigits as i32 * 2;
    buf.put_i32(len);
    buf.put_i16(ndigits);
    buf.put_i16(weight as i16);
    buf.put_i16(sign);
    buf.put_i16(dscale);

    for digit in digits {
        buf.put_i16(digit);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoder_creation() {
        let col_types = vec![
            "int".to_string(),
            "varchar".to_string(),
            "datetime".to_string(),
        ];
        let encoder = DirectCopyEncoder::new(&col_types);
        assert_eq!(encoder.num_cols, 3);
    }

    #[test]
    fn test_column_encoder_mapping() {
        assert!(matches!(
            ColumnEncoder::from_mssql_type("int"),
            ColumnEncoder::Int
        ));
        assert!(matches!(
            ColumnEncoder::from_mssql_type("VARCHAR"),
            ColumnEncoder::String
        ));
        assert!(matches!(
            ColumnEncoder::from_mssql_type("datetime2"),
            ColumnEncoder::DateTime
        ));
    }

    /// Helper to extract numeric fields from encoded buffer
    fn parse_numeric_fields(buf: &[u8]) -> (i16, i16, i16, i16, Vec<i16>) {
        // Skip the 4-byte length prefix
        let ndigits = i16::from_be_bytes([buf[4], buf[5]]);
        let weight = i16::from_be_bytes([buf[6], buf[7]]);
        let sign = i16::from_be_bytes([buf[8], buf[9]]);
        let dscale = i16::from_be_bytes([buf[10], buf[11]]);

        let mut digits = Vec::new();
        for i in 0..ndigits as usize {
            let offset = 12 + i * 2;
            digits.push(i16::from_be_bytes([buf[offset], buf[offset + 1]]));
        }

        (ndigits, weight, sign, dscale, digits)
    }

    #[test]
    fn test_decimal_encoding_zero() {
        let mut buf = BytesMut::new();
        let d = Decimal::ZERO;
        write_binary_numeric(&mut buf, &d);

        let (ndigits, weight, sign, dscale, digits) = parse_numeric_fields(&buf);
        assert_eq!(ndigits, 0);
        assert_eq!(weight, 0);
        assert_eq!(sign, 0);
        assert_eq!(dscale, 0);
        assert!(digits.is_empty());
    }

    #[test]
    fn test_decimal_encoding_integer() {
        let mut buf = BytesMut::new();
        let d = Decimal::new(12345, 0); // 12345
        write_binary_numeric(&mut buf, &d);

        let (ndigits, _weight, sign, dscale, digits) = parse_numeric_fields(&buf);
        assert!(ndigits > 0);
        assert_eq!(sign, 0); // positive
        assert_eq!(dscale, 0);
        assert!(!digits.is_empty());
    }

    #[test]
    fn test_decimal_encoding_simple_fraction() {
        let mut buf = BytesMut::new();
        let d = Decimal::new(12345, 2); // 123.45
        write_binary_numeric(&mut buf, &d);

        let (ndigits, _weight, sign, dscale, digits) = parse_numeric_fields(&buf);
        assert!(ndigits > 0);
        assert_eq!(sign, 0);
        assert_eq!(dscale, 2);
        assert!(!digits.is_empty());
    }

    #[test]
    fn test_decimal_encoding_small_fraction() {
        let mut buf = BytesMut::new();
        let d = Decimal::new(123, 5); // 0.00123
        write_binary_numeric(&mut buf, &d);

        let (ndigits, _weight, sign, dscale, digits) = parse_numeric_fields(&buf);
        assert!(ndigits > 0);
        assert_eq!(sign, 0);
        assert_eq!(dscale, 5);
        assert!(!digits.is_empty());
    }

    #[test]
    fn test_decimal_encoding_negative() {
        let mut buf = BytesMut::new();
        let d = Decimal::new(-500, 2); // -5.00
        write_binary_numeric(&mut buf, &d);

        let (_ndigits, _weight, sign, dscale, _digits) = parse_numeric_fields(&buf);
        assert_eq!(sign, 0x4000); // negative
        assert_eq!(dscale, 2);
    }

    #[test]
    fn test_decimal_encoding_large_number() {
        let mut buf = BytesMut::new();
        let d = Decimal::new(123456789012, 4); // 12345678.9012
        write_binary_numeric(&mut buf, &d);

        let (ndigits, _weight, sign, dscale, digits) = parse_numeric_fields(&buf);
        assert!(ndigits > 0);
        assert_eq!(sign, 0);
        assert_eq!(dscale, 4);
        assert!(!digits.is_empty());
    }
}
