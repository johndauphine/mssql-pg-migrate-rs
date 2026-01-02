# PostgreSQL COPY TO Binary Optimization Plan

## Executive Summary

Implement PostgreSQL `COPY TO BINARY` for source reads to maximize throughput when migrating from PostgreSQL to MSSQL. Unlike the Go implementation where COPY TO was slower than SELECT (193K vs 247K rows/s), Rust's async streaming, zero-copy parsing, and lack of GC make COPY TO potentially 2-3x faster.

## Why COPY TO Should Work Better in Rust

### Go Limitations (Why It Failed)
1. **Per-row allocations**: Go's GC overhead for millions of row/column allocations
2. **io.Pipe overhead**: Synchronous bridge between COPY output and binary parser
3. **Channel contention**: Bounded channel sends block the reader
4. **Reflection costs**: Type assertions for each column value
5. **No zero-copy**: Every value is copied into new heap allocations

### Rust Advantages
1. **Async streams**: `tokio-postgres::copy_out` returns `Pin<Box<dyn Stream<Item = Result<Bytes>>>>` - true async streaming with backpressure
2. **Reduced-copy parsing**: `bytes::Bytes` is O(1) to clone (reference-counted), reducing intermediate allocations
3. **No GC**: Deterministic memory - can reuse buffers and avoid allocation storms
4. **No reflection**: Static dispatch, type-specific parsing at compile time
5. **Flat data structures**: Can use `Vec<SqlValue>` with column count instead of `Vec<Vec<SqlValue>>`

**Note**: True "zero-copy" is limited because `SqlValue` variants like `String` require ownership. However, we can defer parsing for byte data using `bytes::Bytes` and minimize allocations through flat row buffers.

## Current Architecture

```
PostgreSQL Source
       │
       ▼ (SELECT query, row-by-row conversion)
┌─────────────────────────────────────┐
│  PgSourcePool::query_rows_fast()    │
│  - tokio_postgres::Row per row      │
│  - convert_pg_row_value() per col   │
│  - Vec<Vec<SqlValue>> output        │
└─────────────────────────────────────┘
       │
       ▼ (mpsc channel)
┌─────────────────────────────────────┐
│  TransferEngine::execute()          │
│  - RowChunk dispatcher              │
│  - Multiple parallel writers        │
└─────────────────────────────────────┘
       │
       ▼ (MSSQL bulk insert)
       MSSQL Target
```

## Proposed Architecture

```
PostgreSQL Source
       │
       ▼ (COPY TO BINARY streaming)
┌─────────────────────────────────────┐
│  PgSourcePool::copy_rows_binary()   │
│  - copy_out() returns async Stream  │
│  - BinaryRowParser on Bytes buffer  │
│  - Zero-copy SqlValue conversion    │
└─────────────────────────────────────┘
       │
       ▼ (mpsc channel - larger chunks)
┌─────────────────────────────────────┐
│  TransferEngine::execute()          │
│  - Same dispatcher/writer pattern   │
│  - May batch more rows per chunk    │
└─────────────────────────────────────┘
       │
       ▼ (MSSQL bulk insert)
       MSSQL Target
```

## Implementation Phases

### Phase 1: Binary Format Parser (Core)

Create a new module `src/source/pg_binary.rs`:

```rust
//! PostgreSQL COPY BINARY format parser.
//!
//! Binary format: https://www.postgresql.org/docs/current/sql-copy.html#id-1.9.3.55.9.4.5
//!
//! Header: PGCOPY\n\xff\r\n\0 (11 bytes) + flags (4 bytes) + ext_len (4 bytes)
//! Each row: field_count (2 bytes) + [field_len (4 bytes) + data]*
//! Trailer: -1 (2 bytes as field_count)

use bytes::{Buf, BytesMut};
use crate::target::SqlValue;
use crate::error::Result;

/// A flat row chunk with column count for O(1) row access.
pub struct FlatRowChunk {
    pub data: Vec<SqlValue>,
    pub num_columns: usize,
    pub num_rows: usize,
}

impl FlatRowChunk {
    /// Get row i as a slice.
    pub fn row(&self, i: usize) -> &[SqlValue] {
        let start = i * self.num_columns;
        &self.data[start..start + self.num_columns]
    }
}

pub struct BinaryRowParser {
    buffer: BytesMut,  // Mutable for appending stream data
    column_types: Vec<ColumnType>,
    header_parsed: bool,
}

#[derive(Clone, Debug)]
pub enum ColumnType {
    Bool,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Numeric,  // Use rust_decimal for parsing
    Uuid,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Varchar,
    Text,
    Bytea,
    Json,
    Jsonb,
}

impl BinaryRowParser {
    pub fn new(column_types: Vec<ColumnType>) -> Self { ... }

    /// Append more data from the COPY stream.
    pub fn extend(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Try to parse the next row. Returns None if more data needed.
    pub fn next_row(&mut self) -> Result<Option<Vec<SqlValue>>> { ... }

    /// Parse a single field using bytes::Buf trait for automatic BE handling.
    fn parse_field(&mut self, col_type: &ColumnType, len: i32) -> SqlValue {
        if len == -1 {
            return SqlValue::Null(col_type.to_null_type());
        }

        match col_type {
            ColumnType::Int16 => SqlValue::I16(self.buffer.get_i16()),  // BE automatic
            ColumnType::Int32 => SqlValue::I32(self.buffer.get_i32()),
            ColumnType::Int64 => SqlValue::I64(self.buffer.get_i64()),
            ColumnType::Float32 => SqlValue::F32(self.buffer.get_f32()),
            ColumnType::Float64 => SqlValue::F64(self.buffer.get_f64()),
            ColumnType::Bool => SqlValue::Bool(self.buffer.get_u8() != 0),
            ColumnType::Text | ColumnType::Varchar => {
                let bytes = self.buffer.split_to(len as usize);
                // Must validate UTF-8 and allocate String
                let s = String::from_utf8_lossy(&bytes).into_owned();
                SqlValue::String(s)
            }
            ColumnType::Bytea => {
                let bytes = self.buffer.split_to(len as usize);
                SqlValue::Bytes(bytes.to_vec())
            }
            ColumnType::Uuid => {
                let mut uuid_bytes = [0u8; 16];
                self.buffer.copy_to_slice(&mut uuid_bytes);
                SqlValue::Uuid(uuid::Uuid::from_bytes(uuid_bytes))
            }
            ColumnType::Numeric => {
                // Use postgres binary NUMERIC format parser
                // See: https://github.com/sfackler/rust-postgres/blob/master/postgres-types/src/pg_lsn.rs
                parse_pg_numeric(&mut self.buffer, len as usize)
            }
            // ... other types
        }
    }

    /// Check if we've reached the trailer (-1 field count).
    pub fn is_complete(&self) -> bool { ... }
}
```

**Key optimizations:**
1. Use `bytes::Buf` trait methods (`get_i32()`, `get_i64()`) for automatic Big Endian handling
2. Use `BytesMut` for efficient appending of stream chunks
3. Pre-compute OID-to-ColumnType mapping once per table
4. Parse fixed-width types inline without allocation
5. For NUMERIC, port parsing logic from `postgres-types` crate

### Phase 2: Streaming Reader Integration

Add method to `PgSourcePool`:

```rust
impl PgSourcePool {
    /// Read rows using COPY TO BINARY format for maximum throughput.
    ///
    /// This is significantly faster than row-by-row SELECT for large tables:
    /// - ~2-3x throughput improvement (estimated 400-600K rows/s)
    /// - Lower CPU usage due to zero-copy parsing
    /// - Better memory efficiency with streaming
    pub async fn copy_rows_binary(
        &self,
        schema: &str,
        table: &str,
        columns: &[String],
        col_types: &[ColumnType],
        pk_col: Option<&str>,
        min_pk: Option<i64>,
        max_pk: Option<i64>,
        tx: mpsc::Sender<Vec<Vec<SqlValue>>>,
    ) -> Result<i64> {
        let client = self.pool.get().await?;

        // Build COPY query with optional WHERE clause for partitioning
        let query = build_copy_query(schema, table, columns, pk_col, min_pk, max_pk);

        // Execute COPY TO STDOUT BINARY
        let copy_stream = client.copy_out(&query).await?;

        let mut parser = BinaryRowParser::new(col_types.to_vec());
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let mut total_rows = 0i64;

        tokio::pin!(copy_stream);

        while let Some(data) = copy_stream.next().await {
            let bytes = data?;
            parser.extend(bytes);

            // Parse all complete rows from buffer
            while let Some(row) = parser.next_row()? {
                batch.push(row);
                total_rows += 1;

                if batch.len() >= BATCH_SIZE {
                    tx.send(std::mem::take(&mut batch)).await?;
                    batch = Vec::with_capacity(BATCH_SIZE);
                }
            }
        }

        // Send remaining rows
        if !batch.is_empty() {
            tx.send(batch).await?;
        }

        Ok(total_rows)
    }
}

fn build_copy_query(
    schema: &str,
    table: &str,
    columns: &[String],
    pk_col: Option<&str>,
    min_pk: Option<i64>,
    max_pk: Option<i64>,
) -> String {
    let col_list = columns.iter()
        .map(|c| quote_pg_ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    let table_ref = qualify_pg_table(schema, table);

    if let (Some(pk), Some(min), Some(max)) = (pk_col, min_pk, max_pk) {
        // Use subquery for range filtering
        format!(
            "COPY (SELECT {} FROM {} WHERE {} >= {} AND {} <= {} ORDER BY {}) TO STDOUT (FORMAT BINARY)",
            col_list, table_ref,
            quote_pg_ident(pk), min,
            quote_pg_ident(pk), max,
            quote_pg_ident(pk)
        )
    } else {
        format!(
            "COPY (SELECT {} FROM {}) TO STDOUT (FORMAT BINARY)",
            col_list, table_ref
        )
    }
}
```

### Phase 3: Transfer Engine Integration

Modify `transfer/mod.rs` to use COPY for PostgreSQL sources:

```rust
async fn read_table_chunks_parallel(
    source: SourcePoolImpl,
    job: TransferJob,
    columns: Vec<String>,
    col_types: Vec<String>,
    chunk_size: usize,
    num_readers: usize,
    tx: mpsc::Sender<RowChunk>,
) -> Result<()> {
    // If source is PostgreSQL, use COPY TO BINARY
    if source.db_type() == "postgres" {
        return read_table_copy_binary(
            source, job, columns, col_types, chunk_size, num_readers, tx
        ).await;
    }

    // Existing SELECT-based logic for MSSQL sources
    // ...
}

async fn read_table_copy_binary(
    source: SourcePoolImpl,
    job: TransferJob,
    columns: Vec<String>,
    col_types: Vec<String>,
    chunk_size: usize,
    num_readers: usize,
    tx: mpsc::Sender<RowChunk>,
) -> Result<()> {
    // Convert string types to ColumnType enum
    let binary_col_types = convert_to_binary_types(&col_types);

    // For partitioned tables, spawn parallel COPY readers
    let ranges = split_pk_range(min_pk, max_pk, num_readers);

    let mut handles = Vec::with_capacity(num_readers);
    for (reader_id, (range_min, range_max)) in ranges.into_iter().enumerate() {
        let source = source.clone();
        let columns = columns.clone();
        let col_types = binary_col_types.clone();
        let tx = tx.clone();
        let table = job.table.clone();

        handles.push(tokio::spawn(async move {
            source.as_postgres()
                .copy_rows_binary(
                    &table.schema,
                    &table.name,
                    &columns,
                    &col_types,
                    Some(&table.primary_key[0]),
                    range_min,
                    Some(range_max),
                    tx,
                )
                .await
        }));
    }

    // Collect results
    for handle in handles {
        handle.await??;
    }

    Ok(())
}
```

### Phase 4: Benchmarking & Feature Flag

1. **Add feature flag** for easy A/B testing:
```toml
[features]
default = []
pg-copy-binary = []  # Enable COPY TO BINARY for PG source
```

2. **Benchmark harness**:
```rust
#[cfg(test)]
mod benchmarks {
    // Compare SELECT vs COPY TO BINARY throughput
    // Measure: rows/sec, CPU usage, memory usage
}
```

3. **Runtime selection** via config:
```yaml
source:
  type: postgres
  use_copy_binary: true  # Optional, defaults based on feature
```

## Type Mapping: PostgreSQL OIDs to ColumnType

| PostgreSQL Type | OID | Binary Format | ColumnType |
|-----------------|-----|---------------|------------|
| bool | 16 | 1 byte | Bool |
| int2 | 21 | 2 bytes BE | Int16 |
| int4 | 23 | 4 bytes BE | Int32 |
| int8 | 20 | 8 bytes BE | Int64 |
| float4 | 700 | 4 bytes IEEE | Float32 |
| float8 | 701 | 8 bytes IEEE | Float64 |
| numeric | 1700 | Variable | Numeric |
| uuid | 2950 | 16 bytes | Uuid |
| date | 1082 | 4 bytes (days) | Date |
| time | 1083 | 8 bytes (micros) | Time |
| timestamp | 1114 | 8 bytes (micros) | Timestamp |
| timestamptz | 1184 | 8 bytes (micros) | TimestampTz |
| varchar | 1043 | Length-prefixed | Varchar |
| text | 25 | Length-prefixed | Text |
| bytea | 17 | Length-prefixed | Bytea |
| json | 114 | Length-prefixed | Json |
| jsonb | 3802 | Length-prefixed | Jsonb |

## Gemini Review Feedback (Incorporated)

### Key Concerns Addressed

1. **SqlValue Ownership**: Current `SqlValue` owns data (e.g., `String`). True zero-copy would require `Bytes` or `Cow<'a, str>`. For Phase 1, we accept allocation for text fields but minimize with flat row buffers.

2. **NUMERIC Complexity**: Use `bytes::Buf` trait methods and port logic from `rust_decimal` or `pg_numeric` crate rather than hand-rolling the base-10000 digit parsing.

3. **Vec<Vec<SqlValue>> Allocation Storm**: Replace with flat `RowChunk`:
   ```rust
   struct RowChunk {
       data: Vec<SqlValue>,  // Flat: row 0 is [0..cols], row 1 is [cols..2*cols]
       num_columns: usize,
       num_rows: usize,
   }
   ```

4. **Endianness**: Use `bytes::Buf` trait methods (`get_i32()`, `get_i64()`) which handle Big Endian conversion automatically.

5. **SIMD Deferred**: Remove SIMD claims from Phase 1. Focus on memory layout optimization first; add SIMD only if profiling shows CPU bottleneck.

### Questions Answered

1. **SqlValue owns data**: Yes, `SqlValue::String(String)` allocates. We'll add `SqlValue::Bytes(bytes::Bytes)` variant for deferred parsing when beneficial.

2. **NULL handling**: Binary protocol uses `-1` length for NULL. `SqlValue::Null(SqlNullType)` variant already exists and will be used.

3. **Error behavior**: One corrupt row fails the COPY stream. We'll implement row-level validation where possible and clear error messages for debugging.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Binary format parsing bugs | Medium | High | Extensive test coverage with known-good data |
| Type mismatch errors | Medium | Medium | Validate OID mapping at startup |
| Memory pressure from large rows | Low | Medium | Stream processing, don't buffer entire table |
| Deadlock in parallel readers | Low | High | Independent connections per reader |
| Incompatibility with older PG | Low | Low | Check PG version, fallback to SELECT |
| NUMERIC precision errors | Medium | High | Use battle-tested rust_decimal parsing |
| Allocation storm on text-heavy tables | Medium | Medium | Flat RowChunk structure, batch sizing |

## Success Metrics

1. **Throughput**: Target 400-600K rows/s (vs current ~250K with SELECT)
2. **CPU Usage**: <50% reduction in CPU time per row
3. **Memory**: Flat memory profile regardless of table size
4. **Correctness**: 100% data integrity (checksum validation)

## Testing Strategy

1. **Unit tests**: Binary parser with hand-crafted byte sequences
2. **Integration tests**: Round-trip PG -> MSSQL -> validate
3. **Fuzz testing**: Random binary data to catch parser crashes
4. **Performance tests**: Benchmark suite comparing SELECT vs COPY
5. **Edge cases**: NULL values, empty strings, max-size values, unicode

## Revised Implementation Order (Per Gemini Review)

1. **Phase 1a: Core Binary Parser** - Basic types (int, text, uuid, bool)
   - Implement `BinaryRowParser` with `bytes::Buf` for endianness
   - Focus on most common types first
   - **Estimated: 2 days**

2. **Phase 1b: Integration Test** - Hook up `tokio-postgres::copy_out`
   - Verify data flows without crashes
   - Test with real PostgreSQL data
   - **Estimated: 1 day**

3. **Phase 2: Complex Types** - Numeric, TimestampTZ, Date/Time
   - Port NUMERIC parsing from `postgres-types`
   - Implement timestamp offset handling
   - **Estimated: 1-2 days**

4. **Phase 3: Transfer Engine Integration** - Use COPY for PG sources
   - Integrate FlatRowChunk with transfer pipeline
   - Add feature flag for A/B testing
   - **Estimated: 1 day**

5. **Phase 4: Benchmarking & Tuning**
   - Compare SELECT vs COPY throughput
   - Profile allocations, optimize if needed
   - **Estimated: 1-2 days**

6. **Phase 5: Testing & Polish**
   - Edge cases, error handling, documentation
   - **Estimated: 2 days**

**Total: ~8-10 days**

## Files to Create/Modify

### New Files
- `crates/mssql-pg-migrate/src/source/pg_binary.rs` - Binary format parser

### Modified Files
- `crates/mssql-pg-migrate/src/source/postgres.rs` - Add copy_rows_binary method
- `crates/mssql-pg-migrate/src/source/mod.rs` - Export new module
- `crates/mssql-pg-migrate/src/transfer/mod.rs` - Use COPY for PG sources
- `crates/mssql-pg-migrate/Cargo.toml` - Add feature flag

## Appendix: PostgreSQL Binary Format Spec

### Header (11 bytes signature + 8 bytes header data)
```
Bytes 0-10:  PGCOPY\n\xff\r\n\0  (signature)
Bytes 11-14: Flags (4 bytes, network order)
Bytes 15-18: Header extension length (4 bytes, network order)
Bytes 19+:   Header extension data (if length > 0)
```

### Row Format
```
field_count: int16 (network order)
For each field:
  field_length: int32 (network order, -1 = NULL)
  field_data: <field_length> bytes (if length >= 0)
```

### Trailer
```
field_count: int16 = -1 (0xFFFF)
```
