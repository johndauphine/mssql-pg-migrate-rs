# Changelog

All notable changes to this project will be documented in this file.

## [0.8.9] - 2025-12-29

### Performance
- **MSSQL Upsert 100x Faster** - Complete rewrite using staging table approach
  - Bulk insert to staging table → single MERGE to target
  - First pass: ~500 rows/sec → **95,873 rows/sec**
  - Second pass (no changes): **157,057 rows/sec**

### Bug Fixes
- **Zero Deadlocks** - Added `WITH (TABLOCK)` hint to MERGE statements to prevent S→X lock conversion deadlocks
- **Reliable Deadlock Detection** - Uses tiberius built-in `is_deadlock()` instead of string matching
- **SQL Injection Prevention** - Parameterized queries with QUOTENAME for staging table existence check

### Improvements
- Staging tables use target schema with `_staging_[table]_[writerid]` naming (no separate staging schema)
- Linear backoff retries: 5 attempts with 200ms base delay (200ms, 400ms, 600ms, 800ms, 1000ms)
- Added `PartialEq` derive to `SqlValue` and `SqlNullType` for test assertions

### Tests
- Row partitioning logic (bulk-insertable vs oversized strings)
- NULL-safe change detection pattern verification
- Multiple non-PK column handling in MERGE
- Deadlock detection with non-server errors

## [0.8.8] - 2025-12-29

### Bug Fixes
- **Large String Fallback** - Strings exceeding TDS bulk insert limit (65535 UTF-16 bytes) now fall back to parameterized INSERT instead of failing (#51)

### Improvements
- Added `row_has_oversized_strings()` helper to detect rows needing INSERT fallback
- Added unit tests for oversized string detection with various edge cases

## [0.8.7] - 2025-12-29

### Bug Fixes
- **PostgreSQL to MSSQL type mapping fixes** - Fixed ambiguous type detection that caused incorrect column types
  - `text` now correctly maps to `nvarchar(max)` instead of deprecated MSSQL `text` type
  - `varchar` now correctly maps to `nvarchar` for Unicode support
  - `char` now correctly maps to `nchar` for Unicode support
  - `date` now maps to `datetime2` to work around Tiberius bulk insert DATE serialization issues

### Improvements
- Improved comments documenting intentionally excluded types from `is_mssql_type()` check
- Updated type mapping comments to explain rationale for date->datetime2 conversion

## [0.8.6] - 2025-12-29

### Performance
- **TDS Bulk Insert for MSSQL Target** - 6x faster data loading (~180,000 rows/sec) using native TDS bulk insert protocol instead of INSERT statements (#48)

### Bug Fixes
- **JSONB handling in PG to MSSQL migration** - Fixed JSONB values becoming NULL when migrating from PostgreSQL to MSSQL (#47)

### Security
- Fixed SQL injection vulnerability in `create_schema()` using parameterized queries

### Improvements
- Added bounds checking for date conversions to prevent overflow
- Added warning logs for NaN/Infinity and out-of-range date conversions
- Added unit tests for bulk insert type conversions
- IDENTITY columns converted to regular INT/BIGINT for data warehouse use (enables bulk insert)

### Breaking Changes
- MSSQL target tables no longer have IDENTITY columns (intentional for data warehouse use case)

## [0.8.5] - 2025-12-29

### Changes
- Simplified codebase architecture
- Documentation updates for performance and benchmarks

## [0.8.4] - 2025-12-29

### Changes
- Fast upsert mode enabled by default
- Performance optimizations for upsert operations

## [0.8.3] - 2025-12-29

### Bug Fixes
- Fixed nvarchar(max) hash detection issues

## [0.8.2] - 2025-12-28

### Performance
- Verification performance improvements

## [0.8.1] - 2025-12-28

### Features
- Added `hash_text_columns` performance option for large text column handling

## [0.8.0] - 2025-12-28

### Features
- **Bidirectional Migration Support** - Migrate from MSSQL to PostgreSQL and PostgreSQL to MSSQL
- PostgreSQL source pool implementation
- MSSQL target pool implementation
- Type mapping for both directions

## [0.7.1] - 2025-12-28

### Bug Fixes
- Fixed wizard default values

## [0.7.0] - 2025-12-28

### Features
- Batch hash verification for data integrity checks

## [0.3.0] - 2025-12-27

### Features
- Interactive TUI mode for guided migration setup
