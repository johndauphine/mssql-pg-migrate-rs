# Changelog

All notable changes to this project will be documented in this file.

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
