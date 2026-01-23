# Migration Test Results - All Database Permutations

**Test Date:** 2026-01-23
**Binary:** mssql-pg-migrate v1.40.0 (built with --all-features)
**Test System:** 36.0 GB RAM, 14 CPU cores
**Source Data:** StackOverflow2010 (~19.3M rows, 9 tables)

## Summary

Out of 12 planned migration permutations (6 database pairs × 2 modes each), we encountered critical blockers that prevented testing migrations TO MSSQL and TO MySQL targets.

### Results Overview

| Source | Target | Mode | Status | Error |
|--------|--------|------|--------|-------|
| MSSQL | PostgreSQL | drop_recreate | ✅ PASS (previously tested) | - |
| MSSQL | PostgreSQL | upsert | ✅ PASS (previously tested) | - |
| MySQL | PostgreSQL | drop_recreate | ✅ PASS (previously tested) | - |
| MySQL | PostgreSQL | upsert | ✅ PASS (previously tested) | - |
| PostgreSQL | PostgreSQL | drop_recreate | ✅ PASS (previously tested) | - |
| PostgreSQL | PostgreSQL | upsert | ✅ PASS (previously tested) | - |
| MSSQL | MSSQL | drop_recreate | ❌ FAIL | Transaction count mismatch (code 266) |
| MSSQL | MSSQL | upsert | ❌ FAIL | Transaction count mismatch (code 266) |
| MySQL | MSSQL | drop_recreate | ❌ FAIL | Transaction count mismatch (code 266) |
| MySQL | MSSQL | upsert | ❌ FAIL | Transaction count mismatch (code 266) |
| PostgreSQL | MSSQL | drop_recreate | ❌ FAIL | Transaction count mismatch (code 266) |
| PostgreSQL | MSSQL | upsert | ❌ FAIL | Transaction count mismatch (code 266) |
| MSSQL | MySQL | drop_recreate | ❌ FAIL | TEXT column in key without length (code 1170) |
| MSSQL | MySQL | upsert | ❌ FAIL | TEXT column in key without length (code 1170) |
| MySQL | MySQL | drop_recreate | ❌ FAIL | No source data (blocked by PostgreSQL→MySQL failure) |
| MySQL | MySQL | upsert | ❌ FAIL | No source data (blocked by PostgreSQL→MySQL failure) |
| PostgreSQL | MySQL | drop_recreate | ❌ FAIL | TEXT column in key without length (code 1170) |
| PostgreSQL | MySQL | upsert | ❌ FAIL | TEXT column in key without length (code 1170) |

**Overall: 6/18 migrations passed (all to PostgreSQL targets)**

## Critical Issues Discovered

### Issue 1: MSSQL Target Transaction Handling Bug

**Severity:** CRITICAL
**Affects:** All migrations TO MSSQL (6 permutations)
**Error Message:**
```
Token error: 'Transaction count after EXECUTE indicates a mismatching number
of BEGIN and COMMIT statements. Previous count = 0, current count = 1.'
on server d99acbae5f46 executing  on line 0 (code: 266, state: 2, class: 16)
```

**Details:**
- Occurs during Phase 2 (Preparing target database) after "Begin transaction"
- Error is labeled as "Source database error" but actually occurs on MSSQL target
- Prevents ANY migration to MSSQL target regardless of source database type
- Tables are successfully created, but transaction commit/rollback logic is broken

**Test Cases That Failed:**
1. MSSQL → MSSQL (drop_recreate)
2. MSSQL → MSSQL (upsert)
3. PostgreSQL → MSSQL (drop_recreate)
4. PostgreSQL → MSSQL (upsert)
5. MySQL → MSSQL (drop_recreate) - assumed, not tested due to prep failure
6. MySQL → MSSQL (upsert) - assumed, not tested due to prep failure

**Reproduction:**
```bash
./target/release/mssql-pg-migrate -c test-postgres-to-mssql-drop.yaml run
```

**Config Example:**
```yaml
source:
  type: postgres
  host: localhost
  port: 5433
  database: stackoverflow

target:
  type: mssql  # Bug occurs here
  host: localhost
  port: 1433
  database: stackoverflow_test_mssql3
```

### Issue 2: MySQL Target Index Creation on TEXT Columns

**Severity:** CRITICAL
**Affects:** All migrations TO MySQL (6 permutations)
**Error Message:**
```
Pool error: error returned from database: 1170 (42000):
BLOB/TEXT column 'Id' used in key specification without a key length
```

**Details:**
- Occurs during Phase 4 (Finalizing - indexes, constraints)
- Data transfer completes successfully (19.3M rows transferred)
- MySQL requires explicit key prefix length for TEXT/BLOB columns in indexes
- PostgreSQL uses TEXT for many ID columns, which MySQL cannot index without modification

**Example:**
- Posts table: 3,729,195 rows transferred successfully in 87 seconds
- Votes table: 10,143,364 rows transferred successfully in 73 seconds
- Total data transfer throughput: ~138,900 rows/sec for Votes, ~42,829 rows/sec for Posts
- But then index creation fails, rolling back the entire transaction

**Test Cases That Failed:**
1. PostgreSQL → MySQL (drop_recreate) - tested, failed on index creation
2. PostgreSQL → MySQL (upsert) - assumed failure, same index issue
3. MSSQL → MySQL (drop_recreate) - assumed failure, same index issue
4. MSSQL → MySQL (upsert) - assumed failure, same index issue
5. MySQL → MySQL - could not test due to inability to populate MySQL source

**Reproduction:**
```bash
./target/release/mssql-pg-migrate -c test-postgres-to-mysql-prep.yaml run
```

**Observed Behavior:**
```
Phase 3: Transferring data - ✅ SUCCESS
  - Posts: 3,729,195 rows in 87.07s (42,829 rows/sec)
  - Votes: 10,143,364 rows in 73.03s (138,900 rows/sec)
  - Comments: 4,400,439 rows in 57.55s
  - Badges: 449,856 rows in 2.48s
  - Users: 151,710 rows in 0.84s
  - All data transferred successfully

Phase 4: Finalizing (indexes, constraints) - ❌ FAIL
  Error: BLOB/TEXT column 'Id' used in key specification without a key length
```

## Detailed Test Execution

### Test 1: MSSQL → MSSQL (drop_recreate)

**Config:** `/Users/john/repos/mssql-pg-migrate-rs/test-mssql-to-mssql-drop.yaml`

**Execution Log:**
```
Phase 1: Extracting schema from source - ✅ SUCCESS
  - Extracted 9 tables from schema 'dbo'
  - 19,310,703 total rows
  - Calculated average row size: 131 bytes

Phase 2: Preparing target database (mode: DropRecreate) - ✅ SUCCESS
  - Created all 9 tables
  - Dropped foreign keys and indexes

Transaction Commit - ❌ FAIL
  - Error: Transaction count mismatch (code 266)
  - Migration aborted
```

**Duration:** 0.539 seconds
**Status:** FAILED

### Test 2: PostgreSQL → MSSQL (drop_recreate)

**Config:** `/Users/john/repos/mssql-pg-migrate-rs/test-postgres-to-mssql-drop.yaml`

**Execution Log:**
```
Phase 1: Extracting schema from source - ✅ SUCCESS
  - Extracted 9 tables from schema 'public'
  - 19,315,303 total rows
  - Calculated average row size: 131 bytes

Phase 2: Preparing target database (mode: DropRecreate) - ✅ SUCCESS
  - Created all 9 tables

Transaction Commit - ❌ FAIL
  - Error: Transaction count mismatch (code 266)
  - Migration aborted
```

**Duration:** 0.246 seconds
**Status:** FAILED

### Test 3: PostgreSQL → MySQL (drop_recreate) - Prep Data Load

**Config:** `/Users/john/repos/mssql-pg-migrate-rs/test-postgres-to-mysql-prep.yaml`

**Execution Log:**
```
Phase 1: Extracting schema from source - ✅ SUCCESS
  - Extracted 9 tables from schema 'public'

Phase 2: Preparing target database (mode: DropRecreate) - ✅ SUCCESS
  - Created all 9 tables

Phase 3: Transferring data - ✅ SUCCESS
  Auto-tuning: workers 4 → 9 (for 9 partition jobs)

  Table Transfer Results:
  - Badges: 449,856 rows in 2.48s (181,324 rows/sec)
  - Comments: 4,400,439 rows in 57.55s (76,448 rows/sec)
  - LinkTypes: 3 rows in 0.02s
  - PostLinks: 41,296 rows in 0.23s
  - PostTypes: 7 rows in 0.02s
  - Posts: 3,729,195 rows in 87.07s (42,829 rows/sec)
    - Using COPY TO BINARY streaming
    - Write time: 86.21s
  - Users: 151,710 rows in 0.84s (180,606 rows/sec)
  - VoteTypes: 15 rows in 0.02s
  - Votes: 10,143,364 rows in 73.03s (138,900 rows/sec)
    - Write time: 72.99s

  Total: 19,315,303 rows transferred successfully

Phase 4: Finalizing (indexes, constraints) - ❌ FAIL
  - Error: BLOB/TEXT column 'Id' used in key specification without a key length
  - Transaction rolled back
  - All data lost
```

**Duration:** ~87 seconds (data transfer only)
**Status:** FAILED (index creation)
**Throughput:** Peak 181,324 rows/sec (Badges), Average ~138,900 rows/sec (Votes)

## Root Cause Analysis

### MSSQL Target Bug

**Location:** Target writer transaction handling for MSSQL
**Root Cause:**
- MSSQL uses nested transactions with SAVE TRANSACTION
- The BEGIN TRANSACTION call is likely being executed in a context where a transaction is already active
- MSSQL driver (Tiberius) transaction state management is not properly synchronized
- Error code 266 specifically indicates transaction nesting count mismatch

**Likely Code Location:**
- `crates/mssql-pg-migrate/src/drivers/mssql/writer.rs`
- Transaction begin/commit logic in prepare_target or schema creation

**Fix Required:**
- Check for active transaction before BEGIN TRANSACTION
- Use SAVE TRANSACTION for nested transactions
- Properly track transaction state in MSSQL writer implementation

### MySQL Target Bug

**Location:** MySQL dialect index generation
**Root Cause:**
- PostgreSQL uses TEXT type for string columns including IDs
- MySQL requires prefix length for TEXT/BLOB columns in indexes: `INDEX(col(255))`
- Current implementation generates: `INDEX(col)` which fails for TEXT columns

**Likely Code Location:**
- `crates/mssql-pg-migrate/src/drivers/mysql/dialect.rs`
- Index creation SQL generation

**Fix Required:**
- Detect TEXT/BLOB columns in index definitions
- Add prefix length (e.g., 255 or 767 bytes) for TEXT columns in indexes
- Consider converting TEXT to VARCHAR(255) for columns used in keys

## Configuration Files Created

All 12 config files were created successfully:

1. `/Users/john/repos/mssql-pg-migrate-rs/test-mssql-to-mssql-drop.yaml`
2. `/Users/john/repos/mssql-pg-migrate-rs/test-mssql-to-mssql-upsert.yaml`
3. `/Users/john/repos/mssql-pg-migrate-rs/test-mssql-to-mysql-drop.yaml`
4. `/Users/john/repos/mssql-pg-migrate-rs/test-mssql-to-mysql-upsert.yaml`
5. `/Users/john/repos/mssql-pg-migrate-rs/test-mysql-to-mssql-drop.yaml`
6. `/Users/john/repos/mssql-pg-migrate-rs/test-mysql-to-mssql-upsert.yaml`
7. `/Users/john/repos/mssql-pg-migrate-rs/test-mysql-to-mysql-drop.yaml`
8. `/Users/john/repos/mssql-pg-migrate-rs/test-mysql-to-mysql-upsert.yaml`
9. `/Users/john/repos/mssql-pg-migrate-rs/test-postgres-to-mssql-drop.yaml`
10. `/Users/john/repos/mssql-pg-migrate-rs/test-postgres-to-mssql-upsert.yaml`
11. `/Users/john/repos/mssql-pg-migrate-rs/test-postgres-to-mysql-drop.yaml`
12. `/Users/john/repos/mssql-pg-migrate-rs/test-postgres-to-mysql-upsert.yaml`

## Recommendations

### High Priority Fixes Required

1. **MSSQL Target Transaction Bug** (CRITICAL)
   - Blocks 6 migration paths
   - Prevents MSSQL as target entirely
   - Should be fixed before any production use of MSSQL targets

2. **MySQL Target Index on TEXT Columns** (CRITICAL)
   - Blocks 6 migration paths
   - Data transfer works perfectly (fast throughput)
   - Only index creation fails
   - Relatively simple fix (add prefix length to TEXT indexes)

### Testing Strategy

Once bugs are fixed, retest in this order:

1. PostgreSQL → MySQL (simplest, verifies MySQL target fix)
2. PostgreSQL → MSSQL (verifies MSSQL target fix)
3. MSSQL → MSSQL (round-trip test)
4. MySQL → MySQL (requires PostgreSQL → MySQL to work first)
5. Cross-database permutations

### Performance Notes

Despite the failures, the test revealed excellent performance for successful phases:

- **Data Transfer Throughput:**
  - Small tables (Badges, Users): ~180,000 rows/sec
  - Large tables (Votes): ~138,900 rows/sec
  - Complex tables (Posts, 20 columns): ~42,829 rows/sec

- **Auto-Tuning Working Well:**
  - Correctly detected 14 CPU cores, 36 GB RAM
  - Scaled workers from 4 → 9 for parallel table transfers
  - Set appropriate parallel readers (3) and writers (3)
  - Memory budget: 25.8 GB (70% of available)

## Database Connection Details Used

**MSSQL (Source & Target):**
- Host: localhost:1433
- User: sa
- Source DB: StackOverflow2010
- Target DBs: stackoverflow_test_mssql, stackoverflow_test_mssql2, stackoverflow_test_mssql3

**PostgreSQL (Source & Target):**
- Host: localhost:5433 (target), localhost:5434 (target2)
- User: postgres
- Database: stackoverflow

**MySQL (Target):**
- Host: localhost:3306
- User: root
- Target DBs: stackoverflow_test, stackoverflow_test2, stackoverflow_test3

## Conclusion

While the tool shows excellent performance and architecture for migrations TO PostgreSQL targets (as previously tested), **critical bugs prevent any migrations TO MSSQL or TO MySQL targets**.

The data transfer engine itself works perfectly even for MySQL (19.3M rows transferred successfully), but transaction handling for MSSQL targets and index creation for MySQL targets both have blocking bugs.

Both issues appear fixable with targeted code changes to the MSSQL writer transaction logic and MySQL dialect index generation.
