# Upsert Update Detection Bug Investigation

**Date**: 2025-12-28
**Branch**: `fix/tier1-hash-comparison` (partial implementation, may be deleted)
**Status**: Investigation complete, fix requires different approach

## Summary

The multi-tier batch verification system fails to detect row **updates** when running in upsert mode. It correctly detects inserts and deletes, but updates (where row content changes without affecting row count) are missed entirely.

## Problem Description

### Observed Behavior

During testing with StackOverflow 2013 dataset (~106M rows):

1. **Drop/recreate migration** completed successfully
2. **Upsert with no changes** correctly detected 0 rows needing sync
3. **Upsert with modified rows** failed to detect updates:
   - Modified 200 Badges (Name field) → **0 detected** (0/9 partitions mismatched)
   - Modified 50 Posts (Title field) → **0 detected** (0/18 partitions mismatched)
   - Modified 100 Users + inserted 3 → **1,905 detected** (inserts detected, updates may have been missed)

### Root Cause

**Tier 1 and Tier 2 only compare row counts, not data content.**

When source and target have the same row count in a partition, the verification short-circuits and returns "fully synchronized" without ever comparing the actual data hashes.

## Technical Details

### Multi-Tier Verification Architecture

```
Tier 1: NTILE Partition Counts
    ↓ (mismatched partitions only)
Tier 2: ROW_NUMBER Range Counts
    ↓ (mismatched ranges only)
Tier 3: Row-Level Hash Comparison
```

The optimization relies on each tier filtering down to only mismatched regions. However, Tier 1 and Tier 2 filter on **count only**, so updates (which don't change counts) are never passed to Tier 3.

### Affected Code

#### `crates/mssql-pg-migrate/src/verify/universal.rs`

**Original `compare_partition_counts` (lines 379-410):**
```rust
async fn compare_partition_counts(
    &self,
    source_partitions: &[(i64, i64)],  // (partition_id, count)
    target_partitions: &[(i64, i64)],
    ranges: &[RowRange],
) -> Vec<RowRange> {
    // ...
    for range in ranges {
        if let Some(partition_id) = range.partition_id {
            let source_count = source_map.get(&partition_id).copied().unwrap_or(0);
            let target_count = target_map.get(&partition_id).copied().unwrap_or(0);

            if source_count != target_count {  // <-- ONLY checks count!
                mismatches.push(range.clone());
            }
        }
    }
    mismatches
}
```

**Short-circuit code (lines 252-272):**
```rust
if tier1_mismatches.is_empty() {
    info!("Table {} is fully synchronized", table_name);
    return Ok((
        TableVerifyResult {
            rows_to_insert: 0,  // Never reaches Tier 2/3
            rows_to_update: 0,
            rows_to_delete: 0,
            // ...
        },
        RowHashDiffComposite::default(),
    ));
}
```

#### `crates/mssql-pg-migrate/src/verify/hash_query.rs`

NTILE queries only return `(partition_id, row_count)`:
```sql
-- MSSQL
SELECT partition_id, CAST(COUNT(*) AS BIGINT) AS row_count
FROM partitioned GROUP BY partition_id

-- PostgreSQL
SELECT partition_id, COUNT(*)::BIGINT AS row_count
FROM partitioned GROUP BY partition_id
```

## Attempted Fix

### Approach

Modified Tier 1 and Tier 2 to compute aggregate hashes per partition/range:

**MSSQL:**
```sql
SELECT
    partition_id,
    CAST(COUNT(*) AS BIGINT) AS row_count,
    CAST(CHECKSUM_AGG(BINARY_CHECKSUM(*)) AS BIGINT) AS partition_hash
FROM partitioned
GROUP BY partition_id
```

**PostgreSQL:**
```sql
SELECT
    partition_id,
    COUNT(*)::BIGINT AS row_count,
    COALESCE(bit_xor(hashtext(t::text)::bigint), 0)::BIGINT AS partition_hash
FROM partitioned
GROUP BY partition_id
```

### Why It Failed

**Hash algorithm mismatch**: MSSQL's `BINARY_CHECKSUM` and PostgreSQL's `hashtext` produce completely different values for identical data.

Result: **All partitions show as mismatched**, defeating the entire tiered optimization. A 106M row table that should take seconds to verify would take hours because every single row goes to Tier 3.

### Files Modified (partial fix)

- `crates/mssql-pg-migrate/src/verify/universal.rs` - Added hash comparison
- `crates/mssql-pg-migrate/src/verify/hash_query.rs` - Added partition_hash to queries
- `crates/mssql-pg-migrate/src/source/mod.rs` - Return `(partition_id, count, hash)` tuples
- `crates/mssql-pg-migrate/src/target/mod.rs` - Same changes for PostgreSQL

### Additional Issue Fixed

Type conversion panic when reading MSSQL results:
```
cannot interpret I32(Some(10000)) as an i64 value
```

Fixed by trying i32 first, then i64:
```rust
let row_count: i64 = row.get::<i32, _>(0)
    .map(|v| v as i64)
    .or_else(|| row.get::<i64, _>(0))
    .unwrap_or(0);
```

## Recommended Solution

### Use the Stored `row_hash` Column

The PostgreSQL target already has a `row_hash` column that stores MD5 hashes computed using the **same algorithm** as MSSQL during migration (via `mssql_row_hash_expr`).

**Corrected approach:**

1. **MSSQL Tier 1/2**: Compute XOR aggregate using existing `mssql_row_hash_expr`:
   ```sql
   SELECT
       partition_id,
       COUNT(*) AS row_count,
       -- XOR aggregate of MD5 hashes (same algorithm as stored row_hash)
       CHECKSUM_AGG(CHECKSUM(CONVERT(VARCHAR(32), HASHBYTES('MD5', concat_expr), 2))) AS partition_hash
   FROM partitioned
   GROUP BY partition_id
   ```

2. **PostgreSQL Tier 1/2**: Use XOR of stored `row_hash`:
   ```sql
   SELECT
       partition_id,
       COUNT(*) AS row_count,
       bit_xor(('x' || left(row_hash, 16))::bit(64)::bigint) AS partition_hash
   FROM partitioned
   GROUP BY partition_id
   ```

3. **Handle NULL row_hash**: After `drop_recreate` mode, `row_hash` may be NULL on PostgreSQL. Options:
   - Treat NULL as "needs full verification" (fall through to Tier 3)
   - Backfill row_hash during target preparation
   - Use a sentinel hash value for NULLs

### Benefits of This Approach

- Uses **identical hash algorithm** on both sides
- Leverages existing `row_hash` infrastructure
- XOR is commutative (order doesn't matter)
- Can still detect updates efficiently

### Considerations

- **Performance**: May need to test XOR aggregate performance on large tables
- **NULL handling**: Must decide on strategy for missing `row_hash` values
- **Bidirectional migration**: If implementing PG→PG or MSSQL→MSSQL, need to ensure `row_hash` is available or compute it on-the-fly

## Test Data Used

Modified rows in MSSQL StackOverflow2013:
```sql
-- 100 Users updated
UPDATE TOP (100) Users SET Location = 'TEST-UPDATED-LOCATION'

-- 50 Posts updated
UPDATE TOP (50) Posts SET Title = 'TEST-UPDATED-TITLE: ' + Title

-- 200 Badges updated
UPDATE TOP (200) Badges SET Name = 'TEST-' + Name

-- 3 Users inserted
INSERT INTO Users (DisplayName, Location, Reputation, UpVotes, DownVotes)
VALUES ('TestUser1', 'TestCity1', 100, 10, 1), ...
```

## Related Files

| File | Purpose |
|------|---------|
| `crates/mssql-pg-migrate/src/verify/universal.rs` | Main verification logic |
| `crates/mssql-pg-migrate/src/verify/hash_query.rs` | SQL query generation |
| `crates/mssql-pg-migrate/src/source/mod.rs` | MSSQL pool operations |
| `crates/mssql-pg-migrate/src/target/mod.rs` | PostgreSQL pool operations |
| `crates/mssql-pg-migrate/src/transfer/mod.rs` | Row hash computation |

## Conclusion

The tiered verification optimization is fundamentally sound, but the implementation incorrectly assumes that matching row counts mean matching data. The fix requires adding hash comparison to Tier 1 and Tier 2, using a compatible hash algorithm on both sides (the stored `row_hash` column).
