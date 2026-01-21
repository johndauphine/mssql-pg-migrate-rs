# Security Review: Date-Based Incremental Sync Feature

**Reviewer**: Claude Code (Senior Data Engineer)
**Date**: 2026-01-20
**Feature**: Date-based incremental sync for upsert mode (high-water mark pattern)
**Scope**: Security vulnerabilities, correctness issues, optimization opportunities

---

## Executive Summary

The date-based incremental sync feature implements a high-water mark pattern that significantly improves performance for incremental syncs by filtering source data using timestamp columns. While the feature logic is sound, **two critical security vulnerabilities require immediate attention**:

1. **SQL Injection via timestamp string interpolation** (CRITICAL)
2. **Missing state file integrity validation** (HIGH)

Additionally, timezone handling has correctness implications that could lead to subtle data loss scenarios.

---

## Feature Overview

### Implementation Flow

1. **Configuration**: User provides `date_updated_columns` list in YAML (e.g., `["LastActivityDate", "ModifiedDate"]`)
2. **Column Discovery**: For each table, search for first matching timestamp column (case-insensitive)
3. **Watermark Retrieval**: Load `last_sync_timestamp` from state file (JSON)
4. **Query Construction**: Build WHERE clause: `(column > 'timestamp' OR column IS NULL)`
5. **Transfer**: Execute filtered query, transfer only changed rows
6. **Watermark Update**: Save sync start time to state file on successful completion

### Affected Files

- `/Users/john/repos/mssql-pg-migrate-rs/crates/mssql-pg-migrate/src/config/types.rs:432-438` - Config definition
- `/Users/john/repos/mssql-pg-migrate-rs/crates/mssql-pg-migrate/src/state/mod.rs:69-180` - State management
- `/Users/john/repos/mssql-pg-migrate-rs/crates/mssql-pg-migrate/src/source/types.rs:149-180` - Column discovery
- `/Users/john/repos/mssql-pg-migrate-rs/crates/mssql-pg-migrate/src/transfer/mod.rs:22-30, 1257-1268, 1350-1357` - Filter application
- `/Users/john/repos/mssql-pg-migrate-rs/crates/mssql-pg-migrate/src/orchestrator/mod.rs:930-990, 1177-1230` - Orchestration

---

## Critical Security Vulnerabilities

### 1. SQL Injection via Timestamp String Interpolation 🔴 CRITICAL

**Location**: `transfer/mod.rs:1263-1268` and `1352-1357`

**Vulnerable Code**:
```rust
let timestamp_str = filter.timestamp.to_rfc3339();
conditions.push(format!(
    "({} > '{}' OR {} IS NULL)",
    date_quoted, timestamp_str, date_quoted
));
```

**Vulnerability Analysis**:
- Timestamp value is **directly interpolated** into SQL query string
- While `to_rfc3339()` produces formatted output, this is **not parameterization**
- The `DateTime<Utc>` value originates from JSON deserialization (state file)
- An attacker with file system access can modify the state file to inject SQL

**Attack Vector**:
```json
// Malicious state file:
{
  "tables": {
    "dbo.Users": {
      "last_sync_timestamp": "2024-01-01T00:00:00Z' OR '1'='1"
    }
  }
}
```

**Resulting Query**:
```sql
SELECT * FROM dbo.Users
WHERE (LastActivityDate > '2024-01-01T00:00:00Z' OR '1'='1' OR LastActivityDate IS NULL)
ORDER BY UserId
```

**Impact**:
- Bypasses date filter → full table scan instead of incremental
- Performance degradation (DoS)
- Potential data exfiltration if combined with other injection techniques
- Data integrity issues if attacker skips data ranges

**Fix Required** ✅:

Replace string interpolation with parameterized queries:

```rust
// For PostgreSQL
let mut params = Vec::new();
if let Some(filter) = date_filter {
    let date_quoted = quote_pg_ident(&filter.column);
    params.push(filter.timestamp); // Add to parameter list
    conditions.push(format!(
        "({} > $1 OR {} IS NULL)",
        date_quoted, date_quoted
    ));
}

// Execute with parameters
let rows = source.query_rows_parameterized(&query, &params, columns, col_types).await?;
```

```rust
// For MSSQL
let mut params = Vec::new();
if let Some(filter) = date_filter {
    let date_quoted = quote_mssql_ident(&filter.column);
    params.push(filter.timestamp);
    conditions.push(format!(
        "({} > @p1 OR {} IS NULL)",
        date_quoted, date_quoted
    ));
}
```

**Additional Hardening**:
1. Add timestamp validation during deserialization
2. Sanitize timestamp to ensure it's valid `DateTime<Utc>` format
3. Add bounds checking (reject future dates, unreasonably old dates)

---

### 2. State File Integrity Validation Missing 🔴 HIGH

**Location**: `state/mod.rs:113-117`

**Vulnerable Code**:
```rust
pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
    let content = std::fs::read_to_string(path)?;
    let state: Self = serde_json::from_str(&content)?;
    Ok(state)
}
```

**Vulnerability Analysis**:
- No cryptographic signature or HMAC validation
- State file is trusted implicitly after JSON parsing
- Attacker with file system access can modify:
  - `last_sync_timestamp` to bypass date filter
  - Other state fields to corrupt migration state

**Attack Scenarios**:

1. **Timestamp Manipulation**:
   ```json
   {
     "last_sync_timestamp": "2030-01-01T00:00:00Z"  // Future date
   }
   ```
   Result: All rows skipped (believe everything is already synced)

2. **Rollback Attack**:
   ```json
   {
     "last_sync_timestamp": "2020-01-01T00:00:00Z"  // Old date
   }
   ```
   Result: Force full re-sync (performance DoS)

3. **SQL Injection** (via vulnerability #1):
   Combine with timestamp injection for full exploitation

**Current Mitigation**: Limited - only `config_hash` validation exists (lines 134-138), but it doesn't protect state data itself.

**Fix Required** ✅:

Implement HMAC-based integrity protection:

```rust
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn save<P: AsRef<Path>>(&self, path: P, config_hash: &str) -> Result<()> {
    let path = path.as_ref();
    let content = serde_json::to_string_pretty(self)?;

    // Generate HMAC using config_hash as key
    let mut mac = HmacSha256::new_from_slice(config_hash.as_bytes())
        .map_err(|e| MigrateError::Config(format!("HMAC init failed: {}", e)))?;
    mac.update(content.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());

    // Append signature
    let signed_content = format!("{}\n---SIGNATURE---\n{}", content, signature);

    // Atomic write
    let temp_path = path.with_extension("tmp");
    std::fs::write(&temp_path, &signed_content)?;
    std::fs::rename(&temp_path, path)?;

    Ok(())
}

pub fn load<P: AsRef<Path>>(path: P, config_hash: &str) -> Result<Self> {
    let content = std::fs::read_to_string(path)?;

    // Split content and signature
    let parts: Vec<&str> = content.split("\n---SIGNATURE---\n").collect();
    if parts.len() != 2 {
        return Err(MigrateError::Config("Invalid state file format".to_string()));
    }

    let (payload, expected_sig) = (parts[0], parts[1].trim());

    // Verify HMAC
    let mut mac = HmacSha256::new_from_slice(config_hash.as_bytes())
        .map_err(|e| MigrateError::Config(format!("HMAC init failed: {}", e)))?;
    mac.update(payload.as_bytes());

    let actual_sig = hex::encode(mac.finalize().into_bytes());
    if actual_sig != expected_sig {
        return Err(MigrateError::Config("State file integrity check failed - possible tampering detected".to_string()));
    }

    // Deserialize and validate
    let mut state: Self = serde_json::from_str(payload)?;

    // Sanitize timestamps
    for table in state.tables.values_mut() {
        if let Some(ts) = table.last_sync_timestamp {
            validate_timestamp(ts)?;
        }
    }

    Ok(state)
}

fn validate_timestamp(ts: DateTime<Utc>) -> Result<()> {
    let now = Utc::now();
    let min_valid = now - chrono::Duration::days(3650); // 10 years max

    if ts > now + chrono::Duration::hours(24) {
        return Err(MigrateError::Config(
            format!("Timestamp is in the future: {}", ts.to_rfc3339())
        ));
    }
    if ts < min_valid {
        return Err(MigrateError::Config(
            format!("Timestamp is unreasonably old: {}", ts.to_rfc3339())
        ));
    }
    Ok(())
}
```

**Dependencies Required**:
```toml
# Add to Cargo.toml
hmac = "0.12"
sha2 = "0.10"
hex = "0.4"
```

---

### 3. Column Name Validation (Debug Assertions Only) 🟡 MEDIUM

**Location**: `transfer/mod.rs:1594-1603, 1670-1679`

**Current Protection**:
```rust
fn quote_mssql_ident(name: &str) -> String {
    debug_assert!(!name.contains('\0'), "Identifier contains null byte: {:?}", name);
    debug_assert!(name.len() <= 128, "Identifier exceeds maximum length (128): {}", name.len());

    format!("[{}]", name.replace(']', "]]"))
}
```

**Issue**:
- Security-critical validation uses `debug_assert!` which is **stripped in release builds**
- Column names from user config are validated only in debug mode
- Production builds have no protection against malformed identifiers

**Attack Scenario** (requires compromised schema metadata or config):
```yaml
migration:
  date_updated_columns:
    - "UpdatedAt]; DROP TABLE Users; --"
```

**Current Mitigation**:
- Identifier quoting (brackets/quotes) provides some protection
- Requires schema metadata or config compromise (less likely)

**Fix Recommended** ✅:

Convert to runtime validation:

```rust
fn validate_identifier(name: &str, context: &str) -> Result<()> {
    if name.is_empty() {
        return Err(MigrateError::Config(
            format!("{} cannot be empty", context)
        ));
    }

    if name.len() > 128 {
        return Err(MigrateError::Config(
            format!("{} exceeds maximum length (128): {}", context, name.len())
        ));
    }

    if name.contains('\0') {
        return Err(MigrateError::Config(
            format!("{} contains null byte", context)
        ));
    }

    // Detect SQL injection patterns
    if name.contains("--") || name.contains("/*") || name.contains(';') {
        return Err(MigrateError::Config(
            format!("{} contains suspicious SQL pattern: {}", context, name)
        ));
    }

    Ok(())
}

fn quote_mssql_ident(name: &str) -> Result<String> {
    validate_identifier(name, "MSSQL identifier")?;
    Ok(format!("[{}]", name.replace(']', "]]")))
}

fn quote_pg_ident(name: &str) -> Result<String> {
    validate_identifier(name, "PostgreSQL identifier")?;
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}
```

---

## Correctness Issues

### 4. Timezone Handling Ambiguity 🟡 MEDIUM

**Location**: `transfer/mod.rs:1263`, `source/types.rs:167-180`

**Issue**: The code uses UTC timestamps (`to_rfc3339()` produces `2024-01-20T12:00:00Z`) but allows filtering on timezone-naive columns:

**Supported Column Types**:
```rust
fn is_date_type(data_type: &str) -> bool {
    matches!(
        data_type,
        "datetime"                  // MSSQL - no timezone ❌
        | "datetime2"               // MSSQL - no timezone ❌
        | "datetimeoffset"          // MSSQL - with timezone ✅
        | "timestamp"               // PostgreSQL - depends on variant
        | "timestamptz"             // PostgreSQL - with timezone ✅
        | "timestamp with time zone"    // PostgreSQL - with timezone ✅
        | "timestamp without time zone" // PostgreSQL - no timezone ❌
    )
}
```

**Problem**: Comparing UTC timestamp with non-TZ column leads to undefined behavior:

```sql
-- Source column: datetime (no TZ) stores "2024-01-20 12:00:00"
-- Filter compares with: '2024-01-20T12:00:00Z' (UTC)
-- Result: Depends on database's default timezone interpretation!
```

**Data Loss Scenario**:

1. **Database in PST** (UTC-8)
2. **Application in UTC**
3. **Column type**: `datetime` (no timezone)
4. **Row inserted**: `2024-01-20 12:00:00` (local PST = 20:00:00 UTC)
5. **Sync starts**: `2024-01-20T19:00:00Z` (UTC)
6. **Filter**: `datetime > '2024-01-20T19:00:00Z'`
7. **Database interprets** filter as PST: `2024-01-20 19:00:00` (local)
8. **Result**: Row at 12:00 PST is **included** (correct by accident)

But if timezone interpretation changes, data loss can occur!

**Fix Required** ✅:

1. **Restrict allowed column types** to timezone-aware only:
   ```rust
   fn is_date_type_safe(data_type: &str) -> bool {
       matches!(
           data_type,
           "datetimeoffset" | "timestamptz" | "timestamp with time zone"
       )
   }

   fn is_date_type_unsafe(data_type: &str) -> bool {
       matches!(
           data_type,
           "datetime" | "datetime2" | "timestamp without time zone" | "timestamp"
       )
   }
   ```

2. **Warn users** if timezone-naive column is used:
   ```rust
   pub fn find_date_column(&self, candidate_names: &[String]) -> Option<(String, String, bool)> {
       for name in candidate_names {
           if let Some(col) = self.columns.iter().find(|c| c.name.eq_ignore_ascii_case(name)) {
               let data_type = col.data_type.to_lowercase();
               if is_date_type_safe(&data_type) {
                   return Some((col.name.clone(), col.data_type.clone(), true));
               }
               if is_date_type_unsafe(&data_type) {
                   warn!(
                       "Table {}: column {} has timezone-naive type {}. \
                        Incremental sync may produce incorrect results if database \
                        timezone differs from UTC. Consider using datetimeoffset or timestamptz.",
                       self.full_name(), col.name, col.data_type
                   );
                   return Some((col.name.clone(), col.data_type.clone(), false));
               }
           }
       }
       None
   }
   ```

3. **Document timezone requirements** in README and config example

---

### 5. NULL Handling Re-processes Rows 🟢 LOW

**Location**: `transfer/mod.rs:1265-1268`

**Behavior**: Filter includes `OR column IS NULL`:
```sql
WHERE (LastActivityDate > '2024-01-01' OR LastActivityDate IS NULL)
```

**Impact**:
- Rows with NULL timestamps are **always included** on every sync
- Performance degradation if table has many NULL values
- Functionally correct (safer to include than exclude)

**Example**:
- Table has 1M rows: 900K with timestamps, 100K with NULL
- Every incremental sync processes all 100K NULL rows
- Negates benefit of incremental sync

**Recommendation**:
1. **Document this behavior** in README
2. **Add config option** to control NULL handling:
   ```yaml
   migration:
     date_filter_include_nulls: true  # Default: true for safety
   ```
3. **Track NULL rows** in state to avoid warning about re-processing

---

## Optimization Opportunities

### 6. Missing Index Detection 🟢 LOW

**Observation**: Date filter queries can trigger full table scans without proper indexing:

```sql
SELECT * FROM dbo.Users WITH (NOLOCK)
WHERE (LastActivityDate > '2024-01-01' OR LastActivityDate IS NULL)
ORDER BY UserId
```

**Performance Impact**: Without index on `LastActivityDate`, this is an O(n) table scan.

**Recommendation**:

Add index detection and warning:

```rust
pub async fn check_date_column_index(
    &self,
    schema: &str,
    table: &str,
    column: &str
) -> Result<bool> {
    // For MSSQL
    let query = format!(
        "SELECT COUNT(*) FROM sys.indexes i \
         JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id \
         JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id \
         WHERE i.object_id = OBJECT_ID('{}.{}') AND c.name = '{}'",
        schema, table, column
    );

    let result: i32 = self.query_scalar(&query).await?;
    Ok(result > 0)
}

// In orchestrator, after finding date column:
if !source.check_date_column_index(&table.schema, &table.name, &date_col).await? {
    warn!(
        "Table {}: date filter column '{}' is not indexed. \
         Performance will be poor for large tables. \
         Recommend: CREATE INDEX idx_{}_{} ON {}.{} ({})",
        table.full_name(), date_col,
        table.name, date_col,
        table.schema, table.name, date_col
    );
}
```

---

### 7. Redundant Date Column Lookup 🟢 LOW

**Location**: Multiple lookups in orchestrator and transfer

**Issue**: `find_date_column()` is called multiple times with same input

**Fix**: Cache resolved column name in `TransferJob`:
```rust
pub struct TransferJob {
    // ...
    pub date_column_name: Option<String>,  // Pre-resolved
    pub date_filter: Option<DateFilter>,
}
```

---

## Testing Recommendations

### Security Tests Required

1. **SQL Injection Tests**:
   ```rust
   #[test]
   fn test_timestamp_injection_protection() {
       // Attempt to inject SQL via malicious timestamp
       let malicious_state = r#"
       {
           "tables": {
               "dbo.Users": {
                   "last_sync_timestamp": "2024-01-01' OR '1'='1"
               }
           }
       }"#;

       // Should fail to parse or sanitize
       let result = MigrationState::load_from_str(malicious_state);
       assert!(result.is_err());
   }
   ```

2. **State Tampering Tests**:
   ```rust
   #[test]
   fn test_state_integrity_validation() {
       let state = MigrationState::new(/*...*/);
       state.save("test.json", "config_hash").unwrap();

       // Tamper with file
       let mut content = fs::read_to_string("test.json").unwrap();
       content = content.replace("2024-01-01", "2030-01-01");
       fs::write("test.json", content).unwrap();

       // Should fail integrity check
       let result = MigrationState::load("test.json", "config_hash");
       assert!(result.is_err());
   }
   ```

### Correctness Tests Required

1. **Timezone Tests**:
   ```rust
   #[tokio::test]
   async fn test_timezone_aware_columns_only() {
       // Should accept datetimeoffset
       assert!(is_date_type_safe("datetimeoffset"));

       // Should warn on datetime
       assert!(is_date_type_unsafe("datetime"));
   }
   ```

2. **NULL Handling Tests**:
   ```rust
   #[tokio::test]
   async fn test_null_timestamp_handling() {
       // Verify NULL rows are included
       // Verify behavior with include_nulls = false
   }
   ```

---

## Remediation Priority

### Immediate (Week 1)
1. ✅ **Fix SQL injection via parameterized queries** (#1)
2. ✅ **Add state file HMAC validation** (#2)
3. ✅ **Convert debug assertions to runtime checks** (#3)

### High Priority (Week 2)
4. ✅ **Add timezone type validation and warnings** (#4)
5. ✅ **Add timestamp bounds validation**
6. ✅ **Write security test suite**

### Medium Priority (Week 3-4)
7. ✅ **Document NULL handling behavior** (#5)
8. ✅ **Add index detection warnings** (#6)
9. ✅ **Add timezone guidance to documentation**

### Optional
10. Cache date column resolution (#7)
11. Add config option for NULL handling

---

## Conclusion

The date-based incremental sync feature provides significant performance benefits but has critical security vulnerabilities that must be addressed before production use:

- **SQL Injection**: Replace string interpolation with parameterized queries
- **State Integrity**: Add HMAC validation to prevent tampering
- **Timezone Issues**: Restrict to timezone-aware columns and add warnings
- **Runtime Validation**: Convert debug assertions to proper error handling

With these fixes, the feature will be secure and correct for production deployment.

---

**Sign-off**: Senior Data Engineer Review Completed
**Next Steps**: Implement fixes in priority order, run security test suite, update documentation
