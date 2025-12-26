# Gemini Code Review Findings

**Review Date:** 2025-12-25
**Reviewer:** Gemini CLI via Claude Code
**Codebase:** mssql-pg-migrate-rs

---

## CRITICAL Issues (Must Fix)

### 1. Data Loss Risk in Parallel Resume Logic
**File:** `crates/mssql-pg-migrate/src/transfer/mod.rs` (Lines 286-290)

**Problem:** When using parallel readers, chunks arrive out-of-order. The code uses `min()` to track the last PK, but if Reader B (PK 2000-3000) completes before Reader A (PK 0-2000) and the process crashes, resuming will skip rows 0-2000.

**Impact:** Permanent data loss during resume operations.

**Recommendation:** Implement a range-based tracking system (IntervalTree or RangeSet) to only advance the safe resume point when contiguous ranges from the start are complete.

---

### 2. Plaintext Database Credentials
**File:** `crates/mssql-pg-migrate/src/target/mod.rs` (Line 178)

**Problem:** Connection uses `NoTls`, transmitting passwords and data in plaintext. All database traffic including credentials is unencrypted, vulnerable to MITM attacks.

**Recommendation:**
```rust
use postgres_native_tls::MakeTlsConnector;
use native_tls::TlsConnector;

let connector = TlsConnector::builder().build().unwrap();
let connector = MakeTlsConnector::new(connector);
let mgr = Manager::from_config(pg_config, connector, mgr_config);
```

---

### 3. Password Exposure in Logs
**File:** `crates/mssql-pg-migrate/src/config/types.rs` (Lines 115, 151)

**Problem:** `SourceConfig` and `TargetConfig` derive `Debug`, exposing passwords in logs.

**Recommendation:** Implement custom `Debug` to mask passwords or use the `secrecy` crate:
```rust
impl fmt::Debug for SourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &"[REDACTED]")
            .field("schema", &self.schema)
            .field("encrypt", &self.encrypt)
            .field("trust_server_cert", &self.trust_server_cert)
            .finish()
    }
}
```

---

### 4. State File Never Saved During Execution
**File:** `crates/mssql-pg-migrate/src/orchestrator/mod.rs` (Lines 671-965)

**Problem:** State is only saved before starting transfers and after ALL complete. If the process crashes mid-migration, resume functionality is useless because progress isn't checkpointed.

**Recommendation:** Use `JoinSet` to process results as they complete and periodically save state:
```rust
use tokio::task::JoinSet;

while let Some(res) = join_set.join_next().await {
    // Process result
    // Update state
    if tables_completed % 10 == 0 {
        self.save_state(state)?; // Periodic checkpoint
    }
}
```

---

## HIGH Severity Issues

### 5. Off-by-One Error in PK Range
**File:** `crates/mssql-pg-migrate/src/transfer/mod.rs` (Line 695)

**Problem:** Query uses `WHERE pk > last_pk`, but if `last_pk == job.min_pk`, rows with `PK == min_pk` are skipped.

**Recommendation:** Use `>=` for the first chunk or ensure `min_pk` is exclusive.

---

### 6. Resource Exhaustion: Temp Table Churn
**File:** `crates/mssql-pg-migrate/src/target/mod.rs` (Lines 752-770)

**Problem:** `upsert_chunk` creates/drops a temp table for EVERY batch. Creates system catalog bloat, metadata lock contention, excessive WAL generation.

**Recommendation:** Create staging table once per table migration, truncate between batches.

---

### 7. 32-bit Overflow Risk
**File:** `crates/mssql-pg-migrate/src/config/types.rs` (Line 328)

**Problem:** Casting `u64` to `usize` for memory calculations will panic on 32-bit systems with >4GB RAM.

**Recommendation:** Keep calculations in `u64`, cast to `usize` only when necessary with saturation.

---

## MEDIUM Severity Issues

### 8. Incomplete Error Context Preservation
**File:** `crates/mssql-pg-migrate/src/error.rs` (Lines 31-32)

**Problem:** `Pool` error variant drops the underlying error source.

**Recommendation:**
```rust
#[error("Pool error: {message}")]
Pool {
    message: String,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
},
```

---

### 9. Incomplete Configuration Validation
**File:** `crates/mssql-pg-migrate/src/config/validation.rs` (Lines 53-62)

**Problem:** Only validates `workers` and `chunk_size` for zero, but ignores other optional numeric fields. User can set `parallel_readers: Some(0)` which will cause runtime panics.

**Recommendation:** Validate all `Option<usize>` fields when `Some`.

---

### 10. Sequential Task Await Blocks Progress
**File:** `crates/mssql-pg-migrate/src/orchestrator/mod.rs` (Lines 835-918)

**Problem:** Results collected serially - if first table takes 1 hour, fast tables wait.

**Impact:** Delayed progress reporting, slow error detection.

**Recommendation:** Use `JoinSet` or `futures::select_all` to process completions as they occur.

---

### 11. Incomplete Graceful Shutdown
**File:** `crates/mssql-pg-migrate/src/orchestrator/mod.rs` (Line 720)

**Problem:** Cancellation only prevents NEW tasks from spawning, doesn't stop running tasks.

**Recommendation:** Pass `CancellationToken` into transfer tasks or track handles and call `.abort()`.

---

### 12. Unbounded Memory Growth Risk
**File:** `crates/mssql-pg-migrate/src/transfer/mod.rs`

**Problem:** 4 readers × 16 buffers × 50K rows = 3.2M rows buffered (~3.2GB with 1KB rows).

**Recommendation:** Implement global memory limiter or reduce defaults (chunk_size to 5K).

---

## Performance & Code Quality Issues

### 13. SQL Construction Without Parameterization
**File:** `crates/mssql-pg-migrate/src/transfer/mod.rs` (Lines 695-699)

**Problem:** Using `format!` for SQL instead of prepared statements.

**Impact:** Bypasses query plan cache, fragile escaping.

---

### 14. Inefficient Double-Hopping Pipeline
**File:** `crates/mssql-pg-migrate/src/transfer/mod.rs` (Lines 167-170)

**Problem:** Data flows Reader → tokio::mpsc → Dispatcher → async_channel → Writer.

**Impact:** Unnecessary context switching and latency.

**Recommendation:** Pass write channel directly to readers.

---

### 15. Heavy System Resource Detection
**File:** `crates/mssql-pg-migrate/src/config/types.rs` (Line 21)

**Problem:** `System::new_all()` parses all processes, disks, networks.

**Recommendation:** Use `System::new()` + `refresh_memory()` + `refresh_cpu()` only.

---

### 16. Sequential Writer Await
**File:** `crates/mssql-pg-migrate/src/transfer/mod.rs` (Lines 322-336)

**Problem:** Writers joined sequentially - if Writer 1 hangs, Writer 2's errors not reported promptly.

**Recommendation:** Use `futures::future::try_join_all()` for fail-fast behavior.

---

## Security Audit Findings

### 17. CHECK Constraint Parser Fragility
**File:** `crates/mssql-pg-migrate/src/target/mod.rs` (Lines 268-291)

**Problem:** Manual bracket parsing could corrupt SQL with nested brackets or string literals.

**Recommendation:** Consider regex-based approach or warn about complex constraints.

---

## Best Practices & Improvements

1. **Error recoverable classification**: Transfer errors marked non-recoverable, but transient network issues during COPY should be retryable.

2. **Exit code semantics**: Using exit code 5 for cancellation is fine, but Unix convention is 128+signal_number (130 for SIGINT).

3. **Partition assumptions**: PK range splitting assumes uniform distribution - sparse ranges cause load imbalance.

---

## Testing Recommendations

Based on the findings, critical test cases needed:

1. **Resume with parallel readers** - Verify no data loss on crash mid-partition
2. **32-bit compatibility** - Test on ARM32 with >4GB RAM config
3. **Password masking** - Verify no plaintext passwords in logs/panics
4. **Cancellation** - Verify tasks actually stop, not just prevent new spawns
5. **Zero-value configs** - Test `parallel_readers: 0` fails gracefully
6. **Edge case PK values** - Test with `min_pk == 0`, negative PKs, i64::MIN/MAX

---

## Positive Highlights

1. **Excellent error design** - Well-structured error types with proper trait usage
2. **Auto-tuning algorithm** - Sophisticated memory-aware configuration
3. **Binary COPY optimization** - Concurrent serialize/send pipeline is well-designed
4. **Connection pool usage** - Proper RAII pattern prevents leaks
5. **Fast row counting** - Using `sys.partitions` instead of `COUNT(*)` is excellent
6. **Comprehensive testing** - Good test coverage for error types and progress tracking

---

## Fix Priority

| Priority | Issue | Effort |
|----------|-------|--------|
| P0 | Data loss in parallel resume | High |
| P0 | Plaintext credentials (TLS) | Medium |
| P0 | Password exposure in logs | Low |
| P0 | State not saved mid-execution | Medium |
| P1 | Off-by-one PK error | Low |
| P1 | Temp table churn | Medium |
| P1 | 32-bit overflow | Low |
| P2 | Other medium/low issues | Varies |

---

*The codebase demonstrates strong Rust skills and architectural design, but the critical issues must be addressed before production use.*
