# Test Plan: Security and Reliability Fixes

## Overview
This test plan covers the fixes for critical issues identified in the Gemini code review:
1. Password redaction in Debug output
2. PostgreSQL TLS support
3. State checkpointing during migration (#19)
4. Parallel resume data loss prevention (#20)

---

## 1. Password Redaction Tests

### 1.1 Unit Tests (Automated)
| Test ID | Description | Expected Result |
|---------|-------------|-----------------|
| PR-01 | SourceConfig Debug output | Password shows `[REDACTED]` |
| PR-02 | TargetConfig Debug output | Password shows `[REDACTED]` |
| PR-03 | Debug output contains other fields | Host, port, database visible |

### 1.2 Integration Tests (Manual)
| Test ID | Description | Command | Expected Result |
|---------|-------------|---------|-----------------|
| PR-04 | Verbose logging doesn't leak password | `RUST_LOG=debug ./mssql-pg-migrate -c config.yaml health-check` | No password in output |
| PR-05 | Error messages don't leak password | Use invalid config | Error doesn't contain password |

---

## 2. PostgreSQL TLS Tests

### 2.1 Unit Tests (Automated)
| Test ID | Description | Expected Result |
|---------|-------------|-----------------|
| TLS-01 | TLS config builds for `require` mode | No error, uses NoVerifier |
| TLS-02 | TLS config builds for `verify-full` mode | Uses WebPKI roots |

### 2.2 Integration Tests (Manual)
| Test ID | Description | Config | Expected Result |
|---------|-------------|--------|-----------------|
| TLS-03 | ssl_mode: disable | `ssl_mode: disable` | Warning logged, connection works |
| TLS-04 | ssl_mode: require | `ssl_mode: require` | TLS handshake, no cert verify |
| TLS-05 | ssl_mode: verify-full | `ssl_mode: verify-full` | Requires valid cert |
| TLS-06 | Invalid ssl_mode | `ssl_mode: invalid` | Error with guidance |

---

## 3. State Checkpointing Tests (#19)

### 3.1 Code Review Verification
| Test ID | Description | Location | Expected |
|---------|-------------|----------|----------|
| SC-01 | Checkpoint after table success | orchestrator/mod.rs:870 | `save_state()` called |
| SC-02 | Checkpoint after partition success | orchestrator/mod.rs:861 | `save_state()` called |
| SC-03 | Checkpoint after table failure | orchestrator/mod.rs:908 | `save_state()` called |
| SC-04 | Checkpoint after partition failure | orchestrator/mod.rs:898 | `save_state()` called |
| SC-05 | Checkpoint after table panic | orchestrator/mod.rs:943 | `save_state()` called |
| SC-06 | Checkpoint after partition panic | orchestrator/mod.rs:933 | `save_state()` called |

### 3.2 Integration Tests (Manual)
| Test ID | Description | Command | Expected Result |
|---------|-------------|---------|-----------------|
| SC-07 | State file updated during migration | Run migration, check state file | File updated after each table |
| SC-08 | State shows completed tables | Interrupt migration mid-way | Completed tables marked in state |

---

## 4. RangeTracker Tests (#20)

### 4.1 Unit Tests (Automated)
| Test ID | Description | Expected Result |
|---------|-------------|-----------------|
| RT-01 | Empty tracker | Returns None |
| RT-02 | Single range | Returns range end |
| RT-03 | Contiguous ranges | Returns last range end |
| RT-04 | Gap in ranges | Returns end before gap |
| RT-05 | Out of order ranges | Correctly merges |
| RT-06 | Data loss scenario | Prevents skip of incomplete range |
| RT-07 | Non-zero start | Works with resume point |
| RT-08 | Overlapping ranges | Correctly handles overlap |
| RT-09 | None first_pk | Defaults to start_pk |

### 4.2 Code Review Verification
| Test ID | Description | Location | Expected |
|---------|-------------|----------|----------|
| RT-10 | RangeTracker used in dispatcher | transfer/mod.rs:350 | RangeTracker initialized |
| RT-11 | Chunks include first_pk | transfer/mod.rs:619,714 | first_pk field set |
| RT-12 | Safe resume point used | transfer/mod.rs:370 | `safe_resume_point()` called |

---

## 5. Regression Tests

### 5.1 Full Test Suite
| Test ID | Description | Command | Expected Result |
|---------|-------------|---------|-----------------|
| REG-01 | All unit tests pass | `cargo test` | 77+ tests pass |
| REG-02 | CLI tests pass | `cargo test --test cli_tests` | 19 tests pass |
| REG-03 | Release build succeeds | `cargo build --release` | No errors |

### 5.2 Functionality Verification
| Test ID | Description | Expected Result |
|---------|-------------|-----------------|
| REG-04 | Health check works | Exit 0, shows healthy |
| REG-05 | Dry run works | Exit 0, no data transferred |
| REG-06 | Migration completes | All tables transferred |
| REG-07 | Resume works | Continues from last checkpoint |

---

## Test Execution Results

### Date: 2025-12-26
### Tester: Claude Code

### Summary
- **Total Tests Executed**: 97 (77 unit + 19 CLI + 1 doc)
- **All Tests Passed**: Yes
- **Build Status**: Success (release and debug)

### Detailed Results

| Test ID | Status | Notes |
|---------|--------|-------|
| **Password Redaction** | | |
| PR-01 | PASS | SourceConfig shows [REDACTED] |
| PR-02 | PASS | TargetConfig shows [REDACTED] |
| PR-03 | PASS | Other fields visible (host, port, etc.) |
| PR-04 | PASS | Verbose logging tested, no password leaks |
| **PostgreSQL TLS** | | |
| TLS-03 | PASS | ssl_mode: disable shows warning about plaintext |
| TLS-04 | PASS | ssl_mode: require - TLS handshake successful |
| TLS-05 | N/A | Requires valid CA cert for full test |
| **State Checkpointing** | | |
| SC-01 | PASS | save_state() at line 874 after table success |
| SC-02 | PASS | save_state() at line 861 after partition success |
| SC-03 | PASS | save_state() at line 908 after table failure |
| SC-04 | PASS | save_state() at line 898 after partition failure |
| SC-05 | PASS | save_state() at line 943 after table panic |
| SC-06 | PASS | save_state() at line 933 after partition panic |
| **RangeTracker** | | |
| RT-01 | PASS | Empty tracker returns None |
| RT-02 | PASS | Single range returns end |
| RT-03 | PASS | Contiguous ranges work |
| RT-04 | PASS | Gap detection works |
| RT-05 | PASS | Out of order merging works |
| RT-06 | PASS | Data loss scenario prevented |
| RT-07 | PASS | Non-zero start works |
| RT-08 | PASS | Overlapping ranges handled |
| RT-09 | PASS | None first_pk defaults correctly |
| RT-10 | PASS | RangeTracker used at line 350 |
| RT-11 | PASS | first_pk field in RowChunk at lines 619, 714 |
| RT-12 | PASS | safe_resume_point() called at line 370 |
| **Regression** | | |
| REG-01 | PASS | 77 unit tests pass |
| REG-02 | PASS | 19 CLI tests pass |
| REG-03 | PASS | Release build succeeds |
| REG-04 | PASS | Health check works with TLS |
| REG-05 | PASS | Dry run completes successfully |

### Test Commands Used
```bash
# Full test suite
cargo test

# Specific test suites
cargo test --lib password
cargo test --lib range_tracker
cargo test --lib state

# Integration tests
./mssql-pg-migrate -c config.yaml health-check
./mssql-pg-migrate -c config.yaml run --dry-run
```

### Known Limitations
1. TLS verify-full requires valid CA certificate for full testing
2. Parallel resume data loss prevention tested via unit tests (not end-to-end)

---

## 6. State Migration Integration Tests

### Date: 2025-12-26

### Test 6.1: Full Migration with State Tracking
| Aspect | Result |
|--------|--------|
| State file created | PASS |
| All 10 tables completed | PASS |
| 19.3M rows transferred | PASS |
| rows_transferred matches rows_total | PASS |
| completed_at timestamps present | PASS |

### Test 6.2: Resume from Partial State
| Aspect | Result |
|--------|--------|
| Created partial state (5 completed, 5 pending) | PASS |
| Resume shows "Transferring 5 tables" (not 10) | PASS |
| Completed tables skipped | PASS |
| Run ID preserved | PASS |

### Test 6.3: State During Interrupt
| Aspect | Result |
|--------|--------|
| Interrupted migration mid-transfer | PASS |
| In-progress table shows status: "in_progress" | PASS |
| Completed tables preserved | PASS |
| Pending tables remain pending | PASS |

### Sample State File Output
```json
{
  "run_id": "277aa0bb-f324-4574-b37e-cc2872051c03",
  "status": "completed",
  "tables": {
    "dbo.Users": {
      "status": "completed",
      "rows_total": 299398,
      "rows_transferred": 299398,
      "last_pk": 286509,
      "completed_at": "2025-12-26T00:28:13.969481452Z"
    }
  }
}
```

