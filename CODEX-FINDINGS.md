Codex Findings

Scope
- Reviewed Rust repo: /home/johnd/repos/mssql-pg-migrate-rs
- Compared to Go repo: /home/johnd/repos/mssql-pg-migrate

Findings (ordered by impact)
1) Correctness/perf bug: keyset pagination never advances last_pk for smallint/tinyint PKs.
   - Rust only handles I64/I32 when extracting last_pk.
   - Symptom: repeated chunk reads or infinite loop on smallint/tinyint PKs.
   - File: crates/mssql-pg-migrate/src/transfer/mod.rs

2) SQL identifier escaping gaps (Postgres + MSSQL).
   - Rust builds SQL with unescaped identifiers in DDL/DML and MSSQL reads.
   - Names containing quotes or closing brackets can break queries or be abused.
   - Files: crates/mssql-pg-migrate/src/target/mod.rs, crates/mssql-pg-migrate/src/transfer/mod.rs

3) Single reader/single writer per table in Rust.
   - Go splits PK ranges and runs parallel readers + writers.
   - This is likely a primary throughput delta on large tables.
   - Files: crates/mssql-pg-migrate/src/transfer/mod.rs, internal/transfer/transfer.go

4) Per-row metadata lookup in Rust (O(cols^2)).
   - Rust does a linear search per column per row to find data types.
   - Go precomputes column types once and reuses them.
   - File: crates/mssql-pg-migrate/src/source/mod.rs

5) Offset pagination fallback in Rust.
   - OFFSET pagination degrades on large tables and can skip/duplicate rows
     if ordering is unstable.
   - Go uses ROW_NUMBER pagination and rejects tables with no PK.
   - File: crates/mssql-pg-migrate/src/transfer/mod.rs

6) Upsert error logs include row previews.
   - Potentially leaks sensitive data to logs.
   - File: crates/mssql-pg-migrate/src/target/mod.rs

Proposals (minimal changes, highest ROI first)
1) Fix last_pk extraction to handle I16 for smallint/tinyint PKs.
2) Precompute column data types aligned to the selected column list and
   avoid per-row metadata scans in query_rows.
3) Add identifier escaping helpers and use them consistently for both
   PostgreSQL and MSSQL queries.
4) If throughput is still lagging, port Go’s parallel readers/writers
   (split PK ranges, buffered channels, writer pool).
5) Replace OFFSET pagination fallback with ROW_NUMBER pagination or
   fail fast on tables with no PK, matching Go’s safety/perf behavior.
6) Avoid logging row previews on upsert failures (or make it opt-in).

Notes
- Go repo defaults to NOLOCK on SQL Server for speed; Rust currently does not.
- Go uses COPY via pgx; Rust uses COPY text (good), but still serializes per-table.

Patch Plan (Rust)
Phase 1: Correctness + low-risk perf
1) Fix keyset last_pk extraction for smallint/tinyint.
   - Add I16 handling in read_chunk_keyset.
   - File: crates/mssql-pg-migrate/src/transfer/mod.rs
2) Precompute column data types for query_rows to avoid per-row metadata scans.
   - Build a Vec<String> of data types aligned to columns once per table.
   - Update query_rows signature to accept &[String] or &[&str] data types.
   - Files: crates/mssql-pg-migrate/src/source/mod.rs, crates/mssql-pg-migrate/src/transfer/mod.rs
3) Add MSSQL identifier escaping helper and use it in MSSQL read queries.
   - Escape ] in identifiers (SQL Server).
   - Files: crates/mssql-pg-migrate/src/transfer/mod.rs

Phase 2: Safety + consistency
4) Apply Postgres identifier quoting consistently for DDL/DML.
   - Use quote_ident for schema/table/columns everywhere.
   - Files: crates/mssql-pg-migrate/src/target/mod.rs
5) Remove row preview from upsert error logs (or gate behind debug).
   - File: crates/mssql-pg-migrate/src/target/mod.rs

Phase 3: Throughput parity with Go
6) Implement parallel readers + writer pool in Rust transfer engine.
   - Split PK range into N ranges and launch concurrent reader tasks.
   - Fan-in to buffered channel; writers consume with a semaphore or worker pool.
   - File: crates/mssql-pg-migrate/src/transfer/mod.rs
7) Replace OFFSET pagination fallback with ROW_NUMBER pagination or fail fast on no-PK.
   - Mirror Go’s ROW_NUMBER strategy for composite PKs.
   - File: crates/mssql-pg-migrate/src/transfer/mod.rs

Benchmark Plan
Goal: Compare throughput and CPU for Rust vs Go, isolate bottlenecks.

Data
- Use the same dataset (Stack Overflow 2010 or 2013) and identical schema.
- Same host, same disks, same network path for both source and target.

Configuration
- Align chunk_size, workers, read_ahead buffers, and writer counts.
- Ensure SSL/encryption flags match (and NOLOCK parity if used).
- Use same target mode (drop_recreate vs truncate vs upsert).

Measurements
1) End-to-end throughput (rows/sec).
   - Capture total rows and duration.
2) Phase timings (read vs write).
   - Add timing logs around read chunks and write chunks if not already present.
3) DB-side metrics.
   - SQL Server: wait stats, tempdb usage, IO stalls, CPU.
   - Postgres: COPY stats, WAL, checkpoints, IO.
4) Resource usage.
   - CPU, memory, and network throughput on the migration host.

Suggested Runs
- Baseline: current Rust vs current Go with identical configs.
- After Phase 1 patches: validate correctness and compare throughput.
- After Phase 3: compare multi-reader/writer improvements.

Success Criteria
- Rust throughput within 10-15% of Go on the same dataset/config.
- No correctness regressions (row counts match, checksum spot checks).
