# Codex Findings

## Scope
- Reviewed Rust repo `/home/johnd/repos/mssql-pg-migrate-rs` against the earlier Go tool `/home/johnd/repos/mssql-pg-migrate`.
- Focus: perf gaps, security posture, and correctness for the MSSQL → Postgres CLI.

## Current state and measurements
- Branch: `perf-optimizations` (contains identifier quoting, smallint PK fix, auto-tune caps, parallel readers/writers).
- Latest run: `./target/release/mssql-pg-migrate -c test-config.yaml run` (16 readers, auto-tuned 5 writers, chunk 100k) → 19,310,707 rows in 150.15s (~128,608 rows/s). Validation passed for all 10 tables.
- Previous best (manual) was ~105s (~183k rows/s) with 16 readers / 8 writers before auto-tune limited writers.
- Finalization (indexes/constraints) consumed ~83s of the 150s runtime.
- `cargo test` currently fails offline (DNS to crates.io blocked).

## Improvements already landed
- Added identifier quoting for MSSQL readers and Postgres DDL/DML/COPY (`crates/mssql-pg-migrate/src/transfer/mod.rs`, `src/target/mod.rs`).
- Fixed keyset pagination to advance last_pk for smallint/tinyint PKs (`src/transfer/mod.rs`).
- Removed row-preview logging on upsert errors to avoid leaking data (`src/target/mod.rs`).
- Auto-tune caps raised (up to 16 readers / 8 writers; higher pool ceilings in `src/config/types.rs`).

## Perf opportunities (highest impact first)
1) Writers under-provisioned: auto-tune forced writers to 5 even with `parallel_writers: 8` in config, leaving PG pool at 24 conns. Raise the writer floor/cap for drop_recreate mode or honor explicit overrides when memory/connection budgets allow (see `src/config/types.rs`).
2) Finalization cost: indexes/constraints took ~83s. Consider parallel index builds per table, bumping `maintenance_work_mem`, or deferring smaller indexes until after bulk tables finish (Postgres-side).
3) Partition sizing: small tables (e.g., `Badges` 1.1M rows) still partitioned into 16 slices, adding setup overhead. Add a minimum-rows-per-partition heuristic or cap partitions by estimated table rows in `src/transfer/mod.rs`.
4) Connection sizing: MSSQL pool opened 68 conns for 16 readers. Align pool sizes to active readers/writers to reduce handshake overhead and socket pressure.
5) Auto-tune heuristics: currently cores/2 readers and capped writers based on connection math. Incorporate table size and network throughput to scale readers/writers separately per table, and decay concurrency on smaller tables to avoid thrash.
6) Optional: parallelize per-table COPY/write finalization for non-conflicting tables to keep CPUs busy while indexes build.

## Security and operational notes
- TLS is disabled in `test-config.yaml`; enable `encrypt`/`ssl_mode` for real environments. Trust options should be explicit and documented.
- Identifier quoting now reduces SQL injection risk; keep it consistent for future SQL helpers.
- Numerous unused helpers remain in `src/target/mod.rs` (warned at build); prune to shrink surface area.

## Next steps
- Adjust auto-tune to permit 8 writers on this host (or make overrides authoritative) and re-benchmark.
- Add partition heuristic and rerun with `test-config.yaml` to compare against the 105s baseline.
- Capture DB-side metrics (tempdb/WAL) during runs to spot server bottlenecks.
- Restore `cargo test` once network is available; add a smoke test that mocks DB calls so CI can run offline.
