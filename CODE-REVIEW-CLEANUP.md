# Code Review - Re-review (Post-Refactor)

## Scope
- Static review of Rust crates only; no tests or builds were run.
- Focus on cleanup and ensuring the new core/drivers/dialect architecture is the real runtime path.
- All migration sources/targets (MSSQL, PostgreSQL, MySQL) remain in scope.
- CLI wizard/TUI remain in scope.

## Findings (ordered by severity)

### High
- Direct-copy upsert path is broken for MSSQL -> Postgres. Transfer always attempts `upsert_chunk_direct` for direct-copy batches, but the Postgres upsert adapter does not implement `upsert_chunk_direct`, so the default error path triggers. Evidence: `crates/mssql-pg-migrate/src/transfer/mod.rs:524`, `crates/mssql-pg-migrate/src/orchestrator/pools.rs:616`, `crates/mssql-pg-migrate/src/drivers/postgres/writer.rs:312`. Impact: MSSQL -> Postgres upsert fails when direct copy is selected (integer PKs). Fix: implement `upsert_chunk_direct` in the Postgres upsert adapter or disable direct copy until a new-path writer supports it end-to-end.
- Type mapping is not wired for Postgres or MSSQL targets. `TargetPoolImpl::from_config` constructs `PostgresWriter`/`MssqlWriter` without a type mapper, so they assume the source is already native and emit raw types. Evidence: `crates/mssql-pg-migrate/src/orchestrator/pools.rs:599`, `crates/mssql-pg-migrate/src/drivers/postgres/writer.rs:140`, `crates/mssql-pg-migrate/src/drivers/mssql/writer.rs:282`. Impact: cross-database DDL will emit invalid types (e.g., MSSQL `nvarchar` on Postgres, Postgres `uuid` on MSSQL). Fix: select and set a mapper based on source/target (DriverCatalog or explicit mapper selection).

### Medium
- Config ignores `migration.use_binary_copy` and always enables copy/direct-copy paths. TransferConfig is built with `use_copy_binary: true` and `use_direct_copy: true`, ignoring config knobs. Evidence: `crates/mssql-pg-migrate/src/orchestrator/mod.rs:922`. Impact: operators cannot disable copy paths for compatibility; configuration is misleading. Fix: wire config values through or remove the options.
- The new core architecture is still not the runtime path. Transfer uses `SourcePoolImpl`/`TargetPoolImpl` plus legacy `target::SqlValue`, while `DriverCatalog::create_reader/create_writer` and `SourceReader::read_table` are unused. Evidence: `crates/mssql-pg-migrate/src/orchestrator/mod.rs:414`, `crates/mssql-pg-migrate/src/transfer/mod.rs:8`, `crates/mssql-pg-migrate/src/drivers/*/reader.rs:669`. Impact: duplicated code paths and dead code remain, increasing maintenance cost. Fix: move transfer to `core::value::SqlValue` + `SourceReader::read_table` (or delete the core path).
- Legacy compatibility layers remain. `target::SqlValue` duplicates core `SqlValue`, and conversion glue is still present in pool wrappers and drivers. Evidence: `crates/mssql-pg-migrate/src/target/mod.rs:45`, `crates/mssql-pg-migrate/src/orchestrator/pools.rs:44`, `crates/mssql-pg-migrate/src/drivers/*/reader.rs:410`. Impact: extra code and conversion overhead. Fix: standardize on core types and delete the adapters.

### Low
- Dead fields remain: `WriteJob.partition_id` (`crates/mssql-pg-migrate/src/transfer/mod.rs:270`) and `MysqlReader.database` (`crates/mssql-pg-migrate/src/drivers/mysql/reader.rs:24`).
- `crates/mssql-pg-migrate/src/source` is now only re-exports; consider removing the module and the `pub mod source` export in `crates/mssql-pg-migrate/src/lib.rs:36` if API compatibility is not required.

## Suggested next steps (cleanup-focused)
1) Fix direct-copy upsert support for Postgres (or disable direct copy until supported).
2) Wire type mappers for Postgres and MSSQL targets based on source type.
3) Decide and enforce a single runtime path (core/drivers vs legacy wrappers).
4) Remove `target::SqlValue` and adapter methods once transfer uses core values.
5) Delete remaining dead fields and re-export-only modules.

## Test gaps to close after cleanup
- Cross-target DDL creation for each source/target pair (type mapping).
- MSSQL -> Postgres upsert using direct-copy batches.
- Config toggles for `use_binary_copy` and direct-copy behavior.
