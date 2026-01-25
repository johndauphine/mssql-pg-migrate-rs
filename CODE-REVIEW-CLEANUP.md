# Code Review - Cleanup and Reduction Focus

## Scope
- Static review of Rust crates only; no tests or builds were run.
- Covers all migration sources and targets (MSSQL, PostgreSQL, MySQL).
- CLI wizard/TUI remain in scope.
- Goal: identify dead/duplicate code and the largest deletions that still preserve intended behavior.

## Decisions (confirmed)
1) Keep all migration directions (MSSQL, PostgreSQL, MySQL as both source and target).
2) Keep the new core/drivers/dialect architecture; legacy code should be removed.
3) Keep CLI wizard and TUI unchanged at the feature level.

## Findings (ordered by impact)

### High
- Dual architectures are active and cause duplication plus conversion glue. Evidence: legacy traits in `crates/mssql-pg-migrate/src/source/mod.rs:70` and `crates/mssql-pg-migrate/src/target/mod.rs:51`, new traits in `crates/mssql-pg-migrate/src/core/traits.rs:69`, both exported in `crates/mssql-pg-migrate/src/lib.rs:60`; bridging conversions for MySQL in `crates/mssql-pg-migrate/src/orchestrator/pools.rs:44` and `crates/mssql-pg-migrate/src/drivers/mysql/reader.rs:15`. Action: refactor transfer/orchestrator to use new core/drivers APIs and delete legacy modules.
- Pipeline module appears unused. Evidence: only re-exported in `crates/mssql-pg-migrate/src/lib.rs:60`, implemented in `crates/mssql-pg-migrate/src/pipeline/mod.rs:1` with no internal references. Action: either adopt pipeline as the transfer abstraction or delete it to reduce surface area.
- Duplicate transfer primitives. Evidence: `DateFilter` and `TransferJob` are defined in both `crates/mssql-pg-migrate/src/transfer/mod.rs:22` and `crates/mssql-pg-migrate/src/pipeline/job.rs:18`. Action: keep one set and delete the other.
- Duplicate SQL value representations. Evidence: `SqlValue` and `SqlNullType` exist in `crates/mssql-pg-migrate/src/core/value.rs:16` and again in `crates/mssql-pg-migrate/src/target/mod.rs:189`, with conversion glue in `crates/mssql-pg-migrate/src/orchestrator/pools.rs:44`. Action: standardize on core::value types and remove legacy SqlValue types.
- Type mapping is duplicated between old typemap and new dialect mappers. Evidence: legacy mapping in `crates/mssql-pg-migrate/src/typemap/mod.rs:1` is still used by the target code in `crates/mssql-pg-migrate/src/target/mod.rs:25`, while new canonical mappers live in `crates/mssql-pg-migrate/src/dialect/mod.rs:1`. Action: keep dialect mappers and remove legacy typemap.
- Driver catalog entry points are defined but unused. Evidence: `crates/mssql-pg-migrate/src/orchestrator/mod.rs:418` defines create_source_reader/create_target_writer but no call sites exist. Action: wire these into the transfer path (preferred) and remove unused legacy factories.
- NoOp state backend is unused. Evidence: `crates/mssql-pg-migrate/src/state/noop.rs:1` and `crates/mssql-pg-migrate/src/state/mod.rs:61` define NoOp, but `crates/mssql-pg-migrate/src/orchestrator/pools.rs:562` only constructs Postgres/MSSQL/MySQL backends. Action: delete NoOpStateBackend.

### Medium
- SourcePool/TargetPool traits and enums are part of the legacy stack. Evidence: traits in `crates/mssql-pg-migrate/src/source/mod.rs:70` and `crates/mssql-pg-migrate/src/target/mod.rs:51` vs enum wrappers in `crates/mssql-pg-migrate/src/orchestrator/pools.rs:162`. Action: remove these once the transfer path uses `core::traits::{SourceReader, TargetWriter}`.
- Config flags are partially unused in the transfer engine. Evidence: `TransferConfig` is always created with `use_copy_binary: true` and `use_direct_copy: true` in `crates/mssql-pg-migrate/src/orchestrator/mod.rs:920`, ignoring `migration.use_binary_copy`. Action: wire `migration.use_binary_copy` into the new transfer path or delete the config knob.
- Unused dependencies inflate build surface. Evidence: `enum_dispatch` (unused, `crates/mssql-pg-migrate/Cargo.toml:26`), `parking_lot` (unused, `crates/mssql-pg-migrate/Cargo.toml:64`), `rayon` (unused, `crates/mssql-pg-migrate/Cargo.toml:65`), `md-5` (unused, `crates/mssql-pg-migrate/Cargo.toml:76`), `anyhow` (unused in CLI, `crates/mssql-pg-migrate-cli/Cargo.toml:31`), and workspace `deadpool` (unused, `Cargo.toml:79`). Action: delete these dependencies and update Cargo.lock.
- Public API surface is very broad due to re-exports. Evidence: `crates/mssql-pg-migrate/src/lib.rs:46` through `crates/mssql-pg-migrate/src/lib.rs:76` re-exports both legacy and new APIs. Action: narrow exports to new architecture only.

### Low
- Dead fields left for future use. Evidence: `WriteJob.partition_id` in `crates/mssql-pg-migrate/src/transfer/mod.rs:270`, `PgUpsertWriter.copy_buffer_rows` in `crates/mssql-pg-migrate/src/target/mod.rs:1570`, `MysqlReader.database` in `crates/mssql-pg-migrate/src/drivers/mysql/reader.rs:23`. Action: remove these fields or implement the intended functionality.
- Bench-only module adds maintenance surface. Evidence: `crates/mssql-pg-migrate/src/source/bench_copy.rs:1` is an ignored benchmark with local credentials. Action: move to `benches/` or delete if not actively used.

## Deletion plan (new architecture, all sources/targets)
1) Refactor transfer/orchestrator to use the new core/drivers path end-to-end.
   - Use `DriverCatalog::create_reader/create_writer` in `crates/mssql-pg-migrate/src/orchestrator/mod.rs:418` and feed `SourceReaderImpl`/`TargetWriterImpl` into the transfer engine.
   - Replace `SourcePoolImpl`/`TargetPoolImpl` and delete `crates/mssql-pg-migrate/src/orchestrator/pools.rs` once migration is complete.
2) Remove legacy modules once the new path is wired.
   - Delete `crates/mssql-pg-migrate/src/source/`.
   - Delete `crates/mssql-pg-migrate/src/target/`.
   - Delete `crates/mssql-pg-migrate/src/typemap/`.
   - Remove legacy re-exports from `crates/mssql-pg-migrate/src/lib.rs:46`.
3) Consolidate transfer primitives.
   - Keep a single `DateFilter` and `TransferJob` definition.
   - If pipeline remains unused, remove `crates/mssql-pg-migrate/src/pipeline/`; otherwise migrate the transfer engine to the pipeline and delete the legacy `TransferJob`/`DateFilter` in `crates/mssql-pg-migrate/src/transfer/mod.rs:22`.
4) Standardize on `core::value::SqlValue` and remove conversions.
   - Delete legacy `SqlValue`/`SqlNullType` in `crates/mssql-pg-migrate/src/target/mod.rs:189`.
   - Remove conversion glue in `crates/mssql-pg-migrate/src/orchestrator/pools.rs:44` and `crates/mssql-pg-migrate/src/drivers/mysql/reader.rs:15`.
5) Update state backends to work with new drivers.
   - Replace `MssqlTargetPool`/`PgPool` usage in `crates/mssql-pg-migrate/src/state/*` with new driver connection pools or a thin new `state` client built on `drivers/common` utilities.
   - Remove `NoOpStateBackend` (`crates/mssql-pg-migrate/src/state/noop.rs:1`).
6) Clean dependencies and public API surface.
   - Remove unused deps (enum_dispatch, parking_lot, rayon, md-5, anyhow in CLI, deadpool workspace).
   - Narrow `crates/mssql-pg-migrate/src/lib.rs:46` to new architecture exports only.

## Migration checklist (step-by-step)
1) Baseline and safety
   - Capture current CLI behavior for `run`, `resume`, `validate`, `health-check`, `init`, and TUI.
   - Identify existing config fields actually used in the legacy path to avoid silent regressions.
2) Choose the transfer abstraction
   - Option A (recommended): keep `crates/mssql-pg-migrate/src/transfer/` but port it to `core::traits::{SourceReader, TargetWriter}`.
   - Option B: adopt the pipeline module and implement a concrete pipeline, then remove legacy `transfer/`.
3) Port transfer engine to new core/drivers
   - Replace `SourcePoolImpl`/`TargetPoolImpl` usage with `SourceReaderImpl`/`TargetWriterImpl`.
   - Switch row representations to `core::value::SqlValue` end-to-end; remove any conversion glue.
   - Reuse `core::traits::Dialect` (via `DialectImpl`) for query building where applicable.
   - Keep all features: partitioning, keyset pagination, row-number pagination, upsert, direct copy, compression, progress reporting.
4) Wire orchestrator to new engine
   - Update `crates/mssql-pg-migrate/src/orchestrator/mod.rs:418` to call `DriverCatalog::create_reader/create_writer`.
   - Ensure `health_check` uses the new reader/writer connection checks.
   - Pass `migration.use_binary_copy`/`migration.compress_text` through to the new transfer path.
5) Port state backends to new drivers
   - Postgres: use a new helper in `drivers/postgres` to expose a pool or a minimal connection provider.
   - MSSQL: use a driver-level pool/connector instead of `MssqlTargetPool`.
   - MySQL: reuse existing `mysql_async::Pool` from `drivers/mysql` or provide a shared constructor to avoid duplication.
6) Remove legacy modules
   - Delete `crates/mssql-pg-migrate/src/source/`, `crates/mssql-pg-migrate/src/target/`, `crates/mssql-pg-migrate/src/typemap/`, and `crates/mssql-pg-migrate/src/orchestrator/pools.rs`.
   - Remove duplicate `TransferJob`/`DateFilter` definitions (keep only one set).
   - Remove `NoOpStateBackend` and any unused legacy helpers.
7) Clean public API and dependencies
   - Narrow `crates/mssql-pg-migrate/src/lib.rs:46` exports to new architecture only.
   - Remove unused dependencies and update `Cargo.lock`.
8) Validation
   - Build and run core CLI flows for MSSQL/Postgres/MySQL directions.
   - Run `cargo test` and `cargo clippy --all-targets --all-features`.

## Transfer engine deep-dive (data flow + function-level rewrite plan)

### Current data flow (legacy)
```text
Orchestrator
  -> TransferEngine::execute(job)
       -> plan: keyset vs row-number vs offset vs copy/direct
       -> read_table_chunks_* (SourcePoolImpl + legacy SQL builders)
           -> RowChunk { ChunkData::Rows | ChunkData::Direct }
       -> Dispatcher (RangeTracker + progress + compression)
           -> WriteJob
       -> writers (TargetPoolImpl + UpsertWriter)
       -> TransferStats + last_pk
```

### Target data flow (new architecture)
```text
Orchestrator
  -> DriverCatalog
      -> SourceReaderImpl + TargetWriterImpl + DialectImpl
  -> TransferEngine2::execute(job)
       -> plan_read_strategy(job, reader, writer, dialect, config)
       -> spawn_readers(plan, reader) -> BatchEnvelope
       -> dispatcher (RangeTracker + progress + compression)
       -> spawn_writers(writer, target_mode) -> write_batch/upsert_batch
       -> TransferStats + last_key
```

### Function-level rewrite map (concrete tasks)
1) `TransferEngine::execute` (`crates/mssql-pg-migrate/src/transfer/mod.rs:349`)
   - Split into: `plan_read_strategy`, `spawn_readers`, `dispatch_batches`, `spawn_writers`, `collect_stats`.
   - New signature should accept `SourceReaderImpl`, `TargetWriterImpl`, and `DialectImpl` instead of legacy pools.
2) `read_table_chunks_parallel` + `read_chunk_range` + `read_chunk_keyset_fast`
   - Replace with `spawn_keyset_readers(plan, reader, tx)` using `core::traits::ReadOptions`.
   - Use `Dialect::build_select_query` for SQL instead of `quote_*` helpers.
3) `read_table_chunks_copy_binary`
   - Replace with `read_batches_copy_binary(reader, opts)` via a new `ReadOptions` flag or a specialized trait (see Direct copy tasks).
4) `read_table_chunks_direct` + `read_chunk_range_direct`
   - Replace with a direct-copy interface exposed on the new reader, not by downcasting.
   - Proposed extension: add `read_table_direct_copy(opts) -> mpsc::Receiver<Result<DirectBatch>>` to `SourceReader`.
5) `read_chunk_offset` + `read_table_chunks`
   - Replace with `read_batches_offset(reader, dialect, opts)` using `Dialect::build_row_number_query`.
6) `ChunkData`, `RowChunk`, `WriteJob`
   - Replace with a single envelope type:
     - `BatchEnvelope { data: BatchData, read_time, first_key, last_key }`
     - `BatchData::Rows(Batch)` or `BatchData::DirectCopy { data, row_count }`
7) `RangeTracker`
   - Move to a generic `PkValue`-aware tracker (use `core::schema::PkValue`) so non-int PKs can still track resume points for non-direct paths.
8) `quote_*` and `qualify_*` helpers (`crates/mssql-pg-migrate/src/transfer/mod.rs:1740`)
   - Remove these and route all quoting through `DialectImpl::quote_ident` and `Dialect::build_select_query`.
9) Writer path
   - Replace `TargetPoolImpl::get_upsert_writer` with `TargetWriterImpl::upsert_batch`.
   - If direct copy is retained, extend `TargetWriter` with `upsert_batch_direct` for Postgres only.

### Direct copy integration tasks (to preserve MSSQL -> Postgres upsert perf)
1) Extend core types:
   - Add `BatchData::DirectCopy` (Bytes + row_count + optional pk tracking) to avoid legacy `ChunkData`.
2) Extend traits:
   - `SourceReader`: add a direct-copy read method or a `ReadOptions::direct_copy` flag.
   - `TargetWriter`: add `upsert_batch_direct` for Postgres writer only.
3) Wire planners:
   - In `plan_read_strategy`, only enable direct copy when:
     - source supports direct copy
     - target supports direct copy
     - target_mode is Upsert
     - PK is integer or can be mapped to `PkValue::Int`

### Suggested new helper APIs (shape only)
```rust
struct ReadPlan {
    use_keyset: bool,
    use_copy_binary: bool,
    use_direct_copy: bool,
    num_readers: usize,
    pk_idx: Option<usize>,
}

async fn plan_read_strategy(
    job: &TransferJob,
    reader: &SourceReaderImpl,
    writer: &TargetWriterImpl,
    dialect: &DialectImpl,
    config: &TransferConfig,
) -> ReadPlan;

async fn spawn_readers(
    plan: &ReadPlan,
    reader: SourceReaderImpl,
    job: TransferJob,
    tx: mpsc::Sender<BatchEnvelope>,
) -> Vec<JoinHandle<Result<()>>>;
```

## Validation checklist after pruning
- `cargo test`
- `cargo clippy --all-targets --all-features`
- Run core CLI flows: `target/release/mssql-pg-migrate -c config.yaml run` and `target/release/mssql-pg-migrate init`
- If TUI remains enabled: `cargo run --features tui --bin mssql-pg-migrate -- tui`
