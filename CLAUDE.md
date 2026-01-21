# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

High-performance, headless database migration tool written in Rust. Supports MSSQL ↔ PostgreSQL bidirectional migrations, with optional MySQL/MariaDB support. Designed for scripted environments, Kubernetes, and Airflow DAGs.

## Build & Development Commands

```bash
# Build release binary
cargo build --release

# Build with optional features
cargo build --release --features tui        # Terminal UI
cargo build --release --features kerberos   # Kerberos auth
cargo build --release --features mysql      # MySQL/MariaDB support

# Run tests
cargo test

# Lint and format
cargo fmt -- --check
cargo clippy --all-targets --all-features

# Run CLI
target/release/mssql-pg-migrate -c config.yaml run
target/release/mssql-pg-migrate -c config.yaml run --dry-run
target/release/mssql-pg-migrate -c config.yaml resume
target/release/mssql-pg-migrate -c config.yaml validate
target/release/mssql-pg-migrate health-check -c config.yaml
```

## Workspace Structure

```
crates/
├── mssql-pg-migrate/        # Core library
│   └── src/
│       ├── config/          # Config loading, types, validation
│       ├── core/            # Plugin architecture abstractions
│       │   ├── traits.rs    # SourceReader, TargetWriter, Dialect, TypeMapper
│       │   ├── catalog.rs   # DriverCatalog for dependency injection
│       │   ├── schema.rs    # Table, Column, Index, ForeignKey
│       │   └── value.rs     # SqlValue with Cow<'a, str> for zero-copy
│       ├── drivers/         # Database driver implementations (NEW)
│       │   ├── mssql/       # MSSQL dialect, reader, writer
│       │   ├── postgres/    # PostgreSQL dialect, reader, writer
│       │   ├── mysql/       # MySQL/MariaDB dialect (feature: mysql)
│       │   └── common/      # Shared utilities (TLS)
│       ├── dialect/         # Type mapping registry (NEW)
│       │   └── typemap.rs   # All type mappers (MSSQL↔PG, MySQL↔PG, etc.)
│       ├── pipeline/        # Transfer pipeline (Template Method)
│       │   ├── template.rs  # TransferPipeline trait, PipelineConfig
│       │   └── job.rs       # TransferJob (Command pattern)
│       ├── orchestrator/    # Main coordinator, connection pools
│       ├── transfer/        # Parallel read/write engine
│       ├── source/          # SourcePool trait (DEPRECATED → use drivers/)
│       ├── target/          # TargetPool trait (DEPRECATED → use drivers/)
│       ├── state/           # Database-backed migration state
│       │   ├── backend.rs   # StateBackend trait (Strategy pattern)
│       │   ├── db.rs        # PostgreSQL state backend
│       │   └── mssql_db.rs  # MSSQL state backend
│       ├── typemap/         # Bidirectional MSSQL ↔ PostgreSQL type mapping
│       └── error.rs         # Error types and exit codes
│
└── mssql-pg-migrate-cli/    # CLI entry point
    └── src/
        ├── main.rs          # Command parsing, signal handling
        ├── wizard.rs        # Interactive config creation
        └── tui/             # Optional terminal UI (feature: tui)
```

## Architecture

### Core Data Flow

1. **Config** → Load YAML/JSON, auto-tune based on RAM/CPU
2. **Orchestrator** → Connect pools, initialize state backend
3. **Schema Extraction** → List tables, columns, indexes, FKs from source
4. **Type Mapping** → Convert types bidirectionally (MSSQL ↔ PostgreSQL ↔ MySQL)
5. **Target Schema** → Create/truncate tables, drop non-PK indexes
6. **Transfer Engine** → Parallel readers (keyset pagination) + parallel writers (binary COPY)
7. **Finalization** → Create indexes, FKs, constraints sequentially
8. **Validation** → Optional row count verification

### Key Abstractions

**New Plugin Architecture (recommended):**
- **`SourceReader` trait** (`core/traits.rs`): Database-agnostic source operations
- **`TargetWriter` trait** (`core/traits.rs`): Database-agnostic target operations
- **`Dialect` trait** (`core/traits.rs`): SQL syntax strategy for different databases
- **`TypeMapper` trait** (`core/traits.rs`): Type conversion between databases
- **`DriverCatalog`** (`core/catalog.rs`): Factory for creating readers, writers, and mappers
- **`SourceReaderImpl` / `TargetWriterImpl`** (`drivers/mod.rs`): Enum-based static dispatch

**Legacy (deprecated, still functional):**
- **`SourcePool` trait** (`source/mod.rs`): Use `SourceReader` instead
- **`TargetPool` trait** (`target/mod.rs`): Use `TargetWriter` instead

**State Management:**
- **`StateBackend` trait** (`state/backend.rs`): Migration state storage strategy. Implementations: `DbStateBackend`, `MssqlStateBackend`
- **`TransferEngine`** (`transfer/mod.rs`): Manages parallel read-ahead pipeline with configurable workers

### Plugin Architecture (GoF Patterns)

The codebase uses Gang of Four design patterns for extensibility:

| Pattern | Implementation | Purpose |
|---------|----------------|---------|
| **Strategy** | `Dialect` trait, `StateBackend` trait | Interchangeable SQL syntax and state storage |
| **Abstract Factory** | `DriverCatalog` | Creates related driver objects (dialect, mapper) |
| **Template Method** | `TransferPipeline` trait | Algorithm skeleton with customizable steps |
| **Command** | `TransferJob` | Encapsulates transfer work units for queuing |

**Key Components:**

- **`DriverCatalog`** (`core/catalog.rs`): Explicit dependency injection for dialects and type mappers
- **`Dialect`** (`core/traits.rs`): SQL syntax strategy (quoting, upserts, pagination)
- **`TypeMapper`** (`core/traits.rs`): Type conversion with (source, target) pair keying
- **`TransferPipeline`** (`pipeline/template.rs`): Transfer algorithm with setup/execute/finalize phases
- **`SqlValue`** (`core/value.rs`): Universal value type with `Cow<'a, str>` for zero-copy efficiency

### Concurrency Model

- Tokio async runtime with `JoinSet` for table workers
- `Arc<AtomicI64>` for thread-safe progress tracking
- `mpsc` channels for async communication
- `CancellationToken` for coordinated SIGINT/SIGTERM shutdown
- Keyset pagination (`WHERE pk > last_pk`) for efficient parallel partition reads

### Target Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| `drop_recreate` | Drop & recreate tables, bulk insert | Full refresh (fastest) |
| `truncate` | Truncate existing, create if missing | Preserve schema |
| `upsert` | Staging table → `INSERT...ON CONFLICT DO UPDATE` | Incremental sync |

### MySQL/MariaDB Support

MySQL/MariaDB support is available via the `mysql` feature flag:

```bash
cargo build --release --features mysql
```

**Supported Migration Paths:**
- MySQL → PostgreSQL (all types lossless)
- PostgreSQL → MySQL (some lossy: uuid→char(36), interval→varchar)
- MySQL → MSSQL
- MSSQL → MySQL

**MySQL-specific Features:**
- Uses SQLx connection pooling
- Multi-row INSERT batching for bulk writes
- `INSERT...ON DUPLICATE KEY UPDATE` for upserts
- Enforces utf8mb4 charset for full Unicode support
- Explicit transactions for batch safety

### Incremental Sync

For upsert mode with date-based watermarks:
- Configure `date_updated_columns` in config (priority order)
- First sync: full load, records `last_sync_timestamp`
- Subsequent: `WHERE date_column > last_sync_timestamp` (NULL-safe)
- Per-table tracking in `_mssql_pg_migrate.table_state`

## Performance Tuning

Key parameters (auto-tuned if omitted):

| Parameter | Bulk Load | Upsert Mode |
|-----------|-----------|-------------|
| `workers` | 6 | 6 |
| `chunk_size` | 100K-120K | 25K-75K (smaller = less lock contention) |
| `parallel_readers` | 12-14 | 8 |
| `parallel_writers` | 8-10 | 6 |

**UNLOGGED tables** (`use_unlogged_tables: true`): ~65% throughput improvement for initial migrations.

## Exit Codes

For Airflow retry logic:
- `0`: Success
- `1`: Config error (don't retry)
- `2`: Connection error (retry)
- `3`: Transfer error
- `4`: Validation error
- `5`: User cancelled (SIGINT)
- `6`: State/config changed
- `7`: IO error (potentially recoverable)

## Testing

```bash
cargo test                                    # All tests
RUST_LOG=debug cargo test -- --nocapture      # With logging
cargo test config::tests::test_yaml_loading   # Specific test
```

Test scripts in `scripts/`: `test-dry-run.sh`, `test-airflow.sh`, `test-health-check.sh`, `test-signals.sh`

## Code Style

- Rust 2021 edition, MSRV 1.75
- `cargo fmt` for formatting (required)
- `cargo clippy` with no warnings
- Logging via `tracing` crate (no `println!` in library code)
- Doc comments `///` on public items

## Pre-Commit Requirements

**Always run the `pre-commit-test-runner` agent before committing any code changes.** This validates changes through testing before they are committed to a branch.
