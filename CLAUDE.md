# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Standard release build
cargo build --release

# With MySQL/MariaDB support
cargo build --release --features mysql

# With Terminal UI
cargo build --release --features tui

# With Kerberos authentication (MSSQL only)
cargo build --release --features kerberos

# All features
cargo build --release --all-features
```

## Testing

```bash
# Run all tests
cargo test

# Run specific test
cargo test test_name

# Run tests for a specific crate
cargo test -p mssql-pg-migrate
cargo test -p mssql-pg-migrate-cli

# Run tests with output
cargo test -- --nocapture
```

## Code Quality

```bash
cargo fmt -- --check
cargo clippy --all-targets --all-features
```

## Running

```bash
# Run migration
./target/release/mssql-pg-migrate -c config.yaml run

# Dry run (validate only)
./target/release/mssql-pg-migrate -c config.yaml run --dry-run

# Health check connections
./target/release/mssql-pg-migrate -c config.yaml health-check

# Interactive config wizard
./target/release/mssql-pg-migrate init
```

## Architecture

This is a Rust workspace with two crates:

- **mssql-pg-migrate** (`crates/mssql-pg-migrate/`) - Core library
- **mssql-pg-migrate-cli** (`crates/mssql-pg-migrate-cli/`) - CLI application

### Plugin Architecture (GoF Patterns)

The codebase uses enum dispatch for zero-cost polymorphism instead of trait objects. All readers/writers are wrapped in `Arc<T>` for cheap cloning:

```
SourceReaderImpl (enum)          TargetWriterImpl (enum)
├── Mssql(Arc<MssqlReader>)      ├── Mssql(Arc<MssqlWriter>)
├── Mysql(Arc<MysqlReader>)      ├── Mysql(Arc<MysqlWriter>)
└── Postgres(Arc<PostgresReader>)└── Postgres(Arc<PostgresWriter>)

DialectImpl (enum)
├── Mssql(MssqlDialect)
├── Mysql(MysqlDialect)
└── Postgres(PostgresDialect)
```

To add a new database driver:
1. Create module under `drivers/` (e.g., `drivers/newdb/`)
2. Implement `Dialect`, `SourceReader`, and/or `TargetWriter` traits
3. Add enum variant to `DialectImpl`, `SourceReaderImpl`, `TargetWriterImpl`
4. Register type mappers in `DriverCatalog::with_builtins()`
5. Feature-gate in `Cargo.toml`

### Core Traits (`crates/mssql-pg-migrate/src/core/`)

- `SourceReader` - Extract schema, stream rows, parallel reading via PK range splitting
- `TargetWriter` - Create schema, write data (binary COPY), manage constraints
- `Dialect` - SQL syntax generation for different database engines
- `TypeMapper` - Convert types between source and target databases
- `StateBackend` - Persist migration state (Postgres/MSSQL/MySQL/no-op backends)

### Data Flow

```
Config → Auto-Tuning → Schema Extraction → Connection Pools
    → Transfer Engine (parallel readers → read-ahead buffer → parallel writers)
    → Finalization (indexes, FKs, check constraints) → State Persistence
```

### Key Modules

All paths relative to `crates/mssql-pg-migrate/src/`:

| Path | Purpose |
|------|---------|
| `drivers/` | Database driver implementations (mssql, postgres, mysql) |
| `drivers/common/` | Shared utilities: TLS, binary COPY parser |
| `transfer/` | Transfer engine with parallel read-ahead/write-ahead |
| `orchestrator/` | Migration workflow coordinator, connection pools |
| `state/` | Migration state backends (postgres, mssql, mysql, no-op) |
| `dialect/` | SQL dialect strategies and cross-database type mapping |
| `core/` | Traits (`SourceReader`, `TargetWriter`, `Dialect`), schema types, value types |
| `config/` | Configuration loading, validation, auto-tuning |

### Target Modes

- **drop_recreate** - Drop and recreate tables (fastest, destructive)
- **upsert** - INSERT...ON CONFLICT DO UPDATE (incremental sync with date watermarks)

### Error Handling

Custom error types in `src/error.rs` with Airflow-compatible exit codes:
- 0: Success
- 1: Config errors
- 2: Connection errors
- 3: Transfer errors
- 4: Validation errors
- 5: Cancelled (SIGINT/SIGTERM)
- 6: State errors
- 7: IO errors

## Feature Flags

| Feature | Crate | Description |
|---------|-------|-------------|
| `mysql` | mssql-pg-migrate | MySQL/MariaDB source support via SQLx |
| `kerberos` | both | MSSQL Kerberos auth via GSSAPI |
| `tui` | mssql-pg-migrate-cli | Terminal UI with ratatui |

## Dependencies Notes

- Uses a **forked tiberius** with 32KB packet size support (42% faster than default)
- PostgreSQL uses binary COPY protocol for optimal ingestion
- MySQL support is feature-gated to avoid pulling SQLx when not needed
