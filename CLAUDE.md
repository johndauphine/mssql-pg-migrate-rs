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

The codebase uses enum dispatch for zero-cost polymorphism instead of trait objects:

```
SourceReaderImpl (enum)          TargetWriterImpl (enum)
├── Mssql(MssqlReader)           └── Postgres(PostgresWriter)
├── Mysql(MysqlReader)
└── Postgres(PostgresReader)

DialectImpl (enum)
├── Mssql(MssqlDialect)
├── Mysql(MysqlDialect)
└── Postgres(PostgresDialect)
```

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

| Path | Purpose |
|------|---------|
| `src/drivers/` | Database driver implementations (mssql, postgres, mysql) |
| `src/transfer/` | Transfer engine with parallel read-ahead/write-ahead |
| `src/pipeline/` | Template Method pattern for transfer workflow |
| `src/orchestrator/` | Migration workflow coordinator |
| `src/state/` | Migration state backends (DB/file/no-op) |
| `src/typemap/` | MSSQL/MySQL → PostgreSQL type conversion |
| `src/config/` | Configuration loading, validation, auto-tuning |

### Target Modes

- **drop_recreate** - Drop and recreate tables (fastest, destructive)
- **truncate** - Truncate before insert (schema preserving)
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
