# mssql-pg-migrate-rs

High-performance MSSQL to PostgreSQL migration tool written in Rust.

Designed for headless operation in scripted environments, Kubernetes, and Airflow DAGs.

## Performance

Tested with Stack Overflow 2010 dataset (19.3M rows, 10 tables):

| Mode | Duration | Throughput |
|------|----------|------------|
| drop_recreate | 119s | 162,452 rows/sec |
| truncate | ~100s | ~193,000 rows/sec |
| upsert | ~240s | ~80,000 rows/sec |

*Auto-tuned parallelism, localhost MSSQL → PostgreSQL, binary COPY protocol*

## Features

- **High throughput** - Parallel readers/writers with read-ahead pipeline
- **Auto-tuning** - Memory-aware configuration based on system RAM and CPU cores
- **Parallel transfers** - Multiple concurrent readers per large table using PK range splitting
- **Binary COPY protocol** - Optimized PostgreSQL ingestion with adaptive buffering
- **Three target modes** - `drop_recreate`, `truncate`, `upsert`
- **Batch hash verification** - Multi-tier verification detects differences efficiently
- **Incremental sync** - Upsert mode uses batch hashing to sync only changed rows
- **Interactive TUI** - Terminal UI for guided migration with real-time progress
- **Configuration wizard** - Interactive `init` command creates config files
- **Automatic type mapping** - MSSQL to PostgreSQL type conversion
- **Keyset pagination** - Efficient chunked reads using `WHERE pk > last_pk`
- **Resume capability** - JSON state file for process restartability
- **Parallel finalization** - Concurrent index and constraint creation
- **Static binary** - No runtime dependencies, ideal for containers
- **Airflow integration** - JSON output for XCom, automatic retry support

## Installation

### Download pre-built binaries

Download from [GitHub Releases](https://github.com/johndauphine/mssql-pg-migrate-rs/releases/latest):

| Platform | Architecture | Binary |
|----------|--------------|--------|
| Linux | x86_64 | `mssql-pg-migrate-linux-x86_64` |
| Linux | ARM64 | `mssql-pg-migrate-linux-aarch64` |
| macOS | Intel | `mssql-pg-migrate-darwin-x86_64` |
| macOS | Apple Silicon | `mssql-pg-migrate-darwin-aarch64` |
| Windows | x86_64 | `mssql-pg-migrate-windows-x86_64.exe` |

```bash
# Linux/macOS
chmod +x mssql-pg-migrate-*
./mssql-pg-migrate-linux-x86_64 -c config.yaml run
```

### Build from source

```bash
cargo build --release
```

## Usage

### Create configuration

```bash
# Interactive wizard
mssql-pg-migrate init

# With advanced options
mssql-pg-migrate init --advanced

# Specify output file
mssql-pg-migrate init -o my-config.yaml
```

### Run migration

```bash
# Basic migration
mssql-pg-migrate -c config.yaml run

# Dry run (validate without transferring)
mssql-pg-migrate -c config.yaml run --dry-run

# With state file for resume
mssql-pg-migrate -c config.yaml --state-file /tmp/migration.state run

# Resume interrupted migration
mssql-pg-migrate -c config.yaml --state-file /tmp/migration.state resume

# JSON output for Airflow
mssql-pg-migrate -c config.yaml --output-json run
```

### Verify data consistency

```bash
# Multi-tier batch hash verification (read-only)
mssql-pg-migrate -c config.yaml verify

# With custom batch sizes
mssql-pg-migrate -c config.yaml verify --tier1-size 500000 --tier2-size 5000
```

### Interactive TUI mode

```bash
# Launch terminal UI (requires tui feature)
mssql-pg-migrate -c config.yaml tui
```

### Validate row counts

```bash
mssql-pg-migrate -c config.yaml validate
```

## Configuration

```yaml
source:
  type: mssql
  host: mssql.example.com
  port: 1433
  database: SourceDB
  user: sa
  password: "YourPassword"
  schema: dbo
  encrypt: "false"
  trust_server_cert: true

target:
  type: postgres
  host: postgres.example.com
  port: 5432
  database: target_db
  user: postgres
  password: "YourPassword"
  schema: public
  ssl_mode: disable

migration:
  target_mode: drop_recreate  # or truncate, upsert
  workers: 4                  # auto-tuned if not set
  chunk_size: 50000           # auto-tuned based on RAM
  parallel_readers: 8         # auto-tuned based on CPU cores
  parallel_writers: 4         # auto-tuned based on CPU cores
  memory_budget_percent: 70   # % of RAM for buffers (default: 70)
  create_indexes: true
  create_foreign_keys: true
  create_check_constraints: true
```

### Auto-Tuning

When `workers`, `chunk_size`, `parallel_readers`, or `parallel_writers` are not specified, the tool automatically tunes them based on:

- **CPU cores**: Determines worker count and parallelism levels
- **Available RAM**: Sets chunk sizes and buffer counts to fit within memory budget
- **Memory budget**: Configurable percentage (default 70%) of system RAM for transfer buffers

This allows optimal performance across different hardware without manual configuration.

## Target Modes

| Mode | Behavior |
|------|----------|
| `drop_recreate` | Drop target tables, create fresh, bulk insert (fastest for full refresh) |
| `truncate` | Truncate existing tables, create if missing, bulk insert |
| `upsert` | Uses batch hashing to detect changes, syncs only modified rows (ideal for incremental sync) |

### Upsert Mode Details

Upsert mode uses multi-tier batch hashing to efficiently detect differences:

1. **Tier 1**: NTILE partitioning compares ~1M row batches
2. **Tier 2**: Drills into mismatched partitions with ~10K row ranges
3. **Tier 3**: Row-level hash comparison identifies specific inserts/updates

Only changed rows are transferred - no deletes are performed for safety.

## Type Mapping

| MSSQL | PostgreSQL |
|-------|------------|
| bit | boolean |
| tinyint | smallint |
| smallint | smallint |
| int | integer |
| bigint | bigint |
| decimal(p,s) | numeric(p,s) |
| float | double precision |
| real | real |
| varchar(n) | varchar(n) |
| nvarchar(n) | varchar(n) |
| nvarchar(max) | text |
| text/ntext | text |
| datetime | timestamp |
| datetime2 | timestamp |
| datetimeoffset | timestamptz |
| date | date |
| time | time |
| uniqueidentifier | uuid |
| binary/varbinary | bytea |

## Airflow Integration

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('mssql_to_pg_migration', start_date=datetime(2025, 1, 1)) as dag:

    migrate = BashOperator(
        task_id='migrate_data',
        bash_command='''
            mssql-pg-migrate -c /opt/airflow/config/migration.yaml run \
                --state-file /tmp/{{ run_id }}.state \
                --output-json \
                {{ '--resume' if task_instance.try_number > 1 else '' }}
        ''',
        do_xcom_push=True,
    )
```

## Docker

```dockerfile
FROM rust:1.75-alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /app
COPY . .
RUN cargo build --release --target x86_64-unknown-linux-musl

FROM scratch
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/mssql-pg-migrate /
ENTRYPOINT ["/mssql-pg-migrate"]
```

Or use the pre-built binary:

```dockerfile
FROM alpine:latest
ADD https://github.com/johndauphine/mssql-pg-migrate-rs/releases/latest/download/mssql-pg-migrate-linux-x86_64 /usr/local/bin/mssql-pg-migrate
RUN chmod +x /usr/local/bin/mssql-pg-migrate
ENTRYPOINT ["mssql-pg-migrate"]
```

## License

MIT
