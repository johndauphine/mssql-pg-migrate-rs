# mssql-pg-migrate-rs

High-performance database migration tool written in Rust. Supports MSSQL, PostgreSQL, and MySQL.

Designed for headless operation in scripted environments, Kubernetes, and Airflow DAGs.

## Performance

Tested with Stack Overflow 2010 dataset (19.3M rows, 10 tables):

| Target | Mode | Duration | Throughput |
|--------|------|----------|------------|
| PostgreSQL | drop_recreate | 119s | 162,452 rows/sec |
| PostgreSQL | upsert | ~180s | ~106,000 rows/sec |
| MySQL | drop_recreate | 198s | 97,502 rows/sec |

*Auto-tuned parallelism, localhost transfers, binary COPY (PostgreSQL) / batched INSERT (MySQL)*

## Features

- **Multi-database support** - MSSQL, PostgreSQL, and MySQL as sources or targets
- **High throughput** - Parallel readers/writers with read-ahead pipeline
- **Auto-tuning** - Memory-aware configuration based on system RAM and CPU cores
- **Parallel transfers** - Multiple concurrent readers per large table using PK range splitting
- **Binary COPY protocol** - Optimized PostgreSQL ingestion with adaptive buffering
- **MySQL bulk loading** - Optional LOAD DATA LOCAL INFILE for large text tables
- **Two target modes** - `drop_recreate`, `upsert`
- **Incremental sync** - Upsert mode with date-based watermarks for fast delta syncs
- **Database state storage** - Migration state stored in target database (no external files)
- **Interactive TUI** - Terminal UI for guided migration with real-time progress
- **Configuration wizard** - Interactive `init` command creates config files
- **Automatic type mapping** - Cross-database type conversion
- **Keyset pagination** - Efficient chunked reads using `WHERE pk > last_pk`
- **Resume capability** - Automatic crash recovery from database state
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
# Standard build (MSSQL + PostgreSQL)
cargo build --release

# With MySQL support
cargo build --release --features mysql

# All features (MySQL + TUI + Kerberos)
cargo build --release --all-features
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

# Resume interrupted migration (state stored in database)
mssql-pg-migrate -c config.yaml resume

# JSON output for Airflow
mssql-pg-migrate -c config.yaml --output-json run
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
  type: postgres  # or mysql
  host: postgres.example.com
  port: 5432
  database: target_db
  user: postgres
  password: "YourPassword"
  schema: public
  ssl_mode: disable

migration:
  target_mode: drop_recreate  # or upsert
  workers: 4                  # auto-tuned if not set
  chunk_size: 50000           # auto-tuned based on RAM
  parallel_readers: 8         # auto-tuned based on CPU cores
  parallel_writers: 4         # auto-tuned based on CPU cores
  memory_budget_percent: 70   # % of RAM for buffers (default: 70)
  create_indexes: true
  create_foreign_keys: true
  create_check_constraints: true
  # date_updated_columns:     # For upsert mode: date watermark columns (priority order)
  #   - LastActivityDate
  #   - ModifiedDate
  #   - UpdatedAt
  # mysql_load_data: never    # MySQL: never (default) or always (LOAD DATA LOCAL INFILE)
```

### MySQL Target Configuration

```yaml
target:
  type: mysql
  host: mysql.example.com
  port: 3306
  database: target_db
  user: root
  password: "YourPassword"
  ssl_mode: disable  # disable, prefer, require, verify-ca, verify-full

migration:
  target_mode: drop_recreate
  workers: 4
  chunk_size: 50000
  # mysql_load_data: always  # Use LOAD DATA for large text tables (requires local_infile=ON)
```

See [MySQL Performance Tuning](docs/mysql-performance-tuning.md) for optimization guidance.

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
| `upsert` | Stream to staging table, merge with `INSERT...ON CONFLICT DO UPDATE` (ideal for incremental sync) |

### Upsert Mode Details

Upsert mode streams all rows to PostgreSQL:

1. **Staging**: Rows are COPY'd to a temporary staging table using binary protocol
2. **Merge**: `INSERT...ON CONFLICT DO UPDATE SET` upserts rows efficiently
3. **Optimization**: PostgreSQL's MVCC handles unchanged rows efficiently

No deletes are performed for safety. Tables require a primary key for upsert mode.

#### Date-Based Incremental Sync (High-Water Mark)

For incremental syncs in upsert mode, configure date watermark columns to dramatically reduce processing time:

```yaml
migration:
  target_mode: upsert
  date_updated_columns:
    - LastActivityDate  # Check these columns in priority order
    - ModifiedDate
    - UpdatedAt
    - CreationDate
```

How it works:

1. **First sync**: Full table load, records current timestamp
2. **Subsequent syncs**: Only processes rows where `date_column > last_sync_timestamp`
3. **NULL-safe**: Includes rows with NULL timestamps to avoid missing data
4. **Per-table tracking**: Each table uses its own watermark column and timestamp

Example: With 19.3M rows and no changes, full sync takes ~84s. With date watermarks, incremental sync completes in seconds by filtering at the source.

The tool automatically discovers the first matching date/timestamp column for each table. If no match is found, the table falls back to full sync.

### Database State Storage

Migration state is automatically stored in the target PostgreSQL database (`_mssql_pg_migrate` schema), eliminating the need for external state files:

**Benefits:**
- **Transactional safety** - State updates are atomic with data writes
- **Multi-instance coordination** - Database locking prevents concurrent migrations
- **No file system access** - Works in containerized/restricted environments
- **Built-in audit trail** - Query migration history with SQL
- **Automatic resume** - Crash recovery and incremental sync "just work"

**Schema:**
```sql
_mssql_pg_migrate.migration_runs
  - run_id, config_hash, started_at, completed_at, status

_mssql_pg_migrate.table_state
  - run_id, table_name, rows_total, rows_transferred
  - last_sync_timestamp (for incremental sync)
  - status, error
```

**Querying state:**
```sql
-- View recent migrations
SELECT run_id, started_at, status,
       (SELECT COUNT(*) FROM _mssql_pg_migrate.table_state WHERE run_id = mr.run_id) as tables
FROM _mssql_pg_migrate.migration_runs mr
ORDER BY started_at DESC LIMIT 10;

-- Check table sync timestamps
SELECT table_name, last_sync_timestamp, rows_transferred, status
FROM _mssql_pg_migrate.table_state
WHERE run_id = (SELECT run_id FROM _mssql_pg_migrate.migration_runs ORDER BY started_at DESC LIMIT 1);
```

The state schema is automatically initialized on first run. No configuration or setup required.

## Type Mapping

### MSSQL → PostgreSQL

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

### MSSQL → MySQL

| MSSQL | MySQL |
|-------|-------|
| bit | TINYINT(1) |
| tinyint | TINYINT UNSIGNED |
| smallint | SMALLINT |
| int | INT |
| bigint | BIGINT |
| decimal(p,s) | DECIMAL(p,s) |
| float | DOUBLE |
| real | FLOAT |
| varchar(n) | VARCHAR(n) |
| nvarchar(n) | VARCHAR(n) |
| nvarchar(max) | LONGTEXT |
| text/ntext | LONGTEXT |
| datetime | DATETIME(3) |
| datetime2 | DATETIME(6) |
| datetimeoffset | DATETIME(6) |
| date | DATE |
| time | TIME(6) |
| uniqueidentifier | CHAR(36) |
| binary/varbinary | BLOB/LONGBLOB |

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
