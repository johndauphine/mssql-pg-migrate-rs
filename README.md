# mssql-pg-migrate-rs

High-performance MSSQL to PostgreSQL migration tool written in Rust.

Designed for headless operation in scripted environments, Kubernetes, and Airflow DAGs.

## Performance

Tested with Stack Overflow 2010 dataset (19.3M rows, 9 tables):

| Mode | Duration | Throughput |
|------|----------|------------|
| drop_recreate | 228s | 84,388 rows/sec |
| truncate | 193s | 100,056 rows/sec |
| upsert | 257s | 74,991 rows/sec |

*Single worker, chunk_size=1000, localhost MSSQL → PostgreSQL*

## Features

- **High throughput** - Read-ahead pipeline with concurrent reading/writing
- **Three target modes** - `drop_recreate`, `truncate`, `upsert`
- **Automatic type mapping** - MSSQL to PostgreSQL type conversion
- **Keyset pagination** - Efficient chunked reads using `WHERE pk > last_pk`
- **Resume capability** - JSON state file for process restartability
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

### Run migration

```bash
# Basic migration
mssql-pg-migrate -c config.yaml run

# With state file for resume
mssql-pg-migrate -c config.yaml run --state-file /tmp/migration.state

# Resume interrupted migration
mssql-pg-migrate -c config.yaml run --state-file /tmp/migration.state --resume

# JSON output for Airflow
mssql-pg-migrate -c config.yaml run --output-json
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
  workers: 4
  chunk_size: 50000
  create_indexes: true
  create_foreign_keys: true
  create_check_constraints: true
```

## Target Modes

| Mode | Behavior |
|------|----------|
| `drop_recreate` | Drop target tables, create fresh, bulk insert |
| `truncate` | Truncate existing tables, create if missing, bulk insert |
| `upsert` | INSERT new rows, UPDATE changed rows (ON CONFLICT) |

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
