# mssql-pg-migrate-rs

High-performance MSSQL to PostgreSQL migration tool written in Rust.

Designed for headless operation in scripted environments, Kubernetes, and Airflow DAGs.

## Features

- **High throughput** - PostgreSQL COPY protocol for bulk inserts
- **Three target modes** - `drop_recreate`, `truncate`, `upsert`
- **Resume capability** - JSON state file for process restartability
- **Static binary** - No runtime dependencies, ideal for containers
- **Airflow integration** - JSON output for XCom, automatic retry support

## Installation

### Download pre-built binaries

Download from [GitHub Releases](https://github.com/johndauphine/mssql-pg-migrate-rs/releases/latest).

### Build from source

```bash
# Standard build
cargo build --release

# Fully static Linux binary (musl)
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl
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
  password: ${MSSQL_PASSWORD}  # Environment variable support
  schema: dbo
  encrypt: "true"
  trust_server_cert: false

target:
  type: postgres
  host: postgres.example.com
  port: 5432
  database: target_db
  user: postgres
  password: ${PG_PASSWORD}
  schema: public
  ssl_mode: require

migration:
  target_mode: drop_recreate  # or truncate, upsert
  workers: 4
  chunk_size: 100000
  create_indexes: true
  create_foreign_keys: true
  create_check_constraints: true
```

## Target Modes

| Mode | Behavior |
|------|----------|
| `drop_recreate` | Drop target tables, create fresh, bulk copy |
| `truncate` | Truncate existing tables, create if missing, bulk copy |
| `upsert` | INSERT new rows, UPDATE changed rows, preserve target-only data |

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

## License

MIT
