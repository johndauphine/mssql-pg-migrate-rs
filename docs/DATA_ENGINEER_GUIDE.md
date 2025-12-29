# mssql-pg-migrate: Data Engineer's Guide

A comprehensive technical reference for data engineers using mssql-pg-migrate-rs, a high-performance MSSQL to PostgreSQL migration tool.

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Transfer Modes](#transfer-modes)
4. [Change Detection](#change-detection)
5. [Multi-Tier Verification](#multi-tier-verification)
6. [Configuration Reference](#configuration-reference)
7. [CLI Commands](#cli-commands)
8. [Performance Tuning](#performance-tuning)
9. [Production Deployment](#production-deployment)
10. [Troubleshooting](#troubleshooting)

---

## Overview

mssql-pg-migrate-rs is a production-ready data migration tool designed for:

- **High throughput**: 160K-193K rows/sec for bulk operations, ~80K rows/sec for upsert
- **Incremental sync**: Hash-based change detection transfers only modified rows
- **Headless operation**: Ideal for Kubernetes, Airflow DAGs, and CI/CD pipelines
- **Resume capability**: JSON state files enable safe restart after interruption
- **Memory safety**: Auto-tuning prevents OOM conditions

### Performance Benchmarks

Tested with Stack Overflow 2010 dataset (19.3M rows, 10 tables):

| Mode | Duration | Throughput | Use Case |
|------|----------|------------|----------|
| `drop_recreate` | 119s | 162,452 rows/sec | Full refresh |
| `truncate` | ~100s | ~193,000 rows/sec | Schema-preserving refresh |
| `upsert` | ~240s | ~80,000 rows/sec | Incremental sync |

---

## Architecture

### Data Flow Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              MIGRATION FLOW                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────┐    ┌──────────────┐    ┌────────────────┐    ┌─────────────┐ │
│  │  Config  │───>│  Auto-Tune   │───>│ Schema Extract │───>│ Connection  │ │
│  │  Load    │    │  (RAM/CPU)   │    │   (MSSQL)      │    │   Pools     │ │
│  └──────────┘    └──────────────┘    └────────────────┘    └──────┬──────┘ │
│                                                                    │        │
│                                                                    ▼        │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                        TRANSFER ENGINE                                │  │
│  │  ┌─────────────┐   ┌────────────────┐   ┌──────────────────────────┐ │  │
│  │  │  Parallel   │──>│  Read-Ahead    │──>│  Hash-Based Filtering    │ │  │
│  │  │  Readers    │   │  Buffer Queue  │   │  (Upsert Mode Only)      │ │  │
│  │  │  (per table)│   │  (4-32 chunks) │   │                          │ │  │
│  │  └─────────────┘   └────────────────┘   └────────────┬─────────────┘ │  │
│  │                                                       │               │  │
│  │                                                       ▼               │  │
│  │  ┌──────────────────────────────────────────────────────────────────┐│  │
│  │  │                     PARALLEL WRITERS                              ││  │
│  │  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐              ││  │
│  │  │  │ Writer1 │  │ Writer2 │  │ Writer3 │  │ Writer4 │   ...        ││  │
│  │  │  │ (COPY)  │  │ (COPY)  │  │ (COPY)  │  │ (COPY)  │              ││  │
│  │  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘              ││  │
│  │  └──────────────────────────────────────────────────────────────────┘│  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                    │        │
│                                                                    ▼        │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  FINALIZATION: Create Indexes │ Create FKs │ Create Constraints      │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                    │        │
│                                                                    ▼        │
│                         ┌─────────────────────┐                            │
│                         │   Migration Result  │                            │
│                         │   + State File      │                            │
│                         └─────────────────────┘                            │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Responsibility |
|-----------|----------------|
| **Config Loader** | YAML/JSON parsing, validation, environment variable expansion |
| **Auto-Tuner** | RAM/CPU detection, parameter optimization |
| **Schema Extractor** | Table, column, index, FK, constraint discovery from MSSQL |
| **Connection Pools** | bb8 (MSSQL), deadpool (PostgreSQL) connection management |
| **Transfer Engine** | Parallel read/write orchestration with range tracking |
| **Verify Engine** | Multi-tier hash comparison for data consistency |
| **State Manager** | Resume capability, progress tracking, config hash validation |

### Large Table Handling

Tables exceeding `large_table_threshold` (default: 5M rows) are automatically partitioned:

```
Table: orders (20M rows, single-column integer PK)
├── Partition 1: pk >= 0 AND pk < 5,000,000
├── Partition 2: pk >= 5,000,000 AND pk < 10,000,000
├── Partition 3: pk >= 10,000,000 AND pk < 15,000,000
└── Partition 4: pk >= 15,000,000 AND pk <= 20,000,000

Each partition processed by parallel readers with keyset pagination
```

---

## Transfer Modes

### Mode Comparison

| Feature | `drop_recreate` | `truncate` | `upsert` |
|---------|-----------------|------------|----------|
| **Target State Required** | Empty or existing | Existing or new | Existing with data |
| **Primary Key Required** | No | No | **Yes** |
| **Schema Preserved** | No | Yes | Yes |
| **Data Preservation** | None | None | Existing data kept |
| **Transfer Type** | Full copy | Full copy | Delta sync |
| **Change Detection** | N/A | N/A | MD5 hash-based |
| **Deletes Performed** | N/A | N/A | No (safety) |
| **Speed** | Fastest | Fastest | Moderate |
| **Network Usage** | Full dataset | Full dataset | Changed rows only |

### DROP_RECREATE Mode

**Best for**: Initial migrations, full data refresh, development environments

```yaml
migration:
  target_mode: drop_recreate
```

**Behavior**:
1. Drops existing target tables
2. Recreates tables from source schema with mapped PostgreSQL types
3. Transfers all rows using binary COPY protocol
4. Optionally creates indexes, foreign keys, constraints

**Advantages**:
- Fastest mode for full data transfer
- Cleanest target state (no residual data)
- No PK requirement

**Considerations**:
- Destroys existing target data
- Dependent objects (views, functions) may break
- Sequences reset to source values

### TRUNCATE Mode

**Best for**: Periodic refreshes where table structure must be preserved

```yaml
migration:
  target_mode: truncate
```

**Behavior**:
1. Truncates existing tables (deletes all rows, preserves structure)
2. Creates tables if they don't exist
3. Ensures `row_hash` column exists (if hash detection enabled)
4. Optionally converts to UNLOGGED for faster writes
5. Transfers all rows

**Advantages**:
- Preserves table structure, permissions, dependencies
- Faster than drop_recreate when structure matches
- Can use UNLOGGED tables for speed boost

**Considerations**:
- Truncate requires exclusive lock (briefly)
- Existing constraints remain (may cause conflicts)

### UPSERT Mode

**Best for**: Incremental sync, data warehousing, near-real-time replication

```yaml
migration:
  target_mode: upsert
  use_hash_detection: true
  row_hash_column: row_hash
  hash_text_columns: false
  upsert_batch_size: 2000
  upsert_parallel_tasks: 4
```

**Behavior**:
1. Validates primary key exists on all tables
2. Uses multi-tier verification to detect differences
3. INSERTs new rows
4. UPDATEs changed rows (where hash differs)
5. Does NOT delete rows (safety feature)

**Change Detection Process**:

```
Source Table (MSSQL)          Target Table (PostgreSQL)
┌─────┬──────────┬──────┐    ┌─────┬──────────┬──────────────────────┐
│ PK  │  Data    │      │    │ PK  │  Data    │ row_hash             │
├─────┼──────────┼──────┤    ├─────┼──────────┼──────────────────────┤
│  1  │  Alice   │      │───>│  1  │  Alice   │ a1b2c3d4...          │ Match (skip)
│  2  │  Bob*    │      │───>│  2  │  Robert  │ e5f6g7h8...          │ Mismatch (UPDATE)
│  3  │  Carol   │      │───>│     │          │                      │ Missing (INSERT)
│     │          │      │    │  4  │  Dave    │ i9j0k1l2...          │ Extra (keep)
└─────┴──────────┴──────┘    └─────┴──────────┴──────────────────────┘
```

**Advantages**:
- Minimal data transfer (only changes)
- Preserves existing target data
- Suitable for append-heavy workloads
- Safe (no deletes without explicit request)

**Requirements**:
- All tables must have primary keys
- Target tables should exist with `row_hash` column (auto-created if missing)

---

## Change Detection

### Hash Computation Algorithm

The tool uses MD5 hashing to detect row changes. Hash is computed on **non-PK columns only** (PKs identify rows, not content).

**Hash Function** (`crates/mssql-pg-migrate/src/target/mod.rs`):

```rust
fn calculate_row_hash(
    row: &[SqlValue],
    pk_indices: &[usize],
    skip_indices: &[usize],  // Text columns if hash_text_columns: false
) -> String {
    let mut hasher = Md5::new();

    for (i, value) in row.iter().enumerate() {
        if pk_indices.contains(&i) || skip_indices.contains(&i) {
            continue;  // Skip PKs and optional text columns
        }

        let normalized = match value {
            SqlValue::Null => "\\N".to_string(),
            SqlValue::Bool(b) => if *b { "t" } else { "f" }.to_string(),
            SqlValue::Float(f) => format!("{:.15}", f),  // 15 decimal precision
            SqlValue::DateTime(dt) => dt.format("%Y-%m-%d %H:%M:%S%.6f"),
            // ... other types
        };

        hasher.update(normalized.as_bytes());
        hasher.update(b"|");  // Separator
    }

    format!("{:x}", hasher.finalize())  // 32-char hex
}
```

### Value Normalization

Cross-database hash comparison requires identical normalization:

| Data Type | MSSQL Normalization | PostgreSQL Normalization |
|-----------|---------------------|--------------------------|
| DateTime | `CONVERT(VARCHAR, col, 121)` | `TO_CHAR(col, 'YYYY-MM-DD HH24:MI:SS.US')` |
| Boolean/Bit | `'t'` or `'f'` | `CASE WHEN col THEN '1' ELSE '0' END` |
| Float (f64) | `STR(col, 25, 15)` | `col::text` |
| Decimal | `CONVERT(VARCHAR, col)` | `col::text` |
| UUID | `LOWER(CONVERT(VARCHAR(36), col))` | `col::text` (lowercase) |
| Binary | `CONVERT(VARCHAR(MAX), col, 2)` (hex) | `encode(col, 'hex')` |
| NULL | `'\N'` literal | `'\N'` literal |

### Text Column Handling

By default, text columns are **excluded** from hash computation for performance:

```yaml
migration:
  hash_text_columns: false  # Default: skip TEXT, NTEXT, VARCHAR(MAX), NVARCHAR(MAX), XML
```

**Excluded types** (when `hash_text_columns: false`):
- `text`, `ntext`
- `varchar(max)`, `nvarchar(max)`
- `xml`

**When to enable `hash_text_columns: true`**:
- You need to detect changes in large text fields
- Text columns contain critical business data
- Performance impact is acceptable

### Hash Storage

The `row_hash` column is automatically added to target tables:

```sql
ALTER TABLE target_schema.table_name
ADD COLUMN IF NOT EXISTS row_hash VARCHAR(32);
```

---

## Multi-Tier Verification

The `verify` command and upsert mode use a 4-tier drill-down approach for efficient difference detection.

### Tier Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MULTI-TIER VERIFICATION PROCESS                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  TIER 1: COARSE PARTITION (~1M rows each)                                   │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  NTILE(n) partitioning by row_number                                  │  │
│  │  Compare: COUNT(*) + CHECKSUM_AGG(LEFT(row_hash, 8))                 │  │
│  │                                                                       │  │
│  │  Partition 1: ✓ Match     │  Partition 2: ✗ Mismatch  │  ...         │  │
│  │  (Skip)                    │  (Drill down)             │              │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                       │                                      │
│                                       ▼                                      │
│  TIER 2: FINE RANGES (~10K rows each)                                       │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  ROW_NUMBER() ranges within mismatched Tier 1 partitions             │  │
│  │  Compare: COUNT(*) + aggregate hash                                   │  │
│  │                                                                       │  │
│  │  Range 1-10K: ✓  │  Range 10K-20K: ✗  │  Range 20K-30K: ✓  │  ...   │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                              │                                               │
│                              ▼                                               │
│  TIER 3: ROW-LEVEL COMPARISON                                               │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  Fetch individual (PK, row_hash) pairs for mismatched ranges         │  │
│  │  Build HashMap<PK, Hash> for both source and target                  │  │
│  │                                                                       │  │
│  │  Compare:                                                             │  │
│  │  • In source, not in target → INSERT needed                          │  │
│  │  • In both, hash differs → UPDATE needed                             │  │
│  │  • In target, not in source → DELETE candidate (skipped for safety)  │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                              │                                               │
│                              ▼                                               │
│  TIER 4: SYNC (Upsert Mode Only)                                            │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  Transfer only identified changed rows                               │  │
│  │  INSERT...ON CONFLICT for efficient merging                          │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Partition Hash Algorithm

Both MSSQL and PostgreSQL compute aggregate hashes identically:

**MSSQL**:
```sql
SELECT
    partition_num,
    COUNT(*) as row_count,
    CHECKSUM_AGG(
        CONVERT(INT, CONVERT(VARBINARY(4), LEFT(row_hash, 8), 2))
    ) as partition_hash
FROM (
    SELECT *, NTILE(@partitions) OVER (ORDER BY pk) as partition_num
    FROM table_name
) sub
GROUP BY partition_num
```

**PostgreSQL**:
```sql
SELECT
    partition_num,
    COUNT(*) as row_count,
    BIT_XOR(
        ('x' || COALESCE(LEFT(row_hash, 8), '00000000'))::bit(32)::integer
    ) as partition_hash
FROM (
    SELECT *, NTILE(partitions) OVER (ORDER BY pk) as partition_num
    FROM table_name
) sub
GROUP BY partition_num
```

**Why this works**:
- `LEFT(row_hash, 8)` = first 8 hex chars = 4 bytes = 32-bit integer
- `CHECKSUM_AGG` (MSSQL) and `BIT_XOR` (PostgreSQL) both perform XOR aggregation
- XOR is order-independent: same rows in any order produce same result
- XOR is self-inverse: same row twice cancels out

### Verification Configuration

```yaml
migration:
  batch_verify:
    tier1_batch_size: 1000000   # Rows per Tier 1 partition
    tier2_batch_size: 10000     # Rows per Tier 2 range
    parallel_verify_ranges: 4   # Concurrent range verifications
```

---

## Configuration Reference

### Complete Configuration Example

```yaml
# Source database connection
source:
  type: mssql                    # Required: "mssql" or "sqlserver"
  host: mssql.example.com        # Required: hostname or IP
  port: 1433                     # Optional: default 1433
  database: SourceDB             # Required: database name
  user: sa                       # Required: username
  password: "YourPassword"       # Required: password
  schema: dbo                    # Optional: default "dbo"
  encrypt: true                  # Optional: default true
  trust_server_cert: false       # Optional: default false (set true for dev)

# Target database connection
target:
  type: postgres                 # Required: "postgres" or "postgresql"
  host: postgres.example.com     # Required: hostname or IP
  port: 5432                     # Optional: default 5432
  database: target_db            # Required: database name
  user: postgres                 # Required: username
  password: "YourPassword"       # Required: password
  schema: public                 # Optional: default "public"
  ssl_mode: require              # Optional: disable, allow, prefer, require

# Migration settings
migration:
  # Transfer mode
  target_mode: upsert            # drop_recreate, truncate, or upsert

  # Worker parallelism (auto-tuned if not set)
  workers: 4                     # Parallel table transfers
  chunk_size: 50000              # Rows per transfer batch
  parallel_readers: 8            # Reader tasks per large table
  write_ahead_writers: 4         # Parallel writer tasks

  # Memory management
  memory_budget_percent: 70      # % of RAM for buffers
  read_ahead_buffers: 8          # Pipeline buffer chunks

  # Large table partitioning
  large_table_threshold: 5000000 # Rows to trigger partitioning
  max_partitions: 12             # Maximum partitions per table
  min_rows_per_partition: 200000 # Minimum rows per partition

  # Connection pooling
  max_mssql_connections: 50      # MSSQL pool size
  max_pg_connections: 40         # PostgreSQL pool size

  # PostgreSQL optimization
  use_binary_copy: true          # Binary COPY format (~2x faster)
  use_unlogged_tables: false     # UNLOGGED mode (faster, not crash-safe)
  copy_buffer_rows: 10000        # COPY flush threshold

  # Upsert mode settings
  use_hash_detection: true       # Enable hash-based change detection
  row_hash_column: row_hash      # Hash column name
  hash_text_columns: false       # Include text columns in hash
  upsert_batch_size: 2000        # Rows per upsert batch
  upsert_parallel_tasks: 4       # Parallel upsert operations

  # Table filtering (glob patterns)
  include_tables:                # Empty = all tables
    - "sales_*"
    - "inventory_*"
  exclude_tables:
    - "*_archive"
    - "temp_*"

  # Schema objects
  create_indexes: true           # Create non-PK indexes
  create_foreign_keys: true      # Create foreign key constraints
  create_check_constraints: true # Create check constraints

  # Batch verification (for upsert/verify)
  batch_verify:
    tier1_batch_size: 1000000    # Coarse partition size
    tier2_batch_size: 10000      # Fine range size
    parallel_verify_ranges: 4    # Concurrent verifications
```

### Environment Variable Support

Passwords and sensitive values can use environment variables:

```yaml
source:
  password: ${MSSQL_PASSWORD}

target:
  password: ${POSTGRES_PASSWORD}
```

---

## CLI Commands

### Global Options

```bash
mssql-pg-migrate [OPTIONS] <COMMAND>

Options:
  -c, --config <PATH>           Config file path (default: config.yaml)
      --state-file <PATH>       State file for resume capability
      --output-json             Output results as JSON
      --log-format <FORMAT>     Log format: text or json (default: text)
      --verbosity <LEVEL>       Log level: debug, info, warn, error
      --shutdown-timeout <SEC>  Graceful shutdown timeout (default: 60)
      --progress               Print progress as JSON lines
```

### run - Execute Migration

```bash
mssql-pg-migrate -c config.yaml run [OPTIONS]

Options:
      --dry-run              Validate without transferring data
      --source-schema <NAME> Override source schema
      --target-schema <NAME> Override target schema
      --workers <N>          Override worker count
```

**Example workflows**:

```bash
# Basic migration
mssql-pg-migrate -c config.yaml run

# Dry run to validate
mssql-pg-migrate -c config.yaml run --dry-run

# With resume capability
mssql-pg-migrate -c config.yaml --state-file /tmp/migration.state run

# JSON output for Airflow
mssql-pg-migrate -c config.yaml --output-json run
```

### resume - Continue Interrupted Migration

```bash
mssql-pg-migrate -c config.yaml --state-file /tmp/migration.state resume
```

**State file validation**:
- Config hash must match (prevents resume after config changes)
- State file must exist and be readable
- Run ID preserved for continuity

### verify - Data Consistency Check

```bash
mssql-pg-migrate -c config.yaml verify [OPTIONS]

Options:
      --tier1-size <N>  Tier 1 partition size (default: 1000000)
      --tier2-size <N>  Tier 2 range size (default: 10000)
```

**Output includes**:
- Per-table verification status
- Row counts (source vs target)
- Mismatch details (inserts, updates, deletes needed)

### validate - Row Count Check

```bash
mssql-pg-migrate -c config.yaml validate
```

Quick check that row counts match between source and target. Faster than full `verify`.

### health-check - Connection Test

```bash
mssql-pg-migrate -c config.yaml health-check
```

Validates connectivity to both source and target databases.

### init - Interactive Configuration Wizard

```bash
mssql-pg-migrate init [OPTIONS]

Options:
  -o, --output <PATH>  Output file (default: config.yaml)
      --advanced       Show advanced tuning options
  -f, --force          Overwrite existing file without confirmation
```

### tui - Terminal UI Mode

```bash
mssql-pg-migrate -c config.yaml tui
```

Launches interactive terminal UI with real-time progress monitoring.

---

## Performance Tuning

### Auto-Tuning Behavior

When parameters are not specified, the tool auto-tunes based on system resources:

| Parameter | Auto-Tuning Formula |
|-----------|---------------------|
| `workers` | `min(max(cores/2, 4), 8)` |
| `parallel_readers` | `min(max(cores, 8), 16)` |
| `write_ahead_writers` | `min(max(2*cores/3, 6), 12)` |
| `chunk_size` | Based on RAM and memory_budget_percent |
| `read_ahead_buffers` | 8-32 based on available RAM |
| `max_mssql_connections` | Based on workers + readers |
| `max_pg_connections` | Based on workers + writers |

### Memory Budget Calculation

```
available_memory = system_ram * memory_budget_percent / 100
buffer_memory = chunk_size * read_ahead_buffers * avg_row_size
target: buffer_memory <= available_memory / safety_factor(2x)
```

### Recommended Settings by Scale

#### Small Dataset (<1M rows)

```yaml
migration:
  workers: 2
  chunk_size: 25000
  parallel_readers: 4
  write_ahead_writers: 2
```

#### Medium Dataset (1M-50M rows)

```yaml
migration:
  workers: 4
  chunk_size: 50000
  parallel_readers: 8
  write_ahead_writers: 4
```

#### Large Dataset (50M-500M rows)

```yaml
migration:
  workers: 6
  chunk_size: 75000
  parallel_readers: 12
  write_ahead_writers: 8
  large_table_threshold: 10000000
```

#### Enterprise Scale (500M+ rows)

```yaml
migration:
  workers: 8
  chunk_size: 100000
  parallel_readers: 16
  write_ahead_writers: 12
  large_table_threshold: 20000000
  max_partitions: 16
  memory_budget_percent: 80
```

### UNLOGGED Tables Optimization

For maximum write speed (at the cost of crash safety):

```yaml
migration:
  use_unlogged_tables: true  # Tables not WAL-logged during transfer
```

**Warning**: Data not crash-safe until migration completes. Tables are converted back to LOGGED after successful migration.

### Binary COPY Protocol

Enabled by default, provides ~2x speedup over text format:

```yaml
migration:
  use_binary_copy: true
```

---

## Production Deployment

### Airflow DAG Integration

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'mssql_to_pg_sync',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    migrate = BashOperator(
        task_id='incremental_sync',
        bash_command='''
            mssql-pg-migrate \
                -c /opt/airflow/config/migration.yaml \
                --state-file /tmp/{{ run_id }}.state \
                --output-json \
                --progress \
                {{ 'resume' if task_instance.try_number > 1 else 'run' }}
        ''',
        do_xcom_push=True,
    )

    verify = BashOperator(
        task_id='verify_sync',
        bash_command='''
            mssql-pg-migrate \
                -c /opt/airflow/config/migration.yaml \
                --output-json \
                verify
        ''',
    )

    migrate >> verify
```

### Kubernetes Deployment

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: mssql-pg-migrate
spec:
  template:
    spec:
      containers:
      - name: migrate
        image: your-registry/mssql-pg-migrate:latest
        args:
          - "-c"
          - "/config/migration.yaml"
          - "--state-file"
          - "/state/migration.state"
          - "run"
        volumeMounts:
          - name: config
            mountPath: /config
          - name: state
            mountPath: /state
        resources:
          requests:
            memory: "2Gi"
            cpu: "2"
          limits:
            memory: "8Gi"
            cpu: "4"
      volumes:
        - name: config
          configMap:
            name: migration-config
        - name: state
          persistentVolumeClaim:
            claimName: migration-state
      restartPolicy: OnFailure
  backoffLimit: 3
```

### Docker Compose

```yaml
version: '3.8'
services:
  migrate:
    image: your-registry/mssql-pg-migrate:latest
    volumes:
      - ./config.yaml:/config.yaml:ro
      - ./state:/state
    command: ["-c", "/config.yaml", "--state-file", "/state/migration.state", "run"]
    environment:
      - MSSQL_PASSWORD=${MSSQL_PASSWORD}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
    deploy:
      resources:
        limits:
          memory: 4G
```

### Exit Codes

| Code | Meaning | Action |
|------|---------|--------|
| 0 | Success | Continue pipeline |
| 1 | Config error | Fix configuration (don't retry) |
| 2 | Connection error | Retry after delay |
| 3 | Transfer error | Check logs, may retry |
| 4 | Validation error | Investigate data issues |
| 5 | Cancelled | User/signal interruption |
| 6 | State error | Check state file |
| 7 | IO error | Check disk space, permissions |

### Signal Handling

- **SIGINT (Ctrl-C)**: Graceful shutdown with configurable timeout
- **SIGTERM**: Same as SIGINT (Kubernetes pod termination)
- **State saved**: Current progress written to state file before exit

---

## Troubleshooting

### Common Issues

#### "No primary key" Error in Upsert Mode

```
Error: Table 'orders' has no primary key, required for upsert mode
```

**Solution**: Add primary key to source table or switch to `truncate` mode.

#### Hash Mismatch Due to Precision

Floating-point precision differences between MSSQL and PostgreSQL:

**Solution**: Verify your float columns are using consistent precision. Consider using DECIMAL for exact values.

#### Memory Pressure / OOM

```
Error: Out of memory
```

**Solutions**:
1. Reduce `memory_budget_percent`
2. Reduce `chunk_size`
3. Reduce `read_ahead_buffers`
4. Reduce `parallel_readers`

#### Connection Pool Exhaustion

```
Error: Connection pool timeout
```

**Solutions**:
1. Increase `max_mssql_connections` / `max_pg_connections`
2. Reduce `workers` or `parallel_readers`

#### Resume Failed: Config Hash Mismatch

```
Error: Config hash mismatch - cannot resume with different configuration
```

**Solution**: Either use the original config or start a new migration (delete state file).

### Debugging

Enable debug logging:

```bash
mssql-pg-migrate -c config.yaml --verbosity debug run
```

JSON log format for parsing:

```bash
mssql-pg-migrate -c config.yaml --log-format json run 2>&1 | jq
```

Progress monitoring:

```bash
mssql-pg-migrate -c config.yaml --progress run 2>&1 | \
    while read line; do echo "$line" | jq -r '.table + ": " + (.rows_transferred | tostring)'; done
```

---

## Type Mapping Reference

| MSSQL Type | PostgreSQL Type | Notes |
|------------|-----------------|-------|
| `bit` | `boolean` | |
| `tinyint` | `smallint` | PostgreSQL has no unsigned types |
| `smallint` | `smallint` | |
| `int` | `integer` | |
| `bigint` | `bigint` | |
| `decimal(p,s)` | `numeric(p,s)` | Precision preserved |
| `numeric(p,s)` | `numeric(p,s)` | |
| `money` | `numeric(19,4)` | |
| `smallmoney` | `numeric(10,4)` | |
| `float` | `double precision` | |
| `real` | `real` | |
| `char(n)` | `char(n)` | |
| `varchar(n)` | `varchar(n)` | |
| `varchar(max)` | `text` | |
| `nchar(n)` | `char(n)` | UTF-8 in PostgreSQL |
| `nvarchar(n)` | `varchar(n)` | |
| `nvarchar(max)` | `text` | |
| `text` | `text` | Deprecated in MSSQL |
| `ntext` | `text` | Deprecated in MSSQL |
| `datetime` | `timestamp` | |
| `datetime2` | `timestamp` | Higher precision |
| `smalldatetime` | `timestamp` | |
| `date` | `date` | |
| `time` | `time` | |
| `datetimeoffset` | `timestamptz` | |
| `uniqueidentifier` | `uuid` | |
| `binary(n)` | `bytea` | |
| `varbinary(n)` | `bytea` | |
| `varbinary(max)` | `bytea` | |
| `image` | `bytea` | Deprecated in MSSQL |
| `xml` | `text` | |
| `geometry` | `text` | WKT format |
| `geography` | `text` | WKT format |

---

## Appendix: State File Format

```json
{
  "run_id": "550e8400-e29b-41d4-a716-446655440000",
  "config_hash": "sha256:a1b2c3d4...",
  "status": "running",
  "started_at": "2025-01-15T10:30:00Z",
  "updated_at": "2025-01-15T10:35:00Z",
  "tables": {
    "orders": {
      "status": "completed",
      "total_rows": 5000000,
      "transferred_rows": 5000000,
      "skipped_rows": 0,
      "partitions": [
        {
          "partition_id": 0,
          "status": "completed",
          "last_pk": "5000000",
          "rows_transferred": 5000000
        }
      ]
    },
    "customers": {
      "status": "in_progress",
      "total_rows": 1000000,
      "transferred_rows": 500000,
      "last_pk": "500000"
    }
  }
}
```

---

*Documentation generated for mssql-pg-migrate-rs v0.8.2*
