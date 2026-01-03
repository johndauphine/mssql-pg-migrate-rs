# Performance Benchmarks

Comprehensive benchmark results comparing Rust and Go implementations.

## Test Environment

- **Hardware**: Apple M3 Max, 36GB RAM, 14 CPU cores
- **OS**: macOS (Darwin 25.2.0)
- **Databases**: SQL Server 2022, PostgreSQL 15 (Docker containers)
- **Dataset**: StackOverflow2010 (~19.3M rows, 9 tables)
- **Date**: January 2026

> **Note**: SQL Server runs under Rosetta 2 emulation on Apple Silicon, adding overhead. Production Linux deployments will be faster.

## Dataset Details

| Table | Rows | Description |
|-------|------|-------------|
| Votes | 10,143,364 | Largest table |
| Comments | 3,875,183 | Text-heavy |
| Posts | 3,729,195 | Mixed content types |
| Badges | 1,102,020 | Datetime columns |
| Users | 299,398 | Various types |
| PostLinks | 161,519 | Foreign keys |
| VoteTypes | 15 | Small lookup |
| PostTypes | 10 | Small lookup |
| LinkTypes | 2 | Small lookup |
| **Total** | **19,310,706** | |

## Rust vs Go Comparison

### MSSQL → PostgreSQL

| Mode | Rust | Go | Advantage |
|------|------|-----|-----------|
| drop_recreate | **289,372 rows/s** (74s) | 287,000 rows/s (75s) | ~same |
| upsert | 181,450 rows/s (106s) | **235,160 rows/s** (82s) | Go 30% faster |

### PostgreSQL → MSSQL

| Mode | Rust | Go | Advantage |
|------|------|-----|-----------|
| drop_recreate | **145,175 rows/s** (133s) | 140,387 rows/s (137s) | ~same |
| upsert | **80,075 rows/s** (241s) | 74,593 rows/s (258s) | Rust 7% faster |

> **Note**: Go upsert performance improved significantly after memory optimizations (72K→235K for MSSQL→PG).

### Memory Usage

| Direction | Mode | Rust Peak | Go Peak |
|-----------|------|-----------|---------|
| MSSQL→PG | drop_recreate | 4.9 GB | 5.3 GB |
| MSSQL→PG | upsert | 7.4 GB | 10.9 GB |
| PG→MSSQL | drop_recreate | 5.4 GB | 1.8 GB |
| PG→MSSQL | upsert | 3.7 GB | 0.5 GB |

## Key Findings

### 1. Bulk Load Performance is Similar
Both implementations achieve comparable throughput for bulk loads (~290K rows/sec MSSQL→PG). The bottleneck is database I/O, not the application.

### 2. Go Now Leads Upsert Mode (MSSQL→PG)
After significant optimizations, Go is **30% faster** for MSSQL→PG upsert (235K vs 181K rows/s). Go's improvements include:
- Fast-path upsert using COPY + MERGE strategy
- Zero-allocation row scanning with slice recycling
- Optimized keyset pagination

### 3. PG→MSSQL is ~50% Slower Than MSSQL→PG
- PostgreSQL COPY protocol is extremely optimized for bulk writes
- MSSQL bulk insert has more TDS protocol overhead
- MSSQL uses UTF-16 (NVARCHAR) vs PostgreSQL's UTF-8

### 4. Memory Trade-offs
- Go uses less memory for PG→MSSQL due to different buffering strategies
- Rust uses less memory for upsert mode (7.4GB vs 10.9GB)

## Performance Notes

- **Rust** still leads for PG→MSSQL upsert (7% faster)
- **Go** now leads for MSSQL→PG upsert (30% faster) after major optimizations
- Both use similar strategies: staging tables, COPY protocol, parallel workers

## Implemented Optimizations

- [x] Parallel table processing (auto-tuned workers)
- [x] Connection pooling (bb8 for MSSQL, deadpool for PostgreSQL)
- [x] Binary COPY format for reduced serialization
- [x] Parallel readers/writers per table
- [x] UNLOGGED tables option (~65% throughput boost)
- [x] Staging table approach for upsert (2-3x faster than row-by-row)
- [x] Intra-table partitioning for large tables
- [x] 32KB TDS packet size (42% faster bulk inserts)

## Reproduction

```bash
# Build
cargo build --release

# MSSQL → PostgreSQL (drop_recreate)
./target/release/mssql-pg-migrate -c benchmark-config.yaml run

# MSSQL → PostgreSQL (upsert)
./target/release/mssql-pg-migrate -c benchmark-upsert-clean.yaml run

# PostgreSQL → MSSQL (drop_recreate)
./target/release/mssql-pg-migrate -c benchmark-pg-to-mssql.yaml run
```

## Configuration

```yaml
migration:
  workers: 6
  chunk_size: 100000  # 50000 for upsert
  parallel_readers: 8
  parallel_writers: 8
  target_mode: drop_recreate  # or upsert
  large_table_threshold: 1000000
  max_partitions: 8
```

See [PERFORMANCE.md](PERFORMANCE.md) for tuning recommendations.
