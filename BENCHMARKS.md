# Performance Benchmarks

Comprehensive benchmark results comparing Rust and Go implementations.

## Test Environment

- **Hardware**: Apple M3 Pro, 36GB RAM, 14 CPU cores
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

| Mode | Rust | Go | Rust Advantage |
|------|------|-----|----------------|
| drop_recreate | **289,372 rows/s** (74s) | 287,000 rows/s (75s) | ~same |
| upsert | **181,450 rows/s** (106s) | 72,628 rows/s (266s) | **2.5x faster** |

### PostgreSQL → MSSQL

| Mode | Rust | Go | Rust Advantage |
|------|------|-----|----------------|
| drop_recreate | **145,175 rows/s** (133s) | 140,387 rows/s (137s) | ~same |
| upsert | **80,075 rows/s** (241s) | 68,825 rows/s (280s) | **16% faster** |

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

### 2. Rust Dominates Upsert Mode
Rust is **2.5x faster** for MSSQL→PG upsert:
- Zero-cost abstractions vs GC overhead during high-allocation workloads
- More efficient MERGE statement generation
- Better async task scheduling under sustained load

### 3. PG→MSSQL is ~50% Slower Than MSSQL→PG
- PostgreSQL COPY protocol is extremely optimized for bulk writes
- MSSQL bulk insert has more TDS protocol overhead
- MSSQL uses UTF-16 (NVARCHAR) vs PostgreSQL's UTF-8

### 4. Memory Trade-offs
- Go uses less memory for PG→MSSQL due to different buffering strategies
- Rust uses less memory for upsert mode (7.4GB vs 10.9GB)

## Why Rust is Faster for Upsert

1. **Zero-Cost Abstractions**: No GC pauses during millions of row allocations
2. **Efficient Serialization**: Static dispatch vs runtime reflection
3. **Async Runtime**: Tokio's poll-based futures vs Go's stackful goroutines
4. **Binary Protocol**: Direct binary COPY reads with minimal parsing overhead

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
