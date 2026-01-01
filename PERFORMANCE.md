# Performance Tuning Guide

This document provides guidance on tuning `mssql-pg-migrate` for optimal performance based on extensive benchmarking.

## Benchmark Environment

- **Dataset**: StackOverflow 2010 (19.3M rows, 10 tables)
- **Hardware**: WSL2 on Windows, local Docker containers for both databases
- **MSSQL**: SQL Server 2022 in Docker
- **PostgreSQL**: PostgreSQL 15 in Docker

## Performance Results

### Configuration Comparison

| Config | Workers | Chunk | Readers | Writers | Unlogged | Duration | Throughput |
|--------|---------|-------|---------|---------|----------|----------|------------|
| baseline | 6 | 100K | 8 | 6 | false | 110.7s | 174K |
| baseline | 6 | 100K | 8 | 6 | true | 67.3s | 287K |
| high-parallel | 8 | 150K | 10 | 8 | true | 69.1s | 280K |
| low-parallel | 4 | 100K | 6 | 4 | true | 86.4s | 223K |
| **optimal** | 6 | 100K | 12 | 8 | true | 64.3s | 300K |
| **peak** | 6 | 120K | 14 | 10 | true | 61.7s | **313K** |
| over-parallel | 6 | 150K | 16 | 12 | true | 64.5s | 299K |

### Rust vs Go Implementation

Benchmark: Votes table (10.1M rows), drop_recreate mode, 6 workers, 8 readers, 6 writers.

| Metric | Go | Rust | Improvement |
|--------|-----|------|-------------|
| Run 1 | 17.0s (611K rows/s) | 15.0s (677K rows/s) | 12% faster |
| Run 2 | 15.3s (672K rows/s) | 13.3s | 15% faster |
| Run 3 | 17.7s (581K rows/s) | 14.2s (716K rows/s) | 20% faster |
| **Average** | **16.7s** | **14.2s** | **~17% faster** |

*Note: Go times include automatic validation step; Rust does not. Both use identical config.*

### Upsert Mode Performance

Upsert mode (`target_mode: upsert`) uses a staging table approach with `INSERT...ON CONFLICT DO UPDATE`:

| Approach | Chunk Size | Throughput |
|----------|-----------|------------|
| Row-by-row INSERT...ON CONFLICT | N/A | 59K rows/sec |
| Staging table + 123K chunk | 123K | 113K rows/sec |
| Staging table + 25K chunk | 25K | 157K rows/sec |
| Staging table + 50K chunk | 50K | 154K rows/sec |
| **Staging table + 50K (default)** | 50K | **106-200K rows/sec** |

**Key insights**:
- Smaller chunks (25K-75K) perform better due to reduced lock contention during MERGE
- Uses binary COPY protocol for fast staging table population
- PostgreSQL's MVCC handles unchanged rows efficiently at the storage level

## Key Findings

### 1. UNLOGGED Tables (`use_unlogged_tables: true`)

The single biggest performance improvement comes from using UNLOGGED tables:

- **Impact**: ~65% throughput improvement (174K → 287K rows/sec)
- **Trade-off**: UNLOGGED tables are not crash-safe during migration
- **Recommendation**: Enable for initial migrations; disable for production incremental syncs

```yaml
migration:
  use_unlogged_tables: true
```

### 2. Optimal Worker Count

The number of concurrent table workers has a significant impact:

- **Too few (4)**: Underutilizes available parallelism → 223K rows/sec
- **Optimal (6)**: Best balance of parallelism and contention → 300K+ rows/sec
- **Too many (8+)**: Increased contention hurts performance → 280K rows/sec

**Recommendation**: Start with `workers: 6` and adjust based on your hardware.

### 3. Parallel Readers/Writers

These control per-table parallelism for reading from MSSQL and writing to PostgreSQL:

- **Sweet spot**: 12-14 readers, 8-10 writers
- **Diminishing returns**: Beyond 14 readers, contention increases
- **Over-parallelism**: 16+ readers can actually slow things down

```yaml
migration:
  parallel_readers: 12
  parallel_writers: 8
```

### 4. Chunk Size

Chunk size recommendations differ by target mode:

**Bulk Load (drop_recreate/truncate)**:
- **100K-120K**: Optimal for most workloads
- **150K+**: Diminishing returns, higher memory usage
- **50K or less**: Too much overhead from frequent commits

**Upsert Mode**:
- **25K-75K**: Optimal range due to reduced lock contention
- **50K**: Default auto-tuned value for upsert
- **100K+**: Slower due to longer lock hold times during MERGE

### 5. Connection Pool Sizing

Connection pools should be sized to support your parallelism:

```yaml
migration:
  max_mssql_connections: 50  # readers × workers + overhead
  max_pg_connections: 40     # writers × workers + overhead
```

## Recommended Configurations

### High-Performance (Recommended)

Best for initial migrations where speed is critical:

```yaml
migration:
  workers: 6
  chunk_size: 120000
  parallel_readers: 14
  parallel_writers: 10
  max_mssql_connections: 60
  max_pg_connections: 50
  use_unlogged_tables: true
```

### Balanced

Good performance with lower resource usage:

```yaml
migration:
  workers: 6
  chunk_size: 100000
  parallel_readers: 8
  parallel_writers: 6
  max_mssql_connections: 48
  max_pg_connections: 36
  use_unlogged_tables: true
```

### Conservative

For resource-constrained environments:

```yaml
migration:
  workers: 4
  chunk_size: 50000
  parallel_readers: 4
  parallel_writers: 4
  max_mssql_connections: 20
  max_pg_connections: 16
  use_unlogged_tables: false
```

### Upsert Mode

For incremental syncs using upsert:

```yaml
migration:
  target_mode: upsert
  workers: 6
  chunk_size: 50000  # Smaller chunks for better upsert performance
  parallel_readers: 8
  parallel_writers: 6
  max_mssql_connections: 48
  max_pg_connections: 36
```

**Note**: Upsert mode uses a staging table approach: rows are COPY'd to a temp table using binary protocol, then merged into the target with `INSERT...ON CONFLICT DO UPDATE SET`. This is 2-3x faster than row-by-row upserts.

## Troubleshooting

### Slow Performance

1. **Check `use_unlogged_tables`**: Enabling this provides ~65% improvement
2. **Reduce parallelism**: If you see high CPU or connection errors, reduce workers/readers/writers
3. **Check network latency**: Local databases perform significantly better than remote ones
4. **Monitor connection pools**: Ensure pools aren't exhausted (increase `max_*_connections`)

### Out of Memory

1. Reduce `chunk_size` (e.g., 50000)
2. Reduce `parallel_readers` and `parallel_writers`
3. Reduce `workers`
4. Set `memory_budget_percent` lower (default: 70)

### Connection Errors

1. Increase `max_mssql_connections` and `max_pg_connections`
2. Ensure database servers allow enough connections
3. Reduce parallelism settings

## Auto-Tuning Heuristics

The tool automatically tunes parameters based on system resources. The formulas are derived from benchmarking data:

| Parameter | Formula | Constraints |
|-----------|---------|-------------|
| `workers` | cores / 2 | [4, 8] |
| `parallel_readers` | cores | [8, 16] |
| `parallel_writers` | cores × 2/3 | [6, 12] |
| `chunk_size` (bulk load) | 75K + (RAM_GB × 25K / 8) | [50K, 200K] |
| `chunk_size` (upsert) | 50K (fixed) | [10K, memory limit] |

### Example Auto-Tuned Values

| System | Cores | RAM | Workers | Readers | Writers | Chunk |
|--------|-------|-----|---------|---------|---------|-------|
| Small | 4 | 8GB | 4 | 8 | 6 | 100K |
| Medium | 8 | 16GB | 4 | 8 | 6 | 125K |
| Large | 16 | 32GB | 8 | 16 | 10 | 175K |

To override auto-tuning, specify values explicitly in your config file.

## Run-to-Run Variance

Expect 15-20% variance between runs due to:

- Database buffer cache state (warm vs cold)
- OS page cache
- Background processes
- Docker resource contention

For accurate benchmarking, run multiple iterations and take the median result.
