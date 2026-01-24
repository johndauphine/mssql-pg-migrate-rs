# MySQL Performance Tuning Guide

This document covers performance findings for MySQL target migrations, specifically around LOAD DATA LOCAL INFILE and LZ4 text compression features.

## Test Environment

- **Source:** MSSQL Server (StackOverflow2010 dataset)
- **Target:** MySQL 8.0
- **Test Tables:** Posts (3.7M rows), Users (299K rows) - both with large text columns (nvarchar(max))
- **Configuration:** 4 workers, 50,000 chunk_size

## Key Finding: Text Columns Dramatically Slow Migrations

| Table Type | Throughput | Impact |
|------------|------------|--------|
| Without text columns | 194,493 rows/sec | Baseline |
| With text columns | 34,243 rows/sec | **5.7x slower** |

Tables with large text columns (TEXT, LONGTEXT, nvarchar(max)) are significantly slower due to:
- Larger data volume per row
- String escaping overhead
- Memory pressure from buffering

## LOAD DATA LOCAL INFILE

### Configuration

```yaml
migration:
  mysql_load_data: always  # or "never" (default)
```

### How It Works

When enabled, the writer streams batch data as tab-separated values (TSV) directly to MySQL using `LOAD DATA LOCAL INFILE`. This bypasses SQL parsing overhead for bulk inserts.

**Requirements:**
- MySQL server must have `local_infile=ON`
- Client must support local infile (mysql_async does)

### Performance Results

#### Single Table (Isolated Test)

| Method | Write Phase Performance |
|--------|------------------------|
| Batched INSERT | Baseline |
| LOAD DATA | **26% faster** |

#### Full Migration (Parallel, Multiple Tables)

| Method | Total Time | Throughput | Result |
|--------|------------|------------|--------|
| Batched INSERT | 120.7s | 33,387 rows/sec | **Winner** |
| LOAD DATA | ~145s | ~28,000 rows/sec | 20% slower |

### Why LOAD DATA Loses in Parallel Mode

1. **TSV escaping is CPU-intensive** - Converting values to TSV format with proper escaping consumes significant CPU
2. **CPU contention** - TSV generation competes with reader threads for CPU cycles
3. **Batched INSERT is already optimized** - Multi-row INSERT statements are highly efficient

### When to Use LOAD DATA

| Use Case | Recommendation |
|----------|----------------|
| Single large-text table migration | `mysql_load_data: always` |
| Low parallelism (1-2 workers) | `mysql_load_data: always` |
| Reader is bottleneck (slow source) | `mysql_load_data: always` |
| High parallelism (4+ workers) | `mysql_load_data: never` |
| CPU-bound workloads | `mysql_load_data: never` |
| Mixed table sizes | `mysql_load_data: never` |

## LZ4 Text Compression

### Configuration

```yaml
migration:
  compress_text: true  # default: false
```

### How It Works

When enabled, text values larger than 64 bytes are compressed with LZ4 in the transfer pipeline buffer. This reduces memory usage when migrating tables with large text columns. Decompression happens automatically in the writer before inserting into the target database.

### Performance Results

| Metric | No Compression | With Compression | Delta |
|--------|----------------|------------------|-------|
| Total Time | 120.7s | 125.6s | +4.1% |
| Throughput | 33,387 rows/sec | 32,083 rows/sec | **-3.9%** |
| User CPU Time | 68.89s | 75.44s | +9.5% |

### Why Compression Didn't Improve Performance

1. **LZ4 overhead** - While LZ4 is fast, compression/decompression still costs ~10% CPU
2. **Bottleneck mismatch** - The bottleneck was network/disk I/O, not memory
3. **Hot path latency** - Adds processing to every text value in the critical path

### When to Use Compression

| Use Case | Recommendation |
|----------|----------------|
| Memory-constrained environments | `compress_text: true` |
| Very large text values (multi-MB) | `compress_text: true` |
| Risk of OOM during migration | `compress_text: true` |
| Throughput-optimized migrations | `compress_text: false` |
| Small to medium text columns | `compress_text: false` |
| Adequate memory available | `compress_text: false` |

## Recommended Configurations

### Default (Most Workloads)

```yaml
migration:
  workers: 4
  chunk_size: 50000
  # mysql_load_data: never (default)
  # compress_text: false (default)
```

### Memory-Constrained Environment

```yaml
migration:
  workers: 2
  chunk_size: 25000
  compress_text: true
```

### Single Large-Text Table

```yaml
migration:
  workers: 2
  chunk_size: 50000
  mysql_load_data: always
  include_tables:
    - LargeDocuments
```

### High-Throughput (Adequate Resources)

```yaml
migration:
  workers: 8
  chunk_size: 100000
  # Use defaults - INSERT is fastest with high parallelism
```

## Summary

| Feature | Performance Impact | Best Use Case |
|---------|-------------------|---------------|
| `mysql_load_data: always` | +26% single table, -20% parallel | Low parallelism, large text tables |
| `compress_text: true` | -4% throughput, lower memory | Memory-constrained environments |
| Default settings | Optimal for most cases | General workloads |

The default configuration (batched INSERT, no compression) provides the best performance for most migration scenarios. Only enable LOAD DATA or compression when specific constraints require them.
