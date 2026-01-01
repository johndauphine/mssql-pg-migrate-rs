# Performance Benchmarks

## Test Environment

- **Source:** MSSQL Server 2022 (Docker container)
- **Target:** PostgreSQL 15 (Docker container)
- **Dataset:** Stack Overflow 2010 (~19.3M rows, 9 tables)
- **Hardware:** Apple M1 Mac (localhost to localhost)

## Dataset Details

| Table | Rows | Description |
|-------|------|-------------|
| Votes | 10,143,364 | Largest table |
| Comments | 3,875,183 | Text-heavy |
| Posts | 3,729,195 | Mixed types |
| Badges | 1,102,020 | Datetime columns |
| Users | 299,398 | Various types |
| PostLinks | 161,519 | Foreign keys |
| VoteTypes | 15 | Small lookup |
| PostTypes | 8 | Small lookup |
| LinkTypes | 2 | Small lookup |
| **Total** | **19,310,704** | |

## Results

### Single Worker (workers=1, chunk_size=50000)

| Mode | Duration | Throughput | Notes |
|------|----------|------------|-------|
| truncate | 104s | 185,680 rows/sec | Fastest - COPY protocol, no DDL |
| drop_recreate | 136.6s | 141,323 rows/sec | COPY protocol + table creation |
| upsert | ~96s | ~200,000 rows/sec | Staging table + ON CONFLICT DO UPDATE |

### Key Implementation Details

- **COPY Protocol:** `truncate` and `drop_recreate` use PostgreSQL COPY protocol for bulk inserts
- **Upsert:** Streams to staging table via binary COPY, then merges with `INSERT...ON CONFLICT DO UPDATE SET`
- **Read-ahead pipeline:** Concurrent reading/writing with tokio channels

### Observations

1. **truncate is fastest** - No DDL operations, just TRUNCATE + COPY
2. **drop_recreate** - Includes table creation time, still very fast with COPY
3. **upsert is competitive** - Staging table + parallel partitioning achieves excellent throughput

### Resource Usage

- CPU: ~45-100% utilization (single-threaded migration)
- Memory: Minimal (~50MB resident)
- Network: Localhost, no network bottleneck

## Comparison with Go Implementation

The Rust implementation achieves comparable or better performance:

| Mode | Go (rows/sec) | Rust (rows/sec) | Notes |
|------|---------------|-----------------|-------|
| truncate | ~180K | 185K | Comparable |
| drop_recreate | ~140K | 141K | Comparable |
| upsert | ~75K | ~106K | Rust ~40% faster (staging table optimization) |

## Implemented Optimizations

Since initial benchmarks, the following optimizations have been added:

- [x] Parallel table processing with multiple workers (up to 8)
- [x] Connection pooling for concurrent reads (bb8/deadpool)
- [x] Binary COPY format for reduced serialization overhead
- [x] Parallel readers/writers per table (up to 16 readers, 12 writers)
- [x] UNLOGGED tables option for ~65% throughput boost
- [x] Staging table approach for upsert (2-3x faster than row-by-row)
- [x] Efficient `INSERT...ON CONFLICT DO UPDATE SET` for upserts

See PERFORMANCE.md for current tuning recommendations.
