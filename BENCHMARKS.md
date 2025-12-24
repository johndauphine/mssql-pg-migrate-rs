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
| upsert | 242.5s | 79,627 rows/sec | INSERT...ON CONFLICT overhead |

### Key Implementation Details

- **COPY Protocol:** `truncate` and `drop_recreate` use PostgreSQL COPY protocol for bulk inserts
- **Upsert:** Uses `INSERT...ON CONFLICT DO UPDATE` (COPY doesn't support upsert semantics)
- **Read-ahead pipeline:** Concurrent reading/writing with tokio channels

### Observations

1. **truncate is fastest** - No DDL operations, just TRUNCATE + COPY
2. **drop_recreate** - Includes table creation time, still very fast with COPY
3. **upsert is slowest** - ON CONFLICT DO UPDATE adds significant overhead for change detection

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
| upsert | ~75K | 80K | Comparable |

## Bottlenecks

1. **Single connection:** Currently uses single source/target connection per table
2. **Upsert limitation:** Cannot use COPY protocol for upsert operations
3. **Sequential tables:** Tables processed one at a time (workers=1)

## Future Optimizations

- [ ] Parallel table processing with multiple workers
- [ ] Connection pooling for concurrent reads
- [ ] Binary COPY format for reduced serialization overhead
