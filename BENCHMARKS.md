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

### Single Worker (workers=1, chunk_size=1000)

| Mode | Duration | Throughput | Notes |
|------|----------|------------|-------|
| drop_recreate | 228.8s | 84,388 rows/sec | Full schema recreation |
| truncate | 193.0s | 100,056 rows/sec | Fastest - no DDL overhead |
| upsert | 257.5s | 74,991 rows/sec | ON CONFLICT overhead |

### Observations

1. **truncate is fastest** - No DDL operations, just TRUNCATE + INSERT
2. **drop_recreate** - Includes table creation time
3. **upsert is slowest** - ON CONFLICT DO UPDATE adds overhead for change detection

### Resource Usage

- CPU: ~46-60% utilization (single-threaded migration)
- Memory: Minimal (~50MB resident)
- Network: Localhost, no network bottleneck

## Bottlenecks

1. **Single connection:** Currently uses single source/target connection per table
2. **SQL literals:** Using INSERT with literals instead of COPY protocol
3. **Sequential tables:** Tables processed one at a time (workers=1)

## Future Optimizations

- [ ] Implement PostgreSQL COPY protocol for bulk inserts
- [ ] Parallel table processing with multiple workers
- [ ] Connection pooling for concurrent reads
- [ ] Binary protocol for reduced serialization overhead
