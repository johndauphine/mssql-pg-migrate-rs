//! Data transfer engine with parallel read-ahead/write-ahead pipeline.
//!
//! This module implements a high-performance transfer engine that uses:
//! - Multiple parallel readers (PK range splitting) to maximize source throughput
//! - Multiple parallel writers to maximize target throughput
//! - Read-ahead buffering to overlap read and write operations

use crate::config::TargetMode;
use crate::error::{MigrateError, Result};
use crate::orchestrator::{SourcePoolImpl, TargetPoolImpl};
use crate::source::Table;
use crate::target::SqlValue;
use futures::future::try_join_all;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Transfer job for a single table or partition.
#[derive(Debug, Clone)]
pub struct TransferJob {
    /// Table metadata.
    pub table: Table,

    /// Partition ID (None for non-partitioned).
    pub partition_id: Option<i32>,

    /// Minimum PK value (for keyset pagination).
    pub min_pk: Option<i64>,

    /// Maximum PK value (for keyset pagination).
    pub max_pk: Option<i64>,

    /// Resume from this PK value.
    pub resume_from_pk: Option<i64>,

    /// Target mode for this job.
    pub target_mode: TargetMode,

    /// Target schema name.
    pub target_schema: String,
}

/// Statistics from a transfer job.
#[derive(Debug, Clone, Default)]
pub struct TransferStats {
    /// Time spent querying.
    pub query_time: Duration,

    /// Time spent scanning rows.
    pub scan_time: Duration,

    /// Time spent writing.
    pub write_time: Duration,

    /// Total rows transferred.
    pub rows: i64,

    /// Last primary key value processed.
    pub last_pk: Option<i64>,

    /// Whether the job completed successfully.
    pub completed: bool,
}

/// Chunk data - either SqlValue rows or pre-encoded COPY binary data.
#[derive(Debug)]
enum ChunkData {
    /// Standard row data (for non-upsert or when direct copy is disabled).
    Rows(Vec<Vec<SqlValue>>),
    /// Pre-encoded PostgreSQL COPY binary data (for direct copy upsert).
    Direct {
        /// Pre-encoded COPY binary data including header and trailer.
        data: bytes::Bytes,
        /// Number of rows in this chunk.
        row_count: usize,
    },
}

impl ChunkData {
    fn len(&self) -> usize {
        match self {
            ChunkData::Rows(rows) => rows.len(),
            ChunkData::Direct { row_count, .. } => *row_count,
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A chunk of rows to be transferred.
#[derive(Debug)]
struct RowChunk {
    /// Row data (either SqlValue rows or pre-encoded bytes).
    data: ChunkData,
    /// First PK value in this chunk (for range tracking).
    first_pk: Option<i64>,
    /// Last PK value in this chunk (for resume).
    last_pk: Option<i64>,
    /// Time taken to read this chunk.
    read_time: Duration,
}

/// Tracks completed PK ranges to provide safe resume points.
///
/// With parallel readers, chunks may complete out of order. This tracker
/// maintains a set of completed ranges and only reports a safe resume point
/// when all ranges from the start are contiguous. This prevents data loss
/// during resume operations.
#[derive(Debug)]
struct RangeTracker {
    /// Completed ranges as (end_pk) indexed by start_pk, sorted by start
    ranges: BTreeMap<i64, i64>,
    /// The starting point (min_pk or 0)
    start_pk: i64,
}

impl RangeTracker {
    /// Create a new range tracker.
    fn new(start_pk: Option<i64>) -> Self {
        Self {
            ranges: BTreeMap::new(),
            start_pk: start_pk.unwrap_or(0),
        }
    }

    /// Add a completed chunk range.
    fn add_range(&mut self, first_pk: Option<i64>, last_pk: Option<i64>) {
        if let Some(end) = last_pk {
            // Use first_pk if available, otherwise assume it starts just after start_pk
            let start = first_pk.unwrap_or(self.start_pk);
            self.ranges.insert(start, end);
        }
    }

    /// Get the safe resume point - the highest PK where all data from start
    /// has been processed contiguously.
    ///
    /// Returns None if no safe point exists (no contiguous range from start).
    fn safe_resume_point(&self) -> Option<i64> {
        if self.ranges.is_empty() {
            return None;
        }

        // Find the contiguous range starting from start_pk
        let mut current_end = self.start_pk;

        for (&range_start, &range_end) in &self.ranges {
            // A range is contiguous only if it does not start after current_end.
            // Any range_start > current_end indicates a gap and breaks contiguity.
            // Using strict checking to catch boundary tracking bugs early.
            if range_start > current_end {
                // Gap found - stop here
                break;
            }
            // Extend the contiguous range
            if range_end > current_end {
                current_end = range_end;
            }
        }

        // Only return a resume point if we've advanced past the start
        if current_end > self.start_pk {
            Some(current_end)
        } else {
            None
        }
    }
}

/// A write job for the parallel writer pool.
struct WriteJob {
    /// Chunk data to write (either SqlValue rows or pre-encoded bytes).
    data: ChunkData,
    /// Partition ID for partition-aware staging tables (None for non-partitioned jobs).
    #[allow(dead_code)] // Reserved for partition-aware write routing
    partition_id: Option<i32>,
}

/// Transfer engine configuration.
#[derive(Debug, Clone)]
pub struct TransferConfig {
    /// Number of rows per chunk.
    pub chunk_size: usize,
    /// Number of read-ahead chunks to buffer.
    pub read_ahead: usize,
    /// Number of parallel readers per table.
    pub parallel_readers: usize,
    /// Number of parallel writers per table.
    pub parallel_writers: usize,
    /// Use PostgreSQL COPY TO BINARY for source reads (4-5x faster).
    /// Only applies when source is PostgreSQL.
    pub use_copy_binary: bool,
    /// Use direct COPY encoding (bypass SqlValue for MSSQL->PG upsert).
    /// Encodes rows directly to PostgreSQL COPY binary format during reading.
    pub use_direct_copy: bool,
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            chunk_size: 50_000,
            read_ahead: 16,
            parallel_readers: 4,
            parallel_writers: 4,
            use_copy_binary: true, // Default to COPY TO BINARY for PostgreSQL sources
            use_direct_copy: true, // Default to direct COPY encoding for MSSQL->PG upsert
        }
    }
}

/// Transfer engine for moving data between databases.
pub struct TransferEngine {
    source: SourcePoolImpl,
    target: TargetPoolImpl,
    config: TransferConfig,
    rows_transferred: AtomicI64,
    /// Optional shared counter for real-time progress reporting.
    progress_counter: Option<Arc<AtomicI64>>,
}

impl TransferEngine {
    /// Create a new transfer engine.
    pub fn new(source: SourcePoolImpl, target: TargetPoolImpl, config: TransferConfig) -> Self {
        Self {
            source,
            target,
            config,
            rows_transferred: AtomicI64::new(0),
            progress_counter: None,
        }
    }

    /// Set a shared progress counter for real-time row tracking.
    pub fn with_progress_counter(mut self, counter: Arc<AtomicI64>) -> Self {
        self.progress_counter = Some(counter);
        self
    }

    /// Get the total rows transferred so far.
    pub fn rows_transferred(&self) -> i64 {
        self.rows_transferred.load(Ordering::Relaxed)
    }

    /// Execute a transfer job using parallel read-ahead/write-ahead pipeline.
    pub async fn execute(&self, job: TransferJob) -> Result<TransferStats> {
        let table_name = job.table.full_name();
        info!(
            "Starting transfer for {} (mode: {:?}, readers: {}, writers: {})",
            table_name, job.target_mode, self.config.parallel_readers, self.config.parallel_writers,
        );

        let start = Instant::now();
        let mut stats = TransferStats::default();

        // Get column names and pre-compute column types for O(1) lookup
        let columns: Vec<String> = job.table.columns.iter().map(|c| c.name.clone()).collect();
        let col_types: Vec<String> = job
            .table
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect();
        let pk_cols: Vec<String> = job.table.primary_key.clone();

        // Determine if we can use parallel keyset pagination
        let use_keyset = job.table.has_single_pk() && is_pk_sortable(&job.table);
        let num_readers = if use_keyset {
            self.config.parallel_readers
        } else {
            1
        };

        // Check if we can use COPY TO BINARY (PostgreSQL source only)
        let use_copy_binary = self.config.use_copy_binary && self.source.supports_copy_binary();

        // Check if PK column is an integer type (required for direct copy PK tracking)
        let pk_is_integer = if !pk_cols.is_empty() {
            let pk_col = &pk_cols[0];
            if let Some(idx) = columns.iter().position(|c| c == pk_col) {
                is_integer_type(&col_types[idx])
            } else {
                false
            }
        } else {
            false
        };

        // Check if we can use direct COPY encoding (MSSQL source + PG target upsert)
        // Direct copy requires integer PK for proper resume tracking
        let use_direct_copy = self.config.use_direct_copy
            && self.source.supports_direct_copy()
            && job.target_mode == TargetMode::Upsert
            && use_keyset
            && pk_is_integer;

        if use_direct_copy {
            info!(
                "{}: using direct COPY encoding (bypasses SqlValue)",
                table_name
            );
        } else if self.config.use_direct_copy
            && self.source.supports_direct_copy()
            && job.target_mode == TargetMode::Upsert
            && use_keyset
            && !pk_is_integer
        {
            debug!(
                "{}: direct copy disabled - PK type is not integer (required for resume tracking)",
                table_name
            );
        }

        if !use_keyset && !use_copy_binary && !use_direct_copy {
            warn!(
                "{}: using OFFSET pagination (slower, single reader)",
                table_name
            );
        }

        // Create channel for read-ahead pipeline
        let (read_tx, read_rx) = mpsc::channel::<RowChunk>(self.config.read_ahead);

        // Create channel for write jobs (multiple writers consume from this)
        let (write_tx, write_rx) =
            async_channel::bounded::<WriteJob>(self.config.parallel_writers * 2);

        // Spawn parallel readers
        let source = self.source.clone();
        let job_clone = job.clone();
        let chunk_size = self.config.chunk_size;
        let columns_clone = columns.clone();
        let col_types_clone = col_types.clone();

        let reader_handle = tokio::spawn(async move {
            // Use direct COPY encoding for MSSQL->PG upsert (highest performance)
            if use_direct_copy {
                read_table_chunks_direct(
                    source,
                    job_clone,
                    columns_clone,
                    col_types_clone,
                    chunk_size,
                    num_readers,
                    read_tx,
                )
                .await
            // Use COPY TO BINARY for PostgreSQL sources (4-5x faster)
            } else if use_copy_binary {
                read_table_chunks_copy_binary(
                    source,
                    job_clone,
                    columns_clone,
                    col_types_clone,
                    chunk_size,
                    read_tx,
                )
                .await
            } else if use_keyset && num_readers > 1 {
                read_table_chunks_parallel(
                    source,
                    job_clone,
                    columns_clone,
                    col_types_clone,
                    chunk_size,
                    num_readers,
                    read_tx,
                )
                .await
            } else {
                read_table_chunks(
                    source,
                    job_clone,
                    columns_clone,
                    col_types_clone,
                    chunk_size,
                    read_tx,
                )
                .await
            }
        });

        // Spawn parallel writers
        let num_writers = self.config.parallel_writers;
        let mut writer_handles = Vec::with_capacity(num_writers);

        for writer_id in 0..num_writers {
            let write_rx = write_rx.clone();
            let target = self.target.clone();
            let schema = job.target_schema.clone();
            let table_name_clone = job.table.name.clone();
            let columns_clone = columns.clone();
            let pk_cols_clone = pk_cols.clone();
            let target_mode = job.target_mode;
            let partition_id = job.partition_id;

            let handle = tokio::spawn(async move {
                let mut local_write_time = Duration::ZERO;
                let mut local_rows = 0i64;

                // For Upsert mode, create a dedicated writer that holds the connection
                // and reuses the staging table to prevent catalog churn.
                let mut upsert_writer: Option<Box<dyn crate::target::UpsertWriter>> = if target_mode
                    == TargetMode::Upsert
                {
                    Some(
                        target
                            .get_upsert_writer(&schema, &table_name_clone, writer_id, partition_id)
                            .await?,
                    )
                } else {
                    None
                };

                while let Ok(write_job) = write_rx.recv().await {
                    let write_start = Instant::now();
                    let row_count = write_job.data.len() as i64;

                    let result = match target_mode {
                        TargetMode::Upsert => {
                            if let Some(writer) = &mut upsert_writer {
                                match write_job.data {
                                    ChunkData::Direct { data, row_count } => {
                                        // Use direct copy path (bypasses SqlValue encoding)
                                        writer
                                            .upsert_chunk_direct(
                                                &columns_clone,
                                                &pk_cols_clone,
                                                data,
                                                row_count,
                                            )
                                            .await
                                    }
                                    ChunkData::Rows(rows) => {
                                        // Standard path with SqlValue encoding
                                        writer
                                            .upsert_chunk(&columns_clone, &pk_cols_clone, rows)
                                            .await
                                    }
                                }
                            } else {
                                Err(MigrateError::transfer(
                                    table_name_clone.clone(),
                                    "Upsert writer not initialized".to_string(),
                                ))
                            }
                        }
                        _ => {
                            // Non-upsert modes only support Rows
                            match write_job.data {
                                ChunkData::Rows(rows) => {
                                    target
                                        .write_chunk(
                                            &schema,
                                            &table_name_clone,
                                            &columns_clone,
                                            rows,
                                        )
                                        .await
                                }
                                ChunkData::Direct { .. } => Err(MigrateError::transfer(
                                    table_name_clone.clone(),
                                    "Direct copy not supported for non-upsert modes".to_string(),
                                )),
                            }
                        }
                    };

                    if let Err(e) = result {
                        return Err(MigrateError::transfer(
                            table_name_clone.clone(),
                            format!("Writer {} failed: {}", writer_id, e),
                        ));
                    }

                    local_write_time += write_start.elapsed();
                    local_rows += row_count;

                    debug!(
                        "Writer {}: wrote {} rows (local total: {})",
                        writer_id, row_count, local_rows
                    );
                }

                Ok::<(Duration, i64), MigrateError>((local_write_time, local_rows))
            });

            writer_handles.push(handle);
        }

        // Drop our copy of write_rx so channel closes when all writers are done
        drop(write_rx);

        // Dispatcher: read from read_rx, dispatch to write_tx.
        //
        // Architecture note: The dispatcher adds a hop between readers and writers,
        // but this is intentional. It centralizes:
        // 1. Range tracking for safe resume points (RangeTracker)
        // 2. Query timing aggregation
        // 3. Backpressure management between read/write stages
        //
        // Passing write_tx directly to readers would require shared-state range
        // tracking (mutex/atomic), adding complexity for marginal latency gains.
        // The dispatcher overhead is minimal since it just forwards chunks.
        let mut total_query_time = Duration::ZERO;

        // Use RangeTracker for safe resume points with parallel readers.
        // This prevents data loss when chunks arrive out of order.
        let start_pk = job.resume_from_pk.or(job.min_pk);
        let mut range_tracker = RangeTracker::new(start_pk);

        // Use a separate receiver for the read channel
        let mut read_rx = read_rx;

        while let Some(chunk) = read_rx.recv().await {
            total_query_time += chunk.read_time;
            // Track completed ranges for safe resume point calculation.
            // Only contiguous ranges from start are considered safe.
            range_tracker.add_range(chunk.first_pk, chunk.last_pk);

            // Count all rows read for progress tracking
            let chunk_row_count = chunk.data.len() as i64;
            if let Some(ref counter) = self.progress_counter {
                counter.fetch_add(chunk_row_count, Ordering::Relaxed);
            }

            // Send rows to writers
            if !chunk.data.is_empty() {
                let write_job = WriteJob {
                    data: chunk.data,
                    partition_id: job.partition_id,
                };

                if write_tx.send(write_job).await.is_err() {
                    // Writers have all failed
                    break;
                }
            }
        }

        // Get the safe resume point (only contiguous ranges from start)
        let last_pk = range_tracker.safe_resume_point();

        // Close write channel to signal writers to finish
        drop(write_tx);

        // Wait for reader to complete
        match reader_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(e) => {
                return Err(MigrateError::Transfer {
                    table: table_name.clone(),
                    message: format!("Reader task failed: {}", e),
                })
            }
        }

        // Wait for all writers concurrently and aggregate stats.
        // Uses try_join_all for fail-fast behavior: if any writer fails,
        // we get the error immediately rather than waiting for earlier writers.
        let mut total_write_time = Duration::ZERO;
        let mut total_rows = 0i64;

        let writer_futures = writer_handles.into_iter().map(|handle| {
            let table_name = table_name.clone();
            async move {
                match handle.await {
                    Ok(Ok(result)) => Ok(result),
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(MigrateError::Transfer {
                        table: table_name,
                        message: format!("Writer task panicked: {}", e),
                    }),
                }
            }
        });

        let results = try_join_all(writer_futures).await?;

        for (write_time, rows) in results {
            if write_time > total_write_time {
                total_write_time = write_time;
            }
            total_rows += rows;
        }

        // Update global counter
        self.rows_transferred
            .fetch_add(total_rows, Ordering::Relaxed);

        stats.rows = total_rows;
        stats.query_time = total_query_time;
        stats.write_time = total_write_time;
        stats.last_pk = last_pk;

        // Calculate scan time as remainder
        let total_elapsed = start.elapsed();
        stats.scan_time = total_elapsed
            .saturating_sub(stats.query_time)
            .saturating_sub(stats.write_time);
        stats.completed = true;

        let rows_per_sec = if total_elapsed.as_secs_f64() > 0.0 {
            (total_rows as f64 / total_elapsed.as_secs_f64()) as i64
        } else {
            0
        };

        info!(
            "{}: transferred {} rows in {:?} ({} rows/sec, read: {:?}, write: {:?})",
            table_name, stats.rows, total_elapsed, rows_per_sec, stats.query_time, stats.write_time
        );

        Ok(stats)
    }
}

/// Split a PK range into N sub-ranges for parallel reading.
/// Each reader gets an explicit lower bound derived from min_pk to prevent data duplication.
fn split_pk_range(min_pk: i64, max_pk: i64, num_readers: usize) -> Vec<(Option<i64>, i64)> {
    if num_readers <= 1 || max_pk <= min_pk {
        // Single reader or invalid range: use explicit min_pk bound
        return vec![(Some(min_pk), max_pk)];
    }

    let total_range = max_pk - min_pk;
    let range_size = total_range / num_readers as i64;

    if range_size < 1 {
        // Range too small to split: use a single bounded range
        return vec![(Some(min_pk), max_pk)];
    }

    (0..num_readers)
        .map(|i| {
            // Each reader has an explicit lower bound derived from min_pk
            let range_min = Some(min_pk + (i as i64 * range_size));
            let range_max = if i == num_readers - 1 {
                max_pk // Last reader goes to the end
            } else {
                min_pk + ((i + 1) as i64 * range_size)
            };
            (range_min, range_max)
        })
        .collect()
}

/// Read table data using parallel readers with PK range splitting.
async fn read_table_chunks_parallel(
    source: SourcePoolImpl,
    job: TransferJob,
    columns: Vec<String>,
    col_types: Vec<String>,
    chunk_size: usize,
    num_readers: usize,
    tx: mpsc::Sender<RowChunk>,
) -> Result<()> {
    let table = &job.table;
    let table_name = table.full_name();

    // Get min/max PK for range splitting
    // If job already has partition boundaries, use them; otherwise query the table
    let min_pk = job.min_pk.unwrap_or(0);
    let max_pk = match job.max_pk {
        Some(pk) => pk,
        None => {
            // Query max PK if not provided
            source
                .get_max_pk(&table.schema, &table.name, &table.primary_key[0])
                .await?
        }
    };

    // Check if this is already a partitioned job (has min_pk set)
    // If so, we should NOT do nested parallel reading - just use single reader
    // The orchestrator's partitioning provides enough parallelism
    let is_partitioned = job.min_pk.is_some() || job.partition_id.is_some();
    let actual_num_readers = if is_partitioned { 1 } else { num_readers };

    let ranges = split_pk_range(min_pk, max_pk, actual_num_readers);
    let actual_readers = ranges.len();

    info!(
        "{}: starting {} parallel readers for PK range {} to {}{}",
        table_name,
        actual_readers,
        min_pk,
        max_pk,
        if is_partitioned {
            " (partitioned job)"
        } else {
            ""
        }
    );

    let mut reader_handles = Vec::with_capacity(actual_readers);

    for (reader_id, (range_min, range_max)) in ranges.into_iter().enumerate() {
        let source = source.clone();
        let table = job.table.clone();
        let columns = columns.clone();
        let col_types = col_types.clone();
        let tx = tx.clone();

        // For partitioned jobs or resume, use the job's min_pk as starting point
        // Track whether we're resuming (first reader with resume_from_pk) vs fresh start
        let (start_pk, is_resume) = if reader_id == 0 {
            if job.resume_from_pk.is_some() {
                (job.resume_from_pk, true) // Resuming: use > to skip already-read row
            } else {
                (job.min_pk, false) // Fresh start: use >= to include boundary
            }
        } else {
            // Non-first readers: the boundary pk was already read by the previous reader,
            // so use > (exclusive) to avoid reading the same row twice
            (range_min, true)
        };

        let handle = tokio::spawn(async move {
            read_chunk_range(
                source, table, columns, col_types, start_pk, range_max, chunk_size, reader_id,
                is_resume, tx,
            )
            .await
        });

        reader_handles.push(handle);
    }

    // Wait for all readers to complete
    let mut total_rows = 0i64;
    for handle in reader_handles {
        match handle.await {
            Ok(Ok(rows)) => total_rows += rows,
            Ok(Err(e)) => return Err(e),
            Err(e) => {
                return Err(MigrateError::Transfer {
                    table: table_name.clone(),
                    message: format!("Reader task panicked: {}", e),
                })
            }
        }
    }

    info!(
        "{}: all readers finished, total {} rows",
        table_name, total_rows
    );
    Ok(())
}

/// Read table data using direct COPY encoding (bypasses SqlValue for MSSQL->PG upsert).
///
/// This is the highest-performance path for MSSQL->PostgreSQL upsert:
/// - Encodes rows directly to PostgreSQL COPY binary format during reading
/// - Eliminates SqlValue intermediate type and its allocations
/// - Reduces type dispatch from 2x per value to 1x per value
async fn read_table_chunks_direct(
    source: SourcePoolImpl,
    job: TransferJob,
    columns: Vec<String>,
    col_types: Vec<String>,
    chunk_size: usize,
    num_readers: usize,
    tx: mpsc::Sender<RowChunk>,
) -> Result<()> {
    let table = &job.table;
    let table_name = table.full_name();

    // Get min/max PK for range splitting
    let min_pk = job.min_pk.unwrap_or(0);
    let max_pk = match job.max_pk {
        Some(pk) => pk,
        None => {
            source
                .get_max_pk(&table.schema, &table.name, &table.primary_key[0])
                .await?
        }
    };

    // Check if this is already a partitioned job
    let is_partitioned = job.min_pk.is_some() || job.partition_id.is_some();
    let actual_num_readers = if is_partitioned { 1 } else { num_readers };

    let ranges = split_pk_range(min_pk, max_pk, actual_num_readers);
    let actual_readers = ranges.len();

    info!(
        "{}: starting {} direct COPY readers for PK range {} to {}{}",
        table_name,
        actual_readers,
        min_pk,
        max_pk,
        if is_partitioned {
            " (partitioned job)"
        } else {
            ""
        }
    );

    let mut reader_handles = Vec::with_capacity(actual_readers);

    for (reader_id, (range_min, range_max)) in ranges.into_iter().enumerate() {
        let source = source.clone();
        let table = job.table.clone();
        let columns = columns.clone();
        let col_types = col_types.clone();
        let tx = tx.clone();

        let (start_pk, is_resume) = if reader_id == 0 {
            if job.resume_from_pk.is_some() {
                (job.resume_from_pk, true)
            } else {
                (job.min_pk, false)
            }
        } else {
            (range_min, true)
        };

        let handle = tokio::spawn(async move {
            read_chunk_range_direct(
                source, table, columns, col_types, start_pk, range_max, chunk_size, reader_id,
                is_resume, tx,
            )
            .await
        });

        reader_handles.push(handle);
    }

    // Wait for all readers to complete
    let mut total_rows = 0i64;
    for handle in reader_handles {
        match handle.await {
            Ok(Ok(rows)) => total_rows += rows,
            Ok(Err(e)) => return Err(e),
            Err(e) => {
                return Err(MigrateError::Transfer {
                    table: table_name.clone(),
                    message: format!("Direct reader task panicked: {}", e),
                })
            }
        }
    }

    info!(
        "{}: all direct readers finished, total {} rows",
        table_name, total_rows
    );
    Ok(())
}

/// Read chunks for a specific PK range using direct COPY encoding.
async fn read_chunk_range_direct(
    source: SourcePoolImpl,
    table: Table,
    columns: Vec<String>,
    col_types: Vec<String>,
    start_pk: Option<i64>,
    end_pk: i64,
    chunk_size: usize,
    reader_id: usize,
    is_resume: bool,
    tx: mpsc::Sender<RowChunk>,
) -> Result<i64> {
    let table_name = table.full_name();

    let mut last_pk = start_pk;
    let mut total_rows = 0i64;
    let mut chunk_num = 0;
    let mut is_first_chunk = !is_resume;

    loop {
        let read_start = Instant::now();

        // Try direct COPY encoding
        let direct_result = read_chunk_keyset_direct(
            &source,
            &table,
            &columns,
            &col_types,
            last_pk,
            Some(end_pk),
            chunk_size,
            is_first_chunk,
        )
        .await?;

        // Direct copy should always work for MSSQL source
        let (data, first_pk, new_last_pk, row_count) = match direct_result {
            Some(result) => result,
            None => {
                return Err(MigrateError::Transfer {
                    table: table_name.clone(),
                    message: "Direct copy not supported for this source".to_string(),
                });
            }
        };

        is_first_chunk = false;
        let read_time = read_start.elapsed();

        if row_count == 0 {
            break;
        }

        total_rows += row_count as i64;
        chunk_num += 1;

        // Log progress every 10 chunks at info level
        if chunk_num % 10 == 0 {
            info!(
                "{} reader {}: direct read {} rows ({} chunks) in {:?}",
                table_name, reader_id, total_rows, chunk_num, read_time
            );
        } else {
            debug!(
                "{} reader {}: direct read chunk {} with {} rows in {:?}",
                table_name, reader_id, chunk_num, row_count, read_time
            );
        }

        let chunk = RowChunk {
            data: ChunkData::Direct { data, row_count },
            first_pk,
            last_pk: new_last_pk,
            read_time,
        };

        if tx.send(chunk).await.is_err() {
            return Err(MigrateError::Transfer {
                table: table_name.clone(),
                message: format!("Reader {}: write channel closed", reader_id),
            });
        }

        last_pk = new_last_pk;

        // Check if we've reached end of range
        if let Some(lpk) = last_pk {
            if lpk >= end_pk {
                break;
            }
        }

        // If we got fewer rows than chunk_size, we're done
        if row_count < chunk_size {
            break;
        }
    }

    debug!(
        "{} reader {}: finished direct read, {} rows in {} chunks",
        table_name, reader_id, total_rows, chunk_num
    );

    Ok(total_rows)
}

/// Read chunks for a specific PK range.
///
/// `is_resume` indicates whether `start_pk` came from a resume checkpoint (true) or
/// is a fresh range boundary (false). When resuming, the first query uses `>` to skip
/// the already-processed row. For fresh starts, the first query uses `>=` to include it.
async fn read_chunk_range(
    source: SourcePoolImpl,
    table: Table,
    columns: Vec<String>,
    col_types: Vec<String>,
    start_pk: Option<i64>,
    end_pk: i64,
    chunk_size: usize,
    reader_id: usize,
    is_resume: bool,
    tx: mpsc::Sender<RowChunk>,
) -> Result<i64> {
    let table_name = table.full_name();

    let mut last_pk = start_pk;
    let mut total_rows = 0i64;
    let mut chunk_num = 0;
    // First chunk uses >= for fresh ranges, > for resume
    let mut is_first_chunk = !is_resume;

    loop {
        let read_start = Instant::now();

        let (rows, new_last_pk) = read_chunk_keyset_fast(
            &source,
            &table,
            &columns,
            &col_types,
            last_pk,
            Some(end_pk),
            chunk_size,
            is_first_chunk,
        )
        .await?;

        // After first chunk, always use > for subsequent chunks
        is_first_chunk = false;

        let read_time = read_start.elapsed();
        let row_count = rows.len();

        if rows.is_empty() {
            break;
        }

        total_rows += row_count as i64;
        chunk_num += 1;

        debug!(
            "{} reader {}: read chunk {} with {} rows in {:?}",
            table_name, reader_id, chunk_num, row_count, read_time
        );

        // first_pk is the starting boundary for this chunk (previous last_pk or start)
        // This is needed for safe range tracking during parallel reads
        let chunk = RowChunk {
            data: ChunkData::Rows(rows),
            first_pk: last_pk,
            last_pk: new_last_pk,
            read_time,
        };

        if tx.send(chunk).await.is_err() {
            return Err(MigrateError::Transfer {
                table: table_name.clone(),
                message: format!("Reader {}: write channel closed", reader_id),
            });
        }

        last_pk = new_last_pk;

        // Check if we've reached end of range
        if let Some(lpk) = last_pk {
            if lpk >= end_pk {
                break;
            }
        }

        // If we got fewer rows than chunk_size, we're done
        if row_count < chunk_size {
            break;
        }
    }

    debug!(
        "{} reader {}: finished, read {} rows in {} chunks",
        table_name, reader_id, total_rows, chunk_num
    );

    Ok(total_rows)
}

/// Read table data in chunks using single reader (for OFFSET pagination or when parallelism disabled).
async fn read_table_chunks(
    source: SourcePoolImpl,
    job: TransferJob,
    columns: Vec<String>,
    col_types: Vec<String>,
    chunk_size: usize,
    tx: mpsc::Sender<RowChunk>,
) -> Result<()> {
    let table = &job.table;
    let table_name = table.full_name();

    let use_keyset = table.has_single_pk() && is_pk_sortable(table);

    // Determine start point and whether we're resuming
    let is_resume = job.resume_from_pk.is_some();
    let mut last_pk: Option<i64> = job.resume_from_pk.or(job.min_pk);
    let max_pk = job.max_pk;

    let mut total_rows = 0i64;
    let mut chunk_num = 0;
    // First chunk uses >= for fresh ranges, > for resume
    let mut is_first_chunk = !is_resume;

    loop {
        let read_start = Instant::now();

        let (rows, new_last_pk) = if use_keyset {
            read_chunk_keyset_fast(
                &source,
                table,
                &columns,
                &col_types,
                last_pk,
                max_pk,
                chunk_size,
                is_first_chunk,
            )
            .await?
        } else {
            read_chunk_offset(
                &source,
                table,
                &columns,
                &col_types,
                total_rows as usize,
                chunk_size,
            )
            .await?
        };

        // After first chunk, always use > for subsequent chunks
        is_first_chunk = false;

        let read_time = read_start.elapsed();
        let row_count = rows.len();

        if rows.is_empty() {
            debug!("{}: no more rows to read", table_name);
            break;
        }

        total_rows += row_count as i64;
        chunk_num += 1;

        debug!(
            "{}: read chunk {} with {} rows in {:?}",
            table_name, chunk_num, row_count, read_time
        );

        // first_pk is the starting boundary for this chunk (previous last_pk or start)
        // This is needed for safe range tracking during parallel reads
        let chunk = RowChunk {
            data: ChunkData::Rows(rows),
            first_pk: last_pk,
            last_pk: new_last_pk,
            read_time,
        };

        if tx.send(chunk).await.is_err() {
            return Err(MigrateError::Transfer {
                table: table_name.clone(),
                message: "Write channel closed".into(),
            });
        }

        last_pk = new_last_pk;

        // Check if we've reached max_pk
        if let (Some(lpk), Some(mpk)) = (last_pk, max_pk) {
            if lpk >= mpk {
                break;
            }
        }

        // Safety check for non-keyset pagination
        if !use_keyset && row_count < chunk_size {
            break;
        }
    }

    info!(
        "{}: finished reading {} rows in {} chunks",
        table_name, total_rows, chunk_num
    );
    Ok(())
}

/// Read a chunk using keyset pagination with pre-computed column types (O(1) lookup).
///
/// When `first_chunk` is true and `last_pk` is set, uses `>=` (inclusive) for the lower bound.
/// This is needed because for the first chunk of a range, `last_pk` represents the starting
/// boundary that should be included, not a previously-read row to skip.
/// For subsequent chunks, `last_pk` is the last PK we read, so we use `>` (exclusive).
async fn read_chunk_keyset_fast(
    source: &SourcePoolImpl,
    table: &Table,
    columns: &[String],
    col_types: &[String],
    last_pk: Option<i64>,
    max_pk: Option<i64>,
    chunk_size: usize,
    first_chunk: bool,
) -> Result<(Vec<Vec<SqlValue>>, Option<i64>)> {
    let pk_col = &table.primary_key[0];
    let is_postgres = source.db_type() == "postgres";

    // Build column list with appropriate quoting
    let col_list = columns
        .iter()
        .map(|c| {
            if is_postgres {
                quote_pg_ident(c)
            } else {
                quote_mssql_ident(c)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    let pk_quoted = if is_postgres {
        quote_pg_ident(pk_col)
    } else {
        quote_mssql_ident(pk_col)
    };
    let table_ref = if is_postgres {
        qualify_pg_table(&table.schema, &table.name)
    } else {
        qualify_mssql_table(&table.schema, &table.name)
    };

    // Build query with database-specific syntax
    let mut query = if is_postgres {
        format!("SELECT {} FROM {}", col_list, table_ref)
    } else {
        format!(
            "SELECT TOP {} {} FROM {} WITH (NOLOCK)",
            chunk_size, col_list, table_ref
        )
    };

    let mut conditions = Vec::new();
    if let Some(pk) = last_pk {
        // For first chunk of a range, use >= to include the starting boundary
        // For subsequent chunks (or resume), use > to skip the last-read row
        let op = if first_chunk { ">=" } else { ">" };
        conditions.push(format!("{} {} {}", pk_quoted, op, pk));
    }
    if let Some(pk) = max_pk {
        conditions.push(format!("{} <= {}", pk_quoted, pk));
    }

    if !conditions.is_empty() {
        query.push_str(" WHERE ");
        query.push_str(&conditions.join(" AND "));
    }

    query.push_str(&format!(" ORDER BY {}", pk_quoted));

    // PostgreSQL uses LIMIT instead of TOP
    if is_postgres {
        query.push_str(&format!(" LIMIT {}", chunk_size));
    }

    let rows = source.query_rows_fast(&query, columns, col_types).await?;

    // Get the last PK value from the result
    let pk_idx = columns.iter().position(|c| c == pk_col);
    let new_last_pk = if !rows.is_empty() {
        pk_idx.and_then(|idx| {
            rows.last().and_then(|row| match &row[idx] {
                SqlValue::I64(v) => Some(*v),
                SqlValue::I32(v) => Some(*v as i64),
                SqlValue::I16(v) => Some(*v as i64),
                _ => None,
            })
        })
    } else {
        None
    };

    Ok((rows, new_last_pk))
}

/// Read a chunk using direct COPY encoding (bypasses SqlValue for MSSQL->PG).
///
/// Returns (encoded_bytes, first_pk, last_pk, row_count) or None if not supported.
async fn read_chunk_keyset_direct(
    source: &SourcePoolImpl,
    table: &Table,
    columns: &[String],
    col_types: &[String],
    last_pk: Option<i64>,
    max_pk: Option<i64>,
    chunk_size: usize,
    first_chunk: bool,
) -> Result<Option<(bytes::Bytes, Option<i64>, Option<i64>, usize)>> {
    // Only MSSQL supports direct copy
    if !source.supports_direct_copy() {
        return Ok(None);
    }

    let pk_col = &table.primary_key[0];

    // Build column list with MSSQL quoting
    let col_list = columns
        .iter()
        .map(|c| quote_mssql_ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    let pk_quoted = quote_mssql_ident(pk_col);
    let table_ref = qualify_mssql_table(&table.schema, &table.name);

    // Build query with MSSQL syntax
    let mut query = format!(
        "SELECT TOP {} {} FROM {} WITH (NOLOCK)",
        chunk_size, col_list, table_ref
    );

    let mut conditions = Vec::new();
    if let Some(pk) = last_pk {
        let op = if first_chunk { ">=" } else { ">" };
        conditions.push(format!("{} {} {}", pk_quoted, op, pk));
    }
    if let Some(pk) = max_pk {
        conditions.push(format!("{} <= {}", pk_quoted, pk));
    }

    if !conditions.is_empty() {
        query.push_str(" WHERE ");
        query.push_str(&conditions.join(" AND "));
    }

    query.push_str(&format!(" ORDER BY {}", pk_quoted));

    // Find PK column index for tracking
    let pk_idx = columns.iter().position(|c| c == pk_col);

    // Query and encode directly to COPY binary format
    source
        .query_rows_direct_copy(&query, col_types, pk_idx)
        .await
}

/// Read a chunk using OFFSET pagination (for composite PKs).
async fn read_chunk_offset(
    source: &SourcePoolImpl,
    table: &Table,
    columns: &[String],
    col_types: &[String],
    offset: usize,
    chunk_size: usize,
) -> Result<(Vec<Vec<SqlValue>>, Option<i64>)> {
    let is_postgres = source.db_type() == "postgres";

    let col_list = columns
        .iter()
        .map(|c| {
            if is_postgres {
                quote_pg_ident(c)
            } else {
                quote_mssql_ident(c)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    let table_ref = if is_postgres {
        qualify_pg_table(&table.schema, &table.name)
    } else {
        qualify_mssql_table(&table.schema, &table.name)
    };

    // Create ORDER BY from primary key columns
    let order_by = if table.primary_key.is_empty() {
        if !columns.is_empty() {
            if is_postgres {
                quote_pg_ident(&columns[0])
            } else {
                quote_mssql_ident(&columns[0])
            }
        } else {
            return Err(MigrateError::Transfer {
                table: table.full_name(),
                message: "Table has no columns".into(),
            });
        }
    } else {
        table
            .primary_key
            .iter()
            .map(|c| {
                if is_postgres {
                    quote_pg_ident(c)
                } else {
                    quote_mssql_ident(c)
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    };

    let query = if is_postgres {
        format!(
            "SELECT {} FROM {} ORDER BY {} LIMIT {} OFFSET {}",
            col_list, table_ref, order_by, chunk_size, offset
        )
    } else {
        format!(
            "SELECT {} FROM {} WITH (NOLOCK) ORDER BY {} OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
            col_list, table_ref, order_by, offset, chunk_size
        )
    };

    let rows = source.query_rows_fast(&query, columns, col_types).await?;
    Ok((rows, None))
}

/// Read table data using PostgreSQL COPY TO BINARY streaming.
///
/// This is significantly faster than SELECT-based reads (4-5x) because:
/// - Binary format has lower serialization overhead than text
/// - COPY bypasses the query executor's row-by-row processing
/// - Streaming reduces memory pressure vs. loading all rows at once
///
/// Returns when the entire table/range has been read.
async fn read_table_chunks_copy_binary(
    source: SourcePoolImpl,
    job: TransferJob,
    columns: Vec<String>,
    col_types: Vec<String>,
    chunk_size: usize,
    tx: mpsc::Sender<RowChunk>,
) -> Result<()> {
    let table = &job.table;
    let table_name = table.full_name();
    let pk_col_name: Option<String> = if table.has_single_pk() {
        Some(table.primary_key[0].clone())
    } else {
        None
    };

    info!(
        "{}: using COPY TO BINARY streaming (4-5x faster)",
        table_name
    );

    let start_time = Instant::now();

    // Create channel to receive batches from copy_rows_binary
    let (copy_tx, mut copy_rx) = mpsc::channel::<Vec<Vec<SqlValue>>>(16);

    // Spawn the COPY streaming task
    let source_clone = source.clone();
    let schema = table.schema.clone();
    let table_name_clone = table.name.clone();
    let columns_clone = columns.clone();
    let col_types_clone = col_types.clone();
    let min_pk = job.min_pk.or(job.resume_from_pk);
    let max_pk = job.max_pk;
    let pk_col_clone = pk_col_name.clone();

    let copy_handle = tokio::spawn(async move {
        source_clone
            .copy_rows_binary(
                &schema,
                &table_name_clone,
                &columns_clone,
                &col_types_clone,
                pk_col_clone.as_deref(),
                min_pk,
                max_pk,
                copy_tx,
                chunk_size,
            )
            .await
    });

    // Receive batches and forward as RowChunks
    let mut total_rows = 0i64;
    let mut chunk_num = 0;
    let pk_idx = pk_col_name
        .as_ref()
        .and_then(|pk| columns.iter().position(|c| c == pk));

    while let Some(batch) = copy_rx.recv().await {
        if batch.is_empty() {
            continue;
        }

        let row_count = batch.len();
        total_rows += row_count as i64;
        chunk_num += 1;

        // Extract first and last PK values for range tracking
        let first_pk = pk_idx.and_then(|idx| {
            batch.first().and_then(|row| match &row[idx] {
                SqlValue::I64(v) => Some(*v),
                SqlValue::I32(v) => Some(*v as i64),
                SqlValue::I16(v) => Some(*v as i64),
                _ => None,
            })
        });

        let last_pk = pk_idx.and_then(|idx| {
            batch.last().and_then(|row| match &row[idx] {
                SqlValue::I64(v) => Some(*v),
                SqlValue::I32(v) => Some(*v as i64),
                SqlValue::I16(v) => Some(*v as i64),
                _ => None,
            })
        });

        let chunk = RowChunk {
            data: ChunkData::Rows(batch),
            first_pk,
            last_pk,
            read_time: Duration::ZERO, // COPY doesn't provide per-chunk timing
        };

        if tx.send(chunk).await.is_err() {
            return Err(MigrateError::Transfer {
                table: table_name.clone(),
                message: "Write channel closed during COPY streaming".to_string(),
            });
        }

        debug!(
            "{}: sent COPY chunk {} with {} rows",
            table_name, chunk_num, row_count
        );
    }

    // Wait for COPY to complete and check for errors
    let copy_result = copy_handle.await.map_err(|e| MigrateError::Transfer {
        table: table_name.clone(),
        message: format!("COPY task panicked: {}", e),
    })?;

    copy_result?;

    let elapsed = start_time.elapsed();
    let rows_per_sec = if elapsed.as_secs_f64() > 0.0 {
        total_rows as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    info!(
        "{}: COPY TO BINARY completed - {} rows in {:?} ({:.0} rows/s)",
        table_name, total_rows, elapsed, rows_per_sec
    );

    Ok(())
}

/// Quote a PostgreSQL identifier.
///
/// Escapes double quotes by doubling them and wraps in double quotes.
/// Includes defensive validation for suspicious patterns.
fn quote_pg_ident(name: &str) -> String {
    // Defensive validation: reject suspicious patterns
    debug_assert!(
        !name.contains('\0'),
        "Identifier contains null byte: {:?}",
        name
    );
    debug_assert!(
        name.len() <= 128,
        "Identifier exceeds maximum length (128): {}",
        name.len()
    );

    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Qualify a PostgreSQL table name with schema and proper quoting.
fn qualify_pg_table(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_pg_ident(schema), quote_pg_ident(table))
}

/// Check if the primary key is a sortable integer type.
/// Supports both MSSQL and PostgreSQL type names.
fn is_pk_sortable(table: &Table) -> bool {
    if table.pk_columns.is_empty() {
        return false;
    }

    let pk_type = table.pk_columns[0].data_type.to_lowercase();
    matches!(
        pk_type.as_str(),
        // MSSQL types
        "int" | "bigint" | "smallint" | "tinyint" |
        // PostgreSQL types
        "integer" | "int4" | "int8" | "int2" |
        // PostgreSQL aliases
        "serial" | "bigserial" | "smallserial"
    )
}

/// Check if a column type is an integer type suitable for PK tracking in direct copy.
/// Direct copy requires integer PKs because the encoder can only track i64 PK values.
fn is_integer_type(col_type: &str) -> bool {
    let t = col_type.to_lowercase();
    matches!(
        t.as_str(),
        // MSSQL types
        "int" | "bigint" | "smallint" | "tinyint" |
        // PostgreSQL types
        "integer" | "int4" | "int8" | "int2" |
        // PostgreSQL aliases
        "serial" | "bigserial" | "smallserial"
    )
}

/// Quote a SQL Server identifier, escaping closing brackets.
///
/// # SQL Identifier Parameterization
///
/// SQL identifiers (table names, column names, schema names) cannot be passed as
/// parameters in prepared statements - only data values can be parameterized.
/// This is a fundamental limitation of SQL, not a design choice.
///
/// To safely construct dynamic SQL with identifiers, we:
/// 1. Wrap identifiers in brackets `[name]` (SQL Server's quoting mechanism)
/// 2. Escape embedded closing brackets by doubling them: `]` → `]]`
///
/// This prevents SQL injection through identifier names while allowing dynamic
/// table/column selection required for a generic migration tool.
///
/// ## Security Note
///
/// Identifier names come from database schema metadata, which is trusted.
/// However, we add defensive validation to reject suspicious patterns that
/// could indicate tampering or corruption.
fn quote_mssql_ident(name: &str) -> String {
    // Defensive validation: reject suspicious patterns
    // These should never appear in legitimate identifier names
    debug_assert!(
        !name.contains('\0'),
        "Identifier contains null byte: {:?}",
        name
    );
    debug_assert!(
        name.len() <= 128,
        "Identifier exceeds maximum length (128): {}",
        name.len()
    );

    format!("[{}]", name.replace(']', "]]"))
}

/// Qualify a SQL Server table name with schema and proper quoting.
///
/// See [`quote_mssql_ident`] for details on identifier escaping.
fn qualify_mssql_table(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_mssql_ident(schema), quote_mssql_ident(table))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_tracker_empty() {
        let tracker = RangeTracker::new(Some(0));
        assert_eq!(tracker.safe_resume_point(), None);
    }

    #[test]
    fn test_range_tracker_single_range() {
        let mut tracker = RangeTracker::new(Some(0));
        tracker.add_range(Some(0), Some(100));
        assert_eq!(tracker.safe_resume_point(), Some(100));
    }

    #[test]
    fn test_range_tracker_contiguous_ranges() {
        let mut tracker = RangeTracker::new(Some(0));
        tracker.add_range(Some(0), Some(100));
        tracker.add_range(Some(100), Some(200));
        tracker.add_range(Some(200), Some(300));
        assert_eq!(tracker.safe_resume_point(), Some(300));
    }

    #[test]
    fn test_range_tracker_gap_in_ranges() {
        let mut tracker = RangeTracker::new(Some(0));
        // Add range 0-100 and 200-300, leaving a gap at 100-200
        tracker.add_range(Some(0), Some(100));
        tracker.add_range(Some(200), Some(300));
        // Safe point should only be 100 (before the gap)
        assert_eq!(tracker.safe_resume_point(), Some(100));
    }

    #[test]
    fn test_range_tracker_out_of_order() {
        let mut tracker = RangeTracker::new(Some(0));
        // Add ranges out of order (simulating parallel readers)
        tracker.add_range(Some(200), Some(300)); // Reader B completes first
        tracker.add_range(Some(0), Some(100)); // Reader A completes second
                                               // Still gap at 100-200, so safe point is 100
        assert_eq!(tracker.safe_resume_point(), Some(100));

        // Now fill the gap
        tracker.add_range(Some(100), Some(200));
        // All ranges contiguous, safe point is 300
        assert_eq!(tracker.safe_resume_point(), Some(300));
    }

    #[test]
    fn test_range_tracker_prevents_data_loss() {
        // Simulate the exact scenario from issue #20:
        // Reader A: PK 0-2000, Reader B: PK 2000-4000
        // Reader B completes first, then process crashes
        let mut tracker = RangeTracker::new(Some(0));

        // Reader B (2000-4000) completes first
        tracker.add_range(Some(2000), Some(4000));

        // Safe resume point should NOT be 4000 (which would skip 0-2000)
        // It should be None or 0 since we haven't completed 0-2000
        assert_eq!(tracker.safe_resume_point(), None);

        // Now Reader A (0-2000) completes
        tracker.add_range(Some(0), Some(2000));

        // Now safe resume point is 4000 (all data from start is processed)
        assert_eq!(tracker.safe_resume_point(), Some(4000));
    }

    #[test]
    fn test_range_tracker_with_start_pk() {
        // Test with non-zero start (simulating resume from PK 500)
        let mut tracker = RangeTracker::new(Some(500));
        tracker.add_range(Some(500), Some(600));
        assert_eq!(tracker.safe_resume_point(), Some(600));
    }

    #[test]
    fn test_range_tracker_overlapping_ranges() {
        let mut tracker = RangeTracker::new(Some(0));
        // Overlapping ranges should still work correctly
        tracker.add_range(Some(0), Some(150));
        tracker.add_range(Some(100), Some(250));
        tracker.add_range(Some(200), Some(300));
        assert_eq!(tracker.safe_resume_point(), Some(300));
    }

    #[test]
    fn test_range_tracker_none_first_pk() {
        // When first_pk is None, it defaults to start_pk
        let mut tracker = RangeTracker::new(Some(0));
        tracker.add_range(None, Some(100));
        assert_eq!(tracker.safe_resume_point(), Some(100));
    }
}
