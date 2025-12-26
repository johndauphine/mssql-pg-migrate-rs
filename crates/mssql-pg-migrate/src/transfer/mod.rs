//! Data transfer engine with parallel read-ahead/write-ahead pipeline.
//!
//! This module implements a high-performance transfer engine that uses:
//! - Multiple parallel readers (PK range splitting) to maximize source throughput
//! - Multiple parallel writers to maximize target throughput
//! - Read-ahead buffering to overlap read and write operations

use crate::config::TargetMode;
use crate::error::{MigrateError, Result};
use crate::source::{generate_hash_expression, MssqlPool, Table};
use crate::target::{PgPool, SqlValue, TargetPool};
use futures::future::try_join_all;
use rayon::prelude::*;
use std::collections::{BTreeMap, HashMap};
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

    /// Whether to use hash-based change detection for upsert.
    pub use_hash_detection: bool,

    /// Column name for storing row hashes (when hash detection is enabled).
    pub row_hash_column: String,
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

    /// Rows skipped due to hash match (unchanged).
    pub rows_skipped: i64,

    /// Last primary key value processed.
    pub last_pk: Option<i64>,

    /// Whether the job completed successfully.
    pub completed: bool,
}

/// A chunk of rows to be transferred.
#[derive(Debug)]
struct RowChunk {
    /// Row data.
    rows: Vec<Vec<SqlValue>>,
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
    /// Row data to write.
    rows: Vec<Vec<SqlValue>>,
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
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            chunk_size: 50_000,
            read_ahead: 16,
            parallel_readers: 4,
            parallel_writers: 4,
        }
    }
}

/// Transfer engine for moving data between databases.
pub struct TransferEngine {
    source: Arc<MssqlPool>,
    target: Arc<PgPool>,
    config: TransferConfig,
    rows_transferred: AtomicI64,
}

impl TransferEngine {
    /// Create a new transfer engine.
    pub fn new(source: Arc<MssqlPool>, target: Arc<PgPool>, config: TransferConfig) -> Self {
        Self {
            source,
            target,
            config,
            rows_transferred: AtomicI64::new(0),
        }
    }

    /// Get the total rows transferred so far.
    pub fn rows_transferred(&self) -> i64 {
        self.rows_transferred.load(Ordering::Relaxed)
    }

    /// Execute a transfer job using parallel read-ahead/write-ahead pipeline.
    pub async fn execute(&self, job: TransferJob) -> Result<TransferStats> {
        let table_name = job.table.full_name();
        info!(
            "Starting transfer for {} (mode: {:?}, readers: {}, writers: {}{})",
            table_name,
            job.target_mode,
            self.config.parallel_readers,
            self.config.parallel_writers,
            if job.use_hash_detection { ", hash_detection=on" } else { "" }
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

        // Hash detection setup: check if enabled and applicable
        let use_hash = job.use_hash_detection
            && matches!(job.target_mode, TargetMode::Upsert)
            && job.table.has_single_pk();

        // Fetch existing hashes from target if hash detection is enabled
        let existing_hashes: HashMap<i64, String> = if use_hash {
            let pk_col = &pk_cols[0];
            match self
                .target
                .fetch_row_hashes(
                    &job.target_schema,
                    &job.table.name,
                    pk_col,
                    &job.row_hash_column,
                    job.min_pk,
                    job.max_pk,
                )
                .await
            {
                Ok(hashes) => {
                    info!(
                        "{}: fetched {} existing hashes for change detection",
                        table_name,
                        hashes.len()
                    );
                    hashes
                }
                Err(e) => {
                    warn!(
                        "{}: failed to fetch existing hashes, continuing without hash filtering: {}",
                        table_name, e
                    );
                    HashMap::new()
                }
            }
        } else {
            HashMap::new()
        };

        // Build column lists for reading and writing
        // When hash detection is enabled, we read with hash column from source
        // and write with hash column to target
        let (read_columns, read_col_types, write_columns) = if use_hash {
            let mut read_cols = columns.clone();
            read_cols.push(job.row_hash_column.clone());
            let mut read_types = col_types.clone();
            read_types.push("row_hash".to_string()); // Special marker for hash column
            let mut write_cols = columns.clone();
            write_cols.push(job.row_hash_column.clone());
            (read_cols, read_types, write_cols)
        } else {
            (columns.clone(), col_types.clone(), columns.clone())
        };

        // Generate hash expression for SQL Server queries
        let hash_expr = if use_hash {
            Some(generate_hash_expression(&columns, &pk_cols))
        } else {
            None
        };

        // Find PK column index for hash filtering
        let pk_idx = columns.iter().position(|c| c == &pk_cols[0]);

        // Determine if we can use parallel keyset pagination
        let use_keyset = job.table.has_single_pk() && is_pk_sortable(&job.table);
        let num_readers = if use_keyset {
            self.config.parallel_readers
        } else {
            1
        };

        if !use_keyset {
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
        let read_columns_clone = read_columns.clone();
        let read_col_types_clone = read_col_types.clone();
        let hash_expr_clone = hash_expr.clone();

        let reader_handle = tokio::spawn(async move {
            if use_keyset && num_readers > 1 {
                read_table_chunks_parallel(
                    source,
                    job_clone,
                    read_columns_clone,
                    read_col_types_clone,
                    chunk_size,
                    num_readers,
                    read_tx,
                    hash_expr_clone,
                )
                .await
            } else {
                read_table_chunks(
                    source,
                    job_clone,
                    read_columns_clone,
                    read_col_types_clone,
                    chunk_size,
                    read_tx,
                    hash_expr_clone,
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
            let write_columns_clone = write_columns.clone();
            let pk_cols_clone = pk_cols.clone();
            let target_mode = job.target_mode;

            let handle = tokio::spawn(async move {
                let mut local_write_time = Duration::ZERO;
                let mut local_rows = 0i64;

                while let Ok(write_job) = write_rx.recv().await {
                    let write_start = Instant::now();
                    let row_count = write_job.rows.len() as i64;

                    let result = match target_mode {
                        TargetMode::Upsert => {
                            target
                                .upsert_chunk_with_hash(
                                    &schema,
                                    &table_name_clone,
                                    &write_columns_clone,
                                    &pk_cols_clone,
                                    write_job.rows,
                                    writer_id,
                                )
                                .await
                        }
                        _ => {
                            target
                                .write_chunk(
                                    &schema,
                                    &table_name_clone,
                                    &write_columns_clone,
                                    write_job.rows,
                                )
                                .await
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
        // 4. Hash-based filtering (when enabled)
        //
        // Passing write_tx directly to readers would require shared-state range
        // tracking (mutex/atomic), adding complexity for marginal latency gains.
        // The dispatcher overhead is minimal since it just forwards chunks.
        let mut total_query_time = Duration::ZERO;
        let mut total_rows_skipped = 0i64;

        // Use RangeTracker for safe resume points with parallel readers.
        // This prevents data loss when chunks arrive out of order.
        let start_pk = job.resume_from_pk.or(job.min_pk);
        let mut range_tracker = RangeTracker::new(start_pk);

        // Use a separate receiver for the read channel
        let mut read_rx = read_rx;

        // Hash column is the last column when hash detection is enabled
        let hash_col_idx = if use_hash {
            Some(read_columns.len() - 1)
        } else {
            None
        };

        while let Some(chunk) = read_rx.recv().await {
            total_query_time += chunk.read_time;
            // Track completed ranges for safe resume point calculation.
            // Only contiguous ranges from start are considered safe.
            range_tracker.add_range(chunk.first_pk, chunk.last_pk);

            // Filter rows by hash if hash detection is enabled
            let (filtered_rows, skipped) = if use_hash && !existing_hashes.is_empty() {
                filter_rows_by_hash(
                    chunk.rows,
                    pk_idx,
                    hash_col_idx.unwrap(),
                    &existing_hashes,
                )
            } else {
                (chunk.rows, 0)
            };

            total_rows_skipped += skipped;

            // Only send if we have rows to write
            if !filtered_rows.is_empty() {
                let write_job = WriteJob { rows: filtered_rows };

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
        stats.rows_skipped = total_rows_skipped;
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

        if total_rows_skipped > 0 {
            info!(
                "{}: transferred {} rows, skipped {} unchanged in {:?} ({} rows/sec, read: {:?}, write: {:?})",
                table_name, stats.rows, stats.rows_skipped, total_elapsed, rows_per_sec, stats.query_time, stats.write_time
            );
        } else {
            info!(
                "{}: transferred {} rows in {:?} ({} rows/sec, read: {:?}, write: {:?})",
                table_name, stats.rows, total_elapsed, rows_per_sec, stats.query_time, stats.write_time
            );
        }

        Ok(stats)
    }
}

/// Filter rows by comparing source hash with existing target hashes.
/// Uses rayon for parallel processing of large chunks.
///
/// Returns (filtered_rows, skipped_count) where filtered_rows only contains
/// rows that are new or have changed (hash mismatch).
fn filter_rows_by_hash(
    rows: Vec<Vec<SqlValue>>,
    pk_idx: Option<usize>,
    hash_col_idx: usize,
    existing_hashes: &HashMap<i64, String>,
) -> (Vec<Vec<SqlValue>>, i64) {
    let pk_idx = match pk_idx {
        Some(idx) => idx,
        None => return (rows, 0), // Can't filter without PK index
    };

    let total = rows.len();

    // Helper to check if a row should be included (returns true if changed/new)
    let should_include = |row: &Vec<SqlValue>| -> bool {
        // Extract PK value
        let pk = match &row[pk_idx] {
            SqlValue::I64(v) => *v,
            SqlValue::I32(v) => *v as i64,
            SqlValue::I16(v) => *v as i64,
            _ => return true, // Can't extract PK, include the row
        };

        // Extract source hash
        let source_hash = match &row[hash_col_idx] {
            SqlValue::String(s) => s.as_str(),
            _ => return true, // No hash, include the row
        };

        // Check if existing hash matches
        match existing_hashes.get(&pk) {
            Some(existing) if existing == source_hash => false, // Hash matches, skip
            _ => true, // New row or hash changed, include
        }
    };

    // Use parallel processing for large chunks (>1000 rows)
    let filtered: Vec<Vec<SqlValue>> = if rows.len() > 1000 {
        rows.into_par_iter()
            .filter(|row| should_include(row))
            .collect()
    } else {
        rows.into_iter()
            .filter(|row| should_include(row))
            .collect()
    };

    let skipped = (total - filtered.len()) as i64;
    (filtered, skipped)
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
    source: Arc<MssqlPool>,
    job: TransferJob,
    columns: Vec<String>,
    col_types: Vec<String>,
    chunk_size: usize,
    num_readers: usize,
    tx: mpsc::Sender<RowChunk>,
    hash_expr: Option<String>,
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
        let hash_expr = hash_expr.clone();

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
                is_resume, tx, hash_expr,
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

/// Read chunks for a specific PK range.
///
/// `is_resume` indicates whether `start_pk` came from a resume checkpoint (true) or
/// is a fresh range boundary (false). When resuming, the first query uses `>` to skip
/// the already-processed row. For fresh starts, the first query uses `>=` to include it.
async fn read_chunk_range(
    source: Arc<MssqlPool>,
    table: Table,
    columns: Vec<String>,
    col_types: Vec<String>,
    start_pk: Option<i64>,
    end_pk: i64,
    chunk_size: usize,
    reader_id: usize,
    is_resume: bool,
    tx: mpsc::Sender<RowChunk>,
    hash_expr: Option<String>,
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
            hash_expr.as_deref(),
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
            rows,
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
    source: Arc<MssqlPool>,
    job: TransferJob,
    columns: Vec<String>,
    col_types: Vec<String>,
    chunk_size: usize,
    tx: mpsc::Sender<RowChunk>,
    hash_expr: Option<String>,
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
                &source, table, &columns, &col_types, last_pk, max_pk, chunk_size, is_first_chunk,
                hash_expr.as_deref(),
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
            rows,
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
///
/// When `hash_expr` is provided, includes the SQL Server-computed hash in the SELECT.
async fn read_chunk_keyset_fast(
    source: &MssqlPool,
    table: &Table,
    columns: &[String],
    col_types: &[String],
    last_pk: Option<i64>,
    max_pk: Option<i64>,
    chunk_size: usize,
    first_chunk: bool,
    hash_expr: Option<&str>,
) -> Result<(Vec<Vec<SqlValue>>, Option<i64>)> {
    let pk_col = &table.primary_key[0];

    // Build column list - exclude the hash column from quoted columns since it's a computed expression
    let data_col_list = columns
        .iter()
        .filter(|c| !col_types.get(columns.iter().position(|x| x == *c).unwrap_or(0))
            .map(|t| t == "row_hash")
            .unwrap_or(false))
        .map(|c| quote_mssql_ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    // Add hash expression if provided
    let select_list = if let Some(hash) = hash_expr {
        format!("{}, {}", data_col_list, hash)
    } else {
        data_col_list
    };

    let mut query = format!(
        "SELECT TOP {} {} FROM {} WITH (NOLOCK)",
        chunk_size,
        select_list,
        qualify_mssql_table(&table.schema, &table.name)
    );

    let mut conditions = Vec::new();
    if let Some(pk) = last_pk {
        // For first chunk of a range, use >= to include the starting boundary
        // For subsequent chunks (or resume), use > to skip the last-read row
        let op = if first_chunk { ">=" } else { ">" };
        conditions.push(format!("{} {} {}", quote_mssql_ident(pk_col), op, pk));
    }
    if let Some(pk) = max_pk {
        conditions.push(format!("{} <= {}", quote_mssql_ident(pk_col), pk));
    }

    if !conditions.is_empty() {
        query.push_str(" WHERE ");
        query.push_str(&conditions.join(" AND "));
    }

    query.push_str(&format!(" ORDER BY {}", quote_mssql_ident(pk_col)));

    // Use appropriate query method based on whether hash is included
    let rows = if hash_expr.is_some() {
        source.query_rows_with_hash(&query, columns, col_types).await?
    } else {
        source.query_rows_fast(&query, columns, col_types).await?
    };

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

/// Read a chunk using OFFSET pagination (for composite PKs).
async fn read_chunk_offset(
    source: &MssqlPool,
    table: &Table,
    columns: &[String],
    col_types: &[String],
    offset: usize,
    chunk_size: usize,
) -> Result<(Vec<Vec<SqlValue>>, Option<i64>)> {
    let col_list = columns
        .iter()
        .map(|c| quote_mssql_ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    // Create ORDER BY from primary key columns
    let order_by = if table.primary_key.is_empty() {
        if !columns.is_empty() {
            quote_mssql_ident(&columns[0])
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
            .map(|c| quote_mssql_ident(c))
            .collect::<Vec<_>>()
            .join(", ")
    };

    let query = format!(
        "SELECT {} FROM {} WITH (NOLOCK) ORDER BY {} OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
        col_list,
        qualify_mssql_table(&table.schema, &table.name),
        order_by,
        offset,
        chunk_size
    );

    let rows = source.query_rows_fast(&query, columns, col_types).await?;
    Ok((rows, None))
}

/// Check if the primary key is a sortable integer type.
fn is_pk_sortable(table: &Table) -> bool {
    if table.pk_columns.is_empty() {
        return false;
    }

    let pk_type = table.pk_columns[0].data_type.to_lowercase();
    matches!(pk_type.as_str(), "int" | "bigint" | "smallint" | "tinyint")
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
fn quote_mssql_ident(name: &str) -> String {
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
