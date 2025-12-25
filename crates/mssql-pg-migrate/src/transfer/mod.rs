//! Data transfer engine with parallel read-ahead/write-ahead pipeline.
//!
//! This module implements a high-performance transfer engine that uses:
//! - Multiple parallel readers (PK range splitting) to maximize source throughput
//! - Multiple parallel writers to maximize target throughput
//! - Read-ahead buffering to overlap read and write operations

use crate::config::TargetMode;
use crate::error::{MigrateError, Result};
use crate::source::{MssqlPool, Table};
use crate::target::{PgPool, SqlValue, TargetPool};
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

/// A chunk of rows to be transferred.
#[derive(Debug)]
struct RowChunk {
    /// Row data.
    rows: Vec<Vec<SqlValue>>,
    /// Last PK value in this chunk (for resume).
    last_pk: Option<i64>,
    /// Time taken to read this chunk.
    read_time: Duration,
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
            "Starting transfer for {} (mode: {:?}, readers: {}, writers: {})",
            table_name, job.target_mode, self.config.parallel_readers, self.config.parallel_writers
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
        let columns_clone = columns.clone();
        let col_types_clone = col_types.clone();

        let reader_handle = tokio::spawn(async move {
            if use_keyset && num_readers > 1 {
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

            let handle = tokio::spawn(async move {
                let mut local_write_time = Duration::ZERO;
                let mut local_rows = 0i64;

                while let Ok(write_job) = write_rx.recv().await {
                    let write_start = Instant::now();
                    let row_count = write_job.rows.len() as i64;

                    let result = match target_mode {
                        TargetMode::Upsert => {
                            target
                                .upsert_chunk(
                                    &schema,
                                    &table_name_clone,
                                    &columns_clone,
                                    &pk_cols_clone,
                                    write_job.rows,
                                )
                                .await
                        }
                        _ => {
                            target
                                .write_chunk(
                                    &schema,
                                    &table_name_clone,
                                    &columns_clone,
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

        // Dispatcher: read from read_rx, dispatch to write_tx
        let mut total_query_time = Duration::ZERO;
        let mut last_pk = None;

        // Use a separate receiver for the read channel
        let mut read_rx = read_rx;

        while let Some(chunk) = read_rx.recv().await {
            total_query_time += chunk.read_time;
            // With parallel readers, chunks may arrive out of order.
            // For resume safety, track the minimum PK processed so far.
            last_pk = match (last_pk, chunk.last_pk) {
                (None, pk) => pk,
                (pk, None) => pk,
                (Some(a), Some(b)) => Some(a.min(b)),
            };

            let write_job = WriteJob { rows: chunk.rows };

            if write_tx.send(write_job).await.is_err() {
                // Writers have all failed
                break;
            }
        }

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

        // Wait for all writers and aggregate stats
        // Writers run in parallel, so wall-clock write time is approximated
        // by the maximum per-writer duration, not the sum.
        let mut total_write_time = Duration::ZERO;
        let mut total_rows = 0i64;

        for handle in writer_handles {
            match handle.await {
                Ok(Ok((write_time, rows))) => {
                    if write_time > total_write_time {
                        total_write_time = write_time;
                    }
                    total_rows += rows;
                }
                Ok(Err(e)) => return Err(e),
                Err(e) => {
                    return Err(MigrateError::Transfer {
                        table: table_name.clone(),
                        message: format!("Writer task panicked: {}", e),
                    })
                }
            }
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
    source: Arc<MssqlPool>,
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
        let start_pk = if reader_id == 0 {
            job.resume_from_pk.or(job.min_pk)
        } else {
            range_min
        };

        let handle = tokio::spawn(async move {
            read_chunk_range(
                source, table, columns, col_types, start_pk, range_max, chunk_size, reader_id, tx,
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
async fn read_chunk_range(
    source: Arc<MssqlPool>,
    table: Table,
    columns: Vec<String>,
    col_types: Vec<String>,
    start_pk: Option<i64>,
    end_pk: i64,
    chunk_size: usize,
    reader_id: usize,
    tx: mpsc::Sender<RowChunk>,
) -> Result<i64> {
    let table_name = table.full_name();

    let mut last_pk = start_pk;
    let mut total_rows = 0i64;
    let mut chunk_num = 0;

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
        )
        .await?;

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

        let chunk = RowChunk {
            rows,
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
) -> Result<()> {
    let table = &job.table;
    let table_name = table.full_name();

    let use_keyset = table.has_single_pk() && is_pk_sortable(table);

    let mut last_pk: Option<i64> = job.resume_from_pk.or(job.min_pk);
    let max_pk = job.max_pk;

    let mut total_rows = 0i64;
    let mut chunk_num = 0;

    loop {
        let read_start = Instant::now();

        let (rows, new_last_pk) = if use_keyset {
            read_chunk_keyset_fast(
                &source, table, &columns, &col_types, last_pk, max_pk, chunk_size,
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

        let chunk = RowChunk {
            rows,
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
async fn read_chunk_keyset_fast(
    source: &MssqlPool,
    table: &Table,
    columns: &[String],
    col_types: &[String],
    last_pk: Option<i64>,
    max_pk: Option<i64>,
    chunk_size: usize,
) -> Result<(Vec<Vec<SqlValue>>, Option<i64>)> {
    let pk_col = &table.primary_key[0];
    let col_list = columns
        .iter()
        .map(|c| quote_mssql_ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    let mut query = format!(
        "SELECT TOP {} {} FROM {} WITH (NOLOCK)",
        chunk_size,
        col_list,
        qualify_mssql_table(&table.schema, &table.name)
    );

    let mut conditions = Vec::new();
    if let Some(pk) = last_pk {
        conditions.push(format!("{} > {}", quote_mssql_ident(pk_col), pk));
    }
    if let Some(pk) = max_pk {
        conditions.push(format!("{} <= {}", quote_mssql_ident(pk_col), pk));
    }

    if !conditions.is_empty() {
        query.push_str(" WHERE ");
        query.push_str(&conditions.join(" AND "));
    }

    query.push_str(&format!(" ORDER BY {}", quote_mssql_ident(pk_col)));

    // Use fast query with pre-computed column types
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
fn quote_mssql_ident(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

/// Qualify a SQL Server table name with schema and proper quoting.
fn qualify_mssql_table(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_mssql_ident(schema), quote_mssql_ident(table))
}
