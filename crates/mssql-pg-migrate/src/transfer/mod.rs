//! Data transfer engine with read-ahead/write-ahead pipeline.

use crate::config::TargetMode;
use crate::error::{MigrateError, Result};
use crate::source::{MssqlPool, Table};
use crate::target::{PgPool, SqlValue, TargetPool};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
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
    /// Column names.
    columns: Vec<String>,
    /// Row data.
    rows: Vec<Vec<SqlValue>>,
    /// Last PK value in this chunk (for resume).
    last_pk: Option<i64>,
    /// Time taken to read this chunk.
    read_time: Duration,
}

/// Transfer engine configuration.
#[derive(Debug, Clone)]
pub struct TransferConfig {
    /// Number of rows per chunk.
    pub chunk_size: usize,
    /// Number of read-ahead chunks to buffer.
    pub read_ahead: usize,
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            chunk_size: 50_000,
            read_ahead: 10, // Larger buffer reduces reader stalls
        }
    }
}

/// Transfer engine for moving data between databases.
pub struct TransferEngine {
    source: Arc<MssqlPool>,
    target: Arc<PgPool>,
    config: TransferConfig,
    rows_transferred: AtomicI64,
    bytes_transferred: AtomicU64,
}

impl TransferEngine {
    /// Create a new transfer engine.
    pub fn new(source: Arc<MssqlPool>, target: Arc<PgPool>, config: TransferConfig) -> Self {
        Self {
            source,
            target,
            config,
            rows_transferred: AtomicI64::new(0),
            bytes_transferred: AtomicU64::new(0),
        }
    }

    /// Get the total rows transferred so far.
    pub fn rows_transferred(&self) -> i64 {
        self.rows_transferred.load(Ordering::Relaxed)
    }

    /// Execute a transfer job using read-ahead pipeline.
    pub async fn execute(&self, job: TransferJob) -> Result<TransferStats> {
        let table_name = job.table.full_name();
        info!(
            "Starting transfer for {} (mode: {:?})",
            table_name, job.target_mode
        );

        let start = Instant::now();
        let mut stats = TransferStats::default();

        // Create channel for read-ahead pipeline
        let (tx, mut rx) = mpsc::channel::<RowChunk>(self.config.read_ahead);

        // Get column names for this table
        let columns: Vec<String> = job.table.columns.iter().map(|c| c.name.clone()).collect();
        let pk_cols: Vec<String> = job.table.primary_key.clone();

        // Spawn reader task
        let source = self.source.clone();
        let job_clone = job.clone();
        let chunk_size = self.config.chunk_size;
        let columns_clone = columns.clone();

        let reader_handle = tokio::spawn(async move {
            read_table_chunks(source, job_clone, columns_clone, chunk_size, tx).await
        });

        // Write chunks as they arrive
        let mut write_time = Duration::ZERO;
        let mut rows_written: i64 = 0;

        while let Some(chunk) = rx.recv().await {
            let write_start = Instant::now();

            let row_count = chunk.rows.len() as u64;
            stats.last_pk = chunk.last_pk;
            stats.query_time += chunk.read_time;

            match job.target_mode {
                TargetMode::Upsert => {
                    self.target
                        .upsert_chunk(
                            &job.target_schema,
                            &job.table.name,
                            &columns,
                            &pk_cols,
                            chunk.rows,
                        )
                        .await?;
                }
                _ => {
                    self.target
                        .write_chunk(&job.target_schema, &job.table.name, &columns, chunk.rows)
                        .await?;
                }
            }

            write_time += write_start.elapsed();
            rows_written += row_count as i64;
            self.rows_transferred.fetch_add(row_count as i64, Ordering::Relaxed);

            debug!(
                "{}: wrote {} rows (total: {})",
                table_name, row_count, rows_written
            );
        }

        // Wait for reader to complete
        match reader_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(MigrateError::Transfer {
                table: table_name.clone(),
                message: format!("Reader task failed: {}", e),
            }),
        }

        stats.rows = rows_written;
        stats.write_time = write_time;
        // Use saturating_sub to avoid overflow if query_time + write_time > elapsed
        let total_elapsed = start.elapsed();
        stats.scan_time = total_elapsed
            .saturating_sub(stats.query_time)
            .saturating_sub(stats.write_time);
        stats.completed = true;

        info!(
            "{}: transferred {} rows in {:?} (read: {:?}, write: {:?})",
            table_name,
            stats.rows,
            start.elapsed(),
            stats.query_time,
            stats.write_time
        );

        Ok(stats)
    }
}

/// Read table data in chunks using keyset pagination.
async fn read_table_chunks(
    source: Arc<MssqlPool>,
    job: TransferJob,
    columns: Vec<String>,
    chunk_size: usize,
    tx: mpsc::Sender<RowChunk>,
) -> Result<()> {
    let table = &job.table;
    let table_name = table.full_name();

    // Determine starting point
    let mut last_pk: Option<i64> = job.resume_from_pk.or(job.min_pk);
    let max_pk = job.max_pk;

    // Check if we can use keyset pagination
    let use_keyset = table.has_single_pk() && is_pk_sortable(table);

    if !use_keyset {
        warn!(
            "{}: using OFFSET pagination (slower)",
            table_name
        );
    }

    let mut total_rows = 0i64;
    let mut chunk_num = 0;

    loop {
        let read_start = Instant::now();

        let (rows, new_last_pk) = if use_keyset {
            read_chunk_keyset(&source, table, &columns, last_pk, max_pk, chunk_size).await?
        } else {
            read_chunk_offset(&source, table, &columns, total_rows as usize, chunk_size).await?
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
            columns: columns.clone(),
            rows,
            last_pk: new_last_pk,
            read_time,
        };

        // Send chunk to writer (blocks if buffer is full - backpressure)
        if tx.send(chunk).await.is_err() {
            // Receiver dropped, likely due to error
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

    info!("{}: finished reading {} rows in {} chunks", table_name, total_rows, chunk_num);
    Ok(())
}

/// Read a chunk using keyset pagination (WHERE pk > last_pk).
async fn read_chunk_keyset(
    source: &MssqlPool,
    table: &Table,
    columns: &[String],
    last_pk: Option<i64>,
    max_pk: Option<i64>,
    chunk_size: usize,
) -> Result<(Vec<Vec<SqlValue>>, Option<i64>)> {
    let pk_col = &table.primary_key[0];
    let col_list = columns.iter().map(|c| format!("[{}]", c)).collect::<Vec<_>>().join(", ");

    let mut query = format!(
        "SELECT TOP {} {} FROM [{}].[{}]",
        chunk_size, col_list, table.schema, table.name
    );

    let mut conditions = Vec::new();
    if let Some(pk) = last_pk {
        conditions.push(format!("[{}] > {}", pk_col, pk));
    }
    if let Some(pk) = max_pk {
        conditions.push(format!("[{}] <= {}", pk_col, pk));
    }

    if !conditions.is_empty() {
        query.push_str(" WHERE ");
        query.push_str(&conditions.join(" AND "));
    }

    query.push_str(&format!(" ORDER BY [{}]", pk_col));

    let rows = source.query_rows(&query, columns, table).await?;

    // Get the last PK value from the result
    let new_last_pk = if !rows.is_empty() {
        let pk_idx = columns.iter().position(|c| c == pk_col);
        pk_idx.and_then(|idx| {
            rows.last().and_then(|row| {
                match &row[idx] {
                    SqlValue::I64(v) => Some(*v),
                    SqlValue::I32(v) => Some(*v as i64),
                    _ => None,
                }
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
    offset: usize,
    chunk_size: usize,
) -> Result<(Vec<Vec<SqlValue>>, Option<i64>)> {
    let col_list = columns.iter().map(|c| format!("[{}]", c)).collect::<Vec<_>>().join(", ");

    // Create ORDER BY from primary key columns
    let order_by = if table.primary_key.is_empty() {
        // No PK, try to use first column
        if !columns.is_empty() {
            format!("[{}]", columns[0])
        } else {
            return Err(MigrateError::Transfer {
                table: table.full_name(),
                message: "Table has no columns".into(),
            });
        }
    } else {
        table.primary_key.iter().map(|c| format!("[{}]", c)).collect::<Vec<_>>().join(", ")
    };

    let query = format!(
        "SELECT {} FROM [{}].[{}] ORDER BY {} OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
        col_list, table.schema, table.name, order_by, offset, chunk_size
    );

    let rows = source.query_rows(&query, columns, table).await?;
    Ok((rows, None))
}

/// Check if the primary key is a sortable integer type.
fn is_pk_sortable(table: &Table) -> bool {
    if table.pk_columns.is_empty() {
        return false;
    }

    let pk_type = table.pk_columns[0].data_type.to_lowercase();
    matches!(
        pk_type.as_str(),
        "int" | "bigint" | "smallint" | "tinyint"
    )
}
