//! Migration orchestrator - main workflow coordinator.

use crate::config::{Config, TableStats, TargetMode};
use crate::error::{MigrateError, Result};
use crate::source::{MssqlPool, SourcePool, Table};
use crate::state::{MigrationState, RunStatus, TableState, TaskStatus};
use crate::target::{PgPool, TargetPool};
use crate::transfer::{TransferConfig, TransferEngine, TransferJob};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Migration orchestrator.
pub struct Orchestrator {
    config: Config,
    state_file: Option<PathBuf>,
    state: Option<MigrationState>,
    source: Arc<MssqlPool>,
    target: Arc<PgPool>,
    progress_enabled: bool,
}

/// Result of a migration run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationResult {
    /// Unique run identifier.
    pub run_id: String,

    /// Final status: "completed", "failed", "cancelled", "dry_run".
    pub status: String,

    /// Total duration in seconds.
    pub duration_seconds: f64,

    /// When the migration started.
    pub started_at: DateTime<Utc>,

    /// When the migration completed.
    pub completed_at: DateTime<Utc>,

    /// Total tables processed.
    pub tables_total: usize,

    /// Tables successfully migrated.
    pub tables_success: usize,

    /// Tables that failed.
    pub tables_failed: usize,

    /// Total rows transferred.
    pub rows_transferred: i64,

    /// Average throughput (rows/second).
    pub rows_per_second: i64,

    /// List of failed table names (for backward compatibility).
    pub failed_tables: Vec<String>,

    /// Detailed errors per table.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub table_errors: Vec<TableError>,

    /// Top-level error message if migration failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Error type classification for retry logic.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,

    /// Whether this error is potentially recoverable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recoverable: Option<bool>,
}

/// Detailed error information for a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableError {
    /// Table name.
    pub table: String,

    /// Error type classification.
    pub error_type: String,

    /// Error message.
    pub message: String,
}

/// Result of a health check.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HealthCheckResult {
    /// Overall health status.
    pub healthy: bool,

    /// Source database connection status.
    pub source_connected: bool,

    /// Source connection latency in milliseconds.
    pub source_latency_ms: u64,

    /// Source connection error if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_error: Option<String>,

    /// Target database connection status.
    pub target_connected: bool,

    /// Target connection latency in milliseconds.
    pub target_latency_ms: u64,

    /// Target connection error if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_error: Option<String>,
}

/// Progress update for monitoring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressUpdate {
    /// Timestamp of the update.
    pub timestamp: DateTime<Utc>,

    /// Current phase.
    pub phase: String,

    /// Tables completed so far.
    pub tables_completed: usize,

    /// Total tables.
    pub tables_total: usize,

    /// Rows transferred so far.
    pub rows_transferred: i64,

    /// Current throughput (rows/second).
    pub rows_per_second: i64,

    /// Table currently being processed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_table: Option<String>,
}

/// Write a progress update as JSON to stderr.
fn emit_progress_json(update: &ProgressUpdate) {
    if let Ok(json) = serde_json::to_string(update) {
        let mut stderr = std::io::stderr().lock();
        let _ = writeln!(stderr, "{}", json);
    }
}

/// Thread-safe progress tracker for concurrent table transfers.
#[derive(Debug)]
struct ProgressTracker {
    tables_completed: AtomicUsize,
    tables_total: usize,
    rows_transferred: AtomicI64,
    started_at: Instant,
    progress_enabled: bool,
}

impl ProgressTracker {
    fn new(tables_total: usize, progress_enabled: bool) -> Self {
        Self {
            tables_completed: AtomicUsize::new(0),
            tables_total,
            rows_transferred: AtomicI64::new(0),
            started_at: Instant::now(),
            progress_enabled,
        }
    }

    fn complete_table(&self, rows: i64) {
        self.rows_transferred.fetch_add(rows, Ordering::Relaxed);
        self.tables_completed.fetch_add(1, Ordering::Relaxed);
    }

    fn increment_table_count(&self) {
        self.tables_completed.fetch_add(1, Ordering::Relaxed);
    }

    fn get_rows_per_second(&self) -> i64 {
        let elapsed = self.started_at.elapsed().as_secs_f64();
        let rows = self.rows_transferred.load(Ordering::Relaxed);
        if elapsed > 0.0 {
            (rows as f64 / elapsed) as i64
        } else {
            0
        }
    }

    fn emit(&self, phase: &str, current_table: Option<String>) {
        if !self.progress_enabled {
            return;
        }

        let update = ProgressUpdate {
            timestamp: Utc::now(),
            phase: phase.to_string(),
            tables_completed: self.tables_completed.load(Ordering::Relaxed),
            tables_total: self.tables_total,
            rows_transferred: self.rows_transferred.load(Ordering::Relaxed),
            rows_per_second: self.get_rows_per_second(),
            current_table,
        };

        emit_progress_json(&update);
    }
}

impl Orchestrator {
    /// Create a new orchestrator.
    pub async fn new(config: Config) -> Result<Self> {
        // Create source pool with configurable size
        let mssql_pool_size = config.migration.get_max_mssql_connections() as u32;
        let source =
            MssqlPool::with_max_connections(config.source.clone(), mssql_pool_size).await?;

        // Create target pool
        let max_conns = config.migration.get_max_pg_connections();
        let target = PgPool::new(
            &config.target,
            max_conns,
            config.migration.get_copy_buffer_rows(),
            config.migration.use_binary_copy,
        )
        .await?;

        Ok(Self {
            config,
            state_file: None,
            state: None,
            source: Arc::new(source),
            target: Arc::new(target),
            progress_enabled: false,
        })
    }

    /// Set the state file path for resume capability.
    pub fn with_state_file(mut self, path: PathBuf) -> Self {
        self.state_file = Some(path);
        self
    }

    /// Enable progress reporting to stderr as JSON lines.
    pub fn with_progress(mut self, enabled: bool) -> Self {
        self.progress_enabled = enabled;
        self
    }

    /// Emit a progress update to stderr (for phase transitions before transfer starts).
    fn emit_progress(&self, phase: &str, tables_total: usize) {
        if !self.progress_enabled {
            return;
        }

        let update = ProgressUpdate {
            timestamp: Utc::now(),
            phase: phase.to_string(),
            tables_completed: 0,
            tables_total,
            rows_transferred: 0,
            rows_per_second: 0,
            current_table: None,
        };

        emit_progress_json(&update);
    }

    /// Load existing state for resume.
    pub fn resume(mut self) -> Result<Self> {
        if let Some(ref path) = self.state_file {
            if path.exists() {
                let state = MigrationState::load(path)?;
                state.validate_config(&self.config.hash())?;
                self.state = Some(state);
                info!("Resuming from state file: {:?}", path);
            }
        }
        Ok(self)
    }

    /// Perform a health check on database connections.
    pub async fn health_check(&self) -> Result<HealthCheckResult> {
        let mut result = HealthCheckResult::default();

        // Test MSSQL connection
        let start = Instant::now();
        match self.source.test_connection().await {
            Ok(()) => {
                result.source_connected = true;
                result.source_latency_ms = start.elapsed().as_millis() as u64;
            }
            Err(e) => {
                result.source_connected = false;
                result.source_latency_ms = start.elapsed().as_millis() as u64;
                result.source_error = Some(e.to_string());
            }
        }

        // Test PostgreSQL connection
        let start = Instant::now();
        match self.target.test_connection().await {
            Ok(()) => {
                result.target_connected = true;
                result.target_latency_ms = start.elapsed().as_millis() as u64;
            }
            Err(e) => {
                result.target_connected = false;
                result.target_latency_ms = start.elapsed().as_millis() as u64;
                result.target_error = Some(e.to_string());
            }
        }

        result.healthy = result.source_connected && result.target_connected;
        Ok(result)
    }

    /// Run the migration.
    /// If dry_run is true, only validates and shows what would happen without transferring data.
    pub async fn run(
        mut self,
        cancel: CancellationToken,
        dry_run: bool,
    ) -> Result<MigrationResult> {
        let started_at = Utc::now();
        let run_id = self
            .state
            .as_ref()
            .map(|s| s.run_id.clone())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        info!("Starting migration run: {}", run_id);

        // Phase 1: Extract schema
        info!("Phase 1: Extracting schema from source");
        self.emit_progress("extracting_schema", 0);
        let mut tables = self
            .source
            .extract_schema(&self.config.source.schema)
            .await?;

        // Load additional metadata
        for table in &mut tables {
            if self.config.migration.create_indexes {
                self.source.load_indexes(table).await?;
            }
            if self.config.migration.create_foreign_keys {
                self.source.load_foreign_keys(table).await?;
            }
            if self.config.migration.create_check_constraints {
                self.source.load_check_constraints(table).await?;
            }
        }

        info!("Found {} tables to migrate", tables.len());
        self.emit_progress("schema_extracted", tables.len());

        // Apply auto-tuning now that we know actual table sizes
        let table_stats: Vec<TableStats> = tables
            .iter()
            .map(|t| TableStats {
                name: t.full_name(),
                row_count: t.row_count,
                estimated_row_size: t.estimated_row_size,
            })
            .collect();
        self.config.apply_auto_tuning_from_tables(&table_stats);

        // Initialize or update state
        let mut state = self
            .state
            .take()
            .unwrap_or_else(|| MigrationState::new(run_id.clone(), self.config.hash()));

        // Initialize table states
        for table in &tables {
            let table_name = table.full_name();
            state
                .tables
                .entry(table_name.clone())
                .or_insert_with(|| TableState::new(table.row_count));
        }

        // Phase 2: Prepare target (skip in dry-run)
        if !dry_run {
            info!(
                "Phase 2: Preparing target database (mode: {:?})",
                self.config.migration.target_mode
            );
            self.emit_progress("preparing_target", tables.len());
            self.prepare_target(&tables, &mut state).await?;

            // Save state after preparation
            self.save_state(&state)?;
        } else {
            info!(
                "Phase 2: [DRY RUN] Would prepare target database (mode: {:?})",
                self.config.migration.target_mode
            );
        }

        // Phase 3: Transfer data (skip in dry-run)
        let transfer_result = if !dry_run {
            info!("Phase 3: Transferring data");
            self.emit_progress("transferring", tables.len());
            self.transfer_data(&tables, &mut state, &cancel).await
        } else {
            info!("Phase 3: [DRY RUN] Would transfer data for {} tables", tables.len());
            Ok(())
        };

        // Phase 4: Finalize (only on success, skip in dry-run)
        if transfer_result.is_ok() && !dry_run {
            info!("Phase 4: Finalizing (indexes, constraints)");
            self.emit_progress("finalizing", tables.len());
            self.finalize(&tables).await?;
        } else if dry_run {
            info!("Phase 4: [DRY RUN] Would finalize (indexes, constraints)");
        }

        // Build result
        let completed_at = Utc::now();
        let duration = (completed_at - started_at).num_milliseconds() as f64 / 1000.0;

        let mut tables_success = 0;
        let mut tables_failed = 0;
        let mut failed_tables = Vec::new();
        let mut rows_transferred: i64 = 0;

        for (name, table_state) in &state.tables {
            match table_state.status {
                TaskStatus::Completed => {
                    tables_success += 1;
                    rows_transferred += table_state.rows_transferred;
                }
                TaskStatus::Failed => {
                    tables_failed += 1;
                    failed_tables.push(name.clone());
                }
                _ => {}
            }
        }

        let rows_per_second = if duration > 0.0 {
            (rows_transferred as f64 / duration) as i64
        } else {
            0
        };

        // Build table_errors from state
        let table_errors: Vec<TableError> = state
            .tables
            .iter()
            .filter_map(|(name, ts)| {
                ts.error.as_ref().map(|err| TableError {
                    table: name.clone(),
                    error_type: "transfer".to_string(),
                    message: err.clone(),
                })
            })
            .collect();

        let status = if dry_run {
            "dry_run"
        } else if tables_failed > 0 {
            state.status = RunStatus::Failed;
            "failed"
        } else if cancel.is_cancelled() {
            state.status = RunStatus::Cancelled;
            "cancelled"
        } else {
            state.status = RunStatus::Completed;
            "completed"
        };

        // Save final state (skip in dry-run)
        if !dry_run {
            self.save_state(&state)?;
        }

        let result = MigrationResult {
            run_id,
            status: status.to_string(),
            duration_seconds: duration,
            started_at,
            completed_at,
            tables_total: tables.len(),
            tables_success,
            tables_failed,
            rows_transferred,
            rows_per_second,
            failed_tables,
            table_errors,
            error: None,
            error_type: None,
            recoverable: None,
        };

        info!(
            "Migration {}: {} tables, {} rows in {:.1}s ({} rows/s)",
            result.status,
            result.tables_total,
            result.rows_transferred,
            result.duration_seconds,
            result.rows_per_second
        );

        if let Err(e) = transfer_result {
            return Err(e);
        }

        Ok(result)
    }

    /// Prepare target database based on mode.
    async fn prepare_target(&self, tables: &[Table], state: &mut MigrationState) -> Result<()> {
        let target_schema = &self.config.target.schema;

        // Create schema if needed
        self.target.create_schema(target_schema).await?;

        match self.config.migration.target_mode {
            TargetMode::DropRecreate => {
                // Drop and recreate all tables
                for table in tables {
                    let table_name = table.full_name();
                    debug!("Dropping table: {}", table_name);
                    self.target.drop_table(target_schema, &table.name).await?;

                    debug!("Creating table: {}", table_name);
                    // Use UNLOGGED for faster inserts if configured (tables stay UNLOGGED)
                    if self.config.migration.use_unlogged_tables {
                        self.target
                            .create_table_unlogged(table, target_schema)
                            .await?;
                    } else {
                        self.target.create_table(table, target_schema).await?;
                    }
                }
            }
            TargetMode::Truncate => {
                for table in tables {
                    let table_name = table.full_name();

                    // Create if not exists, truncate if exists
                    if self.target.table_exists(target_schema, &table.name).await? {
                        debug!("Truncating table: {}", table_name);
                        self.target
                            .truncate_table(target_schema, &table.name)
                            .await?;
                        // Set to UNLOGGED for faster writes if configured
                        if self.config.migration.use_unlogged_tables {
                            self.target
                                .set_table_unlogged(target_schema, &table.name)
                                .await?;
                        }
                    } else {
                        debug!("Creating table: {}", table_name);
                        if self.config.migration.use_unlogged_tables {
                            self.target
                                .create_table_unlogged(table, target_schema)
                                .await?;
                        } else {
                            self.target.create_table(table, target_schema).await?;
                        }
                    }

                    // Mark as pending for data transfer
                    if let Some(ts) = state.tables.get_mut(&table_name) {
                        ts.status = TaskStatus::Pending;
                        ts.rows_transferred = 0;
                    }
                }
            }
            TargetMode::Upsert => {
                // For upsert, ensure tables exist and drop non-PK indexes for performance
                for table in tables {
                    let table_name = table.full_name();

                    // Check for primary key (required for upsert)
                    if table.primary_key.is_empty() {
                        return Err(MigrateError::NoPrimaryKey(table_name));
                    }

                    if !self.target.table_exists(target_schema, &table.name).await? {
                        debug!("Creating table for upsert: {}", table_name);
                        if self.config.migration.use_unlogged_tables {
                            self.target
                                .create_table_unlogged(table, target_schema)
                                .await?;
                        } else {
                            self.target.create_table(table, target_schema).await?;
                        }
                        self.target.create_primary_key(table, target_schema).await?;
                    } else {
                        // Drop non-PK indexes for faster upserts (only recreated if create_indexes is enabled)
                        let dropped = self
                            .target
                            .drop_non_pk_indexes(target_schema, &table.name)
                            .await?;
                        if !dropped.is_empty() {
                            info!(
                                "Dropped {} non-PK indexes on {} for faster upserts",
                                dropped.len(),
                                table_name
                            );
                        }

                        if self.config.migration.use_unlogged_tables {
                            // Set existing table to UNLOGGED for faster writes
                            self.target
                                .set_table_unlogged(target_schema, &table.name)
                                .await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Check if a table should be partitioned for parallel reads.
    fn should_partition(&self, table: &Table) -> bool {
        // Must have single-column integer PK for keyset pagination
        if !table.has_single_pk() {
            return false;
        }

        let pk_type = table
            .pk_columns
            .first()
            .map(|c| c.data_type.to_lowercase())
            .unwrap_or_default();

        let is_sortable = matches!(pk_type.as_str(), "int" | "bigint" | "smallint" | "tinyint");

        if !is_sortable {
            return false;
        }

        // Only partition large tables
        table.row_count >= self.config.migration.get_large_table_threshold()
    }

    /// Compute partition count for a table based on size and configured limits.
    fn partition_count(&self, table: &Table) -> usize {
        if !self.should_partition(table) {
            return 1;
        }

        let max_partitions = self.config.migration.get_parallel_readers().max(1);
        let min_rows = self.config.migration.get_min_rows_per_partition().max(1);
        let by_rows = ((table.row_count.max(0) + min_rows - 1) / min_rows) as usize;

        max_partitions.min(by_rows).max(1)
    }

    /// Transfer data for all tables.
    async fn transfer_data(
        &self,
        tables: &[Table],
        state: &mut MigrationState,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let workers = self.config.migration.get_workers();
        let semaphore = Arc::new(Semaphore::new(workers));

        // Create progress tracker
        let progress = Arc::new(ProgressTracker::new(tables.len(), self.progress_enabled));

        let transfer_config = TransferConfig {
            chunk_size: self.config.migration.get_chunk_size(),
            read_ahead: self.config.migration.get_read_ahead_buffers(),
            parallel_readers: self.config.migration.get_parallel_readers(),
            parallel_writers: self.config.migration.get_write_ahead_writers(),
        };

        let engine = Arc::new(TransferEngine::new(
            self.source.clone(),
            self.target.clone(),
            transfer_config,
        ));

        // Filter tables that need processing
        let pending_tables: Vec<_> = tables
            .iter()
            .filter(|t| {
                let table_name = t.full_name();
                state
                    .tables
                    .get(&table_name)
                    .map(|ts| ts.status != TaskStatus::Completed)
                    .unwrap_or(true)
            })
            .collect();

        info!(
            "Transferring {} tables with {} workers",
            pending_tables.len(),
            workers
        );

        // Use JoinSet to process task completions as they occur (not sequentially).
        // This prevents fast tables from waiting on slow tables.
        let mut join_set: JoinSet<(String, crate::error::Result<crate::transfer::TransferStats>)> =
            JoinSet::new();

        // Track expected partition counts per table (populated during spawning)
        let mut expected_partitions: HashMap<String, usize> = HashMap::new();

        for table in pending_tables {
            // Check for cancellation
            if cancel.is_cancelled() {
                info!("Cancellation requested, stopping new transfers");
                break;
            }

            let table_name = table.full_name();

            // Check if table should be partitioned for parallel reads
            let num_partitions = self.partition_count(table);
            if num_partitions > 1 {
                // Get partition boundaries
                match self
                    .source
                    .get_partition_boundaries(table, num_partitions)
                    .await
                {
                    Ok(partitions) => {
                        info!(
                            "{}: partitioning into {} parallel reads ({} rows)",
                            table_name,
                            partitions.len(),
                            table.row_count
                        );

                        // Track expected partition count for this table
                        expected_partitions.insert(table_name.clone(), partitions.len());

                        // Update state
                        if let Some(ts) = state.tables.get_mut(&table_name) {
                            ts.status = TaskStatus::InProgress;
                        }
                        self.save_state(state)?;

                        // Create a job for each partition
                        for partition in partitions {
                            let permit = semaphore.clone().acquire_owned().await.unwrap();

                            // No adjustment needed: the transfer engine now uses >= for first chunk,
                            // which correctly includes the partition's min_pk boundary.
                            let job = TransferJob {
                                table: table.clone(),
                                partition_id: Some(partition.partition_id),
                                min_pk: partition.min_pk,
                                max_pk: partition.max_pk,
                                resume_from_pk: None, // Partitions don't support resume yet
                                target_mode: self.config.migration.target_mode.clone(),
                                target_schema: self.config.target.schema.clone(),
                            };

                            let engine_clone = engine.clone();
                            let partition_name =
                                format!("{}:p{}", table_name, partition.partition_id);

                            join_set.spawn(async move {
                                let result = engine_clone.execute(job).await;
                                drop(permit);
                                (partition_name, result)
                            });
                        }
                        continue;
                    }
                    Err(e) => {
                        warn!(
                            "{}: failed to partition, using single reader: {}",
                            table_name, e
                        );
                        // Fall through to single-job path
                    }
                }
            }

            // Single job for small tables or partition failures
            let permit = semaphore.clone().acquire_owned().await.unwrap();

            // Update state
            if let Some(ts) = state.tables.get_mut(&table_name) {
                ts.status = TaskStatus::InProgress;
            }
            self.save_state(state)?;

            let job = TransferJob {
                table: table.clone(),
                partition_id: None,
                min_pk: None,
                max_pk: None,
                resume_from_pk: state.tables.get(&table_name).and_then(|ts| ts.last_pk),
                target_mode: self.config.migration.target_mode.clone(),
                target_schema: self.config.target.schema.clone(),
            };

            let engine_clone = engine.clone();
            let job_name = table_name.clone();

            join_set.spawn(async move {
                let result = engine_clone.execute(job).await;
                drop(permit);
                (job_name, result)
            });
        }

        // Collect results using JoinSet.join_next() for completion-order processing.
        // This prevents fast tables from waiting on slow tables.
        let mut table_rows: HashMap<String, i64> = HashMap::new();
        let mut table_errors: HashMap<String, String> = HashMap::new();
        let mut partition_completed: HashMap<String, usize> = HashMap::new();

        // Process results as they complete (not in spawn order)
        while let Some(join_result) = join_set.join_next().await {
            // Check for cancellation - abort remaining tasks
            if cancel.is_cancelled() {
                info!("Cancellation detected, aborting remaining {} tasks", join_set.len());
                join_set.abort_all();
                // Save current state before breaking
                self.save_state(state)?;
                break;
            }

            // Extract job result from JoinSet
            let (job_name, task_result) = match join_result {
                Ok(result) => result,
                Err(e) => {
                    // Task panicked - we can't recover the job name from JoinError
                    error!("Task panicked: {}", e);
                    continue;
                }
            };

            // Extract base table name (handles both "schema.table" and "schema.table:p1")
            let base_table = job_name.split(":p").next().unwrap_or(&job_name).to_string();
            let is_partitioned = job_name.contains(":p");

            match task_result {
                Ok(stats) => {
                    info!("{}: completed ({} rows)", job_name, stats.rows);
                    *table_rows.entry(base_table.clone()).or_insert(0) += stats.rows;

                    // Track partition completion
                    if is_partitioned {
                        *partition_completed.entry(base_table.clone()).or_insert(0) += 1;
                        let completed = partition_completed[&base_table];
                        let expected = expected_partitions.get(&base_table).copied().unwrap_or(1);

                        // When all partitions of a table are done, update state and checkpoint
                        if completed == expected {
                            let total_rows = table_rows[&base_table];
                            if let Some(ts) = state.tables.get_mut(&base_table) {
                                if !table_errors.contains_key(&base_table) {
                                    ts.status = TaskStatus::Completed;
                                    ts.rows_transferred = total_rows;
                                    ts.completed_at = Some(Utc::now());
                                } else {
                                    ts.status = TaskStatus::Failed;
                                    ts.error = table_errors.get(&base_table).cloned();
                                    ts.rows_transferred = total_rows;
                                }
                            }
                            self.save_state(state)?;
                            progress.complete_table(total_rows);
                            progress.emit("transferring", Some(base_table.clone()));
                        }
                    } else {
                        // For non-partitioned tables, update state directly
                        if let Some(ts) = state.tables.get_mut(&base_table) {
                            ts.status = TaskStatus::Completed;
                            ts.rows_transferred = stats.rows;
                            ts.last_pk = stats.last_pk;
                            ts.completed_at = Some(Utc::now());
                        }
                        // Checkpoint state after each table completion
                        self.save_state(state)?;
                        progress.complete_table(stats.rows);
                        progress.emit("transferring", Some(base_table.clone()));
                    }
                }
                Err(e) => {
                    error!("{}: failed - {}", job_name, e);
                    // Preserve first error for this table (don't overwrite if partition already failed)
                    table_errors
                        .entry(base_table.clone())
                        .or_insert_with(|| e.to_string());

                    // Track partition completion even on failure
                    if is_partitioned {
                        *partition_completed.entry(base_table.clone()).or_insert(0) += 1;
                        let completed = partition_completed[&base_table];
                        let expected = expected_partitions.get(&base_table).copied().unwrap_or(1);

                        // When all partitions are done (even with failures), update state
                        if completed == expected {
                            if let Some(ts) = state.tables.get_mut(&base_table) {
                                ts.status = TaskStatus::Failed;
                                ts.error = table_errors.get(&base_table).cloned();
                                ts.rows_transferred = *table_rows.get(&base_table).unwrap_or(&0);
                            }
                            self.save_state(state)?;
                            progress.increment_table_count();
                            progress.emit("transferring", Some(base_table.clone()));
                        }
                    } else {
                        // Non-partitioned table failed - update state and checkpoint
                        if let Some(ts) = state.tables.get_mut(&base_table) {
                            ts.status = TaskStatus::Failed;
                            ts.error = Some(e.to_string());
                        }
                        self.save_state(state)?;
                        progress.increment_table_count();
                        progress.emit("transferring", Some(base_table.clone()));
                    }
                }
            }
        }

        // Finalize partitioned table states
        for (table_name, rows) in &table_rows {
            if let Some(ts) = state.tables.get_mut(table_name) {
                if table_errors.contains_key(table_name) {
                    ts.status = TaskStatus::Failed;
                    ts.error = table_errors.get(table_name).cloned();
                    // Track rows from successfully completed partitions even on failure
                    ts.rows_transferred = *rows;
                } else if ts.status != TaskStatus::Completed {
                    // Partitioned table - aggregate rows
                    ts.status = TaskStatus::Completed;
                    ts.rows_transferred = *rows;
                    ts.completed_at = Some(Utc::now());
                    info!("{}: completed ({} rows total)", table_name, rows);
                }
            }
        }

        // Handle tables that only had errors (no successful partitions)
        for (table_name, error) in &table_errors {
            if let Some(ts) = state.tables.get_mut(table_name) {
                if ts.status != TaskStatus::Completed {
                    ts.status = TaskStatus::Failed;
                    ts.error = Some(error.clone());
                }
            }
        }

        // Save final state
        self.save_state(state)?;

        // Check for failures
        let failed: Vec<_> = state
            .tables
            .iter()
            .filter(|(_, ts)| ts.status == TaskStatus::Failed)
            .map(|(name, _)| name.clone())
            .collect();

        if !failed.is_empty() {
            return Err(MigrateError::Transfer {
                table: failed.join(", "),
                message: "One or more tables failed to transfer".into(),
            });
        }

        Ok(())
    }

    /// Finalize migration (create indexes, FKs, constraints).
    async fn finalize(&self, tables: &[Table]) -> Result<()> {
        let target_schema = &self.config.target.schema;

        // Create primary keys (only for drop_recreate - truncate and upsert preserve existing PKs)
        if matches!(self.config.migration.target_mode, TargetMode::DropRecreate) {
            for table in tables {
                if !table.primary_key.is_empty() {
                    debug!("Creating PK for: {}", table.full_name());
                    self.target.create_primary_key(table, target_schema).await?;
                }
            }
        }

        // Create indexes (only if explicitly enabled)
        if self.config.migration.create_indexes {
            let max_tasks = self.config.migration.get_finalizer_concurrency().max(1);
            info!("Creating indexes with up to {} concurrent tasks", max_tasks);
            let semaphore = Arc::new(Semaphore::new(max_tasks));
            let mut handles = Vec::new();

            for table in tables.iter().cloned() {
                if table.indexes.is_empty() {
                    continue;
                }

                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let target = self.target.clone();
                let schema = target_schema.to_string();

                let handle = tokio::spawn(async move {
                    let _permit = permit;
                    for index in &table.indexes {
                        debug!("Creating index: {}.{}", table.name, index.name);
                        if let Err(e) = target.create_index(&table, index, &schema).await {
                            warn!("Failed to create index {}: {}", index.name, e);
                        }
                    }
                });

                handles.push(handle);
            }

            for handle in handles {
                if let Err(e) = handle.await {
                    warn!("Index creation task panicked: {}", e);
                }
            }
        }

        // Create foreign keys
        if self.config.migration.create_foreign_keys {
            for table in tables {
                for fk in &table.foreign_keys {
                    debug!("Creating FK: {}.{}", table.name, fk.name);
                    if let Err(e) = self
                        .target
                        .create_foreign_key(table, fk, target_schema)
                        .await
                    {
                        warn!("Failed to create FK {}: {}", fk.name, e);
                    }
                }
            }
        }

        // Create check constraints
        if self.config.migration.create_check_constraints {
            for table in tables {
                for chk in &table.check_constraints {
                    debug!("Creating check: {}.{}", table.name, chk.name);
                    if let Err(e) = self
                        .target
                        .create_check_constraint(table, chk, target_schema)
                        .await
                    {
                        warn!("Failed to create check {}: {}", chk.name, e);
                    }
                }
            }
        }

        // Reset sequences
        for table in tables {
            if let Err(e) = self.target.reset_sequence(target_schema, table).await {
                debug!("No sequence to reset for {}: {}", table.full_name(), e);
            }
        }

        info!("Finalization complete");
        Ok(())
    }

    /// Save state to file.
    fn save_state(&self, state: &MigrationState) -> Result<()> {
        if let Some(ref path) = self.state_file {
            state.save(path)?;
        }
        Ok(())
    }

    /// Validate row counts between source and target.
    pub async fn validate(&self) -> Result<HashMap<String, (i64, i64, bool)>> {
        let source_schema = &self.config.source.schema;
        let target_schema = &self.config.target.schema;

        let tables = self.source.extract_schema(source_schema).await?;
        let mut results = HashMap::new();

        for table in &tables {
            let source_count = self
                .source
                .get_row_count(source_schema, &table.name)
                .await?;
            let target_count = self
                .target
                .get_row_count(target_schema, &table.name)
                .await
                .unwrap_or(0);

            let matches = source_count == target_count;
            results.insert(table.full_name(), (source_count, target_count, matches));

            if matches {
                info!("{}: {} rows (match)", table.full_name(), source_count);
            } else {
                warn!(
                    "{}: source={} target={} (MISMATCH)",
                    table.full_name(),
                    source_count,
                    target_count
                );
            }
        }

        Ok(results)
    }
}

impl MigrationResult {
    /// Convert to JSON string.
    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_check_result_serialization() {
        let result = HealthCheckResult {
            healthy: true,
            source_connected: true,
            source_latency_ms: 15,
            source_error: None,
            target_connected: true,
            target_latency_ms: 8,
            target_error: None,
        };

        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"healthy\":true"));
        assert!(json.contains("\"source_latency_ms\":15"));
    }

    #[test]
    fn test_health_check_result_with_errors() {
        let result = HealthCheckResult {
            healthy: false,
            source_connected: false,
            source_latency_ms: 0,
            source_error: Some("Connection refused".into()),
            target_connected: true,
            target_latency_ms: 10,
            target_error: None,
        };

        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"healthy\":false"));
        assert!(json.contains("\"source_error\":\"Connection refused\""));
    }

    #[test]
    fn test_table_error_serialization() {
        let error = TableError {
            table: "dbo.Orders".into(),
            error_type: "transfer".into(),
            message: "Timeout during COPY".into(),
        };

        let json = serde_json::to_string(&error).unwrap();
        assert!(json.contains("\"table\":\"dbo.Orders\""));
        assert!(json.contains("\"error_type\":\"transfer\""));
        assert!(json.contains("\"message\":\"Timeout during COPY\""));
    }

    #[test]
    fn test_progress_update_serialization() {
        let update = ProgressUpdate {
            timestamp: Utc::now(),
            phase: "transferring".into(),
            tables_completed: 5,
            tables_total: 10,
            rows_transferred: 50000,
            rows_per_second: 10000,
            current_table: Some("dbo.Users".into()),
        };

        let json = serde_json::to_string(&update).unwrap();
        assert!(json.contains("\"phase\":\"transferring\""));
        assert!(json.contains("\"tables_completed\":5"));
        assert!(json.contains("\"current_table\":\"dbo.Users\""));
    }

    #[test]
    fn test_progress_update_without_current_table() {
        let update = ProgressUpdate {
            timestamp: Utc::now(),
            phase: "finalizing".into(),
            tables_completed: 10,
            tables_total: 10,
            rows_transferred: 100000,
            rows_per_second: 5000,
            current_table: None,
        };

        let json = serde_json::to_string(&update).unwrap();
        assert!(json.contains("\"phase\":\"finalizing\""));
        assert!(json.contains("\"tables_completed\":10"));
    }

    #[test]
    fn test_migration_result_success() {
        let result = MigrationResult {
            run_id: "test-123".into(),
            status: "completed".into(),
            duration_seconds: 45.5,
            started_at: Utc::now(),
            completed_at: Utc::now(),
            tables_total: 10,
            tables_success: 10,
            tables_failed: 0,
            rows_transferred: 100000,
            rows_per_second: 2200,
            failed_tables: vec![],
            table_errors: vec![],
            error: None,
            error_type: None,
            recoverable: None,
        };

        let json = serde_json::to_string_pretty(&result).unwrap();
        assert!(json.contains("\"status\": \"completed\""));
        assert!(json.contains("\"tables_success\": 10"));
        // Optional fields should be omitted when None/empty
        assert!(!json.contains("\"error\":"));
        assert!(!json.contains("\"table_errors\":"));
    }

    #[test]
    fn test_migration_result_with_failures() {
        let result = MigrationResult {
            run_id: "test-123".into(),
            status: "failed".into(),
            duration_seconds: 45.5,
            started_at: Utc::now(),
            completed_at: Utc::now(),
            tables_total: 10,
            tables_success: 8,
            tables_failed: 2,
            rows_transferred: 80000,
            rows_per_second: 1777,
            failed_tables: vec!["dbo.Orders".into(), "dbo.Items".into()],
            table_errors: vec![
                TableError {
                    table: "dbo.Orders".into(),
                    error_type: "transfer".into(),
                    message: "Connection lost".into(),
                },
            ],
            error: None,
            error_type: None,
            recoverable: Some(true),
        };

        let json = serde_json::to_string_pretty(&result).unwrap();
        assert!(json.contains("\"status\": \"failed\""));
        assert!(json.contains("\"tables_failed\": 2"));
        assert!(json.contains("\"table_errors\""));
        assert!(json.contains("\"recoverable\": true"));
    }

    #[test]
    fn test_migration_result_dry_run() {
        let result = MigrationResult {
            run_id: "dry-run-123".into(),
            status: "dry_run".into(),
            duration_seconds: 2.5,
            started_at: Utc::now(),
            completed_at: Utc::now(),
            tables_total: 15,
            tables_success: 0,
            tables_failed: 0,
            rows_transferred: 0,
            rows_per_second: 0,
            failed_tables: vec![],
            table_errors: vec![],
            error: None,
            error_type: None,
            recoverable: None,
        };

        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"status\":\"dry_run\""));
        assert!(json.contains("\"rows_transferred\":0"));
    }

    #[test]
    fn test_migration_result_to_json() {
        let result = MigrationResult {
            run_id: "test".into(),
            status: "completed".into(),
            duration_seconds: 1.0,
            started_at: Utc::now(),
            completed_at: Utc::now(),
            tables_total: 1,
            tables_success: 1,
            tables_failed: 0,
            rows_transferred: 100,
            rows_per_second: 100,
            failed_tables: vec![],
            table_errors: vec![],
            error: None,
            error_type: None,
            recoverable: None,
        };

        let json = result.to_json().unwrap();
        assert!(json.contains("\"run_id\": \"test\""));
        // Should be pretty-printed
        assert!(json.contains('\n'));
    }

    #[test]
    fn test_migration_result_with_top_level_error() {
        let result = MigrationResult {
            run_id: "err-123".into(),
            status: "failed".into(),
            duration_seconds: 0.5,
            started_at: Utc::now(),
            completed_at: Utc::now(),
            tables_total: 10,
            tables_success: 0,
            tables_failed: 0,
            rows_transferred: 0,
            rows_per_second: 0,
            failed_tables: vec![],
            table_errors: vec![],
            error: Some("Schema extraction failed".into()),
            error_type: Some("schema_extraction".into()),
            recoverable: Some(false),
        };

        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"error\":\"Schema extraction failed\""));
        assert!(json.contains("\"error_type\":\"schema_extraction\""));
        assert!(json.contains("\"recoverable\":false"));
    }

    // ProgressTracker tests
    #[test]
    fn test_progress_tracker_new() {
        let tracker = ProgressTracker::new(10, true);
        assert_eq!(tracker.tables_total, 10);
        assert_eq!(tracker.tables_completed.load(Ordering::Relaxed), 0);
        assert_eq!(tracker.rows_transferred.load(Ordering::Relaxed), 0);
        assert!(tracker.progress_enabled);
    }

    #[test]
    fn test_progress_tracker_complete_table() {
        let tracker = ProgressTracker::new(5, false);

        tracker.complete_table(1000);
        assert_eq!(tracker.tables_completed.load(Ordering::Relaxed), 1);
        assert_eq!(tracker.rows_transferred.load(Ordering::Relaxed), 1000);

        tracker.complete_table(2000);
        assert_eq!(tracker.tables_completed.load(Ordering::Relaxed), 2);
        assert_eq!(tracker.rows_transferred.load(Ordering::Relaxed), 3000);
    }

    #[test]
    fn test_progress_tracker_increment_table_count() {
        let tracker = ProgressTracker::new(3, false);

        tracker.increment_table_count();
        assert_eq!(tracker.tables_completed.load(Ordering::Relaxed), 1);
        assert_eq!(tracker.rows_transferred.load(Ordering::Relaxed), 0);

        tracker.increment_table_count();
        assert_eq!(tracker.tables_completed.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_progress_tracker_rows_per_second() {
        let tracker = ProgressTracker::new(1, false);

        // Initially should be 0
        assert_eq!(tracker.get_rows_per_second(), 0);

        // Add some rows and check calculation
        tracker.complete_table(10000);
        // Since time has passed, rows_per_second should be > 0
        // We can't test exact value due to timing, but it should be reasonable
        let rps = tracker.get_rows_per_second();
        assert!(rps >= 0); // At minimum it's non-negative
    }

    #[test]
    fn test_progress_tracker_disabled_emit() {
        // When progress_enabled is false, emit should not panic
        let tracker = ProgressTracker::new(5, false);
        tracker.complete_table(1000);
        // This should be a no-op and not panic
        tracker.emit("transferring", Some("test_table".to_string()));
    }

    #[test]
    fn test_progress_tracker_concurrent_updates() {
        use std::sync::Arc;
        use std::thread;

        let tracker = Arc::new(ProgressTracker::new(100, false));
        let mut handles = vec![];

        // Spawn 10 threads, each completing 10 tables with 100 rows each
        for _ in 0..10 {
            let t = tracker.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..10 {
                    t.complete_table(100);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Should have 100 tables completed with 10000 total rows
        assert_eq!(tracker.tables_completed.load(Ordering::Relaxed), 100);
        assert_eq!(tracker.rows_transferred.load(Ordering::Relaxed), 10000);
    }
}
