//! Migration orchestrator - main workflow coordinator.

mod pools;

pub use pools::{SourcePoolImpl, TargetPoolImpl};

use crate::config::{Config, TableStats, TargetMode};
use crate::core::catalog::DriverCatalog;
use crate::drivers::{SourceReaderImpl, TargetWriterImpl};
use crate::error::{MigrateError, Result};
use crate::core::schema::Table;
use crate::state::{MigrationState, RunStatus, StateBackendEnum, TableState, TaskStatus};
use crate::transfer::{DateFilter, TransferConfig, TransferEngine, TransferJob};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Migration orchestrator.
pub struct Orchestrator {
    config: Config,
    state_backend: StateBackendEnum,
    state: Option<MigrationState>,
    source: SourcePoolImpl,
    target: TargetPoolImpl,
    /// Driver catalog for creating readers/writers with the new plugin architecture.
    catalog: DriverCatalog,
    progress_enabled: bool,
    progress_tx: Option<mpsc::Sender<ProgressUpdate>>,
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

    /// Total rows skipped (unchanged in upsert mode with hash detection).
    #[serde(default)]
    pub rows_skipped: i64,

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

    /// Total rows to transfer (across all tables).
    pub total_rows: i64,

    /// Current throughput (rows/second).
    pub rows_per_second: i64,

    /// Table currently being processed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_table: Option<String>,

    /// Table status: "started" or "completed" (when current_table is set).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_status: Option<String>,
}

/// Write a progress update as JSON to stderr.
fn emit_progress_json(update: &ProgressUpdate) {
    if let Ok(json) = serde_json::to_string(update) {
        let mut stderr = std::io::stderr().lock();
        let _ = writeln!(stderr, "{}", json);
    }
}

/// Thread-safe progress tracker for concurrent table transfers.
struct ProgressTracker {
    tables_completed: AtomicUsize,
    tables_total: usize,
    /// Rows transferred - shared with TransferEngine for real-time updates.
    rows_transferred: Arc<AtomicI64>,
    /// Total rows across all tables.
    total_rows: i64,
    started_at: Instant,
    progress_enabled: bool,
    progress_tx: Option<mpsc::Sender<ProgressUpdate>>,
}

impl std::fmt::Debug for ProgressTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProgressTracker")
            .field("tables_completed", &self.tables_completed)
            .field("tables_total", &self.tables_total)
            .field("rows_transferred", &self.rows_transferred)
            .field("total_rows", &self.total_rows)
            .field("progress_enabled", &self.progress_enabled)
            .field("has_progress_tx", &self.progress_tx.is_some())
            .finish()
    }
}

impl ProgressTracker {
    fn new(
        tables_total: usize,
        total_rows: i64,
        progress_enabled: bool,
        progress_tx: Option<mpsc::Sender<ProgressUpdate>>,
    ) -> Self {
        Self {
            tables_completed: AtomicUsize::new(0),
            tables_total,
            rows_transferred: Arc::new(AtomicI64::new(0)),
            total_rows,
            started_at: Instant::now(),
            progress_enabled,
            progress_tx,
        }
    }

    /// Get a clone of the shared rows counter for the transfer engine.
    fn rows_counter(&self) -> Arc<AtomicI64> {
        self.rows_transferred.clone()
    }

    fn complete_table(&self, _rows: i64) {
        // Rows are counted incrementally by the transfer engine via the shared counter.
        // This method just tracks table completion. The rows parameter is ignored since
        // the transfer engine has already counted them in real-time.
        self.tables_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Manually add rows to the counter (for testing or non-transfer scenarios).
    #[cfg(test)]
    fn add_rows(&self, rows: i64) {
        self.rows_transferred.fetch_add(rows, Ordering::Relaxed);
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

    fn emit(&self, phase: &str, current_table: Option<String>, table_status: Option<&str>) {
        if !self.progress_enabled && self.progress_tx.is_none() {
            return;
        }

        let update = ProgressUpdate {
            timestamp: Utc::now(),
            phase: phase.to_string(),
            tables_completed: self.tables_completed.load(Ordering::Relaxed),
            tables_total: self.tables_total,
            rows_transferred: self.rows_transferred.load(Ordering::Relaxed),
            total_rows: self.total_rows,
            rows_per_second: self.get_rows_per_second(),
            current_table,
            table_status: table_status.map(|s| s.to_string()),
        };

        if self.progress_enabled {
            emit_progress_json(&update);
        }

        if let Some(tx) = &self.progress_tx {
            let _ = tx.try_send(update);
        }
    }
}

impl Orchestrator {
    /// Create a new orchestrator.
    pub async fn new(config: Config) -> Result<Self> {
        // Initialize driver catalog with built-in drivers
        let catalog = DriverCatalog::with_builtins();

        // Create source pool based on configured type
        let source = SourcePoolImpl::from_config(&config).await?;
        info!("Connected to {} source database", source.db_type());

        // Create target pool based on configured type
        let target = TargetPoolImpl::from_config(&config).await?;
        info!("Connected to {} target database", target.db_type());

        // Initialize database state backend (supports both PostgreSQL and MSSQL)
        let state_backend = target.create_state_backend()?;
        state_backend.init_schema().await?;
        info!("Initialized migration state schema in target database");

        Ok(Self {
            config,
            state_backend,
            state: None,
            source,
            target,
            catalog,
            progress_enabled: false,
            progress_tx: None,
        })
    }

    /// Enable progress reporting to stderr as JSON lines.
    pub fn with_progress(mut self, enabled: bool) -> Self {
        self.progress_enabled = enabled;
        self
    }

    /// Set a channel for progress updates (used by TUI).
    pub fn with_progress_channel(mut self, tx: mpsc::Sender<ProgressUpdate>) -> Self {
        self.progress_tx = Some(tx);
        self
    }

    /// Emit a progress update (for phase transitions before transfer starts).
    fn emit_progress(&self, phase: &str, tables_total: usize, total_rows: i64) {
        if !self.progress_enabled && self.progress_tx.is_none() {
            return;
        }

        let update = ProgressUpdate {
            timestamp: Utc::now(),
            phase: phase.to_string(),
            tables_completed: 0,
            tables_total,
            rows_transferred: 0,
            total_rows,
            rows_per_second: 0,
            current_table: None,
            table_status: None,
        };

        if self.progress_enabled {
            emit_progress_json(&update);
        }

        if let Some(tx) = &self.progress_tx {
            let _ = tx.try_send(update);
        }
    }

    /// Load existing state for resume from database.
    pub async fn resume(mut self) -> Result<Self> {
        let config_hash = self.config.hash();

        // Try to load latest state from database
        if let Some(state) = self.state_backend.load_latest(&config_hash).await? {
            info!(
                "Resuming from database state: run_id={}, started={}",
                state.run_id,
                state.started_at.format("%Y-%m-%d %H:%M:%S")
            );
            self.state = Some(state);
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

    /// Get a reference to the source connection pool.
    pub fn source_pool(&self) -> SourcePoolImpl {
        self.source.clone()
    }

    /// Get a reference to the target connection pool.
    pub fn target_pool(&self) -> TargetPoolImpl {
        self.target.clone()
    }

    /// Get a reference to the driver catalog.
    pub fn catalog(&self) -> &DriverCatalog {
        &self.catalog
    }

    /// Create a source reader using the new driver architecture.
    ///
    /// This creates a new reader instance using `DriverCatalog`. Use this for
    /// data transfer operations that benefit from the new streaming interface.
    pub async fn create_source_reader(&self, max_conns: usize) -> Result<SourceReaderImpl> {
        self.catalog
            .create_reader(&self.config.source, max_conns)
            .await
    }

    /// Create a target writer using the new driver architecture.
    ///
    /// This creates a new writer instance using `DriverCatalog`. The writer
    /// will have the appropriate type mapper configured for the source→target
    /// database combination.
    pub async fn create_target_writer(&self, max_conns: usize) -> Result<TargetWriterImpl> {
        let source_db_type = DriverCatalog::normalize_db_type(&self.config.source.r#type)?;
        self.catalog
            .create_writer(
                &self.config.target,
                max_conns,
                source_db_type,
                #[cfg(feature = "mysql")]
                self.config.migration.mysql_load_data,
            )
            .await
    }

    /// Extract schema from the source database.
    pub async fn extract_schema(&self) -> Result<Vec<Table>> {
        let tables = self
            .source
            .extract_schema(&self.config.source.schema)
            .await?;

        // Filter tables based on include/exclude patterns
        let filtered: Vec<_> = tables
            .into_iter()
            .filter(|t| {
                let table_names = vec![t.name.clone()];
                !self.config.migration.filter_tables(&table_names).is_empty()
            })
            .collect();

        Ok(filtered)
    }

    /// Run the migration.
    /// If dry_run is true, only validates and shows what would happen without transferring data.
    ///
    /// **Important:** The caller should call `close()` after this method returns,
    /// regardless of success or failure, to ensure database connections are properly closed.
    /// Connection cleanup is handled automatically within this method on successful completion,
    /// but early errors may bypass cleanup.
    pub async fn run(
        &mut self,
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
        self.emit_progress("extracting_schema", 0, 0);
        let mut tables = self
            .source
            .extract_schema(&self.config.source.schema)
            .await?;

        // Apply table filters (include_tables / exclude_tables)
        let original_count = tables.len();
        if !self.config.migration.include_tables.is_empty()
            || !self.config.migration.exclude_tables.is_empty()
        {
            let table_names: Vec<String> = tables.iter().map(|t| t.full_name()).collect();
            let filtered_names = self.config.migration.filter_tables(&table_names);
            // Use HashSet for O(1) lookup instead of Vec::contains()
            let filtered_set: std::collections::HashSet<String> =
                filtered_names.into_iter().collect();
            tables.retain(|t| filtered_set.contains(&t.full_name()));

            if tables.len() < original_count {
                info!(
                    "Filtered tables: {} -> {} (include: {:?}, exclude: {:?})",
                    original_count,
                    tables.len(),
                    self.config.migration.include_tables,
                    self.config.migration.exclude_tables
                );
            }

            if tables.is_empty() {
                warn!(
                    "All {} tables were filtered out by include/exclude patterns. \
                     Check your include_tables ({:?}) and exclude_tables ({:?}) configuration.",
                    original_count,
                    self.config.migration.include_tables,
                    self.config.migration.exclude_tables
                );
            }
        }

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
        let total_rows: i64 = tables.iter().map(|t| t.row_count).sum();
        self.emit_progress("schema_extracted", tables.len(), total_rows);

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
        // In drop_recreate mode: always reset table state (tables are dropped, no resume)
        // In upsert mode: preserve existing state (for crash recovery)
        let is_drop_recreate = self.config.migration.target_mode == TargetMode::DropRecreate;
        for table in &tables {
            let table_name = table.full_name();
            if is_drop_recreate {
                // Always reset state in drop_recreate mode
                state
                    .tables
                    .insert(table_name.clone(), TableState::new(table.row_count));
            } else {
                // Preserve existing state in upsert mode
                state
                    .tables
                    .entry(table_name.clone())
                    .or_insert_with(|| TableState::new(table.row_count));
            }
        }

        // Phase 2: Prepare target (skip in dry-run)
        if !dry_run {
            info!(
                "Phase 2: Preparing target database (mode: {:?})",
                self.config.migration.target_mode
            );
            self.emit_progress("preparing_target", tables.len(), total_rows);
            self.prepare_target(&tables, &mut state).await?;

            // Save state after preparation
            self.save_state(&state).await?;
        } else {
            info!(
                "Phase 2: [DRY RUN] Would prepare target database (mode: {:?})",
                self.config.migration.target_mode
            );
        }

        // Phase 3: Transfer data (skip in dry-run)
        // For upsert mode: stream all rows, let PostgreSQL handle comparison via ON CONFLICT
        // For other modes: full transfer of all rows
        let transfer_result = if !dry_run {
            let is_upsert = matches!(self.config.migration.target_mode, TargetMode::Upsert);

            if is_upsert {
                info!("Phase 3: Upsert (streaming all rows, PostgreSQL handles comparison)");
            } else {
                info!("Phase 3: Transferring data");
            }
            self.emit_progress("transferring", tables.len(), total_rows);
            self.transfer_data(&tables, &mut state, &cancel).await
        } else {
            let is_upsert = matches!(self.config.migration.target_mode, TargetMode::Upsert);

            if is_upsert {
                info!("Phase 3: [DRY RUN] Would upsert (stream all, PostgreSQL compares)");
            } else {
                info!(
                    "Phase 3: [DRY RUN] Would transfer data for {} tables",
                    tables.len()
                );
            }
            Ok(())
        };

        // Phase 4: Finalize (only on success, skip in dry-run)
        if transfer_result.is_ok() && !dry_run {
            info!("Phase 4: Finalizing (indexes, constraints)");
            self.emit_progress("finalizing", tables.len(), total_rows);
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
        let mut rows_skipped: i64 = 0;

        for (name, table_state) in &state.tables {
            match table_state.status {
                TaskStatus::Completed => {
                    tables_success += 1;
                    rows_transferred += table_state.rows_transferred;
                    rows_skipped += table_state.rows_skipped;
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
            self.save_state(&state).await?;
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
            rows_skipped,
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

        // Close database connections before returning (ensures clean shutdown)
        self.close().await;

        transfer_result?;

        Ok(result)
    }

    /// Prepare target database based on mode.
    async fn prepare_target(&self, tables: &[Table], _state: &mut MigrationState) -> Result<()> {
        let target_schema = &self.config.target.schema;

        // Create schema if needed
        self.target.create_schema(target_schema).await?;

        match self.config.migration.target_mode {
            TargetMode::DropRecreate => {
                // Drop and recreate all tables
                for (i, table) in tables.iter().enumerate() {
                    let table_name = table.full_name();
                    info!("Preparing table {}/{}: {}", i + 1, tables.len(), table_name);
                    self.target.drop_table(target_schema, &table.name).await?;

                    if self.config.migration.use_unlogged_tables {
                        self.target
                            .create_table_unlogged(table, target_schema)
                            .await?;
                    } else {
                        self.target.create_table(table, target_schema).await?;
                    }
                }
            }
            TargetMode::Upsert => {
                // For upsert, ensure tables exist and drop non-PK indexes for performance
                for (i, table) in tables.iter().enumerate() {
                    let table_name = table.full_name();
                    info!("Preparing table {}/{}: {}", i + 1, tables.len(), table_name);

                    // Check for primary key (required for upsert)
                    if table.primary_key.is_empty() {
                        return Err(MigrateError::NoPrimaryKey(table_name));
                    }

                    if !self.target.table_exists(target_schema, &table.name).await? {
                        if self.config.migration.use_unlogged_tables {
                            self.target
                                .create_table_unlogged(table, target_schema)
                                .await?;
                        } else {
                            self.target.create_table(table, target_schema).await?;
                        }
                        self.target.create_primary_key(table, target_schema).await?;
                    } else {
                        // For existing tables, ensure primary key exists (required for upsert)
                        if !self
                            .target
                            .has_primary_key(target_schema, &table.name)
                            .await?
                        {
                            info!(
                                "Adding missing primary key to existing table {}",
                                table_name
                            );
                            self.target.create_primary_key(table, target_schema).await?;
                        }

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
        // Calculate total expected jobs for auto-tuning workers
        let total_jobs: usize = tables.iter().map(|t| self.partition_count(t)).sum();
        let configured_workers = self.config.migration.get_workers();

        // Cap workers at connection pool limits to avoid exhausting connections
        // Use the smaller pool as the limit (each job needs both source and target connections)
        let max_by_connections = self
            .config
            .migration
            .get_max_pg_connections()
            .min(self.config.migration.get_max_mssql_connections());

        // Auto-tune workers: ensure we have enough permits to spawn all jobs without blocking
        // If workers < total_jobs, the semaphore would block job spawning, serializing tables
        // But cap at connection pool limits to avoid exhausting database connections
        let workers = if total_jobs > configured_workers {
            let auto_tuned = total_jobs.min(max_by_connections);
            if auto_tuned > configured_workers {
                info!(
                    "Auto-tuning workers: {} -> {} (for {} partition jobs, capped by {} max connections)",
                    configured_workers,
                    auto_tuned,
                    total_jobs,
                    max_by_connections
                );
            }
            auto_tuned
        } else {
            configured_workers.min(max_by_connections)
        };

        let semaphore = Arc::new(Semaphore::new(workers));

        // Create progress tracker
        let total_rows: i64 = tables.iter().map(|t| t.row_count).sum();
        let progress = Arc::new(ProgressTracker::new(
            tables.len(),
            total_rows,
            self.progress_enabled,
            self.progress_tx.clone(),
        ));

        // Spawn periodic progress emitter (updates every 500ms during transfer)
        let progress_emitter = progress.clone();
        let progress_cancel = cancel.clone();
        let progress_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        progress_emitter.emit("transferring", None, None);
                    }
                    _ = progress_cancel.cancelled() => {
                        break;
                    }
                }
            }
        });

        let transfer_config = TransferConfig {
            chunk_size: self.config.migration.get_chunk_size(),
            read_ahead: self.config.migration.get_read_ahead_buffers(),
            parallel_readers: self.config.migration.get_parallel_readers(),
            parallel_writers: self.config.migration.get_write_ahead_writers(),
            use_copy_binary: true, // Enable COPY TO BINARY for PostgreSQL sources
            use_direct_copy: false, // Disabled - upsert writers don't support direct copy yet
            compress_text: self.config.migration.compress_text,
        };

        let engine = Arc::new(
            TransferEngine::new(self.source.clone(), self.target.clone(), transfer_config)
                .with_progress_counter(progress.rows_counter()),
        );

        // Check if date-based incremental sync is enabled (upsert mode + date_updated_columns configured)
        let date_incremental_enabled = self.config.migration.target_mode == TargetMode::Upsert
            && !self.config.migration.date_updated_columns.is_empty();

        // Filter tables that need processing
        // - drop_recreate mode: always process all tables (tables are dropped anyway)
        // - incremental sync (upsert + date columns): process all tables (using date filters)
        // - upsert without date columns: skip completed tables (crash recovery only)
        let is_drop_recreate = self.config.migration.target_mode == TargetMode::DropRecreate;
        let pending_tables: Vec<_> = tables
            .iter()
            .filter(|t| {
                if is_drop_recreate || date_incremental_enabled {
                    // drop_recreate or incremental sync: process all tables
                    true
                } else {
                    // Upsert crash recovery: skip completed tables
                    let table_name = t.full_name();
                    state
                        .tables
                        .get(&table_name)
                        .map(|ts| ts.status != TaskStatus::Completed)
                        .unwrap_or(true)
                }
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

        // Track sync start times per table (for updating watermark after successful completion)
        let mut table_sync_start_times: HashMap<String, DateTime<Utc>> = HashMap::new();

        // Track incremental sync statistics for summary logging
        let mut tables_incremental: usize = 0;
        let mut tables_first_sync: usize = 0;
        let mut tables_no_date_column: usize = 0;

        for table in pending_tables {
            // Check for cancellation
            if cancel.is_cancelled() {
                info!("Cancellation requested, stopping new transfers");
                break;
            }

            let table_name = table.full_name();

            // Record sync start time for watermarking (capture before transfer starts)
            // This enables incremental sync later, even if switching from drop_recreate to upsert
            let sync_start_time = Utc::now();
            table_sync_start_times.insert(table_name.clone(), sync_start_time);

            // Determine date filter for incremental sync (if enabled)
            let date_filter = if date_incremental_enabled {
                // Try to find a matching date column in this table
                if let Some((col_name, col_type)) =
                    table.find_date_column(&self.config.migration.date_updated_columns)
                {
                    // Get last sync timestamp from database (queries historical completed runs)
                    let last_sync = self
                        .state_backend
                        .get_last_sync_timestamp(&table_name)
                        .await?;

                    if let Some(last_sync_ts) = last_sync {
                        // Incremental sync: only rows modified after last sync
                        match DateFilter::new(col_name.clone(), last_sync_ts) {
                            Ok(filter) => {
                                tables_incremental += 1;
                                info!(
                                    "{}: incremental sync from {} using {} ({})",
                                    table_name,
                                    last_sync_ts.to_rfc3339(),
                                    col_name,
                                    col_type
                                );
                                Some(filter)
                            }
                            Err(e) => {
                                tables_first_sync += 1;
                                warn!(
                                    "{}: invalid sync timestamp, falling back to full sync: {}",
                                    table_name, e
                                );
                                None
                            }
                        }
                    } else {
                        // First sync: full load, but record timestamp for next run
                        tables_first_sync += 1;
                        info!(
                            "{}: first sync (full load), date column {} found",
                            table_name, col_name
                        );
                        None
                    }
                } else {
                    // No matching date column found, use full sync
                    tables_no_date_column += 1;
                    info!(
                        "{}: no matching date column (configured: {:?}), using full sync",
                        table_name, self.config.migration.date_updated_columns
                    );
                    None
                }
            } else {
                None
            };

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
                        self.save_state(state).await?;

                        // Emit "started" progress event for the table
                        progress.emit("transferring", Some(table_name.clone()), Some("started"));

                        // Create a job for each partition
                        // IMPORTANT: Acquire semaphore INSIDE the spawned task, not before spawning.
                        // This allows all partition jobs to be spawned immediately, with the
                        // semaphore controlling concurrent execution rather than blocking spawning.
                        for partition in partitions {
                            // No adjustment needed: the transfer engine now uses >= for first chunk,
                            // which correctly includes the partition's min_pk boundary.
                            let job = TransferJob {
                                table: table.clone(),
                                partition_id: Some(partition.partition_id),
                                min_pk: partition.min_pk,
                                max_pk: partition.max_pk,
                                resume_from_pk: None, // Partitions don't support resume yet
                                target_mode: self.config.migration.target_mode,
                                target_schema: self.config.target.schema.clone(),
                                date_filter: date_filter.clone(),
                            };

                            let engine_clone = engine.clone();
                            let semaphore_clone = semaphore.clone();
                            let partition_name =
                                format!("{}:p{}", table_name, partition.partition_id);

                            join_set.spawn(async move {
                                // Acquire permit inside the task - this allows all jobs to be
                                // spawned immediately while controlling concurrent execution
                                let _permit = semaphore_clone.acquire_owned().await.unwrap();
                                let result = engine_clone.execute(job).await;
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
            // Update state
            if let Some(ts) = state.tables.get_mut(&table_name) {
                ts.status = TaskStatus::InProgress;
            }
            self.save_state(state).await?;

            // Emit "started" progress event
            progress.emit("transferring", Some(table_name.clone()), Some("started"));

            let job = TransferJob {
                table: table.clone(),
                partition_id: None,
                min_pk: None,
                max_pk: None,
                resume_from_pk: state.tables.get(&table_name).and_then(|ts| ts.last_pk),
                target_mode: self.config.migration.target_mode,
                target_schema: self.config.target.schema.clone(),
                date_filter: date_filter.clone(),
            };

            let engine_clone = engine.clone();
            let semaphore_clone = semaphore.clone();
            let job_name = table_name.clone();

            join_set.spawn(async move {
                // Acquire permit inside the task for consistent behavior with partitioned tables
                let _permit = semaphore_clone.acquire_owned().await.unwrap();
                let result = engine_clone.execute(job).await;
                (job_name, result)
            });
        }

        // Log incremental sync summary if date-based sync is enabled
        if date_incremental_enabled {
            info!(
                "Sync mode summary: {} incremental, {} first-time full, {} no date column",
                tables_incremental, tables_first_sync, tables_no_date_column
            );
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
                info!(
                    "Cancellation detected, aborting remaining {} tasks",
                    join_set.len()
                );
                join_set.abort_all();
                // Save current state before breaking
                self.save_state(state).await?;
                break;
            }

            // Extract job result from JoinSet
            let (job_name, task_result) = match join_result {
                Ok(result) => result,
                Err(e) => {
                    // Distinguish between cancelled and panicked tasks
                    if e.is_cancelled() {
                        // Task was cancelled (e.g., by abort_all) - this is expected during shutdown
                        debug!("Task was cancelled");
                    } else {
                        // Task panicked - we can't recover the job name from JoinError
                        error!("Task panicked: {}", e);
                    }
                    continue;
                }
            };

            // Extract base table name (handles both "schema.table" and "schema.table:p1")
            let base_table = job_name.split(":p").next().unwrap_or(&job_name).to_string();
            let is_partitioned = job_name.contains(":p");

            // Process the result (success or failure)
            match &task_result {
                Ok(stats) => {
                    info!("{}: completed ({} rows)", job_name, stats.rows);
                    *table_rows.entry(base_table.clone()).or_insert(0) += stats.rows;
                }
                Err(e) => {
                    error!("{}: failed - {}", job_name, e);
                    // Preserve first error for this table (don't overwrite if partition already failed)
                    table_errors
                        .entry(base_table.clone())
                        .or_insert_with(|| e.to_string());
                }
            }

            // Track partition/table completion
            if is_partitioned {
                *partition_completed.entry(base_table.clone()).or_insert(0) += 1;
                let completed = partition_completed[&base_table];
                let expected = expected_partitions.get(&base_table).copied().unwrap_or(1);

                // When all partitions of a table are done, determine final status
                if completed == expected {
                    let total_rows = table_rows.get(&base_table).copied().unwrap_or(0);
                    let mut should_update_timestamp = false;

                    if let Some(ts) = state.tables.get_mut(&base_table) {
                        // Check for any errors across all partitions
                        if table_errors.contains_key(&base_table) {
                            ts.status = TaskStatus::Failed;
                            ts.error = table_errors.get(&base_table).cloned();
                        } else {
                            ts.status = TaskStatus::Completed;
                            ts.completed_at = Some(Utc::now());
                            should_update_timestamp = true;
                        }
                        ts.rows_transferred = total_rows;
                    }

                    // Update sync timestamp for incremental sync (after releasing table state borrow)
                    if should_update_timestamp {
                        if let Some(sync_start) = table_sync_start_times.get(&base_table) {
                            state.update_sync_timestamp(&base_table, *sync_start);
                            debug!(
                                "{}: updated sync timestamp to {}",
                                base_table,
                                sync_start.to_rfc3339()
                            );
                        }
                    }

                    self.save_state(state).await?;
                    // Progress tracking
                    if table_errors.contains_key(&base_table) {
                        progress.increment_table_count();
                    } else {
                        progress.complete_table(total_rows);
                    }
                    progress.emit("transferring", Some(base_table.clone()), Some("completed"));
                    // Yield to allow TUI to process the progress event
                    tokio::task::yield_now().await;
                }
            } else {
                // Non-partitioned table - update state directly based on result
                let is_success = task_result.is_ok();
                let rows = task_result.as_ref().map(|s| s.rows).unwrap_or(0);
                let mut should_update_timestamp = false;

                if let Some(ts) = state.tables.get_mut(&base_table) {
                    match task_result {
                        Ok(stats) => {
                            ts.status = TaskStatus::Completed;
                            ts.rows_transferred = stats.rows;
                            ts.last_pk = stats.last_pk;
                            ts.completed_at = Some(Utc::now());
                            should_update_timestamp = true;
                        }
                        Err(e) => {
                            ts.status = TaskStatus::Failed;
                            ts.error = Some(e.to_string());
                        }
                    }
                }

                // Update sync timestamp for incremental sync (after releasing table state borrow)
                if should_update_timestamp {
                    if let Some(sync_start) = table_sync_start_times.get(&base_table) {
                        state.update_sync_timestamp(&base_table, *sync_start);
                        debug!(
                            "{}: updated sync timestamp to {}",
                            base_table,
                            sync_start.to_rfc3339()
                        );
                    }
                }

                self.save_state(state).await?;
                if is_success {
                    progress.complete_table(rows);
                } else {
                    progress.increment_table_count();
                }
                progress.emit("transferring", Some(base_table.clone()), Some("completed"));
                // Yield to allow TUI to process the progress event
                tokio::task::yield_now().await;
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

        // Stop the periodic progress emitter
        progress_handle.abort();

        // Save final state
        self.save_state(state).await?;

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
    ///
    /// All operations are parallelized where possible:
    /// - Primary keys: parallel (no inter-table dependencies)
    /// - Indexes: parallel (no inter-table dependencies)
    /// - Foreign keys: parallel within dependency levels (topological order)
    /// - Check constraints: parallel (no inter-table dependencies)
    /// - Sequence resets: parallel (no inter-table dependencies)
    async fn finalize(&self, tables: &[Table]) -> Result<()> {
        let target_schema = &self.config.target.schema;
        let max_tasks = self.config.migration.get_finalizer_concurrency().max(1);
        let semaphore = Arc::new(Semaphore::new(max_tasks));

        // Create primary keys (only for drop_recreate - truncate and upsert preserve existing PKs)
        if matches!(self.config.migration.target_mode, TargetMode::DropRecreate) {
            let tables_with_pk: Vec<_> = tables
                .iter()
                .filter(|t| !t.primary_key.is_empty())
                .cloned()
                .collect();

            if !tables_with_pk.is_empty() {
                let pk_start = Instant::now();
                info!(
                    "Creating {} primary keys with up to {} concurrent tasks",
                    tables_with_pk.len(),
                    max_tasks
                );
                let mut handles = Vec::new();

                for table in tables_with_pk {
                    let permit = semaphore.clone().acquire_owned().await.unwrap();
                    let target = self.target.clone();
                    let schema = target_schema.to_string();

                    let handle = tokio::spawn(async move {
                        let _permit = permit;
                        let start = Instant::now();
                        let table_name = table.full_name();
                        let row_count = table.row_count;
                        match target.create_primary_key(&table, &schema).await {
                            Ok(_) => {
                                info!(
                                    "Created PK on {} ({} rows) in {:.2}s",
                                    table_name,
                                    row_count,
                                    start.elapsed().as_secs_f64()
                                );
                                Ok(())
                            }
                            Err(e) => {
                                warn!("Failed to create PK on {}: {}", table_name, e);
                                Err(format!("PK creation failed on {}: {}", table_name, e))
                            }
                        }
                    });
                    handles.push(handle);
                }

                let mut pk_errors = Vec::new();
                for handle in handles {
                    match handle.await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => pk_errors.push(e),
                        Err(e) => warn!("PK creation task panicked: {}", e),
                    }
                }
                if !pk_errors.is_empty() {
                    return Err(crate::error::MigrateError::Transfer {
                        table: "finalization".to_string(),
                        message: pk_errors.join("; "),
                    });
                }
                info!("Primary keys created in {:.2}s", pk_start.elapsed().as_secs_f64());
            }
        }

        // Create indexes (only if explicitly enabled)
        if self.config.migration.create_indexes {
            let tables_with_indexes: Vec<_> = tables
                .iter()
                .filter(|t| !t.indexes.is_empty())
                .cloned()
                .collect();

            if !tables_with_indexes.is_empty() {
                info!(
                    "Creating indexes on {} tables with up to {} concurrent tasks",
                    tables_with_indexes.len(),
                    max_tasks
                );
                let mut handles = Vec::new();

                for table in tables_with_indexes {
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
        }

        // Create foreign keys (parallel within dependency levels)
        if self.config.migration.create_foreign_keys {
            let levels = build_fk_dependency_levels(tables);
            let total_fks: usize = tables.iter().map(|t| t.foreign_keys.len()).sum();

            if total_fks > 0 {
                info!(
                    "Creating {} foreign keys in {} dependency levels",
                    total_fks,
                    levels.len()
                );

                for (level_idx, level_tables) in levels.iter().enumerate() {
                    debug!(
                        "Processing FK level {} with {} tables",
                        level_idx,
                        level_tables.len()
                    );
                    let mut handles = Vec::new();

                    for table in level_tables.iter().cloned() {
                        if table.foreign_keys.is_empty() {
                            continue;
                        }

                        let permit = semaphore.clone().acquire_owned().await.unwrap();
                        let target = self.target.clone();
                        let schema = target_schema.to_string();

                        let handle = tokio::spawn(async move {
                            let _permit = permit;
                            for fk in &table.foreign_keys {
                                debug!("Creating FK: {}.{}", table.name, fk.name);
                                if let Err(e) =
                                    target.create_foreign_key(&table, fk, &schema).await
                                {
                                    warn!("Failed to create FK {}: {}", fk.name, e);
                                }
                            }
                        });
                        handles.push(handle);
                    }

                    // Wait for all FKs in this level before proceeding to next
                    for handle in handles {
                        if let Err(e) = handle.await {
                            warn!("FK creation task panicked: {}", e);
                        }
                    }
                }
            }
        }

        // Create check constraints (parallel)
        if self.config.migration.create_check_constraints {
            let tables_with_checks: Vec<_> = tables
                .iter()
                .filter(|t| !t.check_constraints.is_empty())
                .cloned()
                .collect();

            if !tables_with_checks.is_empty() {
                info!(
                    "Creating check constraints on {} tables with up to {} concurrent tasks",
                    tables_with_checks.len(),
                    max_tasks
                );
                let mut handles = Vec::new();

                for table in tables_with_checks {
                    let permit = semaphore.clone().acquire_owned().await.unwrap();
                    let target = self.target.clone();
                    let schema = target_schema.to_string();

                    let handle = tokio::spawn(async move {
                        let _permit = permit;
                        for chk in &table.check_constraints {
                            debug!("Creating check: {}.{}", table.name, chk.name);
                            if let Err(e) =
                                target.create_check_constraint(&table, chk, &schema).await
                            {
                                warn!("Failed to create check {}: {}", chk.name, e);
                            }
                        }
                    });
                    handles.push(handle);
                }

                for handle in handles {
                    if let Err(e) = handle.await {
                        warn!("Check constraint task panicked: {}", e);
                    }
                }
            }
        }

        // Reset sequences (parallel)
        {
            let mut handles = Vec::new();

            for table in tables {
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let target = self.target.clone();
                let schema = target_schema.to_string();
                let table = table.clone();

                let handle = tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(e) = target.reset_sequence(&schema, &table).await {
                        debug!("No sequence to reset for {}: {}", table.full_name(), e);
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                if let Err(e) = handle.await {
                    warn!("Sequence reset task panicked: {}", e);
                }
            }
        }

        info!("Finalization complete");
        Ok(())
    }

    /// Save state to database.
    async fn save_state(&self, state: &MigrationState) -> Result<()> {
        self.state_backend.save(state).await
    }

    /// Explicitly close all database connections.
    ///
    /// This method closes both source and target connection pools, ensuring
    /// clean disconnection from databases. While pools will eventually be
    /// closed when dropped, calling this method explicitly ensures:
    /// - Connections are closed promptly rather than lingering
    /// - Any pending transactions are properly terminated
    /// - Database server resources are freed immediately
    ///
    /// This is called automatically at the end of `run()`, but can be called
    /// manually for early cleanup in error scenarios.
    pub async fn close(&self) {
        info!("Closing database connections");
        // Close both pools concurrently
        tokio::join!(self.source.close(), self.target.close());
        info!("Database connections closed");
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

/// Build FK dependency levels for topological ordering.
///
/// Returns tables grouped by dependency level. Tables in level 0 have no FK dependencies
/// on other tables in the set. Tables in level N only reference tables in levels 0..N-1.
/// This allows parallel FK creation within each level while respecting dependencies.
fn build_fk_dependency_levels(tables: &[Table]) -> Vec<Vec<Table>> {
    use std::collections::{HashMap, HashSet, VecDeque};

    // Build a map of table name -> table for quick lookup
    let table_map: HashMap<String, &Table> = tables
        .iter()
        .map(|t| (t.full_name(), t))
        .collect();

    // Build adjacency list: table -> set of tables it references
    let mut dependencies: HashMap<String, HashSet<String>> = HashMap::new();

    for table in tables {
        let table_name = table.full_name();
        dependencies.entry(table_name.clone()).or_default();

        for fk in &table.foreign_keys {
            let ref_name = format!("{}.{}", fk.ref_schema, fk.ref_table);
            // Only count dependencies on tables in our set
            if table_map.contains_key(&ref_name) && ref_name != table_name {
                dependencies
                    .entry(table_name.clone())
                    .or_default()
                    .insert(ref_name);
            }
        }
    }

    // Kahn's algorithm with level tracking
    let mut levels: Vec<Vec<Table>> = Vec::new();
    let mut queue: VecDeque<String> = VecDeque::new();
    let mut remaining: HashSet<String> = tables.iter().map(|t| t.full_name()).collect();

    // Start with tables that have no dependencies
    for table in tables {
        let name = table.full_name();
        if dependencies.get(&name).map(|d| d.is_empty()).unwrap_or(true) {
            queue.push_back(name);
        }
    }

    while !remaining.is_empty() {
        let mut current_level: Vec<Table> = Vec::new();
        let level_size = queue.len();

        if level_size == 0 {
            // Circular dependency detected - add remaining tables to final level
            // (FKs may fail but that's expected behavior for circular refs)
            for name in &remaining {
                if let Some(table) = table_map.get(name) {
                    current_level.push((*table).clone());
                }
            }
            remaining.clear();
        } else {
            for _ in 0..level_size {
                if let Some(name) = queue.pop_front() {
                    if remaining.remove(&name) {
                        if let Some(table) = table_map.get(&name) {
                            current_level.push((*table).clone());
                        }
                    }
                }
            }

            // Add tables whose dependencies are now satisfied
            for table in tables {
                let name = table.full_name();
                if remaining.contains(&name) {
                    let deps = dependencies.get(&name).cloned().unwrap_or_default();
                    if deps.iter().all(|d| !remaining.contains(d)) {
                        queue.push_back(name);
                    }
                }
            }
        }

        if !current_level.is_empty() {
            levels.push(current_level);
        }
    }

    // If no levels were created, return all tables in a single level
    if levels.is_empty() && !tables.is_empty() {
        levels.push(tables.to_vec());
    }

    levels
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
            total_rows: 100000,
            rows_per_second: 10000,
            current_table: Some("dbo.Users".into()),
            table_status: None,
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
            total_rows: 100000,
            rows_per_second: 5000,
            current_table: None,
            table_status: None,
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
            rows_skipped: 0,
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
            rows_skipped: 0,
            rows_per_second: 1777,
            failed_tables: vec!["dbo.Orders".into(), "dbo.Items".into()],
            table_errors: vec![TableError {
                table: "dbo.Orders".into(),
                error_type: "transfer".into(),
                message: "Connection lost".into(),
            }],
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
            rows_skipped: 0,
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
            rows_skipped: 0,
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
            rows_skipped: 0,
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
        let tracker = ProgressTracker::new(10, 0, true, None);
        assert_eq!(tracker.tables_total, 10);
        assert_eq!(tracker.tables_completed.load(Ordering::Relaxed), 0);
        assert_eq!(tracker.rows_transferred.load(Ordering::Relaxed), 0);
        assert!(tracker.progress_enabled);
    }

    #[test]
    fn test_progress_tracker_complete_table() {
        let tracker = ProgressTracker::new(5, 0, false, None);

        // Simulate transfer engine counting rows, then complete_table is called
        tracker.add_rows(1000);
        tracker.complete_table(1000);
        assert_eq!(tracker.tables_completed.load(Ordering::Relaxed), 1);
        assert_eq!(tracker.rows_transferred.load(Ordering::Relaxed), 1000);

        tracker.add_rows(2000);
        tracker.complete_table(2000);
        assert_eq!(tracker.tables_completed.load(Ordering::Relaxed), 2);
        assert_eq!(tracker.rows_transferred.load(Ordering::Relaxed), 3000);
    }

    #[test]
    fn test_progress_tracker_increment_table_count() {
        let tracker = ProgressTracker::new(3, 0, false, None);

        tracker.increment_table_count();
        assert_eq!(tracker.tables_completed.load(Ordering::Relaxed), 1);
        assert_eq!(tracker.rows_transferred.load(Ordering::Relaxed), 0);

        tracker.increment_table_count();
        assert_eq!(tracker.tables_completed.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_progress_tracker_rows_per_second() {
        let tracker = ProgressTracker::new(1, 0, false, None);

        // Initially should be 0
        assert_eq!(tracker.get_rows_per_second(), 0);

        // Add some rows (simulating transfer engine) and check calculation
        tracker.add_rows(10000);
        tracker.complete_table(10000);
        // Since time has passed, rows_per_second should be > 0
        // We can't test exact value due to timing, but it should be reasonable
        let rps = tracker.get_rows_per_second();
        assert!(rps >= 0); // At minimum it's non-negative
    }

    #[test]
    fn test_progress_tracker_disabled_emit() {
        // When progress_enabled is false, emit should not panic
        let tracker = ProgressTracker::new(5, 0, false, None);
        tracker.complete_table(1000);
        // This should be a no-op and not panic
        tracker.emit("transferring", Some("test_table".to_string()), None);
    }

    #[test]
    fn test_progress_tracker_concurrent_updates() {
        use std::sync::Arc;
        use std::thread;

        let tracker = Arc::new(ProgressTracker::new(100, 0, false, None));
        let mut handles = vec![];

        // Spawn 10 threads, each completing 10 tables with 100 rows each
        // This simulates the transfer engine adding rows and then completing tables
        for _ in 0..10 {
            let t = tracker.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..10 {
                    t.add_rows(100);
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

    #[test]
    fn test_drop_recreate_resets_completed_table_state() {
        // Simulate the table state initialization logic for drop_recreate mode
        let mut state = MigrationState::new("test-run".to_string(), "hash123".to_string());

        // Add a previously completed table
        let mut completed_table = TableState::new(1000);
        completed_table.status = TaskStatus::Completed;
        completed_table.rows_transferred = 1000;
        completed_table.completed_at = Some(Utc::now());
        state.tables.insert("dbo.Users".to_string(), completed_table);

        // Verify initial state
        assert_eq!(state.tables["dbo.Users"].status, TaskStatus::Completed);
        assert_eq!(state.tables["dbo.Users"].rows_transferred, 1000);

        // Simulate drop_recreate mode initialization (always insert fresh state)
        let is_drop_recreate = true;
        let table_name = "dbo.Users".to_string();
        let row_count = 1000;

        if is_drop_recreate {
            state
                .tables
                .insert(table_name.clone(), TableState::new(row_count));
        } else {
            state
                .tables
                .entry(table_name.clone())
                .or_insert_with(|| TableState::new(row_count));
        }

        // In drop_recreate mode, table should be reset to Pending
        assert_eq!(state.tables["dbo.Users"].status, TaskStatus::Pending);
        assert_eq!(state.tables["dbo.Users"].rows_transferred, 0);
        assert!(state.tables["dbo.Users"].completed_at.is_none());
    }

    #[test]
    fn test_upsert_preserves_completed_table_state() {
        // Simulate the table state initialization logic for upsert mode
        let mut state = MigrationState::new("test-run".to_string(), "hash123".to_string());

        // Add a previously completed table
        let mut completed_table = TableState::new(1000);
        completed_table.status = TaskStatus::Completed;
        completed_table.rows_transferred = 1000;
        completed_table.completed_at = Some(Utc::now());
        state.tables.insert("dbo.Users".to_string(), completed_table);

        // Verify initial state
        assert_eq!(state.tables["dbo.Users"].status, TaskStatus::Completed);
        assert_eq!(state.tables["dbo.Users"].rows_transferred, 1000);

        // Simulate upsert mode initialization (preserve existing state)
        let is_drop_recreate = false;
        let table_name = "dbo.Users".to_string();
        let row_count = 1000;

        if is_drop_recreate {
            state
                .tables
                .insert(table_name.clone(), TableState::new(row_count));
        } else {
            state
                .tables
                .entry(table_name.clone())
                .or_insert_with(|| TableState::new(row_count));
        }

        // In upsert mode, table should retain Completed status
        assert_eq!(state.tables["dbo.Users"].status, TaskStatus::Completed);
        assert_eq!(state.tables["dbo.Users"].rows_transferred, 1000);
        assert!(state.tables["dbo.Users"].completed_at.is_some());
    }

    #[test]
    fn test_drop_recreate_processes_all_tables() {
        // Test the table filtering logic for drop_recreate mode
        let mut state = MigrationState::new("test-run".to_string(), "hash123".to_string());

        // Add completed tables
        let table_names = ["dbo.Users", "dbo.Orders", "dbo.Products"];
        for name in table_names {
            let mut ts = TableState::new(1000);
            ts.status = TaskStatus::Completed;
            state.tables.insert(name.to_string(), ts);
        }

        // Simulate drop_recreate mode filtering (should process all tables)
        let is_drop_recreate = true;
        let date_incremental_enabled = false;

        let pending_tables: Vec<_> = table_names
            .iter()
            .filter(|t| {
                if is_drop_recreate || date_incremental_enabled {
                    true
                } else {
                    state
                        .tables
                        .get(**t)
                        .map(|ts| ts.status != TaskStatus::Completed)
                        .unwrap_or(true)
                }
            })
            .collect();

        // All 3 tables should be included for processing
        assert_eq!(pending_tables.len(), 3);
    }

    #[test]
    fn test_upsert_skips_completed_tables() {
        // Test the table filtering logic for upsert mode (crash recovery)
        let mut state = MigrationState::new("test-run".to_string(), "hash123".to_string());

        // Add mix of completed and pending tables
        let mut completed = TableState::new(1000);
        completed.status = TaskStatus::Completed;
        state.tables.insert("dbo.Users".to_string(), completed);

        let pending = TableState::new(1000);
        state.tables.insert("dbo.Orders".to_string(), pending);

        let table_names = ["dbo.Users", "dbo.Orders", "dbo.Products"];

        // Simulate upsert mode filtering (should skip completed tables)
        let is_drop_recreate = false;
        let date_incremental_enabled = false;

        let pending_tables: Vec<_> = table_names
            .iter()
            .filter(|t| {
                if is_drop_recreate || date_incremental_enabled {
                    true
                } else {
                    state
                        .tables
                        .get(**t)
                        .map(|ts| ts.status != TaskStatus::Completed)
                        .unwrap_or(true)
                }
            })
            .collect();

        // Only 2 tables should be included (Orders is pending, Products is new)
        assert_eq!(pending_tables.len(), 2);
        assert!(pending_tables.contains(&&"dbo.Orders"));
        assert!(pending_tables.contains(&&"dbo.Products"));
        assert!(!pending_tables.contains(&&"dbo.Users")); // Completed, should be skipped
    }

    use crate::core::schema::Column;

    fn make_test_column(name: &str) -> Column {
        Column {
            name: name.to_string(),
            data_type: "int".to_string(),
            max_length: 4,
            precision: 10,
            scale: 0,
            is_nullable: false,
            is_identity: name == "id",
            ordinal_pos: 1,
        }
    }

    fn make_test_table(schema: &str, name: &str) -> Table {
        Table {
            schema: schema.to_string(),
            name: name.to_string(),
            columns: vec![make_test_column("id")],
            primary_key: vec!["id".to_string()],
            pk_columns: vec![make_test_column("id")],
            row_count: 1000,
            estimated_row_size: 100,
            indexes: vec![],
            foreign_keys: vec![],
            check_constraints: vec![],
        }
    }

    #[test]
    fn test_fk_dependency_levels_no_fks() {
        let tables = vec![
            make_test_table("dbo", "Users"),
            make_test_table("dbo", "Products"),
        ];

        let levels = super::build_fk_dependency_levels(&tables);

        // All tables should be in a single level (no dependencies)
        assert_eq!(levels.len(), 1);
        assert_eq!(levels[0].len(), 2);
    }

    #[test]
    fn test_fk_dependency_levels_with_dependencies() {
        use crate::core::schema::ForeignKey;

        // Create tables with FK dependencies:
        // Orders -> Users (Orders references Users)
        // OrderItems -> Orders, Products (OrderItems references both)
        let users = make_test_table("dbo", "Users");
        let products = make_test_table("dbo", "Products");

        let mut orders = make_test_table("dbo", "Orders");
        orders.foreign_keys = vec![ForeignKey {
            name: "FK_Orders_Users".to_string(),
            columns: vec!["user_id".to_string()],
            ref_table: "Users".to_string(),
            ref_schema: "dbo".to_string(),
            ref_columns: vec!["id".to_string()],
            on_delete: "NO ACTION".to_string(),
            on_update: "NO ACTION".to_string(),
        }];

        let mut order_items = make_test_table("dbo", "OrderItems");
        order_items.foreign_keys = vec![
            ForeignKey {
                name: "FK_OrderItems_Orders".to_string(),
                columns: vec!["order_id".to_string()],
                ref_table: "Orders".to_string(),
                ref_schema: "dbo".to_string(),
                ref_columns: vec!["id".to_string()],
                on_delete: "NO ACTION".to_string(),
                on_update: "NO ACTION".to_string(),
            },
            ForeignKey {
                name: "FK_OrderItems_Products".to_string(),
                columns: vec!["product_id".to_string()],
                ref_table: "Products".to_string(),
                ref_schema: "dbo".to_string(),
                ref_columns: vec!["id".to_string()],
                on_delete: "NO ACTION".to_string(),
                on_update: "NO ACTION".to_string(),
            },
        ];

        let tables = vec![order_items, orders, users, products];
        let levels = super::build_fk_dependency_levels(&tables);

        // Should have 3 levels:
        // Level 0: Users, Products (no dependencies)
        // Level 1: Orders (depends on Users)
        // Level 2: OrderItems (depends on Orders and Products)
        assert_eq!(levels.len(), 3);

        // Level 0 should have Users and Products
        let level0_names: Vec<_> = levels[0].iter().map(|t| t.full_name()).collect();
        assert!(level0_names.contains(&"dbo.Users".to_string()));
        assert!(level0_names.contains(&"dbo.Products".to_string()));

        // Level 1 should have Orders
        let level1_names: Vec<_> = levels[1].iter().map(|t| t.full_name()).collect();
        assert!(level1_names.contains(&"dbo.Orders".to_string()));

        // Level 2 should have OrderItems
        let level2_names: Vec<_> = levels[2].iter().map(|t| t.full_name()).collect();
        assert!(level2_names.contains(&"dbo.OrderItems".to_string()));
    }
}
