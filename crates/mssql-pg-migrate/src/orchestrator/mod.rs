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
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{watch, Semaphore};
use tracing::{debug, error, info, warn};

/// Migration orchestrator.
pub struct Orchestrator {
    config: Config,
    state_file: Option<PathBuf>,
    state: Option<MigrationState>,
    source: Arc<MssqlPool>,
    target: Arc<PgPool>,
}

/// Result of a migration run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationResult {
    /// Unique run identifier.
    pub run_id: String,

    /// Final status.
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

    /// List of failed table names.
    pub failed_tables: Vec<String>,
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
        })
    }

    /// Set the state file path for resume capability.
    pub fn with_state_file(mut self, path: PathBuf) -> Self {
        self.state_file = Some(path);
        self
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

    /// Run the migration.
    pub async fn run(mut self, cancel: Option<watch::Receiver<bool>>) -> Result<MigrationResult> {
        let started_at = Utc::now();
        let run_id = self
            .state
            .as_ref()
            .map(|s| s.run_id.clone())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let cancel = cancel.unwrap_or_else(|| {
            let (_, rx) = watch::channel(false);
            rx
        });

        info!("Starting migration run: {}", run_id);

        // Phase 1: Extract schema
        info!("Phase 1: Extracting schema from source");
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

        // Phase 2: Prepare target
        info!(
            "Phase 2: Preparing target database (mode: {:?})",
            self.config.migration.target_mode
        );
        self.prepare_target(&tables, &mut state).await?;

        // Save state after preparation
        self.save_state(&state)?;

        // Phase 3: Transfer data
        info!("Phase 3: Transferring data");
        let transfer_result = self
            .transfer_data(&tables, &mut state, cancel.clone())
            .await;

        // Phase 4: Finalize (only on success)
        if transfer_result.is_ok() {
            info!("Phase 4: Finalizing (indexes, constraints)");
            self.finalize(&tables).await?;
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

        let status = if tables_failed > 0 {
            state.status = RunStatus::Failed;
            "failed"
        } else if *cancel.borrow() {
            state.status = RunStatus::Cancelled;
            "cancelled"
        } else {
            state.status = RunStatus::Completed;
            "completed"
        };

        // Save final state
        self.save_state(&state)?;

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
                    // Use UNLOGGED for faster inserts if configured, convert to LOGGED after
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
                // For upsert, just ensure tables exist
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
                    } else if self.config.migration.use_unlogged_tables {
                        // Set existing table to UNLOGGED for faster writes
                        self.target
                            .set_table_unlogged(target_schema, &table.name)
                            .await?;
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
        cancel: watch::Receiver<bool>,
    ) -> Result<()> {
        let workers = self.config.migration.get_workers();
        let semaphore = Arc::new(Semaphore::new(workers));

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

        let mut handles = Vec::new();

        for table in pending_tables {
            // Check for cancellation
            if *cancel.borrow() {
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

                        // Update state
                        if let Some(ts) = state.tables.get_mut(&table_name) {
                            ts.status = TaskStatus::InProgress;
                        }
                        self.save_state(state)?;

                        // Create a job for each partition
                        for partition in partitions {
                            let permit = semaphore.clone().acquire_owned().await.unwrap();

                            // Subtract 1 from min_pk so that `WHERE pk > (min_pk - 1)` includes the first row.
                            // For the first partition (min_pk = 1), this becomes `WHERE pk > 0`.
                            // Use saturating_sub to handle edge case of i64::MIN.
                            let adjusted_min = partition.min_pk.map(|pk| pk.saturating_sub(1));

                            let job = TransferJob {
                                table: table.clone(),
                                partition_id: Some(partition.partition_id),
                                min_pk: adjusted_min,
                                max_pk: partition.max_pk,
                                resume_from_pk: None, // Partitions don't support resume yet
                                target_mode: self.config.migration.target_mode.clone(),
                                target_schema: self.config.target.schema.clone(),
                            };

                            let engine_clone = engine.clone();
                            let partition_name =
                                format!("{}:p{}", table_name, partition.partition_id);

                            let handle = tokio::spawn(async move {
                                let result = engine_clone.execute(job).await;
                                drop(permit);
                                result
                            });

                            handles.push((partition_name, handle));
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

            let handle = tokio::spawn(async move {
                let result = engine_clone.execute(job).await;
                drop(permit);
                result
            });

            handles.push((table_name, handle));
        }

        // Collect results, tracking partition aggregation
        let mut table_rows: HashMap<String, i64> = HashMap::new();
        let mut table_errors: HashMap<String, String> = HashMap::new();

        for (job_name, handle) in handles {
            // Extract base table name (handles both "schema.table" and "schema.table:p1")
            let base_table = job_name.split(":p").next().unwrap_or(&job_name).to_string();

            match handle.await {
                Ok(Ok(stats)) => {
                    info!("{}: completed ({} rows)", job_name, stats.rows);
                    *table_rows.entry(base_table.clone()).or_insert(0) += stats.rows;

                    // For non-partitioned tables, update state directly
                    if !job_name.contains(":p") {
                        if let Some(ts) = state.tables.get_mut(&base_table) {
                            ts.status = TaskStatus::Completed;
                            ts.rows_transferred = stats.rows;
                            ts.last_pk = stats.last_pk;
                            ts.completed_at = Some(Utc::now());
                        }
                    }
                }
                Ok(Err(e)) => {
                    error!("{}: failed - {}", job_name, e);
                    // Preserve first error for this table (don't overwrite if partition already failed)
                    table_errors
                        .entry(base_table.clone())
                        .or_insert_with(|| e.to_string());
                }
                Err(e) => {
                    error!("{}: task panicked - {}", job_name, e);
                    // Preserve first error for this table (don't overwrite if partition already failed)
                    table_errors
                        .entry(base_table.clone())
                        .or_insert_with(|| format!("Task panicked: {}", e));
                }
            }
        }

        // Finalize partitioned table states
        for (table_name, rows) in &table_rows {
            if let Some(ts) = state.tables.get_mut(table_name) {
                if table_errors.contains_key(table_name) {
                    ts.status = TaskStatus::Failed;
                    ts.error = table_errors.get(table_name).cloned();
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

        // Create indexes
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
