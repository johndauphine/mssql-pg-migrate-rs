//! Migration orchestrator - main workflow coordinator.

use crate::config::{Config, TargetMode};
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
        let mssql_pool_size = config.migration.max_mssql_connections
            .unwrap_or_else(|| (config.migration.workers.max(4) * 2) as usize) as u32;
        let source = MssqlPool::with_max_connections(config.source.clone(), mssql_pool_size).await?;

        // Create target pool
        let max_conns = config.migration.max_pg_connections
            .unwrap_or_else(|| config.migration.workers.max(4) * 2);
        let target = PgPool::new(
            &config.target,
            max_conns,
            config.migration.copy_buffer_rows,
            config.migration.upsert_batch_size,
        ).await?;

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
    pub async fn run(
        mut self,
        cancel: Option<watch::Receiver<bool>>,
    ) -> Result<MigrationResult> {
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
        let mut tables = self.source.extract_schema(&self.config.source.schema).await?;

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

        // Initialize or update state
        let mut state = self.state.take().unwrap_or_else(|| {
            MigrationState::new(run_id.clone(), self.config.hash())
        });

        // Initialize table states
        for table in &tables {
            let table_name = table.full_name();
            state.tables.entry(table_name.clone()).or_insert_with(|| {
                TableState::new(table.row_count)
            });
        }

        // Phase 2: Prepare target
        info!("Phase 2: Preparing target database (mode: {:?})", self.config.migration.target_mode);
        self.prepare_target(&tables, &mut state).await?;

        // Save state after preparation
        self.save_state(&state)?;

        // Phase 3: Transfer data
        info!("Phase 3: Transferring data");
        let transfer_result = self.transfer_data(&tables, &mut state, cancel.clone()).await;

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
                    // Use UNLOGGED for faster inserts, convert to LOGGED after
                    self.target.create_table_unlogged(table, target_schema).await?;
                }
            }
            TargetMode::Truncate => {
                for table in tables {
                    let table_name = table.full_name();

                    // Create if not exists, truncate if exists
                    if self.target.table_exists(target_schema, &table.name).await? {
                        debug!("Truncating table: {}", table_name);
                        self.target.truncate_table(target_schema, &table.name).await?;
                    } else {
                        debug!("Creating table: {}", table_name);
                        self.target.create_table(table, target_schema).await?;
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
                        self.target.create_table(table, target_schema).await?;
                        self.target.create_primary_key(table, target_schema).await?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Transfer data for all tables.
    async fn transfer_data(
        &self,
        tables: &[Table],
        state: &mut MigrationState,
        cancel: watch::Receiver<bool>,
    ) -> Result<()> {
        let workers = self.config.migration.workers;
        let semaphore = Arc::new(Semaphore::new(workers));

        let transfer_config = TransferConfig {
            chunk_size: self.config.migration.chunk_size,
            read_ahead: self.config.migration.read_ahead_buffers,
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
                state.tables.get(&table_name)
                    .map(|ts| ts.status != TaskStatus::Completed)
                    .unwrap_or(true)
            })
            .collect();

        info!("Transferring {} tables with {} workers", pending_tables.len(), workers);

        let mut handles = Vec::new();

        for table in pending_tables {
            // Check for cancellation
            if *cancel.borrow() {
                info!("Cancellation requested, stopping new transfers");
                break;
            }

            let table_name = table.full_name();
            let permit = semaphore.clone().acquire_owned().await.unwrap();

            // Update state
            if let Some(ts) = state.tables.get_mut(&table_name) {
                ts.status = TaskStatus::InProgress;
            }

            // Save state periodically
            self.save_state(state)?;

            let job = TransferJob {
                table: table.clone(),
                partition_id: None,
                min_pk: None,
                max_pk: None,
                resume_from_pk: state.tables.get(&table_name)
                    .and_then(|ts| ts.last_pk),
                target_mode: self.config.migration.target_mode.clone(),
                target_schema: self.config.target.schema.clone(),
            };

            let engine_clone = engine.clone();

            let handle = tokio::spawn(async move {
                let result = engine_clone.execute(job).await;
                drop(permit); // Release worker slot
                result
            });

            handles.push((table_name, handle));
        }

        // Collect results
        for (table_name, handle) in handles {
            match handle.await {
                Ok(Ok(stats)) => {
                    if let Some(ts) = state.tables.get_mut(&table_name) {
                        ts.status = TaskStatus::Completed;
                        ts.rows_transferred = stats.rows;
                        ts.last_pk = stats.last_pk;
                        ts.completed_at = Some(Utc::now());
                    }
                    info!("{}: completed ({} rows)", table_name, stats.rows);
                }
                Ok(Err(e)) => {
                    error!("{}: failed - {}", table_name, e);
                    if let Some(ts) = state.tables.get_mut(&table_name) {
                        ts.status = TaskStatus::Failed;
                        ts.error = Some(e.to_string());
                    }
                }
                Err(e) => {
                    error!("{}: task panicked - {}", table_name, e);
                    if let Some(ts) = state.tables.get_mut(&table_name) {
                        ts.status = TaskStatus::Failed;
                        ts.error = Some(format!("Task panicked: {}", e));
                    }
                }
            }

            // Save state after each table
            self.save_state(state)?;
        }

        // Check for failures
        let failed: Vec<_> = state.tables.iter()
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

        // Convert UNLOGGED tables back to LOGGED (if using drop_recreate)
        if matches!(self.config.migration.target_mode, TargetMode::DropRecreate) {
            for table in tables {
                self.target.set_table_logged(target_schema, &table.name).await?;
            }
        }

        // Create primary keys (for drop_recreate and truncate)
        if !matches!(self.config.migration.target_mode, TargetMode::Upsert) {
            for table in tables {
                if !table.primary_key.is_empty() {
                    debug!("Creating PK for: {}", table.full_name());
                    self.target.create_primary_key(table, target_schema).await?;
                }
            }
        }

        // Create indexes
        if self.config.migration.create_indexes {
            for table in tables {
                for index in &table.indexes {
                    debug!("Creating index: {}.{}", table.name, index.name);
                    if let Err(e) = self.target.create_index(table, index, target_schema).await {
                        warn!("Failed to create index {}: {}", index.name, e);
                    }
                }
            }
        }

        // Create foreign keys
        if self.config.migration.create_foreign_keys {
            for table in tables {
                for fk in &table.foreign_keys {
                    debug!("Creating FK: {}.{}", table.name, fk.name);
                    if let Err(e) = self.target.create_foreign_key(table, fk, target_schema).await {
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
                    if let Err(e) = self.target.create_check_constraint(table, chk, target_schema).await {
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
            let source_count = self.source.get_row_count(source_schema, &table.name).await?;
            let target_count = self.target.get_row_count(target_schema, &table.name).await
                .unwrap_or(0);

            let matches = source_count == target_count;
            results.insert(table.full_name(), (source_count, target_count, matches));

            if matches {
                info!("{}: {} rows (match)", table.full_name(), source_count);
            } else {
                warn!(
                    "{}: source={} target={} (MISMATCH)",
                    table.full_name(), source_count, target_count
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
