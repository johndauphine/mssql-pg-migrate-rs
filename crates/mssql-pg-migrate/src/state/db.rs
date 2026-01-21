//! Database-backed state storage for migration runs.
//!
//! Stores migration state in PostgreSQL tables within the `_mssql_pg_migrate` schema.
//! This is preferred over file-based state for production use:
//! - Transactional safety
//! - Multi-instance coordination
//! - No file system access required
//! - Built-in audit trail

use crate::error::{MigrateError, Result};
use crate::state::{MigrationState, RunStatus, TableState, TaskStatus};
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use std::collections::HashMap;

/// Database state backend for migration runs.
pub struct DbStateBackend {
    pool: Pool,
    schema: String,
}

impl DbStateBackend {
    /// Create a new database state backend.
    pub fn new(pool: Pool) -> Self {
        Self {
            pool,
            schema: "_mssql_pg_migrate".to_string(),
        }
    }

    /// Initialize the state schema and tables.
    pub async fn init_schema(&self) -> Result<()> {
        let conn = self.pool.get().await?;

        // Create schema
        conn.execute(
            &format!("CREATE SCHEMA IF NOT EXISTS {}", self.schema),
            &[],
        )
        .await?;

        // Create denormalized table_state table (includes run-level fields)
        conn.execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {}.table_state (
                    run_id TEXT NOT NULL,
                    config_hash TEXT NOT NULL,
                    run_started_at TIMESTAMPTZ NOT NULL,
                    run_completed_at TIMESTAMPTZ,
                    run_status TEXT NOT NULL CHECK (run_status IN ('running', 'completed', 'failed', 'cancelled')),
                    table_name TEXT NOT NULL,
                    table_status TEXT NOT NULL CHECK (table_status IN ('pending', 'in_progress', 'completed', 'failed')),
                    rows_total BIGINT NOT NULL DEFAULT 0,
                    rows_transferred BIGINT NOT NULL DEFAULT 0,
                    rows_skipped BIGINT NOT NULL DEFAULT 0,
                    last_pk BIGINT,
                    last_sync_timestamp TIMESTAMPTZ,
                    table_completed_at TIMESTAMPTZ,
                    error TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (run_id, table_name)
                )",
                self.schema
            ),
            &[],
        )
        .await?;

        // Create index for incremental sync lookups
        conn.execute(
            &format!(
                "CREATE INDEX IF NOT EXISTS idx_table_state_incremental_sync
                    ON {}.table_state(table_name, table_status, last_sync_timestamp)
                    WHERE table_status = 'completed' AND last_sync_timestamp IS NOT NULL",
                self.schema
            ),
            &[],
        )
        .await?;

        // Create index for latest run lookups
        conn.execute(
            &format!(
                "CREATE INDEX IF NOT EXISTS idx_table_state_latest_run
                    ON {}.table_state(config_hash, run_started_at DESC)",
                self.schema
            ),
            &[],
        )
        .await?;

        Ok(())
    }

    /// Save a migration state to the database.
    pub async fn save(&self, state: &MigrationState) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;

        // Upsert table states with denormalized run-level fields
        for (table_name, table_state) in &state.tables {
            tx.execute(
                &format!(
                    "INSERT INTO {}.table_state
                     (run_id, config_hash, run_started_at, run_completed_at, run_status,
                      table_name, table_status, rows_total, rows_transferred, rows_skipped,
                      last_pk, last_sync_timestamp, table_completed_at, error, updated_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW())
                     ON CONFLICT (run_id, table_name) DO UPDATE SET
                        config_hash = EXCLUDED.config_hash,
                        run_started_at = EXCLUDED.run_started_at,
                        run_completed_at = EXCLUDED.run_completed_at,
                        run_status = EXCLUDED.run_status,
                        table_status = EXCLUDED.table_status,
                        rows_total = EXCLUDED.rows_total,
                        rows_transferred = EXCLUDED.rows_transferred,
                        rows_skipped = EXCLUDED.rows_skipped,
                        last_pk = EXCLUDED.last_pk,
                        last_sync_timestamp = EXCLUDED.last_sync_timestamp,
                        table_completed_at = EXCLUDED.table_completed_at,
                        error = EXCLUDED.error,
                        updated_at = NOW()",
                    self.schema
                ),
                &[
                    &state.run_id,
                    &state.config_hash,
                    &state.started_at,
                    &state.completed_at,
                    &status_to_str(state.status),
                    &table_name,
                    &task_status_to_str(table_state.status),
                    &table_state.rows_total,
                    &table_state.rows_transferred,
                    &table_state.rows_skipped,
                    &table_state.last_pk,
                    &table_state.last_sync_timestamp,
                    &table_state.completed_at,
                    &table_state.error,
                ],
            )
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Load the latest migration state for a given config hash.
    /// Returns None if no matching state found.
    pub async fn load_latest(&self, config_hash: &str) -> Result<Option<MigrationState>> {
        let conn = self.pool.get().await?;

        // Load all table states for the latest run with matching config hash
        let table_rows = conn
            .query(
                &format!(
                    "SELECT run_id, config_hash, run_started_at, run_completed_at, run_status,
                            table_name, table_status, rows_total, rows_transferred, rows_skipped,
                            last_pk, last_sync_timestamp, table_completed_at, error
                     FROM {}.table_state
                     WHERE config_hash = $1
                       AND run_id = (
                           SELECT run_id FROM {}.table_state
                           WHERE config_hash = $1
                           ORDER BY run_started_at DESC
                           LIMIT 1
                       )",
                    self.schema, self.schema
                ),
                &[&config_hash],
            )
            .await?;

        if table_rows.is_empty() {
            return Ok(None);
        }

        // Extract run-level fields from first row (all rows have same values)
        let first_row = &table_rows[0];
        let run_id: String = first_row.get(0);
        let config_hash: String = first_row.get(1);
        let started_at: DateTime<Utc> = first_row.get(2);
        let completed_at: Option<DateTime<Utc>> = first_row.get(3);
        let status_str: String = first_row.get(4);

        // Extract table states
        let mut tables = HashMap::new();
        for row in table_rows {
            let table_name: String = row.get(5);
            let table_status_str: String = row.get(6);
            let rows_total: i64 = row.get(7);
            let rows_transferred: i64 = row.get(8);
            let rows_skipped: i64 = row.get(9);
            let last_pk: Option<i64> = row.get(10);
            let last_sync_timestamp: Option<DateTime<Utc>> = row.get(11);
            let table_completed_at: Option<DateTime<Utc>> = row.get(12);
            let error: Option<String> = row.get(13);

            tables.insert(
                table_name,
                TableState {
                    status: str_to_task_status(&table_status_str)?,
                    rows_total,
                    rows_transferred,
                    rows_skipped,
                    last_pk,
                    partitions: None, // Partitions not stored in DB (in-memory only)
                    completed_at: table_completed_at,
                    error,
                    last_sync_timestamp,
                },
            );
        }

        Ok(Some(MigrationState {
            run_id,
            config_hash,
            started_at,
            status: str_to_run_status(&status_str)?,
            tables,
            completed_at,
            hmac: None, // HMAC not used for database state
        }))
    }

    /// Get the last sync timestamp for a specific table (for incremental sync).
    /// Returns the most recent completed run's timestamp for this table.
    pub async fn get_last_sync_timestamp(
        &self,
        table_name: &str,
    ) -> Result<Option<DateTime<Utc>>> {
        let conn = self.pool.get().await?;

        let row = conn
            .query_opt(
                &format!(
                    "SELECT last_sync_timestamp
                     FROM {}.table_state
                     WHERE table_name = $1
                       AND table_status = 'completed'
                       AND last_sync_timestamp IS NOT NULL
                     ORDER BY updated_at DESC
                     LIMIT 1",
                    self.schema
                ),
                &[&table_name],
            )
            .await?;

        Ok(row.and_then(|r| r.get(0)))
    }
}

fn status_to_str(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Running => "running",
        RunStatus::Completed => "completed",
        RunStatus::Failed => "failed",
        RunStatus::Cancelled => "cancelled",
    }
}

fn str_to_run_status(s: &str) -> Result<RunStatus> {
    match s {
        "running" => Ok(RunStatus::Running),
        "completed" => Ok(RunStatus::Completed),
        "failed" => Ok(RunStatus::Failed),
        "cancelled" => Ok(RunStatus::Cancelled),
        _ => Err(MigrateError::Config(format!("Invalid run status: {}", s))),
    }
}

fn task_status_to_str(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Pending => "pending",
        TaskStatus::InProgress => "in_progress",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
    }
}

fn str_to_task_status(s: &str) -> Result<TaskStatus> {
    match s {
        "pending" => Ok(TaskStatus::Pending),
        "in_progress" => Ok(TaskStatus::InProgress),
        "completed" => Ok(TaskStatus::Completed),
        "failed" => Ok(TaskStatus::Failed),
        _ => Err(MigrateError::Config(format!("Invalid task status: {}", s))),
    }
}
