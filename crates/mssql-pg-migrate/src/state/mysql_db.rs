//! MySQL database-backed state storage for migration runs.
//!
//! Stores migration state in MySQL tables within the `_mssql_pg_migrate` database.
//! This enables resume capability and incremental sync for MySQL targets.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::mysql::MySqlPool;
use sqlx::Row;
use std::collections::HashMap;

use crate::error::{MigrateError, Result};
use crate::state::backend::{
    run_status_to_str, str_to_run_status, str_to_task_status, task_status_to_str, StateBackend,
};
use crate::state::{MigrationState, TableState};

/// MySQL database state backend for migration runs.
pub struct MysqlStateBackend {
    pool: MySqlPool,
    schema: String,
}

impl MysqlStateBackend {
    /// Create a new MySQL database state backend.
    pub fn new(pool: MySqlPool) -> Self {
        Self {
            pool,
            schema: "_mssql_pg_migrate".to_string(),
        }
    }

    /// Initialize the state schema and tables.
    pub async fn init_schema(&self) -> Result<()> {
        // Create database/schema if not exists
        let sql = format!("CREATE DATABASE IF NOT EXISTS `{}`", self.schema);
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map_err(|e| MigrateError::pool(e, "creating MySQL state schema"))?;

        // Create denormalized table_state table (includes run-level fields)
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS `{}`.`table_state` (
                run_id VARCHAR(100) NOT NULL,
                config_hash VARCHAR(100) NOT NULL,
                run_started_at DATETIME(6) NOT NULL,
                run_completed_at DATETIME(6),
                run_status VARCHAR(20) NOT NULL,
                table_name VARCHAR(500) NOT NULL,
                table_status VARCHAR(20) NOT NULL,
                rows_total BIGINT NOT NULL DEFAULT 0,
                rows_transferred BIGINT NOT NULL DEFAULT 0,
                rows_skipped BIGINT NOT NULL DEFAULT 0,
                last_pk BIGINT,
                last_sync_timestamp DATETIME(6),
                table_completed_at DATETIME(6),
                error TEXT,
                updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
                PRIMARY KEY (run_id, table_name),
                INDEX idx_config_hash (config_hash, run_started_at DESC),
                INDEX idx_incremental_sync (table_name, table_status, last_sync_timestamp)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
            self.schema
        );
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map_err(|e| MigrateError::pool(e, "creating MySQL table_state table"))?;

        Ok(())
    }

    /// Save a migration state to the database.
    pub async fn save(&self, state: &MigrationState) -> Result<()> {
        // Start transaction
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| MigrateError::pool(e, "starting MySQL transaction"))?;

        // Upsert table states with denormalized run-level fields
        for (table_name, table_state) in &state.tables {
            let sql = format!(
                "INSERT INTO `{}`.`table_state`
                 (run_id, config_hash, run_started_at, run_completed_at, run_status,
                  table_name, table_status, rows_total, rows_transferred, rows_skipped,
                  last_pk, last_sync_timestamp, table_completed_at, error)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON DUPLICATE KEY UPDATE
                    config_hash = VALUES(config_hash),
                    run_started_at = VALUES(run_started_at),
                    run_completed_at = VALUES(run_completed_at),
                    run_status = VALUES(run_status),
                    table_status = VALUES(table_status),
                    rows_total = VALUES(rows_total),
                    rows_transferred = VALUES(rows_transferred),
                    rows_skipped = VALUES(rows_skipped),
                    last_pk = VALUES(last_pk),
                    last_sync_timestamp = VALUES(last_sync_timestamp),
                    table_completed_at = VALUES(table_completed_at),
                    error = VALUES(error)",
                self.schema
            );

            sqlx::query(&sql)
                .bind(&state.run_id)
                .bind(&state.config_hash)
                .bind(state.started_at)
                .bind(state.completed_at)
                .bind(run_status_to_str(state.status))
                .bind(table_name)
                .bind(task_status_to_str(table_state.status))
                .bind(table_state.rows_total)
                .bind(table_state.rows_transferred)
                .bind(table_state.rows_skipped)
                .bind(table_state.last_pk)
                .bind(table_state.last_sync_timestamp)
                .bind(table_state.completed_at)
                .bind(&table_state.error)
                .execute(&mut *tx)
                .await
                .map_err(|e| MigrateError::pool(e, "saving MySQL table state"))?;
        }

        tx.commit()
            .await
            .map_err(|e| MigrateError::pool(e, "committing MySQL transaction"))?;

        Ok(())
    }

    /// Load the latest migration state for a given config hash.
    pub async fn load_latest(&self, config_hash: &str) -> Result<Option<MigrationState>> {
        // Load all table states for the latest run with matching config hash
        let sql = format!(
            "SELECT run_id, config_hash, run_started_at, run_completed_at, run_status,
                    table_name, table_status, rows_total, rows_transferred, rows_skipped,
                    last_pk, last_sync_timestamp, table_completed_at, error
             FROM `{}`.`table_state`
             WHERE config_hash = ?
               AND run_id = (
                   SELECT run_id FROM `{}`.`table_state`
                   WHERE config_hash = ?
                   ORDER BY run_started_at DESC
                   LIMIT 1
               )",
            self.schema, self.schema
        );

        let rows = sqlx::query(&sql)
            .bind(config_hash)
            .bind(config_hash)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| MigrateError::pool(e, "loading MySQL state"))?;

        if rows.is_empty() {
            return Ok(None);
        }

        // Extract run-level fields from first row (all rows have same values)
        let first_row = &rows[0];
        let run_id: String = first_row.get("run_id");
        let config_hash: String = first_row.get("config_hash");
        let started_at: DateTime<Utc> = first_row.get("run_started_at");
        let completed_at: Option<DateTime<Utc>> = first_row.get("run_completed_at");
        let status_str: String = first_row.get("run_status");

        // Extract table states
        let mut tables = HashMap::new();
        for row in rows {
            let table_name: String = row.get("table_name");
            let table_status_str: String = row.get("table_status");
            let rows_total: i64 = row.get("rows_total");
            let rows_transferred: i64 = row.get("rows_transferred");
            let rows_skipped: i64 = row.get("rows_skipped");
            let last_pk: Option<i64> = row.get("last_pk");
            let last_sync_timestamp: Option<DateTime<Utc>> = row.get("last_sync_timestamp");
            let table_completed_at: Option<DateTime<Utc>> = row.get("table_completed_at");
            let error: Option<String> = row.get("error");

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
    pub async fn get_last_sync_timestamp(&self, table_name: &str) -> Result<Option<DateTime<Utc>>> {
        let sql = format!(
            "SELECT last_sync_timestamp
             FROM `{}`.`table_state`
             WHERE table_name = ?
               AND table_status = 'completed'
               AND last_sync_timestamp IS NOT NULL
             ORDER BY updated_at DESC
             LIMIT 1",
            self.schema
        );

        let row = sqlx::query(&sql)
            .bind(table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL last sync timestamp"))?;

        Ok(row.and_then(|r| r.get("last_sync_timestamp")))
    }

    /// Get the backend type name.
    pub fn backend_type(&self) -> &'static str {
        "mysql"
    }
}

// Implement the StateBackend trait for MysqlStateBackend
#[async_trait]
impl StateBackend for MysqlStateBackend {
    async fn init_schema(&self) -> Result<()> {
        MysqlStateBackend::init_schema(self).await
    }

    async fn save(&self, state: &MigrationState) -> Result<()> {
        MysqlStateBackend::save(self, state).await
    }

    async fn load_latest(&self, config_hash: &str) -> Result<Option<MigrationState>> {
        MysqlStateBackend::load_latest(self, config_hash).await
    }

    async fn get_last_sync_timestamp(&self, table_name: &str) -> Result<Option<DateTime<Utc>>> {
        MysqlStateBackend::get_last_sync_timestamp(self, table_name).await
    }

    fn backend_type(&self) -> &'static str {
        MysqlStateBackend::backend_type(self)
    }
}
