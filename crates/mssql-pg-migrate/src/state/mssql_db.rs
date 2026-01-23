//! MSSQL database-backed state storage for migration runs.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tiberius::Row;

use crate::error::{MigrateError, Result};
use crate::state::backend::{
    run_status_to_str, str_to_run_status, str_to_task_status, task_status_to_str, StateBackend,
};
use crate::state::{MigrationState, RunStatus, TableState};
use crate::target::MssqlTargetPool;

/// MSSQL database state backend for migration runs.
pub struct MssqlStateBackend {
    pool: Arc<MssqlTargetPool>,
    schema: String,
}

impl MssqlStateBackend {
    /// Create a new MSSQL database state backend.
    pub fn new(pool: Arc<MssqlTargetPool>) -> Self {
        Self {
            pool,
            schema: "_mssql_pg_migrate".to_string(),
        }
    }

    /// Initialize the state schema and tables.
    pub async fn init_schema(&self) -> Result<()> {
        let mut conn = self.pool.get_conn().await?;

        // Create schema
        let sql = format!(
            "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{}')
             BEGIN
                 EXEC('CREATE SCHEMA [{}]')
             END",
            self.schema, self.schema
        );
        conn.execute(sql, &[]).await?;

        // Create denormalized table_state table (includes run-level fields)
        let sql = format!(
            "IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'table_state' AND schema_id = SCHEMA_ID('{}'))
             BEGIN
                 CREATE TABLE [{}].[table_state] (
                     run_id NVARCHAR(100) NOT NULL,
                     config_hash NVARCHAR(100) NOT NULL,
                     run_started_at DATETIME2 NOT NULL,
                     run_completed_at DATETIME2,
                     run_status NVARCHAR(20) NOT NULL CHECK (run_status IN ('running', 'completed', 'failed', 'cancelled')),
                     table_name NVARCHAR(500) NOT NULL,
                     table_status NVARCHAR(20) NOT NULL CHECK (table_status IN ('pending', 'in_progress', 'completed', 'failed')),
                     rows_total BIGINT NOT NULL DEFAULT 0,
                     rows_transferred BIGINT NOT NULL DEFAULT 0,
                     rows_skipped BIGINT NOT NULL DEFAULT 0,
                     last_pk BIGINT,
                     last_sync_timestamp DATETIME2,
                     table_completed_at DATETIME2,
                     error NVARCHAR(MAX),
                     updated_at DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
                     PRIMARY KEY (run_id, table_name)
                 )
             END",
            self.schema, self.schema
        );
        conn.execute(sql, &[]).await?;

        // Create index for incremental sync lookups
        let sql = format!(
            "IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_table_state_incremental_sync' AND object_id = OBJECT_ID('[{}].[table_state]'))
             BEGIN
                 CREATE INDEX idx_table_state_incremental_sync
                 ON [{}].[table_state](table_name, table_status, last_sync_timestamp)
                 WHERE table_status = 'completed' AND last_sync_timestamp IS NOT NULL
             END",
            self.schema, self.schema
        );
        conn.execute(sql, &[]).await?;

        // Create index for latest run lookups
        let sql = format!(
            "IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_table_state_latest_run' AND object_id = OBJECT_ID('[{}].[table_state]'))
             BEGIN
                 CREATE INDEX idx_table_state_latest_run
                 ON [{}].[table_state](config_hash, run_started_at DESC)
             END",
            self.schema, self.schema
        );
        conn.execute(sql, &[]).await?;

        Ok(())
    }

    /// Save a migration state to the database.
    ///
    /// Note: Each MERGE statement is atomic, so we don't use explicit transaction
    /// handling here. Using BEGIN TRANSACTION/COMMIT with Tiberius can cause
    /// "Transaction count mismatch" errors (code 266) due to how the driver
    /// manages implicit transactions internally.
    pub async fn save(&self, state: &MigrationState) -> Result<()> {
        let mut conn = self.pool.get_conn().await?;

        // Upsert table states with denormalized run-level fields
        // Each MERGE is atomic, providing row-level consistency
        for (table_name, table_state) in &state.tables {
            let sql = format!(
                "MERGE [{}].[table_state] AS target
                 USING (SELECT @P1 AS run_id, @P6 AS table_name) AS source
                 ON target.run_id = source.run_id AND target.table_name = source.table_name
                 WHEN MATCHED THEN
                     UPDATE SET
                         config_hash = @P2,
                         run_started_at = @P3,
                         run_completed_at = @P4,
                         run_status = @P5,
                         table_status = @P7,
                         rows_total = @P8,
                         rows_transferred = @P9,
                         rows_skipped = @P10,
                         last_pk = @P11,
                         last_sync_timestamp = @P12,
                         table_completed_at = @P13,
                         error = @P14,
                         updated_at = GETUTCDATE()
                 WHEN NOT MATCHED THEN
                     INSERT (run_id, config_hash, run_started_at, run_completed_at, run_status,
                             table_name, table_status, rows_total, rows_transferred, rows_skipped,
                             last_pk, last_sync_timestamp, table_completed_at, error, updated_at)
                     VALUES (@P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10, @P11, @P12, @P13, @P14, GETUTCDATE());",
                self.schema
            );

            conn.execute(
                sql,
                &[
                    &state.run_id,
                    &state.config_hash,
                    &state.started_at,
                    &state.completed_at,
                    &run_status_to_str(state.status),
                    table_name,
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
        Ok(())
    }

    /// Load the latest migration state for a given config hash.
    pub async fn load_latest(&self, config_hash: &str) -> Result<Option<MigrationState>> {
        let mut conn = self.pool.get_conn().await?;

        // Load all table states for the latest run with matching config hash
        let sql = format!(
            "SELECT run_id, config_hash, run_started_at, run_completed_at, run_status,
                    table_name, table_status, rows_total, rows_transferred, rows_skipped,
                    last_pk, last_sync_timestamp, table_completed_at, error
             FROM [{}].[table_state]
             WHERE config_hash = @P1
               AND run_id = (
                   SELECT TOP 1 run_id FROM [{}].[table_state]
                   WHERE config_hash = @P1
                   ORDER BY run_started_at DESC
               )",
            self.schema, self.schema
        );

        let table_rows = conn.query(sql, &[&config_hash]).await?;
        let results = table_rows.into_results().await?;

        if results.is_empty() {
            return Ok(None);
        }

        let rows: Vec<Row> = results.into_iter().flat_map(|r| r.into_iter()).collect();
        if rows.is_empty() {
            return Ok(None);
        }

        // Extract run-level fields from first row (all rows have same values)
        let first_row = &rows[0];
        let run_id: String = first_row
            .get::<&str, _>(0)
            .ok_or_else(|| MigrateError::State("Failed to get run_id from database".to_string()))?
            .to_string();
        let config_hash: String = first_row
            .get::<&str, _>(1)
            .ok_or_else(|| {
                MigrateError::State("Failed to get config_hash from database".to_string())
            })?
            .to_string();
        let started_at: DateTime<Utc> = first_row.get(2).ok_or_else(|| {
            MigrateError::State("Failed to get run_started_at from database".to_string())
        })?;
        let completed_at: Option<DateTime<Utc>> = first_row.get(3);
        let run_status: RunStatus = {
            let status_str: &str = first_row.get(4).ok_or_else(|| {
                MigrateError::State("Failed to get run_status from database".to_string())
            })?;
            str_to_run_status(status_str)?
        };

        // Extract table states
        let mut tables = HashMap::new();
        for row in rows {
            let table_name: &str = row.get(5).ok_or_else(|| {
                MigrateError::State("Failed to get table_name from database".to_string())
            })?;
            let table_status_str: &str = row.get(6).ok_or_else(|| {
                MigrateError::State("Failed to get table_status from database".to_string())
            })?;
            let rows_total: i64 = row.get(7).unwrap_or(0);
            let rows_transferred: i64 = row.get(8).unwrap_or(0);
            let rows_skipped: i64 = row.get(9).unwrap_or(0);
            let last_pk: Option<i64> = row.get(10);
            let last_sync_timestamp: Option<DateTime<Utc>> = row.get(11);
            let table_completed_at: Option<DateTime<Utc>> = row.get(12);
            let error: Option<&str> = row.get(13);

            tables.insert(
                table_name.to_string(),
                TableState {
                    status: str_to_task_status(table_status_str)?,
                    rows_total,
                    rows_transferred,
                    rows_skipped,
                    last_pk,
                    partitions: None,
                    completed_at: table_completed_at,
                    error: error.map(|s| s.to_string()),
                    last_sync_timestamp,
                },
            );
        }

        Ok(Some(MigrationState {
            run_id,
            config_hash,
            started_at,
            status: run_status,
            tables,
            completed_at,
            hmac: None, // HMAC not used for database state
        }))
    }

    /// Get the last sync timestamp for a specific table (for incremental sync).
    pub async fn get_last_sync_timestamp(&self, table_name: &str) -> Result<Option<DateTime<Utc>>> {
        let mut conn = self.pool.get_conn().await?;

        let sql = format!(
            "SELECT TOP 1 last_sync_timestamp
             FROM [{}].[table_state]
             WHERE table_name = @P1
               AND table_status = 'completed'
               AND last_sync_timestamp IS NOT NULL
             ORDER BY updated_at DESC",
            self.schema
        );

        let rows = conn.query(sql, &[&table_name]).await?;
        let row: Option<Row> = rows
            .into_results()
            .await?
            .into_iter()
            .next()
            .and_then(|mut result| result.pop());

        Ok(row.and_then(|r| r.get(0)))
    }

    /// Get the backend type name.
    pub fn backend_type(&self) -> &'static str {
        "mssql"
    }
}

// Implement the StateBackend trait for MssqlStateBackend
#[async_trait]
impl StateBackend for MssqlStateBackend {
    async fn init_schema(&self) -> Result<()> {
        MssqlStateBackend::init_schema(self).await
    }

    async fn save(&self, state: &MigrationState) -> Result<()> {
        MssqlStateBackend::save(self, state).await
    }

    async fn load_latest(&self, config_hash: &str) -> Result<Option<MigrationState>> {
        MssqlStateBackend::load_latest(self, config_hash).await
    }

    async fn get_last_sync_timestamp(&self, table_name: &str) -> Result<Option<DateTime<Utc>>> {
        MssqlStateBackend::get_last_sync_timestamp(self, table_name).await
    }

    fn backend_type(&self) -> &'static str {
        MssqlStateBackend::backend_type(self)
    }
}
