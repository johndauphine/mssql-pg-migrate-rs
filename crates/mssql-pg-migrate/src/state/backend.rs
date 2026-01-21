//! State backend trait for migration state storage.
//!
//! The [`StateBackend`] trait defines the interface for persisting migration state.
//! Implementations can store state in various backends:
//!
//! - **PostgreSQL**: `DbStateBackend` in `db.rs`
//! - **MSSQL**: `MssqlStateBackend` in `mssql_db.rs`
//! - **File**: JSON file storage (legacy, in `mod.rs`)
//!
//! # Design Pattern
//!
//! This uses the Strategy pattern to decouple state storage from the orchestrator.
//! The orchestrator works with `Arc<dyn StateBackend>` without knowing the concrete type.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use super::{MigrationState, RunStatus, TaskStatus};
use crate::error::Result;

/// Trait for migration state persistence backends.
///
/// This trait defines the interface for storing and retrieving migration state.
/// Implementations handle the details of how and where state is persisted.
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to allow sharing across async tasks.
///
/// # Example
///
/// ```rust,ignore
/// let backend: Arc<dyn StateBackend> = Arc::new(DbStateBackend::new(pool));
/// backend.init_schema().await?;
/// backend.save(&state).await?;
/// let loaded = backend.load_latest("config_hash").await?;
/// ```
#[async_trait]
pub trait StateBackend: Send + Sync {
    /// Initialize the state storage schema/structure.
    ///
    /// For database backends, this creates the necessary tables and indexes.
    /// For file backends, this might create directories.
    ///
    /// This should be idempotent - safe to call multiple times.
    async fn init_schema(&self) -> Result<()>;

    /// Save a migration state.
    ///
    /// This persists the complete migration state including all table states.
    /// For database backends, this typically uses upsert semantics.
    ///
    /// # Arguments
    ///
    /// * `state` - The migration state to save
    async fn save(&self, state: &MigrationState) -> Result<()>;

    /// Load the latest migration state for a given config hash.
    ///
    /// Returns the most recent migration run that matches the config hash,
    /// or `None` if no matching state exists.
    ///
    /// # Arguments
    ///
    /// * `config_hash` - SHA256 hash of the configuration
    async fn load_latest(&self, config_hash: &str) -> Result<Option<MigrationState>>;

    /// Get the last sync timestamp for a specific table.
    ///
    /// This is used for incremental sync (upsert mode) to determine the
    /// watermark for filtering source rows.
    ///
    /// Returns the most recent completed run's timestamp for this table,
    /// or `None` if no completed run exists.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Full table name (schema.table)
    async fn get_last_sync_timestamp(&self, table_name: &str) -> Result<Option<DateTime<Utc>>>;

    /// Get the backend type name for logging/debugging.
    fn backend_type(&self) -> &'static str;
}

/// Helper function to convert RunStatus to string representation.
pub fn run_status_to_str(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Running => "running",
        RunStatus::Completed => "completed",
        RunStatus::Failed => "failed",
        RunStatus::Cancelled => "cancelled",
    }
}

/// Helper function to parse RunStatus from string.
pub fn str_to_run_status(s: &str) -> Result<RunStatus> {
    use crate::error::MigrateError;
    match s {
        "running" => Ok(RunStatus::Running),
        "completed" => Ok(RunStatus::Completed),
        "failed" => Ok(RunStatus::Failed),
        "cancelled" => Ok(RunStatus::Cancelled),
        _ => Err(MigrateError::Config(format!("Invalid run status: {}", s))),
    }
}

/// Helper function to convert TaskStatus to string representation.
pub fn task_status_to_str(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Pending => "pending",
        TaskStatus::InProgress => "in_progress",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
    }
}

/// Helper function to parse TaskStatus from string.
pub fn str_to_task_status(s: &str) -> Result<TaskStatus> {
    use crate::error::MigrateError;
    match s {
        "pending" => Ok(TaskStatus::Pending),
        "in_progress" => Ok(TaskStatus::InProgress),
        "completed" => Ok(TaskStatus::Completed),
        "failed" => Ok(TaskStatus::Failed),
        _ => Err(MigrateError::Config(format!("Invalid task status: {}", s))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_status_roundtrip() {
        let statuses = [
            RunStatus::Running,
            RunStatus::Completed,
            RunStatus::Failed,
            RunStatus::Cancelled,
        ];

        for status in statuses {
            let s = run_status_to_str(status);
            let parsed = str_to_run_status(s).unwrap();
            assert_eq!(parsed, status);
        }
    }

    #[test]
    fn test_task_status_roundtrip() {
        let statuses = [
            TaskStatus::Pending,
            TaskStatus::InProgress,
            TaskStatus::Completed,
            TaskStatus::Failed,
        ];

        for status in statuses {
            let s = task_status_to_str(status);
            let parsed = str_to_task_status(s).unwrap();
            assert_eq!(parsed, status);
        }
    }

    #[test]
    fn test_invalid_run_status() {
        assert!(str_to_run_status("invalid").is_err());
    }

    #[test]
    fn test_invalid_task_status() {
        assert!(str_to_task_status("invalid").is_err());
    }
}
