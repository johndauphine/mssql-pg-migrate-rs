//! File-based state management for resume capability.

use crate::error::{MigrateError, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Migration state for resume capability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationState {
    /// Unique run identifier.
    pub run_id: String,

    /// SHA256 hash of the configuration.
    pub config_hash: String,

    /// When the migration started.
    pub started_at: DateTime<Utc>,

    /// Current run status.
    pub status: RunStatus,

    /// Per-table state.
    pub tables: HashMap<String, TableState>,

    /// When the migration completed (if finished).
    pub completed_at: Option<DateTime<Utc>>,
}

/// Overall run status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// Per-table state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableState {
    /// Task status.
    pub status: TaskStatus,

    /// Total rows in the table.
    pub rows_total: i64,

    /// Rows transferred so far.
    pub rows_transferred: i64,

    /// Last processed primary key value (for resume).
    pub last_pk: Option<i64>,

    /// Per-partition state (for large tables).
    pub partitions: Option<HashMap<i32, PartitionState>>,

    /// When the table transfer completed.
    pub completed_at: Option<DateTime<Utc>>,

    /// Error message if failed.
    pub error: Option<String>,
}

/// Per-partition state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionState {
    /// Partition status.
    pub status: TaskStatus,

    /// Last processed primary key value.
    pub last_pk: Option<i64>,

    /// Rows transferred in this partition.
    pub rows_transferred: i64,
}

/// Task status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

impl MigrationState {
    /// Create a new migration state.
    pub fn new(run_id: String, config_hash: String) -> Self {
        Self {
            run_id,
            config_hash,
            started_at: Utc::now(),
            status: RunStatus::Running,
            tables: HashMap::new(),
            completed_at: None,
        }
    }

    /// Load state from a file.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let state: Self = serde_json::from_str(&content)?;
        Ok(state)
    }

    /// Save state to a file (atomic write).
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();
        let content = serde_json::to_string_pretty(self)?;

        // Atomic write: write to temp file, then rename
        let temp_path = path.with_extension("tmp");
        std::fs::write(&temp_path, &content)?;
        std::fs::rename(&temp_path, path)?;

        Ok(())
    }

    /// Validate that the config hash matches for resume.
    pub fn validate_config(&self, config_hash: &str) -> Result<()> {
        if self.config_hash != config_hash {
            return Err(MigrateError::ConfigChanged);
        }
        Ok(())
    }

    /// Get or create table state.
    pub fn get_or_create_table(&mut self, table_name: &str, rows_total: i64) -> &mut TableState {
        self.tables
            .entry(table_name.to_string())
            .or_insert_with(|| TableState {
                status: TaskStatus::Pending,
                rows_total,
                rows_transferred: 0,
                last_pk: None,
                partitions: None,
                completed_at: None,
                error: None,
            })
    }

    /// Check if a table is completed.
    pub fn is_table_completed(&self, table_name: &str) -> bool {
        self.tables
            .get(table_name)
            .map(|t| t.status == TaskStatus::Completed)
            .unwrap_or(false)
    }

    /// Mark the migration as completed.
    pub fn mark_completed(&mut self) {
        self.status = RunStatus::Completed;
        self.completed_at = Some(Utc::now());
    }

    /// Mark the migration as failed.
    pub fn mark_failed(&mut self) {
        self.status = RunStatus::Failed;
        self.completed_at = Some(Utc::now());
    }
}

impl TableState {
    /// Create a new table state.
    pub fn new(rows_total: i64) -> Self {
        Self {
            status: TaskStatus::Pending,
            rows_total,
            rows_transferred: 0,
            last_pk: None,
            partitions: None,
            completed_at: None,
            error: None,
        }
    }

    /// Mark the table as in progress.
    pub fn mark_in_progress(&mut self) {
        self.status = TaskStatus::InProgress;
    }

    /// Mark the table as completed.
    pub fn mark_completed(&mut self) {
        self.status = TaskStatus::Completed;
        self.completed_at = Some(Utc::now());
    }

    /// Mark the table as failed.
    pub fn mark_failed(&mut self, error: &str) {
        self.status = TaskStatus::Failed;
        self.error = Some(error.to_string());
    }

    /// Update progress.
    pub fn update_progress(&mut self, rows_transferred: i64, last_pk: Option<i64>) {
        self.rows_transferred = rows_transferred;
        self.last_pk = last_pk;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_state_save_load() {
        let mut state = MigrationState::new("test-run".into(), "abc123".into());
        state.get_or_create_table("dbo.Users", 1000);

        let file = NamedTempFile::new().unwrap();
        state.save(file.path()).unwrap();

        let loaded = MigrationState::load(file.path()).unwrap();
        assert_eq!(loaded.run_id, "test-run");
        assert_eq!(loaded.config_hash, "abc123");
        assert!(loaded.tables.contains_key("dbo.Users"));
    }

    #[test]
    fn test_config_validation() {
        let state = MigrationState::new("test-run".into(), "abc123".into());
        assert!(state.validate_config("abc123").is_ok());
        assert!(state.validate_config("different").is_err());
    }
}
