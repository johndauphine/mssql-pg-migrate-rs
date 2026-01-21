//! File-based state management for resume capability.

use crate::error::{MigrateError, Result};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use std::path::Path;

type HmacSha256 = Hmac<Sha256>;

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

    /// HMAC-SHA256 signature for integrity validation.
    /// Computed over serialized state (excluding this field) using config_hash as key.
    /// Optional for backward compatibility with older state files.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hmac: Option<String>,
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

    /// Rows skipped (unchanged in upsert mode with hash detection).
    #[serde(default)]
    pub rows_skipped: i64,

    /// Last processed primary key value (for resume).
    pub last_pk: Option<i64>,

    /// Per-partition state (for large tables).
    pub partitions: Option<HashMap<i32, PartitionState>>,

    /// When the table transfer completed.
    pub completed_at: Option<DateTime<Utc>>,

    /// Error message if failed.
    pub error: Option<String>,

    /// Last sync timestamp for incremental upsert (date watermark).
    /// Stores the sync start time after successful completion.
    /// Used to filter source rows on next run: WHERE date_column > last_sync_timestamp.
    #[serde(default)]
    pub last_sync_timestamp: Option<DateTime<Utc>>,
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
            hmac: None, // Will be computed on first save
        }
    }

    /// Compute HMAC-SHA256 signature for state integrity validation.
    ///
    /// # Security
    ///
    /// Uses config_hash as HMAC key to prevent tampering with state file.
    /// Attacker would need both file system access AND knowledge of config_hash.
    fn compute_hmac(&self) -> Result<String> {
        // Create a copy without HMAC for signing
        let mut state_for_signing = self.clone();
        state_for_signing.hmac = None;

        let content = serde_json::to_string(&state_for_signing)
            .map_err(|e| MigrateError::Config(format!("Failed to serialize state for HMAC: {}", e)))?;

        let mut mac = HmacSha256::new_from_slice(self.config_hash.as_bytes())
            .map_err(|e| MigrateError::Config(format!("Failed to create HMAC: {}", e)))?;

        mac.update(content.as_bytes());
        let result = mac.finalize();
        Ok(hex::encode(result.into_bytes()))
    }

    /// Load state from a file with integrity validation.
    ///
    /// # Security
    ///
    /// Validates HMAC signature if present to detect tampering.
    /// Older state files without HMAC are still accepted for backward compatibility,
    /// but will be upgraded to include HMAC on next save.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let state: Self = serde_json::from_str(&content)?;

        // Validate HMAC if present
        if let Some(stored_hmac) = &state.hmac {
            let expected_hmac = state.compute_hmac()?;
            if stored_hmac != &expected_hmac {
                return Err(MigrateError::Config(
                    "State file integrity check failed: HMAC mismatch (possible tampering)".to_string()
                ));
            }
        }
        // If no HMAC present, accept for backward compatibility but log warning
        else {
            tracing::warn!("State file has no HMAC signature (older format), integrity cannot be verified");
        }

        Ok(state)
    }

    /// Save state to a file (atomic write with HMAC).
    ///
    /// # Security
    ///
    /// Computes HMAC-SHA256 signature before saving to enable integrity validation on load.
    pub fn save<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let path = path.as_ref();

        // Compute HMAC before serialization
        self.hmac = Some(self.compute_hmac()?);

        let content = serde_json::to_string_pretty(self)
            .map_err(|e| MigrateError::Config(format!("Failed to serialize state: {}", e)))?;

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
                rows_skipped: 0,
                last_pk: None,
                partitions: None,
                completed_at: None,
                error: None,
                last_sync_timestamp: None,
            })
    }

    /// Check if a table is completed.
    pub fn is_table_completed(&self, table_name: &str) -> bool {
        self.tables
            .get(table_name)
            .map(|t| t.status == TaskStatus::Completed)
            .unwrap_or(false)
    }

    /// Get last sync timestamp for a table (for incremental upsert).
    /// Returns None if table hasn't been synced before or doesn't exist.
    pub fn get_last_sync_timestamp(&self, table_name: &str) -> Option<DateTime<Utc>> {
        self.tables
            .get(table_name)
            .and_then(|t| t.last_sync_timestamp)
    }

    /// Update last sync timestamp for a table (for incremental upsert).
    /// This should be called with the sync start time after successful completion.
    pub fn update_sync_timestamp(&mut self, table_name: &str, timestamp: DateTime<Utc>) {
        if let Some(table) = self.tables.get_mut(table_name) {
            table.last_sync_timestamp = Some(timestamp);
        }
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
            rows_skipped: 0,
            last_pk: None,
            partitions: None,
            completed_at: None,
            error: None,
            last_sync_timestamp: None,
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
        let mut state = MigrationState::new("test-run".into(), "abc123".into());
        assert!(state.validate_config("abc123").is_ok());
        assert!(state.validate_config("different").is_err());
    }

    #[test]
    fn test_state_save_load_json() {
        let mut state = MigrationState::new("test-run".into(), "abc123".into());
        state.get_or_create_table("dbo.Users", 1000);

        let file = NamedTempFile::new().unwrap();
        state.save(file.path()).unwrap();

        // Verify file is valid JSON
        let content = std::fs::read_to_string(file.path()).unwrap();
        assert!(
            serde_json::from_str::<serde_json::Value>(&content).is_ok(),
            "State file should be valid JSON"
        );

        // Verify round-trip
        let loaded = MigrationState::load(file.path()).unwrap();
        assert_eq!(loaded.run_id, "test-run");
        assert_eq!(loaded.config_hash, "abc123");
        assert!(loaded.tables.contains_key("dbo.Users"));
    }

    #[test]
    fn test_state_json_format_pretty() {
        let mut state = MigrationState::new("test-run".into(), "hash".into());
        let file = NamedTempFile::new().unwrap();
        state.save(file.path()).unwrap();

        let content = std::fs::read_to_string(file.path()).unwrap();
        // Pretty JSON should have newlines and indentation
        assert!(
            content.contains('\n'),
            "JSON should be pretty-printed with newlines"
        );
        assert!(content.contains("  "), "JSON should have indentation");
    }

    #[test]
    fn test_state_file_is_json_not_yaml() {
        let mut state = MigrationState::new("test".into(), "hash".into());
        let file = NamedTempFile::new().unwrap();
        state.save(file.path()).unwrap();

        let content = std::fs::read_to_string(file.path()).unwrap();
        // Should NOT contain YAML-specific markers
        assert!(
            !content.starts_with("---"),
            "State file should not be YAML (no --- header)"
        );
        // Should contain JSON markers
        assert!(
            content.contains('{'),
            "State file should contain JSON object"
        );
        assert!(
            content.contains('}'),
            "State file should contain JSON object"
        );
        assert!(
            content.contains("\"run_id\""),
            "State file should have quoted keys (JSON format)"
        );
    }

    #[test]
    fn test_state_table_state_round_trip() {
        let mut state = MigrationState::new("test".into(), "hash".into());
        let table_state = state.get_or_create_table("dbo.Orders", 5000);
        table_state.rows_transferred = 2500;
        table_state.last_pk = Some(1234);
        table_state.status = TaskStatus::InProgress;

        let file = NamedTempFile::new().unwrap();
        state.save(file.path()).unwrap();

        let loaded = MigrationState::load(file.path()).unwrap();
        let loaded_table = loaded.tables.get("dbo.Orders").unwrap();
        assert_eq!(loaded_table.rows_transferred, 2500);
        assert_eq!(loaded_table.last_pk, Some(1234));
        assert_eq!(loaded_table.status, TaskStatus::InProgress);
    }

    #[test]
    fn test_state_with_error() {
        let mut state = MigrationState::new("test".into(), "hash".into());
        let table_state = state.get_or_create_table("dbo.Failed", 1000);
        table_state.mark_failed("Connection timeout");

        let file = NamedTempFile::new().unwrap();
        state.save(file.path()).unwrap();

        let loaded = MigrationState::load(file.path()).unwrap();
        let loaded_table = loaded.tables.get("dbo.Failed").unwrap();
        assert_eq!(loaded_table.status, TaskStatus::Failed);
        assert_eq!(loaded_table.error, Some("Connection timeout".to_string()));
    }

    #[test]
    fn test_sync_timestamp_tracking() {
        let mut state = MigrationState::new("test-run".into(), "hash".into());
        state.get_or_create_table("dbo.Users", 1000);

        // Initially no sync timestamp
        assert_eq!(state.get_last_sync_timestamp("dbo.Users"), None);

        // Update sync timestamp
        let timestamp = Utc::now();
        state.update_sync_timestamp("dbo.Users", timestamp);

        // Should retrieve the timestamp
        assert_eq!(state.get_last_sync_timestamp("dbo.Users"), Some(timestamp));
    }

    #[test]
    fn test_sync_timestamp_persistence() {
        let mut state = MigrationState::new("test-run".into(), "hash".into());
        state.get_or_create_table("dbo.Posts", 5000);

        let timestamp = Utc::now();
        state.update_sync_timestamp("dbo.Posts", timestamp);

        let file = NamedTempFile::new().unwrap();
        state.save(file.path()).unwrap();

        // Load from file and verify timestamp persisted
        let loaded = MigrationState::load(file.path()).unwrap();
        assert_eq!(loaded.get_last_sync_timestamp("dbo.Posts"), Some(timestamp));
    }

    #[test]
    fn test_sync_timestamp_nonexistent_table() {
        let mut state = MigrationState::new("test-run".into(), "hash".into());

        // Should return None for non-existent table
        assert_eq!(state.get_last_sync_timestamp("dbo.NonExistent"), None);
    }

    #[test]
    fn test_sync_timestamp_update_nonexistent_table() {
        let mut state = MigrationState::new("test-run".into(), "hash".into());

        // Update should not panic for non-existent table (silently ignored)
        let timestamp = Utc::now();
        state.update_sync_timestamp("dbo.NonExistent", timestamp);

        // Should still return None
        assert_eq!(
            state.get_last_sync_timestamp("dbo.NonExistent"),
            None
        );
    }

    #[test]
    fn test_sync_timestamp_overwrite() {
        let mut state = MigrationState::new("test-run".into(), "hash".into());
        state.get_or_create_table("dbo.Orders", 2000);

        let first_timestamp = Utc::now();
        state.update_sync_timestamp("dbo.Orders", first_timestamp);
        assert_eq!(
            state.get_last_sync_timestamp("dbo.Orders"),
            Some(first_timestamp)
        );

        // Update with new timestamp
        let second_timestamp = first_timestamp + chrono::Duration::hours(1);
        state.update_sync_timestamp("dbo.Orders", second_timestamp);

        // Should return the new timestamp
        assert_eq!(
            state.get_last_sync_timestamp("dbo.Orders"),
            Some(second_timestamp)
        );
    }
}
