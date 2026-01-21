//! Transfer job (Command pattern) for encapsulating transfer work units.
//!
//! A TransferJob contains all the information needed to transfer data for
//! a single table or partition. This enables:
//!
//! - Queuing jobs for later execution
//! - Serializing job state for resume capability
//! - Tracking job progress and results

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::config::TargetMode;
use crate::core::schema::Table;
use crate::error::{MigrateError, Result};

/// Date-based incremental sync filter.
///
/// Only sync rows where the specified column is greater than the timestamp (or is NULL).
/// This enables efficient incremental migrations using a "high-water mark" approach.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DateFilter {
    /// Date column name to filter on.
    pub column: String,
    /// Only sync rows where column > timestamp (or column IS NULL).
    pub timestamp: DateTime<Utc>,
}

impl DateFilter {
    /// Create a new DateFilter with validation.
    ///
    /// # Security
    ///
    /// Validates timestamp bounds to prevent injection and invalid date ranges:
    /// - Rejects future timestamps (clock skew or tampering)
    /// - Rejects very old timestamps (likely corruption or tampering)
    /// - Column name validated separately during query building
    pub fn new(column: String, timestamp: DateTime<Utc>) -> Result<Self> {
        let now = Utc::now();

        // Reject timestamps more than 1 hour in the future (allows for clock skew)
        if timestamp > now + chrono::Duration::hours(1) {
            return Err(MigrateError::Config(format!(
                "Invalid sync timestamp for column '{}': timestamp {} is in the future (now: {})",
                column,
                timestamp.to_rfc3339(),
                now.to_rfc3339()
            )));
        }

        // Reject timestamps older than 100 years (likely corruption)
        let min_timestamp = now - chrono::Duration::days(36500); // ~100 years
        if timestamp < min_timestamp {
            return Err(MigrateError::Config(format!(
                "Invalid sync timestamp for column '{}': timestamp {} is too old (more than 100 years)",
                column,
                timestamp.to_rfc3339()
            )));
        }

        Ok(Self { column, timestamp })
    }

    /// Get the timestamp as a validated ISO 8601 string suitable for SQL.
    ///
    /// # Security
    ///
    /// Returns a validated ISO 8601 timestamp that has been bounds-checked.
    /// The ISO 8601 format is guaranteed to not contain SQL metacharacters.
    ///
    /// # Format
    ///
    /// Returns "YYYY-MM-DD HH:MM:SS.fff" format compatible with both:
    /// - SQL Server datetime/datetime2 (timezone-naive)
    /// - PostgreSQL timestamp (with implicit UTC assumption)
    pub fn timestamp_sql_safe(&self) -> String {
        // ISO 8601 format without timezone: YYYY-MM-DD HH:MM:SS.fff
        // Compatible with SQL Server datetime and PostgreSQL timestamp
        // No single quotes, no semicolons, no SQL injection risk
        self.timestamp.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
    }
}

/// A transfer job encapsulating all information needed to transfer a table/partition.
///
/// This is the Command pattern - the job contains everything needed to execute
/// the transfer, allowing jobs to be queued, serialized, and executed independently.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferJob {
    /// Table metadata.
    pub table: Table,

    /// Partition ID (None for non-partitioned transfers).
    pub partition_id: Option<i32>,

    /// Minimum PK value (for keyset pagination).
    pub min_pk: Option<i64>,

    /// Maximum PK value (for keyset pagination).
    pub max_pk: Option<i64>,

    /// Resume from this PK value (for interrupted transfers).
    pub resume_from_pk: Option<i64>,

    /// Target mode for this job.
    pub target_mode: TargetMode,

    /// Target schema name.
    pub target_schema: String,

    /// Date filter for incremental sync (upsert mode only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date_filter: Option<DateFilter>,
}

impl TransferJob {
    /// Create a new transfer job for a table.
    pub fn new(table: Table, target_schema: String, target_mode: TargetMode) -> Self {
        Self {
            table,
            partition_id: None,
            min_pk: None,
            max_pk: None,
            resume_from_pk: None,
            target_mode,
            target_schema,
            date_filter: None,
        }
    }

    /// Set the partition for this job.
    pub fn with_partition(mut self, partition_id: i32, min_pk: Option<i64>, max_pk: Option<i64>) -> Self {
        self.partition_id = Some(partition_id);
        self.min_pk = min_pk;
        self.max_pk = max_pk;
        self
    }

    /// Set the resume point for this job.
    pub fn with_resume_from(mut self, pk: i64) -> Self {
        self.resume_from_pk = Some(pk);
        self
    }

    /// Set the date filter for incremental sync.
    pub fn with_date_filter(mut self, filter: DateFilter) -> Self {
        self.date_filter = Some(filter);
        self
    }

    /// Get the full table name.
    pub fn table_name(&self) -> String {
        self.table.full_name()
    }

    /// Get the column names for this job.
    pub fn column_names(&self) -> Vec<String> {
        self.table.columns.iter().map(|c| c.name.clone()).collect()
    }

    /// Get the column types for this job (pre-computed for O(1) lookup).
    pub fn column_types(&self) -> Vec<String> {
        self.table.columns.iter().map(|c| c.data_type.clone()).collect()
    }

    /// Get the primary key column names.
    pub fn pk_columns(&self) -> &[String] {
        &self.table.primary_key
    }

    /// Check if this job supports keyset pagination.
    pub fn supports_keyset_pagination(&self) -> bool {
        self.table.supports_keyset_pagination()
    }
}

/// Result of executing a transfer job.
#[derive(Debug, Clone, Default)]
pub struct JobResult {
    /// Total rows transferred.
    pub rows_transferred: i64,

    /// Time spent reading from source.
    pub read_time: Duration,

    /// Time spent writing to target.
    pub write_time: Duration,

    /// Total elapsed time.
    pub total_time: Duration,

    /// Last primary key value processed (for resume).
    pub last_pk: Option<i64>,

    /// Whether the job completed successfully.
    pub completed: bool,

    /// Error message if the job failed.
    pub error: Option<String>,
}

impl JobResult {
    /// Create a successful result.
    pub fn success(rows: i64, total_time: Duration) -> Self {
        Self {
            rows_transferred: rows,
            total_time,
            completed: true,
            ..Default::default()
        }
    }

    /// Create a failed result.
    pub fn failure(error: impl Into<String>) -> Self {
        Self {
            error: Some(error.into()),
            completed: false,
            ..Default::default()
        }
    }

    /// Set timing details.
    pub fn with_timing(mut self, read_time: Duration, write_time: Duration) -> Self {
        self.read_time = read_time;
        self.write_time = write_time;
        self
    }

    /// Set the last processed PK for resume capability.
    pub fn with_last_pk(mut self, pk: i64) -> Self {
        self.last_pk = Some(pk);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::schema::Column;

    fn make_test_table() -> Table {
        Table {
            schema: "dbo".to_string(),
            name: "Users".to_string(),
            columns: vec![
                Column {
                    name: "Id".to_string(),
                    data_type: "int".to_string(),
                    max_length: 0,
                    precision: 10,
                    scale: 0,
                    is_nullable: false,
                    is_identity: true,
                    ordinal_pos: 1,
                },
                Column {
                    name: "Name".to_string(),
                    data_type: "varchar".to_string(),
                    max_length: 100,
                    precision: 0,
                    scale: 0,
                    is_nullable: true,
                    is_identity: false,
                    ordinal_pos: 2,
                },
            ],
            primary_key: vec!["Id".to_string()],
            pk_columns: vec![],
            row_count: 1000,
            estimated_row_size: 50,
            indexes: vec![],
            foreign_keys: vec![],
            check_constraints: vec![],
        }
    }

    #[test]
    fn test_transfer_job_creation() {
        let table = make_test_table();
        let job = TransferJob::new(table, "public".to_string(), TargetMode::Truncate);

        assert_eq!(job.table_name(), "dbo.Users");
        assert_eq!(job.column_names(), vec!["Id", "Name"]);
        assert_eq!(job.column_types(), vec!["int", "varchar"]);
        assert_eq!(job.pk_columns(), &["Id"]);
    }

    #[test]
    fn test_transfer_job_with_partition() {
        let table = make_test_table();
        let job = TransferJob::new(table, "public".to_string(), TargetMode::Truncate)
            .with_partition(0, Some(1), Some(1000));

        assert_eq!(job.partition_id, Some(0));
        assert_eq!(job.min_pk, Some(1));
        assert_eq!(job.max_pk, Some(1000));
    }

    #[test]
    fn test_date_filter_validation() {
        let now = Utc::now();

        // Valid timestamp
        let filter = DateFilter::new("updated_at".to_string(), now - chrono::Duration::hours(1));
        assert!(filter.is_ok());

        // Future timestamp should fail
        let future = now + chrono::Duration::hours(2);
        let filter = DateFilter::new("updated_at".to_string(), future);
        assert!(filter.is_err());
    }

    #[test]
    fn test_job_result() {
        let result = JobResult::success(1000, Duration::from_secs(5))
            .with_timing(Duration::from_secs(3), Duration::from_secs(2))
            .with_last_pk(999);

        assert!(result.completed);
        assert_eq!(result.rows_transferred, 1000);
        assert_eq!(result.last_pk, Some(999));
        assert!(result.error.is_none());
    }
}
