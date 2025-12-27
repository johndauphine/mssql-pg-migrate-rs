//! Type definitions for multi-tier batch verification.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

// Re-export types from source module
pub use crate::source::PkValue;

// Re-export BatchVerifyConfig from config module
pub use crate::config::BatchVerifyConfig;

/// Composite primary key (supports single and multi-column PKs).
///
/// This is a newtype wrapper around `Vec<PkValue>` that implements `Hash`
/// for use as a HashMap key in row hash lookups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositePk(pub Vec<PkValue>);

impl CompositePk {
    /// Create a new composite PK from a vector of values.
    pub fn new(values: Vec<PkValue>) -> Self {
        Self(values)
    }

    /// Create a single-column PK.
    pub fn single(value: PkValue) -> Self {
        Self(vec![value])
    }

    /// Get the number of columns in this PK.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the PK is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Get the inner values.
    pub fn values(&self) -> &[PkValue] {
        &self.0
    }

    /// Convert to SQL literal for WHERE clause (e.g., for IN or equality).
    pub fn to_sql_literals(&self) -> Vec<String> {
        self.0.iter().map(|v| v.to_sql_literal()).collect()
    }
}

impl Hash for CompositePk {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Include type discriminator to distinguish Int(1) from String("1").
        // NOTE: If PkValue is extended with new variants, update these discriminators
        // and ensure they remain unique (0=Int, 1=Uuid, 2=String, 3+=future).
        for pk in &self.0 {
            match pk {
                PkValue::Int(v) => {
                    0u8.hash(state);
                    v.hash(state);
                }
                PkValue::Uuid(v) => {
                    1u8.hash(state);
                    v.hash(state);
                }
                PkValue::String(v) => {
                    2u8.hash(state);
                    v.hash(state);
                }
            }
        }
    }
}

impl From<i64> for CompositePk {
    fn from(value: i64) -> Self {
        Self::single(PkValue::Int(value))
    }
}

impl From<PkValue> for CompositePk {
    fn from(value: PkValue) -> Self {
        Self::single(value)
    }
}

impl From<Vec<PkValue>> for CompositePk {
    fn from(values: Vec<PkValue>) -> Self {
        Self::new(values)
    }
}

/// Map of composite PK to row hash for efficient lookup.
pub type CompositeRowHashMap = HashMap<CompositePk, String>;

/// Represents a PK range for batch hashing.
#[derive(Debug, Clone)]
pub struct PkRange {
    /// Minimum primary key value (inclusive).
    pub min_pk: i64,
    /// Maximum primary key value (exclusive).
    pub max_pk: i64,
    /// Tier level for this range.
    pub tier: VerifyTier,
}

impl PkRange {
    /// Create a new PK range.
    pub fn new(min_pk: i64, max_pk: i64, tier: VerifyTier) -> Self {
        Self { min_pk, max_pk, tier }
    }

    /// Subdivide this range into smaller ranges for the next tier.
    pub fn subdivide(&self, batch_size: i64, next_tier: VerifyTier) -> Vec<PkRange> {
        let mut ranges = Vec::new();
        let mut current = self.min_pk;

        while current < self.max_pk {
            let next = (current + batch_size).min(self.max_pk);
            ranges.push(PkRange::new(current, next, next_tier));
            current = next;
        }

        ranges
    }
}

/// Represents a row-number-based range for batch verification.
///
/// Unlike `PkRange`, this uses ROW_NUMBER() positions instead of PK values,
/// allowing verification of tables with any PK type (int, uuid, string, composite).
#[derive(Debug, Clone)]
pub struct RowRange {
    /// Starting row number (1-based, inclusive).
    pub start_row: i64,
    /// Ending row number (exclusive).
    pub end_row: i64,
    /// Tier level for this range.
    pub tier: VerifyTier,
    /// Partition ID for NTILE-based ranges (Tier 1 only).
    pub partition_id: Option<i64>,
}

impl RowRange {
    /// Create a new row range.
    pub fn new(start_row: i64, end_row: i64, tier: VerifyTier) -> Self {
        Self {
            start_row,
            end_row,
            tier,
            partition_id: None,
        }
    }

    /// Create a row range for a NTILE partition.
    pub fn from_partition(partition_id: i64, start_row: i64, end_row: i64) -> Self {
        Self {
            start_row,
            end_row,
            tier: VerifyTier::Coarse,
            partition_id: Some(partition_id),
        }
    }

    /// Number of rows in this range.
    pub fn row_count(&self) -> i64 {
        self.end_row - self.start_row
    }

    /// Subdivide this range into smaller row ranges for the next tier.
    pub fn subdivide(&self, batch_size: i64, next_tier: VerifyTier) -> Vec<RowRange> {
        let mut ranges = Vec::new();
        let mut current = self.start_row;

        while current < self.end_row {
            let next = (current + batch_size).min(self.end_row);
            ranges.push(RowRange::new(current, next, next_tier));
            current = next;
        }

        ranges
    }
}

/// Tier level for verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyTier {
    /// Tier 1 - Coarse verification (~1M rows per range).
    Coarse,
    /// Tier 2 - Fine verification (~10K rows per range).
    Fine,
    /// Tier 3 - Individual row hash comparison.
    Row,
}

impl std::fmt::Display for VerifyTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VerifyTier::Coarse => write!(f, "Tier1-Coarse"),
            VerifyTier::Fine => write!(f, "Tier2-Fine"),
            VerifyTier::Row => write!(f, "Tier3-Row"),
        }
    }
}

/// Result of a batch hash comparison.
#[derive(Debug, Clone)]
pub struct BatchHashResult {
    /// The PK range that was compared.
    pub range: PkRange,
    /// Aggregate hash from source (MSSQL).
    pub source_hash: i64,
    /// Aggregate hash from target (PostgreSQL).
    pub target_hash: i64,
    /// Whether the hashes match.
    pub matches: bool,
    /// Row count in source.
    pub source_row_count: i64,
    /// Row count in target.
    pub target_row_count: i64,
}

impl BatchHashResult {
    /// Check if the range has any differences.
    pub fn has_differences(&self) -> bool {
        !self.matches || self.source_row_count != self.target_row_count
    }
}

/// Result of row-level hash comparison (Tier 3).
#[derive(Debug, Clone, Default)]
pub struct RowHashDiff {
    /// PKs that exist in source but not target (need INSERT).
    pub missing_in_target: Vec<i64>,
    /// PKs that exist in target but not source (may need DELETE).
    pub missing_in_source: Vec<i64>,
    /// PKs that exist in both but have different hashes (need UPDATE).
    pub hash_mismatches: Vec<i64>,
}

impl RowHashDiff {
    /// Total number of differences.
    pub fn total_differences(&self) -> usize {
        self.missing_in_target.len() + self.missing_in_source.len() + self.hash_mismatches.len()
    }

    /// Check if there are any differences.
    pub fn has_differences(&self) -> bool {
        self.total_differences() > 0
    }

    /// Merge another diff into this one.
    pub fn merge(&mut self, other: RowHashDiff) {
        self.missing_in_target.extend(other.missing_in_target);
        self.missing_in_source.extend(other.missing_in_source);
        self.hash_mismatches.extend(other.hash_mismatches);
    }
}

/// Result of row-level hash comparison with composite PK support (Tier 3).
///
/// Unlike `RowHashDiff`, this supports any PK type including composite keys.
#[derive(Debug, Clone, Default)]
pub struct RowHashDiffComposite {
    /// PKs that exist in source but not target (need INSERT).
    pub missing_in_target: Vec<CompositePk>,
    /// PKs that exist in target but not source (may need DELETE).
    pub missing_in_source: Vec<CompositePk>,
    /// PKs that exist in both but have different hashes (need UPDATE).
    pub hash_mismatches: Vec<CompositePk>,
}

impl RowHashDiffComposite {
    /// Total number of differences.
    pub fn total_differences(&self) -> usize {
        self.missing_in_target.len() + self.missing_in_source.len() + self.hash_mismatches.len()
    }

    /// Check if there are any differences.
    pub fn has_differences(&self) -> bool {
        self.total_differences() > 0
    }

    /// Merge another diff into this one.
    pub fn merge(&mut self, other: RowHashDiffComposite) {
        self.missing_in_target.extend(other.missing_in_target);
        self.missing_in_source.extend(other.missing_in_source);
        self.hash_mismatches.extend(other.hash_mismatches);
    }
}

/// Statistics from a sync operation.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncStats {
    /// Rows inserted.
    pub rows_inserted: i64,
    /// Rows updated.
    pub rows_updated: i64,
    /// Rows deleted.
    pub rows_deleted: i64,
}

/// Overall verification result for a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableVerifyResult {
    /// Table name (fully qualified).
    pub table_name: String,
    /// Row count in source.
    pub source_row_count: i64,
    /// Row count in target.
    pub target_row_count: i64,
    /// Number of Tier 1 ranges checked.
    pub tier1_ranges_checked: usize,
    /// Number of Tier 1 ranges that had mismatches.
    pub tier1_ranges_mismatched: usize,
    /// Number of Tier 2 ranges checked.
    pub tier2_ranges_checked: usize,
    /// Number of Tier 2 ranges that had mismatches.
    pub tier2_ranges_mismatched: usize,
    /// Rows that need to be inserted.
    pub rows_to_insert: i64,
    /// Rows that need to be updated.
    pub rows_to_update: i64,
    /// Rows that need to be deleted.
    pub rows_to_delete: i64,
    /// Whether sync was performed.
    pub sync_performed: bool,
    /// Whether the table was skipped (e.g., no single integer PK).
    pub skipped: bool,
    /// Reason for skipping (if skipped).
    pub skip_reason: Option<String>,
    /// Duration in milliseconds.
    pub duration_ms: u64,
}

impl TableVerifyResult {
    /// Check if the table is in sync.
    pub fn is_in_sync(&self) -> bool {
        self.rows_to_insert == 0 && self.rows_to_update == 0 && self.rows_to_delete == 0
    }
}

/// Overall verification result for all tables.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyResult {
    /// Results for each table.
    pub tables: Vec<TableVerifyResult>,
    /// Total tables checked.
    pub tables_checked: usize,
    /// Tables that were in sync.
    pub tables_in_sync: usize,
    /// Tables with differences.
    pub tables_with_differences: usize,
    /// Tables that were skipped (no single integer PK).
    pub tables_skipped: usize,
    /// Total rows that need to be inserted across all tables.
    pub total_rows_to_insert: i64,
    /// Total rows that need to be updated across all tables.
    pub total_rows_to_update: i64,
    /// Total rows that need to be deleted across all tables.
    pub total_rows_to_delete: i64,
    /// Whether sync was performed.
    pub sync_performed: bool,
    /// Total duration in milliseconds.
    pub duration_ms: u64,
}

impl VerifyResult {
    /// Create a new empty result.
    pub fn new() -> Self {
        Self {
            tables: Vec::new(),
            tables_checked: 0,
            tables_in_sync: 0,
            tables_with_differences: 0,
            tables_skipped: 0,
            total_rows_to_insert: 0,
            total_rows_to_update: 0,
            total_rows_to_delete: 0,
            sync_performed: false,
            duration_ms: 0,
        }
    }

    /// Add a table result.
    pub fn add_table(&mut self, result: TableVerifyResult) {
        if result.skipped {
            self.tables_skipped += 1;
        } else {
            self.tables_checked += 1;
            if result.is_in_sync() {
                self.tables_in_sync += 1;
            } else {
                self.tables_with_differences += 1;
            }
            self.total_rows_to_insert += result.rows_to_insert;
            self.total_rows_to_update += result.rows_to_update;
            self.total_rows_to_delete += result.rows_to_delete;
            if result.sync_performed {
                self.sync_performed = true;
            }
        }
        self.tables.push(result);
    }
}

impl Default for VerifyResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Progress update for verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyProgressUpdate {
    /// Current phase: "tier1", "tier2", "tier3", "sync".
    pub phase: String,
    /// Current table being processed.
    pub table: String,
    /// Total number of ranges in current phase.
    pub ranges_total: usize,
    /// Number of ranges completed.
    pub ranges_completed: usize,
    /// Number of mismatches found so far.
    pub mismatches_found: usize,
}

/// Row hash entry for Tier 3 comparison.
#[derive(Debug, Clone)]
pub struct RowHashEntry {
    /// Primary key value.
    pub pk: i64,
    /// Row hash (MD5 hex string).
    pub hash: String,
}

/// Map of PK to row hash for efficient lookup.
pub type RowHashMap = HashMap<i64, String>;
