//! Multi-tier batch verification for efficient data sync validation.
//!
//! This module implements a 4-tier drill-down approach to efficiently verify
//! data synchronization between MSSQL and PostgreSQL:
//!
//! - **Tier 1 (Coarse)**: Compare aggregate hashes of ~1M row ranges
//! - **Tier 2 (Fine)**: For mismatches, subdivide into ~10K row ranges
//! - **Tier 3 (Row)**: Fetch (PK, row_hash) pairs and compare
//! - **Tier 4 (Sync)**: Transfer only changed rows
//!
//! This approach minimizes network I/O by progressively narrowing down
//! to only the rows that actually differ.

pub mod hash_query;
pub mod normalize;
pub mod types;

// Re-exports
pub use types::{
    BatchHashResult, PkRange, RowHashDiff, RowHashEntry, RowHashMap, SyncStats, TableVerifyResult,
    VerifyProgressUpdate, VerifyResult, VerifyTier,
};

use crate::config::BatchVerifyConfig;
use crate::error::Result;
use crate::source::{MssqlPool, Table};
use crate::target::PgPool;
use hash_query::{
    mssql_batch_hash_query, mssql_pk_bounds_query, mssql_row_hashes_query,
    postgres_batch_hash_query, postgres_pk_bounds_query, postgres_row_hashes_query,
};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Engine for multi-tier batch verification.
pub struct VerifyEngine {
    source: Arc<MssqlPool>,
    target: Arc<PgPool>,
    config: BatchVerifyConfig,
    row_hash_column: String,
    progress_tx: Option<mpsc::Sender<VerifyProgressUpdate>>,
}

impl VerifyEngine {
    /// Create a new verification engine.
    pub fn new(
        source: Arc<MssqlPool>,
        target: Arc<PgPool>,
        config: BatchVerifyConfig,
        row_hash_column: String,
    ) -> Self {
        Self {
            source,
            target,
            config,
            row_hash_column,
            progress_tx: None,
        }
    }

    /// Set progress channel for updates.
    pub fn with_progress(mut self, tx: mpsc::Sender<VerifyProgressUpdate>) -> Self {
        self.progress_tx = Some(tx);
        self
    }

    /// Send progress update if channel is configured.
    async fn send_progress(&self, update: VerifyProgressUpdate) {
        if let Some(tx) = &self.progress_tx {
            let _ = tx.send(update).await;
        }
    }

    /// Verify a single table using multi-tier approach.
    pub async fn verify_table(
        &self,
        table: &Table,
        source_schema: &str,
        target_schema: &str,
        auto_sync: bool,
    ) -> Result<TableVerifyResult> {
        let start = Instant::now();
        let table_name = format!("{}.{}", source_schema, table.name);

        info!("Starting verification for table: {}", table_name);

        // Check if table has a single integer PK (required for range-based verification)
        if !table.supports_keyset_pagination() {
            warn!(
                "Table {} does not have a single integer PK, skipping verification",
                table_name
            );
            return Ok(TableVerifyResult {
                table_name,
                source_row_count: table.row_count,
                target_row_count: 0,
                tier1_ranges_checked: 0,
                tier1_ranges_mismatched: 0,
                tier2_ranges_checked: 0,
                tier2_ranges_mismatched: 0,
                rows_to_insert: 0,
                rows_to_update: 0,
                rows_to_delete: 0,
                sync_performed: false,
                duration_ms: start.elapsed().as_millis() as u64,
            });
        }

        let pk_column = &table.primary_key[0];

        // Get PK bounds from both databases
        let source_bounds_query = mssql_pk_bounds_query(source_schema, &table.name, pk_column);
        let target_bounds_query = postgres_pk_bounds_query(target_schema, &table.name, pk_column);

        let (source_min, source_max, source_count) =
            self.source.get_pk_bounds(&source_bounds_query).await?;
        let (target_min, target_max, target_count) =
            self.target.get_pk_bounds(&target_bounds_query).await?;

        info!(
            "Table {} - Source: {} rows (PK {}..{}), Target: {} rows (PK {}..{})",
            table_name, source_count, source_min, source_max, target_count, target_min, target_max
        );

        // Use the union of PK ranges for verification
        let min_pk = source_min.min(target_min);
        let max_pk = source_max.max(target_max) + 1; // +1 because max_pk is exclusive

        // Tier 1: Coarse verification
        let tier1_ranges = self.create_ranges(min_pk, max_pk, self.config.tier1_batch_size);
        let tier1_mismatches = self
            .verify_tier(
                table,
                source_schema,
                target_schema,
                pk_column,
                &tier1_ranges,
                VerifyTier::Coarse,
            )
            .await?;

        let tier1_checked = tier1_ranges.len();
        let tier1_mismatched = tier1_mismatches.len();

        info!(
            "Tier 1: {}/{} ranges mismatched",
            tier1_mismatched, tier1_checked
        );

        if tier1_mismatches.is_empty() {
            info!("Table {} is fully synchronized", table_name);
            return Ok(TableVerifyResult {
                table_name,
                source_row_count: source_count,
                target_row_count: target_count,
                tier1_ranges_checked: tier1_checked,
                tier1_ranges_mismatched: 0,
                tier2_ranges_checked: 0,
                tier2_ranges_mismatched: 0,
                rows_to_insert: 0,
                rows_to_update: 0,
                rows_to_delete: 0,
                sync_performed: false,
                duration_ms: start.elapsed().as_millis() as u64,
            });
        }

        // Tier 2: Fine verification on mismatched ranges
        let mut tier2_ranges = Vec::new();
        for range in &tier1_mismatches {
            tier2_ranges.extend(range.subdivide(self.config.tier2_batch_size, VerifyTier::Fine));
        }

        let tier2_mismatches = self
            .verify_tier(
                table,
                source_schema,
                target_schema,
                pk_column,
                &tier2_ranges,
                VerifyTier::Fine,
            )
            .await?;

        let tier2_checked = tier2_ranges.len();
        let tier2_mismatched = tier2_mismatches.len();

        info!(
            "Tier 2: {}/{} ranges mismatched",
            tier2_mismatched, tier2_checked
        );

        // Tier 3: Row-level hash comparison
        let mut total_diff = RowHashDiff::default();
        for range in &tier2_mismatches {
            let diff = self
                .compare_row_hashes(table, source_schema, target_schema, pk_column, range)
                .await?;
            total_diff.merge(diff);
        }

        info!(
            "Tier 3: {} inserts, {} updates, {} deletes needed",
            total_diff.missing_in_target.len(),
            total_diff.hash_mismatches.len(),
            total_diff.missing_in_source.len()
        );

        let rows_to_insert = total_diff.missing_in_target.len() as i64;
        let rows_to_update = total_diff.hash_mismatches.len() as i64;
        let rows_to_delete = total_diff.missing_in_source.len() as i64;

        // Tier 4: Sync if requested
        let sync_performed = if auto_sync && total_diff.has_differences() {
            info!("Tier 4: Syncing {} changed rows", total_diff.total_differences());
            // TODO: Implement actual sync using existing upsert mechanism
            // For now, just report what would be synced
            false
        } else {
            false
        };

        Ok(TableVerifyResult {
            table_name,
            source_row_count: source_count,
            target_row_count: target_count,
            tier1_ranges_checked: tier1_checked,
            tier1_ranges_mismatched: tier1_mismatched,
            tier2_ranges_checked: tier2_checked,
            tier2_ranges_mismatched: tier2_mismatched,
            rows_to_insert,
            rows_to_update,
            rows_to_delete,
            sync_performed,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    /// Create PK ranges for verification.
    fn create_ranges(&self, min_pk: i64, max_pk: i64, batch_size: i64) -> Vec<PkRange> {
        let mut ranges = Vec::new();
        let mut current = min_pk;

        while current < max_pk {
            let next = (current + batch_size).min(max_pk);
            ranges.push(PkRange::new(current, next, VerifyTier::Coarse));
            current = next;
        }

        ranges
    }

    /// Verify a tier and return mismatched ranges.
    async fn verify_tier(
        &self,
        table: &Table,
        source_schema: &str,
        target_schema: &str,
        pk_column: &str,
        ranges: &[PkRange],
        tier: VerifyTier,
    ) -> Result<Vec<PkRange>> {
        let mut mismatches = Vec::new();

        let source_query = mssql_batch_hash_query(source_schema, table, pk_column);
        let target_query = postgres_batch_hash_query(target_schema, table, pk_column);

        for (i, range) in ranges.iter().enumerate() {
            // Execute queries in parallel
            let (source_result, target_result) = tokio::join!(
                self.source
                    .execute_batch_hash(&source_query, range.min_pk, range.max_pk),
                self.target
                    .execute_batch_hash(&target_query, range.min_pk, range.max_pk)
            );

            let (source_hash, source_count) = source_result?;
            let (target_hash, target_count) = target_result?;

            // Check for mismatch
            if source_hash != target_hash || source_count != target_count {
                debug!(
                    "{} range {}..{}: hash mismatch (src: {}/{}, tgt: {}/{})",
                    tier, range.min_pk, range.max_pk, source_hash, source_count, target_hash, target_count
                );
                mismatches.push(PkRange::new(range.min_pk, range.max_pk, tier));
            }

            // Send progress update
            self.send_progress(VerifyProgressUpdate {
                phase: format!("{}", tier),
                table: table.name.clone(),
                ranges_total: ranges.len(),
                ranges_completed: i + 1,
                mismatches_found: mismatches.len(),
            })
            .await;
        }

        Ok(mismatches)
    }

    /// Compare row hashes between source and target for a specific range.
    async fn compare_row_hashes(
        &self,
        table: &Table,
        source_schema: &str,
        target_schema: &str,
        pk_column: &str,
        range: &PkRange,
    ) -> Result<RowHashDiff> {
        let source_query = mssql_row_hashes_query(source_schema, table, pk_column);
        let target_query = postgres_row_hashes_query(
            target_schema,
            &table.name,
            pk_column,
            Some(&self.row_hash_column),
        );

        // Fetch hashes from both databases in parallel
        let (source_hashes, target_hashes) = tokio::join!(
            self.source
                .fetch_row_hashes(&source_query, range.min_pk, range.max_pk),
            self.target
                .fetch_row_hashes_for_verify(&target_query, range.min_pk, range.max_pk)
        );

        let source_hashes = source_hashes?;
        let target_hashes = target_hashes?;

        let mut diff = RowHashDiff::default();

        // Find rows in source but not in target (need INSERT)
        // Find rows with different hashes (need UPDATE)
        for (pk, source_hash) in &source_hashes {
            match target_hashes.get(pk) {
                None => diff.missing_in_target.push(*pk),
                Some(target_hash) if target_hash != source_hash => {
                    diff.hash_mismatches.push(*pk);
                }
                _ => {} // Hashes match, no action needed
            }
        }

        // Find rows in target but not in source (may need DELETE)
        for pk in target_hashes.keys() {
            if !source_hashes.contains_key(pk) {
                diff.missing_in_source.push(*pk);
            }
        }

        Ok(diff)
    }
}
