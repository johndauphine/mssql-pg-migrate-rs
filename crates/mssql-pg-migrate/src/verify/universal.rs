//! Universal verification engine supporting any primary key type.
//!
//! This engine uses NTILE and ROW_NUMBER based queries instead of PK-range queries,
//! enabling verification of tables with:
//! - Integer PKs (existing behavior)
//! - UUID/GUID PKs
//! - String PKs (VARCHAR, NVARCHAR)
//! - Composite PKs (multiple columns)
//!
//! # Performance Notes
//!
//! ROW_NUMBER queries require sorting the entire table for each query, which can
//! be expensive for very large tables. Ensure appropriate indexes exist on PK
//! columns to optimize the ORDER BY. The NTILE approach mitigates this by only
//! requiring one full-table scan at Tier 1; subsequent tiers only scan mismatched
//! partitions.

use crate::config::BatchVerifyConfig;
use crate::error::Result;
use crate::source::{MssqlPool, Table};
use crate::target::{calculate_row_hash, PgPool, SqlValue};
use crate::verify::hash_query::{
    mssql_ntile_partition_query, mssql_row_count_with_rownum_query,
    mssql_row_hashes_with_rownum_query, mssql_total_row_count_query,
    postgres_ntile_partition_query, postgres_row_count_with_rownum_query,
    postgres_row_hashes_with_rownum_query, postgres_total_row_count_query,
};
use crate::verify::{
    CompositePk, RowHashDiffComposite, RowRange, SyncStats,
    TableVerifyResult, VerifyProgressUpdate, VerifyTier,
};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Engine for multi-tier batch verification with universal PK support.
///
/// Unlike the original `VerifyEngine`, this supports any primary key type
/// by using NTILE partitioning and ROW_NUMBER-based ranges.
pub struct UniversalVerifyEngine {
    source: Arc<MssqlPool>,
    target: Arc<PgPool>,
    config: BatchVerifyConfig,
    row_hash_column: String,
    progress_tx: Option<mpsc::Sender<VerifyProgressUpdate>>,
}

impl UniversalVerifyEngine {
    /// Create a new universal verification engine.
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

    /// Verify a single table using multi-tier approach with any PK type.
    pub async fn verify_table(
        &self,
        table: &Table,
        source_schema: &str,
        target_schema: &str,
        auto_sync: bool,
    ) -> Result<TableVerifyResult> {
        let start = Instant::now();
        let table_name = format!("{}.{}", source_schema, table.name);

        info!("Starting universal verification for table: {}", table_name);

        // Check if table has a primary key (any type is OK)
        if table.primary_key.is_empty() {
            warn!(
                "Table {} has no primary key, skipping verification",
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
                skipped: true,
                skip_reason: Some("Table has no primary key".to_string()),
                duration_ms: start.elapsed().as_millis() as u64,
            });
        }

        let pk_columns = &table.primary_key;

        // Get total row counts from both databases
        let source_count_sql = mssql_total_row_count_query(source_schema, &table.name);
        let target_count_sql = postgres_total_row_count_query(target_schema, &table.name);

        let (source_count, target_count) = tokio::join!(
            self.source.get_total_row_count(&source_count_sql),
            self.target.get_total_row_count(&target_count_sql)
        );

        let source_count = source_count?;
        let target_count = target_count?;

        info!(
            "Table {} - Source: {} rows, Target: {} rows",
            table_name, source_count, target_count
        );

        // Calculate number of Tier 1 partitions
        let total_rows = source_count.max(target_count);
        if total_rows == 0 {
            info!("Table {} is empty in both databases", table_name);
            return Ok(TableVerifyResult {
                table_name,
                source_row_count: source_count,
                target_row_count: target_count,
                tier1_ranges_checked: 0,
                tier1_ranges_mismatched: 0,
                tier2_ranges_checked: 0,
                tier2_ranges_mismatched: 0,
                rows_to_insert: 0,
                rows_to_update: 0,
                rows_to_delete: 0,
                sync_performed: false,
                skipped: false,
                skip_reason: None,
                duration_ms: start.elapsed().as_millis() as u64,
            });
        }

        let num_partitions = ((total_rows + self.config.tier1_batch_size - 1)
            / self.config.tier1_batch_size)
            .max(1);

        // Tier 1: Get NTILE partition counts from both databases
        let source_ntile_sql =
            mssql_ntile_partition_query(source_schema, &table.name, pk_columns, num_partitions);
        let target_ntile_sql =
            postgres_ntile_partition_query(target_schema, &table.name, pk_columns, num_partitions);

        let (source_partitions, target_partitions) = tokio::join!(
            self.source.execute_ntile_partition_query(&source_ntile_sql),
            self.target.execute_ntile_partition_query(&target_ntile_sql)
        );

        let source_partitions = source_partitions?;
        let target_partitions = target_partitions?;

        // Convert partition results to RowRanges
        let tier1_ranges = self.partitions_to_row_ranges(&source_partitions, &target_partitions);

        // Compare Tier 1 partition counts
        let tier1_mismatches = self
            .compare_partition_counts(&source_partitions, &target_partitions, &tier1_ranges)
            .await;

        let tier1_checked = tier1_ranges.len();
        let tier1_mismatched = tier1_mismatches.len();

        info!(
            "Tier 1: {}/{} partitions mismatched",
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
                skipped: false,
                skip_reason: None,
                duration_ms: start.elapsed().as_millis() as u64,
            });
        }

        // Tier 2: Fine verification on mismatched ranges using ROW_NUMBER
        let mut tier2_ranges = Vec::new();
        for range in &tier1_mismatches {
            tier2_ranges.extend(range.subdivide(self.config.tier2_batch_size, VerifyTier::Fine));
        }

        let tier2_mismatches = self
            .verify_tier_with_rownum(
                table,
                source_schema,
                target_schema,
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

        // Tier 3: Row-level hash comparison with composite PK support
        let mut total_diff = RowHashDiffComposite::default();
        for range in &tier2_mismatches {
            let diff = self
                .compare_row_hashes_composite(table, source_schema, target_schema, range)
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
            info!(
                "Tier 4: Syncing {} changed rows",
                total_diff.total_differences()
            );
            self.sync_differences_composite(table, source_schema, target_schema, &total_diff)
                .await?;
            true
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
            skipped: false,
            skip_reason: None,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    /// Convert NTILE partition results to RowRanges.
    ///
    /// # Design Note: Using max(source, target) for partition sizes
    ///
    /// We use the maximum row count between source and target for each partition.
    /// This is intentional: when databases have different row counts, the ROW_NUMBER
    /// queries in Tier 2/3 will naturally return fewer results for the database with
    /// fewer rows. The comparison logic in `compare_partition_counts` and
    /// `compare_row_hashes_composite` correctly handles this asymmetry by detecting
    /// rows that exist in one database but not the other.
    fn partitions_to_row_ranges(
        &self,
        source_partitions: &[(i64, i64)],
        target_partitions: &[(i64, i64)],
    ) -> Vec<RowRange> {
        // Use the max of source/target partitions to ensure we check all rows
        let max_partitions = source_partitions.len().max(target_partitions.len());

        let mut ranges = Vec::with_capacity(max_partitions);
        let mut current_row = 1i64;

        for i in 0..max_partitions {
            // Use max to ensure we cover all rows from both databases.
            // ROW_NUMBER queries handle the case where one database has fewer rows.
            let source_count = source_partitions.get(i).map(|(_, c)| *c).unwrap_or(0);
            let target_count = target_partitions.get(i).map(|(_, c)| *c).unwrap_or(0);
            let partition_rows = source_count.max(target_count);

            if partition_rows > 0 {
                ranges.push(RowRange::from_partition(
                    (i + 1) as i64,
                    current_row,
                    current_row + partition_rows,
                ));
                current_row += partition_rows;
            }
        }

        ranges
    }

    /// Compare partition counts and return mismatched ranges.
    async fn compare_partition_counts(
        &self,
        source_partitions: &[(i64, i64)],
        target_partitions: &[(i64, i64)],
        ranges: &[RowRange],
    ) -> Vec<RowRange> {
        let mut mismatches = Vec::new();

        // Create maps for easy lookup
        let source_map: std::collections::HashMap<i64, i64> =
            source_partitions.iter().cloned().collect();
        let target_map: std::collections::HashMap<i64, i64> =
            target_partitions.iter().cloned().collect();

        for range in ranges {
            if let Some(partition_id) = range.partition_id {
                let source_count = source_map.get(&partition_id).copied().unwrap_or(0);
                let target_count = target_map.get(&partition_id).copied().unwrap_or(0);

                if source_count != target_count {
                    debug!(
                        "Partition {}: count mismatch (src: {}, tgt: {})",
                        partition_id, source_count, target_count
                    );
                    mismatches.push(range.clone());
                }
            }
        }

        mismatches
    }

    /// Verify a tier using ROW_NUMBER-based count queries.
    async fn verify_tier_with_rownum(
        &self,
        table: &Table,
        source_schema: &str,
        target_schema: &str,
        ranges: &[RowRange],
        tier: VerifyTier,
    ) -> Result<Vec<RowRange>> {
        let mut mismatches = Vec::new();

        let pk_columns = &table.primary_key;

        // Build count queries
        let source_count_sql =
            mssql_row_count_with_rownum_query(source_schema, &table.name, pk_columns);
        let target_count_sql =
            postgres_row_count_with_rownum_query(target_schema, &table.name, pk_columns);

        for (i, range) in ranges.iter().enumerate() {
            // Execute count queries in parallel
            let (source_result, target_result) = tokio::join!(
                self.source
                    .execute_count_query_with_rownum(&source_count_sql, range),
                self.target
                    .execute_count_query_with_rownum(&target_count_sql, range)
            );

            let source_count = source_result?;
            let target_count = target_result?;

            if source_count != target_count {
                debug!(
                    "{} range {}..{}: count mismatch (src: {}, tgt: {})",
                    tier, range.start_row, range.end_row, source_count, target_count
                );
                mismatches.push(range.clone());
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

    /// Compare row hashes with composite PK support.
    async fn compare_row_hashes_composite(
        &self,
        table: &Table,
        source_schema: &str,
        target_schema: &str,
        range: &RowRange,
    ) -> Result<RowHashDiffComposite> {
        let pk_columns = &table.primary_key;
        let pk_count = pk_columns.len();

        // Build queries for (pk_cols..., row_hash) tuples
        let source_hash_sql = mssql_row_hashes_with_rownum_query(source_schema, table);
        let target_hash_sql = postgres_row_hashes_with_rownum_query(
            target_schema,
            &table.name,
            pk_columns,
            &self.row_hash_column,
        );

        // Fetch hashes from both databases in parallel
        let (source_result, target_result) = tokio::join!(
            self.source
                .fetch_row_hashes_with_rownum(&source_hash_sql, range, pk_count),
            self.target
                .fetch_row_hashes_with_rownum(&target_hash_sql, range, pk_count)
        );

        let source_hashes = source_result?;
        let target_hashes = target_result?;

        let mut diff = RowHashDiffComposite::default();

        // Find rows in source but not in target (need INSERT)
        // Find rows with different hashes (need UPDATE)
        for (pk, source_hash) in &source_hashes {
            match target_hashes.get(pk) {
                None => diff.missing_in_target.push(pk.clone()),
                Some(target_hash) if target_hash != source_hash => {
                    diff.hash_mismatches.push(pk.clone());
                }
                _ => {} // Hashes match, no action needed
            }
        }

        // Find rows in target but not in source (may need DELETE)
        for pk in target_hashes.keys() {
            if !source_hashes.contains_key(pk) {
                diff.missing_in_source.push(pk.clone());
            }
        }

        Ok(diff)
    }

    /// Sync differences using composite PKs.
    async fn sync_differences_composite(
        &self,
        table: &Table,
        source_schema: &str,
        target_schema: &str,
        diff: &RowHashDiffComposite,
    ) -> Result<SyncStats> {
        use crate::target::TargetPool;

        let mut stats = SyncStats::default();
        let pk_columns = &table.primary_key;

        // Combine inserts and updates - both need to fetch from source and upsert
        let mut pks_to_upsert: Vec<CompositePk> = diff.missing_in_target.clone();
        pks_to_upsert.extend(diff.hash_mismatches.iter().cloned());

        if !pks_to_upsert.is_empty() {
            info!(
                "Syncing {} rows ({} inserts, {} updates) for {}.{}",
                pks_to_upsert.len(),
                diff.missing_in_target.len(),
                diff.hash_mismatches.len(),
                source_schema,
                table.name
            );

            // Fetch rows from source by composite PKs
            let rows = self
                .source
                .fetch_rows_by_composite_pks(table, &pks_to_upsert)
                .await?;

            if !rows.is_empty() {
                // Build column list
                let mut columns: Vec<String> =
                    table.columns.iter().map(|c| c.name.clone()).collect();

                // Find PK column indices for hash calculation
                let pk_indices: Vec<usize> = pk_columns
                    .iter()
                    .filter_map(|pk| columns.iter().position(|c| c == pk))
                    .collect();

                // Compute row hashes and add hash column
                let rows_with_hash: Vec<Vec<SqlValue>> = rows
                    .into_iter()
                    .map(|mut row| {
                        let hash = calculate_row_hash(&row, &pk_indices);
                        row.push(SqlValue::String(hash));
                        row
                    })
                    .collect();

                columns.push(self.row_hash_column.clone());

                // Capture count before moving rows_with_hash
                let expected_count = rows_with_hash.len();

                // Upsert to target
                let upserted = self
                    .target
                    .upsert_chunk_with_hash(
                        target_schema,
                        &table.name,
                        &columns,
                        pk_columns,
                        rows_with_hash,
                        0, // writer_id
                        Some(&self.row_hash_column),
                    )
                    .await?;

                stats.rows_inserted = diff.missing_in_target.len() as i64;
                stats.rows_updated = diff.hash_mismatches.len() as i64;

                if upserted as usize != expected_count {
                    warn!(
                        "Upserted {} rows but expected {} for {}.{}",
                        upserted, expected_count, target_schema, table.name
                    );
                }

                info!(
                    "Upserted {} rows to {}.{}",
                    upserted, target_schema, table.name
                );
            }
        }

        // Note: We intentionally do NOT delete rows that exist in target but not in source.
        // This is a safety measure - the target may have legitimate additional data,
        // and deletes are destructive operations that cannot be easily undone.
        // The verify output still reports rows_to_delete for informational purposes.
        if !diff.missing_in_source.is_empty() {
            info!(
                "Skipping {} rows in {}.{} that exist in target but not source (deletes disabled for safety)",
                diff.missing_in_source.len(),
                target_schema,
                table.name
            );
        }

        Ok(stats)
    }
}
