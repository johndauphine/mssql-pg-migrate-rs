//! Template Method pattern for transfer pipelines.
//!
//! The [`TransferPipeline`] trait defines the algorithm skeleton for data transfers,
//! with customizable steps that implementations can override. This enables:
//!
//! - Consistent transfer workflow across different strategies
//! - Easy extension for parallel, serial, or streaming implementations
//! - Clear separation of concerns between what and how
//!
//! # Design Pattern
//!
//! This implements the Template Method pattern where the base trait defines
//! the algorithm structure, and implementations customize specific steps.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::error::Result;

use super::job::{JobResult, TransferJob};

/// Configuration for pipeline behavior.
///
/// This configuration controls how the pipeline executes transfers,
/// including parallelism, batching, and retry behavior.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Number of parallel workers for reading.
    pub parallel_readers: usize,

    /// Number of parallel workers for writing.
    pub parallel_writers: usize,

    /// Maximum rows per batch/chunk.
    pub chunk_size: usize,

    /// Channel buffer size for read-ahead pipeline.
    pub channel_buffer: usize,

    /// Whether to use UNLOGGED tables for initial load (PostgreSQL).
    pub use_unlogged_tables: bool,

    /// Whether to drop indexes before bulk load.
    pub drop_indexes_before_load: bool,

    /// Maximum retries per batch on transient errors.
    pub max_retries: usize,

    /// Base delay between retries (exponential backoff).
    pub retry_base_delay: Duration,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            parallel_readers: 8,
            parallel_writers: 6,
            chunk_size: 100_000,
            channel_buffer: 4,
            use_unlogged_tables: false,
            drop_indexes_before_load: true,
            max_retries: 3,
            retry_base_delay: Duration::from_millis(100),
        }
    }
}

impl PipelineConfig {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the number of parallel readers.
    pub fn with_parallel_readers(mut self, count: usize) -> Self {
        self.parallel_readers = count.max(1);
        self
    }

    /// Set the number of parallel writers.
    pub fn with_parallel_writers(mut self, count: usize) -> Self {
        self.parallel_writers = count.max(1);
        self
    }

    /// Set the chunk size for batching.
    pub fn with_chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = size.max(1000);
        self
    }

    /// Set the channel buffer size.
    pub fn with_channel_buffer(mut self, size: usize) -> Self {
        self.channel_buffer = size.max(1);
        self
    }

    /// Enable or disable UNLOGGED tables.
    pub fn with_unlogged_tables(mut self, enabled: bool) -> Self {
        self.use_unlogged_tables = enabled;
        self
    }

    /// Enable or disable dropping indexes before load.
    pub fn with_drop_indexes(mut self, enabled: bool) -> Self {
        self.drop_indexes_before_load = enabled;
        self
    }

    /// Auto-tune configuration based on system resources.
    ///
    /// This adjusts parallelism based on available CPU cores and memory.
    pub fn auto_tune(mut self, available_memory_mb: usize, cpu_cores: usize) -> Self {
        // Scale readers based on CPU (IO-bound, can exceed cores)
        self.parallel_readers = (cpu_cores * 2).min(16).max(4);

        // Scale writers based on CPU (more CPU-bound due to encoding)
        self.parallel_writers = cpu_cores.min(12).max(2);

        // Scale chunk size based on memory
        // Estimate ~1KB per row average, target 10% of memory for buffering
        let target_buffer_mb = available_memory_mb / 10;
        let rows_per_mb = 1000; // ~1KB per row estimate
        self.chunk_size = (target_buffer_mb * rows_per_mb)
            .min(200_000)
            .max(10_000);

        // Channel buffer scales with readers
        self.channel_buffer = (self.parallel_readers / 2).max(2).min(8);

        self
    }
}

/// Statistics collected during pipeline execution.
///
/// This provides detailed metrics about the transfer process,
/// useful for monitoring, logging, and performance analysis.
#[derive(Debug, Clone, Default)]
pub struct PipelineStats {
    /// Total rows transferred across all jobs.
    pub total_rows: i64,

    /// Total bytes transferred (approximate).
    pub total_bytes: u64,

    /// Number of jobs completed successfully.
    pub jobs_completed: usize,

    /// Number of jobs that failed.
    pub jobs_failed: usize,

    /// Total time spent reading from source.
    pub read_time: Duration,

    /// Total time spent writing to target.
    pub write_time: Duration,

    /// Total elapsed wall-clock time.
    pub total_time: Duration,

    /// Peak memory usage in bytes (if tracked).
    pub peak_memory: Option<u64>,

    /// Average rows per second throughput.
    pub rows_per_second: f64,

    /// Average bytes per second throughput.
    pub bytes_per_second: f64,
}

impl PipelineStats {
    /// Create new empty stats.
    pub fn new() -> Self {
        Self::default()
    }

    /// Merge another stats instance into this one.
    pub fn merge(&mut self, other: &PipelineStats) {
        self.total_rows += other.total_rows;
        self.total_bytes += other.total_bytes;
        self.jobs_completed += other.jobs_completed;
        self.jobs_failed += other.jobs_failed;
        self.read_time += other.read_time;
        self.write_time += other.write_time;
        // total_time and throughput are recalculated
    }

    /// Merge a job result into the stats.
    pub fn merge_job_result(&mut self, result: &JobResult) {
        if result.completed {
            self.total_rows += result.rows_transferred;
            self.jobs_completed += 1;
            self.read_time += result.read_time;
            self.write_time += result.write_time;
        } else {
            self.jobs_failed += 1;
        }
    }

    /// Finalize stats by calculating derived metrics.
    pub fn finalize(&mut self, total_time: Duration) {
        self.total_time = total_time;
        let secs = total_time.as_secs_f64();
        if secs > 0.0 {
            self.rows_per_second = self.total_rows as f64 / secs;
            self.bytes_per_second = self.total_bytes as f64 / secs;
        }
    }

    /// Format a human-readable summary.
    pub fn summary(&self) -> String {
        format!(
            "Transferred {} rows in {:.1}s ({:.0} rows/sec). Jobs: {} completed, {} failed.",
            self.total_rows,
            self.total_time.as_secs_f64(),
            self.rows_per_second,
            self.jobs_completed,
            self.jobs_failed
        )
    }
}

/// Thread-safe progress tracker for pipeline execution.
///
/// This allows multiple workers to atomically update progress
/// without locking, using atomic operations.
#[derive(Debug, Default)]
pub struct ProgressTracker {
    /// Total rows transferred.
    pub rows_transferred: AtomicI64,

    /// Total bytes transferred.
    pub bytes_transferred: AtomicU64,

    /// Number of active workers.
    pub active_workers: AtomicU64,

    /// Start time for throughput calculation.
    start_time: Option<Instant>,
}

impl ProgressTracker {
    /// Create a new progress tracker.
    pub fn new() -> Self {
        Self {
            rows_transferred: AtomicI64::new(0),
            bytes_transferred: AtomicU64::new(0),
            active_workers: AtomicU64::new(0),
            start_time: Some(Instant::now()),
        }
    }

    /// Add rows to the counter.
    pub fn add_rows(&self, count: i64) {
        self.rows_transferred.fetch_add(count, Ordering::Relaxed);
    }

    /// Add bytes to the counter.
    pub fn add_bytes(&self, count: u64) {
        self.bytes_transferred.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment active worker count.
    pub fn worker_started(&self) {
        self.active_workers.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active worker count.
    pub fn worker_finished(&self) {
        self.active_workers.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get current row count.
    pub fn get_rows(&self) -> i64 {
        self.rows_transferred.load(Ordering::Relaxed)
    }

    /// Get current byte count.
    pub fn get_bytes(&self) -> u64 {
        self.bytes_transferred.load(Ordering::Relaxed)
    }

    /// Get current active worker count.
    pub fn get_active_workers(&self) -> u64 {
        self.active_workers.load(Ordering::Relaxed)
    }

    /// Calculate current throughput in rows per second.
    pub fn rows_per_second(&self) -> f64 {
        if let Some(start) = self.start_time {
            let elapsed = start.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                return self.get_rows() as f64 / elapsed;
            }
        }
        0.0
    }

    /// Get elapsed time since start.
    pub fn elapsed(&self) -> Duration {
        self.start_time.map_or(Duration::ZERO, |s| s.elapsed())
    }
}

/// Template Method trait for transfer pipelines.
///
/// This trait defines the algorithm skeleton for transferring data
/// from source to target. Implementations customize specific steps
/// while the overall workflow remains consistent.
///
/// # Algorithm Steps
///
/// 1. **Setup**: Prepare source and target for transfer
/// 2. **Execute Jobs**: Process transfer jobs (parallel or serial)
/// 3. **Finalize**: Complete the transfer (create indexes, etc.)
///
/// # Example Implementation
///
/// ```rust,ignore
/// struct ParallelPipeline {
///     source: Arc<dyn SourceReader>,
///     target: Arc<dyn TargetWriter>,
///     config: PipelineConfig,
/// }
///
/// #[async_trait]
/// impl TransferPipeline for ParallelPipeline {
///     async fn execute_job(&self, job: TransferJob, tracker: Arc<ProgressTracker>) -> Result<JobResult> {
///         // Custom parallel implementation
///     }
/// }
/// ```
#[async_trait]
pub trait TransferPipeline: Send + Sync {
    /// Get the pipeline configuration.
    fn config(&self) -> &PipelineConfig;

    /// Setup phase before transfer begins.
    ///
    /// Default implementation does nothing. Override to prepare
    /// source/target connections, create staging tables, etc.
    async fn setup(&self) -> Result<()> {
        Ok(())
    }

    /// Execute a single transfer job.
    ///
    /// This is the main customization point. Implementations should:
    /// 1. Read batches from source
    /// 2. Write batches to target
    /// 3. Update progress tracker
    /// 4. Handle errors and retries
    async fn execute_job(
        &self,
        job: TransferJob,
        tracker: Arc<ProgressTracker>,
        cancel: CancellationToken,
    ) -> Result<JobResult>;

    /// Execute multiple jobs with the pipeline's strategy.
    ///
    /// Default implementation executes jobs sequentially.
    /// Override for parallel execution.
    async fn execute_jobs(
        &self,
        jobs: Vec<TransferJob>,
        tracker: Arc<ProgressTracker>,
        cancel: CancellationToken,
    ) -> Result<Vec<JobResult>> {
        let mut results = Vec::with_capacity(jobs.len());
        for job in jobs {
            if cancel.is_cancelled() {
                break;
            }
            let result = self.execute_job(job, tracker.clone(), cancel.clone()).await?;
            results.push(result);
        }
        Ok(results)
    }

    /// Finalize phase after all jobs complete.
    ///
    /// Default implementation does nothing. Override to create
    /// indexes, foreign keys, or perform validation.
    async fn finalize(&self) -> Result<()> {
        Ok(())
    }

    /// Run the complete transfer pipeline.
    ///
    /// This is the Template Method - it defines the algorithm skeleton:
    /// 1. Setup
    /// 2. Execute jobs
    /// 3. Finalize
    /// 4. Collect stats
    ///
    /// Subclasses customize behavior by overriding the individual steps.
    async fn run(
        &self,
        jobs: Vec<TransferJob>,
        cancel: CancellationToken,
    ) -> Result<PipelineStats> {
        let start = Instant::now();
        let tracker = Arc::new(ProgressTracker::new());

        // Phase 1: Setup
        self.setup().await?;

        // Phase 2: Execute jobs
        let results = self.execute_jobs(jobs, tracker.clone(), cancel.clone()).await?;

        // Phase 3: Finalize (only if not cancelled)
        if !cancel.is_cancelled() {
            self.finalize().await?;
        }

        // Collect statistics
        let mut stats = PipelineStats::new();
        for result in &results {
            stats.merge_job_result(result);
        }
        stats.total_bytes = tracker.get_bytes();
        stats.finalize(start.elapsed());

        Ok(stats)
    }

    /// Check if the pipeline supports parallel execution.
    fn supports_parallel(&self) -> bool {
        false
    }

    /// Get the recommended batch size for this pipeline.
    fn recommended_batch_size(&self) -> usize {
        self.config().chunk_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TargetMode;
    use crate::core::schema::{Column, Table};

    fn make_test_table() -> Table {
        Table {
            schema: "dbo".to_string(),
            name: "TestTable".to_string(),
            columns: vec![Column {
                name: "Id".to_string(),
                data_type: "int".to_string(),
                max_length: 0,
                precision: 10,
                scale: 0,
                is_nullable: false,
                is_identity: true,
                ordinal_pos: 1,
            }],
            primary_key: vec!["Id".to_string()],
            pk_columns: vec![],
            row_count: 100,
            estimated_row_size: 10,
            indexes: vec![],
            foreign_keys: vec![],
            check_constraints: vec![],
        }
    }

    #[test]
    fn test_pipeline_config_default() {
        let config = PipelineConfig::default();
        assert_eq!(config.parallel_readers, 8);
        assert_eq!(config.parallel_writers, 6);
        assert_eq!(config.chunk_size, 100_000);
    }

    #[test]
    fn test_pipeline_config_builder() {
        let config = PipelineConfig::new()
            .with_parallel_readers(12)
            .with_parallel_writers(8)
            .with_chunk_size(50_000)
            .with_unlogged_tables(true);

        assert_eq!(config.parallel_readers, 12);
        assert_eq!(config.parallel_writers, 8);
        assert_eq!(config.chunk_size, 50_000);
        assert!(config.use_unlogged_tables);
    }

    #[test]
    fn test_pipeline_config_auto_tune() {
        // 16GB RAM, 8 cores
        let config = PipelineConfig::new().auto_tune(16_000, 8);

        assert!(config.parallel_readers >= 4);
        assert!(config.parallel_readers <= 16);
        assert!(config.parallel_writers >= 2);
        assert!(config.parallel_writers <= 12);
        assert!(config.chunk_size >= 10_000);
        assert!(config.chunk_size <= 200_000);
    }

    #[test]
    fn test_pipeline_stats_merge() {
        let mut stats1 = PipelineStats::new();
        stats1.total_rows = 1000;
        stats1.jobs_completed = 2;

        let stats2 = PipelineStats {
            total_rows: 500,
            jobs_completed: 1,
            ..Default::default()
        };

        stats1.merge(&stats2);
        assert_eq!(stats1.total_rows, 1500);
        assert_eq!(stats1.jobs_completed, 3);
    }

    #[test]
    fn test_pipeline_stats_finalize() {
        let mut stats = PipelineStats::new();
        stats.total_rows = 10_000;
        stats.total_bytes = 1_000_000;

        stats.finalize(Duration::from_secs(10));

        assert_eq!(stats.rows_per_second, 1000.0);
        assert_eq!(stats.bytes_per_second, 100_000.0);
    }

    #[test]
    fn test_progress_tracker() {
        let tracker = ProgressTracker::new();

        tracker.add_rows(100);
        tracker.add_rows(200);
        assert_eq!(tracker.get_rows(), 300);

        tracker.add_bytes(1000);
        assert_eq!(tracker.get_bytes(), 1000);

        tracker.worker_started();
        tracker.worker_started();
        assert_eq!(tracker.get_active_workers(), 2);

        tracker.worker_finished();
        assert_eq!(tracker.get_active_workers(), 1);
    }

    #[test]
    fn test_transfer_job_creation() {
        use super::super::job::TransferJob;

        let table = make_test_table();
        let job = TransferJob::new(table, "public".to_string(), TargetMode::Truncate);

        assert_eq!(job.table_name(), "dbo.TestTable");
        assert_eq!(job.target_schema, "public");
        assert!(job.partition_id.is_none());
    }
}
