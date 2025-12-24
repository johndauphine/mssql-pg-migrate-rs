//! Configuration type definitions with auto-tuning based on system resources.

use serde::{Deserialize, Serialize};
use sysinfo::System;
use tracing::info;

/// System resource information for auto-tuning.
#[derive(Debug, Clone)]
pub struct SystemResources {
    /// Total RAM in bytes.
    pub total_memory_bytes: u64,
    /// Total RAM in GB.
    pub total_memory_gb: f64,
    /// Number of CPU cores.
    pub cpu_cores: usize,
}

impl SystemResources {
    /// Detect system resources.
    pub fn detect() -> Self {
        let mut sys = System::new_all();
        sys.refresh_all();

        let total_memory_bytes = sys.total_memory();
        let total_memory_gb = total_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        let cpu_cores = sys.cpus().len();

        Self {
            total_memory_bytes,
            total_memory_gb,
            cpu_cores,
        }
    }

    /// Log detected system resources.
    pub fn log(&self) {
        info!(
            "System resources: {:.1} GB RAM, {} CPU cores",
            self.total_memory_gb, self.cpu_cores
        );
    }
}

/// Root configuration structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Source database configuration (MSSQL).
    pub source: SourceConfig,

    /// Target database configuration (PostgreSQL).
    pub target: TargetConfig,

    /// Migration behavior configuration.
    #[serde(default)]
    pub migration: MigrationConfig,
}

impl Config {
    /// Apply auto-tuned defaults based on system resources.
    /// Only fills in values that weren't explicitly set in the config file.
    pub fn with_auto_tuning(mut self) -> Self {
        let resources = SystemResources::detect();
        resources.log();
        self.migration = self.migration.with_auto_tuning(&resources);
        self
    }
}

/// Source database (MSSQL) configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    /// Database type (always "mssql" for now).
    #[serde(default = "default_mssql")]
    pub r#type: String,

    /// Database host.
    pub host: String,

    /// Database port (default: 1433).
    #[serde(default = "default_mssql_port")]
    pub port: u16,

    /// Database name.
    pub database: String,

    /// Username.
    pub user: String,

    /// Password.
    pub password: String,

    /// Source schema (default: "dbo").
    #[serde(default = "default_dbo_schema")]
    pub schema: String,

    /// Encrypt connection (default: "true").
    #[serde(default = "default_true_string")]
    pub encrypt: String,

    /// Trust server certificate (default: false).
    #[serde(default)]
    pub trust_server_cert: bool,
}

/// Target database (PostgreSQL) configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetConfig {
    /// Database type (always "postgres" for now).
    #[serde(default = "default_postgres")]
    pub r#type: String,

    /// Database host.
    pub host: String,

    /// Database port (default: 5432).
    #[serde(default = "default_pg_port")]
    pub port: u16,

    /// Database name.
    pub database: String,

    /// Username.
    pub user: String,

    /// Password.
    pub password: String,

    /// Target schema (default: "public").
    #[serde(default = "default_public_schema")]
    pub schema: String,

    /// SSL mode (default: "require").
    #[serde(default = "default_require")]
    pub ssl_mode: String,
}

/// Migration behavior configuration.
/// All performance-related fields use Option<T> to distinguish between
/// "not set" (use auto-tuned default) and "explicitly set" (use provided value).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MigrationConfig {
    /// Number of parallel workers. Auto-tuned based on CPU cores if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workers: Option<usize>,

    /// Rows per chunk. Auto-tuned based on RAM if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_size: Option<usize>,

    /// Maximum partitions for large tables. Auto-tuned based on CPU cores if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_partitions: Option<usize>,

    /// Row count threshold for partitioning. Auto-tuned based on RAM if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub large_table_threshold: Option<i64>,

    /// Tables to include (glob patterns).
    #[serde(default)]
    pub include_tables: Vec<String>,

    /// Tables to exclude (glob patterns).
    #[serde(default)]
    pub exclude_tables: Vec<String>,

    /// Target mode (default: drop_recreate).
    #[serde(default)]
    pub target_mode: TargetMode,

    /// Read-ahead buffer count. Auto-tuned based on RAM if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_ahead_buffers: Option<usize>,

    /// Parallel writers. Auto-tuned based on CPU cores if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_ahead_writers: Option<usize>,

    /// Parallel readers per large table. Auto-tuned based on CPU cores if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel_readers: Option<usize>,

    /// Create indexes after transfer (default: true).
    #[serde(default = "default_true")]
    pub create_indexes: bool,

    /// Create foreign keys after transfer (default: true).
    #[serde(default = "default_true")]
    pub create_foreign_keys: bool,

    /// Create check constraints after transfer (default: true).
    #[serde(default = "default_true")]
    pub create_check_constraints: bool,

    /// Maximum MSSQL connections. Auto-tuned based on workers if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_mssql_connections: Option<usize>,

    /// Maximum PostgreSQL connections. Auto-tuned based on workers if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_pg_connections: Option<usize>,

    /// Rows per COPY buffer flush. Auto-tuned based on RAM if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copy_buffer_rows: Option<usize>,

    /// Use binary COPY format (default: false).
    #[serde(default)]
    pub use_binary_copy: bool,

    /// Rows per upsert batch statement. Auto-tuned based on RAM if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upsert_batch_size: Option<usize>,
}

impl MigrationConfig {
    /// Apply auto-tuned defaults based on system resources.
    /// Only fills in values that are None (not explicitly set).
    pub fn with_auto_tuning(mut self, resources: &SystemResources) -> Self {
        let ram_gb = resources.total_memory_gb;
        let cores = resources.cpu_cores;

        // Workers: cores - 2, but at least 2 and at most 32
        // For high core count machines, cap at a reasonable level
        if self.workers.is_none() {
            let workers = cores.saturating_sub(2).max(2).min(32);
            self.workers = Some(workers);
        }
        let workers = self.workers.unwrap();

        // Parallel readers: scale with cores, 2-8 range
        // More cores = more parallel readers per table
        if self.parallel_readers.is_none() {
            let readers = (cores / 4).max(2).min(8);
            self.parallel_readers = Some(readers);
        }

        // Parallel writers: similar to readers
        if self.write_ahead_writers.is_none() {
            let writers = (cores / 4).max(2).min(8);
            self.write_ahead_writers = Some(writers);
        }

        // Max partitions: scale with cores
        if self.max_partitions.is_none() {
            let partitions = (cores / 2).max(4).min(16);
            self.max_partitions = Some(partitions);
        }

        // Chunk size: scale with RAM
        // Base: 50K rows, +25K per 8GB of RAM, cap at 200K
        if self.chunk_size.is_none() {
            let chunk = 50_000 + ((ram_gb / 8.0) as usize * 25_000);
            let chunk = chunk.max(50_000).min(200_000);
            self.chunk_size = Some(chunk);
        }

        // Read-ahead buffers: scale with RAM
        // More RAM = larger read-ahead pipeline
        if self.read_ahead_buffers.is_none() {
            let buffers = ((ram_gb / 4.0) as usize).max(4).min(32);
            self.read_ahead_buffers = Some(buffers);
        }

        // Large table threshold: scale with RAM
        // More RAM = can handle larger tables before partitioning
        if self.large_table_threshold.is_none() {
            let threshold = ((ram_gb / 8.0) as i64 * 1_000_000).max(1_000_000).min(20_000_000);
            self.large_table_threshold = Some(threshold);
        }

        // COPY buffer rows: scale with RAM
        if self.copy_buffer_rows.is_none() {
            let rows = ((ram_gb / 4.0) as usize * 5_000).max(5_000).min(50_000);
            self.copy_buffer_rows = Some(rows);
        }

        // Upsert batch size: scale with RAM
        if self.upsert_batch_size.is_none() {
            let batch = ((ram_gb / 8.0) as usize * 500).max(500).min(5_000);
            self.upsert_batch_size = Some(batch);
        }

        // Connection pool sizes: scale with workers
        if self.max_mssql_connections.is_none() {
            let conns = (workers * 2).max(4).min(64);
            self.max_mssql_connections = Some(conns);
        }

        if self.max_pg_connections.is_none() {
            let conns = (workers * 2).max(4).min(64);
            self.max_pg_connections = Some(conns);
        }

        // Log the auto-tuned values
        info!(
            "Auto-tuned config: workers={}, parallel_readers={}, chunk_size={}, read_ahead={}, \
             large_table_threshold={}, mssql_conns={}, pg_conns={}",
            self.workers.unwrap(),
            self.parallel_readers.unwrap(),
            self.chunk_size.unwrap(),
            self.read_ahead_buffers.unwrap(),
            self.large_table_threshold.unwrap(),
            self.max_mssql_connections.unwrap(),
            self.max_pg_connections.unwrap(),
        );

        self
    }

    // Accessor methods that return the effective value (with fallback defaults)
    // These are used when the config hasn't been auto-tuned yet

    pub fn get_workers(&self) -> usize {
        self.workers.unwrap_or(4)
    }

    pub fn get_chunk_size(&self) -> usize {
        self.chunk_size.unwrap_or(50_000)
    }

    pub fn get_max_partitions(&self) -> usize {
        self.max_partitions.unwrap_or(10)
    }

    pub fn get_large_table_threshold(&self) -> i64 {
        self.large_table_threshold.unwrap_or(5_000_000)
    }

    pub fn get_read_ahead_buffers(&self) -> usize {
        self.read_ahead_buffers.unwrap_or(10)
    }

    pub fn get_write_ahead_writers(&self) -> usize {
        self.write_ahead_writers.unwrap_or(2)
    }

    pub fn get_parallel_readers(&self) -> usize {
        self.parallel_readers.unwrap_or(2)
    }

    pub fn get_max_mssql_connections(&self) -> usize {
        self.max_mssql_connections.unwrap_or(8)
    }

    pub fn get_max_pg_connections(&self) -> usize {
        self.max_pg_connections.unwrap_or(8)
    }

    pub fn get_copy_buffer_rows(&self) -> usize {
        self.copy_buffer_rows.unwrap_or(10_000)
    }

    pub fn get_upsert_batch_size(&self) -> usize {
        self.upsert_batch_size.unwrap_or(1_000)
    }
}

/// Target mode for migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetMode {
    /// Drop and recreate target tables.
    #[default]
    DropRecreate,

    /// Truncate existing tables, create if missing.
    Truncate,

    /// Upsert: INSERT new rows, UPDATE changed rows.
    Upsert,
}

// Default value functions for serde
fn default_mssql() -> String {
    "mssql".to_string()
}

fn default_postgres() -> String {
    "postgres".to_string()
}

fn default_mssql_port() -> u16 {
    1433
}

fn default_pg_port() -> u16 {
    5432
}

fn default_dbo_schema() -> String {
    "dbo".to_string()
}

fn default_public_schema() -> String {
    "public".to_string()
}

fn default_true_string() -> String {
    "true".to_string()
}

fn default_require() -> String {
    "require".to_string()
}

fn default_true() -> bool {
    true
}
