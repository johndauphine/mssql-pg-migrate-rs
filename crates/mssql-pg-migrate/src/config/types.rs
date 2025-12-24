//! Configuration type definitions.

use serde::{Deserialize, Serialize};

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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationConfig {
    /// Number of parallel workers (default: CPU cores - 2).
    #[serde(default = "default_workers")]
    pub workers: usize,

    /// Rows per chunk (default: 100000).
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,

    /// Maximum partitions for large tables (default: 10).
    #[serde(default = "default_max_partitions")]
    pub max_partitions: usize,

    /// Row count threshold for partitioning (default: 5000000).
    #[serde(default = "default_large_table_threshold")]
    pub large_table_threshold: i64,

    /// Tables to include (glob patterns).
    #[serde(default)]
    pub include_tables: Vec<String>,

    /// Tables to exclude (glob patterns).
    #[serde(default)]
    pub exclude_tables: Vec<String>,

    /// Target mode (default: drop_recreate).
    #[serde(default)]
    pub target_mode: TargetMode,

    /// Read-ahead buffer count (default: 4).
    #[serde(default = "default_read_ahead_buffers")]
    pub read_ahead_buffers: usize,

    /// Parallel writers (default: 2).
    #[serde(default = "default_write_ahead_writers")]
    pub write_ahead_writers: usize,

    /// Parallel readers (default: 2).
    #[serde(default = "default_parallel_readers")]
    pub parallel_readers: usize,

    /// Create indexes after transfer (default: true).
    #[serde(default = "default_true")]
    pub create_indexes: bool,

    /// Create foreign keys after transfer (default: true).
    #[serde(default = "default_true")]
    pub create_foreign_keys: bool,

    /// Create check constraints after transfer (default: true).
    #[serde(default = "default_true")]
    pub create_check_constraints: bool,

    /// Maximum MSSQL connections (default: auto).
    #[serde(default)]
    pub max_mssql_connections: Option<usize>,

    /// Maximum PostgreSQL connections (default: auto).
    #[serde(default)]
    pub max_pg_connections: Option<usize>,
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

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            workers: default_workers(),
            chunk_size: default_chunk_size(),
            max_partitions: default_max_partitions(),
            large_table_threshold: default_large_table_threshold(),
            include_tables: Vec::new(),
            exclude_tables: Vec::new(),
            target_mode: TargetMode::default(),
            read_ahead_buffers: default_read_ahead_buffers(),
            write_ahead_writers: default_write_ahead_writers(),
            parallel_readers: default_parallel_readers(),
            create_indexes: true,
            create_foreign_keys: true,
            create_check_constraints: true,
            max_mssql_connections: None,
            max_pg_connections: None,
        }
    }
}

// Default value functions
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

fn default_workers() -> usize {
    let cpus = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4);
    (cpus.saturating_sub(2)).max(2).min(32)
}

fn default_chunk_size() -> usize {
    100_000
}

fn default_max_partitions() -> usize {
    10
}

fn default_large_table_threshold() -> i64 {
    5_000_000
}

fn default_read_ahead_buffers() -> usize {
    4
}

fn default_write_ahead_writers() -> usize {
    2
}

fn default_parallel_readers() -> usize {
    2
}

fn default_true() -> bool {
    true
}
