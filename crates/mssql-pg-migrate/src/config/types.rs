//! Configuration type definitions with auto-tuning based on system resources.

use serde::{Deserialize, Serialize};
use std::fmt;
use sysinfo::System;
use tracing::{info, warn};

/// Database type for bidirectional migration support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseType {
    /// Microsoft SQL Server
    Mssql,
    /// PostgreSQL
    Postgres,
    /// MySQL/MariaDB (requires `mysql` feature)
    Mysql,
}

/// Authentication/connection method for database connections.
///
/// This determines the authentication method used:
/// - `native` (default): Username/password via native Rust driver (tiberius for MSSQL, tokio-postgres for PostgreSQL)
/// - `kerberos`: Kerberos/GSSAPI via Tiberius (MSSQL only, requires `kerberos` feature)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AuthMethod {
    /// Native driver with username/password authentication.
    /// - MSSQL: Uses tiberius (TDS protocol) - fastest option (~180K+ rows/sec)
    /// - PostgreSQL: Uses tokio-postgres - fastest option
    ///
    /// This is the default and requires no external dependencies.
    #[default]
    Native,

    /// Kerberos/GSSAPI authentication via Tiberius (MSSQL only).
    ///
    /// **Requirements:**
    /// - The `kerberos` feature flag must be enabled at compile time
    /// - Linux: `libgssapi-krb5-2` package installed
    /// - macOS: Uses built-in GSS.framework (no extra dependencies)
    /// - Windows: Uses built-in SSPI (no extra dependencies)
    /// - Valid Kerberos ticket (run `kinit user@REALM` on Linux/macOS)
    ///
    /// On Windows with domain-joined machines, authentication is automatic via SSPI.
    ///
    /// **Note:** PostgreSQL Kerberos is not supported (tokio-postgres lacks GSSAPI).
    Kerberos,

    /// Legacy alias for `native` (MSSQL only, for backwards compatibility).
    /// Prefer using `native` instead.
    #[serde(rename = "sql_server")]
    SqlServer,
}

impl AuthMethod {
    /// Returns true if this is a native driver connection.
    /// All auth methods now use native drivers (ODBC has been removed).
    pub fn is_native(&self) -> bool {
        // All methods use native Tiberius/tokio-postgres drivers
        matches!(self, Self::Native | Self::SqlServer | Self::Kerberos)
    }
}

impl DatabaseType {
    /// Parse database type from string (case-insensitive).
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "mssql" | "sqlserver" | "sql_server" => Some(Self::Mssql),
            "postgres" | "postgresql" | "pg" => Some(Self::Postgres),
            "mysql" | "mariadb" => Some(Self::Mysql),
            _ => None,
        }
    }

    /// Get the default port for this database type.
    pub fn default_port(&self) -> u16 {
        match self {
            Self::Mssql => 1433,
            Self::Postgres => 5432,
            Self::Mysql => 3306,
        }
    }

    /// Get the default schema for this database type.
    /// Note: MySQL uses "database" instead of "schema", but we treat
    /// the database name as the schema for consistency.
    pub fn default_schema(&self) -> &'static str {
        match self {
            Self::Mssql => "dbo",
            Self::Postgres => "public",
            Self::Mysql => "", // MySQL uses database name directly
        }
    }

    /// Get the string representation for this database type.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Mssql => "mssql",
            Self::Postgres => "postgres",
            Self::Mysql => "mysql",
        }
    }
}

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
    /// Uses targeted refresh to avoid expensive parsing of all processes, disks, and networks.
    pub fn detect() -> Self {
        let mut sys = System::new();
        // Only refresh what we need - memory and CPU info
        sys.refresh_memory();
        sys.refresh_cpu_all();

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

/// Table statistics for auto-tuning.
#[derive(Debug, Clone)]
pub struct TableStats {
    /// Table name for logging.
    pub name: String,
    /// Row count.
    pub row_count: i64,
    /// Estimated row size in bytes.
    pub estimated_row_size: i64,
}

impl Config {
    /// Apply initial auto-tuned defaults based on system resources.
    /// Only fills in values that weren't explicitly set in the config file.
    /// Uses a default estimated row size (500 bytes) before actual table statistics are available.
    pub fn with_auto_tuning(mut self) -> Self {
        let resources = SystemResources::detect();
        resources.log();
        let target_type = DatabaseType::parse(&self.target.r#type);
        self.migration = self
            .migration
            .with_auto_tuning(&resources, None, target_type);
        self
    }

    /// Apply auto-tuned defaults using actual table statistics.
    /// This should be called after schema extraction when actual row sizes are known.
    pub fn apply_auto_tuning_from_tables(&mut self, tables: &[TableStats]) {
        let resources = SystemResources::detect();
        resources.log();

        // Calculate weighted average row size based on total data volume
        // Use u128 to avoid overflow with large datasets
        let total_rows: u128 = tables.iter().map(|t| t.row_count.max(0) as u128).sum();
        let total_bytes: u128 = tables
            .iter()
            .map(|t| (t.row_count.max(0) as u128) * (t.estimated_row_size.max(0) as u128))
            .sum();

        let avg_row_size = if total_rows > 0 {
            (total_bytes / total_rows) as usize
        } else {
            ESTIMATED_BYTES_PER_ROW // Fallback to default if no tables
        };

        info!(
            "Calculated average row size: {} bytes (from {} tables, {} total rows)",
            avg_row_size,
            tables.len(),
            total_rows
        );

        let target_type = DatabaseType::parse(&self.target.r#type);
        self.migration =
            self.migration
                .clone()
                .with_auto_tuning(&resources, Some(avg_row_size), target_type);
    }
}

/// Source database (MSSQL) configuration.
#[derive(Clone, Serialize, Deserialize)]
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

    /// Username (optional for Kerberos auth).
    #[serde(default)]
    pub user: String,

    /// Password (optional for Kerberos auth).
    #[serde(default)]
    pub password: String,

    /// Source schema (default: "dbo").
    #[serde(default = "default_dbo_schema")]
    pub schema: String,

    /// Encrypt connection (default: true).
    #[serde(default = "default_true")]
    pub encrypt: bool,

    /// Trust server certificate (default: false).
    #[serde(default)]
    pub trust_server_cert: bool,

    /// Authentication/connection method (default: native).
    /// - `native`: Uses tiberius with SQL Server authentication - fastest
    /// - `kerberos`: Uses tiberius with Kerberos/GSSAPI (requires `kerberos` feature)
    #[serde(default)]
    pub auth: AuthMethod,
}

impl fmt::Debug for SourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceConfig")
            .field("type", &self.r#type)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &"[REDACTED]")
            .field("schema", &self.schema)
            .field("encrypt", &self.encrypt)
            .field("trust_server_cert", &self.trust_server_cert)
            .field("auth", &self.auth)
            .finish()
    }
}

/// Target database configuration.
/// Supports PostgreSQL (default) and MSSQL targets.
#[derive(Clone, Serialize, Deserialize)]
pub struct TargetConfig {
    /// Database type ("postgres" or "mssql").
    #[serde(default = "default_postgres")]
    pub r#type: String,

    /// Database host.
    pub host: String,

    /// Database port (default: 5432 for PostgreSQL, 1433 for MSSQL).
    #[serde(default = "default_pg_port")]
    pub port: u16,

    /// Database name.
    pub database: String,

    /// Username (optional for Kerberos auth with MSSQL targets).
    #[serde(default)]
    pub user: String,

    /// Password (optional for Kerberos auth with MSSQL targets).
    #[serde(default)]
    pub password: String,

    /// Target schema (default: "public" for PostgreSQL, "dbo" for MSSQL).
    #[serde(default = "default_public_schema")]
    pub schema: String,

    /// SSL mode for PostgreSQL (default: "require"). Ignored for MSSQL.
    #[serde(default = "default_require")]
    pub ssl_mode: String,

    /// Encrypt connection for MSSQL targets (default: true). Ignored for PostgreSQL.
    #[serde(default = "default_true")]
    pub encrypt: bool,

    /// Trust server certificate for MSSQL targets (default: false). Ignored for PostgreSQL.
    #[serde(default)]
    pub trust_server_cert: bool,

    /// Authentication/connection method (default: native).
    /// - `native`: Uses tiberius (MSSQL) or tokio-postgres (PostgreSQL) - fastest
    /// - `kerberos`: Uses tiberius with Kerberos/GSSAPI (MSSQL only, requires `kerberos` feature)
    #[serde(default)]
    pub auth: AuthMethod,
}

impl fmt::Debug for TargetConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TargetConfig")
            .field("type", &self.r#type)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &"[REDACTED]")
            .field("schema", &self.schema)
            .field("ssl_mode", &self.ssl_mode)
            .field("encrypt", &self.encrypt)
            .field("trust_server_cert", &self.trust_server_cert)
            .field("auth", &self.auth)
            .finish()
    }
}

/// Migration behavior configuration.
/// All performance-related fields use Option<T> to distinguish between
/// "not set" (use auto-tuned default) and "explicitly set" (use provided value).
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "parallel_writers"
    )]
    pub write_ahead_writers: Option<usize>,

    /// Parallel readers per large table. Auto-tuned based on CPU cores if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel_readers: Option<usize>,

    /// Create indexes after transfer (default: false for data warehouse use cases).
    #[serde(default)]
    pub create_indexes: bool,

    /// Create foreign keys after transfer (default: false for data warehouse use cases).
    #[serde(default)]
    pub create_foreign_keys: bool,

    /// Create check constraints after transfer (default: false for data warehouse use cases).
    #[serde(default)]
    pub create_check_constraints: bool,

    /// Maximum MSSQL connections. Auto-tuned based on workers if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_mssql_connections: Option<usize>,

    /// Maximum PostgreSQL connections. Auto-tuned based on workers if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_pg_connections: Option<usize>,

    /// Minimum rows per partition when splitting large tables.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_rows_per_partition: Option<i64>,

    /// Concurrency for finalization tasks (indexes, constraints).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finalizer_concurrency: Option<usize>,

    /// Rows per COPY buffer flush. Auto-tuned based on RAM if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copy_buffer_rows: Option<usize>,

    /// Use binary COPY format (default: true for better performance).
    #[serde(default = "default_true")]
    pub use_binary_copy: bool,

    /// MySQL bulk load strategy (default: auto).
    /// - `auto`: Use LOAD DATA for tables without LOB columns, INSERT for LOB tables
    /// - `always`: Always use LOAD DATA LOCAL INFILE
    /// - `never`: Always use INSERT statements
    #[serde(default)]
    pub mysql_load_data: MysqlLoadData,

    /// Use UNLOGGED tables during transfer (default: false).
    /// UNLOGGED tables are faster for writes but not crash-safe.
    /// If true, tables are created as UNLOGGED and remain UNLOGGED.
    /// If false (default), tables are created as LOGGED from the start.
    #[serde(default)]
    pub use_unlogged_tables: bool,

    /// Rows per upsert batch statement. Auto-tuned based on RAM if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upsert_batch_size: Option<usize>,

    /// Number of parallel upsert tasks. Auto-tuned based on CPU cores if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upsert_parallel_tasks: Option<usize>,

    /// Date-based incremental sync column names (upsert mode only).
    /// When set, the tool will look for these columns in each table (in order)
    /// and only sync rows where column > last_sync_timestamp.
    /// Significantly speeds up incremental syncs by filtering at source.
    /// Example: ["LastActivityDate", "ModifiedDate", "UpdatedAt", "CreationDate"]
    #[serde(default)]
    pub date_updated_columns: Vec<String>,

    /// Memory budget as percentage of available RAM (default: 70).
    /// Auto-tuning will constrain buffer sizes to stay within this limit.
    #[serde(default = "default_memory_budget_percent")]
    pub memory_budget_percent: u8,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            workers: None,
            chunk_size: None,
            max_partitions: None,
            large_table_threshold: None,
            include_tables: Vec::new(),
            exclude_tables: Vec::new(),
            target_mode: TargetMode::default(),
            read_ahead_buffers: None,
            write_ahead_writers: None,
            parallel_readers: None,
            create_indexes: false,
            create_foreign_keys: false,
            create_check_constraints: false,
            max_mssql_connections: None,
            max_pg_connections: None,
            min_rows_per_partition: None,
            finalizer_concurrency: None,
            copy_buffer_rows: None,
            use_binary_copy: default_true(),
            mysql_load_data: MysqlLoadData::default(),
            use_unlogged_tables: false,
            upsert_batch_size: None,
            upsert_parallel_tasks: None,
            date_updated_columns: Vec::new(),
            memory_budget_percent: default_memory_budget_percent(),
        }
    }
}

fn default_memory_budget_percent() -> u8 {
    70
}

/// Estimated bytes per row for buffer calculations (conservative estimate)
const ESTIMATED_BYTES_PER_ROW: usize = 500;

/// Safety factor for memory calculations (2x headroom for overhead)
const MEMORY_SAFETY_FACTOR: usize = 2;

impl MigrationConfig {
    /// Apply auto-tuned defaults based on system resources.
    /// Only fills in values that are None (not explicitly set).
    /// Memory usage is constrained to a configurable percentage of available RAM (default 70%).
    ///
    /// If `actual_row_size` is provided, it will be used instead of the default estimate.
    /// This should be the weighted average row size calculated from actual table metadata.
    ///
    /// If `target_type` is MSSQL, uses conservative parallelism settings due to TABLOCK
    /// bulk insert serialization (more writers = more contention, not more throughput).
    pub fn with_auto_tuning(
        mut self,
        resources: &SystemResources,
        actual_row_size: Option<usize>,
        target_type: Option<DatabaseType>,
    ) -> Self {
        let ram_gb = resources.total_memory_gb;
        let cores = resources.cpu_cores;

        // Use actual row size if provided, otherwise use conservative estimate.
        // Ensure bytes_per_row is at least 1 to prevent division by zero.
        let bytes_per_row = actual_row_size.unwrap_or(ESTIMATED_BYTES_PER_ROW).max(1);

        // Calculate memory budget based on configured percentage of available RAM.
        // Clamp to valid range [1, 100] to prevent division by zero or excessive memory usage.
        let clamped_percent = self.memory_budget_percent.clamp(1, 100);
        if clamped_percent != self.memory_budget_percent {
            info!(
                "Adjusted memory_budget_percent from {} to {} (must be between 1 and 100)",
                self.memory_budget_percent, clamped_percent
            );
        }
        let memory_budget_pct = clamped_percent as f64 / 100.0;
        // Keep calculation in u64 to avoid 32-bit overflow on systems with >4GB RAM
        let memory_budget_bytes_u64 =
            (resources.total_memory_bytes as f64 * memory_budget_pct) as u64;
        // Saturate to usize::MAX on 32-bit systems to prevent panic
        let memory_budget_bytes = usize::try_from(memory_budget_bytes_u64).unwrap_or(usize::MAX);
        let memory_budget_mb = memory_budget_bytes / (1024 * 1024);

        info!(
            "Memory budget: {} MB ({}% of {:.1} GB available), row size estimate: {} bytes",
            memory_budget_mb, clamped_percent, ram_gb, bytes_per_row
        );

        // Workers: cores / 2, but at least 4 and at most 8
        // Benchmarking shows 6 workers is optimal for most workloads.
        // Too few underutilizes parallelism, too many causes contention.
        if self.workers.is_none() {
            let workers = (cores / 2).clamp(4, 16);
            self.workers = Some(workers);
        }
        let workers = self.workers.unwrap();

        // Parallel readers: conservative scaling based on cores.
        // Formula: cores/4 clamped to 2-16 for optimal throughput without overwhelming source.
        if self.parallel_readers.is_none() {
            let readers = (cores / 4).clamp(2, 16);
            self.parallel_readers = Some(readers);
            info!(
                "  ParallelReaders: {} (auto: cores/4 clamped 2-16, {} cores)",
                readers, cores
            );
        }

        // Parallel writers: depends on target database type.
        // MSSQL targets: fixed at 2 due to TABLOCK bulk insert serialization.
        //   - TABLOCK acquires exclusive table lock, serializing all writers
        //   - More writers = more lock contention overhead, not more throughput
        // PostgreSQL targets: cores/4 clamped to 2-4, COPY handles parallelism well.
        if self.write_ahead_writers.is_none() {
            let is_mssql_target = matches!(target_type, Some(DatabaseType::Mssql));
            let writers = if is_mssql_target {
                2 // Fixed for MSSQL due to TABLOCK serialization
            } else {
                (cores / 4).clamp(2, 16)
            };
            self.write_ahead_writers = Some(writers);
            if is_mssql_target {
                info!(
                    "  WriteAheadWriters: {} (auto: fixed 2 for MSSQL TABLOCK)",
                    writers
                );
            } else {
                info!(
                    "  WriteAheadWriters: {} (auto: cores/4 clamped 2-16, {} cores)",
                    writers, cores
                );
            }
        }

        // Max partitions: scale with cores
        if self.max_partitions.is_none() {
            let partitions = (cores / 2).clamp(4, 16);
            self.max_partitions = Some(partitions);
        }

        // Calculate chunk size based on memory budget
        // Memory per active transfer = chunk_size * bytes_per_row * read_ahead_buffers * workers
        // We want: chunk_size * ESTIMATED_BYTES_PER_ROW * read_ahead * workers <= memory_budget_bytes
        // Solve for chunk_size with a safety factor of 2x

        // Start with desired read-ahead buffers
        let desired_read_ahead = ((ram_gb / 4.0) as usize).clamp(4, 32);

        // Calculate max chunk size that fits in memory budget
        let max_chunk_from_memory = memory_budget_bytes
            / (bytes_per_row * desired_read_ahead * workers * MEMORY_SAFETY_FACTOR);
        let max_chunk_from_memory = max_chunk_from_memory.max(10_000); // Minimum viable chunk size

        // Chunk size: scale with RAM but respect memory budget.
        // For upsert mode, smaller chunks (25K-75K) are faster due to less lock contention.
        // For bulk load, larger chunks (100K-120K) are optimal.
        if self.chunk_size.is_none() {
            let chunk = match self.target_mode {
                TargetMode::Upsert => {
                    // Upsert: smaller chunks for faster MERGE statements
                    // Benchmarking shows 25K-75K is optimal, use 50K as default
                    50_000_usize.min(max_chunk_from_memory)
                }
                _ => {
                    // Bulk load: larger chunks for better COPY throughput
                    // Formula: 75K base + 25K per 8GB RAM → 100K at 8GB, 125K at 16GB
                    // Order: apply floor first, then respect memory budget ceiling
                    let desired_chunk = 75_000 + ((ram_gb * 25_000.0 / 8.0) as usize);
                    desired_chunk
                        .max(50_000)
                        .min(max_chunk_from_memory)
                        .min(200_000)
                }
            };
            self.chunk_size = Some(chunk);
        }

        // Read-ahead buffers: scale with RAM but respect memory budget
        if self.read_ahead_buffers.is_none() {
            let chunk_size = self.chunk_size.unwrap();
            // Recalculate max read-ahead based on actual chunk size
            let max_read_ahead_from_memory =
                memory_budget_bytes / (bytes_per_row * chunk_size * workers * MEMORY_SAFETY_FACTOR);
            let buffers = desired_read_ahead
                .min(max_read_ahead_from_memory)
                .clamp(2, 32);
            self.read_ahead_buffers = Some(buffers);
        }

        // Large table threshold: scale with RAM
        // More RAM = can handle larger tables before partitioning
        if self.large_table_threshold.is_none() {
            let threshold = ((ram_gb / 8.0) as i64 * 1_000_000).clamp(1_000_000, 20_000_000);
            self.large_table_threshold = Some(threshold);
        }

        // COPY buffer rows: scale with RAM
        if self.copy_buffer_rows.is_none() {
            let rows = ((ram_gb / 4.0) as usize * 5_000).clamp(5_000, 50_000);
            self.copy_buffer_rows = Some(rows);
        }

        // Connection pool sizes: scale with workers and parallelism
        // Need enough connections for parallel readers + writers + workers
        if self.max_mssql_connections.is_none() {
            let readers = self.parallel_readers.unwrap_or(4);
            let conns = (workers * readers + 4).clamp(8, 80);
            self.max_mssql_connections = Some(conns);
        }

        if self.max_pg_connections.is_none() {
            let writers = self.write_ahead_writers.unwrap_or(4);
            let conns = (workers * writers + 4).clamp(8, 64);
            self.max_pg_connections = Some(conns);
        }

        // Upsert batch size: scale with RAM (more aggressive for better throughput)
        // Larger batches reduce round-trips but use more memory
        if self.upsert_batch_size.is_none() {
            let batch = ((ram_gb / 4.0) as usize * 1_000).clamp(1_000, 10_000);
            self.upsert_batch_size = Some(batch);
        }

        // Upsert parallel tasks: bound by available PostgreSQL connections per writer
        // Prevents upsert tasks from exceeding pool capacity and contending with writers
        if self.upsert_parallel_tasks.is_none() {
            let writers = self.write_ahead_writers.unwrap_or(4).max(1);
            let pg_conns = self.max_pg_connections.unwrap_or(40).max(1);
            let conns_per_writer = pg_conns / writers;
            // Reserve one connection per writer for overhead; at least 1 task
            let safe_tasks = conns_per_writer.saturating_sub(1).max(1);
            // Also bound by CPU cores to avoid overscheduling
            let tasks = safe_tasks.min(cores.max(1));
            self.upsert_parallel_tasks = Some(tasks);
        }

        // Calculate estimated memory usage for logging
        let chunk_size = self.chunk_size.unwrap();
        let read_ahead = self.read_ahead_buffers.unwrap();
        let estimated_buffer_memory_mb =
            (workers * read_ahead * chunk_size * bytes_per_row) / (1024 * 1024);

        // Log the auto-tuned values
        info!(
            "Auto-tuned config: workers={}, parallel_readers={}, parallel_writers={}, \
             chunk_size={}, read_ahead={}, estimated_buffer_memory={}MB (budget={}MB)",
            self.workers.unwrap(),
            self.parallel_readers.unwrap(),
            self.write_ahead_writers.unwrap(),
            chunk_size,
            read_ahead,
            estimated_buffer_memory_mb,
            memory_budget_mb,
        );
        info!(
            "  large_table_threshold={}, mssql_conns={}, pg_conns={}",
            self.large_table_threshold.unwrap(),
            self.max_mssql_connections.unwrap(),
            self.max_pg_connections.unwrap(),
        );

        self
    }

    // Accessor methods that return the effective value (with fallback defaults)
    // These are used when the config hasn't been auto-tuned yet.
    // Defaults are based on benchmarking results for optimal throughput.

    pub fn get_workers(&self) -> usize {
        self.workers.unwrap_or(6) // Optimal from benchmarks
    }

    pub fn get_chunk_size(&self) -> usize {
        // Conservative default to prevent unbounded memory growth.
        // Auto-tuning will increase this based on available memory.
        self.chunk_size.unwrap_or(10_000)
    }

    pub fn get_max_partitions(&self) -> usize {
        self.max_partitions.unwrap_or(12)
    }

    pub fn get_large_table_threshold(&self) -> i64 {
        self.large_table_threshold.unwrap_or(5_000_000)
    }

    pub fn get_min_rows_per_partition(&self) -> i64 {
        self.min_rows_per_partition.unwrap_or(200_000)
    }

    pub fn get_read_ahead_buffers(&self) -> usize {
        // Conservative default to prevent unbounded memory growth.
        // Auto-tuning will increase this based on available memory.
        self.read_ahead_buffers.unwrap_or(4)
    }

    pub fn get_write_ahead_writers(&self) -> usize {
        self.write_ahead_writers.unwrap_or(8) // Optimal: 8-10
    }

    pub fn get_finalizer_concurrency(&self) -> usize {
        self.finalizer_concurrency
            .unwrap_or_else(|| self.get_workers())
    }

    pub fn get_parallel_readers(&self) -> usize {
        // Conservative default to prevent unbounded memory growth.
        // Auto-tuning will increase this based on available memory.
        self.parallel_readers.unwrap_or(4)
    }

    pub fn get_max_mssql_connections(&self) -> usize {
        self.max_mssql_connections.unwrap_or(50)
    }

    pub fn get_max_pg_connections(&self) -> usize {
        self.max_pg_connections.unwrap_or(40)
    }

    pub fn get_copy_buffer_rows(&self) -> usize {
        self.copy_buffer_rows.unwrap_or(10_000)
    }

    pub fn get_upsert_batch_size(&self) -> usize {
        self.upsert_batch_size.unwrap_or(2_000)
    }

    pub fn get_upsert_parallel_tasks(&self) -> usize {
        self.upsert_parallel_tasks.unwrap_or(4)
    }

    /// Check if a table name matches any of the given patterns.
    ///
    /// Patterns support glob wildcards:
    /// - `*` matches any sequence of characters
    /// - `?` matches any single character
    ///
    /// Pattern matching is case-insensitive.
    fn matches_any_pattern(table_name: &str, patterns: &[String]) -> bool {
        use glob::Pattern;

        let table_lower = table_name.to_lowercase();
        for pattern_str in patterns {
            // glob::Pattern matching is case-sensitive by default,
            // so we lowercase both for case-insensitive matching
            let pattern_lower = pattern_str.to_lowercase();
            match Pattern::new(&pattern_lower) {
                Ok(pattern) => {
                    if pattern.matches(&table_lower) {
                        return true;
                    }
                }
                Err(e) => {
                    warn!(
                        "Invalid glob pattern '{}': {}. Pattern will be skipped.",
                        pattern_str, e
                    );
                }
            }
        }
        false
    }

    /// Extract just the table name from a schema-qualified name.
    /// "schema.table" -> "table", "table" -> "table"
    fn extract_table_name(full_name: &str) -> &str {
        full_name.rsplit('.').next().unwrap_or(full_name)
    }

    /// Filter tables based on include_tables and exclude_tables patterns.
    ///
    /// Rules:
    /// - Patterns match against table name only (not schema)
    /// - If include_tables is empty, all tables are included by default
    /// - If include_tables is non-empty, only matching tables are included
    /// - exclude_tables always takes precedence over include_tables
    ///
    /// Returns the filtered list of table names (full schema.table names).
    pub fn filter_tables(&self, table_names: &[String]) -> Vec<String> {
        table_names
            .iter()
            .filter(|full_name| {
                // Extract just the table name for pattern matching
                let table_name = Self::extract_table_name(full_name);

                // If exclude patterns match, exclude the table
                if !self.exclude_tables.is_empty()
                    && Self::matches_any_pattern(table_name, &self.exclude_tables)
                {
                    return false;
                }

                // If include patterns are specified, table must match one
                if !self.include_tables.is_empty() {
                    return Self::matches_any_pattern(table_name, &self.include_tables);
                }

                // No include patterns means include all (that weren't excluded)
                true
            })
            .cloned()
            .collect()
    }
}

/// Target mode for migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetMode {
    /// Drop and recreate target tables.
    #[default]
    DropRecreate,

    /// Upsert: INSERT new rows, UPDATE changed rows.
    Upsert,
}

/// MySQL bulk load strategy using LOAD DATA LOCAL INFILE.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MysqlLoadData {
    /// Auto-detect: use LOAD DATA for tables without LOB columns (TEXT, BLOB),
    /// fall back to INSERT for tables with LOB columns.
    Auto,

    /// Always use LOAD DATA LOCAL INFILE for all tables.
    Always,

    /// Never use LOAD DATA, always use INSERT statements.
    /// This is the default as INSERT often outperforms LOAD DATA in many environments.
    #[default]
    Never,
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

fn default_require() -> String {
    "require".to_string()
}

fn default_true() -> bool {
    true
}
