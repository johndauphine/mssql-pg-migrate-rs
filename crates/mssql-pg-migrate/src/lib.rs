//! # mssql-pg-migrate
//!
//! High-performance MSSQL to PostgreSQL migration library.
//!
//! This library provides the core functionality for migrating data from
//! Microsoft SQL Server to PostgreSQL with support for:
//!
//! - **Bulk transfers** using PostgreSQL COPY protocol
//! - **Upsert mode** for incremental synchronization
//! - **Parallel transfers** with configurable worker pools
//! - **Resume capability** via JSON state files
//! - **Type mapping** between MSSQL and PostgreSQL
//!
//! ## Example
//!
//! ```rust,no_run
//! use mssql_pg_migrate::{Config, Orchestrator};
//! use tokio_util::sync::CancellationToken;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = Config::load("config.yaml")?;
//!     let mut orchestrator = Orchestrator::new(config).await?;
//!     let cancel = CancellationToken::new();
//!     let result = orchestrator.run(cancel, false).await?;
//!     // Always close connections when done
//!     orchestrator.close().await;
//!     println!("Migrated {} rows", result.rows_transferred);
//!     Ok(())
//! }
//! ```

pub mod config;
pub mod core;
pub mod dialect;
pub mod drivers;
pub mod error;
pub mod orchestrator;
pub mod source;
pub mod state;
pub mod target;
pub mod transfer;

// Re-exports for convenient access
pub use config::{
    Config, DatabaseType, MigrationConfig, SourceConfig, TableStats, TargetConfig, TargetMode,
};
pub use error::{MigrateError, Result};
pub use orchestrator::{
    HealthCheckResult, MigrationResult, Orchestrator, ProgressUpdate, TableError,
};
pub use core::schema::Table;
pub use state::MigrationState;
pub use target::{SqlNullType, SqlValue, UpsertWriter};
pub use transfer::{TransferConfig, TransferEngine, TransferJob, TransferStats};


// Core module re-exports (new plugin architecture)
// These provide the new trait-based abstractions for database operations
pub use core::{
    schema::{CheckConstraint, Column, ForeignKey, Index, Partition, PkValue},
    traits::{Dialect, ReadOptions, SourceReader, TargetWriter, TypeMapper},
    value::{Batch, SqlNullType as NewSqlNullType, SqlValue as NewSqlValue},
    DriverCatalog,
};

// Driver implementations (new plugin architecture)
pub use drivers::{
    DialectImpl, MssqlDialect, MssqlReader, MssqlWriter, PostgresDialect, PostgresReader,
    PostgresWriter, SourceReaderImpl, TargetWriterImpl,
};

// MySQL support (feature-gated)
#[cfg(feature = "mysql")]
pub use drivers::{MysqlDialect, MysqlReader, MysqlWriter};
