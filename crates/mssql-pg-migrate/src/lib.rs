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
//!     let orchestrator = Orchestrator::new(config).await?;
//!     let cancel = CancellationToken::new();
//!     let result = orchestrator.run(cancel, false).await?;
//!     println!("Migrated {} rows", result.rows_transferred);
//!     Ok(())
//! }
//! ```

pub mod config;
pub mod error;
pub mod orchestrator;
pub mod source;
pub mod state;
pub mod target;
pub mod transfer;
pub mod typemap;
pub mod verify;

// Re-exports for convenient access
pub use config::{
    BatchVerifyConfig, Config, DatabaseType, MigrationConfig, SourceConfig, TableStats, TargetConfig, TargetMode,
};
pub use error::{MigrateError, Result};
pub use orchestrator::{HealthCheckResult, MigrationResult, Orchestrator, ProgressUpdate, TableError};
pub use source::{MssqlPool, PgSourcePool, Table};
pub use state::MigrationState;
pub use target::{MssqlTargetPool, PgPool, SqlNullType, SqlValue};
pub use transfer::{TransferConfig, TransferEngine, TransferJob, TransferStats};
pub use typemap::{map_type, mssql_to_postgres, postgres_to_mssql, TypeMapping};
pub use verify::{
    BatchHashResult, CompositePk, RowHashDiffComposite, RowRange, TableVerifyResult,
    UniversalVerifyEngine, VerifyEngine, VerifyProgressUpdate, VerifyResult, VerifyTier,
};
