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
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = Config::load("config.yaml")?;
//!     let orchestrator = Orchestrator::new(config).await?;
//!     let result = orchestrator.run(None).await?;
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

// Re-exports for convenient access
pub use config::{Config, MigrationConfig, SourceConfig, TargetConfig, TargetMode};
pub use error::{MigrateError, Result};
pub use orchestrator::{MigrationResult, Orchestrator};
pub use source::{MssqlPool, Table};
pub use state::MigrationState;
pub use target::{PgPool, SqlValue};
pub use transfer::{TransferConfig, TransferEngine, TransferJob, TransferStats};
