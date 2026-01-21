//! Database driver implementations.
//!
//! This module provides database-specific implementations of the core traits:
//!
//! - [`mssql`]: Microsoft SQL Server driver
//! - [`postgres`]: PostgreSQL driver
//! - [`common`]: Shared utilities (TLS, connection helpers)
//!
//! # Architecture
//!
//! Each driver module implements:
//! - `SourceReader`: For reading schema and data from the database
//! - `TargetWriter`: For writing schema and data to the database
//! - `Dialect`: SQL syntax strategy for the database engine
//!
//! # enum_dispatch
//!
//! This module uses `enum_dispatch` for zero-cost polymorphism. Instead of
//! dynamic dispatch via `Box<dyn Trait>`, we use enum variants that implement
//! traits directly, achieving the same abstraction with static dispatch.
//!
//! # Adding New Databases
//!
//! To add support for a new database:
//!
//! 1. Create a new module under `drivers/` (e.g., `drivers/mysql/`)
//! 2. Implement `SourceReader`, `TargetWriter`, and `Dialect` traits
//! 3. Add enum variants to `SourceReaderImpl`, `TargetWriterImpl`, `DialectImpl`
//! 4. Register type mappers in `DriverCatalog::with_builtins()`
//! 5. Gate the driver with a feature flag in `Cargo.toml`

pub mod common;

// Driver modules will be added as they are extracted:
// pub mod mssql;
// pub mod postgres;

// Re-export common utilities
pub use common::{SslMode, TlsBuilder};

// enum_dispatch types will be defined here once drivers are extracted.
// For now, the existing SourcePoolImpl/TargetPoolImpl in orchestrator/pools.rs
// continue to function until migration is complete.
//
// The target structure will be:
//
// ```rust
// use enum_dispatch::enum_dispatch;
//
// #[enum_dispatch(SourceReader)]
// pub enum SourceReaderImpl {
//     Mssql(mssql::MssqlReader),
//     Postgres(postgres::PostgresReader),
// }
//
// #[enum_dispatch(TargetWriter)]
// pub enum TargetWriterImpl {
//     Mssql(mssql::MssqlWriter),
//     Postgres(postgres::PostgresWriter),
// }
//
// #[enum_dispatch(Dialect)]
// pub enum DialectImpl {
//     Mssql(mssql::MssqlDialect),
//     Postgres(postgres::PostgresDialect),
// }
// ```
