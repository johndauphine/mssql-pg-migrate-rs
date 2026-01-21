//! Type mapping and dialect utilities.
//!
//! This module provides type mappers for converting column types between
//! different database dialects. The mappers use a (source, target) pair
//! keying approach to solve the N² complexity problem.
//!
//! # Available Mappers
//!
//! - [`MssqlToPostgresMapper`]: MSSQL → PostgreSQL (all lossless)
//! - [`PostgresToMssqlMapper`]: PostgreSQL → MSSQL (some lossy)
//! - [`IdentityMapper`]: Same dialect transfers (passthrough)
//!
//! # Usage
//!
//! Mappers are typically registered in a [`DriverCatalog`](crate::core::DriverCatalog)
//! and retrieved by source/target dialect pair:
//!
//! ```rust,ignore
//! let catalog = DriverCatalog::with_builtins();
//! let mapper = catalog.require_mapper("mssql", "postgres")?;
//! let mapping = mapper.map_column(&column);
//! ```

mod typemap;

pub use typemap::{IdentityMapper, MssqlToPostgresMapper, PostgresToMssqlMapper};
