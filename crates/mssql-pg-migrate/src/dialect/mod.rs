//! Type mapping and dialect utilities.
//!
//! This module provides type mappers for converting column types between
//! different database dialects.
//!
//! # Hub-and-Spoke Canonical Type System
//!
//! The primary approach uses a canonical intermediate type representation.
//! Instead of n*(n-1) direct mappers, we use 2n converters:
//!
//! - [`ToCanonical`]: Convert native type → canonical type
//! - [`FromCanonical`]: Convert canonical type → native type
//! - [`ComposedMapper`]: Chains both conversions to implement [`TypeMapper`](crate::core::traits::TypeMapper)
//!
//! ## Available Converters
//!
//! | Dialect | ToCanonical | FromCanonical |
//! |---------|-------------|---------------|
//! | MSSQL | [`MssqlToCanonical`] | [`MssqlFromCanonical`] |
//! | PostgreSQL | [`PostgresToCanonical`] | [`PostgresFromCanonical`] |
//! | MySQL | [`MysqlToCanonical`] | [`MysqlFromCanonical`] |
//!
//! ## Canonical Types
//!
//! See [`CanonicalType`] for the full list of intermediate types.
//!
//! # Legacy Direct Mappers
//!
//! The following direct mappers are still available for backwards compatibility:
//!
//! - [`MssqlToPostgresMapper`]: MSSQL → PostgreSQL (all lossless)
//! - [`PostgresToMssqlMapper`]: PostgreSQL → MSSQL (some lossy)
//! - [`MysqlToPostgresMapper`]: MySQL → PostgreSQL (mostly lossless)
//! - [`PostgresToMysqlMapper`]: PostgreSQL → MySQL (some lossy)
//! - [`MysqlToMssqlMapper`]: MySQL → MSSQL
//! - [`MssqlToMysqlMapper`]: MSSQL → MySQL
//! - [`IdentityMapper`]: Same dialect transfers (passthrough)
//!
//! # Usage
//!
//! Mappers are typically registered in a [`DriverCatalog`](crate::core::DriverCatalog)
//! and retrieved by source/target dialect pair:
//!
//! ```rust,ignore
//! // Using ComposedMapper (recommended)
//! use mssql_pg_migrate::dialect::{ComposedMapper, MssqlToCanonical, PostgresFromCanonical};
//! let mapper = ComposedMapper::new(
//!     Arc::new(MssqlToCanonical::new()),
//!     Arc::new(PostgresFromCanonical::new()),
//! );
//!
//! // Or via catalog
//! let catalog = DriverCatalog::with_builtins();
//! let mapper = catalog.require_mapper("mssql", "postgres")?;
//! let mapping = mapper.map_column(&column);
//! ```

mod canonical;
mod typemap;

// Canonical type system exports
pub use canonical::{CanonicalType, CanonicalTypeInfo, ComposedMapper, FromCanonical, ToCanonical};

// Canonical converters
pub use typemap::{
    MssqlFromCanonical, MssqlToCanonical, MysqlFromCanonical, MysqlToCanonical,
    PostgresFromCanonical, PostgresToCanonical,
};

// Legacy direct mappers (for backwards compatibility)
pub use typemap::{
    mssql_to_mysql_basic, postgres_to_mysql_basic, IdentityMapper, MssqlToMysqlMapper,
    MssqlToPostgresMapper, MysqlToMssqlMapper, MysqlToPostgresMapper, PostgresToMssqlMapper,
    PostgresToMysqlMapper,
};
