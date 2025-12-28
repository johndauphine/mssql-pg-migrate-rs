//! Multi-tier batch verification for efficient data sync validation.
//!
//! This module implements a 4-tier drill-down approach to efficiently verify
//! data synchronization between MSSQL and PostgreSQL:
//!
//! - **Tier 1 (Coarse)**: Use NTILE to partition table, compare row counts
//! - **Tier 2 (Fine)**: For mismatches, subdivide using ROW_NUMBER ranges
//! - **Tier 3 (Row)**: Compare (PK, row_hash) pairs using SQL-side hashing
//! - **Tier 4 (Sync)**: Transfer only changed rows
//!
//! MSSQL computes hashes server-side using HASHBYTES to minimize data transfer.
//! PostgreSQL uses the existing row_hash column computed during migration.
//!
//! The `UniversalVerifyEngine` supports any primary key type including integers,
//! UUIDs, strings, and composite keys.

pub mod hash_query;
pub mod normalize;
pub mod normalize_pg;
pub mod types;
pub mod universal;

// Re-exports
pub use types::{
    BatchHashResult, CompositePk, CompositeRowHashMap, PkValue, RowHashDiffComposite, RowRange,
    SyncStats, TableVerifyResult, VerifyProgressUpdate, VerifyResult, VerifyTier,
};
pub use universal::UniversalVerifyEngine;

// Re-export as VerifyEngine for backward compatibility
pub use universal::UniversalVerifyEngine as VerifyEngine;
