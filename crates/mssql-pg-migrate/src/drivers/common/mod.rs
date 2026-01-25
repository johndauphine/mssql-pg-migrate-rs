//! Common utilities shared across database drivers.
//!
//! This module provides shared functionality used by multiple driver implementations:
//!
//! - [`tls`]: Unified TLS configuration for PostgreSQL connections
//! - [`pg_binary`]: PostgreSQL COPY binary format parser
//! - [`direct_copy`]: Direct COPY binary encoder for MSSQL to PostgreSQL

pub mod direct_copy;
pub mod pg_binary;
pub mod tls;

pub use direct_copy::DirectCopyEncoder;
pub use pg_binary::{BinaryColumnType, BinaryRowParser};
pub use tls::{SslMode, TlsBuilder};
