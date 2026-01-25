//! Common utilities shared across database drivers.
//!
//! This module provides shared functionality used by multiple driver implementations:
//!
//! - [`tls`]: Unified TLS configuration for PostgreSQL connections
//! - [`pg_binary`]: PostgreSQL COPY binary format parser

pub mod pg_binary;
pub mod tls;

pub use pg_binary::{BinaryColumnType, BinaryRowParser};
pub use tls::{SslMode, TlsBuilder};
