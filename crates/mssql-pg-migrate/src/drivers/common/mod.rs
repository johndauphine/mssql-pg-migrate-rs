//! Common utilities shared across database drivers.
//!
//! This module provides shared functionality used by multiple driver implementations:
//!
//! - [`tls`]: Unified TLS configuration for PostgreSQL connections

pub mod tls;

pub use tls::{SslMode, TlsBuilder};
