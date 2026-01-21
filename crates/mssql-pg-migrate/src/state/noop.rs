//! No-op state backend for databases without resume capability.
//!
//! Used for MySQL targets where state persistence is not yet implemented.
//! Migrations will complete but cannot be resumed if interrupted.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::warn;

use crate::error::Result;
use crate::state::backend::StateBackend;
use crate::state::MigrationState;

/// No-op state backend that doesn't persist state.
///
/// Used for database targets that don't yet have a dedicated state backend.
/// Logs a warning on first use.
pub struct NoOpStateBackend {
    warned: std::sync::atomic::AtomicBool,
}

impl NoOpStateBackend {
    /// Create a new no-op state backend.
    pub fn new() -> Self {
        Self {
            warned: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn warn_once(&self) {
        if !self
            .warned
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            warn!(
                "Using no-op state backend: migration state will not be persisted. \
                 Resume capability is not available for this target database type."
            );
        }
    }
}

impl Default for NoOpStateBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateBackend for NoOpStateBackend {
    async fn init_schema(&self) -> Result<()> {
        self.warn_once();
        Ok(())
    }

    async fn save(&self, _state: &MigrationState) -> Result<()> {
        // No-op: state is not persisted
        Ok(())
    }

    async fn load_latest(&self, _config_hash: &str) -> Result<Option<MigrationState>> {
        // No-op: always return None (no previous state)
        Ok(None)
    }

    async fn get_last_sync_timestamp(&self, _table_name: &str) -> Result<Option<DateTime<Utc>>> {
        // No-op: no historical sync timestamps
        Ok(None)
    }

    fn backend_type(&self) -> &'static str {
        "noop"
    }
}
