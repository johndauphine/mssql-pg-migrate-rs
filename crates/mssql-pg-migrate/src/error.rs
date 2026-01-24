//! Error types for the migration library.

use thiserror::Error;

// Exit codes for Airflow integration
pub const EXIT_SUCCESS: u8 = 0;
pub const EXIT_CONFIG_ERROR: u8 = 1; // Config, Yaml, Json errors - don't retry
pub const EXIT_CONNECTION_ERROR: u8 = 2; // Source, Target, Pool errors - recoverable
pub const EXIT_TRANSFER_ERROR: u8 = 3; // Transfer, SchemaExtraction errors
pub const EXIT_VALIDATION_ERROR: u8 = 4; // Validation, NoPrimaryKey errors
pub const EXIT_CANCELLED: u8 = 5; // User cancelled (SIGINT/SIGTERM)
pub const EXIT_STATE_ERROR: u8 = 6; // State, ConfigChanged errors
pub const EXIT_IO_ERROR: u8 = 7; // IO errors - potentially recoverable

/// Main error type for migration operations.
#[derive(Error, Debug)]
pub enum MigrateError {
    /// Configuration error (invalid YAML, missing fields, etc.)
    #[error("Configuration error: {0}")]
    Config(String),

    /// Source database connection or query error
    #[error("Source database error: {0}")]
    Source(#[from] tiberius::error::Error),

    /// Target database connection or query error
    #[error("Target database error: {0}")]
    Target(#[from] tokio_postgres::Error),

    /// Connection pool error with context and optional source
    #[error("Pool error: {message}\n  Context: {context}")]
    Pool {
        message: String,
        context: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Schema extraction failed
    #[error("Schema extraction failed: {0}")]
    SchemaExtraction(String),

    /// Data transfer failed for a specific table
    #[error("Transfer failed for table {table}: {message}")]
    Transfer { table: String, message: String },

    /// Row count validation failed
    #[error("Validation failed: {0}")]
    Validation(String),

    /// Table has no primary key (required for upsert mode)
    #[error("Table {0} has no primary key - upsert mode requires primary keys")]
    NoPrimaryKey(String),

    /// State file error
    #[error("State file error: {0}")]
    State(String),

    /// Config hash mismatch on resume
    #[error("Config has changed since last run - cannot resume. Use --force to start fresh.")]
    ConfigChanged,

    /// IO error (file operations)
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// YAML serialization/deserialization error
    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Migration was cancelled (SIGINT, etc.)
    #[error("Migration cancelled")]
    Cancelled,
}

impl From<deadpool_postgres::PoolError> for MigrateError {
    fn from(err: deadpool_postgres::PoolError) -> Self {
        MigrateError::pool(err, "PostgreSQL connection pool")
    }
}

#[cfg(feature = "mysql")]
impl From<mysql_async::Error> for MigrateError {
    fn from(err: mysql_async::Error) -> Self {
        MigrateError::pool(err, "MySQL database")
    }
}

impl MigrateError {
    /// Create a Pool error with context about where it occurred, preserving the source error
    pub fn pool<E>(error: E, context: impl Into<String>) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        MigrateError::Pool {
            message: error.to_string(),
            context: context.into(),
            source: Some(Box::new(error)),
        }
    }

    /// Create a Pool error with just a message (no source error)
    pub fn pool_msg(message: impl Into<String>, context: impl Into<String>) -> Self {
        MigrateError::Pool {
            message: message.into(),
            context: context.into(),
            source: None,
        }
    }

    /// Create a Transfer error
    pub fn transfer(table: impl Into<String>, message: impl Into<String>) -> Self {
        MigrateError::Transfer {
            table: table.into(),
            message: message.into(),
        }
    }

    /// Format error with full details including error chain
    pub fn format_detailed(&self) -> String {
        let mut output = format!("Error: {}\n", self);

        // Add error chain for wrapped errors
        let mut source = std::error::Error::source(self);
        let mut depth = 1;
        while let Some(err) = source {
            output.push_str(&format!("\nCaused by:\n  {}: {}", depth, err));
            source = err.source();
            depth += 1;
        }

        output
    }

    /// Get the exit code for this error type.
    /// Used by CLI for Airflow integration.
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Config(_) | Self::Yaml(_) | Self::Json(_) => EXIT_CONFIG_ERROR,
            Self::Source(_) | Self::Target(_) | Self::Pool { .. } => EXIT_CONNECTION_ERROR,
            Self::Transfer { .. } | Self::SchemaExtraction(_) => EXIT_TRANSFER_ERROR,
            Self::Validation(_) | Self::NoPrimaryKey(_) => EXIT_VALIDATION_ERROR,
            Self::Cancelled => EXIT_CANCELLED,
            Self::State(_) | Self::ConfigChanged => EXIT_STATE_ERROR,
            Self::Io(_) => EXIT_IO_ERROR,
        }
    }

    /// Check if this error is potentially recoverable (worth retrying).
    /// Connection errors and cancellation are typically recoverable.
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Self::Source(_) | Self::Target(_) | Self::Pool { .. } | Self::Io(_) | Self::Cancelled
        )
    }

    /// Get the error type as a string for JSON output.
    pub fn error_type(&self) -> &'static str {
        match self {
            Self::Config(_) => "config",
            Self::Yaml(_) => "yaml",
            Self::Json(_) => "json",
            Self::Source(_) => "source_db",
            Self::Target(_) => "target_db",
            Self::Pool { .. } => "connection_pool",
            Self::Transfer { .. } => "transfer",
            Self::SchemaExtraction(_) => "schema_extraction",
            Self::Validation(_) => "validation",
            Self::NoPrimaryKey(_) => "no_primary_key",
            Self::State(_) => "state",
            Self::ConfigChanged => "config_changed",
            Self::Io(_) => "io",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Result type alias for migration operations.
pub type Result<T> = std::result::Result<T, MigrateError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exit_code_config_errors() {
        assert_eq!(
            MigrateError::Config("test".into()).exit_code(),
            EXIT_CONFIG_ERROR
        );
    }

    #[test]
    fn test_exit_code_connection_errors() {
        let pool_err = MigrateError::Pool {
            message: "test".into(),
            context: "ctx".into(),
            source: None,
        };
        assert_eq!(pool_err.exit_code(), EXIT_CONNECTION_ERROR);
    }

    #[test]
    fn test_exit_code_transfer_errors() {
        let err = MigrateError::Transfer {
            table: "test".into(),
            message: "msg".into(),
        };
        assert_eq!(err.exit_code(), EXIT_TRANSFER_ERROR);
        assert_eq!(
            MigrateError::SchemaExtraction("test".into()).exit_code(),
            EXIT_TRANSFER_ERROR
        );
    }

    #[test]
    fn test_exit_code_validation_errors() {
        assert_eq!(
            MigrateError::Validation("test".into()).exit_code(),
            EXIT_VALIDATION_ERROR
        );
        assert_eq!(
            MigrateError::NoPrimaryKey("table".into()).exit_code(),
            EXIT_VALIDATION_ERROR
        );
    }

    #[test]
    fn test_exit_code_cancelled() {
        assert_eq!(MigrateError::Cancelled.exit_code(), EXIT_CANCELLED);
    }

    #[test]
    fn test_exit_code_state_errors() {
        assert_eq!(
            MigrateError::State("test".into()).exit_code(),
            EXIT_STATE_ERROR
        );
        assert_eq!(MigrateError::ConfigChanged.exit_code(), EXIT_STATE_ERROR);
    }

    #[test]
    fn test_exit_code_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "test");
        assert_eq!(MigrateError::Io(io_err).exit_code(), EXIT_IO_ERROR);
    }

    #[test]
    fn test_is_recoverable_pool_error() {
        let err = MigrateError::Pool {
            message: "".into(),
            context: "".into(),
            source: None,
        };
        assert!(err.is_recoverable());
    }

    #[test]
    fn test_is_recoverable_cancelled() {
        assert!(MigrateError::Cancelled.is_recoverable());
    }

    #[test]
    fn test_is_recoverable_io_error() {
        let io_err = std::io::Error::other("test");
        assert!(MigrateError::Io(io_err).is_recoverable());
    }

    #[test]
    fn test_not_recoverable_config_error() {
        assert!(!MigrateError::Config("test".into()).is_recoverable());
    }

    #[test]
    fn test_not_recoverable_validation_error() {
        assert!(!MigrateError::Validation("test".into()).is_recoverable());
    }

    #[test]
    fn test_not_recoverable_transfer_error() {
        let err = MigrateError::Transfer {
            table: "".into(),
            message: "".into(),
        };
        assert!(!err.is_recoverable());
    }

    #[test]
    fn test_error_type_config() {
        assert_eq!(MigrateError::Config("".into()).error_type(), "config");
    }

    #[test]
    fn test_error_type_cancelled() {
        assert_eq!(MigrateError::Cancelled.error_type(), "cancelled");
    }

    #[test]
    fn test_error_type_connection_pool() {
        let err = MigrateError::Pool {
            message: "".into(),
            context: "".into(),
            source: None,
        };
        assert_eq!(err.error_type(), "connection_pool");
    }

    #[test]
    fn test_error_type_transfer() {
        let err = MigrateError::Transfer {
            table: "".into(),
            message: "".into(),
        };
        assert_eq!(err.error_type(), "transfer");
    }

    #[test]
    fn test_error_type_validation() {
        assert_eq!(
            MigrateError::Validation("".into()).error_type(),
            "validation"
        );
    }

    #[test]
    fn test_error_type_state() {
        assert_eq!(MigrateError::State("".into()).error_type(), "state");
    }

    #[test]
    fn test_error_type_config_changed() {
        assert_eq!(MigrateError::ConfigChanged.error_type(), "config_changed");
    }

    #[test]
    fn test_pool_helper() {
        let io_err = std::io::Error::other("test error");
        let err = MigrateError::pool(io_err, "context");
        match &err {
            MigrateError::Pool {
                message,
                context,
                source,
            } => {
                assert_eq!(message, "test error");
                assert_eq!(context, "context");
                assert!(source.is_some());
            }
            _ => panic!("Expected Pool error"),
        }
        // Verify error chain is preserved
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn test_pool_msg_helper() {
        let err = MigrateError::pool_msg("message", "context");
        match err {
            MigrateError::Pool {
                message,
                context,
                source,
            } => {
                assert_eq!(message, "message");
                assert_eq!(context, "context");
                assert!(source.is_none());
            }
            _ => panic!("Expected Pool error"),
        }
    }

    #[test]
    fn test_transfer_helper() {
        let err = MigrateError::transfer("table", "message");
        match err {
            MigrateError::Transfer { table, message } => {
                assert_eq!(table, "table");
                assert_eq!(message, "message");
            }
            _ => panic!("Expected Transfer error"),
        }
    }
}
