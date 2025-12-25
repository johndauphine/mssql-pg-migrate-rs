//! Error types for the migration library.

use thiserror::Error;

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

    /// Connection pool error with context
    #[error("Pool error: {message}\n  Context: {context}")]
    Pool { message: String, context: String },

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

impl MigrateError {
    /// Create a Pool error with context about where it occurred
    pub fn pool(message: impl Into<String>, context: impl Into<String>) -> Self {
        MigrateError::Pool {
            message: message.into(),
            context: context.into(),
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
}

/// Result type alias for migration operations.
pub type Result<T> = std::result::Result<T, MigrateError>;
