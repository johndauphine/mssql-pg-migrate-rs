//! Configuration loading and validation.

mod types;
mod validation;

pub use types::*;

use crate::error::{MigrateError, Result};
use sha2::{Digest, Sha256};
use std::path::Path;

impl Config {
    /// Load configuration from a file.
    ///
    /// Automatically detects the format based on file extension:
    /// - `.json` files are parsed as JSON
    /// - `.yaml` or `.yml` files are parsed as YAML
    /// - Other extensions default to YAML for backward compatibility
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)?;

        // Detect format from extension
        let extension = path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_lowercase());

        match extension.as_deref() {
            Some("json") => Self::from_json(&content),
            _ => Self::from_yaml(&content), // Default to YAML for .yaml, .yml, or unknown
        }
    }

    /// Parse configuration from a YAML string.
    pub fn from_yaml(yaml: &str) -> Result<Self> {
        let config: Config = serde_yaml::from_str(yaml)?;
        config.validate()?;
        Ok(config)
    }

    /// Parse configuration from a JSON string.
    pub fn from_json(json: &str) -> Result<Self> {
        let config: Config = serde_json::from_str(json).map_err(|e| {
            MigrateError::Config(format!("Failed to parse JSON config: {}", e))
        })?;
        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        validation::validate(self)
    }

    /// Compute a SHA256 hash of the configuration for resume validation.
    pub fn hash(&self) -> String {
        let yaml = serde_yaml::to_string(self).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(yaml.as_bytes());
        format!("{:x}", hasher.finalize())
    }
}

impl SourceConfig {
    /// Build a connection string for tiberius.
    pub fn connection_string(&self) -> String {
        let encrypt = match self.encrypt.to_lowercase().as_str() {
            "true" | "yes" | "1" => "true",
            "false" | "no" | "0" | "disable" => "false",
            _ => "true",
        };

        format!(
            "Server=tcp:{},{};Database={};User Id={};Password={};Encrypt={};TrustServerCertificate={}",
            self.host,
            self.port,
            self.database,
            self.user,
            self.password,
            encrypt,
            self.trust_server_cert
        )
    }
}

impl TargetConfig {
    /// Build a connection string for tokio-postgres.
    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} dbname={} user={} password={} sslmode={}",
            self.host, self.port, self.database, self.user, self.password, self.ssl_mode
        )
    }
}
