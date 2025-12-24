//! Configuration loading and validation.

mod types;
mod validation;

pub use types::*;

use crate::error::Result;
use sha2::{Digest, Sha256};
use std::path::Path;

impl Config {
    /// Load configuration from a YAML file.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Self::from_yaml(&content)
    }

    /// Parse configuration from a YAML string.
    pub fn from_yaml(yaml: &str) -> Result<Self> {
        let config: Config = serde_yaml::from_str(yaml)?;
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
