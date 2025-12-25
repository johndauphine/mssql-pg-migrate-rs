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
        format!(
            "Server=tcp:{},{};Database={};User Id={};Password={};Encrypt={};TrustServerCertificate={}",
            self.host,
            self.port,
            self.database,
            self.user,
            self.password,
            self.encrypt,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    const VALID_YAML: &str = r#"
source:
  type: mssql
  host: localhost
  port: 1433
  database: source_db
  user: sa
  password: password
  schema: dbo
  encrypt: false
  trust_server_cert: true

target:
  type: postgres
  host: localhost
  port: 5432
  database: target_db
  user: postgres
  password: password
  schema: public
  ssl_mode: disable

migration:
  workers: 4
  chunk_size: 100000
"#;

    const VALID_JSON: &str = r#"{
  "source": {
    "type": "mssql",
    "host": "localhost",
    "port": 1433,
    "database": "source_db",
    "user": "sa",
    "password": "password",
    "schema": "dbo",
    "encrypt": false,
    "trust_server_cert": true
  },
  "target": {
    "type": "postgres",
    "host": "localhost",
    "port": 5432,
    "database": "target_db",
    "user": "postgres",
    "password": "password",
    "schema": "public",
    "ssl_mode": "disable"
  },
  "migration": {
    "workers": 4,
    "chunk_size": 100000
  }
}"#;

    // Tests for from_json() method
    #[test]
    fn test_from_json_valid() {
        let config = Config::from_json(VALID_JSON).unwrap();
        assert_eq!(config.source.host, "localhost");
        assert_eq!(config.source.port, 1433);
        assert_eq!(config.target.database, "target_db");
        assert_eq!(config.migration.workers, Some(4));
    }

    #[test]
    fn test_from_json_invalid_syntax() {
        let invalid = r#"{ "source": { invalid json }"#;
        let result = Config::from_json(invalid);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, MigrateError::Config(_)));
    }

    #[test]
    fn test_from_json_missing_required_field() {
        let missing_host = r#"{
          "source": {
            "type": "mssql",
            "port": 1433,
            "database": "db",
            "user": "sa",
            "password": "pass",
            "schema": "dbo"
          },
          "target": {
            "type": "postgres",
            "host": "localhost",
            "port": 5432,
            "database": "db",
            "user": "pg",
            "password": "pass",
            "schema": "public"
          }
        }"#;
        let result = Config::from_json(missing_host);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_json_validates_config() {
        // Empty host should fail validation
        let invalid_config = r#"{
          "source": {
            "type": "mssql",
            "host": "",
            "port": 1433,
            "database": "db",
            "user": "sa",
            "password": "pass",
            "schema": "dbo"
          },
          "target": {
            "type": "postgres",
            "host": "localhost",
            "port": 5432,
            "database": "db",
            "user": "pg",
            "password": "pass",
            "schema": "public"
          }
        }"#;
        let result = Config::from_json(invalid_config);
        assert!(result.is_err());
    }

    // Tests for from_yaml() method
    #[test]
    fn test_from_yaml_valid() {
        let config = Config::from_yaml(VALID_YAML).unwrap();
        assert_eq!(config.source.host, "localhost");
        assert_eq!(config.source.port, 1433);
        assert_eq!(config.target.database, "target_db");
        assert_eq!(config.migration.workers, Some(4));
    }

    // Tests for load() file extension detection
    #[test]
    fn test_load_json_extension() {
        let mut file = NamedTempFile::with_suffix(".json").unwrap();
        file.write_all(VALID_JSON.as_bytes()).unwrap();
        file.flush().unwrap();

        let config = Config::load(file.path()).unwrap();
        assert_eq!(config.source.host, "localhost");
        assert_eq!(config.source.database, "source_db");
    }

    #[test]
    fn test_load_yaml_extension() {
        let mut file = NamedTempFile::with_suffix(".yaml").unwrap();
        file.write_all(VALID_YAML.as_bytes()).unwrap();
        file.flush().unwrap();

        let config = Config::load(file.path()).unwrap();
        assert_eq!(config.source.host, "localhost");
        assert_eq!(config.source.database, "source_db");
    }

    #[test]
    fn test_load_yml_extension() {
        let mut file = NamedTempFile::with_suffix(".yml").unwrap();
        file.write_all(VALID_YAML.as_bytes()).unwrap();
        file.flush().unwrap();

        let config = Config::load(file.path()).unwrap();
        assert_eq!(config.source.host, "localhost");
        assert_eq!(config.source.database, "source_db");
    }

    #[test]
    fn test_load_unknown_extension_defaults_to_yaml() {
        let mut file = NamedTempFile::with_suffix(".conf").unwrap();
        file.write_all(VALID_YAML.as_bytes()).unwrap();
        file.flush().unwrap();

        let config = Config::load(file.path()).unwrap();
        assert_eq!(config.source.host, "localhost");
    }

    #[test]
    fn test_load_uppercase_json_extension() {
        let mut file = NamedTempFile::with_suffix(".JSON").unwrap();
        file.write_all(VALID_JSON.as_bytes()).unwrap();
        file.flush().unwrap();

        let config = Config::load(file.path()).unwrap();
        assert_eq!(config.source.host, "localhost");
    }

    #[test]
    fn test_load_uppercase_yaml_extension() {
        let mut file = NamedTempFile::with_suffix(".YAML").unwrap();
        file.write_all(VALID_YAML.as_bytes()).unwrap();
        file.flush().unwrap();

        let config = Config::load(file.path()).unwrap();
        assert_eq!(config.source.host, "localhost");
    }

    #[test]
    fn test_json_and_yaml_produce_same_config() {
        let json_config = Config::from_json(VALID_JSON).unwrap();
        let yaml_config = Config::from_yaml(VALID_YAML).unwrap();

        assert_eq!(json_config.source.host, yaml_config.source.host);
        assert_eq!(json_config.source.port, yaml_config.source.port);
        assert_eq!(json_config.source.database, yaml_config.source.database);
        assert_eq!(json_config.target.host, yaml_config.target.host);
        assert_eq!(json_config.target.port, yaml_config.target.port);
        assert_eq!(json_config.target.database, yaml_config.target.database);
        assert_eq!(json_config.migration.workers, yaml_config.migration.workers);
        assert_eq!(json_config.migration.chunk_size, yaml_config.migration.chunk_size);
    }
}
