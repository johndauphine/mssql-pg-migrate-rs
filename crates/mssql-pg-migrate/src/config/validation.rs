//! Configuration validation.

use super::Config;
use crate::error::{MigrateError, Result};

/// Validate the configuration.
pub fn validate(config: &Config) -> Result<()> {
    // Source validation
    if config.source.host.is_empty() {
        return Err(MigrateError::Config("source.host is required".into()));
    }
    if config.source.database.is_empty() {
        return Err(MigrateError::Config("source.database is required".into()));
    }
    if config.source.user.is_empty() {
        return Err(MigrateError::Config("source.user is required".into()));
    }
    if config.source.r#type != "mssql" {
        return Err(MigrateError::Config(format!(
            "source.type must be 'mssql', got '{}'",
            config.source.r#type
        )));
    }

    // Target validation
    if config.target.host.is_empty() {
        return Err(MigrateError::Config("target.host is required".into()));
    }
    if config.target.database.is_empty() {
        return Err(MigrateError::Config("target.database is required".into()));
    }
    if config.target.user.is_empty() {
        return Err(MigrateError::Config("target.user is required".into()));
    }
    if config.target.r#type != "postgres" {
        return Err(MigrateError::Config(format!(
            "target.type must be 'postgres', got '{}'",
            config.target.r#type
        )));
    }

    // Cannot migrate to the same database
    if config.source.host == config.target.host
        && config.source.port as u16 == config.target.port
        && config.source.database == config.target.database
    {
        return Err(MigrateError::Config(
            "source and target cannot be the same database".into(),
        ));
    }

    // Migration config validation - only check if explicitly set
    if let Some(0) = config.migration.workers {
        return Err(MigrateError::Config(
            "migration.workers must be at least 1".into(),
        ));
    }
    if let Some(0) = config.migration.chunk_size {
        return Err(MigrateError::Config(
            "migration.chunk_size must be at least 1".into(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MigrationConfig, SourceConfig, TargetConfig};

    fn valid_config() -> Config {
        Config {
            source: SourceConfig {
                r#type: "mssql".to_string(),
                host: "localhost".to_string(),
                port: 1433,
                database: "source_db".to_string(),
                user: "sa".to_string(),
                password: "password".to_string(),
                schema: "dbo".to_string(),
                encrypt: false,
                trust_server_cert: true,
            },
            target: TargetConfig {
                r#type: "postgres".to_string(),
                host: "localhost".to_string(),
                port: 5432,
                database: "target_db".to_string(),
                user: "postgres".to_string(),
                password: "password".to_string(),
                schema: "public".to_string(),
                ssl_mode: "disable".to_string(),
            },
            migration: MigrationConfig::default(),
        }
    }

    #[test]
    fn test_valid_config() {
        let config = valid_config();
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_missing_source_host() {
        let mut config = valid_config();
        config.source.host = "".to_string();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn test_wrong_source_type() {
        let mut config = valid_config();
        config.source.r#type = "postgres".to_string();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn test_wrong_target_type() {
        let mut config = valid_config();
        config.target.r#type = "mssql".to_string();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn test_source_config_debug_redacts_password() {
        let mut config = valid_config();
        config.source.password = "super_secret_password_123".to_string();
        let debug_output = format!("{:?}", config.source);
        assert!(
            debug_output.contains("[REDACTED]"),
            "Debug output should contain [REDACTED]"
        );
        assert!(
            !debug_output.contains("super_secret_password_123"),
            "Debug output should not contain actual password value"
        );
    }

    #[test]
    fn test_target_config_debug_redacts_password() {
        let mut config = valid_config();
        config.target.password = "super_secret_password_456".to_string();
        let debug_output = format!("{:?}", config.target);
        assert!(
            debug_output.contains("[REDACTED]"),
            "Debug output should contain [REDACTED]"
        );
        assert!(
            !debug_output.contains("super_secret_password_456"),
            "Debug output should not contain actual password value"
        );
    }
}
