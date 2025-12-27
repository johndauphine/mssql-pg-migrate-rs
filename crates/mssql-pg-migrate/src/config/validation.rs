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

    // Migration config validation - validate all Option<usize> fields when Some(0)
    // These fields must be at least 1 if explicitly set
    validate_nonzero_option(config.migration.workers, "migration.workers")?;
    validate_nonzero_option(config.migration.chunk_size, "migration.chunk_size")?;
    validate_nonzero_option(config.migration.max_partitions, "migration.max_partitions")?;
    validate_nonzero_option(config.migration.read_ahead_buffers, "migration.read_ahead_buffers")?;
    validate_nonzero_option(config.migration.write_ahead_writers, "migration.write_ahead_writers")?;
    validate_nonzero_option(config.migration.parallel_readers, "migration.parallel_readers")?;
    validate_nonzero_option(config.migration.max_mssql_connections, "migration.max_mssql_connections")?;
    validate_nonzero_option(config.migration.max_pg_connections, "migration.max_pg_connections")?;
    validate_nonzero_option(config.migration.finalizer_concurrency, "migration.finalizer_concurrency")?;
    validate_nonzero_option(config.migration.copy_buffer_rows, "migration.copy_buffer_rows")?;
    validate_nonzero_option(config.migration.upsert_batch_size, "migration.upsert_batch_size")?;
    validate_nonzero_option(config.migration.upsert_parallel_tasks, "migration.upsert_parallel_tasks")?;

    // Validate table filter patterns for SQL injection prevention
    for pattern in &config.migration.include_tables {
        validate_table_pattern(pattern, "include_tables")?;
    }
    for pattern in &config.migration.exclude_tables {
        validate_table_pattern(pattern, "exclude_tables")?;
    }

    Ok(())
}

/// Validate that an optional usize field is not zero when set
fn validate_nonzero_option(value: Option<usize>, field_name: &str) -> Result<()> {
    if let Some(0) = value {
        return Err(MigrateError::Config(format!(
            "{} must be at least 1",
            field_name
        )));
    }
    Ok(())
}

/// Validate a table name pattern for SQL injection prevention.
///
/// Allowed characters:
/// - Alphanumeric (a-z, A-Z, 0-9)
/// - Underscore (_)
/// - Dot (.) for schema.table patterns
/// - Glob wildcards (* and ?)
///
/// This prevents SQL injection through table filter patterns.
fn validate_table_pattern(pattern: &str, field_name: &str) -> Result<()> {
    if pattern.is_empty() {
        return Err(MigrateError::Config(format!(
            "{} contains empty pattern",
            field_name
        )));
    }

    for ch in pattern.chars() {
        if !ch.is_ascii_alphanumeric() && ch != '_' && ch != '.' && ch != '*' && ch != '?' {
            return Err(MigrateError::Config(format!(
                "{} pattern '{}' contains invalid character '{}'. \
                 Only alphanumeric, underscore, dot, and glob wildcards (* ?) are allowed.",
                field_name, pattern, ch
            )));
        }
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

    #[test]
    fn test_zero_parallel_readers_rejected() {
        let mut config = valid_config();
        config.migration.parallel_readers = Some(0);
        let result = validate(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("parallel_readers"));
    }

    #[test]
    fn test_zero_parallel_writers_rejected() {
        let mut config = valid_config();
        config.migration.write_ahead_writers = Some(0);
        let result = validate(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("write_ahead_writers"));
    }

    #[test]
    fn test_zero_max_connections_rejected() {
        let mut config = valid_config();
        config.migration.max_mssql_connections = Some(0);
        let result = validate(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("max_mssql_connections"));
    }

    #[test]
    fn test_valid_nonzero_options_accepted() {
        let mut config = valid_config();
        config.migration.parallel_readers = Some(4);
        config.migration.write_ahead_writers = Some(2);
        config.migration.max_mssql_connections = Some(10);
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_zero_max_partitions_rejected() {
        let mut config = valid_config();
        config.migration.max_partitions = Some(0);
        let result = validate(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("max_partitions"));
    }

    #[test]
    fn test_zero_read_ahead_buffers_rejected() {
        let mut config = valid_config();
        config.migration.read_ahead_buffers = Some(0);
        let result = validate(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("read_ahead_buffers"));
    }

    #[test]
    fn test_zero_max_pg_connections_rejected() {
        let mut config = valid_config();
        config.migration.max_pg_connections = Some(0);
        let result = validate(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("max_pg_connections"));
    }

    #[test]
    fn test_zero_finalizer_concurrency_rejected() {
        let mut config = valid_config();
        config.migration.finalizer_concurrency = Some(0);
        let result = validate(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("finalizer_concurrency"));
    }

    #[test]
    fn test_zero_copy_buffer_rows_rejected() {
        let mut config = valid_config();
        config.migration.copy_buffer_rows = Some(0);
        let result = validate(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("copy_buffer_rows"));
    }

    #[test]
    fn test_zero_upsert_batch_size_rejected() {
        let mut config = valid_config();
        config.migration.upsert_batch_size = Some(0);
        let result = validate(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("upsert_batch_size"));
    }

    #[test]
    fn test_zero_upsert_parallel_tasks_rejected() {
        let mut config = valid_config();
        config.migration.upsert_parallel_tasks = Some(0);
        let result = validate(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("upsert_parallel_tasks"));
    }

    #[test]
    fn test_valid_table_patterns_accepted() {
        let mut config = valid_config();
        config.migration.include_tables = vec![
            "Users".to_string(),
            "dbo.Orders".to_string(),
            "User*".to_string(),
            "???_Table".to_string(),
            "Schema.Table_Name123".to_string(),
        ];
        assert!(validate(&config).is_ok());
    }

    #[test]
    fn test_sql_injection_in_include_tables_rejected() {
        let mut config = valid_config();
        config.migration.include_tables = vec!["Users; DROP TABLE Users--".to_string()];
        let result = validate(&config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("include_tables"));
        assert!(err.contains("invalid character"));
    }

    #[test]
    fn test_sql_injection_in_exclude_tables_rejected() {
        let mut config = valid_config();
        config.migration.exclude_tables = vec!["Users' OR '1'='1".to_string()];
        let result = validate(&config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("exclude_tables"));
        assert!(err.contains("invalid character"));
    }

    #[test]
    fn test_empty_pattern_rejected() {
        let mut config = valid_config();
        config.migration.include_tables = vec!["".to_string()];
        let result = validate(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty pattern"));
    }

    #[test]
    fn test_special_chars_in_pattern_rejected() {
        let mut config = valid_config();
        // Test various SQL injection characters
        for pattern in &["Table;", "Table--", "Table/*", "Table'", "Table\"", "Table\\", "Table()", "Table[]"] {
            config.migration.include_tables = vec![pattern.to_string()];
            let result = validate(&config);
            assert!(result.is_err(), "Pattern '{}' should be rejected", pattern);
        }
    }
}
