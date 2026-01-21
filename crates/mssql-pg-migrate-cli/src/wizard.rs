//! Interactive configuration wizard for creating/editing config files.

use dialoguer::{Confirm, Input, Password, Select};
use mssql_pg_migrate::{
    Config, MigrationConfig, Orchestrator, SourceConfig, TargetConfig, TargetMode,
};
use std::path::Path;

/// Result type for wizard operations.
pub type WizardResult<T> = Result<T, WizardError>;

/// Errors that can occur during wizard execution.
#[derive(Debug)]
pub enum WizardError {
    /// User cancelled the wizard.
    Cancelled,
    /// IO error (file read/write).
    Io(std::io::Error),
    /// Config parsing error.
    Config(String),
    /// Validation error.
    Validation(String),
}

impl std::fmt::Display for WizardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled => write!(f, "Configuration cancelled"),
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::Config(msg) => write!(f, "Config error: {}", msg),
            Self::Validation(msg) => write!(f, "Validation error: {}", msg),
        }
    }
}

impl std::error::Error for WizardError {}

impl From<std::io::Error> for WizardError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<dialoguer::Error> for WizardError {
    fn from(e: dialoguer::Error) -> Self {
        Self::Io(std::io::Error::other(e.to_string()))
    }
}

/// Action to take when config file already exists.
#[derive(Debug, Clone, Copy, PartialEq)]
enum ExistingFileAction {
    Edit,
    Overwrite,
    Abort,
}

/// Run the configuration wizard.
pub async fn run_wizard(output: &Path, advanced: bool, force: bool) -> WizardResult<()> {
    println!();
    println!("MSSQL to PostgreSQL Migration - Configuration Wizard");
    println!("=====================================================");
    println!();

    // Check if file exists and determine action
    let existing_config = if output.exists() && !force {
        let action = prompt_existing_file_action(output)?;
        match action {
            ExistingFileAction::Edit => {
                println!("Loading existing configuration...");
                match Config::load(output) {
                    Ok(config) => Some(config),
                    Err(e) => {
                        println!("Warning: Could not parse existing file: {}", e);
                        println!("Starting with fresh configuration.\n");
                        None
                    }
                }
            }
            ExistingFileAction::Overwrite => {
                println!("Starting with fresh configuration.\n");
                None
            }
            ExistingFileAction::Abort => {
                return Err(WizardError::Cancelled);
            }
        }
    } else {
        None
    };

    // Prompt for source configuration
    let source = prompt_source_config(existing_config.as_ref().map(|c| &c.source))?;

    // Prompt for target configuration
    let target = prompt_target_config(existing_config.as_ref().map(|c| &c.target))?;

    // Prompt for migration configuration
    let migration =
        prompt_migration_config(existing_config.as_ref().map(|c| &c.migration), advanced)?;

    // Build the config
    let config = Config {
        source,
        target,
        migration,
    };

    // Validate the config
    if let Err(e) = config.validate() {
        return Err(WizardError::Validation(e.to_string()));
    }

    // Show summary
    print_summary(&config);

    // Offer connection test
    if prompt_connection_test()? {
        test_connections(&config).await?;
    }

    // Confirm save
    if !prompt_save_confirm(output)? {
        return Err(WizardError::Cancelled);
    }

    // Write the config file
    write_config(&config, output)?;

    println!("\nConfiguration saved to {}", output.display());
    println!("Run 'mssql-pg-migrate run' to start the migration.");

    Ok(())
}

fn prompt_existing_file_action(path: &Path) -> WizardResult<ExistingFileAction> {
    println!("File already exists: {}\n", path.display());

    let options = &["Edit existing configuration", "Overwrite with new", "Abort"];
    let selection = Select::new()
        .with_prompt("What would you like to do?")
        .items(options)
        .default(0)
        .interact()?;

    Ok(match selection {
        0 => ExistingFileAction::Edit,
        1 => ExistingFileAction::Overwrite,
        _ => ExistingFileAction::Abort,
    })
}

fn prompt_source_config(existing: Option<&SourceConfig>) -> WizardResult<SourceConfig> {
    println!("Source Database (MSSQL)");
    println!("-----------------------");

    let host: String = Input::new()
        .with_prompt("  Host")
        .default(existing.map(|c| c.host.clone()).unwrap_or_default())
        .interact_text()?;

    let port: u16 = Input::new()
        .with_prompt("  Port")
        .default(existing.map(|c| c.port).unwrap_or(1433))
        .interact_text()?;

    let database: String = Input::new()
        .with_prompt("  Database")
        .default(existing.map(|c| c.database.clone()).unwrap_or_default())
        .interact_text()?;

    let user: String = Input::new()
        .with_prompt("  User")
        .default(existing.map(|c| c.user.clone()).unwrap_or_default())
        .interact_text()?;

    let password = prompt_password("  Password", existing.is_some())?;
    let password = if password.is_empty() {
        existing.map(|e| e.password.clone()).unwrap_or(password)
    } else {
        password
    };

    let schema: String = Input::new()
        .with_prompt("  Schema")
        .default(
            existing
                .map(|c| c.schema.clone())
                .unwrap_or_else(|| "dbo".to_string()),
        )
        .interact_text()?;

    let encrypt = Confirm::new()
        .with_prompt("  Encrypt connection")
        .default(existing.map(|c| c.encrypt).unwrap_or(true))
        .interact()?;

    let trust_server_cert = Confirm::new()
        .with_prompt("  Trust server certificate")
        .default(existing.map(|c| c.trust_server_cert).unwrap_or(false))
        .interact()?;

    println!();

    Ok(SourceConfig {
        r#type: "mssql".to_string(),
        host,
        port,
        database,
        user,
        password,
        schema,
        encrypt,
        trust_server_cert,
        auth: Default::default(), // SQL Server auth by default; Kerberos via config file
    })
}

fn prompt_target_config(existing: Option<&TargetConfig>) -> WizardResult<TargetConfig> {
    println!("Target Database (PostgreSQL)");
    println!("----------------------------");

    let host: String = Input::new()
        .with_prompt("  Host")
        .default(existing.map(|c| c.host.clone()).unwrap_or_default())
        .interact_text()?;

    let port: u16 = Input::new()
        .with_prompt("  Port")
        .default(existing.map(|c| c.port).unwrap_or(5432))
        .interact_text()?;

    let database: String = Input::new()
        .with_prompt("  Database")
        .default(existing.map(|c| c.database.clone()).unwrap_or_default())
        .interact_text()?;

    let user: String = Input::new()
        .with_prompt("  User")
        .default(existing.map(|c| c.user.clone()).unwrap_or_default())
        .interact_text()?;

    let password = prompt_password("  Password", existing.is_some())?;
    let password = if password.is_empty() {
        existing.map(|e| e.password.clone()).unwrap_or(password)
    } else {
        password
    };

    let schema: String = Input::new()
        .with_prompt("  Schema")
        .default(
            existing
                .map(|c| c.schema.clone())
                .unwrap_or_else(|| "public".to_string()),
        )
        .interact_text()?;

    let ssl_modes = &["require", "disable", "verify-ca", "verify-full"];
    let default_idx = existing
        .map(|c| ssl_modes.iter().position(|&m| m == c.ssl_mode).unwrap_or(0))
        .unwrap_or(0);

    let ssl_mode_idx = Select::new()
        .with_prompt("  SSL Mode")
        .items(ssl_modes)
        .default(default_idx)
        .interact()?;

    println!();

    Ok(TargetConfig {
        r#type: "postgres".to_string(),
        host,
        port,
        database,
        user,
        password,
        schema,
        ssl_mode: ssl_modes[ssl_mode_idx].to_string(),
        encrypt: true,            // Default for MSSQL targets (ignored for PostgreSQL)
        trust_server_cert: false, // Default for MSSQL targets (ignored for PostgreSQL)
        auth: Default::default(), // SQL Server auth by default (ignored for PostgreSQL)
    })
}

fn prompt_migration_config(
    existing: Option<&MigrationConfig>,
    advanced: bool,
) -> WizardResult<MigrationConfig> {
    println!("Migration Settings");
    println!("------------------");

    // Target mode selection
    let target_modes = &["drop_recreate", "truncate", "upsert"];
    let default_idx = existing
        .map(|c| match c.target_mode {
            TargetMode::DropRecreate => 0,
            TargetMode::Truncate => 1,
            TargetMode::Upsert => 2,
        })
        .unwrap_or(0);

    let mode_idx = Select::new()
        .with_prompt("  Target mode")
        .items(target_modes)
        .default(default_idx)
        .interact()?;

    let target_mode = match mode_idx {
        0 => TargetMode::DropRecreate,
        1 => TargetMode::Truncate,
        _ => TargetMode::Upsert,
    };

    // Table filters
    let include_str: String = Input::new()
        .with_prompt("  Include tables (comma-separated, blank for all)")
        .default(
            existing
                .map(|c| c.include_tables.join(", "))
                .unwrap_or_default(),
        )
        .allow_empty(true)
        .interact_text()?;

    let include_tables: Vec<String> = include_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let exclude_str: String = Input::new()
        .with_prompt("  Exclude tables (comma-separated, blank for none)")
        .default(
            existing
                .map(|c| c.exclude_tables.join(", "))
                .unwrap_or_default(),
        )
        .allow_empty(true)
        .interact_text()?;

    let exclude_tables: Vec<String> = exclude_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // Constraint creation
    let create_indexes = Confirm::new()
        .with_prompt("  Create indexes after transfer")
        .default(existing.map(|c| c.create_indexes).unwrap_or(false))
        .interact()?;

    let create_foreign_keys = Confirm::new()
        .with_prompt("  Create foreign keys after transfer")
        .default(existing.map(|c| c.create_foreign_keys).unwrap_or(false))
        .interact()?;

    let create_check_constraints = Confirm::new()
        .with_prompt("  Create check constraints after transfer")
        .default(
            existing
                .map(|c| c.create_check_constraints)
                .unwrap_or(false),
        )
        .interact()?;

    // Start with defaults, then set what we've gathered
    let mut config = existing.cloned().unwrap_or_default();
    config.target_mode = target_mode;
    config.include_tables = include_tables;
    config.exclude_tables = exclude_tables;
    config.create_indexes = create_indexes;
    config.create_foreign_keys = create_foreign_keys;
    config.create_check_constraints = create_check_constraints;

    // Advanced settings
    if advanced {
        println!("\n  Advanced Performance Settings");
        println!("  (Leave blank to use auto-tuned values)");

        config.workers = prompt_optional_usize("  Workers", existing.and_then(|c| c.workers))?;
        config.chunk_size =
            prompt_optional_usize("  Chunk size", existing.and_then(|c| c.chunk_size))?;

        let memory_budget: u8 = Input::new()
            .with_prompt("  Memory budget percent (1-100)")
            .default(existing.map(|c| c.memory_budget_percent).unwrap_or(70))
            .interact_text()?;
        if memory_budget == 0 || memory_budget > 100 {
            return Err(WizardError::Validation(
                "Memory budget percent must be between 1 and 100.".to_string(),
            ));
        }
        config.memory_budget_percent = memory_budget;

        config.use_binary_copy = Confirm::new()
            .with_prompt("  Use binary COPY format")
            .default(existing.map(|c| c.use_binary_copy).unwrap_or(true))
            .interact()?;

        config.use_unlogged_tables = Confirm::new()
            .with_prompt("  Use UNLOGGED tables (faster, not crash-safe)")
            .default(existing.map(|c| c.use_unlogged_tables).unwrap_or(false))
            .interact()?;

        config.max_partitions =
            prompt_optional_usize("  Max partitions", existing.and_then(|c| c.max_partitions))?;
        config.parallel_readers = prompt_optional_usize(
            "  Parallel readers",
            existing.and_then(|c| c.parallel_readers),
        )?;
        config.write_ahead_writers = prompt_optional_usize(
            "  Parallel writers",
            existing.and_then(|c| c.write_ahead_writers),
        )?;
        config.read_ahead_buffers = prompt_optional_usize(
            "  Read-ahead buffers",
            existing.and_then(|c| c.read_ahead_buffers),
        )?;
        config.max_mssql_connections = prompt_optional_usize(
            "  Max MSSQL connections",
            existing.and_then(|c| c.max_mssql_connections),
        )?;
        config.max_pg_connections = prompt_optional_usize(
            "  Max PostgreSQL connections",
            existing.and_then(|c| c.max_pg_connections),
        )?;
    }

    println!();

    Ok(config)
}

fn prompt_password(prompt: &str, has_existing: bool) -> WizardResult<String> {
    if has_existing {
        let input: String = Password::new()
            .with_prompt(format!("{} (blank to keep existing)", prompt))
            .allow_empty_password(true)
            .interact()?;
        Ok(input)
    } else {
        let input: String = Password::new().with_prompt(prompt).interact()?;
        Ok(input)
    }
}

fn prompt_optional_usize(prompt: &str, existing: Option<usize>) -> WizardResult<Option<usize>> {
    let default_str = existing
        .map(|v| v.to_string())
        .unwrap_or_else(|| "auto".to_string());

    let input: String = Input::new()
        .with_prompt(prompt)
        .default(default_str)
        .allow_empty(true)
        .interact_text()?;

    let trimmed = input.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("auto") {
        Ok(None)
    } else {
        match trimmed.parse::<usize>() {
            Ok(v) => Ok(Some(v)),
            Err(_) => {
                println!("    Invalid number, using auto-tuned value");
                Ok(None)
            }
        }
    }
}

fn print_summary(config: &Config) {
    println!("Configuration Summary");
    println!("---------------------");
    println!(
        "  Source: {}@{}:{}/{} ({})",
        config.source.user,
        config.source.host,
        config.source.port,
        config.source.database,
        config.source.schema
    );
    println!(
        "  Target: {}@{}:{}/{} ({})",
        config.target.user,
        config.target.host,
        config.target.port,
        config.target.database,
        config.target.schema
    );
    println!("  Mode: {:?}", config.migration.target_mode);

    if !config.migration.include_tables.is_empty() {
        println!("  Include: {:?}", config.migration.include_tables);
    }
    if !config.migration.exclude_tables.is_empty() {
        println!("  Exclude: {:?}", config.migration.exclude_tables);
    }

    let mut features = Vec::new();
    if config.migration.create_indexes {
        features.push("indexes");
    }
    if config.migration.create_foreign_keys {
        features.push("foreign keys");
    }
    if config.migration.create_check_constraints {
        features.push("check constraints");
    }
    if !features.is_empty() {
        println!("  Create: {}", features.join(", "));
    }

    println!();
}

fn prompt_connection_test() -> WizardResult<bool> {
    Ok(Confirm::new()
        .with_prompt("Test database connections?")
        .default(false)
        .interact()?)
}

async fn test_connections(config: &Config) -> WizardResult<()> {
    use std::time::Duration;
    use tokio::time::timeout;

    println!("\nTesting connections...");

    // Use a 30-second timeout to prevent hanging indefinitely
    let timeout_duration = Duration::from_secs(30);

    let orchestrator = match timeout(timeout_duration, Orchestrator::new(config.clone())).await {
        Ok(Ok(orch)) => orch,
        Ok(Err(e)) => {
            println!("  Failed to initialize: {}", e);
            println!();
            return Ok(());
        }
        Err(_) => {
            println!("  Connection timed out after 30 seconds");
            println!();
            return Ok(());
        }
    };

    let result = match timeout(timeout_duration, orchestrator.health_check()).await {
        Ok(r) => r,
        Err(_) => {
            println!("  Health check timed out after 30 seconds");
            println!();
            return Ok(());
        }
    };

    match result {
        Ok(health) => {
            println!(
                "  Source (MSSQL): {} ({}ms)",
                if health.source_connected {
                    "OK"
                } else {
                    "FAILED"
                },
                health.source_latency_ms
            );
            if let Some(ref err) = health.source_error {
                println!("    Error: {}", err);
            }

            println!(
                "  Target (PostgreSQL): {} ({}ms)",
                if health.target_connected {
                    "OK"
                } else {
                    "FAILED"
                },
                health.target_latency_ms
            );
            if let Some(ref err) = health.target_error {
                println!("    Error: {}", err);
            }

            if !health.healthy {
                println!("\n  Warning: One or more connections failed.");
            }
        }
        Err(e) => {
            println!("  Connection test failed: {}", e);
        }
    }

    println!();
    Ok(())
}

fn prompt_save_confirm(path: &Path) -> WizardResult<bool> {
    Ok(Confirm::new()
        .with_prompt(format!("Save to {}?", path.display()))
        .default(true)
        .interact()?)
}

fn write_config(config: &Config, path: &Path) -> WizardResult<()> {
    let header = r#"# MSSQL to PostgreSQL Migration Configuration
# Generated by mssql-pg-migrate init
# Documentation: https://github.com/johndauphine/mssql-pg-migrate-rs

"#;

    let yaml = serde_yaml::to_string(config).map_err(|e| WizardError::Config(e.to_string()))?;

    std::fs::write(path, format!("{}{}", header, yaml))?;

    Ok(())
}
