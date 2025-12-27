//! mssql-pg-migrate CLI - High-performance MSSQL to PostgreSQL migration.

mod wizard;

#[cfg(feature = "tui")]
mod tui;

use clap::{Parser, Subcommand};
use mssql_pg_migrate::{Config, MigrateError, Orchestrator};
use std::path::PathBuf;
use std::process::ExitCode;
use tokio_util::sync::CancellationToken;
use tracing::{info, Level};
use tracing_subscriber::fmt::format::FmtSpan;

#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};

#[derive(Parser)]
#[command(name = "mssql-pg-migrate")]
#[command(about = "High-performance MSSQL to PostgreSQL migration")]
#[command(version)]
struct Cli {
    /// Path to YAML configuration file
    #[arg(short, long, default_value = "config.yaml")]
    config: PathBuf,

    /// Path to state file for resume capability
    #[arg(long)]
    state_file: Option<PathBuf>,

    /// Output JSON result to stdout
    #[arg(long)]
    output_json: bool,

    /// Log format: text or json
    #[arg(long, default_value = "text")]
    log_format: String,

    /// Log verbosity: debug, info, warn, error
    #[arg(long, default_value = "info")]
    verbosity: String,

    /// Timeout in seconds for graceful shutdown (default: 60)
    #[arg(long, default_value = "60")]
    shutdown_timeout: u64,

    /// Print progress updates as JSON lines to stderr
    #[arg(long)]
    progress: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start a new migration
    Run {
        /// Override source schema
        #[arg(long)]
        source_schema: Option<String>,

        /// Override target schema
        #[arg(long)]
        target_schema: Option<String>,

        /// Override number of workers
        #[arg(long)]
        workers: Option<usize>,

        /// Dry run: validate and show plan without transferring data
        #[arg(long)]
        dry_run: bool,
    },

    /// Resume a previously interrupted migration
    Resume {
        /// Override source schema
        #[arg(long)]
        source_schema: Option<String>,

        /// Override target schema
        #[arg(long)]
        target_schema: Option<String>,

        /// Override number of workers
        #[arg(long)]
        workers: Option<usize>,
    },

    /// Validate row counts between source and target
    Validate,

    /// Verify data consistency using multi-tier batch hashing
    Verify {
        /// Automatically sync mismatched rows
        #[arg(long)]
        sync: bool,

        /// Tier 1 (coarse) batch size in rows (default: 1000000)
        #[arg(long, default_value = "1000000")]
        tier1_size: i64,

        /// Tier 2 (fine) batch size in rows (default: 10000)
        #[arg(long, default_value = "10000")]
        tier2_size: i64,
    },

    /// Test database connections
    HealthCheck,

    /// Create or edit a configuration file interactively
    Init {
        /// Output path for configuration file [default: config.yaml]
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Show advanced performance tuning options
        #[arg(long)]
        advanced: bool,

        /// Force overwrite existing file without confirmation
        #[arg(long, short)]
        force: bool,
    },

    /// Launch interactive TUI mode
    #[cfg(feature = "tui")]
    Tui,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{}", e.format_detailed());
            ExitCode::from(e.exit_code())
        }
    }
}

async fn run() -> Result<(), MigrateError> {
    let cli = Cli::parse();

    // Handle init command separately (doesn't need existing config)
    if let Commands::Init { output, advanced, force } = cli.command {
        // No logging setup for wizard - keeps terminal clean for interactive prompts
        let output_path = output.unwrap_or_else(|| PathBuf::from("config.yaml"));
        wizard::run_wizard(&output_path, advanced, force)
            .await
            .map_err(|e| MigrateError::Config(e.to_string()))?;
        return Ok(());
    }

    // Handle TUI command separately (manages its own terminal)
    #[cfg(feature = "tui")]
    if let Commands::Tui = cli.command {
        return tui::run(&cli.config).await;
    }

    // Setup logging
    setup_logging(&cli.verbosity, &cli.log_format)
        .map_err(|e| MigrateError::Config(e.to_string()))?;

    // Load configuration with initial auto-tuning for connection pool sizes.
    // Will be re-tuned after schema extraction with actual row sizes.
    let mut config = Config::load(&cli.config)?.with_auto_tuning();
    info!("Loaded configuration from {:?}", cli.config);

    // Setup signal handling for graceful shutdown (SIGINT and SIGTERM)
    let cancel_token = setup_signal_handler(cli.shutdown_timeout).await?;

    match cli.command {
        Commands::Init { .. } => unreachable!(), // Handled above
        #[cfg(feature = "tui")]
        Commands::Tui => unreachable!(), // Handled above
        Commands::Run {
            source_schema,
            target_schema,
            workers,
            dry_run,
        } => {
            // Apply overrides
            if let Some(schema) = source_schema {
                config.source.schema = schema;
            }
            if let Some(schema) = target_schema {
                config.target.schema = schema;
            }
            if let Some(w) = workers {
                config.migration.workers = Some(w);
            }

            // Create orchestrator
            let mut orchestrator = Orchestrator::new(config).await?;

            // Apply global state_file if provided
            if let Some(ref path) = cli.state_file {
                orchestrator = orchestrator.with_state_file(path.clone());
            }

            // Enable progress reporting if requested
            if cli.progress {
                orchestrator = orchestrator.with_progress(true);
            }

            // Run fresh migration (or dry-run)
            let result = orchestrator.run(cancel_token, dry_run).await?;

            if cli.output_json {
                println!("{}", result.to_json()?);
            } else {
                let status_msg = if dry_run { "Dry run completed!" } else { "Migration completed!" };
                println!("\n{}", status_msg);
                println!("  Run ID: {}", result.run_id);
                println!("  Duration: {:.2}s", result.duration_seconds);
                println!(
                    "  Tables: {}/{}",
                    result.tables_success, result.tables_total
                );
                println!("  Rows: {}", result.rows_transferred);
                println!("  Throughput: {} rows/sec", result.rows_per_second);
                if !result.failed_tables.is_empty() {
                    println!("  Failed tables: {:?}", result.failed_tables);
                }
            }
        }

        Commands::Resume {
            source_schema,
            target_schema,
            workers,
        } => {
            // State file is required for resume
            let state_file = cli.state_file.ok_or_else(|| {
                MigrateError::Config("--state-file is required for resume".to_string())
            })?;

            // Verify state file exists
            if !state_file.exists() {
                return Err(MigrateError::Config(format!(
                    "State file not found: {:?}",
                    state_file
                )));
            }

            // Apply overrides
            if let Some(schema) = source_schema {
                config.source.schema = schema;
            }
            if let Some(schema) = target_schema {
                config.target.schema = schema;
            }
            if let Some(w) = workers {
                config.migration.workers = Some(w);
            }

            // Create orchestrator with resume
            let mut orchestrator = Orchestrator::new(config)
                .await?
                .with_state_file(state_file)
                .resume()?;

            // Enable progress reporting if requested
            if cli.progress {
                orchestrator = orchestrator.with_progress(true);
            }

            info!("Resuming from previous state");

            // Run migration (not dry-run for resume)
            let result = orchestrator.run(cancel_token, false).await?;

            if cli.output_json {
                println!("{}", result.to_json()?);
            } else {
                println!("\nMigration resumed and completed!");
                println!("  Run ID: {}", result.run_id);
                println!("  Duration: {:.2}s", result.duration_seconds);
                println!(
                    "  Tables: {}/{}",
                    result.tables_success, result.tables_total
                );
                println!("  Rows: {}", result.rows_transferred);
                println!("  Throughput: {} rows/sec", result.rows_per_second);
                if !result.failed_tables.is_empty() {
                    println!("  Failed tables: {:?}", result.failed_tables);
                }
            }
        }

        Commands::Validate => {
            let orchestrator = Orchestrator::new(config).await?;
            orchestrator.validate().await?;
            println!("Validation completed successfully");
        }

        Commands::Verify {
            sync,
            tier1_size,
            tier2_size,
        } => {
            use mssql_pg_migrate::{BatchVerifyConfig, VerifyEngine};

            let orchestrator = Orchestrator::new(config.clone()).await?;

            // Extract schema from source
            let source_schema = &config.source.schema;
            let target_schema = &config.target.schema;
            let tables = orchestrator.extract_schema().await?;

            // Create verify config
            let verify_config = BatchVerifyConfig {
                tier1_batch_size: tier1_size,
                tier2_batch_size: tier2_size,
                parallel_verify_ranges: 4,
            };

            // Get connection pools from orchestrator
            let source = orchestrator.source_pool();
            let target = orchestrator.target_pool();

            let engine = VerifyEngine::new(
                source,
                target,
                verify_config,
                config.migration.row_hash_column.clone(),
            );

            println!("Starting multi-tier verification...\n");

            let mut total_result = mssql_pg_migrate::VerifyResult::new();
            let start = std::time::Instant::now();

            for table in &tables {
                match engine
                    .verify_table(table, source_schema, target_schema, sync)
                    .await
                {
                    Ok(result) => {
                        let status = if result.is_in_sync() {
                            "✓ In sync"
                        } else {
                            "✗ Differs"
                        };
                        println!(
                            "  {} {} (insert: {}, update: {}, delete: {})",
                            status,
                            result.table_name,
                            result.rows_to_insert,
                            result.rows_to_update,
                            result.rows_to_delete
                        );
                        total_result.add_table(result);
                    }
                    Err(e) => {
                        println!("  ✗ Error verifying {}: {}", table.name, e);
                    }
                }
            }

            total_result.duration_ms = start.elapsed().as_millis() as u64;

            println!("\nVerification Summary:");
            println!("  Tables checked: {}", total_result.tables_checked);
            println!("  Tables in sync: {}", total_result.tables_in_sync);
            println!(
                "  Tables with differences: {}",
                total_result.tables_with_differences
            );
            println!("  Total rows to insert: {}", total_result.total_rows_to_insert);
            println!("  Total rows to update: {}", total_result.total_rows_to_update);
            println!("  Total rows to delete: {}", total_result.total_rows_to_delete);
            println!("  Duration: {:.2}s", total_result.duration_ms as f64 / 1000.0);

            if cli.output_json {
                println!("\n{}", serde_json::to_string_pretty(&total_result)?);
            }
        }

        Commands::HealthCheck => {
            let orchestrator = Orchestrator::new(config).await?;
            let result = orchestrator.health_check().await?;

            if cli.output_json {
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else {
                println!("Health Check Results:");
                println!(
                    "  Source (MSSQL): {} ({}ms)",
                    if result.source_connected { "OK" } else { "FAILED" },
                    result.source_latency_ms
                );
                if let Some(ref err) = result.source_error {
                    println!("    Error: {}", err);
                }
                println!(
                    "  Target (PostgreSQL): {} ({}ms)",
                    if result.target_connected { "OK" } else { "FAILED" },
                    result.target_latency_ms
                );
                if let Some(ref err) = result.target_error {
                    println!("    Error: {}", err);
                }
                println!(
                    "\n  Overall: {}",
                    if result.healthy { "HEALTHY" } else { "UNHEALTHY" }
                );
            }

            if !result.healthy {
                return Err(MigrateError::Config("Health check failed".to_string()));
            }
        }
    }

    Ok(())
}

fn setup_logging(verbosity: &str, format: &str) -> Result<(), String> {
    let level = match verbosity.to_lowercase().as_str() {
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber = tracing_subscriber::fmt()
        .with_max_level(level)
        .with_span_events(FmtSpan::CLOSE)
        .with_target(false);

    if format == "json" {
        subscriber.json().init();
    } else {
        subscriber.init();
    }

    Ok(())
}

/// Setup signal handlers for graceful shutdown.
/// Handles both SIGINT (Ctrl-C) and SIGTERM (Kubernetes/Airflow shutdown).
/// Returns a CancellationToken that will be cancelled when a signal is received.
#[cfg(unix)]
async fn setup_signal_handler(shutdown_timeout: u64) -> Result<CancellationToken, MigrateError> {
    let cancel_token = CancellationToken::new();

    // Clone token for each signal handler
    let token_int = cancel_token.clone();
    let token_term = cancel_token.clone();

    // SIGINT handler (Ctrl-C)
    tokio::spawn(async move {
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to setup SIGINT handler");
        sigint.recv().await;
        eprintln!("\nReceived SIGINT. Shutting down gracefully (timeout: {}s)...", shutdown_timeout);
        token_int.cancel();
    });

    // SIGTERM handler (Kubernetes/Airflow)
    tokio::spawn(async move {
        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to setup SIGTERM handler");
        sigterm.recv().await;
        eprintln!("\nReceived SIGTERM. Shutting down gracefully (timeout: {}s)...", shutdown_timeout);
        token_term.cancel();
    });

    Ok(cancel_token)
}

/// Setup signal handler for Windows (only SIGINT/Ctrl-C)
#[cfg(not(unix))]
async fn setup_signal_handler(_shutdown_timeout: u64) -> Result<CancellationToken, MigrateError> {
    let cancel_token = CancellationToken::new();
    let token = cancel_token.clone();

    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to setup Ctrl-C handler");
        eprintln!("\nReceived Ctrl-C. Shutting down gracefully...");
        token.cancel();
    });

    Ok(cancel_token)
}
