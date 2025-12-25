//! mssql-pg-migrate CLI - High-performance MSSQL to PostgreSQL migration.

use clap::{Parser, Subcommand};
use mssql_pg_migrate::{Config, MigrateError, Orchestrator};
use std::path::PathBuf;
use std::process::ExitCode;
use tokio::sync::watch;
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

    /// Test database connections
    HealthCheck,
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

    // Setup logging
    setup_logging(&cli.verbosity, &cli.log_format)
        .map_err(|e| MigrateError::Config(e.to_string()))?;

    // Load configuration with initial auto-tuning for connection pool sizes.
    // Will be re-tuned after schema extraction with actual row sizes.
    let mut config = Config::load(&cli.config)?.with_auto_tuning();
    info!("Loaded configuration from {:?}", cli.config);

    // Setup signal handling for graceful shutdown (SIGINT and SIGTERM)
    let cancel_rx = setup_signal_handler(cli.shutdown_timeout).await?;

    match cli.command {
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
            let result = orchestrator.run(Some(cancel_rx), dry_run).await?;

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
            let result = orchestrator.run(Some(cancel_rx), false).await?;

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
/// Returns a watch channel receiver that will be set to true when a signal is received.
#[cfg(unix)]
async fn setup_signal_handler(shutdown_timeout: u64) -> Result<watch::Receiver<bool>, MigrateError> {
    let (cancel_tx, cancel_rx) = watch::channel(false);

    // Clone sender for each signal handler
    let tx_int = cancel_tx.clone();
    let tx_term = cancel_tx;

    // SIGINT handler (Ctrl-C)
    tokio::spawn(async move {
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to setup SIGINT handler");
        sigint.recv().await;
        eprintln!("\nReceived SIGINT. Shutting down gracefully (timeout: {}s)...", shutdown_timeout);
        let _ = tx_int.send(true);
    });

    // SIGTERM handler (Kubernetes/Airflow)
    tokio::spawn(async move {
        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to setup SIGTERM handler");
        sigterm.recv().await;
        eprintln!("\nReceived SIGTERM. Shutting down gracefully (timeout: {}s)...", shutdown_timeout);
        let _ = tx_term.send(true);
    });

    Ok(cancel_rx)
}

/// Setup signal handler for Windows (only SIGINT/Ctrl-C)
#[cfg(not(unix))]
async fn setup_signal_handler(_shutdown_timeout: u64) -> Result<watch::Receiver<bool>, MigrateError> {
    let (cancel_tx, cancel_rx) = watch::channel(false);

    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to setup Ctrl-C handler");
        eprintln!("\nReceived Ctrl-C. Shutting down gracefully...");
        let _ = cancel_tx.send(true);
    });

    Ok(cancel_rx)
}
