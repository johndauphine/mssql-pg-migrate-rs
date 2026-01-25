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
use tracing_subscriber::EnvFilter;

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
    /// Start a migration (idempotent: auto-resumes from database state if it exists)
    ///
    /// State is automatically stored in the target PostgreSQL database (_mssql_pg_migrate schema):
    /// - First run: Creates new migration state in database
    /// - Subsequent runs: Automatically resumes from database state (enables incremental sync with date watermarks)
    ///
    /// Use 'resume' command for explicit crash recovery.
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

    /// Resume from database state (explicit crash recovery - requires existing state)
    ///
    /// Unlike 'run', this command errors if no state exists in the database.
    /// Use this for crash recovery scenarios where you know migration was interrupted.
    ///
    /// State is stored in the target PostgreSQL database (_mssql_pg_migrate schema).
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
    // Install the ring crypto provider for rustls before any TLS operations.
    // This is required when multiple crates (tokio-postgres-rustls, mysql_async)
    // bring in rustls with different default features.
    if let Err(e) = rustls::crypto::ring::default_provider().install_default() {
        // Only fail if it's not already installed - Err means a different provider is set
        eprintln!(
            "Warning: Could not install ring crypto provider: {:?}. TLS may not work correctly.",
            e
        );
    }

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
    if let Commands::Init {
        output,
        advanced,
        force,
    } = cli.command
    {
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

            // Create orchestrator (automatically initializes database state schema)
            let mut orchestrator = Orchestrator::new(config).await?;

            // Auto-resume from database state if it exists
            // This makes 'run' idempotent and enables incremental sync workflows:
            // - First run: Creates new state in database, saves last_sync_timestamp
            // - Subsequent runs: Loads state from database, uses last_sync_timestamp for date filters
            orchestrator = orchestrator.resume().await?;

            // Enable progress reporting if requested
            if cli.progress {
                orchestrator = orchestrator.with_progress(true);
            }

            // Run migration (or dry-run)
            let result = orchestrator.run(cancel_token, dry_run).await;

            // Always close connections, regardless of success or failure
            orchestrator.close().await;

            // Now propagate any error
            let result = result?;

            if cli.output_json {
                println!("{}", result.to_json()?);
            } else {
                let status_msg = if dry_run {
                    "Dry run completed!"
                } else {
                    "Migration completed!"
                };
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

            // Create orchestrator and load state from database
            let mut orchestrator = Orchestrator::new(config).await?.resume().await?;

            // Enable progress reporting if requested
            if cli.progress {
                orchestrator = orchestrator.with_progress(true);
            }

            info!("Resuming from database state");

            // Run migration (not dry-run for resume)
            let result = orchestrator.run(cancel_token, false).await;

            // Always close connections, regardless of success or failure
            orchestrator.close().await;

            // Now propagate any error
            let result = result?;

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
            let result = orchestrator.validate().await;
            orchestrator.close().await;
            result?;
            println!("Validation completed successfully");
        }

        Commands::HealthCheck => {
            let orchestrator = Orchestrator::new(config).await?;
            let health_result = orchestrator.health_check().await;
            orchestrator.close().await;
            let result = health_result?;

            if cli.output_json {
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else {
                println!("Health Check Results:");
                println!(
                    "  Source (MSSQL): {} ({}ms)",
                    if result.source_connected {
                        "OK"
                    } else {
                        "FAILED"
                    },
                    result.source_latency_ms
                );
                if let Some(ref err) = result.source_error {
                    println!("    Error: {}", err);
                }
                println!(
                    "  Target (PostgreSQL): {} ({}ms)",
                    if result.target_connected {
                        "OK"
                    } else {
                        "FAILED"
                    },
                    result.target_latency_ms
                );
                if let Some(ref err) = result.target_error {
                    println!("    Error: {}", err);
                }
                println!(
                    "\n  Overall: {}",
                    if result.healthy {
                        "HEALTHY"
                    } else {
                        "UNHEALTHY"
                    }
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

    // Build filter that suppresses sqlx query logging (which prints huge SQL strings with ? placeholders)
    // sqlx logs queries at DEBUG/TRACE level, which floods terminals and makes debugging impossible
    let level_str = match level {
        Level::DEBUG => "debug",
        Level::INFO => "info",
        Level::WARN => "warn",
        Level::ERROR => "error",
        Level::TRACE => "trace",
    };
    // Set default level, but suppress all sqlx logging to error-only to avoid SQL query spam
    // This filter takes precedence over RUST_LOG for sqlx targets
    let filter_str = format!(
        "{},sqlx=error,sqlx::query=off,sqlx_core=error,sqlx_mysql=error",
        level_str
    );
    let filter = EnvFilter::builder()
        .with_default_directive(level.into())
        .parse_lossy(&filter_str);

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
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
        eprintln!(
            "\nReceived SIGINT. Shutting down gracefully (timeout: {}s)...",
            shutdown_timeout
        );
        token_int.cancel();
    });

    // SIGTERM handler (Kubernetes/Airflow)
    tokio::spawn(async move {
        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to setup SIGTERM handler");
        sigterm.recv().await;
        eprintln!(
            "\nReceived SIGTERM. Shutting down gracefully (timeout: {}s)...",
            shutdown_timeout
        );
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
