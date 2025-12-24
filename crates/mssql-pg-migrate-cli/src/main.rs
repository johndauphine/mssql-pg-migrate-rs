//! mssql-pg-migrate CLI - High-performance MSSQL to PostgreSQL migration.

use anyhow::Result;
use clap::{Parser, Subcommand};
use mssql_pg_migrate::{Config, Orchestrator};
use std::path::PathBuf;
use tokio::sync::watch;
use tracing::{info, Level};
use tracing_subscriber::fmt::format::FmtSpan;

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

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start a new migration
    Run {
        /// Path to state file for resume capability
        #[arg(long)]
        state_file: Option<PathBuf>,

        /// Resume from previous state
        #[arg(long)]
        resume: bool,

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
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Setup logging
    setup_logging(&cli.verbosity, &cli.log_format)?;

    // Load configuration with initial auto-tuning for connection pool sizes.
    // Will be re-tuned after schema extraction with actual row sizes.
    let mut config = Config::load(&cli.config)?.with_auto_tuning();
    info!("Loaded configuration from {:?}", cli.config);

    // Setup cancellation
    let (cancel_tx, cancel_rx) = watch::channel(false);
    ctrlc::set_handler(move || {
        eprintln!("\nInterrupted. Shutting down gracefully...");
        let _ = cancel_tx.send(true);
    })?;

    match cli.command {
        Commands::Run {
            state_file,
            resume,
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

            // Create orchestrator
            let mut orchestrator = Orchestrator::new(config).await?;

            if let Some(path) = state_file {
                orchestrator = orchestrator.with_state_file(path);
            }

            if resume {
                orchestrator = orchestrator.resume()?;
                info!("Resuming from previous state");
            }

            // Run migration
            let result = orchestrator.run(Some(cancel_rx)).await?;

            if cli.output_json {
                println!("{}", result.to_json()?);
            } else {
                println!("\nMigration completed!");
                println!("  Run ID: {}", result.run_id);
                println!("  Duration: {:.2}s", result.duration_seconds);
                println!("  Tables: {}/{}", result.tables_success, result.tables_total);
                println!("  Rows: {}", result.rows_transferred);
                println!(
                    "  Throughput: {} rows/sec",
                    result.rows_per_second
                );
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
    }

    Ok(())
}

fn setup_logging(verbosity: &str, format: &str) -> Result<()> {
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
