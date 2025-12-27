//! Application state for the TUI.
//!
//! Follows the Elm architecture pattern:
//! - Model: App struct containing all application state
//! - Update: handle_event method that transforms state based on events
//! - View: Separate ui module renders state to terminal

use crate::tui::events::AppEvent;
use crate::tui::actions::Action;
use mssql_pg_migrate::{Config, MigrateError, ProgressUpdate, MigrationResult};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// Maximum number of log lines to keep in memory.
const MAX_LOG_LINES: usize = 1000;

/// Maximum number of throughput samples for sparkline.
const MAX_THROUGHPUT_SAMPLES: usize = 60;

/// Migration phase for display.
#[derive(Debug, Clone, PartialEq)]
pub enum MigrationPhase {
    Idle,
    Connecting,
    ExtractingSchema,
    PreparingTarget,
    Transferring,
    Finalizing,
    Completed,
    Failed,
    Cancelled,
}

impl std::fmt::Display for MigrationPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationPhase::Idle => write!(f, "Idle"),
            MigrationPhase::Connecting => write!(f, "Connecting"),
            MigrationPhase::ExtractingSchema => write!(f, "Extracting Schema"),
            MigrationPhase::PreparingTarget => write!(f, "Preparing Target"),
            MigrationPhase::Transferring => write!(f, "Transferring"),
            MigrationPhase::Finalizing => write!(f, "Finalizing"),
            MigrationPhase::Completed => write!(f, "Completed"),
            MigrationPhase::Failed => write!(f, "Failed"),
            MigrationPhase::Cancelled => write!(f, "Cancelled"),
        }
    }
}

/// Progress for a single table.
#[derive(Debug, Clone)]
pub struct TableProgress {
    pub name: String,
    pub rows_total: i64,
    pub rows_transferred: i64,
    pub status: TableStatus,
}

impl TableProgress {
    pub fn percent_complete(&self) -> f64 {
        if self.rows_total == 0 {
            0.0
        } else {
            (self.rows_transferred as f64 / self.rows_total as f64) * 100.0
        }
    }
}

/// Status of a table transfer.
#[derive(Debug, Clone, PartialEq)]
pub enum TableStatus {
    Pending,
    InProgress,
    Completed,
    Failed(String),
}

/// An entry in the transcript (conversation-style output).
#[derive(Debug, Clone)]
pub struct TranscriptEntry {
    pub icon: char,
    pub message: String,
    pub detail: Option<String>,
    pub timestamp: Instant,
}

impl TranscriptEntry {
    pub fn info(message: impl Into<String>) -> Self {
        Self {
            icon: '\u{25B6}', // Black right-pointing triangle
            message: message.into(),
            detail: None,
            timestamp: Instant::now(),
        }
    }

    pub fn success(message: impl Into<String>) -> Self {
        Self {
            icon: '\u{2713}', // Check mark
            message: message.into(),
            detail: None,
            timestamp: Instant::now(),
        }
    }

    pub fn progress(message: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            icon: '\u{23F3}', // Hourglass
            message: message.into(),
            detail: Some(detail.into()),
            timestamp: Instant::now(),
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            icon: '\u{2717}', // X mark
            message: message.into(),
            detail: None,
            timestamp: Instant::now(),
        }
    }
}

/// Summary of loaded configuration.
#[derive(Debug, Clone)]
pub struct ConfigSummary {
    pub source_host: String,
    pub source_database: String,
    pub target_host: String,
    pub target_database: String,
    pub target_mode: String,
    pub workers: usize,
    pub state_file: Option<PathBuf>,
}

impl From<&Config> for ConfigSummary {
    fn from(config: &Config) -> Self {
        Self {
            source_host: config.source.host.clone(),
            source_database: config.source.database.clone(),
            target_host: config.target.host.clone(),
            target_database: config.target.database.clone(),
            target_mode: format!("{:?}", config.migration.target_mode),
            workers: config.migration.workers.unwrap_or(4),
            state_file: None,
        }
    }
}

/// Main application state.
pub struct App {
    /// Current migration phase.
    pub phase: MigrationPhase,

    /// Progress for each table.
    pub tables: Vec<TableProgress>,

    /// Conversation-style transcript entries.
    pub transcript: Vec<TranscriptEntry>,

    /// Log output buffer (ring buffer).
    pub logs: VecDeque<String>,

    /// Configuration summary for display.
    pub config_summary: ConfigSummary,

    /// Throughput history for sparkline (rows/sec samples).
    pub throughput_history: Vec<u64>,

    /// Total rows transferred.
    pub rows_transferred: i64,

    /// Current rows per second.
    pub rows_per_second: i64,

    /// Tables completed / total.
    pub tables_completed: usize,
    pub tables_total: usize,

    /// Elapsed time since migration started.
    pub started_at: Option<Instant>,

    /// Whether command palette is open.
    pub palette_open: bool,

    /// Current palette search query.
    pub palette_query: String,

    /// Selected action index in palette.
    pub selected_action: usize,

    /// Available actions filtered by query.
    pub filtered_actions: Vec<Action>,

    /// Whether help overlay is shown.
    pub show_help: bool,

    /// Log panel scroll offset.
    pub log_scroll: usize,

    /// Whether log panel is expanded.
    pub log_expanded: bool,

    /// Full config for operations.
    config: Config,

    /// Config file path.
    config_path: PathBuf,

    /// Cancellation token for current operation.
    cancel_token: Option<CancellationToken>,

    /// Last migration result.
    pub last_result: Option<MigrationResult>,
}

impl App {
    /// Create a new application with the given config.
    pub fn new(config: Config, config_path: PathBuf) -> Self {
        let config_summary = ConfigSummary::from(&config);

        let mut app = Self {
            phase: MigrationPhase::Idle,
            tables: Vec::new(),
            transcript: Vec::new(),
            logs: VecDeque::with_capacity(MAX_LOG_LINES),
            config_summary,
            throughput_history: Vec::with_capacity(MAX_THROUGHPUT_SAMPLES),
            rows_transferred: 0,
            rows_per_second: 0,
            tables_completed: 0,
            tables_total: 0,
            started_at: None,
            palette_open: false,
            palette_query: String::new(),
            selected_action: 0,
            filtered_actions: Action::all(),
            show_help: false,
            log_scroll: 0,
            log_expanded: false,
            config,
            config_path,
            cancel_token: None,
            last_result: None,
        };

        app.add_transcript(TranscriptEntry::info(format!(
            "Loaded config from {}",
            app.config_path.display()
        )));

        app
    }

    /// Handle an application event. Returns true if the app should quit.
    pub async fn handle_event(&mut self, event: AppEvent) -> Result<bool, MigrateError> {
        match event {
            AppEvent::Quit => {
                if self.phase == MigrationPhase::Transferring {
                    // Confirm quit during migration
                    self.add_transcript(TranscriptEntry::info("Press 'q' again to confirm quit"));
                    // TODO: Add confirmation state
                }
                return Ok(true);
            }

            AppEvent::Tick => {
                // Update throughput history for sparkline
                if self.phase == MigrationPhase::Transferring {
                    self.throughput_history.push(self.rows_per_second as u64);
                    if self.throughput_history.len() > MAX_THROUGHPUT_SAMPLES {
                        self.throughput_history.remove(0);
                    }
                }
            }

            AppEvent::OpenPalette => {
                self.palette_open = true;
                self.palette_query.clear();
                self.selected_action = 0;
                self.filtered_actions = Action::all();
            }

            AppEvent::ClosePalette => {
                self.palette_open = false;
            }

            AppEvent::PaletteInput(c) => {
                self.palette_query.push(c);
                self.filter_actions();
            }

            AppEvent::PaletteBackspace => {
                self.palette_query.pop();
                self.filter_actions();
            }

            AppEvent::PaletteUp => {
                if self.selected_action > 0 {
                    self.selected_action -= 1;
                }
            }

            AppEvent::PaletteDown => {
                if self.selected_action < self.filtered_actions.len().saturating_sub(1) {
                    self.selected_action += 1;
                }
            }

            AppEvent::PaletteSelect => {
                if let Some(action) = self.filtered_actions.get(self.selected_action).cloned() {
                    self.palette_open = false;
                    self.execute_action(action).await?;
                }
            }

            AppEvent::ToggleHelp => {
                self.show_help = !self.show_help;
            }

            AppEvent::ToggleLogs => {
                self.log_expanded = !self.log_expanded;
            }

            AppEvent::ScrollLogsUp => {
                if self.log_scroll > 0 {
                    self.log_scroll -= 1;
                }
            }

            AppEvent::ScrollLogsDown => {
                if self.log_scroll < self.logs.len().saturating_sub(1) {
                    self.log_scroll += 1;
                }
            }

            AppEvent::Progress(update) => {
                self.handle_progress(update);
            }

            AppEvent::Log(line) => {
                self.add_log(line);
            }

            AppEvent::MigrationComplete(result) => {
                self.handle_migration_complete(result);
            }

            AppEvent::Error(msg) => {
                self.add_error(msg);
            }

            AppEvent::Cancel => {
                if let Some(token) = &self.cancel_token {
                    token.cancel();
                    self.add_transcript(TranscriptEntry::info("Cancellation requested..."));
                }
            }
        }

        Ok(false)
    }

    /// Execute an action from the palette.
    async fn execute_action(&mut self, action: Action) -> Result<(), MigrateError> {
        match action {
            Action::RunMigration => {
                self.add_transcript(TranscriptEntry::info("Starting migration..."));
                // TODO: Spawn migration task
            }
            Action::DryRun => {
                self.add_transcript(TranscriptEntry::info("Starting dry run..."));
                // TODO: Spawn dry run task
            }
            Action::Resume => {
                self.add_transcript(TranscriptEntry::info("Resuming migration..."));
                // TODO: Spawn resume task
            }
            Action::Validate => {
                self.add_transcript(TranscriptEntry::info("Starting validation..."));
                // TODO: Spawn validation task
            }
            Action::HealthCheck => {
                self.add_transcript(TranscriptEntry::info("Running health check..."));
                // TODO: Spawn health check task
            }
            Action::SaveLogs => {
                self.add_transcript(TranscriptEntry::info("Saving logs..."));
                // TODO: Save logs to file
            }
            Action::Cancel => {
                if let Some(token) = &self.cancel_token {
                    token.cancel();
                    self.add_transcript(TranscriptEntry::info("Cancellation requested..."));
                }
            }
            Action::Quit => {
                return Ok(()); // Will be handled by event loop
            }
        }
        Ok(())
    }

    /// Filter actions based on the current palette query.
    fn filter_actions(&mut self) {
        if self.palette_query.is_empty() {
            self.filtered_actions = Action::all();
        } else {
            let query = self.palette_query.to_lowercase();
            self.filtered_actions = Action::all()
                .into_iter()
                .filter(|a| a.label().to_lowercase().contains(&query))
                .collect();
        }
        self.selected_action = 0;
    }

    /// Handle a progress update from the orchestrator.
    fn handle_progress(&mut self, update: ProgressUpdate) {
        // Update phase
        self.phase = match update.phase.as_str() {
            "extracting_schema" => MigrationPhase::ExtractingSchema,
            "schema_extracted" => MigrationPhase::ExtractingSchema,
            "preparing_target" => MigrationPhase::PreparingTarget,
            "transferring" => MigrationPhase::Transferring,
            "finalizing" => MigrationPhase::Finalizing,
            _ => self.phase.clone(),
        };

        // Update stats
        self.tables_completed = update.tables_completed;
        self.tables_total = update.tables_total;
        self.rows_transferred = update.rows_transferred;
        self.rows_per_second = update.rows_per_second;

        // Update transcript for phase changes
        if update.phase == "extracting_schema" && self.started_at.is_none() {
            self.started_at = Some(Instant::now());
            self.add_transcript(TranscriptEntry::info("Extracting schema from source..."));
        } else if update.phase == "preparing_target" {
            self.add_transcript(TranscriptEntry::success(format!(
                "Schema extracted ({} tables)",
                update.tables_total
            )));
            self.add_transcript(TranscriptEntry::info("Preparing target database..."));
        } else if update.phase == "transferring" {
            if let Some(table) = &update.current_table {
                // Update or add transcript for current table
                self.add_transcript(TranscriptEntry::progress(
                    format!("Transferring {}", table),
                    format!("{} rows/sec", update.rows_per_second),
                ));
            }
        } else if update.phase == "finalizing" {
            self.add_transcript(TranscriptEntry::info("Finalizing (creating indexes)..."));
        }
    }

    /// Handle migration completion.
    fn handle_migration_complete(&mut self, result: MigrationResult) {
        if result.status == "completed" {
            self.phase = MigrationPhase::Completed;
            self.add_transcript(TranscriptEntry::success(format!(
                "Migration completed: {} rows in {:.1}s ({} rows/sec)",
                result.rows_transferred,
                result.duration_seconds,
                result.rows_per_second
            )));
        } else if result.status == "cancelled" {
            self.phase = MigrationPhase::Cancelled;
            self.add_transcript(TranscriptEntry::info("Migration cancelled"));
        } else {
            self.phase = MigrationPhase::Failed;
            let error_msg = result.error.clone().unwrap_or_else(|| "Unknown error".to_string());
            self.add_transcript(TranscriptEntry::error(format!(
                "Migration failed: {}",
                error_msg
            )));
        }
        self.last_result = Some(result);
        self.cancel_token = None;
    }

    /// Add a transcript entry.
    pub fn add_transcript(&mut self, entry: TranscriptEntry) {
        self.transcript.push(entry);
    }

    /// Add a log line.
    pub fn add_log(&mut self, line: String) {
        if self.logs.len() >= MAX_LOG_LINES {
            self.logs.pop_front();
        }
        self.logs.push_back(line);
        // Auto-scroll to bottom
        self.log_scroll = self.logs.len().saturating_sub(1);
    }

    /// Add an error to transcript.
    pub fn add_error(&mut self, message: String) {
        self.add_transcript(TranscriptEntry::error(message));
    }

    /// Get elapsed time as Duration.
    pub fn elapsed(&self) -> Duration {
        self.started_at.map(|s| s.elapsed()).unwrap_or(Duration::ZERO)
    }

    /// Format elapsed time as HH:MM:SS.
    pub fn elapsed_formatted(&self) -> String {
        let secs = self.elapsed().as_secs();
        let hours = secs / 3600;
        let mins = (secs % 3600) / 60;
        let secs = secs % 60;
        format!("{:02}:{:02}:{:02}", hours, mins, secs)
    }
}
