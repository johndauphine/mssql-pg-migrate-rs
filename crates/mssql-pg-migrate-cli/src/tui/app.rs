//! Application state for the TUI.
//!
//! Follows the Elm architecture pattern:
//! - Model: App struct containing all application state
//! - Update: handle_event method that transforms state based on events
//! - View: Separate ui module renders state to terminal

use crate::tui::events::AppEvent;
use crate::tui::actions::Action;
use mssql_pg_migrate::{Config, MigrateError, Orchestrator, ProgressUpdate, MigrationResult};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
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

    /// Whether command palette is open (shared with event handler).
    pub palette_open: Arc<AtomicBool>,

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

    /// Event sender for spawning background tasks.
    event_tx: mpsc::Sender<AppEvent>,
}

impl App {
    /// Create a new application with the given config.
    pub fn new(
        config: Config,
        config_path: PathBuf,
        event_tx: mpsc::Sender<AppEvent>,
        palette_open: Arc<AtomicBool>,
    ) -> Self {
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
            palette_open,
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
            event_tx,
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
                self.palette_open.store(true, Ordering::Relaxed);
                self.palette_query.clear();
                self.selected_action = 0;
                self.filtered_actions = Action::all();
            }

            AppEvent::ClosePalette => {
                self.palette_open.store(false, Ordering::Relaxed);
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
                    self.palette_open.store(false, Ordering::Relaxed);
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
                // Reset phase so user can try again
                self.phase = MigrationPhase::Idle;
                self.cancel_token = None;
            }

            AppEvent::Success(msg) => {
                self.add_transcript(TranscriptEntry::success(msg));
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
                if !self.can_start_migration() {
                    self.add_transcript(TranscriptEntry::error("Migration already in progress"));
                    return Ok(());
                }
                self.add_transcript(TranscriptEntry::info("Starting migration..."));
                self.spawn_migration(false, false).await;
            }
            Action::DryRun => {
                if !self.can_start_migration() {
                    self.add_transcript(TranscriptEntry::error("Migration already in progress"));
                    return Ok(());
                }
                self.add_transcript(TranscriptEntry::info("Starting dry run..."));
                self.spawn_migration(true, false).await;
            }
            Action::Resume => {
                if !self.can_start_migration() {
                    self.add_transcript(TranscriptEntry::error("Migration already in progress"));
                    return Ok(());
                }
                self.add_transcript(TranscriptEntry::info("Resuming migration..."));
                self.spawn_migration(false, true).await;
            }
            Action::Validate => {
                self.add_transcript(TranscriptEntry::info("Validation not yet implemented"));
            }
            Action::HealthCheck => {
                self.spawn_health_check().await;
            }
            Action::SaveLogs => {
                self.save_logs();
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

    /// Spawn a migration task in the background.
    async fn spawn_migration(&mut self, dry_run: bool, resume: bool) {
        // Create cancellation token
        let cancel = CancellationToken::new();
        self.cancel_token = Some(cancel.clone());
        self.phase = MigrationPhase::Connecting;

        // Clone what we need for the spawned task
        let config = self.config.clone();
        let event_tx = self.event_tx.clone();

        // Create progress channel
        let (progress_tx, mut progress_rx) = mpsc::channel::<ProgressUpdate>(100);

        // Spawn progress forwarder
        let progress_event_tx = event_tx.clone();
        tokio::spawn(async move {
            while let Some(update) = progress_rx.recv().await {
                let _ = progress_event_tx.send(AppEvent::Progress(update)).await;
            }
        });

        // Spawn migration task
        tokio::spawn(async move {
            let result = Self::run_migration(config, progress_tx, cancel, dry_run, resume).await;
            match result {
                Ok(migration_result) => {
                    let _ = event_tx.send(AppEvent::MigrationComplete(migration_result)).await;
                }
                Err(e) => {
                    let _ = event_tx.send(AppEvent::Error(e.to_string())).await;
                }
            }
        });
    }

    /// Run the actual migration (called in background task).
    async fn run_migration(
        config: Config,
        progress_tx: mpsc::Sender<ProgressUpdate>,
        cancel: CancellationToken,
        dry_run: bool,
        resume: bool,
    ) -> Result<MigrationResult, MigrateError> {
        let mut orchestrator = Orchestrator::new(config)
            .await?
            .with_progress_channel(progress_tx);

        if resume {
            orchestrator = orchestrator.resume()?;
        }

        orchestrator.run(cancel, dry_run).await
    }

    /// Spawn a health check task.
    async fn spawn_health_check(&mut self) {
        self.add_transcript(TranscriptEntry::info("Running health check..."));

        let config = self.config.clone();
        let event_tx = self.event_tx.clone();

        tokio::spawn(async move {
            match Orchestrator::new(config).await {
                Ok(orchestrator) => {
                    match orchestrator.health_check().await {
                        Ok(result) => {
                            if result.healthy {
                                let msg = format!(
                                    "Health check passed (source: {}ms, target: {}ms)",
                                    result.source_latency_ms, result.target_latency_ms
                                );
                                let _ = event_tx.send(AppEvent::Success(msg)).await;
                            } else {
                                let msg = format!(
                                    "Health check failed: source={}, target={}",
                                    result.source_error.unwrap_or_else(|| "ok".to_string()),
                                    result.target_error.unwrap_or_else(|| "ok".to_string())
                                );
                                let _ = event_tx.send(AppEvent::Error(msg)).await;
                            }
                        }
                        Err(e) => {
                            let _ = event_tx.send(AppEvent::Error(format!("Health check error: {}", e))).await;
                        }
                    }
                }
                Err(e) => {
                    let _ = event_tx.send(AppEvent::Error(format!("Failed to connect: {}", e))).await;
                }
            }
        });
    }

    /// Save logs to a file.
    fn save_logs(&mut self) {
        let filename = format!("migration-logs-{}.txt", chrono::Utc::now().format("%Y%m%d-%H%M%S"));
        match std::fs::write(&filename, self.logs.iter().cloned().collect::<Vec<_>>().join("\n")) {
            Ok(()) => {
                self.add_transcript(TranscriptEntry::success(format!("Logs saved to {}", filename)));
            }
            Err(e) => {
                self.add_transcript(TranscriptEntry::error(format!("Failed to save logs: {}", e)));
            }
        }
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
        // Don't update phase if already in terminal state (handles race condition
        // where progress events arrive after MigrationComplete)
        if matches!(
            self.phase,
            MigrationPhase::Completed | MigrationPhase::Failed | MigrationPhase::Cancelled
        ) {
            return;
        }

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
        // Update final stats from result
        self.tables_completed = result.tables_success;
        self.tables_total = result.tables_total;
        self.rows_transferred = result.rows_transferred;
        self.rows_per_second = result.rows_per_second;

        match result.status.as_str() {
            "completed" => {
                self.phase = MigrationPhase::Completed;
                self.add_transcript(TranscriptEntry::success(format!(
                    "Migration completed: {} rows in {:.1}s ({} rows/sec)",
                    result.rows_transferred,
                    result.duration_seconds,
                    result.rows_per_second
                )));
            }
            "dry_run" => {
                self.phase = MigrationPhase::Completed;
                self.add_transcript(TranscriptEntry::success(format!(
                    "Dry run completed: {} tables, {} rows would be transferred",
                    result.tables_total,
                    result.rows_transferred
                )));
            }
            "cancelled" => {
                self.phase = MigrationPhase::Cancelled;
                self.add_transcript(TranscriptEntry::info("Migration cancelled"));
            }
            "failed" | _ => {
                self.phase = MigrationPhase::Failed;
                let error_msg = result.error.clone().unwrap_or_else(|| "Unknown error".to_string());
                self.add_transcript(TranscriptEntry::error(format!(
                    "Migration failed: {}",
                    error_msg
                )));
            }
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

    /// Check if a new migration can be started.
    fn can_start_migration(&self) -> bool {
        matches!(
            self.phase,
            MigrationPhase::Idle
                | MigrationPhase::Completed
                | MigrationPhase::Failed
                | MigrationPhase::Cancelled
        )
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
