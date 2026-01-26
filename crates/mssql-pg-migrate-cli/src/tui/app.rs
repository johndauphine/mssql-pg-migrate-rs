//! Application state for the TUI.
//!
//! Follows the Elm architecture pattern:
//! - Model: App struct containing all application state
//! - Update: handle_event method that transforms state based on events
//! - View: Separate ui module renders state to terminal

use crate::tui::actions::Action;
use crate::tui::events::{
    AppEvent, SharedInputMode, INPUT_MODE_COMMAND, INPUT_MODE_FILE, INPUT_MODE_NORMAL,
    INPUT_MODE_WIZARD,
};
use crate::tui::wizard::WizardState;
use mssql_pg_migrate::{Config, MigrateError, MigrationResult, Orchestrator, ProgressUpdate};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Maximum number of log lines to keep in memory.
const MAX_LOG_LINES: usize = 1000;

/// Maximum number of throughput samples for sparkline.
const MAX_THROUGHPUT_SAMPLES: usize = 60;

/// Maximum number of suggestions to show.
const MAX_SUGGESTIONS: usize = 10;

/// Maximum command history size.
const MAX_HISTORY: usize = 100;

/// Input mode for the TUI.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum InputMode {
    /// Normal mode - keyboard shortcuts active.
    #[default]
    Normal,
    /// Command mode - typing a slash command.
    Command,
    /// File input mode - typing a file path after @.
    FileInput,
    /// Wizard mode - step-by-step configuration.
    Wizard,
}

/// A slash command with metadata for auto-completion.
#[derive(Debug, Clone)]
pub struct SlashCommand {
    pub name: &'static str,
    pub description: &'static str,
    pub shortcut: Option<&'static str>,
}

impl SlashCommand {
    /// Get all available slash commands.
    pub fn all() -> Vec<Self> {
        vec![
            SlashCommand {
                name: "/run",
                description: "Start migration",
                shortcut: None,
            },
            SlashCommand {
                name: "/resume",
                description: "Resume interrupted migration",
                shortcut: None,
            },
            SlashCommand {
                name: "/dry-run",
                description: "Run without making changes",
                shortcut: None,
            },
            SlashCommand {
                name: "/validate",
                description: "Validate row counts",
                shortcut: None,
            },
            SlashCommand {
                name: "/health",
                description: "Run health check",
                shortcut: None,
            },
            SlashCommand {
                name: "/wizard",
                description: "Configuration wizard [@path]",
                shortcut: None,
            },
            SlashCommand {
                name: "/config",
                description: "Show config (reloads from disk) [@path]",
                shortcut: None,
            },
            SlashCommand {
                name: "/logs",
                description: "Save logs to file",
                shortcut: None,
            },
            SlashCommand {
                name: "/clear",
                description: "Clear transcript",
                shortcut: None,
            },
            SlashCommand {
                name: "/help",
                description: "Show help",
                shortcut: Some("?"),
            },
            SlashCommand {
                name: "/quit",
                description: "Exit application",
                shortcut: Some("q"),
            },
        ]
    }
}

/// An auto-completion suggestion.
#[derive(Debug, Clone)]
pub struct Suggestion {
    /// Display text shown in the dropdown.
    pub display: String,
    /// Value to insert when selected.
    pub value: String,
    /// Whether this is a directory (for file completion).
    pub is_dir: bool,
}

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
    pub config_path: PathBuf,
    pub source_type: String,
    pub source_host: String,
    pub source_database: String,
    pub target_type: String,
    pub target_host: String,
    pub target_database: String,
    pub target_mode: String,
    pub workers: usize,
    pub state_file: Option<PathBuf>,
}

impl ConfigSummary {
    /// Create a ConfigSummary from a Config and its file path.
    pub fn from_config(config: &Config, path: &Path) -> Self {
        Self {
            config_path: path.to_path_buf(),
            source_type: config.source.r#type.clone(),
            source_host: config.source.host.clone(),
            source_database: config.source.database.clone(),
            target_type: config.target.r#type.clone(),
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

    /// Total rows to transfer.
    pub total_rows: i64,

    /// Current rows per second.
    pub rows_per_second: i64,

    /// Tables completed / total.
    pub tables_completed: usize,
    pub tables_total: usize,

    /// Elapsed time since migration started.
    pub started_at: Option<Instant>,

    /// When the migration completed (to freeze the timer).
    pub completed_at: Option<Instant>,

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

    /// Transcript scroll offset.
    pub transcript_scroll: usize,

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

    // --- Command input state ---
    /// Current input mode (local copy for rendering).
    pub input_mode: InputMode,

    /// Shared input mode state (synced with EventHandler).
    shared_input_mode: SharedInputMode,

    /// Command input buffer.
    pub command_input: String,

    /// Auto-completion suggestions.
    pub suggestions: Vec<Suggestion>,

    /// Selected suggestion index.
    pub selected_suggestion: usize,

    /// Command history.
    pub command_history: Vec<String>,

    /// Current position in command history (-1 = current input).
    pub history_index: isize,

    /// Saved input when navigating history.
    pub saved_input: String,

    // --- Wizard state ---
    /// Active wizard state (None when not in wizard mode).
    pub wizard: Option<WizardState>,
}

impl App {
    /// Create a new application with the given config.
    pub fn new(
        config: Config,
        config_path: PathBuf,
        event_tx: mpsc::Sender<AppEvent>,
        palette_open: Arc<AtomicBool>,
        shared_input_mode: SharedInputMode,
    ) -> Self {
        let config_summary = ConfigSummary::from_config(&config, &config_path);

        let mut app = Self {
            phase: MigrationPhase::Idle,
            tables: Vec::new(),
            transcript: Vec::new(),
            logs: VecDeque::with_capacity(MAX_LOG_LINES),
            config_summary,
            throughput_history: Vec::with_capacity(MAX_THROUGHPUT_SAMPLES),
            rows_transferred: 0,
            total_rows: 0,
            rows_per_second: 0,
            tables_completed: 0,
            tables_total: 0,
            started_at: None,
            completed_at: None,
            palette_open,
            palette_query: String::new(),
            selected_action: 0,
            filtered_actions: Action::all(),
            show_help: false,
            log_scroll: 0,
            transcript_scroll: 0,
            log_expanded: false,
            config,
            config_path,
            cancel_token: None,
            last_result: None,
            event_tx,
            // Command input state
            input_mode: InputMode::Normal,
            shared_input_mode,
            command_input: String::new(),
            suggestions: Vec::new(),
            selected_suggestion: 0,
            command_history: Vec::with_capacity(MAX_HISTORY),
            history_index: -1,
            saved_input: String::new(),
            // Wizard state
            wizard: None,
        };

        app.add_transcript(TranscriptEntry::info(format!(
            "Loaded config from {}",
            app.config_path.display()
        )));

        app
    }

    /// Start the configuration wizard immediately.
    /// Used when config file is missing or invalid on startup.
    pub fn start_wizard(&mut self, output_path: PathBuf) {
        let path_display = output_path.display().to_string();

        // Clear the "Loaded config" message since we're using a default config
        self.transcript.clear();

        // Create wizard state with current (default) config
        let mut wizard_state = WizardState::with_config(output_path, &self.config);
        wizard_state.init_selection_for_step();
        self.wizard = Some(wizard_state);
        self.input_mode = InputMode::Wizard;
        self.shared_input_mode
            .store(INPUT_MODE_WIZARD, Ordering::Relaxed);

        self.add_transcript(TranscriptEntry::info(
            "Config file not found. Starting configuration wizard...".to_string(),
        ));
        self.add_transcript(TranscriptEntry::info(format!(
            "Configuration will be saved to: {}",
            path_display
        )));
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
                // Close palette, help, and any other overlays - return to home screen
                self.palette_open.store(false, Ordering::Relaxed);
                self.show_help = false;
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

            AppEvent::ScrollTranscriptUp => {
                if self.transcript_scroll > 0 {
                    self.transcript_scroll -= 1;
                }
            }

            AppEvent::ScrollTranscriptDown => {
                if self.transcript_scroll < self.transcript.len().saturating_sub(1) {
                    self.transcript_scroll += 1;
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

            // --- Command input events ---
            AppEvent::CommandInput(c) => {
                // Handle starting command mode with '/'
                if c == '/' && self.input_mode == InputMode::Normal && self.command_input.is_empty()
                {
                    self.input_mode = InputMode::Command;
                    self.shared_input_mode
                        .store(INPUT_MODE_COMMAND, Ordering::Relaxed);
                }

                self.command_input.push(c);
                self.update_suggestions();
                self.history_index = -1;
            }

            AppEvent::CommandBackspace => {
                self.command_input.pop();

                // Exit command mode if input is empty
                if self.command_input.is_empty() {
                    self.input_mode = InputMode::Normal;
                    self.shared_input_mode
                        .store(INPUT_MODE_NORMAL, Ordering::Relaxed);
                    self.suggestions.clear();
                } else {
                    self.update_suggestions();
                }
            }

            AppEvent::CommandUp => {
                if !self.suggestions.is_empty() {
                    // Navigate suggestions
                    if self.selected_suggestion > 0 {
                        self.selected_suggestion -= 1;
                    }
                } else {
                    // Navigate command history
                    self.navigate_history_up();
                }
            }

            AppEvent::CommandDown => {
                if !self.suggestions.is_empty() {
                    // Navigate suggestions
                    if self.selected_suggestion < self.suggestions.len().saturating_sub(1) {
                        self.selected_suggestion += 1;
                    }
                } else {
                    // Navigate command history
                    self.navigate_history_down();
                }
            }

            AppEvent::AcceptSuggestion => {
                if let Some(suggestion) = self.suggestions.get(self.selected_suggestion).cloned() {
                    // Check if we're in file completion mode (has @)
                    if let Some(at_idx) = self.command_input.rfind('@') {
                        // Preserve the command prefix before @, replace only the path portion
                        let prefix = &self.command_input[..=at_idx]; // Include the @
                        self.command_input = format!("{}{}", prefix, suggestion.value);
                    } else {
                        // Regular command suggestion - replace entirely
                        self.command_input = suggestion.value;
                    }
                    if suggestion.is_dir {
                        // Keep file mode open for directory
                        self.command_input.push('/');
                    }
                    self.suggestions.clear();
                    self.selected_suggestion = 0;
                    self.update_suggestions();
                }
            }

            AppEvent::ExecuteCommand => {
                let command = self.command_input.trim().to_string();
                if !command.is_empty() {
                    // Add to history
                    if self.command_history.last() != Some(&command) {
                        self.command_history.push(command.clone());
                        if self.command_history.len() > MAX_HISTORY {
                            self.command_history.remove(0);
                        }
                    }

                    // Execute the command
                    self.execute_slash_command(&command).await?;
                }

                // Reset command input state
                self.command_input.clear();
                self.suggestions.clear();
                self.selected_suggestion = 0;
                self.history_index = -1;

                // Only reset to Normal mode if we didn't start the wizard
                if self.wizard.is_none() {
                    self.input_mode = InputMode::Normal;
                    self.shared_input_mode
                        .store(INPUT_MODE_NORMAL, Ordering::Relaxed);
                }
            }

            AppEvent::ExitCommandMode => {
                self.command_input.clear();
                self.suggestions.clear();
                self.selected_suggestion = 0;
                self.history_index = -1;
                self.input_mode = InputMode::Normal;
                self.shared_input_mode
                    .store(INPUT_MODE_NORMAL, Ordering::Relaxed);
            }

            AppEvent::ClearTranscript => {
                self.transcript.clear();
            }

            // --- Wizard events ---
            AppEvent::WizardInput(c) => {
                if let Some(ref mut wizard) = self.wizard {
                    // Ignore text input for enum selection steps
                    if !wizard.step.is_enum_selection() {
                        wizard.input.push(c);
                    }
                }
            }

            AppEvent::WizardBackspace => {
                if let Some(ref mut wizard) = self.wizard {
                    if wizard.step.is_enum_selection() {
                        // On enum steps, backspace goes back to previous step
                        wizard.step = wizard.step.prev();
                        // Skip SourceTrustCert for non-MSSQL sources
                        wizard.skip_trust_cert_if_needed_backward();
                        wizard.input.clear();
                        wizard.error = None;
                        wizard.selected_option = 0;
                        wizard.init_selection_for_step();
                    } else if wizard.input.is_empty() {
                        // On text steps with empty input, go back to previous step
                        wizard.step = wizard.step.prev();
                        // Skip SourceTrustCert for non-MSSQL sources
                        wizard.skip_trust_cert_if_needed_backward();
                        wizard.input.clear();
                        wizard.error = None;
                        wizard.selected_option = 0;
                        wizard.init_selection_for_step();
                    } else {
                        // Otherwise delete character
                        wizard.input.pop();
                    }
                }
            }

            AppEvent::WizardSubmit => {
                let (done, was_saved, output_path) = if let Some(ref mut wizard) = self.wizard {
                    match wizard.process_input() {
                        Ok(done) => (
                            done,
                            wizard.was_saved,
                            wizard.output_path.display().to_string(),
                        ),
                        Err(e) => {
                            wizard.error = Some(e);
                            (false, false, String::new())
                        }
                    }
                } else {
                    (false, false, String::new())
                };

                if done {
                    self.input_mode = InputMode::Normal;
                    self.shared_input_mode
                        .store(INPUT_MODE_NORMAL, Ordering::Relaxed);
                    if was_saved {
                        self.add_transcript(TranscriptEntry::success(format!(
                            "Configuration saved to {}",
                            output_path
                        )));
                        // Reload the newly saved config
                        if self.try_load_config(Some(&output_path)) {
                            self.add_transcript(TranscriptEntry::success(
                                "Configuration loaded. Ready to run migration.".to_string(),
                            ));
                        }
                    } else {
                        self.add_transcript(TranscriptEntry::info("Wizard cancelled".to_string()));
                    }
                    self.wizard = None;
                }
            }

            AppEvent::WizardBack => {
                if let Some(ref mut wizard) = self.wizard {
                    if wizard.step.is_enum_selection() {
                        // Navigate up in enum selection
                        wizard.select_prev();
                    } else {
                        // Go back to previous step
                        wizard.step = wizard.step.prev();
                        wizard.input.clear();
                        wizard.error = None;
                        wizard.selected_option = 0;
                        wizard.init_selection_for_step();
                    }
                }
            }

            AppEvent::WizardDown => {
                if let Some(ref mut wizard) = self.wizard {
                    if wizard.step.is_enum_selection() {
                        // Navigate down in enum selection
                        wizard.select_next();
                    }
                    // Down arrow does nothing in text input mode
                }
            }

            AppEvent::WizardCancel => {
                self.wizard = None;
                self.input_mode = InputMode::Normal;
                self.shared_input_mode
                    .store(INPUT_MODE_NORMAL, Ordering::Relaxed);
                self.add_transcript(TranscriptEntry::info("Wizard cancelled"));
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
                // Reload config before running
                if !self.try_load_config(None) {
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
                // Reload config before running
                if !self.try_load_config(None) {
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
                // Reload config before running
                if !self.try_load_config(None) {
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
                    let _ = event_tx
                        .send(AppEvent::MigrationComplete(migration_result))
                        .await;
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
            orchestrator = orchestrator.resume().await?;
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
                Ok(orchestrator) => match orchestrator.health_check().await {
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
                        let _ = event_tx
                            .send(AppEvent::Error(format!("Health check error: {}", e)))
                            .await;
                    }
                },
                Err(e) => {
                    let _ = event_tx
                        .send(AppEvent::Error(format!("Failed to connect: {}", e)))
                        .await;
                }
            }
        });
    }

    /// Save logs to a file.
    fn save_logs(&mut self) {
        let filename = format!(
            "migration-logs-{}.txt",
            chrono::Utc::now().format("%Y%m%d-%H%M%S")
        );
        match std::fs::write(
            &filename,
            self.logs.iter().cloned().collect::<Vec<_>>().join("\n"),
        ) {
            Ok(()) => {
                self.add_transcript(TranscriptEntry::success(format!(
                    "Logs saved to {}",
                    filename
                )));
            }
            Err(e) => {
                self.add_transcript(TranscriptEntry::error(format!(
                    "Failed to save logs: {}",
                    e
                )));
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
        // Log progress for debugging
        tracing::debug!(
            "Progress: phase={}, tables={}/{}, rows={}, rps={}",
            update.phase,
            update.tables_completed,
            update.tables_total,
            update.rows_transferred,
            update.rows_per_second
        );

        // Don't update phase if already in terminal state (handles race condition
        // where progress events arrive after MigrationComplete)
        if matches!(
            self.phase,
            MigrationPhase::Completed | MigrationPhase::Failed | MigrationPhase::Cancelled
        ) {
            return;
        }

        // Capture old phase before updating (for transcript logic)
        let old_phase = self.phase.clone();

        // Update phase
        self.phase = match update.phase.as_str() {
            "extracting_schema" => MigrationPhase::ExtractingSchema,
            "schema_extracted" => MigrationPhase::ExtractingSchema,
            "preparing_target" => MigrationPhase::PreparingTarget,
            "transferring" => MigrationPhase::Transferring,
            "finalizing" => MigrationPhase::Finalizing,
            _ => self.phase.clone(),
        };

        // Update stats (always update, even if values are the same)
        self.tables_completed = update.tables_completed;
        self.tables_total = update.tables_total;
        self.rows_transferred = update.rows_transferred;
        self.total_rows = update.total_rows;
        self.rows_per_second = update.rows_per_second;

        // Debug: log when tables_completed changes
        tracing::trace!(
            "Stats update: tables={}/{}, rows={}, rps={}",
            self.tables_completed,
            self.tables_total,
            self.rows_transferred,
            self.rows_per_second
        );

        // Update transcript for phase changes

        if update.phase == "extracting_schema" && self.started_at.is_none() {
            self.started_at = Some(Instant::now());
            self.completed_at = None; // Reset for new migration
            self.add_transcript(TranscriptEntry::info("Extracting schema from source..."));
        } else if update.phase == "preparing_target" && old_phase != MigrationPhase::PreparingTarget
        {
            self.add_transcript(TranscriptEntry::success(format!(
                "Schema extracted ({} tables)",
                update.tables_total
            )));
            self.add_transcript(TranscriptEntry::info("Preparing target database..."));
        } else if update.phase == "transferring" {
            // Show "Target prepared" message once when entering transfer phase
            if old_phase == MigrationPhase::PreparingTarget {
                self.add_transcript(TranscriptEntry::success("Target database prepared"));
                self.add_transcript(TranscriptEntry::info("Starting data transfer..."));
            }
            // Show table status when current_table is provided
            if let Some(table) = &update.current_table {
                match update.table_status.as_deref() {
                    Some("started") => {
                        self.add_transcript(TranscriptEntry::progress(
                            format!("Transferring {}", table),
                            format!("{} rows", update.rows_transferred),
                        ));
                    }
                    Some("completed") => {
                        self.add_transcript(TranscriptEntry::success(format!(
                            "Completed {} ({}/{} tables)",
                            table, update.tables_completed, update.tables_total
                        )));
                    }
                    _ => {}
                }
            }
        } else if update.phase == "finalizing" && old_phase != MigrationPhase::Finalizing {
            let msg = if self.config.migration.create_indexes {
                "Finalizing (creating indexes)..."
            } else {
                "Finalizing..."
            };
            self.add_transcript(TranscriptEntry::info(msg));
        }
    }

    /// Handle migration completion.
    fn handle_migration_complete(&mut self, result: MigrationResult) {
        // Freeze the timer
        self.completed_at = Some(Instant::now());

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
                    result.rows_transferred, result.duration_seconds, result.rows_per_second
                )));
            }
            "dry_run" => {
                self.phase = MigrationPhase::Completed;
                self.add_transcript(TranscriptEntry::success(format!(
                    "Dry run completed: {} tables, {} rows would be transferred",
                    result.tables_total, result.rows_transferred
                )));
            }
            "cancelled" => {
                self.phase = MigrationPhase::Cancelled;
                self.add_transcript(TranscriptEntry::info("Migration cancelled"));
            }
            _ => {
                self.phase = MigrationPhase::Failed;
                let error_msg = result
                    .error
                    .clone()
                    .unwrap_or_else(|| "Unknown error".to_string());
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

    /// Try to load a config file if a path is provided.
    /// If no path is provided, reloads the current config file.
    /// Returns true if successful.
    /// Returns false if loading failed (error is added to transcript).
    fn try_load_config(&mut self, file_path: Option<&str>) -> bool {
        // Use provided path or fall back to current config_path
        let path = if let Some(p) = file_path.filter(|p| !p.is_empty()) {
            p.to_string()
        } else {
            // Auto-reload current config file
            self.config_path
                .to_str()
                .unwrap_or("config.yaml")
                .to_string()
        };

        match Config::load(&path) {
            Ok(new_config) => {
                self.config_path = PathBuf::from(&path);
                self.config_summary = ConfigSummary::from_config(&new_config, &self.config_path);
                self.config = new_config;
                self.add_transcript(TranscriptEntry::info(format!("Loaded config: {}", path)));
                true
            }
            Err(e) => {
                self.add_transcript(TranscriptEntry::error(format!(
                    "Failed to load config: {}",
                    e
                )));
                false
            }
        }
    }

    /// Get elapsed time as Duration.
    pub fn elapsed(&self) -> Duration {
        match (self.started_at, self.completed_at) {
            (Some(start), Some(end)) => end.duration_since(start),
            (Some(start), None) => start.elapsed(),
            _ => Duration::ZERO,
        }
    }

    /// Format elapsed time as HH:MM:SS.
    pub fn elapsed_formatted(&self) -> String {
        let secs = self.elapsed().as_secs();
        let hours = secs / 3600;
        let mins = (secs % 3600) / 60;
        let secs = secs % 60;
        format!("{:02}:{:02}:{:02}", hours, mins, secs)
    }

    // --- Command input helpers ---

    /// Update auto-completion suggestions based on current input.
    fn update_suggestions(&mut self) {
        self.suggestions.clear();
        self.selected_suggestion = 0;

        let input = &self.command_input;

        // Check for file completion (@ prefix)
        if let Some(at_idx) = input.rfind('@') {
            // Only trigger if @ is after a space or at start
            if at_idx == 0 || input.chars().nth(at_idx.saturating_sub(1)) == Some(' ') {
                let path_prefix = &input[at_idx + 1..];
                self.suggestions = self.glob_files(path_prefix);
                self.input_mode = InputMode::FileInput;
                self.shared_input_mode
                    .store(INPUT_MODE_FILE, Ordering::Relaxed);
                return;
            }
        }

        // Command completion (starts with /)
        if input.starts_with('/') {
            let query = input.to_lowercase();
            for cmd in SlashCommand::all() {
                if cmd.name.to_lowercase().starts_with(&query) {
                    self.suggestions.push(Suggestion {
                        display: format!("{:<12} {}", cmd.name, cmd.description),
                        value: cmd.name.to_string(),
                        is_dir: false,
                    });
                }
            }

            // Limit suggestions
            if self.suggestions.len() > MAX_SUGGESTIONS {
                self.suggestions.truncate(MAX_SUGGESTIONS);
            }
        }
    }

    /// Glob files for auto-completion.
    fn glob_files(&self, prefix: &str) -> Vec<Suggestion> {
        use std::path::Path;

        let mut suggestions = Vec::new();

        // Determine the directory and pattern
        let (dir, file_prefix) = if prefix.contains('/') || prefix.contains('\\') {
            let path = Path::new(prefix);
            (
                path.parent().map(|p| p.to_path_buf()).unwrap_or_default(),
                path.file_name()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_default(),
            )
        } else {
            (
                std::env::current_dir().unwrap_or_default(),
                prefix.to_string(),
            )
        };

        // Read directory
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.filter_map(|e| e.ok()) {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.to_lowercase().starts_with(&file_prefix.to_lowercase()) {
                    let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
                    let full_path = if prefix.contains('/') || prefix.contains('\\') {
                        let base = Path::new(prefix)
                            .parent()
                            .map(|p| p.to_path_buf())
                            .unwrap_or_default();
                        base.join(&name).to_string_lossy().to_string()
                    } else {
                        name.clone()
                    };

                    // Filter for yaml/yml files or directories
                    if is_dir || name.ends_with(".yaml") || name.ends_with(".yml") {
                        suggestions.push(Suggestion {
                            display: if is_dir { format!("{}/", name) } else { name },
                            value: full_path,
                            is_dir,
                        });
                    }
                }
            }
        }

        // Sort and limit
        suggestions.sort_by(|a, b| a.display.cmp(&b.display));
        if suggestions.len() > MAX_SUGGESTIONS {
            suggestions.truncate(MAX_SUGGESTIONS);
        }

        suggestions
    }

    /// Navigate command history up.
    fn navigate_history_up(&mut self) {
        if self.command_history.is_empty() {
            return;
        }

        // Save current input when starting to navigate
        if self.history_index == -1 {
            self.saved_input = self.command_input.clone();
        }

        // Move up in history
        let max_index = self.command_history.len() as isize - 1;
        if self.history_index < max_index {
            self.history_index += 1;
            let idx = self.command_history.len() - 1 - self.history_index as usize;
            self.command_input = self.command_history[idx].clone();
        }
    }

    /// Navigate command history down.
    fn navigate_history_down(&mut self) {
        if self.history_index > 0 {
            self.history_index -= 1;
            let idx = self.command_history.len() - 1 - self.history_index as usize;
            self.command_input = self.command_history[idx].clone();
        } else if self.history_index == 0 {
            // Restore saved input
            self.history_index = -1;
            self.command_input = self.saved_input.clone();
        }
    }

    /// Execute a slash command.
    async fn execute_slash_command(&mut self, command: &str) -> Result<(), MigrateError> {
        // Parse command and arguments
        // Handle both "/cmd @path" and "/cmd@path" formats
        let parts: Vec<&str> = command.split_whitespace().collect();
        let (cmd, args) = match parts.split_first() {
            Some((cmd, args)) => (*cmd, args),
            None => return Ok(()),
        };

        // Check if @ is attached to the command (e.g., "/wizard@path")
        let (cmd, inline_path) = if let Some(at_pos) = cmd.find('@') {
            (&cmd[..at_pos], Some(&cmd[at_pos + 1..]))
        } else {
            (cmd, None)
        };

        // Extract file path from args (look for @path) or use inline path
        let file_path =
            inline_path.or_else(|| args.iter().find(|a| a.starts_with('@')).map(|a| &a[1..]));

        match cmd {
            "/run" => {
                if !self.can_start_migration() {
                    self.add_transcript(TranscriptEntry::error("Migration already in progress"));
                    return Ok(());
                }
                // Load config after validation passes
                if !self.try_load_config(file_path) {
                    return Ok(());
                }
                self.add_transcript(TranscriptEntry::info("Starting migration..."));
                self.spawn_migration(false, false).await;
            }
            "/resume" => {
                if !self.can_start_migration() {
                    self.add_transcript(TranscriptEntry::error("Migration already in progress"));
                    return Ok(());
                }
                // Load config after validation passes
                if !self.try_load_config(file_path) {
                    return Ok(());
                }
                self.add_transcript(TranscriptEntry::info("Resuming migration..."));
                self.spawn_migration(false, true).await;
            }
            "/dry-run" => {
                if !self.can_start_migration() {
                    self.add_transcript(TranscriptEntry::error("Migration already in progress"));
                    return Ok(());
                }
                // Load config after validation passes
                if !self.try_load_config(file_path) {
                    return Ok(());
                }
                self.add_transcript(TranscriptEntry::info("Starting dry run..."));
                self.spawn_migration(true, false).await;
            }
            "/validate" => {
                // Load config if provided (validation doesn't require can_start_migration check)
                if !self.try_load_config(file_path) {
                    return Ok(());
                }
                self.add_transcript(TranscriptEntry::info("Validation not yet implemented"));
            }
            "/health" => {
                // Load config if provided (health check doesn't require can_start_migration check)
                if !self.try_load_config(file_path) {
                    return Ok(());
                }
                self.spawn_health_check().await;
            }
            "/wizard" => {
                // Start the configuration wizard
                // Use @path if specified, otherwise default to config.yaml
                // Strip any leading @ that might have been included
                let output_path = file_path
                    .filter(|p| !p.is_empty())
                    .map(|p| p.strip_prefix('@').unwrap_or(p))
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from("config.yaml"));
                let path_display = output_path.display().to_string();

                // If the file exists, load it for editing; otherwise use current config
                let config_for_wizard = if output_path.exists() {
                    match Config::load(&output_path) {
                        Ok(loaded_config) => {
                            self.add_transcript(TranscriptEntry::info(format!(
                                "Loading existing config: {}",
                                path_display
                            )));
                            loaded_config
                        }
                        Err(e) => {
                            self.add_transcript(TranscriptEntry::error(format!(
                                "Failed to load {}: {}. Using current config.",
                                path_display, e
                            )));
                            self.config.clone()
                        }
                    }
                } else {
                    self.config.clone()
                };

                let mut wizard_state = WizardState::with_config(output_path, &config_for_wizard);
                wizard_state.init_selection_for_step();
                self.wizard = Some(wizard_state);
                self.input_mode = InputMode::Wizard;
                self.shared_input_mode
                    .store(INPUT_MODE_WIZARD, Ordering::Relaxed);
                self.add_transcript(TranscriptEntry::info(format!(
                    "Starting configuration wizard (output: {})...",
                    path_display
                )));
            }
            "/config" => {
                // Reload the config file and display it
                if self.try_load_config(file_path) {
                    // Convert config to YAML for display
                    match serde_yaml::to_string(&self.config) {
                        Ok(yaml) => {
                            self.add_transcript(TranscriptEntry::info("Current configuration:"));
                            for line in yaml.lines() {
                                self.add_transcript(TranscriptEntry::info(format!("  {}", line)));
                            }
                        }
                        Err(e) => {
                            self.add_transcript(TranscriptEntry::error(format!(
                                "Failed to serialize config: {}",
                                e
                            )));
                        }
                    }
                }
            }
            "/logs" => {
                self.save_logs();
            }
            "/clear" => {
                self.transcript.clear();
            }
            "/help" => {
                self.show_help = true;
            }
            "/quit" | "/q" => {
                // This will be caught by the event loop
                self.add_transcript(TranscriptEntry::info("Use 'q' key or Ctrl+C to quit"));
            }
            _ => {
                self.add_transcript(TranscriptEntry::error(format!("Unknown command: {}", cmd)));
            }
        }

        Ok(())
    }
}
