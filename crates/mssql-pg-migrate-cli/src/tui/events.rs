//! Event handling for the TUI.
//!
//! Handles keyboard input and tick events, converting them to AppEvents.

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use mssql_pg_migrate::{MigrationResult, ProgressUpdate};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Application events that drive state changes.
#[derive(Debug, Clone)]
pub enum AppEvent {
    /// Quit the application.
    Quit,

    /// Periodic tick for UI updates.
    Tick,

    /// Open the command palette.
    OpenPalette,

    /// Close the command palette.
    ClosePalette,

    /// Character input in palette.
    PaletteInput(char),

    /// Backspace in palette.
    PaletteBackspace,

    /// Move up in palette.
    PaletteUp,

    /// Move down in palette.
    PaletteDown,

    /// Select current palette item.
    PaletteSelect,

    /// Toggle help overlay.
    ToggleHelp,

    /// Toggle log panel expansion.
    ToggleLogs,

    /// Scroll logs up.
    ScrollLogsUp,

    /// Scroll logs down.
    ScrollLogsDown,

    /// Scroll transcript up.
    ScrollTranscriptUp,

    /// Scroll transcript down.
    ScrollTranscriptDown,

    /// Progress update from orchestrator.
    Progress(ProgressUpdate),

    /// Log line from tracing.
    Log(String),

    /// Migration completed.
    MigrationComplete(MigrationResult),

    /// Error occurred.
    Error(String),

    /// Success message for transcript.
    Success(String),

    /// Cancel current operation.
    Cancel,

    // --- Command input events ---
    /// Character input in command mode.
    CommandInput(char),

    /// Backspace in command mode.
    CommandBackspace,

    /// Move up in suggestions or history.
    CommandUp,

    /// Move down in suggestions.
    CommandDown,

    /// Accept selected suggestion (Tab).
    AcceptSuggestion,

    /// Execute command (Enter).
    ExecuteCommand,

    /// Exit command mode (Esc).
    ExitCommandMode,

    /// Clear the transcript.
    ClearTranscript,

    // --- Wizard events ---
    /// Character input in wizard mode.
    WizardInput(char),

    /// Backspace in wizard mode.
    WizardBackspace,

    /// Submit wizard input (Enter).
    WizardSubmit,

    /// Go back to previous step.
    WizardBack,

    /// Cancel wizard.
    WizardCancel,
}

use std::sync::atomic::AtomicU8;

/// Shared input mode state (mirrors InputMode enum).
/// 0 = Normal, 1 = Command, 2 = FileInput
pub type SharedInputMode = Arc<AtomicU8>;

/// Input mode constants for atomic operations.
pub const INPUT_MODE_NORMAL: u8 = 0;
pub const INPUT_MODE_COMMAND: u8 = 1;
pub const INPUT_MODE_FILE: u8 = 2;
pub const INPUT_MODE_WIZARD: u8 = 3;

/// Event handler that polls for keyboard and tick events.
pub struct EventHandler {
    tx: mpsc::Sender<AppEvent>,
    tick_rate: Duration,
    palette_open: Arc<AtomicBool>,
    input_mode: SharedInputMode,
}

impl EventHandler {
    /// Create a new event handler.
    pub fn new(
        tx: mpsc::Sender<AppEvent>,
        palette_open: Arc<AtomicBool>,
        input_mode: SharedInputMode,
    ) -> Self {
        Self {
            tx,
            tick_rate: Duration::from_millis(250),
            palette_open,
            input_mode,
        }
    }

    /// Run the event handler loop.
    pub async fn run(self) {
        loop {
            // Poll for crossterm events with timeout
            if event::poll(self.tick_rate).unwrap_or(false) {
                if let Ok(Event::Key(key)) = event::read() {
                    if let Some(app_event) = self.handle_key(key) {
                        if self.tx.send(app_event).await.is_err() {
                            break;
                        }
                    }
                }
            } else {
                // No event, send tick
                if self.tx.send(AppEvent::Tick).await.is_err() {
                    break;
                }
            }
        }
    }

    /// Convert a key event to an app event.
    fn handle_key(&self, key: KeyEvent) -> Option<AppEvent> {
        let is_palette_open = self.palette_open.load(Ordering::Relaxed);
        let input_mode = self.input_mode.load(Ordering::Relaxed);

        // Handle Ctrl combinations first (global)
        if key.modifiers.contains(KeyModifiers::CONTROL) {
            return match key.code {
                KeyCode::Char('c') => Some(AppEvent::Cancel),
                KeyCode::Char('p') => Some(AppEvent::OpenPalette),
                _ => None,
            };
        }

        // Palette mode takes precedence
        if is_palette_open {
            return match key.code {
                KeyCode::Esc => Some(AppEvent::ClosePalette),
                KeyCode::Enter => Some(AppEvent::PaletteSelect),
                KeyCode::Up => Some(AppEvent::PaletteUp),
                KeyCode::Down => Some(AppEvent::PaletteDown),
                KeyCode::Backspace => Some(AppEvent::PaletteBackspace),
                KeyCode::Char(c) => Some(AppEvent::PaletteInput(c)),
                _ => None,
            };
        }

        // Command input mode
        if input_mode == INPUT_MODE_COMMAND || input_mode == INPUT_MODE_FILE {
            return match key.code {
                KeyCode::Esc => Some(AppEvent::ExitCommandMode),
                KeyCode::Enter => Some(AppEvent::ExecuteCommand),
                KeyCode::Tab => Some(AppEvent::AcceptSuggestion),
                KeyCode::Up => Some(AppEvent::CommandUp),
                KeyCode::Down => Some(AppEvent::CommandDown),
                KeyCode::Backspace => Some(AppEvent::CommandBackspace),
                KeyCode::Char(c) => Some(AppEvent::CommandInput(c)),
                _ => None,
            };
        }

        // Wizard mode
        if input_mode == INPUT_MODE_WIZARD {
            return match key.code {
                KeyCode::Esc => Some(AppEvent::WizardCancel),
                KeyCode::Enter => Some(AppEvent::WizardSubmit),
                KeyCode::Backspace => Some(AppEvent::WizardBackspace),
                KeyCode::Up => Some(AppEvent::WizardBack),
                KeyCode::Char(c) => Some(AppEvent::WizardInput(c)),
                _ => None,
            };
        }

        // Normal mode key handling
        match key.code {
            KeyCode::Char('q') => Some(AppEvent::Quit),
            KeyCode::Char('/') => Some(AppEvent::CommandInput('/')), // Start command mode
            KeyCode::Char(':') => Some(AppEvent::OpenPalette),
            KeyCode::Char('?') => Some(AppEvent::ToggleHelp),
            KeyCode::Char('l') => Some(AppEvent::ToggleLogs),
            // j/k scroll transcript, J/K scroll logs
            KeyCode::Char('j') => Some(AppEvent::ScrollTranscriptDown),
            KeyCode::Char('k') => Some(AppEvent::ScrollTranscriptUp),
            KeyCode::Char('J') => Some(AppEvent::ScrollLogsDown),
            KeyCode::Char('K') => Some(AppEvent::ScrollLogsUp),
            KeyCode::Down => Some(AppEvent::ScrollTranscriptDown),
            KeyCode::Up => Some(AppEvent::ScrollTranscriptUp),
            KeyCode::Esc => Some(AppEvent::ClosePalette),
            _ => None,
        }
    }
}
