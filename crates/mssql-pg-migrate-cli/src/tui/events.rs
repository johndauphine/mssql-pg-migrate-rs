//! Event handling for the TUI.
//!
//! Handles keyboard input and tick events, converting them to AppEvents.

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use mssql_pg_migrate::{MigrationResult, ProgressUpdate};
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

    /// Progress update from orchestrator.
    Progress(ProgressUpdate),

    /// Log line from tracing.
    Log(String),

    /// Migration completed.
    MigrationComplete(MigrationResult),

    /// Error occurred.
    Error(String),

    /// Cancel current operation.
    Cancel,
}

/// Event handler that polls for keyboard and tick events.
pub struct EventHandler {
    tx: mpsc::Sender<AppEvent>,
    tick_rate: Duration,
}

impl EventHandler {
    /// Create a new event handler.
    pub fn new(tx: mpsc::Sender<AppEvent>) -> Self {
        Self {
            tx,
            tick_rate: Duration::from_millis(250),
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
        // Handle Ctrl combinations first
        if key.modifiers.contains(KeyModifiers::CONTROL) {
            return match key.code {
                KeyCode::Char('c') => Some(AppEvent::Cancel),
                KeyCode::Char('p') => Some(AppEvent::OpenPalette),
                _ => None,
            };
        }

        // Regular keys
        match key.code {
            KeyCode::Char('q') => Some(AppEvent::Quit),
            KeyCode::Char(':') => Some(AppEvent::OpenPalette),
            KeyCode::Char('?') => Some(AppEvent::ToggleHelp),
            KeyCode::Char('l') => Some(AppEvent::ToggleLogs),
            KeyCode::Char('j') | KeyCode::Down => Some(AppEvent::ScrollLogsDown),
            KeyCode::Char('k') | KeyCode::Up => Some(AppEvent::ScrollLogsUp),
            KeyCode::Esc => Some(AppEvent::ClosePalette),
            KeyCode::Enter => Some(AppEvent::PaletteSelect),
            KeyCode::Backspace => Some(AppEvent::PaletteBackspace),
            KeyCode::Char(c) => Some(AppEvent::PaletteInput(c)),
            _ => None,
        }
    }
}

/// Context for palette key handling (different behavior when palette is open).
pub struct PaletteKeyHandler;

impl PaletteKeyHandler {
    /// Handle a key event when palette is open.
    pub fn handle_key(key: KeyEvent) -> Option<AppEvent> {
        match key.code {
            KeyCode::Esc => Some(AppEvent::ClosePalette),
            KeyCode::Enter => Some(AppEvent::PaletteSelect),
            KeyCode::Up | KeyCode::Char('k') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Some(AppEvent::PaletteUp)
            }
            KeyCode::Down | KeyCode::Char('j') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Some(AppEvent::PaletteDown)
            }
            KeyCode::Up => Some(AppEvent::PaletteUp),
            KeyCode::Down => Some(AppEvent::PaletteDown),
            KeyCode::Backspace => Some(AppEvent::PaletteBackspace),
            KeyCode::Char(c) => Some(AppEvent::PaletteInput(c)),
            _ => None,
        }
    }
}
