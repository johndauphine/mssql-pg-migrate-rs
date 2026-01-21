//! TUI module for mssql-pg-migrate.
//!
//! Provides a Claude Code CLI-style interface with:
//! - Command palette (Ctrl+P)
//! - Real-time progress display
//! - Streaming log output
//! - Interactive migration control

#![allow(dead_code, unused_imports)]

mod actions;
mod app;
mod events;
mod logging;
mod ui;
mod widgets;
mod wizard;

pub use actions::Action;
pub use app::{App, InputMode, SlashCommand, Suggestion};
pub use events::{
    AppEvent, EventHandler, SharedInputMode, INPUT_MODE_COMMAND, INPUT_MODE_FILE,
    INPUT_MODE_NORMAL, INPUT_MODE_WIZARD,
};
pub use logging::TuiLogLayer;
pub use wizard::{WizardState, WizardStep};

use crate::tui::ui::render;
use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use mssql_pg_migrate::{Config, MigrateError};
use ratatui::{backend::CrosstermBackend, Terminal};
use std::io::{self, Stdout};
use std::panic;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU8};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Type alias for the terminal backend.
pub type Tui = Terminal<CrosstermBackend<Stdout>>;

/// Setup the terminal for TUI mode.
pub fn setup_terminal() -> Result<Tui, MigrateError> {
    enable_raw_mode()
        .map_err(|e| MigrateError::Config(format!("Failed to enable raw mode: {}", e)))?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)
        .map_err(|e| MigrateError::Config(format!("Failed to enter alternate screen: {}", e)))?;
    let backend = CrosstermBackend::new(stdout);
    Terminal::new(backend)
        .map_err(|e| MigrateError::Config(format!("Failed to create terminal: {}", e)))
}

/// Restore the terminal to normal mode.
pub fn restore_terminal(terminal: &mut Tui) -> Result<(), MigrateError> {
    disable_raw_mode()
        .map_err(|e| MigrateError::Config(format!("Failed to disable raw mode: {}", e)))?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )
    .map_err(|e| MigrateError::Config(format!("Failed to leave alternate screen: {}", e)))?;
    terminal
        .show_cursor()
        .map_err(|e| MigrateError::Config(format!("Failed to show cursor: {}", e)))?;
    Ok(())
}

/// Install a panic hook that restores the terminal before panicking.
fn install_panic_hook() {
    let original_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // Attempt to restore terminal
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen, DisableMouseCapture);
        original_hook(panic_info);
    }));
}

/// Run the TUI application.
pub async fn run<P: AsRef<Path>>(config_path: P) -> Result<(), MigrateError> {
    // Load config BEFORE setting up terminal so errors display properly
    let config = Config::load(config_path.as_ref())?;

    // Install panic hook to restore terminal on panic
    install_panic_hook();

    // Setup terminal
    let mut terminal = setup_terminal()?;

    // Create event channels
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);

    // Create shared state for palette and input mode
    let palette_open = Arc::new(AtomicBool::new(false));
    let shared_input_mode: SharedInputMode = Arc::new(AtomicU8::new(INPUT_MODE_NORMAL));

    // Create log channel and layer
    let (log_tx, mut log_rx) = mpsc::channel::<String>(500);
    let tui_layer = TuiLogLayer::new(log_tx);

    // Set up tracing with the TUI layer
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,mssql_pg_migrate=debug"));

    if let Err(e) = tracing_subscriber::registry()
        .with(filter)
        .with(tui_layer)
        .try_init()
    {
        eprintln!("Warning: Failed to set up TUI logging: {}", e);
    }

    // Test log to verify tracing is working
    tracing::info!("TUI logging initialized");

    // Create application state
    let mut app = App::new(
        config,
        config_path.as_ref().to_path_buf(),
        event_tx.clone(),
        palette_open.clone(),
        shared_input_mode.clone(),
    );

    // Spawn log forwarder
    let log_event_tx = event_tx.clone();
    tokio::spawn(async move {
        while let Some(log_line) = log_rx.recv().await {
            let _ = log_event_tx.send(AppEvent::Log(log_line)).await;
        }
    });

    // Create event handler for keyboard/tick events
    let event_handler = EventHandler::new(event_tx.clone(), palette_open, shared_input_mode);
    let _event_handle = tokio::spawn(async move {
        event_handler.run().await;
    });

    // Main event loop
    loop {
        // Render UI
        terminal.draw(|frame| render(frame, &app))?;

        // Wait for at least one event
        if let Some(event) = event_rx.recv().await {
            // Check if this is a progress event with table completion
            let is_table_completion =
                matches!(&event, AppEvent::Progress(p) if p.current_table.is_some());

            match app.handle_event(event).await {
                Ok(should_quit) => {
                    if should_quit {
                        break;
                    }
                }
                Err(e) => app.add_error(format!("Error: {}", e)),
            }

            // Force immediate render after table completion for visual feedback
            if is_table_completion {
                terminal.draw(|frame| render(frame, &app))?;
            }
        }
    }

    // Restore terminal
    restore_terminal(&mut terminal)?;

    Ok(())
}
