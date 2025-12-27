//! TUI module for mssql-pg-migrate.
//!
//! Provides a Claude Code CLI-style interface with:
//! - Command palette (Ctrl+P)
//! - Real-time progress display
//! - Streaming log output
//! - Interactive migration control

mod app;
mod events;
mod ui;
mod actions;
mod logging;
mod widgets;

pub use app::App;
pub use events::{AppEvent, EventHandler};
pub use actions::Action;
pub use logging::TuiLogLayer;

use crate::tui::ui::render;
use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use mssql_pg_migrate::{Config, MigrateError};
use ratatui::{backend::CrosstermBackend, Terminal};
use std::io::{self, Stdout};
use std::path::Path;
use std::panic;
use tokio::sync::mpsc;

/// Type alias for the terminal backend.
pub type Tui = Terminal<CrosstermBackend<Stdout>>;

/// Setup the terminal for TUI mode.
pub fn setup_terminal() -> Result<Tui, MigrateError> {
    enable_raw_mode().map_err(|e| MigrateError::Config(format!("Failed to enable raw mode: {}", e)))?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)
        .map_err(|e| MigrateError::Config(format!("Failed to enter alternate screen: {}", e)))?;
    let backend = CrosstermBackend::new(stdout);
    Terminal::new(backend).map_err(|e| MigrateError::Config(format!("Failed to create terminal: {}", e)))
}

/// Restore the terminal to normal mode.
pub fn restore_terminal(terminal: &mut Tui) -> Result<(), MigrateError> {
    disable_raw_mode().map_err(|e| MigrateError::Config(format!("Failed to disable raw mode: {}", e)))?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )
    .map_err(|e| MigrateError::Config(format!("Failed to leave alternate screen: {}", e)))?;
    terminal.show_cursor().map_err(|e| MigrateError::Config(format!("Failed to show cursor: {}", e)))?;
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
    // Install panic hook to restore terminal on panic
    install_panic_hook();

    // Setup terminal
    let mut terminal = setup_terminal()?;

    // Load config for display (don't connect yet)
    let config = Config::load(config_path.as_ref())?;

    // Create application state
    let mut app = App::new(config, config_path.as_ref().to_path_buf());

    // Create event channels
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);

    // Create event handler for keyboard/tick events
    let event_handler = EventHandler::new(event_tx.clone());
    let _event_handle = tokio::spawn(async move {
        event_handler.run().await;
    });

    // Main event loop
    loop {
        // Render UI
        terminal.draw(|frame| render(frame, &app))?;

        // Handle events
        if let Some(event) = event_rx.recv().await {
            match app.handle_event(event).await {
                Ok(should_quit) => {
                    if should_quit {
                        break;
                    }
                }
                Err(e) => {
                    app.add_error(format!("Error: {}", e));
                }
            }
        }
    }

    // Restore terminal
    restore_terminal(&mut terminal)?;

    Ok(())
}
