//! Command palette actions for the TUI.

/// Actions available in the command palette.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Start a new migration.
    RunMigration,

    /// Run migration in dry-run mode.
    DryRun,

    /// Resume a previous migration.
    Resume,

    /// Validate row counts.
    Validate,

    /// Run health check.
    HealthCheck,

    /// Save logs to file.
    SaveLogs,

    /// Cancel current operation.
    Cancel,

    /// Quit the application.
    Quit,
}

impl Action {
    /// Get all available actions.
    pub fn all() -> Vec<Action> {
        vec![
            Action::RunMigration,
            Action::DryRun,
            Action::Resume,
            Action::Validate,
            Action::HealthCheck,
            Action::SaveLogs,
            Action::Cancel,
            Action::Quit,
        ]
    }

    /// Get the display label for this action.
    pub fn label(&self) -> &'static str {
        match self {
            Action::RunMigration => "Run Migration",
            Action::DryRun => "Dry Run",
            Action::Resume => "Resume Migration",
            Action::Validate => "Validate Row Counts",
            Action::HealthCheck => "Health Check",
            Action::SaveLogs => "Save Logs to File",
            Action::Cancel => "Cancel Current Operation",
            Action::Quit => "Quit",
        }
    }

    /// Get a short description of this action.
    pub fn description(&self) -> &'static str {
        match self {
            Action::RunMigration => "Start a new migration with current config",
            Action::DryRun => "Validate and show plan without transferring data",
            Action::Resume => "Continue a previously interrupted migration",
            Action::Validate => "Compare row counts between source and target",
            Action::HealthCheck => "Test database connections",
            Action::SaveLogs => "Export log buffer to a file",
            Action::Cancel => "Stop the current operation gracefully",
            Action::Quit => "Exit the application",
        }
    }

    /// Get the keyboard shortcut hint for this action.
    pub fn shortcut(&self) -> Option<&'static str> {
        match self {
            Action::Cancel => Some("Ctrl+C"),
            Action::Quit => Some("q"),
            _ => None,
        }
    }
}
