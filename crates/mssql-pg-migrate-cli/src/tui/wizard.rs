//! Configuration wizard for the TUI.
//!
//! Provides a step-by-step interactive configuration builder similar to the Go implementation.

use mssql_pg_migrate::Config;
use std::path::PathBuf;

/// Wizard step enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum WizardStep {
    #[default]
    SourceType,
    SourceHost,
    SourcePort,
    SourceDatabase,
    SourceUser,
    SourcePassword,
    TargetHost,
    TargetPort,
    TargetDatabase,
    TargetUser,
    TargetPassword,
    TargetMode,
    Workers,
    OutputFile,
    Confirm,
    Done,
}

/// Options for enum selection steps.
#[derive(Debug, Clone)]
pub struct EnumOption {
    pub value: &'static str,
    pub label: &'static str,
    pub description: &'static str,
}

impl WizardStep {
    /// Check if this step is an enum selection (not free text input).
    pub fn is_enum_selection(&self) -> bool {
        matches!(self, Self::SourceType | Self::TargetMode | Self::Confirm)
    }

    /// Get the available options for enum selection steps.
    pub fn get_options(&self) -> Vec<EnumOption> {
        match self {
            Self::SourceType => vec![
                EnumOption {
                    value: "mssql",
                    label: "SQL Server",
                    description: "Microsoft SQL Server (MSSQL)",
                },
                EnumOption {
                    value: "mysql",
                    label: "MySQL",
                    description: "MySQL or MariaDB",
                },
            ],
            Self::TargetMode => vec![
                EnumOption {
                    value: "drop_recreate",
                    label: "Drop & Recreate",
                    description: "Drop target tables and recreate them (clean migration)",
                },
                EnumOption {
                    value: "truncate",
                    label: "Truncate",
                    description: "Truncate existing tables before inserting data",
                },
                EnumOption {
                    value: "upsert",
                    label: "Upsert",
                    description: "Insert or update rows based on primary key",
                },
            ],
            Self::Confirm => vec![
                EnumOption {
                    value: "y",
                    label: "Yes",
                    description: "Save configuration and exit wizard",
                },
                EnumOption {
                    value: "n",
                    label: "No",
                    description: "Cancel without saving",
                },
            ],
            _ => vec![],
        }
    }

    /// Advance to the next step.
    pub fn next(self) -> Self {
        match self {
            Self::SourceType => Self::SourceHost,
            Self::SourceHost => Self::SourcePort,
            Self::SourcePort => Self::SourceDatabase,
            Self::SourceDatabase => Self::SourceUser,
            Self::SourceUser => Self::SourcePassword,
            Self::SourcePassword => Self::TargetHost,
            Self::TargetHost => Self::TargetPort,
            Self::TargetPort => Self::TargetDatabase,
            Self::TargetDatabase => Self::TargetUser,
            Self::TargetUser => Self::TargetPassword,
            Self::TargetPassword => Self::TargetMode,
            Self::TargetMode => Self::Workers,
            Self::Workers => Self::OutputFile,
            Self::OutputFile => Self::Confirm,
            Self::Confirm => Self::Done,
            Self::Done => Self::Done,
        }
    }

    /// Go back to the previous step.
    pub fn prev(self) -> Self {
        match self {
            Self::SourceType => Self::SourceType,
            Self::SourceHost => Self::SourceType,
            Self::SourcePort => Self::SourceHost,
            Self::SourceDatabase => Self::SourcePort,
            Self::SourceUser => Self::SourceDatabase,
            Self::SourcePassword => Self::SourceUser,
            Self::TargetHost => Self::SourcePassword,
            Self::TargetPort => Self::TargetHost,
            Self::TargetDatabase => Self::TargetPort,
            Self::TargetUser => Self::TargetDatabase,
            Self::TargetPassword => Self::TargetUser,
            Self::TargetMode => Self::TargetPassword,
            Self::Workers => Self::TargetMode,
            Self::OutputFile => Self::Workers,
            Self::Confirm => Self::OutputFile,
            Self::Done => Self::Confirm,
        }
    }

    /// Check if this step requires password input (masked).
    pub fn is_password(&self) -> bool {
        matches!(self, Self::SourcePassword | Self::TargetPassword)
    }

    /// Get the step number (1-indexed).
    pub fn step_number(&self) -> usize {
        match self {
            Self::SourceType => 1,
            Self::SourceHost => 2,
            Self::SourcePort => 3,
            Self::SourceDatabase => 4,
            Self::SourceUser => 5,
            Self::SourcePassword => 6,
            Self::TargetHost => 7,
            Self::TargetPort => 8,
            Self::TargetDatabase => 9,
            Self::TargetUser => 10,
            Self::TargetPassword => 11,
            Self::TargetMode => 12,
            Self::Workers => 13,
            Self::OutputFile => 14,
            Self::Confirm => 15,
            Self::Done => 15,
        }
    }

    /// Get the total number of steps.
    pub fn total_steps() -> usize {
        15
    }
}

/// Configuration being built by the wizard.
#[derive(Debug, Clone, Default)]
pub struct WizardConfig {
    pub source_type: String,
    pub source_host: String,
    pub source_port: u16,
    pub source_database: String,
    pub source_user: String,
    pub source_password: String,
    pub target_host: String,
    pub target_port: u16,
    pub target_database: String,
    pub target_user: String,
    pub target_password: String,
    pub target_mode: String,
    pub workers: usize,
}

impl WizardConfig {
    /// Create a new config with defaults.
    pub fn new() -> Self {
        Self {
            source_type: "mssql".to_string(),
            source_host: "localhost".to_string(),
            source_port: 1433,
            source_database: String::new(),
            source_user: String::new(),
            source_password: String::new(),
            target_host: "localhost".to_string(),
            target_port: 5432,
            target_database: String::new(),
            target_user: String::new(),
            target_password: String::new(),
            target_mode: "drop_recreate".to_string(),
            workers: 4,
        }
    }

    /// Create a config pre-populated from an existing Config.
    pub fn from_config(config: &Config) -> Self {
        use mssql_pg_migrate::TargetMode;
        let target_mode = match config.migration.target_mode {
            TargetMode::DropRecreate => "drop_recreate",
            TargetMode::Truncate => "truncate",
            TargetMode::Upsert => "upsert",
        };
        // Wizard always outputs postgres as target, so ensure port is correct
        // even if loaded config had a different target type
        let target_port = if config.target.r#type == "postgres" || config.target.r#type == "postgresql" {
            config.target.port
        } else {
            5432 // Default postgres port
        };
        Self {
            source_type: config.source.r#type.clone(),
            source_host: config.source.host.clone(),
            source_port: config.source.port,
            source_database: config.source.database.clone(),
            source_user: config.source.user.clone(),
            source_password: config.source.password.clone(),
            target_host: config.target.host.clone(),
            target_port,
            target_database: config.target.database.clone(),
            target_user: config.target.user.clone(),
            target_password: config.target.password.clone(),
            target_mode: target_mode.to_string(),
            workers: config.migration.get_workers(),
        }
    }

    /// Generate YAML configuration string.
    pub fn to_yaml(&self) -> String {
        format!(
            r#"# Generated by mssql-pg-migrate wizard
source:
  type: {}
  host: {}
  port: {}
  database: {}
  user: {}
  password: {}

target:
  type: postgres
  host: {}
  port: {}
  database: {}
  user: {}
  password: {}

migration:
  target_mode: {}
  workers: {}
"#,
            self.source_type,
            self.source_host,
            self.source_port,
            self.source_database,
            self.source_user,
            self.source_password,
            self.target_host,
            self.target_port,
            self.target_database,
            self.target_user,
            self.target_password,
            self.target_mode,
            self.workers,
        )
    }
}

/// Wizard state.
#[derive(Debug, Clone)]
pub struct WizardState {
    /// Current step.
    pub step: WizardStep,

    /// Configuration being built.
    pub config: WizardConfig,

    /// Output file path.
    pub output_path: PathBuf,

    /// Transcript of previous answers.
    pub transcript: Vec<String>,

    /// Current input value.
    pub input: String,

    /// Error message to display.
    pub error: Option<String>,

    /// Selected option index for enum selection steps.
    pub selected_option: usize,

    /// Whether the configuration was successfully saved.
    pub was_saved: bool,
}

impl WizardState {
    /// Create a new wizard state.
    pub fn new(output_path: PathBuf) -> Self {
        Self {
            step: WizardStep::default(),
            config: WizardConfig::new(),
            output_path,
            transcript: Vec::new(),
            input: String::new(),
            error: None,
            selected_option: 0,
            was_saved: false,
        }
    }

    /// Create a wizard state pre-populated from an existing Config.
    pub fn with_config(output_path: PathBuf, config: &Config) -> Self {
        Self {
            step: WizardStep::default(),
            config: WizardConfig::from_config(config),
            output_path,
            transcript: Vec::new(),
            input: String::new(),
            error: None,
            selected_option: 0,
            was_saved: false,
        }
    }

    /// Move selection up in enum selection mode.
    pub fn select_prev(&mut self) {
        if self.step.is_enum_selection() {
            let options = self.step.get_options();
            if !options.is_empty() && self.selected_option > 0 {
                self.selected_option -= 1;
            }
        }
    }

    /// Move selection down in enum selection mode.
    pub fn select_next(&mut self) {
        if self.step.is_enum_selection() {
            let options = self.step.get_options();
            if !options.is_empty() && self.selected_option < options.len() - 1 {
                self.selected_option += 1;
            }
        }
    }

    /// Initialize selection to match current config value when entering an enum step.
    pub fn init_selection_for_step(&mut self) {
        if !self.step.is_enum_selection() {
            return;
        }
        let options = self.step.get_options();
        let current_value = match self.step {
            WizardStep::SourceType => &self.config.source_type,
            WizardStep::TargetMode => &self.config.target_mode,
            WizardStep::Confirm => return, // Always start at "Yes"
            _ => return,
        };
        self.selected_option = options
            .iter()
            .position(|o| o.value == current_value)
            .unwrap_or(0);
    }

    /// Get the prompt for the current step.
    pub fn get_prompt(&self) -> String {
        let step_info = format!(
            "[{}/{}] ",
            self.step.step_number(),
            WizardStep::total_steps()
        );
        match self.step {
            WizardStep::SourceType => format!(
                "{}Select source database type:",
                step_info
            ),
            WizardStep::SourceHost => {
                let db_name = match self.config.source_type.as_str() {
                    "mysql" => "MySQL",
                    _ => "SQL Server",
                };
                format!(
                    "{}Source {} Host [{}]: ",
                    step_info, db_name, self.config.source_host
                )
            }
            WizardStep::SourcePort => {
                format!("{}Source Port [{}]: ", step_info, self.config.source_port)
            }
            WizardStep::SourceDatabase => {
                if self.config.source_database.is_empty() {
                    format!("{}Source Database: ", step_info)
                } else {
                    format!(
                        "{}Source Database [{}]: ",
                        step_info, self.config.source_database
                    )
                }
            }
            WizardStep::SourceUser => {
                if self.config.source_user.is_empty() {
                    format!("{}Source User: ", step_info)
                } else {
                    format!("{}Source User [{}]: ", step_info, self.config.source_user)
                }
            }
            WizardStep::SourcePassword => {
                if self.config.source_password.is_empty() {
                    format!("{}Source Password: ", step_info)
                } else {
                    format!("{}Source Password [********]: ", step_info)
                }
            }
            WizardStep::TargetHost => format!(
                "{}Target PostgreSQL Host [{}]: ",
                step_info, self.config.target_host
            ),
            WizardStep::TargetPort => {
                format!("{}Target Port [{}]: ", step_info, self.config.target_port)
            }
            WizardStep::TargetDatabase => {
                if self.config.target_database.is_empty() {
                    format!("{}Target Database: ", step_info)
                } else {
                    format!(
                        "{}Target Database [{}]: ",
                        step_info, self.config.target_database
                    )
                }
            }
            WizardStep::TargetUser => {
                if self.config.target_user.is_empty() {
                    format!("{}Target User: ", step_info)
                } else {
                    format!("{}Target User [{}]: ", step_info, self.config.target_user)
                }
            }
            WizardStep::TargetPassword => {
                if self.config.target_password.is_empty() {
                    format!("{}Target Password: ", step_info)
                } else {
                    format!("{}Target Password [********]: ", step_info)
                }
            }
            WizardStep::TargetMode => format!(
                "{}Target Mode (drop_recreate/truncate/upsert) [{}]: ",
                step_info, self.config.target_mode
            ),
            WizardStep::Workers => {
                format!("{}Number of Workers [{}]: ", step_info, self.config.workers)
            }
            WizardStep::OutputFile => format!(
                "{}Output File [{}]: ",
                step_info,
                self.output_path.display()
            ),
            WizardStep::Confirm => "Save configuration? (y/n): ".to_string(),
            WizardStep::Done => String::new(),
        }
    }

    /// Process input for the current step.
    pub fn process_input(&mut self) -> Result<bool, String> {
        let input = self.input.trim().to_string();
        self.error = None;

        match self.step {
            WizardStep::SourceType => {
                // Use selected option from enum selector
                let options = self.step.get_options();
                if let Some(option) = options.get(self.selected_option) {
                    self.config.source_type = option.value.to_string();
                    // Update port default based on type selection
                    self.config.source_port = match option.value {
                        "mysql" => 3306,
                        _ => 1433,
                    };
                    self.transcript
                        .push(format!("Source Type: {} ({})", option.label, option.value));
                }
            }
            WizardStep::SourceHost => {
                if !input.is_empty() {
                    self.config.source_host = input.clone();
                }
                self.transcript
                    .push(format!("Source Host: {}", self.config.source_host));
            }
            WizardStep::SourcePort => {
                if !input.is_empty() {
                    self.config.source_port = input
                        .parse()
                        .map_err(|_| "Invalid port number".to_string())?;
                }
                self.transcript
                    .push(format!("Source Port: {}", self.config.source_port));
            }
            WizardStep::SourceDatabase => {
                if input.is_empty() && self.config.source_database.is_empty() {
                    return Err("Database name is required".to_string());
                }
                if !input.is_empty() {
                    self.config.source_database = input;
                }
                self.transcript
                    .push(format!("Source Database: {}", self.config.source_database));
            }
            WizardStep::SourceUser => {
                if input.is_empty() && self.config.source_user.is_empty() {
                    return Err("Username is required".to_string());
                }
                if !input.is_empty() {
                    self.config.source_user = input;
                }
                self.transcript
                    .push(format!("Source User: {}", self.config.source_user));
            }
            WizardStep::SourcePassword => {
                // Only update password if input is provided
                if !input.is_empty() {
                    self.config.source_password = input;
                }
                self.transcript
                    .push("Source Password: ********".to_string());
            }
            WizardStep::TargetHost => {
                if !input.is_empty() {
                    self.config.target_host = input.clone();
                }
                self.transcript
                    .push(format!("Target Host: {}", self.config.target_host));
            }
            WizardStep::TargetPort => {
                if !input.is_empty() {
                    self.config.target_port = input
                        .parse()
                        .map_err(|_| "Invalid port number".to_string())?;
                }
                self.transcript
                    .push(format!("Target Port: {}", self.config.target_port));
            }
            WizardStep::TargetDatabase => {
                if input.is_empty() && self.config.target_database.is_empty() {
                    return Err("Database name is required".to_string());
                }
                if !input.is_empty() {
                    self.config.target_database = input;
                }
                self.transcript
                    .push(format!("Target Database: {}", self.config.target_database));
            }
            WizardStep::TargetUser => {
                if input.is_empty() && self.config.target_user.is_empty() {
                    return Err("Username is required".to_string());
                }
                if !input.is_empty() {
                    self.config.target_user = input;
                }
                self.transcript
                    .push(format!("Target User: {}", self.config.target_user));
            }
            WizardStep::TargetPassword => {
                // Only update password if input is provided
                if !input.is_empty() {
                    self.config.target_password = input;
                }
                self.transcript
                    .push("Target Password: ********".to_string());
            }
            WizardStep::TargetMode => {
                // Use selected option from enum selector
                let options = self.step.get_options();
                if let Some(option) = options.get(self.selected_option) {
                    self.config.target_mode = option.value.to_string();
                    self.transcript
                        .push(format!("Target Mode: {} ({})", option.label, option.value));
                }
            }
            WizardStep::Workers => {
                if !input.is_empty() {
                    self.config.workers =
                        input.parse().map_err(|_| "Invalid number".to_string())?;
                    if self.config.workers == 0 {
                        return Err("Workers must be at least 1".to_string());
                    }
                }
                self.transcript
                    .push(format!("Workers: {}", self.config.workers));
            }
            WizardStep::OutputFile => {
                if !input.is_empty() {
                    self.output_path = PathBuf::from(&input);
                }
                self.transcript
                    .push(format!("Output: {}", self.output_path.display()));
            }
            WizardStep::Confirm => {
                // Use selected option from enum selector (0 = Yes, 1 = No)
                let confirmed = self.selected_option == 0;
                if confirmed {
                    // Save configuration
                    if let Err(e) = self.save() {
                        return Err(format!("Failed to save: {}", e));
                    }
                    self.was_saved = true;
                    self.transcript.push(format!(
                        "Configuration saved to {}",
                        self.output_path.display()
                    ));
                } else {
                    self.was_saved = false;
                    self.transcript.push("Configuration cancelled.".to_string());
                }
                self.step = WizardStep::Done;
                self.input.clear();
                return Ok(true); // Wizard complete
            }
            WizardStep::Done => {
                return Ok(true);
            }
        }

        // Advance to next step
        self.step = self.step.next();
        self.input.clear();
        self.selected_option = 0;
        self.init_selection_for_step();
        Ok(false)
    }

    /// Save the configuration to file.
    fn save(&self) -> std::io::Result<()> {
        std::fs::write(&self.output_path, self.config.to_yaml())
    }

    /// Check if the wizard is complete.
    pub fn is_done(&self) -> bool {
        self.step == WizardStep::Done
    }
}
