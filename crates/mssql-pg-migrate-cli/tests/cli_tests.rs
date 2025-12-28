//! CLI integration tests for mssql-pg-migrate.
//!
//! These tests verify command-line argument parsing, help output,
//! and exit codes for various error conditions.

use assert_cmd::Command;
use predicates::prelude::*;
use std::io::Write;

/// Get a command for the mssql-pg-migrate binary.
fn cmd() -> Command {
    Command::cargo_bin("mssql-pg-migrate").unwrap()
}

// =============================================================================
// Help and Version Tests
// =============================================================================

#[test]
fn test_help_shows_all_commands() {
    cmd()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("run"))
        .stdout(predicate::str::contains("resume"))
        .stdout(predicate::str::contains("validate"))
        .stdout(predicate::str::contains("health-check"));
}

#[test]
fn test_run_subcommand_help() {
    cmd()
        .args(["run", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--dry-run"))
        .stdout(predicate::str::contains("--source-schema"))
        .stdout(predicate::str::contains("--target-schema"))
        .stdout(predicate::str::contains("--workers"));
}

#[test]
fn test_resume_subcommand_help() {
    cmd()
        .args(["resume", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--source-schema"))
        .stdout(predicate::str::contains("--target-schema"));
}

#[test]
fn test_version_flag() {
    cmd()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("mssql-pg-migrate"));
}

// =============================================================================
// Global Flags Tests
// =============================================================================

#[test]
fn test_shutdown_timeout_default() {
    cmd()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("--shutdown-timeout"))
        .stdout(predicate::str::contains("[default: 60]"));
}

#[test]
fn test_progress_flag_exists() {
    cmd()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("--progress"));
}

#[test]
fn test_output_json_flag_exists() {
    cmd()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("--output-json"));
}

#[test]
fn test_state_file_flag_exists() {
    cmd()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("--state-file"));
}

#[test]
fn test_log_format_flag_exists() {
    cmd()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("--log-format"))
        .stdout(predicate::str::contains("[default: text]"));
}

#[test]
fn test_verbosity_flag_exists() {
    cmd()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("--verbosity"))
        .stdout(predicate::str::contains("[default: info]"));
}

// =============================================================================
// Exit Code Tests - Config Errors (Exit Code 1)
// =============================================================================

#[test]
fn test_missing_config_exits_with_code_7() {
    // Missing file is an IO error (code 7), not config error (code 1)
    cmd()
        .args(["--config", "nonexistent_config_file.yaml", "health-check"])
        .assert()
        .code(7); // EXIT_IO_ERROR - file not found
}

#[test]
fn test_invalid_yaml_exits_with_code_1() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    writeln!(file, "invalid: yaml: content: [").unwrap();

    cmd()
        .args(["--config", file.path().to_str().unwrap(), "health-check"])
        .assert()
        .code(1); // EXIT_CONFIG_ERROR
}

#[test]
fn test_empty_config_exits_with_code_1() {
    let file = tempfile::NamedTempFile::new().unwrap();
    // Empty file is invalid YAML config

    cmd()
        .args(["--config", file.path().to_str().unwrap(), "health-check"])
        .assert()
        .code(1); // EXIT_CONFIG_ERROR
}

#[test]
fn test_missing_required_fields_exits_with_code_1() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    // Valid YAML but missing required config fields
    writeln!(file, "source:").unwrap();
    writeln!(file, "  type: mssql").unwrap();

    cmd()
        .args(["--config", file.path().to_str().unwrap(), "health-check"])
        .assert()
        .code(1); // EXIT_CONFIG_ERROR
}

// =============================================================================
// Subcommand Existence Tests
// =============================================================================

#[test]
fn test_health_check_command_exists() {
    cmd()
        .args(["health-check", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Test database connections"));
}

#[test]
fn test_validate_command_exists() {
    cmd()
        .args(["validate", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Validate row counts"));
}

// =============================================================================
// Config Path Tests
// =============================================================================

#[test]
fn test_config_default_path() {
    cmd()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("[default: config.yaml]"));
}

#[test]
fn test_short_config_flag() {
    // -c should work as short for --config
    cmd()
        .args(["-c", "some_config.yaml", "--help"])
        .assert()
        .success();
}

// =============================================================================
// No Subcommand Tests
// =============================================================================

#[test]
fn test_no_subcommand_shows_help() {
    cmd()
        .assert()
        .failure()
        .stderr(predicate::str::contains("Usage:"));
}
