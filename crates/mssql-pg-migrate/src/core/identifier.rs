//! Centralized identifier validation and quoting for SQL injection prevention.
//!
//! This module provides secure, consistent functions for handling SQL identifiers
//! across all database backends. It replaces scattered quote_ident functions
//! throughout the codebase with a single, well-tested implementation.
//!
//! # Security
//!
//! SQL identifiers (table names, column names, schema names) cannot be passed as
//! parameters in prepared statements - only data values can be parameterized.
//! This is a fundamental limitation of SQL, not a design choice.
//!
//! To safely construct dynamic SQL with identifiers, we:
//! 1. Validate identifiers for suspicious patterns (null bytes, excessive length)
//! 2. Apply database-specific quoting (brackets, double quotes, backticks)
//! 3. Escape special characters within the quotes
//!
//! This prevents SQL injection through identifier names while allowing dynamic
//! table/column selection required for a generic migration tool.

use crate::error::{MigrateError, Result};

/// Maximum identifier length (conservative limit across databases).
/// - PostgreSQL: 63 bytes
/// - SQL Server: 128 characters
/// - MySQL: 64 characters
const MAX_IDENTIFIER_LENGTH: usize = 128;

/// Validate an identifier for security issues.
///
/// Rejects:
/// - Empty identifiers
/// - Identifiers containing null bytes (injection vector)
/// - Identifiers exceeding maximum length
///
/// # Errors
///
/// Returns `MigrateError::Config` for invalid identifiers with a descriptive message.
pub fn validate_identifier(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(MigrateError::Config(
            "Identifier cannot be empty".to_string(),
        ));
    }

    if name.contains('\0') {
        return Err(MigrateError::Config(format!(
            "SECURITY: Identifier contains null byte (possible injection attempt): {:?}",
            name
        )));
    }

    if name.len() > MAX_IDENTIFIER_LENGTH {
        return Err(MigrateError::Config(format!(
            "SECURITY: Identifier exceeds maximum length of {} bytes (got {} bytes): {:?}",
            MAX_IDENTIFIER_LENGTH,
            name.len(),
            name
        )));
    }

    Ok(())
}

/// Quote a PostgreSQL identifier.
///
/// Escapes double quotes by doubling them and wraps in double quotes.
/// Validates the identifier before quoting.
///
/// # Examples
///
/// ```ignore
/// assert_eq!(quote_pg("users")?, "\"users\"");
/// assert_eq!(quote_pg("table\"name")?, "\"table\"\"name\"");
/// ```
pub fn quote_pg(name: &str) -> Result<String> {
    validate_identifier(name)?;
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}

/// Quote a MySQL identifier using backticks.
///
/// Escapes backticks by doubling them and wraps in backticks.
/// Validates the identifier before quoting.
///
/// # Examples
///
/// ```ignore
/// assert_eq!(quote_mysql("users")?, "`users`");
/// assert_eq!(quote_mysql("table`name")?, "`table``name`");
/// ```
pub fn quote_mysql(name: &str) -> Result<String> {
    validate_identifier(name)?;
    Ok(format!("`{}`", name.replace('`', "``")))
}

/// Quote a SQL Server identifier using brackets.
///
/// Escapes closing brackets by doubling them and wraps in brackets.
/// Validates the identifier before quoting.
///
/// # Examples
///
/// ```ignore
/// assert_eq!(quote_mssql("users")?, "[users]");
/// assert_eq!(quote_mssql("table]name")?, "[table]]name]");
/// ```
pub fn quote_mssql(name: &str) -> Result<String> {
    validate_identifier(name)?;
    Ok(format!("[{}]", name.replace(']', "]]")))
}

/// Qualify a PostgreSQL table name with schema.
///
/// Returns `schema.table` with proper quoting.
pub fn qualify_pg(schema: &str, table: &str) -> Result<String> {
    Ok(format!("{}.{}", quote_pg(schema)?, quote_pg(table)?))
}

/// Qualify a MySQL table name with schema/database.
///
/// Returns `schema.table` with proper quoting.
pub fn qualify_mysql(schema: &str, table: &str) -> Result<String> {
    Ok(format!("{}.{}", quote_mysql(schema)?, quote_mysql(table)?))
}

/// Qualify a SQL Server table name with schema.
///
/// Returns `schema.table` with proper quoting.
pub fn qualify_mssql(schema: &str, table: &str) -> Result<String> {
    Ok(format!("{}.{}", quote_mssql(schema)?, quote_mssql(table)?))
}

/// Validate a check constraint definition for SQL injection patterns.
///
/// Check constraint definitions come from source databases and should only contain
/// simple boolean expressions. This function rejects definitions that contain
/// patterns commonly used in SQL injection attacks.
///
/// # Rejected Patterns
///
/// - Semicolons (multiple statement injection)
/// - SQL comments (`--`, `/*`, `*/`)
/// - `EXEC`/`EXECUTE` keywords
/// - Stored procedure prefixes (`xp_`, `sp_`)
///
/// # Examples
///
/// ```ignore
/// // Valid constraints
/// validate_check_constraint("value > 0")?;
/// validate_check_constraint("status IN ('active', 'inactive')")?;
///
/// // Rejected (injection attempts)
/// validate_check_constraint("1=1; DROP TABLE users").is_err();
/// validate_check_constraint("1=1 -- comment").is_err();
/// validate_check_constraint("1=1; EXEC sp_something").is_err();
/// ```
pub fn validate_check_constraint(definition: &str) -> Result<()> {
    // Normalize for case-insensitive checks
    let lower = definition.to_lowercase();

    // Check for semicolons (multiple statements)
    if definition.contains(';') {
        return Err(MigrateError::Config(format!(
            "SECURITY: Check constraint contains semicolon (possible injection): {:?}",
            definition
        )));
    }

    // Check for SQL comments
    if definition.contains("--") || definition.contains("/*") || definition.contains("*/") {
        return Err(MigrateError::Config(format!(
            "SECURITY: Check constraint contains SQL comment markers (possible injection): {:?}",
            definition
        )));
    }

    // Check for EXEC/EXECUTE keywords (word boundaries)
    // Match "exec" or "execute" as whole words
    if lower.split_whitespace().any(|word| {
        word == "exec"
            || word == "execute"
            || word.starts_with("exec(")
            || word.starts_with("execute(")
    }) {
        return Err(MigrateError::Config(format!(
            "SECURITY: Check constraint contains EXEC/EXECUTE keyword (possible injection): {:?}",
            definition
        )));
    }

    // Check for known dangerous stored procedures
    // We use a specific list of dangerous procedures rather than broad prefix matching
    // to avoid false positives for legitimate column names like 'sp_rate' or 'xp_value'
    const DANGEROUS_PROCEDURES: &[&str] = &[
        // Extended stored procedures (xp_) - OS/file system access
        "xp_cmdshell",
        "xp_regread",
        "xp_regwrite",
        "xp_regdelete",
        "xp_dirtree",
        "xp_fileexist",
        "xp_availablemedia",
        "xp_enumgroups",
        "xp_loginconfig",
        "xp_makecab",
        "xp_ntsec_enumdomains",
        "xp_subdirs",
        // System stored procedures (sp_) - dynamic SQL and OLE automation
        "sp_executesql",
        "sp_execute",
        "sp_oacreate",
        "sp_oamethod",
        "sp_oagetproperty",
        "sp_oasetproperty",
        "sp_oadestroy",
        "sp_addextendedproc",
        "sp_configure",
        "sp_makewebtask",
    ];

    for proc in DANGEROUS_PROCEDURES {
        // Check if the dangerous procedure name appears as a word (with optional parenthesis)
        // Use word boundary detection by checking characters before/after
        for (idx, _) in lower.match_indices(proc) {
            // Check character before (must be start of string or non-alphanumeric)
            let before_ok = idx == 0 || !lower[..idx].chars().last().unwrap().is_alphanumeric();
            // Check character after (must be end of string, parenthesis, or whitespace)
            let after_idx = idx + proc.len();
            let after_ok = after_idx >= lower.len() || {
                let next_char = lower[after_idx..].chars().next().unwrap();
                !next_char.is_alphanumeric() || next_char == '('
            };

            if before_ok && after_ok {
                return Err(MigrateError::Config(format!(
                    "SECURITY: Check constraint contains dangerous stored procedure '{}' (possible injection): {:?}",
                    proc, definition
                )));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Validation tests
    // =========================================================================

    #[test]
    fn test_validate_identifier_normal() {
        assert!(validate_identifier("users").is_ok());
        assert!(validate_identifier("my_table").is_ok());
        assert!(validate_identifier("Table123").is_ok());
        assert!(validate_identifier("column with spaces").is_ok());
        assert!(validate_identifier("日本語").is_ok()); // Unicode
    }

    #[test]
    fn test_validate_identifier_rejects_empty() {
        let result = validate_identifier("");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    #[test]
    fn test_validate_identifier_rejects_null_byte() {
        let result = validate_identifier("table\0name");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("null byte"));
    }

    #[test]
    fn test_validate_identifier_rejects_too_long() {
        let long_name = "a".repeat(MAX_IDENTIFIER_LENGTH + 1);
        let result = validate_identifier(&long_name);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("maximum length"));
    }

    #[test]
    fn test_validate_identifier_accepts_max_length() {
        let max_name = "a".repeat(MAX_IDENTIFIER_LENGTH);
        assert!(validate_identifier(&max_name).is_ok());
    }

    // =========================================================================
    // PostgreSQL quoting tests
    // =========================================================================

    #[test]
    fn test_quote_pg_normal() {
        assert_eq!(quote_pg("users").unwrap(), "\"users\"");
        assert_eq!(quote_pg("my_table").unwrap(), "\"my_table\"");
        assert_eq!(quote_pg("Table123").unwrap(), "\"Table123\"");
    }

    #[test]
    fn test_quote_pg_escapes_double_quote() {
        assert_eq!(quote_pg("table\"name").unwrap(), "\"table\"\"name\"");
        assert_eq!(quote_pg("a\"b\"c").unwrap(), "\"a\"\"b\"\"c\"");
    }

    #[test]
    fn test_quote_pg_rejects_null_byte() {
        assert!(quote_pg("table\0name").is_err());
    }

    #[test]
    fn test_quote_pg_sql_injection_safely_quoted() {
        // These should be safely quoted (no validation failure, just quoted)
        let result = quote_pg("Robert'); DROP TABLE Students;--");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "\"Robert'); DROP TABLE Students;--\"");
    }

    // =========================================================================
    // MySQL quoting tests
    // =========================================================================

    #[test]
    fn test_quote_mysql_normal() {
        assert_eq!(quote_mysql("users").unwrap(), "`users`");
        assert_eq!(quote_mysql("my_table").unwrap(), "`my_table`");
    }

    #[test]
    fn test_quote_mysql_escapes_backtick() {
        assert_eq!(quote_mysql("table`name").unwrap(), "`table``name`");
        assert_eq!(quote_mysql("a`b`c").unwrap(), "`a``b``c`");
    }

    #[test]
    fn test_quote_mysql_rejects_null_byte() {
        assert!(quote_mysql("table\0name").is_err());
    }

    #[test]
    fn test_quote_mysql_sql_injection_safely_quoted() {
        let result = quote_mysql("Robert`); DROP TABLE Students;--");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "`Robert``); DROP TABLE Students;--`");
    }

    // =========================================================================
    // SQL Server quoting tests
    // =========================================================================

    #[test]
    fn test_quote_mssql_normal() {
        assert_eq!(quote_mssql("users").unwrap(), "[users]");
        assert_eq!(quote_mssql("my_table").unwrap(), "[my_table]");
    }

    #[test]
    fn test_quote_mssql_escapes_bracket() {
        assert_eq!(quote_mssql("table]name").unwrap(), "[table]]name]");
        assert_eq!(quote_mssql("a]b]c").unwrap(), "[a]]b]]c]");
    }

    #[test]
    fn test_quote_mssql_rejects_null_byte() {
        assert!(quote_mssql("table\0name").is_err());
    }

    #[test]
    fn test_quote_mssql_sql_injection_safely_quoted() {
        let result = quote_mssql("Robert]; DROP TABLE Students;--");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "[Robert]]; DROP TABLE Students;--]");
    }

    // =========================================================================
    // Qualification tests
    // =========================================================================

    #[test]
    fn test_qualify_pg() {
        assert_eq!(
            qualify_pg("public", "users").unwrap(),
            "\"public\".\"users\""
        );
    }

    #[test]
    fn test_qualify_mysql() {
        assert_eq!(qualify_mysql("mydb", "users").unwrap(), "`mydb`.`users`");
    }

    #[test]
    fn test_qualify_mssql() {
        assert_eq!(qualify_mssql("dbo", "users").unwrap(), "[dbo].[users]");
    }

    #[test]
    fn test_qualify_rejects_invalid_schema() {
        assert!(qualify_pg("", "users").is_err());
        assert!(qualify_mysql("schema\0name", "users").is_err());
    }

    #[test]
    fn test_qualify_rejects_invalid_table() {
        assert!(qualify_pg("public", "").is_err());
        assert!(qualify_mssql("dbo", "table\0name").is_err());
    }

    // =========================================================================
    // Check constraint validation tests
    // =========================================================================

    #[test]
    fn test_check_constraint_valid() {
        assert!(validate_check_constraint("value > 0").is_ok());
        assert!(validate_check_constraint("status IN ('active', 'inactive')").is_ok());
        assert!(validate_check_constraint("price >= 0 AND price <= 1000000").is_ok());
        assert!(validate_check_constraint("LEN(name) > 0").is_ok());
        assert!(validate_check_constraint("email LIKE '%@%'").is_ok());
    }

    #[test]
    fn test_check_constraint_rejects_semicolon() {
        let result = validate_check_constraint("1=1; DROP TABLE users");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("semicolon"));
    }

    #[test]
    fn test_check_constraint_rejects_line_comment() {
        let result = validate_check_constraint("1=1 -- bypass check");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("comment"));
    }

    #[test]
    fn test_check_constraint_rejects_block_comment() {
        let result = validate_check_constraint("1=1 /* comment */ OR 1=1");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("comment"));
    }

    #[test]
    fn test_check_constraint_rejects_exec() {
        let result = validate_check_constraint("1=1; EXEC xp_cmdshell");
        assert!(result.is_err());
        // Could match either semicolon or EXEC
    }

    #[test]
    fn test_check_constraint_rejects_execute() {
        let result = validate_check_constraint("execute('SELECT 1')");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("EXEC"));
    }

    #[test]
    fn test_check_constraint_rejects_xp_cmdshell() {
        let result = validate_check_constraint("xp_cmdshell('cmd')");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("dangerous stored procedure"));
    }

    #[test]
    fn test_check_constraint_rejects_sp_executesql() {
        let result = validate_check_constraint("sp_executesql(@sql)");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("dangerous stored procedure"));
    }

    #[test]
    fn test_check_constraint_case_insensitive() {
        assert!(validate_check_constraint("EXEC something").is_err());
        assert!(validate_check_constraint("Exec something").is_err());
        assert!(validate_check_constraint("exec something").is_err());
        assert!(validate_check_constraint("XP_cmdshell").is_err());
        assert!(validate_check_constraint("SP_executesql").is_err());
    }

    #[test]
    fn test_check_constraint_allows_partial_matches() {
        // "executive" should be allowed (doesn't contain "exec" as a word)
        assert!(validate_check_constraint("status = 'executive'").is_ok());
        // "respect" should be allowed
        assert!(validate_check_constraint("type = 'respect'").is_ok());
    }

    #[test]
    fn test_check_constraint_allows_legitimate_sp_xp_columns() {
        // Legitimate column names containing sp_ or xp_ prefixes should be allowed
        assert!(validate_check_constraint("sp_rate > 0").is_ok());
        assert!(validate_check_constraint("xp_value IS NOT NULL").is_ok());
        assert!(validate_check_constraint("custom_sp_format = 'A'").is_ok());
        assert!(validate_check_constraint("my_xp_column < 100").is_ok());
    }

    // =========================================================================
    // Serialization tests (for password field fix)
    // =========================================================================

    #[test]
    fn test_source_config_password_not_serialized() {
        use crate::config::SourceConfig;

        let config = SourceConfig {
            r#type: "mssql".to_string(),
            host: "localhost".to_string(),
            port: 1433,
            database: "test".to_string(),
            user: "sa".to_string(),
            password: "secret_password".to_string(),
            schema: "dbo".to_string(),
            ssl_mode: "disable".to_string(),
            encrypt: true,
            trust_server_cert: false,
            auth: Default::default(),
        };

        let json = serde_json::to_string(&config).unwrap();
        assert!(
            !json.contains("secret_password"),
            "Password was serialized: {}",
            json
        );
    }

    #[test]
    fn test_target_config_password_not_serialized() {
        use crate::config::TargetConfig;

        let config = TargetConfig {
            r#type: "postgres".to_string(),
            host: "localhost".to_string(),
            port: 5432,
            database: "test".to_string(),
            user: "postgres".to_string(),
            password: "super_secret".to_string(),
            schema: "public".to_string(),
            ssl_mode: "require".to_string(),
            encrypt: true,
            trust_server_cert: false,
            auth: Default::default(),
        };

        let json = serde_json::to_string(&config).unwrap();
        assert!(
            !json.contains("super_secret"),
            "Password was serialized: {}",
            json
        );
    }
}
