//! ODBC-based MSSQL target for Kerberos and SQL Server authentication.
//!
//! This module provides an ODBC-based implementation of `TargetPool` that supports:
//! - Kerberos/Windows Integrated Authentication (`auth: kerberos`)
//! - SQL Server Authentication via ODBC (`auth: odbc`)
//!
//! **Requirements:**
//! - The `kerberos` feature must be enabled
//! - Microsoft ODBC Driver for SQL Server must be installed:
//!   - Windows: Download from Microsoft (uses SSPI automatically for Kerberos)
//!   - Linux: `apt install msodbcsql18` or `yum install msodbcsql18`
//!   - macOS: `brew install msodbcsql18`
//!
//! **Usage (Kerberos):**
//! On Linux/macOS, obtain a Kerberos ticket before running:
//! ```sh
//! kinit user@REALM
//! ```
//!
//! On Windows with domain-joined machines, authentication is automatic via SSPI.
//!
//! **Usage (SQL Server auth via ODBC):**
//! Set `auth: odbc` in the config and provide username/password.
//!
//! **Performance Notes:**
//! ODBC does not support TDS bulk insert, so this implementation uses parameterized
//! batch INSERT statements. While slower than TDS bulk insert (~10-30K rows/sec vs
//! ~70K+ rows/sec), it still provides reasonable performance for most workloads.

use crate::config::TargetConfig;
use crate::error::{MigrateError, Result};
use crate::source::{CheckConstraint, ForeignKey, Index, Table};
use crate::target::{SqlValue, TargetPool};
use crate::typemap::postgres_to_mssql;
use async_trait::async_trait;
use odbc_api::{ConnectionOptions, Cursor, Environment, ResultSetMetadata};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// Maximum number of parameters per ODBC batch (MSSQL limit is 2100).
const MAX_PARAMS_PER_BATCH: usize = 2100;

/// Maximum rows per INSERT VALUES clause (MSSQL limit is 1000).
const MAX_ROWS_PER_INSERT: usize = 1000;

/// Number of INSERT statements to batch into a single execute call.
/// Multiple INSERT statements separated by semicolons are sent as one batch,
/// reducing round-trip overhead significantly.
const STATEMENTS_PER_EXECUTE: usize = 10;

/// Maximum number of retry attempts for deadlock errors.
const DEADLOCK_MAX_RETRIES: u32 = 5;

/// Base delay between deadlock retry attempts in milliseconds.
const DEADLOCK_RETRY_DELAY_MS: u64 = 200;

/// ODBC-based MSSQL target pool for Kerberos authentication.
pub struct OdbcMssqlTargetPool {
    env: Arc<Environment>,
    connection_string: String,
    #[allow(dead_code)]
    config: TargetConfig,
    /// Mutex to serialize ODBC operations (ODBC is not fully thread-safe)
    conn_mutex: Mutex<()>,
}

impl OdbcMssqlTargetPool {
    /// Create a new ODBC-based MSSQL target pool.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The ODBC environment cannot be created (driver not installed)
    /// - The connection to the database fails
    pub async fn new(config: &TargetConfig, _max_conns: u32) -> Result<Self> {
        use crate::config::AuthMethod;

        // Create ODBC environment - this will fail if ODBC is not installed
        let env = Environment::new().map_err(|e| {
            MigrateError::pool_msg(
                format!(
                    "Failed to create ODBC environment: {}.\n\n\
                     ODBC authentication requires the Microsoft ODBC Driver for SQL Server.\n\
                     Please install it:\n\
                     - Windows: Download from Microsoft\n\
                     - Linux: apt install msodbcsql18 (or yum install msodbcsql18)\n\
                     - macOS: brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release && \
                              brew install msodbcsql18",
                    e
                ),
                "ODBC connection",
            )
        })?;

        // Build connection string based on auth method
        let (connection_string, auth_desc) = match config.auth {
            AuthMethod::Kerberos => {
                // Kerberos: use Trusted_Connection (Windows Integrated Auth)
                let conn_str = format!(
                    "Driver={{ODBC Driver 18 for SQL Server}};\
                     Server={},{};\
                     Database={};\
                     Trusted_Connection=yes;\
                     Encrypt={};\
                     TrustServerCertificate={};",
                    config.host,
                    config.port,
                    config.database,
                    if config.encrypt { "yes" } else { "no" },
                    if config.trust_server_cert { "yes" } else { "no" },
                );
                (conn_str, "Kerberos")
            }
            AuthMethod::Odbc | AuthMethod::Native | AuthMethod::SqlServer => {
                // SQL Server auth via ODBC: use UID/PWD
                let conn_str = format!(
                    "Driver={{ODBC Driver 18 for SQL Server}};\
                     Server={},{};\
                     Database={};\
                     UID={};\
                     PWD={};\
                     Encrypt={};\
                     TrustServerCertificate={};",
                    config.host,
                    config.port,
                    config.database,
                    config.user,
                    config.password,
                    if config.encrypt { "yes" } else { "no" },
                    if config.trust_server_cert { "yes" } else { "no" },
                );
                (conn_str, "SQL Server")
            }
        };

        debug!(
            "ODBC target connection string (credentials hidden): Driver={{ODBC Driver 18 for SQL Server}};Server={},{};Database={};Auth={};...",
            config.host, config.port, config.database, auth_desc
        );

        // Test connection
        {
            let conn = env
                .connect_with_connection_string(&connection_string, ConnectionOptions::default())
                .map_err(|e| {
                    let help_msg = if config.auth == AuthMethod::Kerberos {
                        "Ensure you have a valid Kerberos ticket:\n\
                         - Linux/macOS: Run 'kinit user@REALM' before running the migration\n\
                         - Windows: Be on a domain-joined machine or use 'runas /netonly'"
                    } else {
                        "Check that the username and password are correct."
                    };
                    MigrateError::pool_msg(
                        format!(
                            "Failed to connect to MSSQL target via ODBC ({}): {}.\n\n{}",
                            auth_desc, e, help_msg
                        ),
                        "ODBC connection",
                    )
                })?;

            // Verify connection with a simple query
            let _ = conn.execute("SELECT 1", ());
        }

        info!(
            "Connected to MSSQL target via ODBC ({}): {}:{}/{}",
            auth_desc, config.host, config.port, config.database
        );

        Ok(Self {
            env: Arc::new(env),
            connection_string,
            config: config.clone(),
            conn_mutex: Mutex::new(()),
        })
    }

    /// Get a new ODBC connection.
    fn get_connection(&self) -> Result<odbc_api::Connection<'_>> {
        self.env
            .connect_with_connection_string(&self.connection_string, ConnectionOptions::default())
            .map_err(|e| {
                MigrateError::pool_msg(
                    format!("ODBC target connection failed: {}", e),
                    "getting ODBC target connection",
                )
            })
    }

    /// Execute a non-query SQL statement.
    fn execute(&self, sql: &str) -> Result<()> {
        let conn = self.get_connection()?;
        conn.execute(sql, ()).map_err(|e| {
            MigrateError::transfer("ODBC target", format!("execute failed: {} - SQL: {}", e, sql))
        })?;
        Ok(())
    }

    /// Execute a query and return the first column of the first row as i64.
    fn query_scalar_i64(&self, sql: &str) -> Result<i64> {
        let conn = self.get_connection()?;

        if let Some(mut cursor) = conn.execute(sql, ()).map_err(|e| {
            MigrateError::transfer("ODBC target", format!("query failed: {} - SQL: {}", e, sql))
        })? {
            let num_cols = cursor.num_result_cols().map_err(|e| {
                MigrateError::transfer("ODBC target", format!("num_result_cols failed: {}", e))
            })?;

            if num_cols > 0 {
                let mut buffers =
                    odbc_api::buffers::TextRowSet::for_cursor(1, &mut cursor, Some(256)).map_err(
                        |e| {
                            MigrateError::transfer(
                                "ODBC target",
                                format!("create buffer failed: {}", e),
                            )
                        },
                    )?;

                let mut row_cursor = cursor.bind_buffer(&mut buffers).map_err(|e| {
                    MigrateError::transfer("ODBC target", format!("bind buffer failed: {}", e))
                })?;

                if let Some(batch) = row_cursor.fetch().map_err(|e| {
                    MigrateError::transfer("ODBC target", format!("fetch failed: {}", e))
                })? {
                    if batch.num_rows() > 0 {
                        if let Some(bytes) = batch.at(0, 0) {
                            let s = String::from_utf8_lossy(bytes);
                            return s.parse::<i64>().map_err(|e| {
                                MigrateError::transfer(
                                    "ODBC target",
                                    format!("parse i64 failed: {} - value: {}", e, s),
                                )
                            });
                        }
                    }
                }
            }
        }
        Ok(0)
    }

    /// Execute a query and return the first column of the first row as bool (1 = true).
    fn query_scalar_bool(&self, sql: &str) -> Result<bool> {
        let value = self.query_scalar_i64(sql)?;
        Ok(value > 0)
    }

    /// Quote an MSSQL identifier with brackets.
    fn quote_ident(name: &str) -> String {
        format!("[{}]", name.replace(']', "]]"))
    }

    /// Qualify a table name with schema.
    fn qualify_table(schema: &str, table: &str) -> String {
        format!("{}.{}", Self::quote_ident(schema), Self::quote_ident(table))
    }

    /// Check if a data type is an MSSQL native type.
    fn is_mssql_type(data_type: &str) -> bool {
        let lower = data_type.to_lowercase();
        matches!(
            lower.as_str(),
            "bigint"
                | "int"
                | "smallint"
                | "tinyint"
                | "bit"
                | "decimal"
                | "numeric"
                | "money"
                | "smallmoney"
                | "float"
                | "real"
                | "datetime"
                | "datetime2"
                | "smalldatetime"
                | "time"
                | "datetimeoffset"
                | "nchar"
                | "nvarchar"
                | "binary"
                | "varbinary"
                | "image"
                | "uniqueidentifier"
                | "xml"
                | "sql_variant"
                | "rowversion"
                | "geography"
                | "geometry"
                | "hierarchyid"
        )
    }

    /// Format an MSSQL type with proper length/precision.
    fn format_mssql_type(data_type: &str, max_length: i32, precision: i32, scale: i32) -> String {
        let lower = data_type.to_lowercase();
        match lower.as_str() {
            // Fixed-length types
            "bigint" | "int" | "smallint" | "tinyint" | "bit" | "money" | "smallmoney" | "real"
            | "datetime" | "smalldatetime" | "date" | "text" | "ntext" | "image"
            | "uniqueidentifier" | "xml" | "sql_variant" | "timestamp" | "rowversion"
            | "geography" | "geometry" | "hierarchyid" => data_type.to_string(),

            "float" => {
                if precision > 0 {
                    format!("float({})", precision)
                } else {
                    "float".to_string()
                }
            }

            "decimal" | "numeric" => {
                if precision > 0 {
                    format!("{}({}, {})", data_type, precision, scale)
                } else {
                    format!("{}(18, 0)", data_type)
                }
            }

            "datetime2" => {
                if scale > 0 {
                    format!("datetime2({})", scale)
                } else {
                    "datetime2".to_string()
                }
            }

            "time" => {
                if scale > 0 {
                    format!("time({})", scale)
                } else {
                    "time".to_string()
                }
            }

            "datetimeoffset" => {
                if scale > 0 {
                    format!("datetimeoffset({})", scale)
                } else {
                    "datetimeoffset".to_string()
                }
            }

            "char" | "varchar" | "nchar" | "nvarchar" => {
                if max_length == -1 {
                    format!("{}(max)", data_type)
                } else if max_length > 0 {
                    let len = if lower.starts_with('n') && max_length > 0 {
                        max_length / 2
                    } else {
                        max_length
                    };
                    format!("{}({})", data_type, len)
                } else {
                    format!("{}(255)", data_type)
                }
            }

            "binary" | "varbinary" => {
                if max_length == -1 {
                    format!("{}(max)", data_type)
                } else if max_length > 0 {
                    format!("{}({})", data_type, max_length)
                } else {
                    format!("{}(255)", data_type)
                }
            }

            _ => data_type.to_string(),
        }
    }

    /// Check if table has an identity column.
    fn has_identity_column(&self, schema: &str, table: &str) -> Result<bool> {
        let sql = format!(
            r#"SELECT COUNT(*)
               FROM sys.columns c
               JOIN sys.tables t ON c.object_id = t.object_id
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = '{}' AND t.name = '{}' AND c.is_identity = 1"#,
            schema.replace('\'', "''"),
            table.replace('\'', "''")
        );
        self.query_scalar_bool(&sql)
    }

    /// Get column definitions for creating a staging table.
    fn get_column_definitions(&self, schema: &str, table: &str) -> Result<Vec<(String, String, bool)>> {
        let conn = self.get_connection()?;

        let sql = format!(
            r#"
            SELECT
                c.name AS column_name,
                CASE
                    WHEN t.name IN ('nvarchar', 'nchar') AND c.max_length = -1 THEN t.name + '(max)'
                    WHEN t.name IN ('nvarchar', 'nchar') THEN t.name + '(' + CAST(c.max_length/2 AS VARCHAR) + ')'
                    WHEN t.name IN ('varchar', 'char', 'varbinary', 'binary') AND c.max_length = -1 THEN t.name + '(max)'
                    WHEN t.name IN ('varchar', 'char', 'varbinary', 'binary') THEN t.name + '(' + CAST(c.max_length AS VARCHAR) + ')'
                    WHEN t.name IN ('decimal', 'numeric') THEN t.name + '(' + CAST(c.precision AS VARCHAR) + ',' + CAST(c.scale AS VARCHAR) + ')'
                    WHEN t.name IN ('datetime2', 'time', 'datetimeoffset') AND c.scale > 0 THEN t.name + '(' + CAST(c.scale AS VARCHAR) + ')'
                    ELSE t.name
                END AS data_type,
                c.is_nullable
            FROM sys.columns c
            JOIN sys.types t ON c.user_type_id = t.user_type_id
            JOIN sys.tables tbl ON c.object_id = tbl.object_id
            JOIN sys.schemas s ON tbl.schema_id = s.schema_id
            WHERE s.name = '{}' AND tbl.name = '{}'
            ORDER BY c.column_id
            "#,
            schema.replace('\'', "''"),
            table.replace('\'', "''")
        );

        let mut columns = Vec::new();

        if let Some(mut cursor) = conn.execute(&sql, ()).map_err(|e| {
            MigrateError::transfer(
                format!("{}.{}", schema, table),
                format!("getting column definitions: {}", e),
            )
        })? {
            let mut buffers =
                odbc_api::buffers::TextRowSet::for_cursor(100, &mut cursor, Some(4096)).map_err(
                    |e| {
                        MigrateError::transfer(
                            format!("{}.{}", schema, table),
                            format!("create buffer for columns: {}", e),
                        )
                    },
                )?;

            let mut row_cursor = cursor.bind_buffer(&mut buffers).map_err(|e| {
                MigrateError::transfer(
                    format!("{}.{}", schema, table),
                    format!("bind buffer for columns: {}", e),
                )
            })?;

            while let Some(batch) = row_cursor.fetch().map_err(|e| {
                MigrateError::transfer(
                    format!("{}.{}", schema, table),
                    format!("fetch columns: {}", e),
                )
            })? {
                for row_idx in 0..batch.num_rows() {
                    let name = batch
                        .at(0, row_idx)
                        .map(|b| String::from_utf8_lossy(b).to_string())
                        .unwrap_or_default();
                    let data_type = batch
                        .at(1, row_idx)
                        .map(|b| String::from_utf8_lossy(b).to_string())
                        .unwrap_or_default();
                    let is_nullable = batch
                        .at(2, row_idx)
                        .map(|b| String::from_utf8_lossy(b) == "1")
                        .unwrap_or(true);

                    columns.push((name, data_type, is_nullable));
                }
            }
        }

        Ok(columns)
    }

    /// Ensure a staging table exists for upsert operations.
    fn ensure_staging_table(
        &self,
        schema: &str,
        table: &str,
        writer_id: usize,
        partition_id: Option<i32>,
    ) -> Result<String> {
        let staging_table_name = match partition_id {
            Some(pid) => format!("_staging_{}_p{}_{}", table, pid, writer_id),
            None => format!("_staging_{}_{}", table, writer_id),
        };
        let qualified_staging = Self::qualify_table(schema, &staging_table_name);

        // Check if staging table exists
        let check_sql = format!(
            "SELECT OBJECT_ID(N'{}.{}', 'U')",
            schema.replace('\'', "''"),
            staging_table_name.replace('\'', "''")
        );

        let exists = self.query_scalar_i64(&check_sql)? != 0;

        if exists {
            // Truncate existing staging table
            let truncate_sql = format!("TRUNCATE TABLE {}", qualified_staging);
            self.execute(&truncate_sql)?;
            debug!("Truncated staging table {}", qualified_staging);
        } else {
            // Get column definitions from target table
            let columns = self.get_column_definitions(schema, table)?;

            if columns.is_empty() {
                return Err(MigrateError::transfer(
                    format!("{}.{}", schema, table),
                    "no columns found in target table".to_string(),
                ));
            }

            // Build CREATE TABLE statement without identity columns
            let col_defs: Vec<String> = columns
                .iter()
                .map(|(name, data_type, is_nullable)| {
                    let null_str = if *is_nullable { "NULL" } else { "NOT NULL" };
                    format!("{} {} {}", Self::quote_ident(name), data_type, null_str)
                })
                .collect();

            let create_sql = format!(
                "CREATE TABLE {} ({})",
                qualified_staging,
                col_defs.join(", ")
            );

            self.execute(&create_sql)?;
            debug!("Created staging table {}", qualified_staging);
        }

        Ok(qualified_staging)
    }

    /// Build MERGE SQL statement from staging table to target table.
    fn build_staging_merge_sql(
        target_table: &str,
        staging_table: &str,
        cols: &[String],
        pk_cols: &[String],
    ) -> String {
        let col_list: Vec<String> = cols.iter().map(|c| Self::quote_ident(c)).collect();
        let col_str = col_list.join(", ");

        // Build ON clause for PK matching
        let join_condition: Vec<String> = pk_cols
            .iter()
            .map(|pk| {
                format!(
                    "target.{} = source.{}",
                    Self::quote_ident(pk),
                    Self::quote_ident(pk)
                )
            })
            .collect();

        // Build UPDATE SET clause (exclude PK columns)
        let update_cols: Vec<String> = cols
            .iter()
            .filter(|c| !pk_cols.contains(c))
            .map(|c| format!("{} = source.{}", Self::quote_ident(c), Self::quote_ident(c)))
            .collect();

        // Build source column references for INSERT
        let source_cols: Vec<String> = cols
            .iter()
            .map(|c| format!("source.{}", Self::quote_ident(c)))
            .collect();

        if update_cols.is_empty() {
            // PK-only table
            format!(
                r#"MERGE INTO {} WITH (TABLOCK) AS target
                   USING {} AS source
                   ON {}
                   WHEN NOT MATCHED THEN INSERT ({}) VALUES ({});"#,
                target_table,
                staging_table,
                join_condition.join(" AND "),
                col_str,
                source_cols.join(", ")
            )
        } else {
            // Build change detection
            let change_detection: Vec<String> = cols
                .iter()
                .filter(|c| !pk_cols.contains(c))
                .map(|c| {
                    let quoted = Self::quote_ident(c);
                    format!(
                        "(target.{0} <> source.{0} OR (target.{0} IS NULL AND source.{0} IS NOT NULL) OR (target.{0} IS NOT NULL AND source.{0} IS NULL))",
                        quoted
                    )
                })
                .collect();

            format!(
                r#"MERGE INTO {} WITH (TABLOCK) AS target
                   USING {} AS source
                   ON {}
                   WHEN MATCHED AND ({}) THEN UPDATE SET {}
                   WHEN NOT MATCHED THEN INSERT ({}) VALUES ({});"#,
                target_table,
                staging_table,
                join_condition.join(" AND "),
                change_detection.join(" OR "),
                update_cols.join(", "),
                col_str,
                source_cols.join(", ")
            )
        }
    }

    /// Insert rows using multi-statement batched INSERT.
    ///
    /// ODBC doesn't support TDS bulk insert, so we use batched INSERT statements.
    /// To reduce round-trip overhead, multiple INSERT statements are combined
    /// into a single execute call (separated by semicolons).
    ///
    /// Performance optimization: Instead of executing each INSERT individually,
    /// we batch STATEMENTS_PER_EXECUTE INSERT statements together, reducing
    /// network round-trips by ~10x.
    async fn insert_batch(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        rows: &[Vec<SqlValue>],
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let _lock = self.conn_mutex.lock().await;
        let conn = self.get_connection()?;
        let qualified_table = Self::qualify_table(schema, table);

        let col_list: Vec<String> = cols.iter().map(|c| Self::quote_ident(c)).collect();
        let col_str = col_list.join(", ");

        // Calculate batch size based on MSSQL limits
        let cols_per_row = cols.len();
        if cols_per_row == 0 {
            return Err(MigrateError::transfer(
                &qualified_table,
                "Cannot insert rows with zero columns".to_string(),
            ));
        }

        let max_rows_per_insert = (MAX_PARAMS_PER_BATCH / cols_per_row)
            .min(MAX_ROWS_PER_INSERT)
            .max(1);

        let mut total_inserted = 0u64;

        // Build individual INSERT statements
        let mut insert_statements: Vec<String> = Vec::new();
        let mut rows_in_current_batch = 0usize;

        for batch in rows.chunks(max_rows_per_insert) {
            // Build multi-row VALUES clause for this INSERT
            let mut value_groups = Vec::with_capacity(batch.len());
            for row in batch {
                let values: Vec<String> = row.iter().map(sql_value_to_sql_literal).collect();
                value_groups.push(format!("({})", values.join(", ")));
            }

            let sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                qualified_table,
                col_str,
                value_groups.join(", ")
            );
            insert_statements.push(sql);
            rows_in_current_batch += batch.len();

            // Execute when we have enough statements or this is the last batch
            if insert_statements.len() >= STATEMENTS_PER_EXECUTE {
                let batch_sql = insert_statements.join(";\n");
                conn.execute(&batch_sql, ()).map_err(|e| {
                    MigrateError::transfer(
                        &qualified_table,
                        format!("multi-statement INSERT ({} rows, {} statements): {}",
                               rows_in_current_batch, insert_statements.len(), e),
                    )
                })?;
                total_inserted += rows_in_current_batch as u64;
                insert_statements.clear();
                rows_in_current_batch = 0;
            }
        }

        // Execute any remaining statements
        if !insert_statements.is_empty() {
            let batch_sql = insert_statements.join(";\n");
            conn.execute(&batch_sql, ()).map_err(|e| {
                MigrateError::transfer(
                    &qualified_table,
                    format!("multi-statement INSERT ({} rows, {} statements): {}",
                           rows_in_current_batch, insert_statements.len(), e),
                )
            })?;
            total_inserted += rows_in_current_batch as u64;
        }

        debug!(
            "Inserted {} rows into {} via ODBC multi-statement INSERT",
            total_inserted, qualified_table
        );

        Ok(total_inserted)
    }
}

#[async_trait]
impl TargetPool for OdbcMssqlTargetPool {
    async fn create_schema(&self, schema: &str) -> Result<()> {
        let _lock = self.conn_mutex.lock().await;
        let quoted_schema = Self::quote_ident(schema);
        let sql = format!(
            "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{}') EXEC(N'CREATE SCHEMA {}')",
            schema.replace('\'', "''"),
            quoted_schema.replace('\'', "''")
        );
        self.execute(&sql)?;
        debug!("Created schema: {}", schema);
        Ok(())
    }

    async fn create_table(&self, table: &Table, target_schema: &str) -> Result<()> {
        let _lock = self.conn_mutex.lock().await;

        // Generate column definitions
        let col_defs: Vec<String> = table
            .columns
            .iter()
            .map(|c| {
                let target_type = if Self::is_mssql_type(&c.data_type) {
                    Self::format_mssql_type(&c.data_type, c.max_length, c.precision, c.scale)
                } else {
                    let mapping = postgres_to_mssql(&c.data_type, c.max_length, c.precision, c.scale);
                    if mapping.is_lossy {
                        if let Some(warning) = &mapping.warning {
                            warn!("Column {}.{}: {}", table.name, c.name, warning);
                        }
                    }
                    mapping.target_type
                };

                let null_clause = if c.is_nullable { "NULL" } else { "NOT NULL" };
                format!("{} {} {}", Self::quote_ident(&c.name), target_type, null_clause)
            })
            .collect();

        // Build PRIMARY KEY CLUSTERED constraint if table has a primary key
        let pk_constraint = if !table.primary_key.is_empty() {
            let pk_cols: Vec<String> = table
                .primary_key
                .iter()
                .map(|c| Self::quote_ident(c))
                .collect();
            let pk_name = format!("PK_{}_{}", target_schema, table.name);
            format!(
                ",\n    CONSTRAINT {} PRIMARY KEY CLUSTERED ({})",
                Self::quote_ident(&pk_name),
                pk_cols.join(", ")
            )
        } else {
            String::new()
        };

        let ddl = format!(
            "CREATE TABLE {} (\n    {}{}\n)",
            Self::qualify_table(target_schema, &table.name),
            col_defs.join(",\n    "),
            pk_constraint
        );

        self.execute(&ddl)?;
        debug!("Created table via ODBC: {}.{}", target_schema, table.name);
        Ok(())
    }

    async fn create_table_unlogged(&self, table: &Table, target_schema: &str) -> Result<()> {
        // MSSQL doesn't have UNLOGGED tables; use regular CREATE TABLE
        self.create_table(table, target_schema).await
    }

    async fn drop_table(&self, schema: &str, table: &str) -> Result<()> {
        let _lock = self.conn_mutex.lock().await;
        let sql = format!(
            "IF OBJECT_ID(N'{}.{}', 'U') IS NOT NULL DROP TABLE {}",
            schema.replace('\'', "''"),
            table.replace('\'', "''"),
            Self::qualify_table(schema, table)
        );
        self.execute(&sql)?;
        debug!("Dropped table via ODBC: {}.{}", schema, table);
        Ok(())
    }

    async fn truncate_table(&self, schema: &str, table: &str) -> Result<()> {
        let _lock = self.conn_mutex.lock().await;
        let sql = format!("TRUNCATE TABLE {}", Self::qualify_table(schema, table));
        self.execute(&sql)?;
        debug!("Truncated table via ODBC: {}.{}", schema, table);
        Ok(())
    }

    async fn table_exists(&self, schema: &str, table: &str) -> Result<bool> {
        let _lock = self.conn_mutex.lock().await;
        let sql = format!(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}'",
            schema.replace('\'', "''"),
            table.replace('\'', "''")
        );
        let count = self.query_scalar_i64(&sql)?;
        Ok(count > 0)
    }

    async fn create_primary_key(&self, table: &Table, target_schema: &str) -> Result<()> {
        if table.primary_key.is_empty() {
            return Ok(());
        }

        let _lock = self.conn_mutex.lock().await;
        let pk_name = format!("PK_{}_{}", target_schema, table.name);

        // Check if primary key already exists
        let check_sql = format!(
            r#"SELECT COUNT(*) FROM sys.key_constraints
               WHERE name = '{}' AND parent_object_id = OBJECT_ID(N'{}.{}')"#,
            pk_name.replace('\'', "''"),
            target_schema.replace('\'', "''"),
            table.name.replace('\'', "''")
        );
        if self.query_scalar_bool(&check_sql)? {
            debug!("Primary key already exists on {}.{}", target_schema, table.name);
            return Ok(());
        }

        let pk_cols: Vec<String> = table.primary_key.iter().map(|c| Self::quote_ident(c)).collect();
        let sql = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} PRIMARY KEY ({})",
            Self::qualify_table(target_schema, &table.name),
            Self::quote_ident(&pk_name),
            pk_cols.join(", ")
        );

        self.execute(&sql)?;
        debug!("Created primary key via ODBC on {}.{}", target_schema, table.name);
        Ok(())
    }

    async fn create_index(&self, table: &Table, idx: &Index, target_schema: &str) -> Result<()> {
        let _lock = self.conn_mutex.lock().await;
        let idx_cols: Vec<String> = idx.columns.iter().map(|c| Self::quote_ident(c)).collect();

        let unique = if idx.is_unique { "UNIQUE " } else { "" };
        let clustered = if idx.is_clustered {
            "CLUSTERED "
        } else {
            "NONCLUSTERED "
        };

        let sql = format!(
            "CREATE {}{}INDEX {} ON {} ({})",
            unique,
            clustered,
            Self::quote_ident(&idx.name),
            Self::qualify_table(target_schema, &table.name),
            idx_cols.join(", ")
        );

        self.execute(&sql)?;
        debug!("Created index via ODBC: {} on {}.{}", idx.name, target_schema, table.name);
        Ok(())
    }

    async fn drop_non_pk_indexes(&self, schema: &str, table: &str) -> Result<Vec<String>> {
        let _lock = self.conn_mutex.lock().await;
        let conn = self.get_connection()?;

        let sql = format!(
            r#"SELECT i.name
               FROM sys.indexes i
               JOIN sys.tables t ON i.object_id = t.object_id
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = '{}' AND t.name = '{}'
                 AND i.is_primary_key = 0 AND i.type > 0"#,
            schema.replace('\'', "''"),
            table.replace('\'', "''")
        );

        let mut dropped_indexes = Vec::new();

        if let Some(mut cursor) = conn.execute(&sql, ()).map_err(|e| {
            MigrateError::transfer(format!("{}.{}", schema, table), format!("query indexes: {}", e))
        })? {
            let mut buffers = odbc_api::buffers::TextRowSet::for_cursor(100, &mut cursor, Some(256))
                .map_err(|e| {
                    MigrateError::transfer(
                        format!("{}.{}", schema, table),
                        format!("create buffer: {}", e),
                    )
                })?;

            let mut row_cursor = cursor.bind_buffer(&mut buffers).map_err(|e| {
                MigrateError::transfer(format!("{}.{}", schema, table), format!("bind buffer: {}", e))
            })?;

            while let Some(batch) = row_cursor.fetch().map_err(|e| {
                MigrateError::transfer(format!("{}.{}", schema, table), format!("fetch: {}", e))
            })? {
                for row_idx in 0..batch.num_rows() {
                    if let Some(bytes) = batch.at(0, row_idx) {
                        let index_name = String::from_utf8_lossy(bytes).to_string();
                        dropped_indexes.push(index_name);
                    }
                }
            }
        }

        // Drop the indexes in a separate connection
        for index_name in &dropped_indexes {
            let drop_sql = format!(
                "DROP INDEX {} ON {}",
                Self::quote_ident(index_name),
                Self::qualify_table(schema, table)
            );
            self.execute(&drop_sql)?;
        }

        debug!("Dropped {} indexes via ODBC on {}.{}", dropped_indexes.len(), schema, table);
        Ok(dropped_indexes)
    }

    async fn create_foreign_key(
        &self,
        table: &Table,
        fk: &ForeignKey,
        target_schema: &str,
    ) -> Result<()> {
        let _lock = self.conn_mutex.lock().await;
        let fk_cols: Vec<String> = fk.columns.iter().map(|c| Self::quote_ident(c)).collect();
        let ref_cols: Vec<String> = fk.ref_columns.iter().map(|c| Self::quote_ident(c)).collect();

        let sql = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({}) ON DELETE {} ON UPDATE {}",
            Self::qualify_table(target_schema, &table.name),
            Self::quote_ident(&fk.name),
            fk_cols.join(", "),
            Self::qualify_table(&fk.ref_schema, &fk.ref_table),
            ref_cols.join(", "),
            fk.on_delete.replace('_', " "),
            fk.on_update.replace('_', " ")
        );

        self.execute(&sql)?;
        debug!("Created foreign key via ODBC: {} on {}.{}", fk.name, target_schema, table.name);
        Ok(())
    }

    async fn create_check_constraint(
        &self,
        table: &Table,
        chk: &CheckConstraint,
        target_schema: &str,
    ) -> Result<()> {
        let _lock = self.conn_mutex.lock().await;

        let sql = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} {}",
            Self::qualify_table(target_schema, &table.name),
            Self::quote_ident(&chk.name),
            chk.definition
        );

        self.execute(&sql)?;
        debug!(
            "Created check constraint via ODBC: {} on {}.{}",
            chk.name, target_schema, table.name
        );
        Ok(())
    }

    async fn has_primary_key(&self, schema: &str, table: &str) -> Result<bool> {
        let _lock = self.conn_mutex.lock().await;
        let sql = format!(
            r#"SELECT COUNT(*)
               FROM sys.indexes i
               JOIN sys.tables t ON i.object_id = t.object_id
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = '{}' AND t.name = '{}' AND i.is_primary_key = 1"#,
            schema.replace('\'', "''"),
            table.replace('\'', "''")
        );
        self.query_scalar_bool(&sql)
    }

    async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        let _lock = self.conn_mutex.lock().await;
        let sql = format!(
            "SELECT CAST(COUNT(*) AS BIGINT) FROM {} WITH (NOLOCK)",
            Self::qualify_table(schema, table)
        );
        self.query_scalar_i64(&sql)
    }

    async fn reset_sequence(&self, _schema: &str, _table: &Table) -> Result<()> {
        // MSSQL identity columns auto-increment; no sequence reset needed
        Ok(())
    }

    async fn set_table_logged(&self, _schema: &str, _table: &str) -> Result<()> {
        // MSSQL doesn't have UNLOGGED tables
        Ok(())
    }

    async fn set_table_unlogged(&self, _schema: &str, _table: &str) -> Result<()> {
        // MSSQL doesn't have UNLOGGED tables
        Ok(())
    }

    async fn write_chunk(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        rows: Vec<Vec<SqlValue>>,
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        self.insert_batch(schema, table, cols, &rows).await
    }

    async fn upsert_chunk(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        pk_cols: &[String],
        rows: Vec<Vec<SqlValue>>,
        writer_id: usize,
        partition_id: Option<i32>,
    ) -> Result<u64> {
        use std::time::Instant;

        if rows.is_empty() {
            return Ok(0);
        }

        if pk_cols.is_empty() {
            return Err(MigrateError::NoPrimaryKey(table.to_string()));
        }

        let row_count = rows.len() as u64;
        let chunk_start = Instant::now();
        let qualified_target = Self::qualify_table(schema, table);

        let _lock = self.conn_mutex.lock().await;

        // 1. Create or reuse staging table
        let staging_start = Instant::now();
        let staging_table = self.ensure_staging_table(schema, table, writer_id, partition_id)?;
        let staging_time = staging_start.elapsed();

        // 2. Insert rows into staging table using multi-statement batch INSERT
        let insert_start = Instant::now();
        let conn = self.get_connection()?;

        let col_list: Vec<String> = cols.iter().map(|c| Self::quote_ident(c)).collect();
        let col_str = col_list.join(", ");

        let cols_per_row = cols.len();
        let max_rows_per_insert = (MAX_PARAMS_PER_BATCH / cols_per_row)
            .min(MAX_ROWS_PER_INSERT)
            .max(1);

        // Build individual INSERT statements and batch them
        let mut insert_statements: Vec<String> = Vec::new();

        for batch in rows.chunks(max_rows_per_insert) {
            let mut value_groups = Vec::with_capacity(batch.len());

            for row in batch {
                let values: Vec<String> = row.iter().map(sql_value_to_sql_literal).collect();
                value_groups.push(format!("({})", values.join(", ")));
            }

            let sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                staging_table,
                col_str,
                value_groups.join(", ")
            );
            insert_statements.push(sql);

            // Execute when we have enough statements
            if insert_statements.len() >= STATEMENTS_PER_EXECUTE {
                let batch_sql = insert_statements.join(";\n");
                conn.execute(&batch_sql, ()).map_err(|e| {
                    MigrateError::transfer(&staging_table, format!("staging multi-INSERT: {}", e))
                })?;
                insert_statements.clear();
            }
        }

        // Execute remaining statements
        if !insert_statements.is_empty() {
            let batch_sql = insert_statements.join(";\n");
            conn.execute(&batch_sql, ()).map_err(|e| {
                MigrateError::transfer(&staging_table, format!("staging multi-INSERT: {}", e))
            })?;
        }
        let insert_time = insert_start.elapsed();

        // 3. Build and execute MERGE from staging to target
        let merge_start = Instant::now();

        // Check if target has identity column
        let has_identity = self.has_identity_column(schema, table)?;

        // Build MERGE SQL
        let merge_sql = Self::build_staging_merge_sql(&qualified_target, &staging_table, cols, pk_cols);

        // Wrap with IDENTITY_INSERT if needed
        let batch_sql = if has_identity {
            format!(
                "SET IDENTITY_INSERT {} ON; {} SET IDENTITY_INSERT {} OFF;",
                qualified_target, merge_sql, qualified_target
            )
        } else {
            merge_sql
        };

        // Execute MERGE with deadlock retry
        let mut retries = 0;
        loop {
            match conn.execute(&batch_sql, ()) {
                Ok(_) => break,
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("1205") && retries < DEADLOCK_MAX_RETRIES {
                        retries += 1;
                        warn!(
                            "Deadlock detected during MERGE to {}, retry {}/{}",
                            qualified_target, retries, DEADLOCK_MAX_RETRIES
                        );
                        std::thread::sleep(std::time::Duration::from_millis(
                            DEADLOCK_RETRY_DELAY_MS * retries as u64,
                        ));
                        continue;
                    }
                    return Err(MigrateError::transfer(
                        &qualified_target,
                        format!("MERGE failed: {}", e),
                    ));
                }
            }
        }

        let merge_time = merge_start.elapsed();
        let total_time = chunk_start.elapsed();

        if row_count >= 1000 {
            debug!(
                "{}.{}: ODBC upsert {} rows - staging: {:?}, insert: {:?}, merge: {:?}, total: {:?}",
                schema, table, row_count, staging_time, insert_time, merge_time, total_time
            );
        }

        Ok(row_count)
    }

    fn db_type(&self) -> &str {
        "mssql"
    }

    async fn close(&self) {
        // ODBC connections are closed when dropped
    }
}

/// Convert SqlValue to SQL literal string for INSERT statements.
///
/// This function escapes values appropriately for embedding in SQL.
/// Note: This is less secure than parameterized queries but necessary for
/// ODBC batch INSERT since ODBC parameter binding has limitations.
fn sql_value_to_sql_literal(value: &SqlValue) -> String {
    match value {
        SqlValue::Null(_) => "NULL".to_string(),
        SqlValue::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        SqlValue::I16(n) => n.to_string(),
        SqlValue::I32(n) => n.to_string(),
        SqlValue::I64(n) => n.to_string(),
        SqlValue::F32(f) => {
            if f.is_nan() || f.is_infinite() {
                "NULL".to_string()
            } else {
                f.to_string()
            }
        }
        SqlValue::F64(f) => {
            if f.is_nan() || f.is_infinite() {
                "NULL".to_string()
            } else {
                f.to_string()
            }
        }
        SqlValue::String(s) => format!("N'{}'", s.replace('\'', "''")),
        SqlValue::Bytes(b) => format!("0x{}", hex::encode(b)),
        SqlValue::Uuid(u) => format!("'{}'", u),
        SqlValue::Decimal(d) => d.to_string(),
        SqlValue::DateTime(dt) => format!("'{}'", dt.format("%Y-%m-%d %H:%M:%S%.6f")),
        SqlValue::DateTimeOffset(dto) => format!("'{}'", dto.format("%Y-%m-%d %H:%M:%S%.6f %:z")),
        SqlValue::Date(d) => format!("'{}'", d.format("%Y-%m-%d")),
        SqlValue::Time(t) => format!("'{}'", t.format("%H:%M:%S%.6f")),
    }
}

/// Check if the ODBC driver is installed and available.
///
/// Returns Ok(()) if ODBC is available, or an error with installation instructions.
pub fn check_odbc_available() -> Result<()> {
    Environment::new().map_err(|e| {
        MigrateError::Config(format!(
            "ODBC driver not found: {}.\n\n\
             Kerberos authentication requires the Microsoft ODBC Driver for SQL Server.\n\
             Please install it:\n\
             - Windows: Download from https://aka.ms/msodbcsql\n\
             - Linux: apt install msodbcsql18 (Debian/Ubuntu) or yum install msodbcsql18 (RHEL/CentOS)\n\
             - macOS: brew tap microsoft/mssql-release && brew install msodbcsql18\n\n\
             After installation, you may need to set LIBRARY_PATH on macOS:\n\
             export LIBRARY_PATH=\"/opt/homebrew/opt/unixodbc/lib:$LIBRARY_PATH\"",
            e
        ))
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::target::SqlNullType;

    #[test]
    fn test_sql_value_to_sql_literal_null() {
        assert_eq!(sql_value_to_sql_literal(&SqlValue::Null(SqlNullType::I32)), "NULL");
    }

    #[test]
    fn test_sql_value_to_sql_literal_bool() {
        assert_eq!(sql_value_to_sql_literal(&SqlValue::Bool(true)), "1");
        assert_eq!(sql_value_to_sql_literal(&SqlValue::Bool(false)), "0");
    }

    #[test]
    fn test_sql_value_to_sql_literal_numbers() {
        assert_eq!(sql_value_to_sql_literal(&SqlValue::I16(42)), "42");
        assert_eq!(sql_value_to_sql_literal(&SqlValue::I32(-100)), "-100");
        assert_eq!(sql_value_to_sql_literal(&SqlValue::I64(9999999999)), "9999999999");
    }

    #[test]
    fn test_sql_value_to_sql_literal_string_escaping() {
        assert_eq!(
            sql_value_to_sql_literal(&SqlValue::String("hello".to_string())),
            "N'hello'"
        );
        assert_eq!(
            sql_value_to_sql_literal(&SqlValue::String("it's".to_string())),
            "N'it''s'"
        );
        assert_eq!(
            sql_value_to_sql_literal(&SqlValue::String("a'b'c".to_string())),
            "N'a''b''c'"
        );
    }

    #[test]
    fn test_sql_value_to_sql_literal_nan_infinity() {
        assert_eq!(sql_value_to_sql_literal(&SqlValue::F32(f32::NAN)), "NULL");
        assert_eq!(sql_value_to_sql_literal(&SqlValue::F64(f64::INFINITY)), "NULL");
        assert_eq!(sql_value_to_sql_literal(&SqlValue::F64(f64::NEG_INFINITY)), "NULL");
    }

    #[test]
    fn test_sql_value_to_sql_literal_bytes() {
        assert_eq!(
            sql_value_to_sql_literal(&SqlValue::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF])),
            "0xdeadbeef"
        );
    }

    #[test]
    fn test_sql_value_to_sql_literal_uuid() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(
            sql_value_to_sql_literal(&SqlValue::Uuid(uuid)),
            "'550e8400-e29b-41d4-a716-446655440000'"
        );
    }

    #[test]
    fn test_quote_ident() {
        assert_eq!(OdbcMssqlTargetPool::quote_ident("users"), "[users]");
        assert_eq!(OdbcMssqlTargetPool::quote_ident("user]name"), "[user]]name]");
    }

    #[test]
    fn test_qualify_table() {
        assert_eq!(
            OdbcMssqlTargetPool::qualify_table("dbo", "users"),
            "[dbo].[users]"
        );
    }

    #[test]
    fn test_build_staging_merge_sql() {
        let cols = vec!["id".to_string(), "name".to_string(), "value".to_string()];
        let pk_cols = vec!["id".to_string()];

        let sql = OdbcMssqlTargetPool::build_staging_merge_sql(
            "[dbo].[users]",
            "[dbo].[_staging_users_0]",
            &cols,
            &pk_cols,
        );

        assert!(sql.contains("MERGE INTO [dbo].[users] WITH (TABLOCK)"));
        assert!(sql.contains("USING [dbo].[_staging_users_0]"));
        assert!(sql.contains("target.[id] = source.[id]"));
        assert!(sql.contains("[name] = source.[name]"));
    }

    #[test]
    fn test_build_staging_merge_sql_pk_only() {
        let cols = vec!["id".to_string()];
        let pk_cols = vec!["id".to_string()];

        let sql = OdbcMssqlTargetPool::build_staging_merge_sql(
            "[dbo].[lookup]",
            "[dbo].[_staging_lookup_0]",
            &cols,
            &pk_cols,
        );

        // PK-only table should not have WHEN MATCHED
        assert!(!sql.contains("WHEN MATCHED"));
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"));
    }
}
