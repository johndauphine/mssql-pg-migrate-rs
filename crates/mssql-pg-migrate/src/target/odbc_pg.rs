//! ODBC-based PostgreSQL target for Kerberos and ODBC authentication.
//!
//! This module provides an ODBC-based implementation of `TargetPool` that supports:
//! - Kerberos/GSSAPI Authentication (`auth: kerberos`)
//! - ODBC Authentication with username/password (`auth: odbc`)
//!
//! **Requirements:**
//! - The `kerberos` feature must be enabled
//! - PostgreSQL ODBC Driver (psqlODBC) must be installed:
//!   - Windows: Download from https://www.postgresql.org/ftp/odbc/versions/
//!   - Linux: `apt install odbc-postgresql` or `yum install postgresql-odbc`
//!   - macOS: `brew install psqlodbc`
//!
//! **Performance Notes:**
//! ODBC does not support the PostgreSQL COPY protocol, so this implementation uses
//! batched INSERT statements. While slower than COPY (~10-30K rows/sec vs ~100K+ rows/sec),
//! it still provides reasonable performance for most workloads.

use crate::config::TargetConfig;
use crate::error::{MigrateError, Result};
use crate::source::{CheckConstraint, ForeignKey, Index, Table};
use crate::target::{SqlValue, TargetPool};
use crate::typemap::mssql_to_postgres;
use async_trait::async_trait;
use odbc_api::{ConnectionOptions, Cursor, Environment, ResultSetMetadata};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};

/// Maximum rows per INSERT VALUES clause (PostgreSQL doesn't have a hard limit,
/// but we batch for memory efficiency).
const MAX_ROWS_PER_INSERT: usize = 1000;

/// Maximum number of parameters per query (avoid excessive memory usage).
const MAX_PARAMS_PER_BATCH: usize = 32000;

/// Escape a SQL string literal value to prevent SQL injection.
/// Doubles single quotes: `O'Brien` -> `O''Brien`
fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// Escape a PostgreSQL identifier for use in quoted notation.
/// Doubles double quotes: `Table"Name` -> `Table""Name`
#[allow(dead_code)]
fn escape_pg_ident(s: &str) -> String {
    s.replace('"', "\"\"")
}

/// Validate that a PostgreSQL data type is safe to interpolate into DDL.
/// Only allows characters commonly used in PostgreSQL type names.
fn validate_data_type(data_type: &str) -> Result<&str> {
    // Allow alphanumerics, space, underscore, parentheses, commas, brackets, quotes
    if data_type.chars().all(|c| {
        c.is_ascii_alphanumeric() || matches!(c, ' ' | '_' | '(' | ')' | ',' | '[' | ']' | '"')
    }) {
        Ok(data_type)
    } else {
        Err(MigrateError::transfer(
            "data type validation",
            format!("unsafe or unsupported PostgreSQL data type '{}'", data_type),
        ))
    }
}

/// ODBC-based PostgreSQL target pool.
pub struct OdbcPgTargetPool {
    env: Arc<Environment>,
    connection_string: String,
    #[allow(dead_code)]
    config: TargetConfig,
    /// Mutex to serialize ODBC operations (ODBC is not fully thread-safe)
    conn_mutex: Mutex<()>,
}

impl OdbcPgTargetPool {
    /// Create a new ODBC-based PostgreSQL target pool.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The ODBC environment cannot be created (driver not installed)
    /// - The connection to the database fails
    pub async fn new(config: &TargetConfig, _max_conns: u32) -> Result<Self> {
        use crate::config::AuthMethod;

        // Create ODBC environment
        let env = Environment::new().map_err(|e| {
            MigrateError::pool_msg(
                format!(
                    "Failed to create ODBC environment: {}.\n\n\
                     ODBC authentication requires the PostgreSQL ODBC Driver (psqlODBC).\n\
                     Please install it:\n\
                     - Windows: Download from postgresql.org/ftp/odbc/versions\n\
                     - Linux: apt install odbc-postgresql (or yum install postgresql-odbc)\n\
                     - macOS: brew install psqlodbc",
                    e
                ),
                "ODBC connection",
            )
        })?;

        // Build connection string based on auth method
        let (connection_string, auth_desc) = match config.auth {
            AuthMethod::Kerberos => {
                // Kerberos: use GSSAPI authentication
                let conn_str = format!(
                    "Driver={{PostgreSQL Unicode}};\
                     Server={};\
                     Port={};\
                     Database={};\
                     UseKerberos=1;\
                     SSLMode={};",
                    config.host,
                    config.port,
                    config.database,
                    &config.ssl_mode,
                );
                (conn_str, "Kerberos")
            }
            AuthMethod::Odbc | AuthMethod::Native | AuthMethod::SqlServer => {
                // Username/password auth via ODBC
                let conn_str = format!(
                    "Driver={{PostgreSQL Unicode}};\
                     Server={};\
                     Port={};\
                     Database={};\
                     Uid={};\
                     Pwd={};\
                     SSLMode={};",
                    config.host,
                    config.port,
                    config.database,
                    config.user,
                    config.password,
                    &config.ssl_mode,
                );
                (conn_str, "ODBC")
            }
        };

        debug!(
            "PostgreSQL ODBC target connection string (credentials hidden): Driver={{PostgreSQL Unicode}};Server={};Port={};Database={};Auth={};...",
            config.host, config.port, config.database, auth_desc
        );

        // Test connection
        {
            let conn = env
                .connect_with_connection_string(&connection_string, ConnectionOptions::default())
                .map_err(|e| {
                    let help_msg = if config.auth == AuthMethod::Kerberos {
                        "Ensure you have a valid Kerberos ticket:\n\
                         - Linux/macOS: Run 'kinit user@REALM' before running the migration"
                    } else {
                        "Check that the username and password are correct."
                    };
                    MigrateError::pool_msg(
                        format!(
                            "Failed to connect to PostgreSQL target via ODBC ({}): {}.\n\n{}",
                            auth_desc, e, help_msg
                        ),
                        "ODBC connection",
                    )
                })?;

            // Verify connection with a simple query
            let _ = conn.execute("SELECT 1", ());
        }

        info!(
            "Connected to PostgreSQL target via ODBC ({}): {}:{}/{}",
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

    /// Quote a PostgreSQL identifier with double quotes.
    fn quote_ident(name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    /// Qualify a table name with schema.
    fn qualify_table(schema: &str, table: &str) -> String {
        format!("{}.{}", Self::quote_ident(schema), Self::quote_ident(table))
    }

    /// Check if a data type is a PostgreSQL native type.
    fn is_pg_type(data_type: &str) -> bool {
        let lower = data_type.to_lowercase();
        matches!(
            lower.as_str(),
            "int2"
                | "int4"
                | "int8"
                | "smallint"
                | "integer"
                | "bigint"
                | "serial"
                | "bigserial"
                | "smallserial"
                | "bool"
                | "boolean"
                | "real"
                | "float4"
                | "double precision"
                | "float8"
                | "numeric"
                | "decimal"
                | "money"
                | "text"
                | "varchar"
                | "char"
                | "bpchar"
                | "bytea"
                | "uuid"
                | "json"
                | "jsonb"
                | "xml"
                | "timestamp"
                | "timestamptz"
                | "date"
                | "time"
                | "timetz"
                | "interval"
                | "inet"
                | "cidr"
                | "macaddr"
                | "point"
                | "line"
                | "lseg"
                | "box"
                | "path"
                | "polygon"
                | "circle"
        )
    }

    /// Get column definitions from the target table.
    fn get_column_definitions(&self, schema: &str, table: &str) -> Result<Vec<(String, String, bool)>> {
        let conn = self.get_connection()?;

        let sql = format!(
            r#"
            SELECT
                column_name,
                CASE
                    WHEN character_maximum_length IS NOT NULL THEN udt_name || '(' || character_maximum_length || ')'
                    WHEN numeric_precision IS NOT NULL AND numeric_scale IS NOT NULL AND udt_name IN ('numeric', 'decimal')
                        THEN udt_name || '(' || numeric_precision || ',' || numeric_scale || ')'
                    ELSE udt_name
                END AS data_type,
                CASE WHEN is_nullable = 'YES' THEN true ELSE false END AS is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{}'
                AND table_name = '{}'
            ORDER BY ordinal_position
            "#,
            escape_sql_string(schema),
            escape_sql_string(table)
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
                        .map(|b| {
                            let s = String::from_utf8_lossy(b);
                            s == "t" || s == "true" || s == "1"
                        })
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
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}'",
            escape_sql_string(schema),
            escape_sql_string(&staging_table_name)
        );

        let exists = self.query_scalar_bool(&check_sql)?;

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

            // Build CREATE TABLE statement with data type validation
            let col_defs: Vec<String> = columns
                .iter()
                .map(|(name, data_type, is_nullable)| -> Result<String> {
                    let safe_type = validate_data_type(data_type)?;
                    let null_str = if *is_nullable { "NULL" } else { "NOT NULL" };
                    Ok(format!("{} {} {}", Self::quote_ident(name), safe_type, null_str))
                })
                .collect::<Result<Vec<_>>>()?;

            // Create as UNLOGGED for performance
            let create_sql = format!(
                "CREATE UNLOGGED TABLE {} ({})",
                qualified_staging,
                col_defs.join(", ")
            );

            self.execute(&create_sql)?;
            debug!("Created staging table {}", qualified_staging);
        }

        Ok(qualified_staging)
    }

    /// Insert rows using batched INSERT statements.
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

        // Calculate batch size
        let cols_per_row = cols.len();
        if cols_per_row == 0 {
            return Err(MigrateError::transfer(
                &qualified_table,
                "Cannot insert rows with zero columns".to_string(),
            ));
        }

        let max_rows_per_batch = (MAX_PARAMS_PER_BATCH / cols_per_row)
            .min(MAX_ROWS_PER_INSERT)
            .max(1);

        let mut total_inserted = 0u64;

        // Process rows in batches
        for batch in rows.chunks(max_rows_per_batch) {
            // Build multi-row VALUES clause
            let mut value_groups = Vec::with_capacity(batch.len());

            for row in batch {
                let values: Vec<String> = row.iter().map(pg_sql_value_to_literal).collect();
                value_groups.push(format!("({})", values.join(", ")));
            }

            let sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                qualified_table,
                col_str,
                value_groups.join(", ")
            );

            conn.execute(&sql, ()).map_err(|e| {
                MigrateError::transfer(
                    &qualified_table,
                    format!("batch INSERT ({} rows): {}", batch.len(), e),
                )
            })?;

            total_inserted += batch.len() as u64;
        }

        debug!(
            "Inserted {} rows into {} via ODBC batched INSERT",
            total_inserted, qualified_table
        );

        Ok(total_inserted)
    }
}

#[async_trait]
impl TargetPool for OdbcPgTargetPool {
    async fn create_schema(&self, schema: &str) -> Result<()> {
        let _lock = self.conn_mutex.lock().await;
        let quoted_schema = Self::quote_ident(schema);
        let sql = format!("CREATE SCHEMA IF NOT EXISTS {}", quoted_schema);
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
                let target_type = if Self::is_pg_type(&c.data_type) {
                    c.data_type.clone()
                } else {
                    mssql_to_postgres(&c.data_type, c.max_length, c.precision, c.scale)
                };

                let null_clause = if c.is_nullable { "NULL" } else { "NOT NULL" };
                format!("{} {} {}", Self::quote_ident(&c.name), target_type, null_clause)
            })
            .collect();

        let ddl = format!(
            "CREATE TABLE {} (\n    {}\n)",
            Self::qualify_table(target_schema, &table.name),
            col_defs.join(",\n    ")
        );

        self.execute(&ddl)?;
        debug!("Created table via ODBC: {}.{}", target_schema, table.name);
        Ok(())
    }

    async fn create_table_unlogged(&self, table: &Table, target_schema: &str) -> Result<()> {
        let _lock = self.conn_mutex.lock().await;

        // Generate column definitions
        let col_defs: Vec<String> = table
            .columns
            .iter()
            .map(|c| {
                let target_type = if Self::is_pg_type(&c.data_type) {
                    c.data_type.clone()
                } else {
                    mssql_to_postgres(&c.data_type, c.max_length, c.precision, c.scale)
                };

                let null_clause = if c.is_nullable { "NULL" } else { "NOT NULL" };
                format!("{} {} {}", Self::quote_ident(&c.name), target_type, null_clause)
            })
            .collect();

        let ddl = format!(
            "CREATE UNLOGGED TABLE {} (\n    {}\n)",
            Self::qualify_table(target_schema, &table.name),
            col_defs.join(",\n    ")
        );

        self.execute(&ddl)?;
        debug!("Created UNLOGGED table via ODBC: {}.{}", target_schema, table.name);
        Ok(())
    }

    async fn drop_table(&self, schema: &str, table: &str) -> Result<()> {
        let _lock = self.conn_mutex.lock().await;
        let sql = format!("DROP TABLE IF EXISTS {} CASCADE", Self::qualify_table(schema, table));
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
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}'",
            escape_sql_string(schema),
            escape_sql_string(table)
        );
        let count = self.query_scalar_i64(&sql)?;
        Ok(count > 0)
    }

    async fn create_primary_key(&self, table: &Table, target_schema: &str) -> Result<()> {
        if table.primary_key.is_empty() {
            return Ok(());
        }

        let _lock = self.conn_mutex.lock().await;
        let pk_name = format!("pk_{}_{}", target_schema, table.name);

        // Check if primary key already exists
        let check_sql = format!(
            "SELECT COUNT(*) FROM pg_constraint WHERE conname = '{}' AND conrelid = '{}.{}'::regclass",
            escape_sql_string(&pk_name),
            escape_sql_string(target_schema),
            escape_sql_string(&table.name)
        );
        if self.query_scalar_bool(&check_sql).unwrap_or(false) {
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

        let mut sql = format!(
            "CREATE {}INDEX {} ON {} ({})",
            unique,
            Self::quote_ident(&idx.name),
            Self::qualify_table(target_schema, &table.name),
            idx_cols.join(", ")
        );

        // Add INCLUDE columns if present (PostgreSQL 11+)
        if !idx.include_cols.is_empty() {
            let include_cols: Vec<String> = idx.include_cols.iter().map(|c| Self::quote_ident(c)).collect();
            sql.push_str(&format!(" INCLUDE ({})", include_cols.join(", ")));
        }

        self.execute(&sql)?;
        debug!("Created index via ODBC: {} on {}.{}", idx.name, target_schema, table.name);
        Ok(())
    }

    async fn drop_non_pk_indexes(&self, schema: &str, table: &str) -> Result<Vec<String>> {
        let _lock = self.conn_mutex.lock().await;
        let conn = self.get_connection()?;

        let sql = format!(
            r#"
            SELECT i.relname
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = '{}'
              AND t.relname = '{}'
              AND NOT ix.indisprimary
            "#,
            escape_sql_string(schema),
            escape_sql_string(table)
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

        // Drop the indexes
        for index_name in &dropped_indexes {
            let drop_sql = format!(
                "DROP INDEX IF EXISTS {}.{}",
                Self::quote_ident(schema),
                Self::quote_ident(index_name)
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
            r#"
            SELECT COUNT(*)
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = '{}' AND c.relname = '{}' AND i.indisprimary
            "#,
            escape_sql_string(schema),
            escape_sql_string(table)
        );
        self.query_scalar_bool(&sql)
    }

    async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        let _lock = self.conn_mutex.lock().await;
        let sql = format!(
            "SELECT COUNT(*)::bigint FROM {}",
            Self::qualify_table(schema, table)
        );
        self.query_scalar_i64(&sql)
    }

    async fn reset_sequence(&self, schema: &str, table: &Table) -> Result<()> {
        // Find identity columns and reset their sequences
        for col in &table.columns {
            if col.is_identity {
                let _lock = self.conn_mutex.lock().await;
                let sql = format!(
                    "SELECT setval(pg_get_serial_sequence('{}.{}', '{}'), COALESCE((SELECT MAX({}) FROM {}), 1))",
                    escape_sql_string(schema),
                    escape_sql_string(&table.name),
                    escape_sql_string(&col.name),
                    Self::quote_ident(&col.name),
                    Self::qualify_table(schema, &table.name)
                );
                let _ = self.execute(&sql); // Ignore errors if sequence doesn't exist
            }
        }
        Ok(())
    }

    async fn set_table_logged(&self, schema: &str, table: &str) -> Result<()> {
        let _lock = self.conn_mutex.lock().await;
        let sql = format!("ALTER TABLE {} SET LOGGED", Self::qualify_table(schema, table));
        self.execute(&sql)?;
        debug!("Set table to LOGGED via ODBC: {}.{}", schema, table);
        Ok(())
    }

    async fn set_table_unlogged(&self, schema: &str, table: &str) -> Result<()> {
        let _lock = self.conn_mutex.lock().await;
        let sql = format!("ALTER TABLE {} SET UNLOGGED", Self::qualify_table(schema, table));
        self.execute(&sql)?;
        debug!("Set table to UNLOGGED via ODBC: {}.{}", schema, table);
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

        // 2. Insert rows into staging table
        let insert_start = Instant::now();
        let conn = self.get_connection()?;

        let col_list: Vec<String> = cols.iter().map(|c| Self::quote_ident(c)).collect();
        let col_str = col_list.join(", ");

        let cols_per_row = cols.len();
        let max_rows_per_batch = (MAX_PARAMS_PER_BATCH / cols_per_row)
            .min(MAX_ROWS_PER_INSERT)
            .max(1);

        for batch in rows.chunks(max_rows_per_batch) {
            let mut value_groups = Vec::with_capacity(batch.len());

            for row in batch {
                let values: Vec<String> = row.iter().map(pg_sql_value_to_literal).collect();
                value_groups.push(format!("({})", values.join(", ")));
            }

            let sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                staging_table,
                col_str,
                value_groups.join(", ")
            );

            conn.execute(&sql, ()).map_err(|e| {
                MigrateError::transfer(&staging_table, format!("staging INSERT: {}", e))
            })?;
        }
        let insert_time = insert_start.elapsed();

        // 3. Build and execute INSERT ON CONFLICT from staging to target
        let upsert_start = Instant::now();

        // Build the upsert SQL
        let pk_cols_quoted: Vec<String> = pk_cols.iter().map(|c| Self::quote_ident(c)).collect();
        let update_cols: Vec<String> = cols
            .iter()
            .filter(|c| !pk_cols.contains(c))
            .map(|c| format!("{} = EXCLUDED.{}", Self::quote_ident(c), Self::quote_ident(c)))
            .collect();

        let upsert_sql = if update_cols.is_empty() {
            // PK-only table - just insert and ignore conflicts
            format!(
                "INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT ({}) DO NOTHING",
                qualified_target,
                col_str,
                col_str,
                staging_table,
                pk_cols_quoted.join(", ")
            )
        } else {
            format!(
                "INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT ({}) DO UPDATE SET {}",
                qualified_target,
                col_str,
                col_str,
                staging_table,
                pk_cols_quoted.join(", "),
                update_cols.join(", ")
            )
        };

        conn.execute(&upsert_sql, ()).map_err(|e| {
            MigrateError::transfer(&qualified_target, format!("upsert failed: {}", e))
        })?;

        let upsert_time = upsert_start.elapsed();
        let total_time = chunk_start.elapsed();

        if row_count >= 1000 {
            debug!(
                "{}.{}: ODBC upsert {} rows - staging: {:?}, insert: {:?}, upsert: {:?}, total: {:?}",
                schema, table, row_count, staging_time, insert_time, upsert_time, total_time
            );
        }

        Ok(row_count)
    }

    fn db_type(&self) -> &str {
        "postgres"
    }

    async fn close(&self) {
        // ODBC connections are closed when dropped
    }
}

/// Convert SqlValue to PostgreSQL SQL literal string for INSERT statements.
fn pg_sql_value_to_literal(value: &SqlValue) -> String {
    match value {
        SqlValue::Null(_) => "NULL".to_string(),
        SqlValue::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        SqlValue::I16(n) => n.to_string(),
        SqlValue::I32(n) => n.to_string(),
        SqlValue::I64(n) => n.to_string(),
        SqlValue::F32(f) => {
            if f.is_nan() {
                "'NaN'::float4".to_string()
            } else if f.is_infinite() {
                if f.is_sign_positive() {
                    "'Infinity'::float4".to_string()
                } else {
                    "'-Infinity'::float4".to_string()
                }
            } else {
                f.to_string()
            }
        }
        SqlValue::F64(f) => {
            if f.is_nan() {
                "'NaN'::float8".to_string()
            } else if f.is_infinite() {
                if f.is_sign_positive() {
                    "'Infinity'::float8".to_string()
                } else {
                    "'-Infinity'::float8".to_string()
                }
            } else {
                f.to_string()
            }
        }
        SqlValue::String(s) => format!("'{}'", s.replace('\'', "''")),
        SqlValue::Bytes(b) => format!("'\\x{}'::bytea", hex::encode(b)),
        SqlValue::Uuid(u) => format!("'{}'::uuid", u),
        SqlValue::Decimal(d) => d.to_string(),
        SqlValue::DateTime(dt) => format!("'{}'::timestamp", dt.format("%Y-%m-%d %H:%M:%S%.6f")),
        SqlValue::DateTimeOffset(dto) => format!("'{}'::timestamptz", dto.format("%Y-%m-%d %H:%M:%S%.6f %:z")),
        SqlValue::Date(d) => format!("'{}'::date", d.format("%Y-%m-%d")),
        SqlValue::Time(t) => format!("'{}'::time", t.format("%H:%M:%S%.6f")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::target::SqlNullType;

    #[test]
    fn test_pg_sql_value_to_literal_null() {
        assert_eq!(pg_sql_value_to_literal(&SqlValue::Null(SqlNullType::I32)), "NULL");
    }

    #[test]
    fn test_pg_sql_value_to_literal_bool() {
        assert_eq!(pg_sql_value_to_literal(&SqlValue::Bool(true)), "true");
        assert_eq!(pg_sql_value_to_literal(&SqlValue::Bool(false)), "false");
    }

    #[test]
    fn test_pg_sql_value_to_literal_numbers() {
        assert_eq!(pg_sql_value_to_literal(&SqlValue::I16(42)), "42");
        assert_eq!(pg_sql_value_to_literal(&SqlValue::I32(-100)), "-100");
        assert_eq!(pg_sql_value_to_literal(&SqlValue::I64(9999999999)), "9999999999");
    }

    #[test]
    fn test_pg_sql_value_to_literal_string_escaping() {
        assert_eq!(
            pg_sql_value_to_literal(&SqlValue::String("hello".to_string())),
            "'hello'"
        );
        assert_eq!(
            pg_sql_value_to_literal(&SqlValue::String("it's".to_string())),
            "'it''s'"
        );
    }

    #[test]
    fn test_pg_sql_value_to_literal_nan_infinity() {
        assert_eq!(pg_sql_value_to_literal(&SqlValue::F32(f32::NAN)), "'NaN'::float4");
        assert_eq!(pg_sql_value_to_literal(&SqlValue::F64(f64::INFINITY)), "'Infinity'::float8");
        assert_eq!(pg_sql_value_to_literal(&SqlValue::F64(f64::NEG_INFINITY)), "'-Infinity'::float8");
    }

    #[test]
    fn test_pg_sql_value_to_literal_bytes() {
        assert_eq!(
            pg_sql_value_to_literal(&SqlValue::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF])),
            "'\\xdeadbeef'::bytea"
        );
    }

    #[test]
    fn test_pg_sql_value_to_literal_uuid() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(
            pg_sql_value_to_literal(&SqlValue::Uuid(uuid)),
            "'550e8400-e29b-41d4-a716-446655440000'::uuid"
        );
    }

    #[test]
    fn test_quote_ident() {
        assert_eq!(OdbcPgTargetPool::quote_ident("users"), "\"users\"");
        assert_eq!(OdbcPgTargetPool::quote_ident("user\"name"), "\"user\"\"name\"");
    }

    #[test]
    fn test_qualify_table() {
        assert_eq!(
            OdbcPgTargetPool::qualify_table("public", "users"),
            "\"public\".\"users\""
        );
    }
}
