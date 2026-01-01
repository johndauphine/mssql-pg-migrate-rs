//! ODBC-based MSSQL source for Kerberos authentication.
//!
//! This module provides an ODBC-based implementation of `SourcePool` that supports
//! Kerberos/Windows Integrated Authentication via the Microsoft ODBC Driver for SQL Server.
//!
//! **Requirements:**
//! - The `kerberos` feature must be enabled
//! - Microsoft ODBC Driver for SQL Server must be installed:
//!   - Windows: Download from Microsoft (uses SSPI automatically)
//!   - Linux: `apt install msodbcsql18` or `yum install msodbcsql18`
//!   - macOS: `brew install msodbcsql18`
//!
//! **Usage:**
//! On Linux/macOS, obtain a Kerberos ticket before running:
//! ```sh
//! kinit user@REALM
//! ```
//!
//! On Windows with domain-joined machines, authentication is automatic via SSPI.

use crate::config::SourceConfig;
use crate::error::{MigrateError, Result};
use crate::source::{
    CheckConstraint, Column, ForeignKey, Index, Partition, SourcePool, Table,
};
use crate::target::{SqlNullType, SqlValue};
use async_trait::async_trait;
use odbc_api::{buffers::TextRowSet, ConnectionOptions, Cursor, Environment, ResultSetMetadata};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};

/// ODBC-based MSSQL source pool for Kerberos authentication.
pub struct OdbcMssqlPool {
    env: Arc<Environment>,
    connection_string: String,
    #[allow(dead_code)]
    config: SourceConfig,
    /// Mutex to serialize ODBC operations (ODBC is not thread-safe)
    conn_mutex: Mutex<()>,
}

/// Escape a SQL string literal value to prevent SQL injection.
/// Doubles single quotes: `O'Brien` -> `O''Brien`
fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// Escape a SQL identifier for use in bracketed notation.
/// Doubles right brackets: `Table]Name` -> `Table]]Name`
fn escape_sql_ident(s: &str) -> String {
    s.replace(']', "]]")
}

impl OdbcMssqlPool {
    /// Create a new ODBC-based MSSQL source pool.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The ODBC environment cannot be created
    /// - The connection to the database fails
    /// - The ODBC driver is not installed
    pub async fn new(config: SourceConfig) -> Result<Self> {
        Self::with_max_connections(config, 8).await
    }

    /// Create a new ODBC-based MSSQL source pool with specified max connections.
    pub async fn with_max_connections(config: SourceConfig, _max_size: u32) -> Result<Self> {
        use crate::config::AuthMethod;

        // Create ODBC environment
        let env = Environment::new().map_err(|e| {
            MigrateError::pool_msg(
                format!(
                    "Failed to create ODBC environment: {}. \
                     Make sure the Microsoft ODBC Driver for SQL Server is installed. \
                     Windows: Download from Microsoft. \
                     Linux: apt install msodbcsql18. \
                     macOS: brew install msodbcsql18.",
                    e
                ),
                "ODBC connection"
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
            "ODBC connection string (credentials hidden): Driver={{ODBC Driver 18 for SQL Server}};Server={},{};Database={};Auth={};...",
            config.host, config.port, config.database, auth_desc
        );

        // Test connection - use a scope so conn is dropped before we move env
        {
            let conn = env
                .connect_with_connection_string(&connection_string, ConnectionOptions::default())
                .map_err(|e| {
                    let help_msg = if config.auth == AuthMethod::Kerberos {
                        "Ensure you have a valid Kerberos ticket (run 'kinit user@REALM' on Linux/macOS) \
                         or are on a domain-joined Windows machine."
                    } else {
                        "Check that the username and password are correct."
                    };
                    MigrateError::pool_msg(
                        format!(
                            "Failed to connect to MSSQL via ODBC ({}): {}. {}",
                            auth_desc, e, help_msg
                        ),
                        "ODBC connection"
                    )
                })?;

            // Verify connection with a simple query
            let _ = conn.execute("SELECT 1", ());
        }

        info!(
            "Connected to MSSQL via ODBC ({}): {}:{}/{}",
            auth_desc, config.host, config.port, config.database
        );

        Ok(Self {
            env: Arc::new(env),
            connection_string,
            config,
            conn_mutex: Mutex::new(()),
        })
    }

    /// Get a new ODBC connection.
    fn get_connection(&self) -> Result<odbc_api::Connection<'_>> {
        self.env
            .connect_with_connection_string(&self.connection_string, ConnectionOptions::default())
            .map_err(|e| MigrateError::pool_msg(format!("ODBC connection failed: {}", e), "getting ODBC connection"))
    }

    /// Execute a query and return rows as Vec<Vec<String>>.
    fn execute_query(&self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        let conn = self.get_connection()?;

        let mut rows = Vec::new();

        if let Some(mut cursor) = conn.execute(sql, ()).map_err(|e| {
            MigrateError::SchemaExtraction(format!("ODBC query failed: {} - SQL: {}", e, sql))
        })? {
            // Determine number of columns
            let num_cols = cursor.num_result_cols().map_err(|e| {
                MigrateError::SchemaExtraction(format!("Failed to get column count: {}", e))
            })? as usize;

            // Create a buffer for fetching rows
            let mut buffers = TextRowSet::for_cursor(1000, &mut cursor, Some(4096)).map_err(|e| {
                MigrateError::SchemaExtraction(format!("Failed to create row buffer: {}", e))
            })?;

            let mut row_cursor = cursor.bind_buffer(&mut buffers).map_err(|e| {
                MigrateError::SchemaExtraction(format!("Failed to bind buffer: {}", e))
            })?;

            while let Some(batch) = row_cursor.fetch().map_err(|e| {
                MigrateError::SchemaExtraction(format!("Failed to fetch rows: {}", e))
            })? {
                for row_idx in 0..batch.num_rows() {
                    let mut row = Vec::with_capacity(num_cols);
                    for col_idx in 0..num_cols {
                        let value = batch
                            .at(col_idx, row_idx)
                            .map(|bytes| String::from_utf8_lossy(bytes).to_string());
                        row.push(value);
                    }
                    rows.push(row);
                }
            }
        }

        Ok(rows)
    }

    /// Query rows with pre-computed column types for O(1) lookup per value.
    /// This is the high-performance version used by the parallel transfer engine.
    pub fn query_rows_fast_sync(
        &self,
        sql: &str,
        col_types: &[String],
    ) -> Result<Vec<Vec<SqlValue>>> {
        let conn = self.get_connection()?;

        let mut result = Vec::new();

        if let Some(mut cursor) = conn.execute(sql, ()).map_err(|e| {
            MigrateError::transfer("ODBC query", format!("Query failed: {} - SQL: {}", e, sql))
        })? {
            // Determine number of columns from result set
            let num_cols: usize = cursor.num_result_cols().map_err(|e| {
                MigrateError::transfer("ODBC query", format!("Failed to get column count: {}", e))
            })? as usize;

            // Create a buffer for fetching rows - larger buffer for data transfer
            let mut buffers =
                TextRowSet::for_cursor(5000, &mut cursor, Some(65536)).map_err(|e| {
                    MigrateError::transfer("ODBC query", format!("Failed to create row buffer: {}", e))
                })?;

            let mut row_cursor = cursor.bind_buffer(&mut buffers).map_err(|e| {
                MigrateError::transfer("ODBC query", format!("Failed to bind buffer: {}", e))
            })?;

            while let Some(batch) = row_cursor.fetch().map_err(|e| {
                MigrateError::transfer("ODBC query", format!("Failed to fetch rows: {}", e))
            })? {
                for row_idx in 0..batch.num_rows() {
                    let mut row = Vec::with_capacity(num_cols);
                    for col_idx in 0..num_cols {
                        let col_type = col_types.get(col_idx).map(|s| s.as_str()).unwrap_or("varchar");
                        let text_value = batch
                            .at(col_idx, row_idx)
                            .map(|bytes| String::from_utf8_lossy(bytes).to_string());

                        let value = convert_text_to_sqlvalue(text_value, col_type);
                        row.push(value);
                    }
                    result.push(row);
                }
            }
        }

        Ok(result)
    }

    /// Get the maximum primary key value for a table.
    pub fn get_max_pk_sync(&self, schema: &str, table: &str, pk_col: &str) -> Result<i64> {
        let pk_ident = escape_sql_ident(pk_col);
        let schema_ident = escape_sql_ident(schema);
        let table_ident = escape_sql_ident(table);

        let sql = format!(
            "SELECT CAST(MAX([{pk_ident}]) AS BIGINT) FROM [{schema_ident}].[{table_ident}] WITH (NOLOCK)"
        );

        let rows = self.execute_query(&sql)?;

        Ok(rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_ref())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0))
    }

    /// Async version of query_rows_fast.
    /// Query rows with pre-computed column types for O(1) lookup per value.
    pub async fn query_rows_fast(
        &self,
        sql: &str,
        _columns: &[String],
        col_types: &[String],
    ) -> Result<Vec<Vec<SqlValue>>> {
        let _lock = self.conn_mutex.lock().await;
        self.query_rows_fast_sync(sql, col_types)
    }

    /// Async version of get_max_pk.
    /// Get the maximum primary key value for a table.
    pub async fn get_max_pk(&self, schema: &str, table: &str, pk_col: &str) -> Result<i64> {
        let _lock = self.conn_mutex.lock().await;
        self.get_max_pk_sync(schema, table, pk_col)
    }

    fn load_columns_sync(&self, schema: &str, table_name: &str, table: &mut Table) -> Result<()> {
        // Use escape_sql_ident for bracketed identifiers, escape_sql_string for string literals
        let schema_ident = escape_sql_ident(schema);
        let table_ident = escape_sql_ident(table_name);
        let schema_lit = escape_sql_string(schema);
        let table_lit = escape_sql_string(table_name);

        let sql = format!(
            r#"
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                CAST(ISNULL(CHARACTER_MAXIMUM_LENGTH, 0) AS INT),
                CAST(ISNULL(NUMERIC_PRECISION, 0) AS INT),
                CAST(ISNULL(NUMERIC_SCALE, 0) AS INT),
                CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END,
                COLUMNPROPERTY(OBJECT_ID('[{schema_ident}].[{table_ident}]'), COLUMN_NAME, 'IsIdentity'),
                ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema_lit}'
                AND TABLE_NAME = '{table_lit}'
            ORDER BY ORDINAL_POSITION
            "#
        );

        let rows = self.execute_query(&sql)?;

        for row in rows {
            let name = row.get(0).and_then(|v| v.clone()).unwrap_or_default();
            let data_type = row.get(1).and_then(|v| v.clone()).unwrap_or_default();
            let max_length: i32 = row
                .get(2)
                .and_then(|v| v.as_ref())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let precision: i32 = row
                .get(3)
                .and_then(|v| v.as_ref())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let scale: i32 = row
                .get(4)
                .and_then(|v| v.as_ref())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let is_nullable: bool = row
                .get(5)
                .and_then(|v| v.as_ref())
                .map(|s| s == "1")
                .unwrap_or(false);
            let is_identity: bool = row
                .get(6)
                .and_then(|v| v.as_ref())
                .map(|s| s == "1")
                .unwrap_or(false);
            let ordinal_pos: i32 = row
                .get(7)
                .and_then(|v| v.as_ref())
                .and_then(|s| s.parse().ok())
                .unwrap_or(table.columns.len() as i32 + 1);

            table.columns.push(Column {
                name,
                data_type,
                max_length,
                precision,
                scale,
                is_nullable,
                is_identity,
                ordinal_pos,
            });
        }

        Ok(())
    }

    fn load_primary_key_sync(
        &self,
        schema: &str,
        table_name: &str,
        table: &mut Table,
    ) -> Result<()> {
        let schema_lit = escape_sql_string(schema);
        let table_lit = escape_sql_string(table_name);

        let sql = format!(
            r#"
            SELECT c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
                ON tc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = c.TABLE_SCHEMA
                AND tc.TABLE_NAME = c.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND tc.TABLE_SCHEMA = '{schema_lit}'
                AND tc.TABLE_NAME = '{table_lit}'
            ORDER BY c.ORDINAL_POSITION
            "#
        );

        let rows = self.execute_query(&sql)?;

        for row in rows {
            if let Some(col_name) = row.get(0).and_then(|v| v.clone()) {
                table.primary_key.push(col_name.clone());
                // Also add to pk_columns
                if let Some(col) = table.columns.iter().find(|c| c.name == col_name) {
                    table.pk_columns.push(col.clone());
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl SourcePool for OdbcMssqlPool {
    async fn extract_schema(&self, schema: &str) -> Result<Vec<Table>> {
        let _lock = self.conn_mutex.lock().await;

        let schema_lit = escape_sql_string(schema);

        // Query to get all tables with their row counts
        let sql = format!(
            r#"
            SELECT
                t.TABLE_NAME,
                ISNULL(p.rows, 0) as row_count
            FROM INFORMATION_SCHEMA.TABLES t
            LEFT JOIN sys.partitions p ON p.object_id = OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME)
                AND p.index_id IN (0, 1)
            WHERE t.TABLE_SCHEMA = '{schema_lit}'
                AND t.TABLE_TYPE = 'BASE TABLE'
            ORDER BY t.TABLE_NAME
            "#
        );

        let table_rows = self.execute_query(&sql)?;
        let mut tables = Vec::new();

        for row in table_rows {
            let table_name = row.get(0).and_then(|v| v.clone()).unwrap_or_default();
            let row_count: i64 = row
                .get(1)
                .and_then(|v| v.as_ref())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            let mut table = Table {
                schema: schema.to_string(),
                name: table_name.clone(),
                columns: Vec::new(),
                primary_key: Vec::new(),
                pk_columns: Vec::new(),
                row_count,
                estimated_row_size: 0,
                indexes: Vec::new(),
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
            };

            // Load columns for this table
            self.load_columns_sync(schema, &table_name, &mut table)?;

            // Load primary key
            self.load_primary_key_sync(schema, &table_name, &mut table)?;

            // Estimate row size (simple heuristic: sum of column sizes)
            table.estimated_row_size = table
                .columns
                .iter()
                .map(|c| match c.data_type.as_str() {
                    "int" => 4,
                    "bigint" => 8,
                    "smallint" => 2,
                    "tinyint" => 1,
                    "bit" => 1,
                    "float" => 8,
                    "real" => 4,
                    "datetime" | "datetime2" => 8,
                    "date" => 3,
                    "time" => 5,
                    "uniqueidentifier" => 16,
                    "varchar" | "nvarchar" | "char" | "nchar" => {
                        if c.max_length == -1 {
                            100
                        } else {
                            c.max_length.min(100) as i64
                        }
                    }
                    _ => 8, // default
                })
                .sum();

            tables.push(table);
        }

        info!(
            "Extracted {} tables from schema '{}' via ODBC",
            tables.len(),
            schema
        );
        Ok(tables)
    }

    async fn get_partition_boundaries(
        &self,
        table: &Table,
        num_partitions: usize,
    ) -> Result<Vec<Partition>> {
        let _lock = self.conn_mutex.lock().await;

        if table.primary_key.is_empty() {
            return Err(MigrateError::NoPrimaryKey(table.full_name()));
        }

        let pk_col = &table.primary_key[0];
        let pk_ident = escape_sql_ident(pk_col);
        let schema_ident = escape_sql_ident(&table.schema);
        let table_ident = escape_sql_ident(&table.name);

        // Get min and max PK values
        let sql = format!(
            "SELECT MIN([{pk_ident}]), MAX([{pk_ident}]) FROM [{schema_ident}].[{table_ident}]"
        );

        let rows = self.execute_query(&sql)?;

        if rows.is_empty() {
            return Ok(vec![Partition {
                table_name: table.full_name(),
                partition_id: 0,
                min_pk: None,
                max_pk: None,
                start_row: 0,
                end_row: 0,
                row_count: 0,
            }]);
        }

        let min_pk: i64 = rows[0]
            .get(0)
            .and_then(|v| v.as_ref())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let max_pk: i64 = rows[0]
            .get(1)
            .and_then(|v| v.as_ref())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let range = max_pk - min_pk;
        let partition_size = range / num_partitions as i64;
        let rows_per_partition = table.row_count / num_partitions as i64;

        let mut partitions = Vec::with_capacity(num_partitions);
        for i in 0..num_partitions {
            let start = min_pk + (i as i64 * partition_size);
            let end = if i == num_partitions - 1 {
                max_pk
            } else {
                min_pk + ((i + 1) as i64 * partition_size) - 1
            };

            partitions.push(Partition {
                table_name: table.full_name(),
                partition_id: i as i32,
                min_pk: Some(start),
                max_pk: Some(end),
                start_row: i as i64 * rows_per_partition,
                end_row: if i == num_partitions - 1 {
                    table.row_count
                } else {
                    (i + 1) as i64 * rows_per_partition
                },
                row_count: rows_per_partition,
            });
        }

        Ok(partitions)
    }

    async fn load_indexes(&self, table: &mut Table) -> Result<()> {
        let _lock = self.conn_mutex.lock().await;

        let schema_ident = escape_sql_ident(&table.schema);
        let table_ident = escape_sql_ident(&table.name);

        let sql = format!(
            r#"
            SELECT
                i.name AS index_name,
                i.is_unique,
                i.type_desc,
                c.name AS column_name,
                ic.is_included_column
            FROM sys.indexes i
            INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE i.object_id = OBJECT_ID('[{schema_ident}].[{table_ident}]')
                AND i.is_primary_key = 0
                AND i.type > 0
            ORDER BY i.name, ic.key_ordinal
            "#
        );

        let rows = self.execute_query(&sql)?;

        let mut indexes: std::collections::HashMap<String, Index> =
            std::collections::HashMap::new();

        for row in rows {
            let index_name = row.get(0).and_then(|v| v.clone()).unwrap_or_default();
            let is_unique: bool = row
                .get(1)
                .and_then(|v| v.as_ref())
                .map(|s| s == "1" || s.to_lowercase() == "true")
                .unwrap_or(false);
            let type_desc = row.get(2).and_then(|v| v.clone()).unwrap_or_default();
            let column_name = row.get(3).and_then(|v| v.clone()).unwrap_or_default();
            let is_included: bool = row
                .get(4)
                .and_then(|v| v.as_ref())
                .map(|s| s == "1" || s.to_lowercase() == "true")
                .unwrap_or(false);

            let index = indexes.entry(index_name.clone()).or_insert_with(|| Index {
                name: index_name,
                is_unique,
                is_clustered: type_desc == "CLUSTERED",
                columns: Vec::new(),
                include_cols: Vec::new(),
            });

            if is_included {
                index.include_cols.push(column_name);
            } else {
                index.columns.push(column_name);
            }
        }

        table.indexes = indexes.into_values().collect();
        Ok(())
    }

    async fn load_foreign_keys(&self, table: &mut Table) -> Result<()> {
        let _lock = self.conn_mutex.lock().await;

        let schema_ident = escape_sql_ident(&table.schema);
        let table_ident = escape_sql_ident(&table.name);

        let sql = format!(
            r#"
            SELECT
                fk.name AS fk_name,
                COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name,
                OBJECT_SCHEMA_NAME(fkc.referenced_object_id) AS ref_schema,
                OBJECT_NAME(fkc.referenced_object_id) AS ref_table,
                COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS ref_column,
                fk.delete_referential_action_desc,
                fk.update_referential_action_desc
            FROM sys.foreign_keys fk
            INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            WHERE fk.parent_object_id = OBJECT_ID('[{schema_ident}].[{table_ident}]')
            ORDER BY fk.name, fkc.constraint_column_id
            "#
        );

        let rows = self.execute_query(&sql)?;

        let mut fks: std::collections::HashMap<String, ForeignKey> =
            std::collections::HashMap::new();

        for row in rows {
            let fk_name = row.get(0).and_then(|v| v.clone()).unwrap_or_default();
            let column_name = row.get(1).and_then(|v| v.clone()).unwrap_or_default();
            let ref_schema = row.get(2).and_then(|v| v.clone()).unwrap_or_default();
            let ref_table = row.get(3).and_then(|v| v.clone()).unwrap_or_default();
            let ref_column = row.get(4).and_then(|v| v.clone()).unwrap_or_default();
            let on_delete = row.get(5).and_then(|v| v.clone()).unwrap_or_default();
            let on_update = row.get(6).and_then(|v| v.clone()).unwrap_or_default();

            let fk = fks.entry(fk_name.clone()).or_insert_with(|| ForeignKey {
                name: fk_name,
                columns: Vec::new(),
                ref_schema,
                ref_table,
                ref_columns: Vec::new(),
                on_delete,
                on_update,
            });

            fk.columns.push(column_name);
            fk.ref_columns.push(ref_column);
        }

        table.foreign_keys = fks.into_values().collect();
        Ok(())
    }

    async fn load_check_constraints(&self, table: &mut Table) -> Result<()> {
        let _lock = self.conn_mutex.lock().await;

        let schema_ident = escape_sql_ident(&table.schema);
        let table_ident = escape_sql_ident(&table.name);

        let sql = format!(
            r#"
            SELECT
                cc.name AS constraint_name,
                cc.definition
            FROM sys.check_constraints cc
            WHERE cc.parent_object_id = OBJECT_ID('[{schema_ident}].[{table_ident}]')
            "#
        );

        let rows = self.execute_query(&sql)?;

        for row in rows {
            let name = row.get(0).and_then(|v| v.clone()).unwrap_or_default();
            let definition = row.get(1).and_then(|v| v.clone()).unwrap_or_default();

            table.check_constraints.push(CheckConstraint { name, definition });
        }

        Ok(())
    }

    async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        let _lock = self.conn_mutex.lock().await;

        let schema_ident = escape_sql_ident(schema);
        let table_ident = escape_sql_ident(table);

        let sql = format!("SELECT COUNT(*) FROM [{schema_ident}].[{table_ident}]");

        let rows = self.execute_query(&sql)?;

        Ok(rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_ref())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0))
    }

    fn db_type(&self) -> &str {
        "mssql"
    }

    async fn close(&self) {
        // ODBC connections are closed when dropped
    }
}

/// Convert a text value from ODBC to SqlValue based on the column type.
fn convert_text_to_sqlvalue(text: Option<String>, data_type: &str) -> SqlValue {
    let Some(s) = text else {
        // Return appropriate null type based on data type
        return match data_type.to_lowercase().as_str() {
            "bit" => SqlValue::Null(SqlNullType::Bool),
            "tinyint" | "smallint" => SqlValue::Null(SqlNullType::I16),
            "int" => SqlValue::Null(SqlNullType::I32),
            "bigint" => SqlValue::Null(SqlNullType::I64),
            "real" => SqlValue::Null(SqlNullType::F32),
            "float" => SqlValue::Null(SqlNullType::F64),
            "uniqueidentifier" => SqlValue::Null(SqlNullType::Uuid),
            "datetime" | "datetime2" | "smalldatetime" => SqlValue::Null(SqlNullType::DateTime),
            "date" => SqlValue::Null(SqlNullType::Date),
            "time" => SqlValue::Null(SqlNullType::Time),
            "binary" | "varbinary" | "image" => SqlValue::Null(SqlNullType::Bytes),
            "decimal" | "numeric" | "money" | "smallmoney" => SqlValue::Null(SqlNullType::Decimal),
            _ => SqlValue::Null(SqlNullType::String),
        };
    };

    let dt = data_type.to_lowercase();

    match dt.as_str() {
        "bit" => match s.as_str() {
            "1" | "true" | "True" | "TRUE" => SqlValue::Bool(true),
            "0" | "false" | "False" | "FALSE" => SqlValue::Bool(false),
            _ => SqlValue::Bool(s.parse().unwrap_or(false)),
        },
        "tinyint" => s
            .parse::<u8>()
            .map(|v| SqlValue::I16(v as i16))
            .unwrap_or(SqlValue::Null(SqlNullType::I16)),
        "smallint" => s
            .parse::<i16>()
            .map(SqlValue::I16)
            .unwrap_or(SqlValue::Null(SqlNullType::I16)),
        "int" => s
            .parse::<i32>()
            .map(SqlValue::I32)
            .unwrap_or(SqlValue::Null(SqlNullType::I32)),
        "bigint" => s
            .parse::<i64>()
            .map(SqlValue::I64)
            .unwrap_or(SqlValue::Null(SqlNullType::I64)),
        "real" => s
            .parse::<f32>()
            .map(SqlValue::F32)
            .unwrap_or(SqlValue::Null(SqlNullType::F32)),
        "float" => s
            .parse::<f64>()
            .map(SqlValue::F64)
            .unwrap_or(SqlValue::Null(SqlNullType::F64)),
        "uniqueidentifier" => uuid::Uuid::parse_str(&s)
            .map(SqlValue::Uuid)
            .unwrap_or(SqlValue::Null(SqlNullType::Uuid)),
        "datetime" | "datetime2" | "smalldatetime" => {
            // ODBC typically returns datetime in format: "2023-01-15 10:30:45.123"
            // Try multiple formats
            chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S"))
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f"))
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S"))
                .map(SqlValue::DateTime)
                .unwrap_or(SqlValue::Null(SqlNullType::DateTime))
        }
        "date" => chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d")
            .map(SqlValue::Date)
            .unwrap_or(SqlValue::Null(SqlNullType::Date)),
        "time" => chrono::NaiveTime::parse_from_str(&s, "%H:%M:%S%.f")
            .or_else(|_| chrono::NaiveTime::parse_from_str(&s, "%H:%M:%S"))
            .map(SqlValue::Time)
            .unwrap_or(SqlValue::Null(SqlNullType::Time)),
        "binary" | "varbinary" | "image" => {
            // ODBC returns binary as hex string (e.g., "0xDEADBEEF" or just hex digits)
            let hex_str = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")).unwrap_or(&s);
            hex::decode(hex_str)
                .map(SqlValue::Bytes)
                .unwrap_or_else(|_| SqlValue::Bytes(s.into_bytes()))
        }
        "decimal" | "numeric" | "money" | "smallmoney" => {
            // Remove currency symbols and commas that might be present for money types
            let cleaned = s.replace(['$', ','], "");
            rust_decimal::Decimal::from_str_exact(&cleaned)
                .or_else(|_| cleaned.parse::<rust_decimal::Decimal>())
                .map(SqlValue::Decimal)
                .unwrap_or_else(|_| {
                    // Fallback to f64
                    cleaned
                        .parse::<f64>()
                        .map(SqlValue::F64)
                        .unwrap_or(SqlValue::Null(SqlNullType::Decimal))
                })
        }
        _ => {
            // Default: treat as string
            SqlValue::String(s)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Timelike};

    // === escape_sql_string tests ===

    #[test]
    fn test_escape_sql_string_no_quotes() {
        assert_eq!(escape_sql_string("hello"), "hello");
    }

    #[test]
    fn test_escape_sql_string_single_quote() {
        assert_eq!(escape_sql_string("O'Brien"), "O''Brien");
    }

    #[test]
    fn test_escape_sql_string_multiple_quotes() {
        assert_eq!(escape_sql_string("It's a 'test'"), "It''s a ''test''");
    }

    #[test]
    fn test_escape_sql_string_empty() {
        assert_eq!(escape_sql_string(""), "");
    }

    #[test]
    fn test_escape_sql_string_sql_injection_attempt() {
        // Attempt: '; DROP TABLE users; --
        let malicious = "'; DROP TABLE users; --";
        let escaped = escape_sql_string(malicious);
        assert_eq!(escaped, "''; DROP TABLE users; --");
        // The doubled quote prevents the injection
    }

    // === escape_sql_ident tests ===

    #[test]
    fn test_escape_sql_ident_no_brackets() {
        assert_eq!(escape_sql_ident("TableName"), "TableName");
    }

    #[test]
    fn test_escape_sql_ident_with_bracket() {
        assert_eq!(escape_sql_ident("Table]Name"), "Table]]Name");
    }

    #[test]
    fn test_escape_sql_ident_multiple_brackets() {
        assert_eq!(escape_sql_ident("a]b]c"), "a]]b]]c");
    }

    #[test]
    fn test_escape_sql_ident_empty() {
        assert_eq!(escape_sql_ident(""), "");
    }

    // === convert_text_to_sqlvalue tests ===

    #[test]
    fn test_convert_null_values() {
        // Null for various types
        assert!(matches!(convert_text_to_sqlvalue(None, "int"), SqlValue::Null(SqlNullType::I32)));
        assert!(matches!(convert_text_to_sqlvalue(None, "bigint"), SqlValue::Null(SqlNullType::I64)));
        assert!(matches!(convert_text_to_sqlvalue(None, "varchar"), SqlValue::Null(SqlNullType::String)));
        assert!(matches!(convert_text_to_sqlvalue(None, "bit"), SqlValue::Null(SqlNullType::Bool)));
        assert!(matches!(convert_text_to_sqlvalue(None, "datetime"), SqlValue::Null(SqlNullType::DateTime)));
    }

    #[test]
    fn test_convert_bit_values() {
        assert_eq!(convert_text_to_sqlvalue(Some("1".into()), "bit"), SqlValue::Bool(true));
        assert_eq!(convert_text_to_sqlvalue(Some("0".into()), "bit"), SqlValue::Bool(false));
        assert_eq!(convert_text_to_sqlvalue(Some("true".into()), "bit"), SqlValue::Bool(true));
        assert_eq!(convert_text_to_sqlvalue(Some("false".into()), "bit"), SqlValue::Bool(false));
        assert_eq!(convert_text_to_sqlvalue(Some("TRUE".into()), "bit"), SqlValue::Bool(true));
        assert_eq!(convert_text_to_sqlvalue(Some("FALSE".into()), "bit"), SqlValue::Bool(false));
    }

    #[test]
    fn test_convert_integer_values() {
        assert_eq!(convert_text_to_sqlvalue(Some("42".into()), "int"), SqlValue::I32(42));
        assert_eq!(convert_text_to_sqlvalue(Some("-100".into()), "int"), SqlValue::I32(-100));
        assert_eq!(convert_text_to_sqlvalue(Some("9223372036854775807".into()), "bigint"), SqlValue::I64(i64::MAX));
        assert_eq!(convert_text_to_sqlvalue(Some("255".into()), "tinyint"), SqlValue::I16(255));
        assert_eq!(convert_text_to_sqlvalue(Some("-32768".into()), "smallint"), SqlValue::I16(-32768));
    }

    #[test]
    fn test_convert_float_values() {
        assert_eq!(convert_text_to_sqlvalue(Some("3.14".into()), "real"), SqlValue::F32(3.14));
        assert_eq!(convert_text_to_sqlvalue(Some("2.718281828".into()), "float"), SqlValue::F64(2.718281828));
    }

    #[test]
    fn test_convert_string_values() {
        assert_eq!(
            convert_text_to_sqlvalue(Some("Hello World".into()), "varchar"),
            SqlValue::String("Hello World".into())
        );
        assert_eq!(
            convert_text_to_sqlvalue(Some("Unicode: 日本語".into()), "nvarchar"),
            SqlValue::String("Unicode: 日本語".into())
        );
    }

    #[test]
    fn test_convert_datetime_values() {
        let dt = convert_text_to_sqlvalue(Some("2023-12-25 10:30:45.123".into()), "datetime");
        match dt {
            SqlValue::DateTime(ndt) => {
                assert_eq!(ndt.year(), 2023);
                assert_eq!(ndt.month(), 12);
                assert_eq!(ndt.day(), 25);
                assert_eq!(ndt.hour(), 10);
                assert_eq!(ndt.minute(), 30);
            }
            _ => panic!("Expected DateTime"),
        }

        // Test ISO format
        let dt2 = convert_text_to_sqlvalue(Some("2023-12-25T10:30:45".into()), "datetime2");
        assert!(matches!(dt2, SqlValue::DateTime(_)));
    }

    #[test]
    fn test_convert_date_values() {
        let d = convert_text_to_sqlvalue(Some("2023-12-25".into()), "date");
        match d {
            SqlValue::Date(nd) => {
                assert_eq!(nd.year(), 2023);
                assert_eq!(nd.month(), 12);
                assert_eq!(nd.day(), 25);
            }
            _ => panic!("Expected Date"),
        }
    }

    #[test]
    fn test_convert_time_values() {
        let t = convert_text_to_sqlvalue(Some("14:30:45.123".into()), "time");
        match t {
            SqlValue::Time(nt) => {
                assert_eq!(nt.hour(), 14);
                assert_eq!(nt.minute(), 30);
                assert_eq!(nt.second(), 45);
            }
            _ => panic!("Expected Time"),
        }
    }

    #[test]
    fn test_convert_uuid_values() {
        let u = convert_text_to_sqlvalue(
            Some("550e8400-e29b-41d4-a716-446655440000".into()),
            "uniqueidentifier",
        );
        match u {
            SqlValue::Uuid(uuid) => {
                assert_eq!(uuid.to_string(), "550e8400-e29b-41d4-a716-446655440000");
            }
            _ => panic!("Expected Uuid"),
        }
    }

    #[test]
    fn test_convert_binary_values() {
        // With 0x prefix
        let b = convert_text_to_sqlvalue(Some("0xDEADBEEF".into()), "varbinary");
        match b {
            SqlValue::Bytes(bytes) => {
                assert_eq!(bytes, vec![0xDE, 0xAD, 0xBE, 0xEF]);
            }
            _ => panic!("Expected Bytes"),
        }

        // Without prefix
        let b2 = convert_text_to_sqlvalue(Some("CAFEBABE".into()), "binary");
        match b2 {
            SqlValue::Bytes(bytes) => {
                assert_eq!(bytes, vec![0xCA, 0xFE, 0xBA, 0xBE]);
            }
            _ => panic!("Expected Bytes"),
        }
    }

    #[test]
    fn test_convert_decimal_values() {
        let d = convert_text_to_sqlvalue(Some("123.456".into()), "decimal");
        match d {
            SqlValue::Decimal(dec) => {
                assert_eq!(dec.to_string(), "123.456");
            }
            _ => panic!("Expected Decimal"),
        }

        // Money with currency symbol
        let m = convert_text_to_sqlvalue(Some("$1,234.56".into()), "money");
        match m {
            SqlValue::Decimal(dec) => {
                assert_eq!(dec.to_string(), "1234.56");
            }
            _ => panic!("Expected Decimal for money"),
        }
    }

    #[test]
    fn test_convert_invalid_values_return_null() {
        // Invalid int
        assert!(matches!(
            convert_text_to_sqlvalue(Some("not_a_number".into()), "int"),
            SqlValue::Null(SqlNullType::I32)
        ));

        // Invalid uuid
        assert!(matches!(
            convert_text_to_sqlvalue(Some("not-a-uuid".into()), "uniqueidentifier"),
            SqlValue::Null(SqlNullType::Uuid)
        ));

        // Invalid date
        assert!(matches!(
            convert_text_to_sqlvalue(Some("not-a-date".into()), "date"),
            SqlValue::Null(SqlNullType::Date)
        ));
    }
}
