//! ODBC-based PostgreSQL source for Kerberos and ODBC authentication.
//!
//! This module provides an ODBC-based implementation of `SourcePool` that supports:
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
//! **Usage (Kerberos):**
//! On Linux/macOS, obtain a Kerberos ticket before running:
//! ```sh
//! kinit user@REALM
//! ```

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

/// ODBC-based PostgreSQL source pool.
pub struct OdbcPgSourcePool {
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

/// Escape a PostgreSQL identifier for use in quoted notation.
/// Doubles double quotes: `Table"Name` -> `Table""Name`
fn escape_pg_ident(s: &str) -> String {
    s.replace('"', "\"\"")
}

impl OdbcPgSourcePool {
    /// Create a new ODBC-based PostgreSQL source pool.
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

    /// Create a new ODBC-based PostgreSQL source pool with specified max connections.
    pub async fn with_max_connections(config: SourceConfig, _max_size: u32) -> Result<Self> {
        use crate::config::AuthMethod;

        // Create ODBC environment
        let env = Environment::new().map_err(|e| {
            MigrateError::pool_msg(
                format!(
                    "Failed to create ODBC environment: {}. \
                     Make sure the PostgreSQL ODBC Driver (psqlODBC) is installed. \
                     Windows: Download from postgresql.org. \
                     Linux: apt install odbc-postgresql. \
                     macOS: brew install psqlodbc.",
                    e
                ),
                "ODBC connection"
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
                     SSLMode=prefer;",
                    config.host,
                    config.port,
                    config.database,
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
                     SSLMode=prefer;",
                    config.host,
                    config.port,
                    config.database,
                    config.user,
                    config.password,
                );
                (conn_str, "ODBC")
            }
        };

        debug!(
            "PostgreSQL ODBC connection string (credentials hidden): Driver={{PostgreSQL Unicode}};Server={};Port={};Database={};Auth={};...",
            config.host, config.port, config.database, auth_desc
        );

        // Test connection
        {
            let conn = env
                .connect_with_connection_string(&connection_string, ConnectionOptions::default())
                .map_err(|e| {
                    let help_msg = if config.auth == AuthMethod::Kerberos {
                        "Ensure you have a valid Kerberos ticket (run 'kinit user@REALM' on Linux/macOS)."
                    } else {
                        "Check that the username and password are correct."
                    };
                    MigrateError::pool_msg(
                        format!(
                            "Failed to connect to PostgreSQL via ODBC ({}): {}. {}",
                            auth_desc, e, help_msg
                        ),
                        "ODBC connection"
                    )
                })?;

            // Verify connection with a simple query
            let _ = conn.execute("SELECT 1", ());
        }

        info!(
            "Connected to PostgreSQL via ODBC ({}): {}:{}/{}",
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

    /// Execute a query and return rows as Vec<Vec<Option<String>>>.
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
            let num_cols: usize = cursor.num_result_cols().map_err(|e| {
                MigrateError::transfer("ODBC query", format!("Failed to get column count: {}", e))
            })? as usize;

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
                        let col_type = col_types.get(col_idx).map(|s| s.as_str()).unwrap_or("text");
                        let text_value = batch
                            .at(col_idx, row_idx)
                            .map(|bytes| String::from_utf8_lossy(bytes).to_string());

                        let value = convert_pg_text_to_sqlvalue(text_value, col_type);
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
        let pk_ident = escape_pg_ident(pk_col);
        let schema_ident = escape_pg_ident(schema);
        let table_ident = escape_pg_ident(table);

        let sql = format!(
            r#"SELECT CAST(MAX("{pk_ident}") AS BIGINT) FROM "{schema_ident}"."{table_ident}""#
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
    pub async fn get_max_pk(&self, schema: &str, table: &str, pk_col: &str) -> Result<i64> {
        let _lock = self.conn_mutex.lock().await;
        self.get_max_pk_sync(schema, table, pk_col)
    }

    fn load_columns_sync(&self, schema: &str, table_name: &str, table: &mut Table) -> Result<()> {
        let schema_lit = escape_sql_string(schema);
        let table_lit = escape_sql_string(table_name);

        let sql = format!(
            r#"
            SELECT
                column_name,
                udt_name,
                COALESCE(character_maximum_length, 0)::int4,
                COALESCE(numeric_precision, 0)::int4,
                COALESCE(numeric_scale, 0)::int4,
                CASE WHEN is_nullable = 'YES' THEN 1 ELSE 0 END,
                COALESCE(
                    (SELECT 1 FROM pg_catalog.pg_class c
                     JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                     WHERE n.nspname = columns.table_schema
                       AND c.relname = columns.table_name
                       AND a.attname = columns.column_name
                       AND a.attidentity IN ('a', 'd')),
                    0
                ) AS is_identity,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = '{schema_lit}'
                AND table_name = '{table_lit}'
            ORDER BY ordinal_position
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
            SELECT a.attname
            FROM pg_catalog.pg_constraint c
            JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid
            WHERE n.nspname = '{schema_lit}'
              AND t.relname = '{table_lit}'
              AND c.contype = 'p'
              AND a.attnum = ANY(c.conkey)
            ORDER BY array_position(c.conkey, a.attnum)
            "#
        );

        let rows = self.execute_query(&sql)?;

        for row in rows {
            if let Some(col_name) = row.get(0).and_then(|v| v.clone()) {
                table.primary_key.push(col_name.clone());
                if let Some(col) = table.columns.iter().find(|c| c.name == col_name) {
                    table.pk_columns.push(col.clone());
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl SourcePool for OdbcPgSourcePool {
    async fn extract_schema(&self, schema: &str) -> Result<Vec<Table>> {
        let _lock = self.conn_mutex.lock().await;

        let schema_lit = escape_sql_string(schema);

        // Query to get all tables with their row counts
        let sql = format!(
            r#"
            SELECT
                t.table_name,
                COALESCE(c.reltuples::bigint, 0) as row_count
            FROM information_schema.tables t
            LEFT JOIN pg_catalog.pg_class c ON c.relname = t.table_name
            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
            WHERE t.table_schema = '{schema_lit}'
                AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_name
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

            // Estimate row size
            table.estimated_row_size = table
                .columns
                .iter()
                .map(|c| match c.data_type.as_str() {
                    "int4" => 4,
                    "int8" => 8,
                    "int2" => 2,
                    "bool" => 1,
                    "float8" => 8,
                    "float4" => 4,
                    "timestamp" | "timestamptz" => 8,
                    "date" => 4,
                    "time" | "timetz" => 8,
                    "uuid" => 16,
                    "varchar" | "text" | "char" | "bpchar" => {
                        if c.max_length == 0 {
                            100
                        } else {
                            c.max_length.min(100) as i64
                        }
                    }
                    _ => 8,
                })
                .sum();

            tables.push(table);
        }

        info!(
            "Extracted {} tables from schema '{}' via PostgreSQL ODBC",
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
        let pk_ident = escape_pg_ident(pk_col);
        let schema_ident = escape_pg_ident(&table.schema);
        let table_ident = escape_pg_ident(&table.name);

        // Get min and max PK values
        let sql = format!(
            r#"SELECT MIN("{pk_ident}")::bigint, MAX("{pk_ident}")::bigint FROM "{schema_ident}"."{table_ident}""#
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

        let schema_lit = escape_sql_string(&table.schema);
        let table_lit = escape_sql_string(&table.name);

        let sql = format!(
            r#"
            SELECT
                i.relname AS index_name,
                ix.indisunique,
                am.amname,
                array_to_string(array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)), ',') AS columns
            FROM pg_catalog.pg_class t
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_catalog.pg_index ix ON t.oid = ix.indrelid
            JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
            JOIN pg_catalog.pg_am am ON am.oid = i.relam
            JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE n.nspname = '{schema_lit}'
              AND t.relname = '{table_lit}'
              AND NOT ix.indisprimary
            GROUP BY i.relname, ix.indisunique, am.amname
            ORDER BY i.relname
            "#
        );

        let rows = self.execute_query(&sql)?;

        for row in rows {
            let index_name = row.get(0).and_then(|v| v.clone()).unwrap_or_default();
            let is_unique: bool = row
                .get(1)
                .and_then(|v| v.as_ref())
                .map(|s| s == "t" || s == "true" || s == "1")
                .unwrap_or(false);
            let _am_name = row.get(2).and_then(|v| v.clone()).unwrap_or_default();
            let columns_str = row.get(3).and_then(|v| v.clone()).unwrap_or_default();

            table.indexes.push(Index {
                name: index_name,
                is_unique,
                is_clustered: false, // PostgreSQL doesn't have clustered indexes like MSSQL
                columns: columns_str
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect(),
                include_cols: Vec::new(), // PostgreSQL INCLUDE columns require different query
            });
        }

        Ok(())
    }

    async fn load_foreign_keys(&self, table: &mut Table) -> Result<()> {
        let _lock = self.conn_mutex.lock().await;

        let schema_lit = escape_sql_string(&table.schema);
        let table_lit = escape_sql_string(&table.name);

        let sql = format!(
            r#"
            SELECT
                c.conname AS fk_name,
                array_to_string(array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum)), ',') AS columns,
                rn.nspname AS ref_schema,
                rc.relname AS ref_table,
                array_to_string(array_agg(ra.attname ORDER BY array_position(c.confkey, ra.attnum)), ',') AS ref_columns,
                CASE c.confdeltype
                    WHEN 'a' THEN 'NO_ACTION'
                    WHEN 'r' THEN 'RESTRICT'
                    WHEN 'c' THEN 'CASCADE'
                    WHEN 'n' THEN 'SET_NULL'
                    WHEN 'd' THEN 'SET_DEFAULT'
                    ELSE 'NO_ACTION'
                END AS on_delete,
                CASE c.confupdtype
                    WHEN 'a' THEN 'NO_ACTION'
                    WHEN 'r' THEN 'RESTRICT'
                    WHEN 'c' THEN 'CASCADE'
                    WHEN 'n' THEN 'SET_NULL'
                    WHEN 'd' THEN 'SET_DEFAULT'
                    ELSE 'NO_ACTION'
                END AS on_update
            FROM pg_catalog.pg_constraint c
            JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_catalog.pg_class rc ON rc.oid = c.confrelid
            JOIN pg_catalog.pg_namespace rn ON rn.oid = rc.relnamespace
            JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
            JOIN pg_catalog.pg_attribute ra ON ra.attrelid = rc.oid AND ra.attnum = ANY(c.confkey)
            WHERE n.nspname = '{schema_lit}'
              AND t.relname = '{table_lit}'
              AND c.contype = 'f'
            GROUP BY c.conname, rn.nspname, rc.relname, c.confdeltype, c.confupdtype
            ORDER BY c.conname
            "#
        );

        let rows = self.execute_query(&sql)?;

        for row in rows {
            let fk_name = row.get(0).and_then(|v| v.clone()).unwrap_or_default();
            let columns_str = row.get(1).and_then(|v| v.clone()).unwrap_or_default();
            let ref_schema = row.get(2).and_then(|v| v.clone()).unwrap_or_default();
            let ref_table = row.get(3).and_then(|v| v.clone()).unwrap_or_default();
            let ref_columns_str = row.get(4).and_then(|v| v.clone()).unwrap_or_default();
            let on_delete = row.get(5).and_then(|v| v.clone()).unwrap_or_default();
            let on_update = row.get(6).and_then(|v| v.clone()).unwrap_or_default();

            table.foreign_keys.push(ForeignKey {
                name: fk_name,
                columns: columns_str
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect(),
                ref_schema,
                ref_table,
                ref_columns: ref_columns_str
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect(),
                on_delete,
                on_update,
            });
        }

        Ok(())
    }

    async fn load_check_constraints(&self, table: &mut Table) -> Result<()> {
        let _lock = self.conn_mutex.lock().await;

        let schema_lit = escape_sql_string(&table.schema);
        let table_lit = escape_sql_string(&table.name);

        let sql = format!(
            r#"
            SELECT
                c.conname AS constraint_name,
                pg_get_constraintdef(c.oid) AS definition
            FROM pg_catalog.pg_constraint c
            JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = '{schema_lit}'
              AND t.relname = '{table_lit}'
              AND c.contype = 'c'
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

        let schema_ident = escape_pg_ident(schema);
        let table_ident = escape_pg_ident(table);

        let sql = format!(r#"SELECT COUNT(*)::bigint FROM "{schema_ident}"."{table_ident}""#);

        let rows = self.execute_query(&sql)?;

        Ok(rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_ref())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0))
    }

    fn db_type(&self) -> &str {
        "postgres"
    }

    async fn close(&self) {
        // ODBC connections are closed when dropped
    }
}

/// Convert a text value from ODBC to SqlValue based on PostgreSQL column type.
fn convert_pg_text_to_sqlvalue(text: Option<String>, data_type: &str) -> SqlValue {
    let Some(s) = text else {
        return match data_type.to_lowercase().as_str() {
            "bool" => SqlValue::Null(SqlNullType::Bool),
            "int2" => SqlValue::Null(SqlNullType::I16),
            "int4" => SqlValue::Null(SqlNullType::I32),
            "int8" => SqlValue::Null(SqlNullType::I64),
            "float4" => SqlValue::Null(SqlNullType::F32),
            "float8" => SqlValue::Null(SqlNullType::F64),
            "uuid" => SqlValue::Null(SqlNullType::Uuid),
            "timestamp" | "timestamptz" => SqlValue::Null(SqlNullType::DateTime),
            "date" => SqlValue::Null(SqlNullType::Date),
            "time" | "timetz" => SqlValue::Null(SqlNullType::Time),
            "bytea" => SqlValue::Null(SqlNullType::Bytes),
            "numeric" | "decimal" | "money" => SqlValue::Null(SqlNullType::Decimal),
            _ => SqlValue::Null(SqlNullType::String),
        };
    };

    let dt = data_type.to_lowercase();

    match dt.as_str() {
        "bool" => match s.as_str() {
            "t" | "true" | "1" | "yes" | "on" => SqlValue::Bool(true),
            "f" | "false" | "0" | "no" | "off" => SqlValue::Bool(false),
            _ => SqlValue::Bool(s.parse().unwrap_or(false)),
        },
        "int2" => s
            .parse::<i16>()
            .map(SqlValue::I16)
            .unwrap_or(SqlValue::Null(SqlNullType::I16)),
        "int4" => s
            .parse::<i32>()
            .map(SqlValue::I32)
            .unwrap_or(SqlValue::Null(SqlNullType::I32)),
        "int8" => s
            .parse::<i64>()
            .map(SqlValue::I64)
            .unwrap_or(SqlValue::Null(SqlNullType::I64)),
        "float4" => s
            .parse::<f32>()
            .map(SqlValue::F32)
            .unwrap_or(SqlValue::Null(SqlNullType::F32)),
        "float8" => s
            .parse::<f64>()
            .map(SqlValue::F64)
            .unwrap_or(SqlValue::Null(SqlNullType::F64)),
        "uuid" => uuid::Uuid::parse_str(&s)
            .map(SqlValue::Uuid)
            .unwrap_or(SqlValue::Null(SqlNullType::Uuid)),
        "timestamp" | "timestamptz" => {
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
        "time" | "timetz" => chrono::NaiveTime::parse_from_str(&s, "%H:%M:%S%.f")
            .or_else(|_| chrono::NaiveTime::parse_from_str(&s, "%H:%M:%S"))
            .map(SqlValue::Time)
            .unwrap_or(SqlValue::Null(SqlNullType::Time)),
        "bytea" => {
            // PostgreSQL bytea in hex format: \x followed by hex digits
            let hex_str = s.strip_prefix("\\x").unwrap_or(&s);
            hex::decode(hex_str)
                .map(SqlValue::Bytes)
                .unwrap_or_else(|_| SqlValue::Bytes(s.into_bytes()))
        }
        "numeric" | "decimal" | "money" => {
            let cleaned = s.replace(['$', ','], "");
            rust_decimal::Decimal::from_str_exact(&cleaned)
                .or_else(|_| cleaned.parse::<rust_decimal::Decimal>())
                .map(SqlValue::Decimal)
                .unwrap_or_else(|_| {
                    cleaned
                        .parse::<f64>()
                        .map(SqlValue::F64)
                        .unwrap_or(SqlValue::Null(SqlNullType::Decimal))
                })
        }
        _ => SqlValue::String(s),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_sql_string_no_quotes() {
        assert_eq!(escape_sql_string("hello"), "hello");
    }

    #[test]
    fn test_escape_sql_string_single_quote() {
        assert_eq!(escape_sql_string("O'Brien"), "O''Brien");
    }

    #[test]
    fn test_escape_pg_ident_no_quotes() {
        assert_eq!(escape_pg_ident("TableName"), "TableName");
    }

    #[test]
    fn test_escape_pg_ident_with_quote() {
        assert_eq!(escape_pg_ident("Table\"Name"), "Table\"\"Name");
    }

    #[test]
    fn test_convert_pg_null_values() {
        assert!(matches!(convert_pg_text_to_sqlvalue(None, "int4"), SqlValue::Null(SqlNullType::I32)));
        assert!(matches!(convert_pg_text_to_sqlvalue(None, "int8"), SqlValue::Null(SqlNullType::I64)));
        assert!(matches!(convert_pg_text_to_sqlvalue(None, "text"), SqlValue::Null(SqlNullType::String)));
        assert!(matches!(convert_pg_text_to_sqlvalue(None, "bool"), SqlValue::Null(SqlNullType::Bool)));
    }

    #[test]
    fn test_convert_pg_bool_values() {
        assert_eq!(convert_pg_text_to_sqlvalue(Some("t".into()), "bool"), SqlValue::Bool(true));
        assert_eq!(convert_pg_text_to_sqlvalue(Some("f".into()), "bool"), SqlValue::Bool(false));
        assert_eq!(convert_pg_text_to_sqlvalue(Some("true".into()), "bool"), SqlValue::Bool(true));
        assert_eq!(convert_pg_text_to_sqlvalue(Some("false".into()), "bool"), SqlValue::Bool(false));
    }

    #[test]
    fn test_convert_pg_integer_values() {
        assert_eq!(convert_pg_text_to_sqlvalue(Some("42".into()), "int4"), SqlValue::I32(42));
        assert_eq!(convert_pg_text_to_sqlvalue(Some("-100".into()), "int4"), SqlValue::I32(-100));
        assert_eq!(convert_pg_text_to_sqlvalue(Some("9223372036854775807".into()), "int8"), SqlValue::I64(i64::MAX));
        assert_eq!(convert_pg_text_to_sqlvalue(Some("-32768".into()), "int2"), SqlValue::I16(-32768));
    }

    #[test]
    fn test_convert_pg_string_values() {
        assert_eq!(
            convert_pg_text_to_sqlvalue(Some("Hello World".into()), "text"),
            SqlValue::String("Hello World".into())
        );
    }
}
