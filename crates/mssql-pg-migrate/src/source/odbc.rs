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
                "ODBC Kerberos authentication"
            )
        })?;

        // Build connection string for Kerberos authentication
        let connection_string = format!(
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

        debug!("ODBC connection string (without credentials): Driver={{ODBC Driver 18 for SQL Server}};Server={},{};Database={};Trusted_Connection=yes;...",
               config.host, config.port, config.database);

        // Test connection - use a scope so conn is dropped before we move env
        {
            let conn = env
                .connect_with_connection_string(&connection_string, ConnectionOptions::default())
                .map_err(|e| {
                    MigrateError::pool_msg(
                        format!(
                            "Failed to connect to MSSQL via ODBC with Kerberos: {}. \
                             Ensure you have a valid Kerberos ticket (run 'kinit user@REALM' on Linux/macOS) \
                             or are on a domain-joined Windows machine.",
                            e
                        ),
                        "ODBC Kerberos authentication"
                    )
                })?;

            // Verify connection with a simple query
            let _ = conn.execute("SELECT 1", ());
        }

        info!(
            "Connected to MSSQL via ODBC (Kerberos): {}:{}/{}",
            config.host, config.port, config.database
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

    fn load_columns_sync(&self, schema: &str, table_name: &str, table: &mut Table) -> Result<()> {
        let sql = format!(
            r#"
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                CAST(ISNULL(CHARACTER_MAXIMUM_LENGTH, 0) AS INT),
                CAST(ISNULL(NUMERIC_PRECISION, 0) AS INT),
                CAST(ISNULL(NUMERIC_SCALE, 0) AS INT),
                CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END,
                COLUMNPROPERTY(OBJECT_ID('[{}].[{}]'), COLUMN_NAME, 'IsIdentity'),
                ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{}'
                AND TABLE_NAME = '{}'
            ORDER BY ORDINAL_POSITION
            "#,
            schema, table_name, schema, table_name
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
        let sql = format!(
            r#"
            SELECT c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
                ON tc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = c.TABLE_SCHEMA
                AND tc.TABLE_NAME = c.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND tc.TABLE_SCHEMA = '{}'
                AND tc.TABLE_NAME = '{}'
            ORDER BY c.ORDINAL_POSITION
            "#,
            schema, table_name
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

        // Query to get all tables with their row counts
        let sql = format!(
            r#"
            SELECT
                t.TABLE_NAME,
                ISNULL(p.rows, 0) as row_count
            FROM INFORMATION_SCHEMA.TABLES t
            LEFT JOIN sys.partitions p ON p.object_id = OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME)
                AND p.index_id IN (0, 1)
            WHERE t.TABLE_SCHEMA = '{}'
                AND t.TABLE_TYPE = 'BASE TABLE'
            ORDER BY t.TABLE_NAME
            "#,
            schema
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
        let qualified_table = format!("[{}].[{}]", table.schema, table.name);

        // Get min and max PK values
        let sql = format!(
            "SELECT MIN([{pk}]), MAX([{pk}]) FROM {table}",
            pk = pk_col,
            table = qualified_table
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
            WHERE i.object_id = OBJECT_ID('[{}].[{}]')
                AND i.is_primary_key = 0
                AND i.type > 0
            ORDER BY i.name, ic.key_ordinal
            "#,
            table.schema, table.name
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
            WHERE fk.parent_object_id = OBJECT_ID('[{}].[{}]')
            ORDER BY fk.name, fkc.constraint_column_id
            "#,
            table.schema, table.name
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

        let sql = format!(
            r#"
            SELECT
                cc.name AS constraint_name,
                cc.definition
            FROM sys.check_constraints cc
            WHERE cc.parent_object_id = OBJECT_ID('[{}].[{}]')
            "#,
            table.schema, table.name
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

        let sql = format!("SELECT COUNT(*) FROM [{}].[{}]", schema, table);

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
