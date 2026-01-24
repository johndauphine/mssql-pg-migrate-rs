//! MySQL/MariaDB target writer implementation.
//!
//! Implements the `TargetWriter` trait for writing data to MySQL/MariaDB databases.
//! Uses mysql_async for connection pooling and LOAD DATA LOCAL INFILE for bulk loading.
//!
//! # Performance
//!
//! mysql_async provides significantly better performance through:
//! - LOAD DATA LOCAL INFILE for bulk data loading (5-10x faster than INSERT)
//! - Native MySQL protocol implementation
//! - Efficient connection pooling
//! - Falls back to batched INSERT when LOAD DATA is not supported

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, SslOpts, TxOpts};
use tracing::{debug, info, warn};

use crate::config::TargetConfig;
use crate::core::schema::{CheckConstraint, Column, ForeignKey, Index, Table};
use crate::core::traits::{TargetWriter, TypeMapper};
use crate::core::value::{Batch, SqlValue};
use crate::error::{MigrateError, Result};

/// MySQL target writer implementation using mysql_async.
///
/// Uses efficient batched INSERT operations for data writing.
pub struct MysqlWriter {
    pool: Pool,
    type_mapper: Option<Arc<dyn TypeMapper>>,
}

impl MysqlWriter {
    /// Create a new MySQL writer from configuration.
    pub async fn new(config: &TargetConfig, max_conns: usize) -> Result<Self> {
        let ssl_opts = match config.ssl_mode.to_lowercase().as_str() {
            "disable" => {
                warn!("MySQL TLS is disabled. Credentials will be transmitted in plaintext.");
                None
            }
            "prefer" => Some(SslOpts::default().with_danger_accept_invalid_certs(true)),
            "require" => Some(SslOpts::default().with_danger_accept_invalid_certs(true)),
            "verify-ca" | "verify_ca" => Some(SslOpts::default()),
            "verify-full" | "verify_identity" => Some(SslOpts::default()),
            _ => {
                warn!(
                    "Unknown ssl_mode '{}', defaulting to Preferred",
                    config.ssl_mode
                );
                Some(SslOpts::default().with_danger_accept_invalid_certs(true))
            }
        };

        let mut builder = OptsBuilder::default()
            .ip_or_hostname(&config.host)
            .tcp_port(config.port)
            .db_name(Some(&config.database))
            .user(Some(&config.user))
            .pass(Some(&config.password))
            // Use utf8mb4 for full Unicode support
            .init(vec!["SET NAMES utf8mb4"]);

        if let Some(ssl) = ssl_opts {
            builder = builder.ssl_opts(ssl);
        }

        let pool_opts =
            PoolOpts::new().with_constraints(PoolConstraints::new(1, max_conns).unwrap());

        let opts: Opts = builder.pool_opts(pool_opts).into();
        let pool = Pool::new(opts);

        // Test connection
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "creating MySQL target pool"))?;

        conn.query_drop("SELECT 1")
            .await
            .map_err(|e| MigrateError::pool(e, "testing MySQL target connection"))?;

        drop(conn);

        info!(
            "Connected to MySQL target: {}:{}/{}",
            config.host, config.port, config.database
        );

        Ok(Self {
            pool,
            type_mapper: None,
        })
    }

    /// Set the type mapper for schema creation.
    pub fn with_type_mapper(mut self, mapper: Arc<dyn TypeMapper>) -> Self {
        self.type_mapper = Some(mapper);
        self
    }

    /// Get a clone of the underlying connection pool.
    pub fn pool(&self) -> Pool {
        self.pool.clone()
    }

    /// Test the database connection.
    pub async fn test_connection(&self) -> Result<()> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "testing MySQL connection"))?;
        conn.query_drop("SELECT 1")
            .await
            .map_err(|e| MigrateError::pool(e, "testing MySQL connection"))?;
        Ok(())
    }

    /// Quote a MySQL identifier.
    fn quote_ident(name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }

    /// Qualify a table name with database.
    fn qualify_table(schema: &str, table: &str) -> String {
        format!("{}.{}", Self::quote_ident(schema), Self::quote_ident(table))
    }

    /// Map a source type to MySQL type.
    fn map_type(&self, col: &Column) -> String {
        if let Some(mapper) = &self.type_mapper {
            let mapping = mapper.map_column(col);
            if let Some(warning) = &mapping.warning {
                warn!("Column {}: {}", col.name, warning);
            }
            mapping.target_type
        } else {
            // No mapper provided - use default MSSQL to MySQL mapping
            use crate::dialect::mssql_to_mysql_basic;
            let target_type =
                mssql_to_mysql_basic(&col.data_type, col.max_length, col.precision, col.scale);
            if target_type.is_empty() {
                warn!(
                    "Unknown MSSQL type '{}' for column '{}', passing through",
                    col.data_type, col.name
                );
                col.data_type.to_uppercase()
            } else {
                target_type
            }
        }
    }

    /// Generate table DDL.
    fn generate_ddl(&self, table: &Table, target_schema: &str) -> String {
        let col_defs: Vec<String> = table
            .columns
            .iter()
            .map(|c| {
                let target_type = self.map_type(c);
                let null_clause = if c.is_nullable { "" } else { " NOT NULL" };
                format!(
                    "{} {}{}",
                    Self::quote_ident(&c.name),
                    target_type,
                    null_clause
                )
            })
            .collect();

        format!(
            "CREATE TABLE {} (\n    {}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
            Self::qualify_table(target_schema, &table.name),
            col_defs.join(",\n    ")
        )
    }

    /// Write batch using multi-row INSERT.
    async fn write_batch_insert(
        &self,
        conn: &mut Conn,
        schema: &str,
        table: &str,
        cols: &[String],
        batch: Batch,
    ) -> Result<u64> {
        let rows = batch.rows;
        if rows.is_empty() {
            return Ok(0);
        }

        let row_count = rows.len() as u64;
        let qualified_table = Self::qualify_table(schema, table);
        let col_list: Vec<String> = cols.iter().map(|c| Self::quote_ident(c)).collect();
        let col_list_str = col_list.join(", ");

        // MySQL max placeholders is 65535
        const MYSQL_MAX_PLACEHOLDERS: usize = 65535;
        let num_cols = cols.len();
        let max_rows_per_batch = if num_cols > 0 {
            MYSQL_MAX_PLACEHOLDERS / num_cols
        } else {
            return Ok(0);
        };

        // Process rows in sub-batches
        for chunk in rows.chunks(max_rows_per_batch) {
            let placeholders_per_row = format!("({})", vec!["?"; num_cols].join(", "));
            let all_placeholders: Vec<String> =
                std::iter::repeat_n(placeholders_per_row, chunk.len()).collect();

            let sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                qualified_table,
                col_list_str,
                all_placeholders.join(", ")
            );

            // Collect all values for binding
            let params: Vec<mysql_async::Value> = chunk
                .iter()
                .flat_map(|row| row.iter().map(sql_value_to_mysql))
                .collect();

            conn.exec_drop(&sql, params).await.map_err(|e| {
                MigrateError::transfer(&qualified_table, format!("INSERT batch: {}", e))
            })?;
        }

        debug!(
            "MySQL: wrote {} rows to {} using INSERT",
            row_count, qualified_table
        );
        Ok(row_count)
    }

    /// Write batch using LOAD DATA LOCAL INFILE for maximum performance.
    /// Returns Ok(Some(count)) on success, Ok(None) if LOAD DATA is not supported.
    async fn write_batch_load_data(
        &self,
        conn: &mut Conn,
        schema: &str,
        table: &str,
        cols: &[String],
        batch: Batch,
    ) -> Result<Option<u64>> {
        let rows = batch.rows;
        if rows.is_empty() {
            return Ok(Some(0));
        }

        let row_count = rows.len() as u64;
        let qualified_table = Self::qualify_table(schema, table);
        let col_list: Vec<String> = cols.iter().map(|c| Self::quote_ident(c)).collect();
        let col_list_str = col_list.join(", ");

        // Build CSV data for streaming to MySQL
        // Use tab-separated format with NULL represented as \N
        let csv_data: Vec<Bytes> = rows
            .iter()
            .map(|row| {
                let line = row
                    .iter()
                    .map(|v| Self::escape_csv_value(v))
                    .collect::<Vec<_>>()
                    .join("\t")
                    + "\n";
                Bytes::from(line)
            })
            .collect();

        // Set up one-time infile handler that streams our batch data
        // The handler must be Sync, so capture the Vec<Bytes> (which is Sync) and create the stream inside
        conn.set_infile_handler(async move {
            let stream: BoxStream<'static, std::io::Result<Bytes>> =
                stream::iter(csv_data.into_iter().map(Ok)).boxed();
            Ok(stream)
        });

        // Execute LOAD DATA LOCAL INFILE
        // The filename is arbitrary since our handler ignores it
        let sql = format!(
            r#"LOAD DATA LOCAL INFILE 'batch'
               INTO TABLE {}
               CHARACTER SET utf8mb4
               FIELDS TERMINATED BY '\t'
               LINES TERMINATED BY '\n'
               ({})"#,
            qualified_table, col_list_str
        );

        match conn.query_drop(&sql).await {
            Ok(_) => {
                debug!(
                    "MySQL: wrote {} rows to {} using LOAD DATA LOCAL INFILE",
                    row_count, qualified_table
                );
                Ok(Some(row_count))
            }
            Err(e) => {
                let err_str = e.to_string();
                // Check for common LOAD DATA errors:
                // - Error 1148: LOAD DATA LOCAL INFILE command is disabled
                // - Error 3948: Loading local data is disabled
                // - Error 2068: Local infile request rejected
                if err_str.contains("1148")
                    || err_str.contains("3948")
                    || err_str.contains("2068")
                    || err_str.contains("LOCAL INFILE")
                    || err_str.contains("local data")
                {
                    debug!("MySQL: LOAD DATA LOCAL INFILE not supported, will fall back to INSERT");
                    Ok(None) // Indicate fallback needed
                } else {
                    Err(MigrateError::transfer(
                        &qualified_table,
                        format!("LOAD DATA: {}", e),
                    ))
                }
            }
        }
    }

    /// Escape a value for tab-separated CSV format used by LOAD DATA.
    fn escape_csv_value(value: &SqlValue<'_>) -> String {
        match value {
            SqlValue::Null(_) => "\\N".to_string(),
            SqlValue::Bool(b) => {
                if *b {
                    "1".to_string()
                } else {
                    "0".to_string()
                }
            }
            SqlValue::I16(i) => i.to_string(),
            SqlValue::I32(i) => i.to_string(),
            SqlValue::I64(i) => i.to_string(),
            SqlValue::F32(f) => f.to_string(),
            SqlValue::F64(f) => f.to_string(),
            SqlValue::Text(s) => Self::escape_csv_string(s),
            SqlValue::Bytes(b) => {
                // Encode binary data as hex for MySQL
                format!("0x{}", hex::encode(b.as_ref()))
            }
            SqlValue::Uuid(u) => u.to_string(),
            SqlValue::Decimal(d) => d.to_string(),
            SqlValue::DateTime(dt) => dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
            SqlValue::DateTimeOffset(dto) => {
                dto.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string()
            }
            SqlValue::Date(d) => d.format("%Y-%m-%d").to_string(),
            SqlValue::Time(t) => t.format("%H:%M:%S%.6f").to_string(),
        }
    }

    /// Escape a string for tab-separated CSV format.
    /// MySQL LOAD DATA expects: \t for tab, \n for newline, \\ for backslash, \N for NULL
    fn escape_csv_string(s: &str) -> String {
        let mut result = String::with_capacity(s.len());
        for c in s.chars() {
            match c {
                '\t' => result.push_str("\\t"),
                '\n' => result.push_str("\\n"),
                '\r' => result.push_str("\\r"),
                '\\' => result.push_str("\\\\"),
                '\0' => result.push_str("\\0"),
                _ => result.push(c),
            }
        }
        result
    }
}

#[async_trait]
impl TargetWriter for MysqlWriter {
    async fn create_schema(&self, schema: &str) -> Result<()> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        let sql = format!(
            "CREATE DATABASE IF NOT EXISTS {} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
            Self::quote_ident(schema)
        );
        conn.query_drop(&sql).await?;

        debug!("Created database '{}'", schema);
        Ok(())
    }

    async fn create_table(&self, table: &Table, target_schema: &str) -> Result<()> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        let ddl = self.generate_ddl(table, target_schema);
        conn.query_drop(&ddl).await?;

        debug!("Created table {}.{}", target_schema, table.name);
        Ok(())
    }

    async fn create_table_unlogged(&self, table: &Table, target_schema: &str) -> Result<()> {
        // MySQL doesn't have UNLOGGED tables
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        let ddl = self.generate_ddl(table, target_schema);
        conn.query_drop(&ddl).await?;

        debug!(
            "Created table {}.{} (MySQL doesn't support UNLOGGED)",
            target_schema, table.name
        );
        Ok(())
    }

    async fn drop_table(&self, schema: &str, table: &str) -> Result<()> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        let sql = format!(
            "DROP TABLE IF EXISTS {}",
            Self::qualify_table(schema, table)
        );
        conn.query_drop(&sql).await?;

        debug!("Dropped table {}.{}", schema, table);
        Ok(())
    }

    async fn table_exists(&self, schema: &str, table: &str) -> Result<bool> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        let sql = r#"
            SELECT COUNT(*) as cnt FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        "#;

        let count: Option<i64> = conn
            .exec_first(sql, (schema, table))
            .await
            .map_err(|e| MigrateError::pool(e, "checking table existence"))?;

        Ok(count.unwrap_or(0) > 0)
    }

    async fn create_primary_key(&self, table: &Table, target_schema: &str) -> Result<()> {
        if table.primary_key.is_empty() {
            return Ok(());
        }

        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        let pk_cols: Vec<String> = table
            .primary_key
            .iter()
            .map(|c| Self::quote_ident(c))
            .collect();

        let sql = format!(
            "ALTER TABLE {} ADD PRIMARY KEY ({})",
            Self::qualify_table(target_schema, &table.name),
            pk_cols.join(", ")
        );

        conn.query_drop(&sql).await?;
        debug!("Created primary key on {}.{}", target_schema, table.name);
        Ok(())
    }

    async fn create_index(&self, table: &Table, idx: &Index, target_schema: &str) -> Result<()> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        // Build column list with prefix lengths for TEXT/BLOB types
        let idx_cols: Vec<String> = idx
            .columns
            .iter()
            .map(|col_name| {
                let quoted = Self::quote_ident(col_name);
                if let Some(col) = table.columns.iter().find(|c| &c.name == col_name) {
                    let dtype = col.data_type.to_lowercase();
                    if dtype.contains("text") || dtype.contains("blob") {
                        return format!("{}(255)", quoted);
                    }
                }
                quoted
            })
            .collect();

        let unique = if idx.is_unique { "UNIQUE " } else { "" };

        let sql = format!(
            "CREATE {}INDEX {} ON {} ({})",
            unique,
            Self::quote_ident(&idx.name),
            Self::qualify_table(target_schema, &table.name),
            idx_cols.join(", ")
        );

        conn.query_drop(&sql).await?;
        debug!(
            "Created index {} on {}.{}",
            idx.name, target_schema, table.name
        );
        Ok(())
    }

    async fn drop_non_pk_indexes(&self, schema: &str, table: &str) -> Result<Vec<String>> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        let query = r#"
            SELECT DISTINCT INDEX_NAME
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME != 'PRIMARY'
        "#;

        let indexes: Vec<String> = conn.exec(query, (schema, table)).await?;

        let mut dropped_indexes = Vec::new();

        for name in indexes {
            let drop_sql = format!(
                "DROP INDEX {} ON {}",
                Self::quote_ident(&name),
                Self::qualify_table(schema, table)
            );
            conn.query_drop(&drop_sql).await?;
            dropped_indexes.push(name);
        }

        debug!(
            "Dropped {} indexes on {}.{}",
            dropped_indexes.len(),
            schema,
            table
        );
        Ok(dropped_indexes)
    }

    async fn create_foreign_key(
        &self,
        table: &Table,
        fk: &ForeignKey,
        target_schema: &str,
    ) -> Result<()> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        let fk_cols: Vec<String> = fk.columns.iter().map(|c| Self::quote_ident(c)).collect();
        let ref_cols: Vec<String> = fk
            .ref_columns
            .iter()
            .map(|c| Self::quote_ident(c))
            .collect();

        let sql = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}.{} ({}) ON DELETE {} ON UPDATE {}",
            Self::qualify_table(target_schema, &table.name),
            Self::quote_ident(&fk.name),
            fk_cols.join(", "),
            Self::quote_ident(&fk.ref_schema),
            Self::quote_ident(&fk.ref_table),
            ref_cols.join(", "),
            map_referential_action(&fk.on_delete),
            map_referential_action(&fk.on_update)
        );

        conn.query_drop(&sql).await?;
        debug!(
            "Created foreign key {} on {}.{}",
            fk.name, target_schema, table.name
        );
        Ok(())
    }

    async fn create_check_constraint(
        &self,
        table: &Table,
        chk: &CheckConstraint,
        target_schema: &str,
    ) -> Result<()> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        // MySQL 8.0.16+ supports CHECK constraints
        let sql = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} {}",
            Self::qualify_table(target_schema, &table.name),
            Self::quote_ident(&chk.name),
            convert_check_definition(&chk.definition)
        );

        match conn.query_drop(&sql).await {
            Ok(_) => {
                debug!(
                    "Created check constraint {} on {}.{}",
                    chk.name, target_schema, table.name
                );
            }
            Err(e) => {
                warn!(
                    "Failed to create check constraint {} on {}.{}: {}",
                    chk.name, target_schema, table.name, e
                );
            }
        }
        Ok(())
    }

    async fn has_primary_key(&self, schema: &str, table: &str) -> Result<bool> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        let sql = r#"
            SELECT COUNT(*) as cnt
            FROM information_schema.TABLE_CONSTRAINTS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_TYPE = 'PRIMARY KEY'
        "#;

        let count: Option<i64> = conn.exec_first(sql, (schema, table)).await?;
        Ok(count.unwrap_or(0) > 0)
    }

    async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        let sql = format!(
            "SELECT COUNT(*) as cnt FROM {}",
            Self::qualify_table(schema, table)
        );
        let count: Option<i64> = conn.query_first(&sql).await?;
        Ok(count.unwrap_or(0))
    }

    async fn reset_sequence(&self, schema: &str, table: &Table) -> Result<()> {
        if table.primary_key.is_empty() {
            return Ok(());
        }

        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        for pk_col in &table.primary_key {
            // Check if this is an auto_increment column
            let check_sql = r#"
                SELECT COLUMN_NAME FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ? AND EXTRA LIKE '%auto_increment%'
            "#;

            let result: Option<String> = conn
                .exec_first(check_sql, (schema, &table.name, pk_col))
                .await?;

            if result.is_some() {
                // Get max value and set auto_increment
                let max_sql = format!(
                    "SELECT COALESCE(MAX({}), 0) as max_val FROM {}",
                    Self::quote_ident(pk_col),
                    Self::qualify_table(schema, &table.name)
                );

                let max_val: Option<i64> = conn.query_first(&max_sql).await?;
                let next_val = max_val.unwrap_or(0) + 1;

                let reset_sql = format!(
                    "ALTER TABLE {} AUTO_INCREMENT = {}",
                    Self::qualify_table(schema, &table.name),
                    next_val
                );
                conn.query_drop(&reset_sql).await?;

                debug!(
                    "Reset AUTO_INCREMENT to {} for {}.{}",
                    next_val, schema, table.name
                );
            }
        }

        Ok(())
    }

    async fn set_table_logged(&self, _schema: &str, _table: &str) -> Result<()> {
        // MySQL doesn't have UNLOGGED tables
        Ok(())
    }

    async fn set_table_unlogged(&self, _schema: &str, _table: &str) -> Result<()> {
        // MySQL doesn't have UNLOGGED tables
        Ok(())
    }

    async fn write_batch(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        batch: Batch,
    ) -> Result<u64> {
        if batch.rows.is_empty() {
            return Ok(0);
        }

        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        // Try LOAD DATA LOCAL INFILE first for better performance
        // Clone the batch for potential fallback
        let batch_clone = Batch {
            rows: batch.rows.clone(),
            last_key: batch.last_key.clone(),
            is_last: batch.is_last,
        };

        match self
            .write_batch_load_data(&mut conn, schema, table, cols, batch)
            .await?
        {
            Some(count) => Ok(count),
            None => {
                // LOAD DATA not supported, fall back to INSERT
                // Need to get a fresh connection since the previous one might be in a bad state
                drop(conn);
                let mut conn =
                    self.pool.get_conn().await.map_err(|e| {
                        MigrateError::pool(e, "getting MySQL connection for fallback")
                    })?;
                self.write_batch_insert(&mut conn, schema, table, cols, batch_clone)
                    .await
            }
        }
    }

    async fn upsert_batch(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        pk_cols: &[String],
        batch: Batch,
        writer_id: usize,
        partition_id: Option<i32>,
    ) -> Result<u64> {
        let rows = batch.rows;
        if rows.is_empty() {
            return Ok(0);
        }

        if pk_cols.is_empty() {
            return Err(MigrateError::NoPrimaryKey(table.to_string()));
        }

        let row_count = rows.len() as u64;
        let qualified_target = Self::qualify_table(schema, table);
        let col_list: Vec<String> = cols.iter().map(|c| Self::quote_ident(c)).collect();
        let col_list_str = col_list.join(", ");

        // Create staging table name
        let staging_name = match partition_id {
            Some(pid) => format!("_staging_{}_p{}_{}", table, pid, writer_id),
            None => format!("_staging_{}_{}", table, writer_id),
        };
        let staging_qualified = Self::qualify_table(schema, &staging_name);

        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        // Start transaction
        let mut tx = conn.start_transaction(TxOpts::default()).await?;

        // Drop if exists, create staging as copy of target structure
        let drop_sql = format!("DROP TABLE IF EXISTS {}", staging_qualified);
        tx.query_drop(&drop_sql).await?;

        let create_sql = format!(
            "CREATE TABLE {} LIKE {}",
            staging_qualified, qualified_target
        );
        tx.query_drop(&create_sql).await?;

        // Insert into staging table using batched INSERT
        const MYSQL_MAX_PLACEHOLDERS: usize = 65535;
        let num_cols = cols.len();
        let max_rows_per_batch = if num_cols > 0 {
            MYSQL_MAX_PLACEHOLDERS / num_cols
        } else {
            return Ok(0);
        };

        for chunk in rows.chunks(max_rows_per_batch) {
            let placeholders_per_row = format!("({})", vec!["?"; num_cols].join(", "));
            let all_placeholders: Vec<String> =
                std::iter::repeat_n(placeholders_per_row, chunk.len()).collect();

            let sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                staging_qualified,
                col_list_str,
                all_placeholders.join(", ")
            );

            let params: Vec<mysql_async::Value> = chunk
                .iter()
                .flat_map(|row| row.iter().map(sql_value_to_mysql))
                .collect();

            tx.exec_drop(&sql, params).await?;
        }

        // Build INSERT ... ON DUPLICATE KEY UPDATE from staging
        let select_cols = cols
            .iter()
            .map(|c| format!("s.{}", Self::quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");

        let non_pk_cols: Vec<_> = cols.iter().filter(|c| !pk_cols.contains(c)).collect();

        let upsert_sql = if non_pk_cols.is_empty() {
            format!(
                "INSERT IGNORE INTO {} ({}) SELECT {} FROM {} AS s",
                qualified_target, col_list_str, select_cols, staging_qualified
            )
        } else {
            let update_set = non_pk_cols
                .iter()
                .map(|c| {
                    format!(
                        "{} = VALUES({})",
                        Self::quote_ident(c),
                        Self::quote_ident(c)
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");

            format!(
                "INSERT INTO {} ({}) SELECT {} FROM {} AS s ON DUPLICATE KEY UPDATE {}",
                qualified_target, col_list_str, select_cols, staging_qualified, update_set
            )
        };

        tx.query_drop(&upsert_sql).await?;

        // Drop staging
        let drop_staging_sql = format!("DROP TABLE IF EXISTS {}", staging_qualified);
        tx.query_drop(&drop_staging_sql).await?;

        // Commit transaction
        tx.commit().await?;

        debug!("MySQL: upserted {} rows to {}", row_count, qualified_target);
        Ok(row_count)
    }

    fn db_type(&self) -> &str {
        "mysql"
    }

    async fn close(&self) {
        self.pool.clone().disconnect().await.ok();
    }

    fn supports_direct_copy(&self) -> bool {
        false
    }
}

/// Convert SqlValue to mysql_async::Value.
fn sql_value_to_mysql(value: &SqlValue<'_>) -> mysql_async::Value {
    match value {
        SqlValue::Null(_) => mysql_async::Value::NULL,
        SqlValue::Bool(b) => mysql_async::Value::from(*b),
        SqlValue::I16(i) => mysql_async::Value::from(*i),
        SqlValue::I32(i) => mysql_async::Value::from(*i),
        SqlValue::I64(i) => mysql_async::Value::from(*i),
        SqlValue::F32(f) => mysql_async::Value::from(*f),
        SqlValue::F64(f) => mysql_async::Value::from(*f),
        SqlValue::Text(s) => mysql_async::Value::from(s.as_ref()),
        SqlValue::Bytes(b) => mysql_async::Value::from(b.as_ref()),
        SqlValue::Uuid(u) => mysql_async::Value::from(u.to_string()),
        SqlValue::Decimal(d) => mysql_async::Value::from(d.to_string()),
        SqlValue::DateTime(dt) => mysql_async::Value::from(*dt),
        SqlValue::DateTimeOffset(dto) => mysql_async::Value::from(dto.naive_utc()),
        SqlValue::Date(d) => mysql_async::Value::from(*d),
        SqlValue::Time(t) => mysql_async::Value::from(*t),
    }
}

/// Map referential action.
fn map_referential_action(action: &str) -> &str {
    match action.to_uppercase().as_str() {
        "CASCADE" => "CASCADE",
        "SET_NULL" | "SET NULL" => "SET NULL",
        "SET_DEFAULT" | "SET DEFAULT" => "SET DEFAULT",
        "NO_ACTION" | "NO ACTION" => "NO ACTION",
        "RESTRICT" => "RESTRICT",
        _ => "NO ACTION",
    }
}

/// Convert check definition for MySQL.
fn convert_check_definition(def: &str) -> String {
    let mut result = def.to_string();

    // Convert SQL Server bracket-quoted identifiers to MySQL backticks
    while let Some(start) = result.find('[') {
        if let Some(end) = result[start..].find(']') {
            let col_name = &result[start + 1..start + end];
            result = format!(
                "{}`{}`{}",
                &result[..start],
                col_name,
                &result[start + end + 1..]
            );
        } else {
            break;
        }
    }

    // Convert PostgreSQL double-quotes to MySQL backticks
    result = result.replace('"', "`");

    // Convert function names
    result = result.replace("getdate()", "NOW()");
    result = result.replace("GETDATE()", "NOW()");

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_ident() {
        assert_eq!(MysqlWriter::quote_ident("name"), "`name`");
        assert_eq!(MysqlWriter::quote_ident("table`name"), "`table``name`");
    }

    #[test]
    fn test_convert_check_definition() {
        assert_eq!(
            convert_check_definition("CHECK ([age] >= 0)"),
            "CHECK (`age` >= 0)"
        );
        assert_eq!(
            convert_check_definition("CHECK (\"status\" IN ('A', 'B'))"),
            "CHECK (`status` IN ('A', 'B'))"
        );
    }

    #[test]
    fn test_map_referential_action() {
        assert_eq!(map_referential_action("CASCADE"), "CASCADE");
        assert_eq!(map_referential_action("SET_NULL"), "SET NULL");
        assert_eq!(map_referential_action("NO_ACTION"), "NO ACTION");
    }
}
