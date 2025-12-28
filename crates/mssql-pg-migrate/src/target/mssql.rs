//! MSSQL target database operations.
//!
//! This module implements `TargetPool` for MSSQL, enabling MSSQL
//! to be used as a target database for bidirectional migrations.

use crate::config::TargetConfig;
use crate::error::{MigrateError, Result};
use crate::source::{CheckConstraint, ForeignKey, Index, Table};
use crate::target::{SqlValue, TargetPool};
use crate::typemap::postgres_to_mssql;
use async_trait::async_trait;
use bb8::{Pool, PooledConnection};
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::{debug, info, warn};

/// Maximum rows per INSERT statement for MSSQL.
/// MSSQL has a limit of 1000 rows per INSERT VALUES statement.
const MAX_ROWS_PER_INSERT: usize = 1000;

/// Connection manager for bb8 pool with tiberius for MSSQL target.
#[derive(Clone)]
struct TiberiusTargetConnectionManager {
    config: TargetConfig,
}

impl TiberiusTargetConnectionManager {
    fn new(config: TargetConfig) -> Self {
        Self { config }
    }

    fn build_config(&self) -> Config {
        let mut config = Config::new();
        config.host(&self.config.host);
        config.port(self.config.port);
        config.database(&self.config.database);
        config.authentication(AuthMethod::sql_server(
            &self.config.user,
            &self.config.password,
        ));

        // Map ssl_mode to MSSQL encryption settings
        match self.config.ssl_mode.to_lowercase().as_str() {
            "disable" => {
                config.encryption(EncryptionLevel::NotSupported);
            }
            _ => {
                config.trust_cert();
                config.encryption(EncryptionLevel::Required);
            }
        }

        config
    }
}

#[async_trait]
impl bb8::ManageConnection for TiberiusTargetConnectionManager {
    type Connection = Client<Compat<TcpStream>>;
    type Error = tiberius::error::Error;

    async fn connect(&self) -> std::result::Result<Self::Connection, Self::Error> {
        let config = self.build_config();
        let tcp = TcpStream::connect(config.get_addr()).await.map_err(|e| {
            tiberius::error::Error::Io {
                kind: e.kind(),
                message: e.to_string(),
            }
        })?;

        tcp.set_nodelay(true).ok();

        Client::connect(config, tcp.compat_write()).await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> std::result::Result<(), Self::Error> {
        conn.simple_query("SELECT 1").await?.into_row().await?;
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

/// MSSQL target pool implementation.
pub struct MssqlTargetPool {
    pool: Pool<TiberiusTargetConnectionManager>,
}

impl MssqlTargetPool {
    /// Create a new MSSQL target pool from TargetConfig.
    pub async fn new(config: &TargetConfig, max_conns: u32) -> Result<Self> {
        let manager = TiberiusTargetConnectionManager::new(config.clone());
        let pool = Pool::builder()
            .max_size(max_conns)
            .build(manager)
            .await
            .map_err(|e| MigrateError::pool(e, "creating MSSQL target pool"))?;

        // Test connection
        {
            let mut conn = pool
                .get()
                .await
                .map_err(|e| MigrateError::pool(e, "testing MSSQL target connection"))?;
            conn.simple_query("SELECT 1").await?;
        }

        info!(
            "Connected to MSSQL target: {}:{}/{}",
            config.host, config.port, config.database
        );

        Ok(Self { pool })
    }

    /// Get a connection from the pool.
    async fn get_conn(
        &self,
    ) -> Result<PooledConnection<'_, TiberiusTargetConnectionManager>> {
        self.pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MSSQL target connection"))
    }

    /// Quote an MSSQL identifier with brackets.
    fn quote_ident(name: &str) -> String {
        format!("[{}]", name.replace(']', "]]"))
    }

    /// Format a SqlValue for MSSQL INSERT statement.
    fn format_value(value: &SqlValue) -> String {
        match value {
            SqlValue::Null(_) => "NULL".to_string(),
            SqlValue::Bool(b) => if *b { "1" } else { "0" }.to_string(),
            SqlValue::I16(i) => i.to_string(),
            SqlValue::I32(i) => i.to_string(),
            SqlValue::I64(i) => i.to_string(),
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

    /// Check if a table has an identity column.
    async fn has_identity_column(&self, schema: &str, table: &str) -> Result<bool> {
        let mut conn = self.get_conn().await?;
        let query = format!(
            r#"SELECT COUNT(*)
               FROM sys.columns c
               JOIN sys.tables t ON c.object_id = t.object_id
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = '{}' AND t.name = '{}' AND c.is_identity = 1"#,
            schema.replace('\'', "''"),
            table.replace('\'', "''")
        );

        let result = conn.simple_query(&query).await?;
        if let Some(row) = result.into_first_result().await?.into_iter().next() {
            let count: i32 = row.get(0).unwrap_or(0);
            return Ok(count > 0);
        }
        Ok(false)
    }
}

#[async_trait]
impl TargetPool for MssqlTargetPool {
    async fn create_schema(&self, schema: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let query = format!(
            "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{}') EXEC('CREATE SCHEMA {}')",
            schema.replace('\'', "''"),
            Self::quote_ident(schema)
        );
        conn.execute(&query, &[]).await?;
        debug!("Created schema: {}", schema);
        Ok(())
    }

    async fn create_table(&self, table: &Table, target_schema: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;

        // Generate column definitions
        let col_defs: Vec<String> = table
            .columns
            .iter()
            .map(|c| {
                let mapping = postgres_to_mssql(
                    &c.data_type,
                    c.max_length,
                    c.precision,
                    c.scale,
                );

                if mapping.is_lossy {
                    if let Some(warning) = &mapping.warning {
                        warn!("Column {}.{}: {}", table.name, c.name, warning);
                    }
                }

                let null_clause = if c.is_nullable { "NULL" } else { "NOT NULL" };
                let identity_clause = if c.is_identity { " IDENTITY(1,1)" } else { "" };

                format!(
                    "{} {}{}  {}",
                    Self::quote_ident(&c.name),
                    mapping.target_type,
                    identity_clause,
                    null_clause
                )
            })
            .collect();

        let ddl = format!(
            "CREATE TABLE {}.{} (\n    {}\n)",
            Self::quote_ident(target_schema),
            Self::quote_ident(&table.name),
            col_defs.join(",\n    ")
        );

        conn.execute(&ddl, &[]).await?;
        debug!("Created table: {}.{}", target_schema, table.name);
        Ok(())
    }

    async fn create_table_unlogged(&self, table: &Table, target_schema: &str) -> Result<()> {
        // MSSQL doesn't have UNLOGGED tables; use regular CREATE TABLE
        self.create_table(table, target_schema).await
    }

    async fn drop_table(&self, schema: &str, table: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let query = format!(
            "IF OBJECT_ID(N'{}.{}', 'U') IS NOT NULL DROP TABLE {}.{}",
            schema.replace('\'', "''"),
            table.replace('\'', "''"),
            Self::quote_ident(schema),
            Self::quote_ident(table)
        );
        conn.execute(&query, &[]).await?;
        debug!("Dropped table: {}.{}", schema, table);
        Ok(())
    }

    async fn truncate_table(&self, schema: &str, table: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let query = format!(
            "TRUNCATE TABLE {}.{}",
            Self::quote_ident(schema),
            Self::quote_ident(table)
        );
        conn.execute(&query, &[]).await?;
        debug!("Truncated table: {}.{}", schema, table);
        Ok(())
    }

    async fn table_exists(&self, schema: &str, table: &str) -> Result<bool> {
        let mut conn = self.get_conn().await?;
        let query = format!(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}'",
            schema.replace('\'', "''"),
            table.replace('\'', "''")
        );

        let result = conn.simple_query(&query).await?;
        if let Some(row) = result.into_first_result().await?.into_iter().next() {
            let count: i32 = row.get(0).unwrap_or(0);
            return Ok(count > 0);
        }
        Ok(false)
    }

    async fn create_primary_key(&self, table: &Table, target_schema: &str) -> Result<()> {
        if table.primary_key.is_empty() {
            return Ok(());
        }

        let mut conn = self.get_conn().await?;
        let pk_cols: Vec<String> = table.primary_key.iter().map(|c| Self::quote_ident(c)).collect();
        let pk_name = format!("PK_{}_{}", target_schema, table.name);

        let query = format!(
            "ALTER TABLE {}.{} ADD CONSTRAINT {} PRIMARY KEY ({})",
            Self::quote_ident(target_schema),
            Self::quote_ident(&table.name),
            Self::quote_ident(&pk_name),
            pk_cols.join(", ")
        );

        conn.execute(&query, &[]).await?;
        debug!("Created primary key on {}.{}", target_schema, table.name);
        Ok(())
    }

    async fn create_index(&self, table: &Table, idx: &Index, target_schema: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let idx_cols: Vec<String> = idx.columns.iter().map(|c| Self::quote_ident(c)).collect();

        let unique = if idx.is_unique { "UNIQUE " } else { "" };
        let clustered = if idx.is_clustered { "CLUSTERED " } else { "NONCLUSTERED " };

        let query = format!(
            "CREATE {}{}INDEX {} ON {}.{} ({})",
            unique,
            clustered,
            Self::quote_ident(&idx.name),
            Self::quote_ident(target_schema),
            Self::quote_ident(&table.name),
            idx_cols.join(", ")
        );

        conn.execute(&query, &[]).await?;
        debug!("Created index {} on {}.{}", idx.name, target_schema, table.name);
        Ok(())
    }

    async fn drop_non_pk_indexes(&self, schema: &str, table: &str) -> Result<Vec<String>> {
        let mut conn = self.get_conn().await?;

        // Get list of non-PK indexes
        let query = format!(
            r#"SELECT i.name
               FROM sys.indexes i
               JOIN sys.tables t ON i.object_id = t.object_id
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = '{}' AND t.name = '{}'
                 AND i.is_primary_key = 0 AND i.type > 0"#,
            schema.replace('\'', "''"),
            table.replace('\'', "''")
        );

        let result = conn.simple_query(&query).await?;
        let rows = result.into_first_result().await?;
        let mut dropped_indexes = Vec::new();

        for row in rows {
            if let Some(name) = row.get::<&str, _>(0) {
                let drop_query = format!(
                    "DROP INDEX {} ON {}.{}",
                    Self::quote_ident(name),
                    Self::quote_ident(schema),
                    Self::quote_ident(table)
                );
                conn.execute(&drop_query, &[]).await?;
                dropped_indexes.push(name.to_string());
            }
        }

        debug!("Dropped {} indexes on {}.{}", dropped_indexes.len(), schema, table);
        Ok(dropped_indexes)
    }

    async fn create_foreign_key(
        &self,
        table: &Table,
        fk: &ForeignKey,
        target_schema: &str,
    ) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let fk_cols: Vec<String> = fk.columns.iter().map(|c| Self::quote_ident(c)).collect();
        let ref_cols: Vec<String> = fk.ref_columns.iter().map(|c| Self::quote_ident(c)).collect();

        let query = format!(
            "ALTER TABLE {}.{} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}.{} ({}) ON DELETE {} ON UPDATE {}",
            Self::quote_ident(target_schema),
            Self::quote_ident(&table.name),
            Self::quote_ident(&fk.name),
            fk_cols.join(", "),
            Self::quote_ident(&fk.ref_schema),
            Self::quote_ident(&fk.ref_table),
            ref_cols.join(", "),
            fk.on_delete.replace('_', " "),
            fk.on_update.replace('_', " ")
        );

        conn.execute(&query, &[]).await?;
        debug!("Created foreign key {} on {}.{}", fk.name, target_schema, table.name);
        Ok(())
    }

    async fn create_check_constraint(
        &self,
        table: &Table,
        chk: &CheckConstraint,
        target_schema: &str,
    ) -> Result<()> {
        let mut conn = self.get_conn().await?;

        let query = format!(
            "ALTER TABLE {}.{} ADD CONSTRAINT {} {}",
            Self::quote_ident(target_schema),
            Self::quote_ident(&table.name),
            Self::quote_ident(&chk.name),
            chk.definition
        );

        conn.execute(&query, &[]).await?;
        debug!("Created check constraint {} on {}.{}", chk.name, target_schema, table.name);
        Ok(())
    }

    async fn has_primary_key(&self, schema: &str, table: &str) -> Result<bool> {
        let mut conn = self.get_conn().await?;
        let query = format!(
            r#"SELECT COUNT(*)
               FROM sys.indexes
               WHERE object_id = OBJECT_ID(N'{}.{}') AND is_primary_key = 1"#,
            schema.replace('\'', "''"),
            table.replace('\'', "''")
        );

        let result = conn.simple_query(&query).await?;
        if let Some(row) = result.into_first_result().await?.into_iter().next() {
            let count: i32 = row.get(0).unwrap_or(0);
            return Ok(count > 0);
        }
        Ok(false)
    }

    async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        let mut conn = self.get_conn().await?;
        let query = format!(
            "SELECT CAST(COUNT(*) AS BIGINT) FROM {}.{} WITH (NOLOCK)",
            Self::quote_ident(schema),
            Self::quote_ident(table)
        );

        let result = conn.simple_query(&query).await?;
        if let Some(row) = result.into_first_result().await?.into_iter().next() {
            let count: i64 = row.get(0).unwrap_or(0);
            return Ok(count);
        }
        Ok(0)
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

        let mut conn = self.get_conn().await?;
        let has_identity = self.has_identity_column(schema, table).await?;
        let total_rows = rows.len() as u64;

        // Enable IDENTITY_INSERT if needed
        if has_identity {
            let enable_identity = format!(
                "SET IDENTITY_INSERT {}.{} ON",
                Self::quote_ident(schema),
                Self::quote_ident(table)
            );
            conn.execute(&enable_identity, &[]).await?;
        }

        // Build column list
        let col_list: Vec<String> = cols.iter().map(|c| Self::quote_ident(c)).collect();
        let col_str = col_list.join(", ");

        // Insert in batches
        for chunk in rows.chunks(MAX_ROWS_PER_INSERT) {
            let values: Vec<String> = chunk
                .iter()
                .map(|row| {
                    let row_values: Vec<String> = row.iter().map(Self::format_value).collect();
                    format!("({})", row_values.join(", "))
                })
                .collect();

            let insert_sql = format!(
                "INSERT INTO {}.{} ({}) VALUES {}",
                Self::quote_ident(schema),
                Self::quote_ident(table),
                col_str,
                values.join(", ")
            );

            conn.execute(&insert_sql, &[]).await?;
        }

        // Disable IDENTITY_INSERT if we enabled it
        if has_identity {
            let disable_identity = format!(
                "SET IDENTITY_INSERT {}.{} OFF",
                Self::quote_ident(schema),
                Self::quote_ident(table)
            );
            conn.execute(&disable_identity, &[]).await?;
        }

        Ok(total_rows)
    }

    async fn upsert_chunk(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        pk_cols: &[String],
        rows: Vec<Vec<SqlValue>>,
        _writer_id: usize,
        row_hash_column: Option<&str>,
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let mut conn = self.get_conn().await?;
        let has_identity = self.has_identity_column(schema, table).await?;
        let total_rows = rows.len() as u64;

        // Enable IDENTITY_INSERT if needed
        if has_identity {
            let enable_identity = format!(
                "SET IDENTITY_INSERT {}.{} ON",
                Self::quote_ident(schema),
                Self::quote_ident(table)
            );
            conn.execute(&enable_identity, &[]).await?;
        }

        // Build column list
        let col_list: Vec<String> = cols.iter().map(|c| Self::quote_ident(c)).collect();
        let col_str = col_list.join(", ");

        // Process in batches
        for chunk in rows.chunks(MAX_ROWS_PER_INSERT) {
            let values: Vec<String> = chunk
                .iter()
                .map(|row| {
                    let row_values: Vec<String> = row.iter().map(Self::format_value).collect();
                    format!("({})", row_values.join(", "))
                })
                .collect();

            // Build MERGE statement
            let join_condition: Vec<String> = pk_cols
                .iter()
                .map(|pk| format!("target.{} = source.{}", Self::quote_ident(pk), Self::quote_ident(pk)))
                .collect();

            let update_cols: Vec<String> = cols
                .iter()
                .filter(|c| !pk_cols.contains(c))
                .map(|c| format!("{} = source.{}", Self::quote_ident(c), Self::quote_ident(c)))
                .collect();

            let source_cols: Vec<String> = cols
                .iter()
                .map(|c| format!("source.{}", Self::quote_ident(c)))
                .collect();

            // Build the update condition
            let update_condition = if let Some(hash_col) = row_hash_column {
                // Only update if hash differs
                format!(
                    " AND (target.{} IS NULL OR target.{} <> source.{})",
                    Self::quote_ident(hash_col),
                    Self::quote_ident(hash_col),
                    Self::quote_ident(hash_col)
                )
            } else {
                String::new()
            };

            let merge_sql = format!(
                r#"MERGE INTO {schema}.{table} AS target
                   USING (VALUES {values}) AS source ({cols})
                   ON {join_condition}
                   WHEN MATCHED{update_condition} THEN UPDATE SET {update_set}
                   WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({source_cols});"#,
                schema = Self::quote_ident(schema),
                table = Self::quote_ident(table),
                values = values.join(", "),
                cols = col_str,
                join_condition = join_condition.join(" AND "),
                update_condition = update_condition,
                update_set = update_cols.join(", "),
                source_cols = source_cols.join(", ")
            );

            conn.execute(&merge_sql, &[]).await?;
        }

        // Disable IDENTITY_INSERT if we enabled it
        if has_identity {
            let disable_identity = format!(
                "SET IDENTITY_INSERT {}.{} OFF",
                Self::quote_ident(schema),
                Self::quote_ident(table)
            );
            conn.execute(&disable_identity, &[]).await?;
        }

        Ok(total_rows)
    }

    async fn upsert_chunk_with_hash(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        pk_cols: &[String],
        rows: Vec<Vec<SqlValue>>,
        writer_id: usize,
        row_hash_column: Option<&str>,
    ) -> Result<u64> {
        // Same as upsert_chunk - hash filtering is done before calling this
        self.upsert_chunk(schema, table, cols, pk_cols, rows, writer_id, row_hash_column)
            .await
    }

    fn db_type(&self) -> &str {
        "mssql"
    }

    async fn close(&self) {
        // bb8 handles cleanup automatically
    }
}
