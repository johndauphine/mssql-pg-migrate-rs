//! MSSQL target writer implementation.
//!
//! Implements the `TargetWriter` trait for writing data to MSSQL databases.
//! Uses Tiberius with bb8 connection pooling and TDS bulk insert for
//! high-performance data loading.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bb8::{Pool, PooledConnection};
use chrono::Timelike;
use tiberius::{
    AuthMethod as TiberiusAuthMethod, Client, ColumnData, Config, EncryptionLevel, ToSql, TokenRow,
};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::{debug, info, warn};

#[cfg(feature = "kerberos")]
use crate::config::AuthMethod as ConfigAuthMethod;
use crate::config::TargetConfig;
use crate::core::schema::{CheckConstraint, Column, ForeignKey, Index, Table};
use crate::core::traits::{TargetWriter, TypeMapper};
use crate::core::value::{Batch, SqlNullType, SqlValue};
use crate::error::{MigrateError, Result};

/// Maximum string length (in bytes) for TDS bulk insert.
const BULK_INSERT_STRING_LIMIT: usize = 65535;

/// Maximum number of retry attempts for deadlock errors.
const DEADLOCK_MAX_RETRIES: u32 = 5;

/// Base delay between deadlock retry attempts in milliseconds.
const DEADLOCK_RETRY_DELAY_MS: u64 = 200;

/// Maximum TDS packet size.
const TDS_MAX_PACKET_SIZE: u32 = 32767;

/// Connection pool timeouts.
const POOL_CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const POOL_MAX_LIFETIME: Duration = Duration::from_secs(1800);
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// Connection manager for bb8 pool with Tiberius.
#[derive(Clone)]
struct TiberiusConnectionManager {
    config: TargetConfig,
}

impl TiberiusConnectionManager {
    fn new(config: TargetConfig) -> Self {
        Self { config }
    }

    fn build_config(&self) -> Config {
        let mut config = Config::new();
        config.host(&self.config.host);
        config.port(self.config.port);
        config.database(&self.config.database);

        match self.config.auth {
            #[cfg(feature = "kerberos")]
            ConfigAuthMethod::Kerberos => {
                info!("Using Kerberos authentication via Tiberius GSSAPI for MSSQL target");
                config.authentication(TiberiusAuthMethod::Integrated);
            }
            _ => {
                config.authentication(TiberiusAuthMethod::sql_server(
                    &self.config.user,
                    &self.config.password,
                ));
            }
        }

        match self.config.ssl_mode.to_lowercase().as_str() {
            "disable" => {
                config.encryption(EncryptionLevel::NotSupported);
            }
            _ => {
                config.trust_cert();
                config.encryption(EncryptionLevel::Required);
            }
        }

        config.packet_size(TDS_MAX_PACKET_SIZE);
        config
    }
}

#[async_trait]
impl bb8::ManageConnection for TiberiusConnectionManager {
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

        if let Ok(std_tcp) = tcp.into_std() {
            use std::net::TcpStream as StdTcpStream;
            let socket = socket2::Socket::from(std_tcp);

            let keepalive = socket2::TcpKeepalive::new()
                .with_time(TCP_KEEPALIVE_INTERVAL)
                .with_interval(TCP_KEEPALIVE_INTERVAL);

            if let Err(e) = socket.set_tcp_keepalive(&keepalive) {
                warn!("Failed to set TCP keepalive on MSSQL connection: {}", e);
            }

            let std_tcp: StdTcpStream = socket.into();
            std_tcp.set_nonblocking(true).ok();
            let tcp = TcpStream::from_std(std_tcp).map_err(|e| tiberius::error::Error::Io {
                kind: e.kind(),
                message: format!("Failed to convert socket: {}", e),
            })?;

            Client::connect(config, tcp.compat_write()).await
        } else {
            warn!("Failed to configure TCP keepalives on MSSQL connection");
            let tcp = TcpStream::connect(config.get_addr()).await.map_err(|e| {
                tiberius::error::Error::Io {
                    kind: e.kind(),
                    message: e.to_string(),
                }
            })?;
            tcp.set_nodelay(true).ok();
            Client::connect(config, tcp.compat_write()).await
        }
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> std::result::Result<(), Self::Error> {
        conn.simple_query("SELECT 1").await?.into_row().await?;
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

/// MSSQL target writer implementation.
///
/// Provides high-performance data writing to MSSQL databases using:
/// - TDS bulk insert protocol
/// - Connection pooling via bb8
/// - Staging table approach for upserts
pub struct MssqlWriter {
    pool: Pool<TiberiusConnectionManager>,
    type_mapper: Option<Arc<dyn TypeMapper>>,
}

impl MssqlWriter {
    /// Create a new MSSQL writer from configuration.
    pub async fn new(config: TargetConfig) -> Result<Self> {
        Self::with_pool_size(config, 8).await
    }

    /// Create a new MSSQL writer with specified pool size.
    pub async fn with_pool_size(config: TargetConfig, max_size: u32) -> Result<Self> {
        let manager = TiberiusConnectionManager::new(config.clone());
        let pool = Pool::builder()
            .max_size(max_size)
            .min_idle(Some(1))
            .connection_timeout(POOL_CONNECTION_TIMEOUT)
            .idle_timeout(Some(POOL_IDLE_TIMEOUT))
            .max_lifetime(Some(POOL_MAX_LIFETIME))
            .test_on_check_out(true)
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
            "Connected to MSSQL target: {}:{}/{} (pool_size={})",
            config.host, config.port, config.database, max_size
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

    /// Get a pooled connection.
    async fn get_conn(&self) -> Result<PooledConnection<'_, TiberiusConnectionManager>> {
        self.pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MSSQL target connection"))
    }

    /// Quote an MSSQL identifier with brackets.
    fn quote_ident(name: &str) -> String {
        format!("[{}]", name.replace(']', "]]"))
    }

    /// Qualify a table name with schema.
    fn qualify_table(schema: &str, table: &str) -> String {
        format!("{}.{}", Self::quote_ident(schema), Self::quote_ident(table))
    }

    /// Map a source type to MSSQL type.
    fn map_type(&self, col: &Column) -> String {
        if let Some(mapper) = &self.type_mapper {
            let mapping = mapper.map_column(col);
            if let Some(warning) = &mapping.warning {
                warn!("Column {}: {}", col.name, warning);
            }
            mapping.target_type
        } else {
            // No mapper - assume source is MSSQL and use as-is
            format_mssql_type(&col.data_type, col.max_length, col.precision, col.scale)
        }
    }

    /// Check if a row has strings exceeding bulk insert limit.
    fn row_has_oversized_strings(row: &[SqlValue<'_>]) -> bool {
        for value in row {
            if let SqlValue::Text(s) = value {
                let utf16_len: usize = s.chars().map(|c| c.len_utf16() * 2).sum();
                if utf16_len > BULK_INSERT_STRING_LIMIT {
                    return true;
                }
            }
        }
        false
    }

    /// Check if error is a deadlock.
    fn is_deadlock_error(e: &tiberius::error::Error) -> bool {
        e.is_deadlock()
    }

    /// Build MERGE SQL for upsert.
    fn build_merge_sql(
        target_table: &str,
        staging_table: &str,
        cols: &[String],
        pk_cols: &[String],
    ) -> String {
        let col_list: Vec<String> = cols.iter().map(|c| Self::quote_ident(c)).collect();
        let col_str = col_list.join(", ");

        // Join condition
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

        // Update columns (non-PK)
        let update_cols: Vec<String> = cols
            .iter()
            .filter(|c| !pk_cols.contains(c))
            .map(|c| format!("{} = source.{}", Self::quote_ident(c), Self::quote_ident(c)))
            .collect();

        // Source column references
        let source_cols: Vec<String> = cols
            .iter()
            .map(|c| format!("source.{}", Self::quote_ident(c)))
            .collect();

        if update_cols.is_empty() {
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
}

#[async_trait]
impl TargetWriter for MssqlWriter {
    async fn create_schema(&self, schema: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let quoted_schema = Self::quote_ident(schema);
        let query = format!(
            "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = @P1) EXEC(N'CREATE SCHEMA {}')",
            quoted_schema.replace('\'', "''")
        );
        conn.execute(&query, &[&schema]).await?;
        debug!("Created schema: {}", schema);
        Ok(())
    }

    async fn create_table(&self, table: &Table, target_schema: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;

        let col_defs: Vec<String> = table
            .columns
            .iter()
            .map(|c| {
                let target_type = self.map_type(c);
                let null_clause = if c.is_nullable { "NULL" } else { "NOT NULL" };
                format!(
                    "{} {} {}",
                    Self::quote_ident(&c.name),
                    target_type,
                    null_clause
                )
            })
            .collect();

        // Clustered PK
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
            "CREATE TABLE {}.{} (\n    {}{}\n)",
            Self::quote_ident(target_schema),
            Self::quote_ident(&table.name),
            col_defs.join(",\n    "),
            pk_constraint
        );

        conn.execute(&ddl, &[]).await?;
        debug!("Created table: {}.{}", target_schema, table.name);
        Ok(())
    }

    async fn create_table_unlogged(&self, table: &Table, target_schema: &str) -> Result<()> {
        // MSSQL doesn't have UNLOGGED tables
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

    async fn table_exists(&self, schema: &str, table: &str) -> Result<bool> {
        let mut conn = self.get_conn().await?;
        let query = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @P1 AND TABLE_NAME = @P2";
        let result = conn.query(query, &[&schema, &table]).await?;
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
        let pk_name = format!("PK_{}_{}", target_schema, table.name);

        // Check if PK exists
        let check_query = r#"SELECT 1 FROM sys.key_constraints
            WHERE name = @P1
            AND parent_object_id = OBJECT_ID(QUOTENAME(@P2) + '.' + QUOTENAME(@P3))"#;
        let result = conn
            .query(check_query, &[&pk_name, &target_schema, &table.name])
            .await?;
        if !result.into_first_result().await?.is_empty() {
            debug!(
                "Primary key already exists on {}.{}",
                target_schema, table.name
            );
            return Ok(());
        }

        let pk_cols: Vec<String> = table
            .primary_key
            .iter()
            .map(|c| Self::quote_ident(c))
            .collect();

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
        let clustered = if idx.is_clustered {
            "CLUSTERED "
        } else {
            "NONCLUSTERED "
        };

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
        debug!(
            "Created index {} on {}.{}",
            idx.name, target_schema, table.name
        );
        Ok(())
    }

    async fn drop_non_pk_indexes(&self, schema: &str, table: &str) -> Result<Vec<String>> {
        let mut conn = self.get_conn().await?;

        let query = r#"SELECT i.name
               FROM sys.indexes i
               JOIN sys.tables t ON i.object_id = t.object_id
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = @P1 AND t.name = @P2
                 AND i.is_primary_key = 0 AND i.type > 0"#;

        let result = conn.query(query, &[&schema, &table]).await?;
        let rows = result.into_first_result().await?;
        let mut dropped_indexes = Vec::new();

        for row in rows {
            if let Some(name) = row.get::<&str, _>(0) {
                let drop_query = format!(
                    "DROP INDEX {} ON {}",
                    Self::quote_ident(name),
                    Self::qualify_table(schema, table)
                );
                conn.execute(&drop_query, &[]).await?;
                dropped_indexes.push(name.to_string());
            }
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
        let mut conn = self.get_conn().await?;
        let fk_cols: Vec<String> = fk.columns.iter().map(|c| Self::quote_ident(c)).collect();
        let ref_cols: Vec<String> = fk
            .ref_columns
            .iter()
            .map(|c| Self::quote_ident(c))
            .collect();

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
        let mut conn = self.get_conn().await?;

        let query = format!(
            "ALTER TABLE {}.{} ADD CONSTRAINT {} {}",
            Self::quote_ident(target_schema),
            Self::quote_ident(&table.name),
            Self::quote_ident(&chk.name),
            chk.definition
        );

        conn.execute(&query, &[]).await?;
        debug!(
            "Created check constraint {} on {}.{}",
            chk.name, target_schema, table.name
        );
        Ok(())
    }

    async fn has_primary_key(&self, schema: &str, table: &str) -> Result<bool> {
        let mut conn = self.get_conn().await?;
        let query = r#"SELECT COUNT(*)
               FROM sys.indexes i
               JOIN sys.tables t ON i.object_id = t.object_id
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = @P1 AND t.name = @P2 AND i.is_primary_key = 1"#;

        let result = conn.query(query, &[&schema, &table]).await?;
        if let Some(row) = result.into_first_result().await?.into_iter().next() {
            let count: i32 = row.get(0).unwrap_or(0);
            return Ok(count > 0);
        }
        Ok(false)
    }

    async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        let mut conn = self.get_conn().await?;
        let query = format!(
            "SELECT CAST(COUNT(*) AS BIGINT) FROM {} WITH (NOLOCK)",
            Self::qualify_table(schema, table)
        );

        let result = conn.simple_query(&query).await?;
        if let Some(row) = result.into_first_result().await?.into_iter().next() {
            let count: i64 = row.get(0).unwrap_or(0);
            return Ok(count);
        }
        Ok(0)
    }

    async fn reset_sequence(&self, _schema: &str, _table: &Table) -> Result<()> {
        // MSSQL identity columns auto-increment
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

    async fn write_batch(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        batch: Batch,
    ) -> Result<u64> {
        let rows = batch.rows;
        if rows.is_empty() {
            return Ok(0);
        }

        let qualified_table = Self::qualify_table(schema, table);

        let mut conn = self.get_conn().await?;

        // Partition by string size
        let mut bulk_rows = Vec::with_capacity(rows.len());
        let mut oversized_rows = Vec::new();

        for row in rows {
            if Self::row_has_oversized_strings(&row) {
                oversized_rows.push(row);
            } else {
                bulk_rows.push(row);
            }
        }

        let mut total_inserted = 0u64;

        // Bulk insert normal rows
        if !bulk_rows.is_empty() {
            let mut bulk_load = conn.bulk_insert(&qualified_table).await.map_err(|e| {
                MigrateError::transfer(&qualified_table, format!("bulk insert init: {}", e))
            })?;

            for row in bulk_rows {
                let mut token_row = TokenRow::new();
                for value in &row {
                    token_row.push(sql_value_to_column_data(value));
                }
                bulk_load.send(token_row).await.map_err(|e| {
                    MigrateError::transfer(&qualified_table, format!("bulk insert send: {}", e))
                })?;
                total_inserted += 1;
            }

            bulk_load.finalize().await.map_err(|e| {
                MigrateError::transfer(&qualified_table, format!("bulk insert finalize: {}", e))
            })?;
        }

        // Fallback for oversized strings
        if !oversized_rows.is_empty() {
            debug!(
                "Falling back to INSERT for {} rows with oversized strings",
                oversized_rows.len()
            );
            total_inserted +=
                insert_rows_fallback(&mut conn, &qualified_table, cols, &oversized_rows).await?;
        }

        Ok(total_inserted)
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

        let mut conn = self.get_conn().await?;

        // Create/reuse staging table
        let staging_table =
            ensure_staging_table(&mut conn, schema, table, writer_id, partition_id).await?;

        // Bulk insert to staging
        bulk_insert_to_staging(&mut conn, &staging_table, cols, &rows).await?;

        // Check for identity column
        let has_identity = has_identity_column(&mut conn, schema, table).await?;

        // Build and execute MERGE
        let merge_sql = Self::build_merge_sql(&qualified_target, &staging_table, cols, pk_cols);
        let batch_sql = if has_identity {
            format!(
                "SET IDENTITY_INSERT {} ON; {} SET IDENTITY_INSERT {} OFF;",
                qualified_target, merge_sql, qualified_target
            )
        } else {
            merge_sql
        };

        // Execute with deadlock retry
        let mut retries = 0;
        loop {
            match conn.simple_query(&batch_sql).await {
                Ok(result) => {
                    result.into_results().await.map_err(|e| {
                        MigrateError::transfer(&qualified_target, format!("merge results: {}", e))
                    })?;
                    break;
                }
                Err(e) => {
                    if Self::is_deadlock_error(&e) && retries < DEADLOCK_MAX_RETRIES {
                        retries += 1;
                        warn!(
                            "Deadlock detected during MERGE to {}, retry {}/{}",
                            qualified_target, retries, DEADLOCK_MAX_RETRIES
                        );
                        tokio::time::sleep(Duration::from_millis(
                            DEADLOCK_RETRY_DELAY_MS * retries as u64,
                        ))
                        .await;
                        continue;
                    }
                    return Err(MigrateError::transfer(
                        &qualified_target,
                        format!("merge failed: {}", e),
                    ));
                }
            }
        }

        Ok(row_count)
    }

    fn db_type(&self) -> &str {
        "mssql"
    }

    async fn close(&self) {
        // bb8 handles cleanup
    }

    fn supports_direct_copy(&self) -> bool {
        false
    }
}

// Helper functions

fn quote_ident(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

async fn ensure_staging_table(
    conn: &mut PooledConnection<'_, TiberiusConnectionManager>,
    schema: &str,
    table: &str,
    writer_id: usize,
    partition_id: Option<i32>,
) -> Result<String> {
    let staging_name = match partition_id {
        Some(pid) => format!("_staging_{}_p{}_{}", table, pid, writer_id),
        None => format!("_staging_{}_{}", table, writer_id),
    };
    let qualified_staging = format!("{}.{}", quote_ident(schema), quote_ident(&staging_name));

    // Check if exists
    let check_sql = "SELECT OBJECT_ID(QUOTENAME(@P1) + '.' + QUOTENAME(@P2), 'U')";
    let result = conn.query(check_sql, &[&schema, &staging_name]).await?;
    let rows = result.into_first_result().await?;
    let table_exists = rows.first().and_then(|r| r.get::<i32, _>(0)).is_some();

    if table_exists {
        let drop_sql = format!("DROP TABLE {}", qualified_staging);
        conn.execute(&drop_sql, &[]).await?;
    }

    // Get column definitions
    let col_query = r#"
        SELECT c.name,
               CASE
                   WHEN t.name = 'tinyint' THEN 'smallint'
                   WHEN t.name IN ('datetime', 'smalldatetime') THEN 'datetime2(7)'
                   WHEN t.name IN ('nvarchar', 'nchar') AND c.max_length = -1 THEN t.name + '(max)'
                   WHEN t.name IN ('nvarchar', 'nchar') THEN t.name + '(' + CAST(c.max_length/2 AS VARCHAR) + ')'
                   WHEN t.name IN ('varchar', 'char', 'varbinary', 'binary') AND c.max_length = -1 THEN t.name + '(max)'
                   WHEN t.name IN ('varchar', 'char', 'varbinary', 'binary') THEN t.name + '(' + CAST(c.max_length AS VARCHAR) + ')'
                   WHEN t.name IN ('decimal', 'numeric') THEN t.name + '(' + CAST(c.precision AS VARCHAR) + ',' + CAST(c.scale AS VARCHAR) + ')'
                   WHEN t.name IN ('datetime2', 'time', 'datetimeoffset') AND c.scale > 0 THEN t.name + '(' + CAST(c.scale AS VARCHAR) + ')'
                   ELSE t.name
               END,
               c.is_nullable
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        JOIN sys.tables tbl ON c.object_id = tbl.object_id
        JOIN sys.schemas s ON tbl.schema_id = s.schema_id
        WHERE s.name = @P1 AND tbl.name = @P2
        ORDER BY c.column_id"#;

    let result = conn.query(col_query, &[&schema, &table]).await?;
    let rows = result.into_first_result().await?;

    let col_defs: Vec<String> = rows
        .iter()
        .map(|r| {
            let name: &str = r.get(0).unwrap_or_default();
            let dtype: &str = r.get(1).unwrap_or_default();
            let nullable: bool = r.get(2).unwrap_or(true);
            let null_str = if nullable { "NULL" } else { "NOT NULL" };
            format!("{} {} {}", quote_ident(name), dtype, null_str)
        })
        .collect();

    let create_sql = format!(
        "CREATE TABLE {} ({})",
        qualified_staging,
        col_defs.join(", ")
    );
    conn.execute(&create_sql, &[]).await?;

    Ok(qualified_staging)
}

async fn bulk_insert_to_staging(
    conn: &mut PooledConnection<'_, TiberiusConnectionManager>,
    staging_table: &str,
    _cols: &[String],
    rows: &[Vec<SqlValue<'static>>],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut bulk_load = conn
        .bulk_insert(staging_table)
        .await
        .map_err(|e| MigrateError::transfer(staging_table, format!("bulk insert init: {}", e)))?;

    for row in rows {
        let mut token_row = TokenRow::new();
        for value in row {
            token_row.push(sql_value_to_column_data(value));
        }
        bulk_load.send(token_row).await.map_err(|e| {
            MigrateError::transfer(staging_table, format!("bulk insert send: {}", e))
        })?;
    }

    bulk_load.finalize().await.map_err(|e| {
        MigrateError::transfer(staging_table, format!("bulk insert finalize: {}", e))
    })?;

    Ok(())
}

async fn has_identity_column(
    conn: &mut PooledConnection<'_, TiberiusConnectionManager>,
    schema: &str,
    table: &str,
) -> Result<bool> {
    let query = r#"SELECT COUNT(*)
           FROM sys.columns c
           JOIN sys.tables t ON c.object_id = t.object_id
           JOIN sys.schemas s ON t.schema_id = s.schema_id
           WHERE s.name = @P1 AND t.name = @P2 AND c.is_identity = 1"#;
    let result = conn.query(query, &[&schema, &table]).await?;
    if let Some(row) = result.into_first_result().await?.into_iter().next() {
        let count: i32 = row.get(0).unwrap_or(0);
        return Ok(count > 0);
    }
    Ok(false)
}

async fn insert_rows_fallback(
    conn: &mut PooledConnection<'_, TiberiusConnectionManager>,
    qualified_table: &str,
    cols: &[String],
    rows: &[Vec<SqlValue<'static>>],
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }

    let col_list: Vec<String> = cols.iter().map(|c| quote_ident(c)).collect();
    let col_str = col_list.join(", ");
    let cols_per_row = cols.len();

    if cols_per_row == 0 {
        return Err(MigrateError::transfer(
            qualified_table,
            "Cannot insert with zero columns",
        ));
    }

    let max_rows_per_batch = (2100 / cols_per_row).clamp(1, 1000);
    let mut total_inserted = 0u64;

    for batch in rows.chunks(max_rows_per_batch) {
        let mut value_groups = Vec::with_capacity(batch.len());
        let mut param_idx = 1;

        for _ in batch.iter() {
            let placeholders: Vec<String> = (0..cols_per_row)
                .map(|_| {
                    let p = format!("@P{}", param_idx);
                    param_idx += 1;
                    p
                })
                .collect();
            value_groups.push(format!("({})", placeholders.join(", ")));
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            qualified_table,
            col_str,
            value_groups.join(", ")
        );

        let params: Vec<Box<dyn ToSql>> = batch
            .iter()
            .flat_map(|row| row.iter().map(sql_value_to_sql_param))
            .collect();

        let param_refs: Vec<&dyn ToSql> = params.iter().map(|p| p.as_ref()).collect();
        conn.execute(sql.as_str(), &param_refs).await.map_err(|e| {
            MigrateError::transfer(qualified_table, format!("batched INSERT: {}", e))
        })?;

        total_inserted += batch.len() as u64;
    }

    Ok(total_inserted)
}

fn format_mssql_type(data_type: &str, max_length: i32, precision: i32, scale: i32) -> String {
    let lower = data_type.to_lowercase();
    match lower.as_str() {
        "bigint" | "int" | "smallint" | "tinyint" | "bit" | "money" | "smallmoney" | "real"
        | "date" | "image" | "uniqueidentifier" | "xml" => data_type.to_string(),
        // Convert datetime/smalldatetime to datetime2 for bulk insert compatibility
        // (sql_value_to_column_data always sends DateTime2 data)
        "datetime" | "smalldatetime" => "datetime2(7)".to_string(),
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
        // Note: max_length comes from INFORMATION_SCHEMA.CHARACTER_MAXIMUM_LENGTH
        // which is already in character units (not bytes), so no division needed
        "char" | "varchar" | "nchar" | "nvarchar" => {
            if max_length == -1 {
                format!("{}(max)", data_type)
            } else if max_length > 0 {
                format!("{}({})", data_type, max_length)
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

fn sql_value_to_sql_param(value: &SqlValue<'_>) -> Box<dyn ToSql> {
    match value {
        SqlValue::Null(_) => Box::new(Option::<String>::None),
        SqlValue::Bool(b) => Box::new(*b),
        SqlValue::I16(i) => Box::new(*i),
        SqlValue::I32(i) => Box::new(*i),
        SqlValue::I64(i) => Box::new(*i),
        SqlValue::F32(f) => Box::new(*f),
        SqlValue::F64(f) => Box::new(*f),
        SqlValue::Text(s) => Box::new(s.to_string()),
        SqlValue::Bytes(b) => Box::new(b.to_vec()),
        SqlValue::Uuid(u) => Box::new(*u),
        SqlValue::Decimal(d) => Box::new(*d),
        SqlValue::DateTime(dt) => Box::new(*dt),
        SqlValue::DateTimeOffset(dto) => Box::new(*dto),
        SqlValue::Date(d) => Box::new(d.and_hms_opt(0, 0, 0).unwrap()),
        SqlValue::Time(t) => Box::new(*t),
    }
}

fn sql_value_to_column_data(value: &SqlValue<'_>) -> ColumnData<'static> {
    match value {
        SqlValue::Null(null_type) => match null_type {
            SqlNullType::Bool => ColumnData::Bit(None),
            SqlNullType::I16 => ColumnData::I16(None),
            SqlNullType::I32 => ColumnData::I32(None),
            SqlNullType::I64 => ColumnData::I64(None),
            SqlNullType::F32 => ColumnData::F32(None),
            SqlNullType::F64 => ColumnData::F64(None),
            SqlNullType::String => ColumnData::String(None),
            SqlNullType::Bytes => ColumnData::Binary(None),
            SqlNullType::Uuid => ColumnData::Guid(None),
            SqlNullType::Decimal => ColumnData::Numeric(None),
            SqlNullType::DateTime => ColumnData::DateTime2(None),
            SqlNullType::DateTimeOffset => ColumnData::DateTimeOffset(None),
            SqlNullType::Date => ColumnData::DateTime2(None),
            SqlNullType::Time => ColumnData::Time(None),
        },
        SqlValue::Bool(b) => ColumnData::Bit(Some(*b)),
        SqlValue::I16(i) => ColumnData::I16(Some(*i)),
        SqlValue::I32(i) => ColumnData::I32(Some(*i)),
        SqlValue::I64(i) => ColumnData::I64(Some(*i)),
        SqlValue::F32(f) => {
            if f.is_nan() || f.is_infinite() {
                ColumnData::F32(None)
            } else {
                ColumnData::F32(Some(*f))
            }
        }
        SqlValue::F64(f) => {
            if f.is_nan() || f.is_infinite() {
                ColumnData::F64(None)
            } else {
                ColumnData::F64(Some(*f))
            }
        }
        SqlValue::Text(s) => ColumnData::String(Some(Cow::Owned(s.to_string()))),
        SqlValue::Bytes(b) => ColumnData::Binary(Some(Cow::Owned(b.to_vec()))),
        SqlValue::Uuid(u) => ColumnData::Guid(Some(*u)),
        SqlValue::Decimal(d) => {
            let scale = d.scale() as u8;
            let mantissa = d.mantissa();
            ColumnData::Numeric(Some(tiberius::numeric::Numeric::new_with_scale(
                mantissa, scale,
            )))
        }
        SqlValue::DateTime(dt) => {
            let epoch = chrono::NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
            let days_i64 = (dt.date() - epoch).num_days();
            if days_i64 < 0 || days_i64 > u32::MAX as i64 {
                return ColumnData::DateTime2(None);
            }
            let days = days_i64 as u32;
            let date = tiberius::time::Date::new(days);
            let time_val = dt.time();
            let nanos = time_val.num_seconds_from_midnight() as u64 * 1_000_000_000
                + time_val.nanosecond() as u64;
            let increments = nanos / 100;
            let time = tiberius::time::Time::new(increments, 7);
            ColumnData::DateTime2(Some(tiberius::time::DateTime2::new(date, time)))
        }
        SqlValue::DateTimeOffset(dto) => {
            let naive = dto.naive_utc();
            let epoch = chrono::NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
            let days_i64 = (naive.date() - epoch).num_days();
            if days_i64 < 0 || days_i64 > u32::MAX as i64 {
                return ColumnData::DateTimeOffset(None);
            }
            let days = days_i64 as u32;
            let date = tiberius::time::Date::new(days);
            let time_val = naive.time();
            let nanos = time_val.num_seconds_from_midnight() as u64 * 1_000_000_000
                + time_val.nanosecond() as u64;
            let increments = nanos / 100;
            let time = tiberius::time::Time::new(increments, 7);
            let datetime2 = tiberius::time::DateTime2::new(date, time);
            let offset_seconds = dto.offset().local_minus_utc();
            let offset_minutes = (offset_seconds / 60) as i16;
            ColumnData::DateTimeOffset(Some(tiberius::time::DateTimeOffset::new(
                datetime2,
                offset_minutes,
            )))
        }
        SqlValue::Date(d) => {
            let epoch = chrono::NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
            let days_i64 = (*d - epoch).num_days();
            if days_i64 < 0 || days_i64 > u32::MAX as i64 {
                return ColumnData::DateTime2(None);
            }
            let days = days_i64 as u32;
            let date = tiberius::time::Date::new(days);
            let time = tiberius::time::Time::new(0, 7);
            ColumnData::DateTime2(Some(tiberius::time::DateTime2::new(date, time)))
        }
        SqlValue::Time(t) => {
            let nanos =
                t.num_seconds_from_midnight() as u64 * 1_000_000_000 + t.nanosecond() as u64;
            let increments = nanos / 100;
            ColumnData::Time(Some(tiberius::time::Time::new(increments, 7)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_ident() {
        assert_eq!(quote_ident("users"), "[users]");
        assert_eq!(quote_ident("user]table"), "[user]]table]");
    }

    #[test]
    fn test_format_mssql_type() {
        assert_eq!(format_mssql_type("int", 0, 0, 0), "int");
        assert_eq!(format_mssql_type("varchar", 255, 0, 0), "varchar(255)");
        assert_eq!(format_mssql_type("varchar", -1, 0, 0), "varchar(max)");
        assert_eq!(format_mssql_type("decimal", 0, 18, 2), "decimal(18, 2)");
    }
}
