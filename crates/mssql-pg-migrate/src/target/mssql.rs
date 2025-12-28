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

    /// Check if a data type is an MSSQL native type.
    fn is_mssql_type(data_type: &str) -> bool {
        let lower = data_type.to_lowercase();
        matches!(
            lower.as_str(),
            "bigint" | "int" | "smallint" | "tinyint" | "bit"
            | "decimal" | "numeric" | "money" | "smallmoney"
            | "float" | "real"
            | "datetime" | "datetime2" | "smalldatetime" | "date" | "time" | "datetimeoffset"
            | "char" | "varchar" | "text" | "nchar" | "nvarchar" | "ntext"
            | "binary" | "varbinary" | "image"
            | "uniqueidentifier" | "xml" | "sql_variant" | "timestamp" | "rowversion"
            | "geography" | "geometry" | "hierarchyid"
        )
    }

    /// Format an MSSQL type with proper length/precision.
    fn format_mssql_type(data_type: &str, max_length: i32, precision: i32, scale: i32) -> String {
        let lower = data_type.to_lowercase();
        match lower.as_str() {
            // Fixed-length types
            "bigint" | "int" | "smallint" | "tinyint" | "bit" | "money" | "smallmoney"
            | "real" | "datetime" | "smalldatetime" | "date" | "text" | "ntext" | "image"
            | "uniqueidentifier" | "xml" | "sql_variant" | "timestamp" | "rowversion"
            | "geography" | "geometry" | "hierarchyid" => data_type.to_string(),

            // Float can have optional precision
            "float" => {
                if precision > 0 {
                    format!("float({})", precision)
                } else {
                    "float".to_string()
                }
            }

            // Decimal/numeric with precision and scale
            "decimal" | "numeric" => {
                if precision > 0 {
                    format!("{}({}, {})", data_type, precision, scale)
                } else {
                    format!("{}(18, 0)", data_type)
                }
            }

            // datetime2 with optional precision
            "datetime2" => {
                if scale > 0 {
                    format!("datetime2({})", scale)
                } else {
                    "datetime2".to_string()
                }
            }

            // time with optional precision
            "time" => {
                if scale > 0 {
                    format!("time({})", scale)
                } else {
                    "time".to_string()
                }
            }

            // datetimeoffset with optional precision
            "datetimeoffset" => {
                if scale > 0 {
                    format!("datetimeoffset({})", scale)
                } else {
                    "datetimeoffset".to_string()
                }
            }

            // Variable length character types
            "char" | "varchar" | "nchar" | "nvarchar" => {
                if max_length == -1 {
                    format!("{}(max)", data_type)
                } else if max_length > 0 {
                    // nvarchar stores 2 bytes per char, so divide by 2
                    let len = if lower.starts_with('n') && max_length > 0 {
                        max_length / 2
                    } else {
                        max_length
                    };
                    format!("{}({})", data_type, len)
                } else {
                    // Default length
                    format!("{}(255)", data_type)
                }
            }

            // Variable length binary types
            "binary" | "varbinary" => {
                if max_length == -1 {
                    format!("{}(max)", data_type)
                } else if max_length > 0 {
                    format!("{}({})", data_type, max_length)
                } else {
                    format!("{}(255)", data_type)
                }
            }

            // Unknown type - return as-is
            _ => data_type.to_string(),
        }
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

    /// Fetch row hashes for hash-based change detection.
    /// Returns a HashMap of serialized PK -> row hash.
    pub async fn fetch_row_hashes(
        &self,
        schema: &str,
        table: &str,
        pk_cols: &[String],
        row_hash_col: &str,
        min_pk: Option<i64>,
        max_pk: Option<i64>,
    ) -> Result<std::collections::HashMap<String, String>> {
        use std::collections::HashMap;

        let mut conn = self.get_conn().await?;

        // Build PK column select list
        let pk_select: Vec<String> = pk_cols.iter().map(|c| Self::quote_ident(c)).collect();
        let pk_select_str = pk_select.join(", ");

        // Build query
        let mut query = format!(
            "SELECT {}, {} FROM {}.{}",
            pk_select_str,
            Self::quote_ident(row_hash_col),
            Self::quote_ident(schema),
            Self::quote_ident(table)
        );

        // Add WHERE clause for single-column integer PK range
        if pk_cols.len() == 1 {
            let pk_col = Self::quote_ident(&pk_cols[0]);
            let mut conditions = Vec::new();

            if let Some(min) = min_pk {
                conditions.push(format!("{} >= {}", pk_col, min));
            }
            if let Some(max) = max_pk {
                conditions.push(format!("{} <= {}", pk_col, max));
            }

            if !conditions.is_empty() {
                query.push_str(" WHERE ");
                query.push_str(&conditions.join(" AND "));
            }
        }

        let result = conn.simple_query(&query).await?;
        let mut hashes = HashMap::new();

        for row in result.into_first_result().await? {
            // Build PK key from PK columns
            let pk_key: String = pk_cols
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    let val: Option<&str> = row.try_get(i).ok().flatten();
                    val.unwrap_or("NULL").to_string()
                })
                .collect::<Vec<_>>()
                .join("|");

            // Get hash value (last column)
            let hash_idx = pk_cols.len();
            let hash: Option<&str> = row.try_get(hash_idx).ok().flatten();
            if let Some(h) = hash {
                hashes.insert(pk_key, h.to_string());
            }
        }

        Ok(hashes)
    }

    /// Create a table with row hash column for change detection.
    pub async fn create_table_with_hash(
        &self,
        table: &Table,
        target_schema: &str,
        row_hash_column: &str,
    ) -> Result<()> {
        let mut conn = self.get_conn().await?;

        // Generate column definitions
        let mut col_defs: Vec<String> = table
            .columns
            .iter()
            .map(|c| {
                // Detect if source type is already MSSQL (by checking for MSSQL-specific types)
                let target_type = if Self::is_mssql_type(&c.data_type) {
                    // Source is MSSQL - format the type with proper length/precision
                    Self::format_mssql_type(&c.data_type, c.max_length, c.precision, c.scale)
                } else {
                    // Source is PostgreSQL - use the type mapper
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
                    mapping.target_type
                };

                let null_clause = if c.is_nullable { "NULL" } else { "NOT NULL" };
                let identity_clause = if c.is_identity { " IDENTITY(1,1)" } else { "" };

                format!(
                    "{} {}{}  {}",
                    Self::quote_ident(&c.name),
                    target_type,
                    identity_clause,
                    null_clause
                )
            })
            .collect();

        // Add row hash column for change detection
        col_defs.push(format!("{} NVARCHAR(32) NULL", Self::quote_ident(row_hash_column)));

        let ddl = format!(
            "CREATE TABLE {}.{} (\n    {}\n)",
            Self::quote_ident(target_schema),
            Self::quote_ident(&table.name),
            col_defs.join(",\n    ")
        );

        conn.execute(&ddl, &[]).await?;
        debug!("Created table with hash column: {}.{}", target_schema, table.name);
        Ok(())
    }

    /// Ensure the row hash column exists on the table.
    pub async fn ensure_row_hash_column(
        &self,
        schema: &str,
        table: &str,
        row_hash_column: &str,
    ) -> Result<bool> {
        let mut conn = self.get_conn().await?;

        // Check if column exists
        let check_query = format!(
            r#"SELECT COUNT(*) FROM sys.columns c
               JOIN sys.tables t ON c.object_id = t.object_id
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = '{}' AND t.name = '{}' AND c.name = '{}'"#,
            schema.replace('\'', "''"),
            table.replace('\'', "''"),
            row_hash_column.replace('\'', "''")
        );

        let result = conn.simple_query(&check_query).await?;
        if let Some(row) = result.into_first_result().await?.into_iter().next() {
            let count: i32 = row.get(0).unwrap_or(0);
            if count > 0 {
                return Ok(false); // Column already exists
            }
        }

        // Add the column
        let alter_query = format!(
            "ALTER TABLE {}.{} ADD {} NVARCHAR(32) NULL",
            Self::quote_ident(schema),
            Self::quote_ident(table),
            Self::quote_ident(row_hash_column)
        );
        conn.execute(&alter_query, &[]).await?;
        debug!("Added {} column to {}.{}", row_hash_column, schema, table);

        Ok(true)
    }

    /// Get total row count from a query.
    /// Used by verification engine.
    pub async fn get_total_row_count(&self, query: &str) -> Result<i64> {
        let mut conn = self.get_conn().await?;
        let result = conn.simple_query(query).await?;
        if let Some(row) = result.into_first_result().await?.into_iter().next() {
            let count: i64 = row.get(0).unwrap_or(0);
            return Ok(count);
        }
        Ok(0)
    }

    /// Execute NTILE partition query for verification.
    /// Returns (row_count, combined_hash) tuples.
    pub async fn execute_ntile_partition_query(&self, query: &str) -> Result<Vec<(i64, i64)>> {
        let mut conn = self.get_conn().await?;
        let result = conn.simple_query(query).await?;
        let mut partitions = Vec::new();

        for row in result.into_first_result().await? {
            let row_count: i64 = row.get(0).unwrap_or(0);
            let combined_hash: i64 = row.get(1).unwrap_or(0);
            partitions.push((row_count, combined_hash));
        }

        Ok(partitions)
    }

    /// Execute count query with ROW_NUMBER range.
    pub async fn execute_count_query_with_rownum(
        &self,
        query: &str,
        range: &crate::verify::RowRange,
    ) -> Result<i64> {
        let mut conn = self.get_conn().await?;
        // Build query with range substitution
        let final_query = query
            .replace("$1", &range.start_row.to_string())
            .replace("$2", &range.end_row.to_string());

        let result = conn.simple_query(&final_query).await?;
        if let Some(row) = result.into_first_result().await?.into_iter().next() {
            let count: i64 = row.get(0).unwrap_or(0);
            return Ok(count);
        }
        Ok(0)
    }

    /// Fetch row hashes with ROW_NUMBER range for verification.
    pub async fn fetch_row_hashes_with_rownum(
        &self,
        query: &str,
        range: &crate::verify::RowRange,
        pk_column_count: usize,
    ) -> Result<crate::verify::CompositeRowHashMap> {
        use crate::source::PkValue;
        use crate::verify::{CompositePk, CompositeRowHashMap};

        let mut conn = self.get_conn().await?;
        // Build query with range substitution
        let final_query = query
            .replace("$1", &range.start_row.to_string())
            .replace("$2", &range.end_row.to_string());

        let result = conn.simple_query(&final_query).await?;
        let mut hashes = CompositeRowHashMap::new();

        for row in result.into_first_result().await? {
            // Extract PK columns
            let mut pk_values = Vec::with_capacity(pk_column_count);
            for i in 0..pk_column_count {
                let val: Option<&str> = row.try_get(i).ok().flatten();
                pk_values.push(PkValue::String(val.unwrap_or("NULL").to_string()));
            }
            let pk = CompositePk(pk_values);

            // Hash is the last column
            let hash: Option<&str> = row.try_get(pk_column_count).ok().flatten();
            if let Some(h) = hash {
                hashes.insert(pk, h.to_string());
            }
        }

        Ok(hashes)
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
                // Detect if source type is already MSSQL (by checking for MSSQL-specific types)
                let target_type = if Self::is_mssql_type(&c.data_type) {
                    // Source is MSSQL - format the type with proper length/precision
                    Self::format_mssql_type(&c.data_type, c.max_length, c.precision, c.scale)
                } else {
                    // Source is PostgreSQL - use the type mapper
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
                    mapping.target_type
                };

                let null_clause = if c.is_nullable { "NULL" } else { "NOT NULL" };
                let identity_clause = if c.is_identity { " IDENTITY(1,1)" } else { "" };

                format!(
                    "{} {}{}  {}",
                    Self::quote_ident(&c.name),
                    target_type,
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
        let total_rows = rows.len() as u64;

        // Check for identity column using the SAME connection
        let identity_query = format!(
            r#"SELECT COUNT(*)
               FROM sys.columns c
               JOIN sys.tables t ON c.object_id = t.object_id
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = '{}' AND t.name = '{}' AND c.is_identity = 1"#,
            schema.replace('\'', "''"),
            table.replace('\'', "''")
        );
        debug!("Identity check query for {}.{}: {}", schema, table, identity_query);
        let result = conn.simple_query(&identity_query).await?;
        let has_identity = if let Some(row) = result.into_first_result().await?.into_iter().next() {
            let count: i32 = row.get(0).unwrap_or(0);
            debug!("Identity check result for {}.{}: count={}, has_identity={}", schema, table, count, count > 0);
            count > 0
        } else {
            debug!("Identity check result for {}.{}: no rows returned", schema, table);
            false
        };

        // Build column list
        let col_list: Vec<String> = cols.iter().map(|c| Self::quote_ident(c)).collect();
        let col_str = col_list.join(", ");

        let qualified_table = format!("{}.{}", Self::quote_ident(schema), Self::quote_ident(table));

        // Insert in batches, combining IDENTITY_INSERT with INSERT in single batch
        for chunk in rows.chunks(MAX_ROWS_PER_INSERT) {
            let values: Vec<String> = chunk
                .iter()
                .map(|row| {
                    let row_values: Vec<String> = row.iter().map(Self::format_value).collect();
                    format!("({})", row_values.join(", "))
                })
                .collect();

            // Build the SQL - if has_identity, wrap in IDENTITY_INSERT ON/OFF
            let batch_sql = if has_identity {
                format!(
                    "SET IDENTITY_INSERT {} ON; INSERT INTO {} ({}) VALUES {}; SET IDENTITY_INSERT {} OFF;",
                    qualified_table,
                    qualified_table,
                    col_str,
                    values.join(", "),
                    qualified_table
                )
            } else {
                format!(
                    "INSERT INTO {} ({}) VALUES {}",
                    qualified_table,
                    col_str,
                    values.join(", ")
                )
            };

            debug!("Executing batch: {}...", &batch_sql[..std::cmp::min(200, batch_sql.len())]);
            conn.simple_query(&batch_sql).await?.into_results().await?;
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
        let total_rows = rows.len() as u64;

        // Check for identity column using the SAME connection
        let identity_query = format!(
            r#"SELECT COUNT(*)
               FROM sys.columns c
               JOIN sys.tables t ON c.object_id = t.object_id
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = '{}' AND t.name = '{}' AND c.is_identity = 1"#,
            schema.replace('\'', "''"),
            table.replace('\'', "''")
        );
        let result = conn.simple_query(&identity_query).await?;
        let has_identity = if let Some(row) = result.into_first_result().await?.into_iter().next() {
            let count: i32 = row.get(0).unwrap_or(0);
            count > 0
        } else {
            false
        };

        // Build column list
        let col_list: Vec<String> = cols.iter().map(|c| Self::quote_ident(c)).collect();
        let col_str = col_list.join(", ");

        let qualified_table = format!("{}.{}", Self::quote_ident(schema), Self::quote_ident(table));

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
                r#"MERGE INTO {} AS target
                   USING (VALUES {}) AS source ({})
                   ON {}
                   WHEN MATCHED{} THEN UPDATE SET {}
                   WHEN NOT MATCHED THEN INSERT ({}) VALUES ({});"#,
                qualified_table,
                values.join(", "),
                col_str,
                join_condition.join(" AND "),
                update_condition,
                update_cols.join(", "),
                col_str,
                source_cols.join(", ")
            );

            // Combine IDENTITY_INSERT with MERGE in single batch
            let batch_sql = if has_identity {
                format!(
                    "SET IDENTITY_INSERT {} ON; {} SET IDENTITY_INSERT {} OFF;",
                    qualified_table,
                    merge_sql,
                    qualified_table
                )
            } else {
                merge_sql
            };

            conn.simple_query(&batch_sql).await?.into_results().await?;
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
