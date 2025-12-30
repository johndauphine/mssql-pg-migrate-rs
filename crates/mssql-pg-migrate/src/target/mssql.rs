//! MSSQL target database operations.
//!
//! This module implements `TargetPool` for MSSQL, enabling MSSQL
//! to be used as a target database for bidirectional migrations.

use crate::config::TargetConfig;
use crate::error::{MigrateError, Result};
use crate::source::{CheckConstraint, ForeignKey, Index, Table};
use crate::target::{SqlNullType, SqlValue, TargetPool};
use crate::typemap::postgres_to_mssql;
use async_trait::async_trait;
use bb8::{Pool, PooledConnection};
use chrono::Timelike;
use std::borrow::Cow;
use tiberius::{AuthMethod, Client, ColumnData, Config, EncryptionLevel, ToSql, TokenRow};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::{debug, info, warn};

/// Maximum string length (in bytes) for TDS bulk insert.
/// Tiberius bulk insert has a hard limit of 65535 bytes for UTF-16 encoded strings.
/// UTF-16 uses 2 bytes per code unit for BMP characters, so approximately 32K code units.
/// The byte-length calculation (using `c.len_utf16() * 2`) already accounts for surrogate pairs,
/// so this constant represents the exact 65535-byte limit.
const BULK_INSERT_STRING_LIMIT: usize = 65535;

/// Maximum number of retry attempts for deadlock errors during MERGE operations.
/// Increased to 5 to handle high-contention scenarios with many parallel writers.
const DEADLOCK_MAX_RETRIES: u32 = 5;

/// Base delay between deadlock retry attempts in milliseconds.
/// Uses linear backoff: 200ms, 400ms, 600ms, 800ms, 1000ms (delay * attempt)
const DEADLOCK_RETRY_DELAY_MS: u64 = 200;

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
    async fn get_conn(&self) -> Result<PooledConnection<'_, TiberiusTargetConnectionManager>> {
        self.pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MSSQL target connection"))
    }

    /// Quote an MSSQL identifier with brackets.
    fn quote_ident(name: &str) -> String {
        format!("[{}]", name.replace(']', "]]"))
    }

    /// Check if a table has an identity column.
    ///
    /// Uses parameterized query against sys catalog tables for SQL injection safety.
    /// This check is performed per-chunk to ensure correctness; future optimization
    /// could cache this at the table level during schema extraction.
    async fn has_identity_column(
        conn: &mut tiberius::Client<Compat<TcpStream>>,
        schema: &str,
        table: &str,
    ) -> Result<bool> {
        let query = r#"SELECT COUNT(*)
               FROM sys.columns c
               JOIN sys.tables t ON c.object_id = t.object_id
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = @P1 AND t.name = @P2 AND c.is_identity = 1"#;
        debug!("Identity check query for {}.{}", schema, table);
        let result = conn.query(query, &[&schema, &table]).await?;
        if let Some(row) = result.into_first_result().await?.into_iter().next() {
            let count: i32 = row.get(0).unwrap_or(0);
            debug!(
                "Identity check result for {}.{}: count={}, has_identity={}",
                schema,
                table,
                count,
                count > 0
            );
            Ok(count > 0)
        } else {
            debug!(
                "Identity check result for {}.{}: no rows returned",
                schema, table
            );
            Ok(false)
        }
    }

    /// Get column definitions for creating a staging table.
    ///
    /// Returns column definitions WITHOUT identity property to allow bulk insert.
    /// The staging table mirrors the target table structure but with identity removed.
    async fn get_column_definitions(
        conn: &mut tiberius::Client<Compat<TcpStream>>,
        schema: &str,
        table: &str,
    ) -> Result<Vec<(String, String, bool)>> {
        // Query column metadata: name, data type with length/precision, and nullability
        let query = r#"
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
            WHERE s.name = @P1 AND tbl.name = @P2
            ORDER BY c.column_id
        "#;

        let result = conn.query(query, &[&schema, &table]).await.map_err(|e| {
            MigrateError::transfer(
                format!("{}.{}", schema, table),
                format!("getting column definitions: {}", e),
            )
        })?;

        let rows = result.into_first_result().await.map_err(|e| {
            MigrateError::transfer(
                format!("{}.{}", schema, table),
                format!("reading column definitions: {}", e),
            )
        })?;

        let mut columns = Vec::new();
        for row in rows {
            let name: &str = row.get(0).ok_or_else(|| {
                MigrateError::transfer(
                    format!("{}.{}", schema, table),
                    "missing column name".to_string(),
                )
            })?;
            let data_type: &str = row.get(1).ok_or_else(|| {
                MigrateError::transfer(
                    format!("{}.{}", schema, table),
                    "missing data type".to_string(),
                )
            })?;
            let is_nullable: bool = row.get(2).unwrap_or(true);
            columns.push((name.to_string(), data_type.to_string(), is_nullable));
        }

        Ok(columns)
    }

    /// Ensure a staging table exists for the given target table and writer.
    ///
    /// Creates a staging table in the target schema with the same structure
    /// as the target table, but WITHOUT identity columns (to allow bulk insert).
    /// Staging table name: _staging_[table]_[writer_id]
    /// Uses TRUNCATE for subsequent calls to reuse the table efficiently.
    async fn ensure_staging_table(
        conn: &mut tiberius::Client<Compat<TcpStream>>,
        schema: &str,
        table: &str,
        writer_id: usize,
    ) -> Result<String> {
        // Build staging table name in target schema: [schema].[_staging_table_writerid]
        // Prefix with underscore to clearly mark as staging/internal table
        let staging_table_name = format!("_staging_{}_{}", table, writer_id);
        let qualified_staging = format!(
            "{}.{}",
            Self::quote_ident(schema),
            Self::quote_ident(&staging_table_name)
        );

        // Check if staging table already exists using QUOTENAME for safe identifier handling
        let check_sql = format!(
            "SELECT OBJECT_ID(QUOTENAME(@P1) + '.' + QUOTENAME(@P2), 'U')"
        );
        let result = conn.query(&check_sql, &[&schema, &staging_table_name]).await.map_err(|e| {
            MigrateError::transfer(&qualified_staging, format!("checking staging table: {}", e))
        })?;
        let rows = result.into_first_result().await.map_err(|e| {
            MigrateError::transfer(&qualified_staging, format!("reading staging check: {}", e))
        })?;

        let table_exists = rows
            .first()
            .and_then(|r| r.get::<i32, _>(0))
            .is_some();

        if table_exists {
            // Truncate existing staging table for reuse
            let truncate_sql = format!("TRUNCATE TABLE {}", qualified_staging);
            conn.execute(&truncate_sql, &[]).await.map_err(|e| {
                MigrateError::transfer(&qualified_staging, format!("truncating staging: {}", e))
            })?;
            debug!("Truncated staging table {}", qualified_staging);
        } else {
            // Get column definitions from target table
            let columns = Self::get_column_definitions(conn, schema, table).await?;

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

            conn.execute(&create_sql, &[]).await.map_err(|e| {
                MigrateError::transfer(&qualified_staging, format!("creating staging: {}", e))
            })?;
            debug!("Created staging table {}", qualified_staging);
        }

        Ok(qualified_staging)
    }

    /// Build MERGE SQL statement from staging table to target table.
    ///
    /// Generates an efficient MERGE statement that:
    /// - Updates existing rows when any non-PK column has changed
    /// - Inserts new rows that don't exist in target
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
            // PK-only table: just INSERT new rows, ignore matches
            // WITH (TABLOCK) serializes MERGE operations to prevent S->X lock conversion deadlocks
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
            // Build change detection using <> with NULL handling
            // MSSQL doesn't have IS DISTINCT FROM, so we use ISNULL comparison pattern
            let change_detection: Vec<String> = cols
                .iter()
                .filter(|c| !pk_cols.contains(c))
                .map(|c| {
                    // Use pattern: (target.col <> source.col OR (target.col IS NULL AND source.col IS NOT NULL) OR (target.col IS NOT NULL AND source.col IS NULL))
                    let quoted = Self::quote_ident(c);
                    format!(
                        "(target.{0} <> source.{0} OR (target.{0} IS NULL AND source.{0} IS NOT NULL) OR (target.{0} IS NOT NULL AND source.{0} IS NULL))",
                        quoted
                    )
                })
                .collect();

            // WITH (TABLOCK) serializes MERGE operations to prevent S->X lock conversion deadlocks
            // This is critical for parallel writers targeting the same table
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

    /// Check if a tiberius error is a deadlock error (error 1205).
    fn is_deadlock_error(e: &tiberius::error::Error) -> bool {
        // Use tiberius built-in method which checks error code 1205
        e.is_deadlock()
    }

    /// Perform bulk insert into a table (staging or target).
    ///
    /// This is a helper that handles both regular rows and oversized string rows.
    /// Returns the number of rows inserted.
    async fn bulk_insert_to_table(
        conn: &mut PooledConnection<'_, TiberiusTargetConnectionManager>,
        qualified_table: &str,
        cols: &[String],
        rows: &[Vec<SqlValue>],
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        // Partition rows: bulk-insertable vs oversized strings
        let mut bulk_rows = Vec::with_capacity(rows.len());
        let mut oversized_rows = Vec::new();

        for row in rows {
            if Self::row_has_oversized_strings(row) {
                oversized_rows.push(row.clone());
            } else {
                bulk_rows.push(row.clone());
            }
        }

        let mut total_inserted = 0u64;

        // Process bulk-insertable rows via TDS bulk insert
        if !bulk_rows.is_empty() {
            let bulk_count = bulk_rows.len() as u64;

            let mut bulk_load = conn
                .bulk_insert(qualified_table)
                .await
                .map_err(|e| {
                    MigrateError::transfer(qualified_table, format!("bulk insert init: {}", e))
                })?;

            for row in bulk_rows {
                let mut token_row = TokenRow::new();
                for value in &row {
                    token_row.push(sql_value_to_column_data(value));
                }
                bulk_load.send(token_row).await.map_err(|e| {
                    MigrateError::transfer(qualified_table, format!("bulk insert send: {}", e))
                })?;
            }

            bulk_load.finalize().await.map_err(|e| {
                MigrateError::transfer(qualified_table, format!("bulk insert finalize: {}", e))
            })?;

            total_inserted += bulk_count;
        }

        // Process oversized rows via parameterized INSERT
        if !oversized_rows.is_empty() {
            debug!(
                "Falling back to INSERT for {} rows with oversized strings in staging",
                oversized_rows.len()
            );
            let inserted = Self::insert_rows_fallback(conn, qualified_table, cols, &oversized_rows).await?;
            total_inserted += inserted;
        }

        Ok(total_inserted)
    }

    /// Check if a data type is an MSSQL native type.
    ///
    /// Note: "timestamp" is NOT included because PostgreSQL also has a "timestamp" type.
    /// MSSQL's "timestamp" is actually a rowversion (auto-generated binary), not a datetime.
    /// We use "rowversion" explicitly for MSSQL's auto-generated version column.
    fn is_mssql_type(data_type: &str) -> bool {
        // Note: The following ambiguous types are intentionally NOT included here:
        // - 'text', 'ntext': Deprecated MSSQL types (use nvarchar(max) instead)
        //   Also, PostgreSQL has 'text' type
        // - 'char', 'varchar': PostgreSQL also has these types, and we want
        //   PostgreSQL char/varchar to map to nchar/nvarchar for Unicode support
        //
        // Types listed here are MSSQL-specific types that should be formatted
        // directly rather than going through postgres_to_mssql mapping.
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
                    // nvarchar/nchar store 2 bytes per character in MSSQL, so max_length
                    // (in bytes) divided by 2 gives the character count. Integer division
                    // is intentional: a 5-byte limit can only hold 2 complete characters.
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

    /// Check if a row contains any string values that exceed the bulk insert limit.
    /// Tiberius bulk insert has a 65535 byte limit for UTF-16 encoded strings.
    fn row_has_oversized_strings(row: &[SqlValue]) -> bool {
        for value in row {
            if let SqlValue::String(s) = value {
                // Calculate UTF-16 encoded byte length: len_utf16() returns code units (1 for BMP,
                // 2 for surrogate pairs), multiplied by 2 to get bytes (2 for BMP, 4 for surrogates).
                let utf16_len: usize = s.chars().map(|c| c.len_utf16() * 2).sum();
                if utf16_len > BULK_INSERT_STRING_LIMIT {
                    return true;
                }
            }
        }
        false
    }

    /// Insert rows using parameterized INSERT statements (fallback for oversized strings).
    /// Uses parameterized queries to prevent SQL injection.
    /// Batches multiple rows per INSERT for better performance (up to MSSQL's 2100 param limit).
    async fn insert_rows_fallback(
        conn: &mut PooledConnection<'_, TiberiusTargetConnectionManager>,
        qualified_table: &str,
        cols: &[String],
        rows: &[Vec<SqlValue>],
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let col_list: Vec<String> = cols.iter().map(|c| Self::quote_ident(c)).collect();
        let col_str = col_list.join(", ");

        // MSSQL has a 2100 parameter limit per query
        // Calculate how many rows we can fit per batch
        let cols_per_row = cols.len();
        let max_rows_per_batch = if cols_per_row > 0 {
            (2100 / cols_per_row).max(1) // At least 1 row per batch
        } else {
            rows.len()
        };

        let mut total_inserted = 0u64;

        // Process rows in batches
        for batch in rows.chunks(max_rows_per_batch) {
            // Build multi-row VALUES clause: VALUES (@P1, @P2), (@P3, @P4), ...
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

            // Flatten all row values into a single params vector
            let params: Vec<Box<dyn ToSql>> = batch
                .iter()
                .flat_map(|row| row.iter().map(sql_value_to_sql_param))
                .collect();

            let param_refs: Vec<&dyn ToSql> = params.iter().map(|p| p.as_ref()).collect();

            conn.execute(sql.as_str(), &param_refs).await.map_err(|e| {
                MigrateError::transfer(
                    qualified_table,
                    format!("batched INSERT ({} rows): {}", batch.len(), e),
                )
            })?;

            total_inserted += batch.len() as u64;
        }

        debug!(
            "Inserted {} rows via batched INSERT fallback (batch size: {})",
            total_inserted, max_rows_per_batch
        );

        Ok(total_inserted)
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
                    let mapping =
                        postgres_to_mssql(&c.data_type, c.max_length, c.precision, c.scale);
                    if mapping.is_lossy {
                        if let Some(warning) = &mapping.warning {
                            warn!("Column {}.{}: {}", table.name, c.name, warning);
                        }
                    }
                    mapping.target_type
                };

                let null_clause = if c.is_nullable { "NULL" } else { "NOT NULL" };
                // Note: IDENTITY columns are converted to regular INT/BIGINT for data warehouse use.
                // This enables TDS bulk insert which is 5-10x faster than INSERT statements.
                // Identity semantics are not needed in the target data warehouse.

                format!(
                    "{} {} {}",
                    Self::quote_ident(&c.name),
                    target_type,
                    null_clause
                )
            })
            .collect();

        // Add row hash column for change detection
        col_defs.push(format!(
            "{} NVARCHAR(32) NULL",
            Self::quote_ident(row_hash_column)
        ));

        let ddl = format!(
            "CREATE TABLE {}.{} (\n    {}\n)",
            Self::quote_ident(target_schema),
            Self::quote_ident(&table.name),
            col_defs.join(",\n    ")
        );

        conn.execute(&ddl, &[]).await?;
        debug!(
            "Created table with hash column: {}.{}",
            target_schema, table.name
        );
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

        // Check if column exists using parameterized query
        let check_query = r#"SELECT COUNT(*) FROM sys.columns c
               JOIN sys.tables t ON c.object_id = t.object_id
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = @P1 AND t.name = @P2 AND c.name = @P3"#;

        let result = conn
            .query(check_query, &[&schema, &table, &row_hash_column])
            .await?;
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
    ///
    /// Returns (partition_id, row_count, partition_hash) tuples for Tier 1.
    /// The partition_hash enables detection of updates even when row counts match.
    pub async fn execute_ntile_partition_query(&self, query: &str) -> Result<Vec<(i64, i64, i64)>> {
        let mut conn = self.get_conn().await?;
        let result = conn.simple_query(query).await?;
        let mut partitions = Vec::new();

        for row in result.into_first_result().await? {
            let partition_id: i64 = row
                .get::<i64, _>(0)
                .unwrap_or_else(|| row.get::<i32, _>(0).map(|v| v as i64).unwrap_or(0));
            let row_count: i64 = row
                .get::<i64, _>(1)
                .unwrap_or_else(|| row.get::<i32, _>(1).map(|v| v as i64).unwrap_or(0));
            // partition_hash is column 2 (optional - default to 0 if not present)
            let partition_hash: i64 = row
                .get::<i64, _>(2)
                .unwrap_or_else(|| row.get::<i32, _>(2).map(|v| v as i64).unwrap_or(0));
            partitions.push((partition_id, row_count, partition_hash));
        }

        Ok(partitions)
    }

}

#[async_trait]
impl TargetPool for MssqlTargetPool {
    async fn create_schema(&self, schema: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;
        // Use parameterized query for schema existence check to prevent SQL injection.
        // The CREATE SCHEMA must use EXEC with quoted identifier since DDL doesn't support parameters.
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
                    let mapping =
                        postgres_to_mssql(&c.data_type, c.max_length, c.precision, c.scale);
                    if mapping.is_lossy {
                        if let Some(warning) = &mapping.warning {
                            warn!("Column {}.{}: {}", table.name, c.name, warning);
                        }
                    }
                    mapping.target_type
                };

                let null_clause = if c.is_nullable { "NULL" } else { "NOT NULL" };
                // Note: IDENTITY columns are converted to regular INT/BIGINT for data warehouse use.
                // This enables TDS bulk insert which is 5-10x faster than INSERT statements.
                // Identity semantics are not needed in the target data warehouse.

                format!(
                    "{} {} {}",
                    Self::quote_ident(&c.name),
                    target_type,
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
        let pk_cols: Vec<String> = table
            .primary_key
            .iter()
            .map(|c| Self::quote_ident(c))
            .collect();
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

        // Get list of non-PK indexes using parameterized query
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
                    "DROP INDEX {} ON {}.{}",
                    Self::quote_ident(name),
                    Self::quote_ident(schema),
                    Self::quote_ident(table)
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
        // Use parameterized query with sys catalog tables instead of OBJECT_ID
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

        let qualified_table = format!("{}.{}", Self::quote_ident(schema), Self::quote_ident(table));

        // Partition rows: bulk-insertable vs oversized strings
        // Tiberius bulk insert has a 65535 byte limit for UTF-16 encoded strings.
        let mut bulk_rows = Vec::with_capacity(rows.len());
        let mut oversized_rows = Vec::new();

        for row in rows {
            if Self::row_has_oversized_strings(&row) {
                oversized_rows.push(row);
            } else {
                bulk_rows.push(row);
            }
        }

        // Get a single connection for the entire chunk operation
        let mut conn = self.get_conn().await?;
        let mut total_inserted = 0u64;

        // If we have both bulk and oversized rows, wrap the entire operation
        // in a transaction to ensure atomicity
        let needs_transaction = !bulk_rows.is_empty() && !oversized_rows.is_empty();
        if needs_transaction {
            conn.execute("BEGIN TRANSACTION", &[]).await.map_err(|e| {
                MigrateError::transfer(&qualified_table, format!("begin transaction: {}", e))
            })?;
        }

        // Process bulk-insertable rows via TDS bulk insert (5-10x faster)
        if !bulk_rows.is_empty() {
            let bulk_count = bulk_rows.len() as u64;

            // Start bulk insert - Tiberius reads column metadata from the target table
            let mut bulk_load = conn
                .bulk_insert(&qualified_table)
                .await
                .map_err(|e| {
                    MigrateError::transfer(&qualified_table, format!("bulk insert init: {}", e))
                })?;

            // Send each row
            for row in bulk_rows {
                let mut token_row = TokenRow::new();
                for value in &row {
                    token_row.push(sql_value_to_column_data(value));
                }
                bulk_load.send(token_row).await.map_err(|e| {
                    MigrateError::transfer(&qualified_table, format!("bulk insert send: {}", e))
                })?;
            }

            // Finalize the bulk insert
            let result = bulk_load.finalize().await.map_err(|e| {
                MigrateError::transfer(&qualified_table, format!("bulk insert finalize: {}", e))
            })?;

            let rows_affected = result.total();
            debug!(
                "Bulk inserted {} rows into {} (reported: {})",
                bulk_count, qualified_table, rows_affected
            );

            total_inserted += bulk_count;
        }

        // Fall back to parameterized INSERT statements for rows with oversized strings
        if !oversized_rows.is_empty() {
            let oversized_count = oversized_rows.len();
            debug!(
                "Falling back to INSERT for {} rows with oversized strings in {}",
                oversized_count, qualified_table
            );

            match Self::insert_rows_fallback(&mut conn, &qualified_table, cols, &oversized_rows)
                .await
            {
                Ok(inserted) => {
                    total_inserted += inserted;
                }
                Err(e) => {
                    // Rollback transaction if we started one
                    if needs_transaction {
                        let _ = conn.execute("ROLLBACK TRANSACTION", &[]).await;
                    }
                    return Err(e);
                }
            }
        }

        // Commit transaction if we started one
        if needs_transaction {
            conn.execute("COMMIT TRANSACTION", &[]).await.map_err(|e| {
                MigrateError::transfer(&qualified_table, format!("commit transaction: {}", e))
            })?;
        }

        Ok(total_inserted)
    }

    /// Upsert rows using staging table approach for high performance.
    ///
    /// Instead of row-by-row MERGE with VALUES, this approach:
    /// 1. Creates/reuses a staging table (without identity columns)
    /// 2. Bulk inserts rows into staging using TDS protocol (70K+ rows/sec)
    /// 3. Single MERGE statement from staging to target
    ///
    /// This is 100x faster than row-by-row because:
    /// - TDS bulk insert is much faster than individual VALUES clauses
    /// - Single MERGE statement vs thousands of statements
    /// - Fewer round-trips to the database
    async fn upsert_chunk(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        pk_cols: &[String],
        rows: Vec<Vec<SqlValue>>,
        writer_id: usize,
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

        let qualified_target = format!("{}.{}", Self::quote_ident(schema), Self::quote_ident(table));

        // Get a connection for the entire operation
        let mut conn = self.get_conn().await?;

        // 1. Create or reuse staging table (TRUNCATE if exists)
        let staging_start = Instant::now();
        let staging_table = Self::ensure_staging_table(&mut conn, schema, table, writer_id).await?;
        let staging_time = staging_start.elapsed();

        // 2. Bulk insert rows into staging table
        let bulk_start = Instant::now();
        Self::bulk_insert_to_table(&mut conn, &staging_table, cols, &rows).await?;
        let bulk_time = bulk_start.elapsed();

        // 3. Build and execute MERGE from staging to target
        let merge_start = Instant::now();

        // Check if target has identity column
        let has_identity = Self::has_identity_column(&mut conn, schema, table).await?;

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
                        tokio::time::sleep(tokio::time::Duration::from_millis(
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

        let merge_time = merge_start.elapsed();
        let total_time = chunk_start.elapsed();

        // Log detailed timing for profiling (only for larger chunks)
        if row_count >= 1000 {
            debug!(
                "{}.{}: upsert chunk {} rows - staging: {:?}, bulk: {:?}, merge: {:?}, total: {:?}",
                schema, table, row_count, staging_time, bulk_time, merge_time, total_time
            );
        }

        Ok(row_count)
    }

    fn db_type(&self) -> &str {
        "mssql"
    }

    async fn close(&self) {
        // bb8 handles cleanup automatically
    }
}

/// Convert SqlValue to a boxed ToSql trait object for parameterized queries.
///
/// This is used by the fallback INSERT path for rows with oversized strings.
/// Using parameterized queries prevents SQL injection attacks.
fn sql_value_to_sql_param(value: &SqlValue) -> Box<dyn ToSql> {
    match value {
        SqlValue::Null(_) => Box::new(Option::<String>::None),
        SqlValue::Bool(b) => Box::new(*b),
        SqlValue::I16(i) => Box::new(*i),
        SqlValue::I32(i) => Box::new(*i),
        SqlValue::I64(i) => Box::new(*i),
        SqlValue::F32(f) => Box::new(*f),
        SqlValue::F64(f) => Box::new(*f),
        SqlValue::String(s) => Box::new(s.clone()),
        SqlValue::Bytes(b) => Box::new(b.clone()),
        SqlValue::Uuid(u) => Box::new(*u),
        SqlValue::Decimal(d) => Box::new(*d),
        SqlValue::DateTime(dt) => Box::new(*dt),
        SqlValue::DateTimeOffset(dto) => Box::new(*dto),
        SqlValue::Date(d) => {
            // Convert Date to NaiveDateTime at midnight for MSSQL compatibility
            let dt = d.and_hms_opt(0, 0, 0).unwrap();
            Box::new(dt)
        }
        SqlValue::Time(t) => Box::new(*t),
    }
}

/// Convert SqlValue to Tiberius ColumnData for bulk insert.
///
/// This function maps our internal SqlValue enum to Tiberius's ColumnData
/// which is used by the TDS bulk insert protocol for high-performance data loading.
fn sql_value_to_column_data(value: &SqlValue) -> ColumnData<'static> {
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
            SqlNullType::Date => ColumnData::DateTime2(None), // Date maps to datetime2 for bulk insert compatibility
            SqlNullType::Time => ColumnData::Time(None),
        },
        SqlValue::Bool(b) => ColumnData::Bit(Some(*b)),
        SqlValue::I16(i) => ColumnData::I16(Some(*i)),
        SqlValue::I32(i) => ColumnData::I32(Some(*i)),
        SqlValue::I64(i) => ColumnData::I64(Some(*i)),
        SqlValue::F32(f) => {
            if f.is_nan() || f.is_infinite() {
                // MSSQL doesn't support NaN/Infinity, convert to NULL
                warn!("Converting F32 NaN/Infinity to NULL for MSSQL compatibility");
                ColumnData::F32(None)
            } else {
                ColumnData::F32(Some(*f))
            }
        }
        SqlValue::F64(f) => {
            if f.is_nan() || f.is_infinite() {
                // MSSQL doesn't support NaN/Infinity, convert to NULL
                warn!("Converting F64 NaN/Infinity to NULL for MSSQL compatibility");
                ColumnData::F64(None)
            } else {
                ColumnData::F64(Some(*f))
            }
        }
        SqlValue::String(s) => ColumnData::String(Some(Cow::Owned(s.clone()))),
        SqlValue::Bytes(b) => ColumnData::Binary(Some(Cow::Owned(b.clone()))),
        SqlValue::Uuid(u) => ColumnData::Guid(Some(*u)),
        SqlValue::Decimal(d) => {
            // Convert rust_decimal to Tiberius Numeric
            // Tiberius Numeric uses i128 internally with scale
            let scale = d.scale() as u8;
            let mantissa = d.mantissa();
            ColumnData::Numeric(Some(tiberius::numeric::Numeric::new_with_scale(
                mantissa, scale,
            )))
        }
        SqlValue::DateTime(dt) => {
            // Convert chrono::NaiveDateTime to Tiberius DateTime2
            // DateTime2 uses Date (days since year 1) + Time (100-nanosecond increments)
            let epoch = chrono::NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
            let days_i64 = (dt.date() - epoch).num_days();
            // Bounds check: dates before year 1 or beyond u32::MAX days are invalid
            if days_i64 < 0 || days_i64 > u32::MAX as i64 {
                warn!("DateTime out of valid range (days={}), converting to NULL", days_i64);
                return ColumnData::DateTime2(None);
            }
            let days = days_i64 as u32;
            let date = tiberius::time::Date::new(days);
            let time_val = dt.time();
            let nanos = time_val.num_seconds_from_midnight() as u64 * 1_000_000_000
                + time_val.nanosecond() as u64;
            // Scale 7 = 100 nanosecond increments for maximum precision
            let increments = nanos / 100;
            let time = tiberius::time::Time::new(increments, 7);
            ColumnData::DateTime2(Some(tiberius::time::DateTime2::new(date, time)))
        }
        SqlValue::DateTimeOffset(dto) => {
            // Convert chrono::DateTime<FixedOffset> to Tiberius DateTimeOffset
            // DateTimeOffset is DateTime2 + offset in minutes from UTC
            let naive = dto.naive_utc();
            // Days since year 1, January 1
            let epoch = chrono::NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
            let days_i64 = (naive.date() - epoch).num_days();
            // Bounds check: dates before year 1 or beyond u32::MAX days are invalid
            if days_i64 < 0 || days_i64 > u32::MAX as i64 {
                warn!("DateTimeOffset out of valid range (days={}), converting to NULL", days_i64);
                return ColumnData::DateTimeOffset(None);
            }
            let days = days_i64 as u32;
            let date = tiberius::time::Date::new(days);
            // Time with nanosecond precision (scale=7 means 10^-7 second increments)
            let time_val = naive.time();
            let nanos = time_val.num_seconds_from_midnight() as u64 * 1_000_000_000
                + time_val.nanosecond() as u64;
            // Scale 7 = 100 nanosecond increments
            let increments = nanos / 100;
            let time = tiberius::time::Time::new(increments, 7);
            let datetime2 = tiberius::time::DateTime2::new(date, time);
            // Offset in minutes (integer division truncates sub-minute offsets)
            let offset_seconds = dto.offset().local_minus_utc();
            let offset_minutes = (offset_seconds / 60) as i16;
            ColumnData::DateTimeOffset(Some(tiberius::time::DateTimeOffset::new(
                datetime2,
                offset_minutes,
            )))
        }
        SqlValue::Date(d) => {
            // Convert chrono::NaiveDate to Tiberius DateTime2 with midnight time.
            // Note: We use DateTime2 instead of Date because Tiberius bulk insert
            // has issues with the DATE type serialization. The type mapping also
            // maps PostgreSQL date to datetime2 for this reason.
            let epoch = chrono::NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
            let days_i64 = (*d - epoch).num_days();
            // Bounds check: dates before year 1 or beyond u32::MAX days are invalid
            if days_i64 < 0 || days_i64 > u32::MAX as i64 {
                warn!("Date out of valid range (days={}), converting to NULL", days_i64);
                return ColumnData::DateTime2(None);
            }
            let days = days_i64 as u32;
            let date = tiberius::time::Date::new(days);
            // Midnight time (0 increments at scale 7)
            let time = tiberius::time::Time::new(0, 7);
            ColumnData::DateTime2(Some(tiberius::time::DateTime2::new(date, time)))
        }
        SqlValue::Time(t) => {
            // Convert chrono::NaiveTime to Tiberius Time
            // Time is increments of 10^-scale seconds since midnight
            // Using scale=7 (100 nanosecond increments) for maximum precision
            let nanos = t.num_seconds_from_midnight() as u64 * 1_000_000_000
                + t.nanosecond() as u64;
            let increments = nanos / 100; // Convert to 100-nanosecond increments
            ColumnData::Time(Some(tiberius::time::Time::new(increments, 7)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_value_to_column_data_nan_converts_to_null() {
        let nan_f32 = SqlValue::F32(f32::NAN);
        let nan_f64 = SqlValue::F64(f64::NAN);
        let inf_f32 = SqlValue::F32(f32::INFINITY);
        let neg_inf_f64 = SqlValue::F64(f64::NEG_INFINITY);

        assert!(matches!(sql_value_to_column_data(&nan_f32), ColumnData::F32(None)));
        assert!(matches!(sql_value_to_column_data(&nan_f64), ColumnData::F64(None)));
        assert!(matches!(sql_value_to_column_data(&inf_f32), ColumnData::F32(None)));
        assert!(matches!(sql_value_to_column_data(&neg_inf_f64), ColumnData::F64(None)));
    }

    #[test]
    fn test_sql_value_to_column_data_valid_floats() {
        let f32_val = SqlValue::F32(3.14);
        let f64_val = SqlValue::F64(2.718281828);

        assert!(matches!(sql_value_to_column_data(&f32_val), ColumnData::F32(Some(_))));
        assert!(matches!(sql_value_to_column_data(&f64_val), ColumnData::F64(Some(_))));
    }

    #[test]
    fn test_sql_value_to_column_data_null_types() {
        let null_bool = SqlValue::Null(SqlNullType::Bool);
        let null_string = SqlValue::Null(SqlNullType::String);
        let null_datetime = SqlValue::Null(SqlNullType::DateTime);

        assert!(matches!(sql_value_to_column_data(&null_bool), ColumnData::Bit(None)));
        assert!(matches!(sql_value_to_column_data(&null_string), ColumnData::String(None)));
        assert!(matches!(sql_value_to_column_data(&null_datetime), ColumnData::DateTime2(None)));
    }

    #[test]
    fn test_sql_value_to_column_data_basic_types() {
        let bool_val = SqlValue::Bool(true);
        let i32_val = SqlValue::I32(42);
        let i64_val = SqlValue::I64(1234567890);
        let string_val = SqlValue::String("hello".to_string());

        assert!(matches!(sql_value_to_column_data(&bool_val), ColumnData::Bit(Some(true))));
        assert!(matches!(sql_value_to_column_data(&i32_val), ColumnData::I32(Some(42))));
        assert!(matches!(sql_value_to_column_data(&i64_val), ColumnData::I64(Some(1234567890))));
        if let ColumnData::String(Some(s)) = sql_value_to_column_data(&string_val) {
            assert_eq!(s.as_ref(), "hello");
        } else {
            panic!("Expected ColumnData::String");
        }
    }

    #[test]
    fn test_sql_value_to_column_data_valid_date() {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let date_val = SqlValue::Date(date);

        // Date is converted to DateTime2 for bulk insert compatibility
        assert!(matches!(sql_value_to_column_data(&date_val), ColumnData::DateTime2(Some(_))));
    }

    #[test]
    fn test_sql_value_to_column_data_decimal() {
        let decimal = rust_decimal::Decimal::new(12345, 2); // 123.45
        let decimal_val = SqlValue::Decimal(decimal);

        assert!(matches!(sql_value_to_column_data(&decimal_val), ColumnData::Numeric(Some(_))));
    }

    #[test]
    fn test_sql_value_to_column_data_uuid() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let uuid_val = SqlValue::Uuid(uuid);

        assert!(matches!(sql_value_to_column_data(&uuid_val), ColumnData::Guid(Some(_))));
    }

    #[test]
    fn test_row_has_oversized_strings_empty_row() {
        let row: Vec<SqlValue> = vec![];
        assert!(!MssqlTargetPool::row_has_oversized_strings(&row));
    }

    #[test]
    fn test_row_has_oversized_strings_small_strings() {
        let row = vec![
            SqlValue::String("hello".to_string()),
            SqlValue::String("world".to_string()),
            SqlValue::I32(42),
        ];
        assert!(!MssqlTargetPool::row_has_oversized_strings(&row));
    }

    #[test]
    fn test_row_has_oversized_strings_non_string_values() {
        let row = vec![
            SqlValue::I32(42),
            SqlValue::I64(123456789),
            SqlValue::F64(3.14159),
            SqlValue::Bool(true),
        ];
        assert!(!MssqlTargetPool::row_has_oversized_strings(&row));
    }

    #[test]
    fn test_row_has_oversized_strings_at_limit() {
        // 65535 bytes / 2 bytes per BMP char = 32767 chars (and 1 byte remainder)
        // Actually, 65535 / 2 = 32767.5, so 32767 chars = 65534 bytes (under limit)
        let at_limit = "a".repeat(32767); // 32767 * 2 = 65534 bytes (under limit)
        let row = vec![SqlValue::String(at_limit)];
        assert!(!MssqlTargetPool::row_has_oversized_strings(&row));
    }

    #[test]
    fn test_row_has_oversized_strings_over_limit() {
        // 32768 chars * 2 bytes = 65536 bytes (over 65535 limit)
        let over_limit = "a".repeat(32768);
        let row = vec![SqlValue::String(over_limit)];
        assert!(MssqlTargetPool::row_has_oversized_strings(&row));
    }

    #[test]
    fn test_row_has_oversized_strings_with_surrogate_pairs() {
        // Emoji characters require surrogate pairs (4 bytes each in UTF-16)
        // 65535 / 4 = 16383.75, so 16383 emojis = 65532 bytes (under limit)
        // 16384 emojis = 65536 bytes (over limit)
        let under_limit = "😀".repeat(16383); // 16383 * 4 = 65532 bytes
        let row = vec![SqlValue::String(under_limit)];
        assert!(!MssqlTargetPool::row_has_oversized_strings(&row));

        let over_limit = "😀".repeat(16384); // 16384 * 4 = 65536 bytes
        let row = vec![SqlValue::String(over_limit)];
        assert!(MssqlTargetPool::row_has_oversized_strings(&row));
    }

    #[test]
    fn test_row_has_oversized_strings_mixed_row() {
        // One oversized string among normal values should trigger true
        let normal_string = SqlValue::String("hello".to_string());
        let oversized_string = SqlValue::String("x".repeat(40000)); // 80000 bytes
        let row = vec![
            SqlValue::I32(1),
            normal_string,
            oversized_string,
            SqlValue::Bool(false),
        ];
        assert!(MssqlTargetPool::row_has_oversized_strings(&row));
    }

    #[test]
    fn test_build_staging_merge_sql_basic() {
        let cols = vec!["id".to_string(), "name".to_string(), "value".to_string()];
        let pk_cols = vec!["id".to_string()];

        let sql = MssqlTargetPool::build_staging_merge_sql(
            "[dbo].[users]",
            "[dbo].[_staging_users_0]",
            &cols,
            &pk_cols,
        );

        // Should have MERGE INTO target WITH (TABLOCK) for deadlock prevention
        assert!(sql.contains("MERGE INTO [dbo].[users] WITH (TABLOCK) AS target"));
        // Should have USING staging (now in target schema with _staging_ prefix)
        assert!(sql.contains("USING [dbo].[_staging_users_0] AS source"));
        // Should have join condition on PK
        assert!(sql.contains("ON target.[id] = source.[id]"));
        // Should have change detection (MSSQL style with <> and IS NULL checks)
        assert!(sql.contains("WHEN MATCHED AND"));
        assert!(sql.contains("target.[name] <> source.[name]"));
        // Should have UPDATE SET for non-PK columns
        assert!(sql.contains("[name] = source.[name]"));
        assert!(sql.contains("[value] = source.[value]"));
        // Should have INSERT for new rows
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"));
    }

    #[test]
    fn test_build_staging_merge_sql_pk_only() {
        let cols = vec!["id".to_string()];
        let pk_cols = vec!["id".to_string()];

        let sql = MssqlTargetPool::build_staging_merge_sql(
            "[dbo].[lookup]",
            "[dbo].[_staging_lookup_0]",
            &cols,
            &pk_cols,
        );

        // Should have WITH (TABLOCK) for deadlock prevention
        assert!(sql.contains("WITH (TABLOCK)"));
        // PK-only table should not have WHEN MATCHED (no columns to update)
        assert!(!sql.contains("WHEN MATCHED"));
        // Should still have INSERT for new rows
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"));
    }

    #[test]
    fn test_build_staging_merge_sql_composite_pk() {
        let cols = vec![
            "tenant_id".to_string(),
            "user_id".to_string(),
            "role".to_string(),
        ];
        let pk_cols = vec!["tenant_id".to_string(), "user_id".to_string()];

        let sql = MssqlTargetPool::build_staging_merge_sql(
            "[dbo].[user_roles]",
            "[dbo].[_staging_user_roles_0]",
            &cols,
            &pk_cols,
        );

        // Should have composite join condition
        assert!(sql.contains("target.[tenant_id] = source.[tenant_id]"));
        assert!(sql.contains("target.[user_id] = source.[user_id]"));
        // Should only update non-PK column (check UPDATE SET clause format)
        assert!(sql.contains("UPDATE SET [role] = source.[role]"));
        // UPDATE SET should NOT contain PK columns (only role should be in SET)
        // The SET clause should only have one column
        let update_set_start = sql.find("UPDATE SET").unwrap();
        let update_set_part = &sql[update_set_start..];
        assert!(!update_set_part.contains("[tenant_id] = source.[tenant_id]"));
        assert!(!update_set_part.contains("[user_id] = source.[user_id]"));
    }

    #[test]
    fn test_build_staging_merge_sql_null_safe_change_detection() {
        let cols = vec!["id".to_string(), "nullable_col".to_string()];
        let pk_cols = vec!["id".to_string()];

        let sql = MssqlTargetPool::build_staging_merge_sql(
            "[dbo].[test]",
            "[dbo].[_staging_test_0]",
            &cols,
            &pk_cols,
        );

        // Should have NULL-safe change detection pattern for nullable columns
        // Pattern: (col <> col OR (col IS NULL AND col IS NOT NULL) OR (col IS NOT NULL AND col IS NULL))
        assert!(sql.contains("target.[nullable_col] <> source.[nullable_col]"));
        assert!(sql.contains("target.[nullable_col] IS NULL AND source.[nullable_col] IS NOT NULL"));
        assert!(sql.contains("target.[nullable_col] IS NOT NULL AND source.[nullable_col] IS NULL"));
    }

    #[test]
    fn test_partition_rows_by_string_size() {
        // Test helper to simulate the partitioning logic in bulk_insert_to_table
        let normal_row1 = vec![
            SqlValue::I32(1),
            SqlValue::String("short".to_string()),
        ];
        let normal_row2 = vec![
            SqlValue::I32(2),
            SqlValue::String("also short".to_string()),
        ];
        let oversized_row = vec![
            SqlValue::I32(3),
            SqlValue::String("x".repeat(40000)), // 80000 UTF-16 bytes > 65535 limit
        ];
        let rows = vec![normal_row1.clone(), oversized_row.clone(), normal_row2.clone()];

        // Partition rows (mimics bulk_insert_to_table logic)
        let mut bulk_rows = Vec::new();
        let mut oversized_rows = Vec::new();
        for row in &rows {
            if MssqlTargetPool::row_has_oversized_strings(row) {
                oversized_rows.push(row.clone());
            } else {
                bulk_rows.push(row.clone());
            }
        }

        // Should partition correctly
        assert_eq!(bulk_rows.len(), 2);
        assert_eq!(oversized_rows.len(), 1);

        // Verify correct rows in each partition
        assert_eq!(bulk_rows[0], normal_row1);
        assert_eq!(bulk_rows[1], normal_row2);
        assert_eq!(oversized_rows[0], oversized_row);
    }

    #[test]
    fn test_partition_rows_all_normal() {
        let rows: Vec<Vec<SqlValue>> = (0..100)
            .map(|i| vec![
                SqlValue::I32(i),
                SqlValue::String(format!("row_{}", i)),
            ])
            .collect();

        let mut bulk_rows = Vec::new();
        let mut oversized_rows = Vec::new();
        for row in &rows {
            if MssqlTargetPool::row_has_oversized_strings(row) {
                oversized_rows.push(row.clone());
            } else {
                bulk_rows.push(row.clone());
            }
        }

        assert_eq!(bulk_rows.len(), 100);
        assert_eq!(oversized_rows.len(), 0);
    }

    #[test]
    fn test_partition_rows_all_oversized() {
        let oversized_string = "x".repeat(40000);
        let rows: Vec<Vec<SqlValue>> = (0..5)
            .map(|i| vec![
                SqlValue::I32(i),
                SqlValue::String(oversized_string.clone()),
            ])
            .collect();

        let mut bulk_rows = Vec::new();
        let mut oversized_rows = Vec::new();
        for row in &rows {
            if MssqlTargetPool::row_has_oversized_strings(row) {
                oversized_rows.push(row.clone());
            } else {
                bulk_rows.push(row.clone());
            }
        }

        assert_eq!(bulk_rows.len(), 0);
        assert_eq!(oversized_rows.len(), 5);
    }

    #[test]
    fn test_partition_rows_empty() {
        let rows: Vec<Vec<SqlValue>> = vec![];

        let mut bulk_rows = Vec::new();
        let mut oversized_rows = Vec::new();
        for row in &rows {
            if MssqlTargetPool::row_has_oversized_strings(row) {
                oversized_rows.push(row.clone());
            } else {
                bulk_rows.push(row.clone());
            }
        }

        assert_eq!(bulk_rows.len(), 0);
        assert_eq!(oversized_rows.len(), 0);
    }

    #[test]
    fn test_batched_insert_batch_size_calculation() {
        // MSSQL has a 2100 parameter limit per query
        // Test that batch size is correctly calculated based on column count

        // 10 columns -> 2100 / 10 = 210 rows per batch
        let cols_10 = 10;
        let max_rows_10 = (2100 / cols_10).max(1);
        assert_eq!(max_rows_10, 210);

        // 100 columns -> 2100 / 100 = 21 rows per batch
        let cols_100 = 100;
        let max_rows_100 = (2100 / cols_100).max(1);
        assert_eq!(max_rows_100, 21);

        // 3 columns -> 2100 / 3 = 700 rows per batch
        let cols_3 = 3;
        let max_rows_3 = (2100 / cols_3).max(1);
        assert_eq!(max_rows_3, 700);

        // 2100 columns -> 2100 / 2100 = 1 row per batch
        let cols_2100 = 2100;
        let max_rows_2100 = (2100 / cols_2100).max(1);
        assert_eq!(max_rows_2100, 1);

        // 3000 columns -> 2100 / 3000 = 0, but max(1) ensures at least 1
        let cols_3000 = 3000;
        let max_rows_3000 = (2100 / cols_3000).max(1);
        assert_eq!(max_rows_3000, 1);
    }

    #[test]
    fn test_build_staging_merge_sql_multiple_non_pk_columns() {
        let cols = vec![
            "id".to_string(),
            "col1".to_string(),
            "col2".to_string(),
            "col3".to_string(),
        ];
        let pk_cols = vec!["id".to_string()];

        let sql = MssqlTargetPool::build_staging_merge_sql(
            "[dbo].[test]",
            "[dbo].[_staging_test_0]",
            &cols,
            &pk_cols,
        );

        // Should have change detection with OR between columns
        assert!(sql.contains(" OR "));

        // All non-PK columns should be in change detection
        assert!(sql.contains("target.[col1]"));
        assert!(sql.contains("target.[col2]"));
        assert!(sql.contains("target.[col3]"));

        // All non-PK columns should be in UPDATE SET
        assert!(sql.contains("[col1] = source.[col1]"));
        assert!(sql.contains("[col2] = source.[col2]"));
        assert!(sql.contains("[col3] = source.[col3]"));
    }

    #[test]
    fn test_is_deadlock_error_uses_tiberius_builtin() {
        // Note: is_deadlock_error uses tiberius::Error::is_deadlock() which checks error code 1205.
        // We can't easily unit test this without creating actual tiberius Server errors,
        // but we verify the function exists and compiles correctly.
        // Integration tests should cover actual deadlock scenarios.

        // Test with non-server errors (should return false)
        let io_error = tiberius::error::Error::Io {
            kind: std::io::ErrorKind::ConnectionRefused,
            message: "connection refused".to_string(),
        };
        assert!(!MssqlTargetPool::is_deadlock_error(&io_error));

        let protocol_error = tiberius::error::Error::Protocol("protocol error".into());
        assert!(!MssqlTargetPool::is_deadlock_error(&protocol_error));
    }
}
