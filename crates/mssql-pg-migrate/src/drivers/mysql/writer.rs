//! MySQL/MariaDB target writer implementation.
//!
//! Implements the `TargetWriter` trait for writing data to MySQL/MariaDB databases.
//! Uses SQLx for connection pooling and multi-row INSERT batching.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlSslMode};
use sqlx::Row;
use tracing::{debug, info, warn};

use crate::config::TargetConfig;
use crate::core::schema::{CheckConstraint, Column, ForeignKey, Index, Table};
use crate::core::traits::{TargetWriter, TypeMapper};
use crate::core::value::{Batch, SqlValue};
use crate::error::{MigrateError, Result};

/// Connection pool timeout.
const POOL_CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// MySQL maximum placeholders per prepared statement.
/// MySQL has a hard limit of 65,535 placeholders.
const MYSQL_MAX_PLACEHOLDERS: usize = 65535;

/// MySQL target writer implementation.
pub struct MysqlWriter {
    pool: MySqlPool,
    type_mapper: Option<Arc<dyn TypeMapper>>,
}

impl MysqlWriter {
    /// Create a new MySQL writer from configuration.
    pub async fn new(config: &TargetConfig, max_conns: usize) -> Result<Self> {
        let ssl_mode = match config.ssl_mode.to_lowercase().as_str() {
            "disable" => {
                warn!("MySQL TLS is disabled. Credentials will be transmitted in plaintext.");
                MySqlSslMode::Disabled
            }
            "prefer" => MySqlSslMode::Preferred,
            "require" => MySqlSslMode::Required,
            "verify-ca" | "verify_ca" => MySqlSslMode::VerifyCa,
            "verify-full" | "verify_identity" => MySqlSslMode::VerifyIdentity,
            _ => {
                warn!(
                    "Unknown ssl_mode '{}', defaulting to Preferred",
                    config.ssl_mode
                );
                MySqlSslMode::Preferred
            }
        };

        let options = MySqlConnectOptions::new()
            .host(&config.host)
            .port(config.port)
            .database(&config.database)
            .username(&config.user)
            .password(&config.password)
            .ssl_mode(ssl_mode)
            // Enforce utf8mb4 for full Unicode support
            .charset("utf8mb4");

        let pool = MySqlPoolOptions::new()
            .max_connections(max_conns as u32)
            .acquire_timeout(POOL_CONNECTION_TIMEOUT)
            .connect_with(options)
            .await
            .map_err(|e| MigrateError::pool(e, "creating MySQL target pool"))?;

        // Test connection
        sqlx::query("SELECT 1")
            .execute(&pool)
            .await
            .map_err(|e| MigrateError::pool(e, "testing MySQL target connection"))?;

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
    pub fn pool(&self) -> MySqlPool {
        self.pool.clone()
    }

    /// Test the database connection.
    pub async fn test_connection(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
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
            // This is a fallback for the common case of migrating from MSSQL
            use crate::dialect::mssql_to_mysql_basic;
            let target_type = mssql_to_mysql_basic(&col.data_type, col.max_length, col.precision, col.scale);
            if target_type.is_empty() {
                // Unknown type - pass through with warning
                warn!("Unknown MSSQL type '{}' for column '{}', passing through", col.data_type, col.name);
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
}

#[async_trait]
impl TargetWriter for MysqlWriter {
    async fn create_schema(&self, schema: &str) -> Result<()> {
        let sql = format!(
            "CREATE DATABASE IF NOT EXISTS {} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
            Self::quote_ident(schema)
        );
        sqlx::query(&sql).execute(&self.pool).await?;

        debug!("Created database '{}'", schema);
        Ok(())
    }

    async fn create_table(&self, table: &Table, target_schema: &str) -> Result<()> {
        let ddl = self.generate_ddl(table, target_schema);
        sqlx::query(&ddl).execute(&self.pool).await?;

        debug!("Created table {}.{}", target_schema, table.name);
        Ok(())
    }

    async fn create_table_unlogged(&self, table: &Table, target_schema: &str) -> Result<()> {
        // MySQL doesn't have UNLOGGED tables like PostgreSQL.
        // For faster initial loads, we could use MEMORY engine but it has limitations.
        // Instead, we disable keys temporarily which achieves similar goals.
        let ddl = self.generate_ddl(table, target_schema);
        sqlx::query(&ddl).execute(&self.pool).await?;

        debug!(
            "Created table {}.{} (MySQL doesn't support UNLOGGED)",
            target_schema, table.name
        );
        Ok(())
    }

    async fn drop_table(&self, schema: &str, table: &str) -> Result<()> {
        let sql = format!("DROP TABLE IF EXISTS {}", Self::qualify_table(schema, table));
        sqlx::query(&sql).execute(&self.pool).await?;

        debug!("Dropped table {}.{}", schema, table);
        Ok(())
    }

    async fn truncate_table(&self, schema: &str, table: &str) -> Result<()> {
        let sql = format!("TRUNCATE TABLE {}", Self::qualify_table(schema, table));
        sqlx::query(&sql).execute(&self.pool).await?;

        debug!("Truncated table {}.{}", schema, table);
        Ok(())
    }

    async fn table_exists(&self, schema: &str, table: &str) -> Result<bool> {
        let sql = r#"
            SELECT COUNT(*) as cnt FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        "#;

        let row = sqlx::query(sql)
            .bind(schema)
            .bind(table)
            .fetch_one(&self.pool)
            .await?;

        let count: i64 = row.get("cnt");
        Ok(count > 0)
    }

    async fn create_primary_key(&self, table: &Table, target_schema: &str) -> Result<()> {
        if table.primary_key.is_empty() {
            return Ok(());
        }

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

        sqlx::query(&sql).execute(&self.pool).await?;
        debug!(
            "Created primary key on {}.{}",
            target_schema, table.name
        );
        Ok(())
    }

    async fn create_index(&self, table: &Table, idx: &Index, target_schema: &str) -> Result<()> {
        let idx_cols: Vec<String> = idx.columns.iter().map(|c| Self::quote_ident(c)).collect();
        let unique = if idx.is_unique { "UNIQUE " } else { "" };

        let sql = format!(
            "CREATE {}INDEX {} ON {} ({})",
            unique,
            Self::quote_ident(&idx.name),
            Self::qualify_table(target_schema, &table.name),
            idx_cols.join(", ")
        );

        sqlx::query(&sql).execute(&self.pool).await?;
        debug!(
            "Created index {} on {}.{}",
            idx.name, target_schema, table.name
        );
        Ok(())
    }

    async fn drop_non_pk_indexes(&self, schema: &str, table: &str) -> Result<Vec<String>> {
        // Query non-PK indexes
        let query = r#"
            SELECT DISTINCT INDEX_NAME
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME != 'PRIMARY'
        "#;

        let rows = sqlx::query(query)
            .bind(schema)
            .bind(table)
            .fetch_all(&self.pool)
            .await?;

        let mut dropped_indexes = Vec::new();

        for row in rows {
            let name: String = row.get("INDEX_NAME");
            let drop_sql = format!(
                "DROP INDEX {} ON {}",
                Self::quote_ident(&name),
                Self::qualify_table(schema, table)
            );
            sqlx::query(&drop_sql).execute(&self.pool).await?;
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
        let fk_cols: Vec<String> = fk.columns.iter().map(|c| Self::quote_ident(c)).collect();
        let ref_cols: Vec<String> = fk.ref_columns.iter().map(|c| Self::quote_ident(c)).collect();

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

        sqlx::query(&sql).execute(&self.pool).await?;
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
        // MySQL 8.0.16+ supports CHECK constraints
        let sql = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} {}",
            Self::qualify_table(target_schema, &table.name),
            Self::quote_ident(&chk.name),
            convert_check_definition(&chk.definition)
        );

        match sqlx::query(&sql).execute(&self.pool).await {
            Ok(_) => {
                debug!(
                    "Created check constraint {} on {}.{}",
                    chk.name, target_schema, table.name
                );
            }
            Err(e) => {
                // CHECK constraints may not be supported on older MySQL versions
                warn!(
                    "Failed to create check constraint {} on {}.{}: {}",
                    chk.name, target_schema, table.name, e
                );
            }
        }
        Ok(())
    }

    async fn has_primary_key(&self, schema: &str, table: &str) -> Result<bool> {
        let sql = r#"
            SELECT COUNT(*) as cnt
            FROM information_schema.TABLE_CONSTRAINTS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_TYPE = 'PRIMARY KEY'
        "#;

        let row = sqlx::query(sql)
            .bind(schema)
            .bind(table)
            .fetch_one(&self.pool)
            .await?;

        let count: i64 = row.get("cnt");
        Ok(count > 0)
    }

    async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        let sql = format!(
            "SELECT COUNT(*) as cnt FROM {}",
            Self::qualify_table(schema, table)
        );
        let row = sqlx::query(&sql).fetch_one(&self.pool).await?;
        Ok(row.get("cnt"))
    }

    async fn reset_sequence(&self, schema: &str, table: &Table) -> Result<()> {
        if table.primary_key.is_empty() {
            return Ok(());
        }

        // In MySQL, AUTO_INCREMENT is reset by setting the table's auto_increment value
        for pk_col in &table.primary_key {
            // Check if this is an auto_increment column
            let check_sql = r#"
                SELECT COLUMN_NAME FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ? AND EXTRA LIKE '%auto_increment%'
            "#;

            let result = sqlx::query(check_sql)
                .bind(schema)
                .bind(&table.name)
                .bind(pk_col)
                .fetch_optional(&self.pool)
                .await?;

            if result.is_some() {
                // Get max value and set auto_increment
                let max_sql = format!(
                    "SELECT COALESCE(MAX({}), 0) as max_val FROM {}",
                    Self::quote_ident(pk_col),
                    Self::qualify_table(schema, &table.name)
                );

                let row = sqlx::query(&max_sql).fetch_one(&self.pool).await?;
                let max_val: i64 = row.get("max_val");

                let reset_sql = format!(
                    "ALTER TABLE {} AUTO_INCREMENT = {}",
                    Self::qualify_table(schema, &table.name),
                    max_val + 1
                );
                sqlx::query(&reset_sql).execute(&self.pool).await?;

                debug!(
                    "Reset AUTO_INCREMENT to {} for {}.{}",
                    max_val + 1,
                    schema,
                    table.name
                );
            }
        }

        Ok(())
    }

    async fn set_table_logged(&self, _schema: &str, _table: &str) -> Result<()> {
        // MySQL doesn't have UNLOGGED tables
        // This is a no-op
        Ok(())
    }

    async fn set_table_unlogged(&self, _schema: &str, _table: &str) -> Result<()> {
        // MySQL doesn't have UNLOGGED tables
        // This is a no-op
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

        let row_count = rows.len() as u64;
        let qualified_table = Self::qualify_table(schema, table);
        let col_list: Vec<String> = cols.iter().map(|c| Self::quote_ident(c)).collect();
        let col_list_str = col_list.join(", ");

        // Calculate max rows per batch to stay under MySQL's placeholder limit
        let num_cols = cols.len();
        let max_rows_per_batch = if num_cols > 0 {
            MYSQL_MAX_PLACEHOLDERS / num_cols
        } else {
            return Ok(0);
        };

        // Use explicit transaction for batch safety
        let mut tx = self.pool.begin().await?;

        // Process rows in sub-batches to respect MySQL placeholder limit
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

            let mut query = sqlx::query(&sql);

            for row in chunk {
                for value in row {
                    query = bind_value(query, value);
                }
            }

            query.execute(&mut *tx).await.map_err(|e| {
                MigrateError::transfer(&qualified_table, format!("INSERT batch: {}", e))
            })?;
        }

        tx.commit().await?;

        debug!("MySQL: wrote {} rows to {}", row_count, qualified_table);
        Ok(row_count)
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

        // Calculate max rows per batch to stay under MySQL's placeholder limit
        let num_cols = cols.len();
        let max_rows_per_batch = if num_cols > 0 {
            MYSQL_MAX_PLACEHOLDERS / num_cols
        } else {
            return Ok(0);
        };

        // Create staging table
        let staging_name = match partition_id {
            Some(pid) => format!("_staging_{}_p{}_{}", table, pid, writer_id),
            None => format!("_staging_{}_{}", table, writer_id),
        };
        let staging_qualified = Self::qualify_table(schema, &staging_name);

        let mut tx = self.pool.begin().await?;

        // Drop if exists, create staging as copy of target structure
        let drop_sql = format!("DROP TABLE IF EXISTS {}", staging_qualified);
        sqlx::query(&drop_sql).execute(&mut *tx).await?;

        let create_sql = format!(
            "CREATE TABLE {} LIKE {}",
            staging_qualified, qualified_target
        );
        sqlx::query(&create_sql).execute(&mut *tx).await?;

        // Insert into staging table in sub-batches to respect MySQL placeholder limit
        for chunk in rows.chunks(max_rows_per_batch) {
            let placeholders_per_row = format!("({})", vec!["?"; num_cols].join(", "));
            let all_placeholders: Vec<String> =
                std::iter::repeat_n(placeholders_per_row, chunk.len()).collect();

            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                staging_qualified,
                col_list_str,
                all_placeholders.join(", ")
            );

            let mut insert_query = sqlx::query(&insert_sql);
            for row in chunk {
                for value in row {
                    insert_query = bind_value(insert_query, value);
                }
            }

            insert_query.execute(&mut *tx).await?;
        }

        // Build INSERT ... ON DUPLICATE KEY UPDATE from staging
        let select_cols = cols
            .iter()
            .map(|c| format!("s.{}", Self::quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");

        let non_pk_cols: Vec<_> = cols.iter().filter(|c| !pk_cols.contains(c)).collect();

        let upsert_sql = if non_pk_cols.is_empty() {
            // Only PK columns - use INSERT IGNORE
            format!(
                "INSERT IGNORE INTO {} ({}) SELECT {} FROM {} AS s",
                qualified_target,
                col_list_str,
                select_cols,
                staging_qualified
            )
        } else {
            // Has non-PK columns - use ON DUPLICATE KEY UPDATE
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
                qualified_target,
                col_list_str,
                select_cols,
                staging_qualified,
                update_set
            )
        };

        sqlx::query(&upsert_sql).execute(&mut *tx).await?;

        // Drop staging
        let drop_staging_sql = format!("DROP TABLE IF EXISTS {}", staging_qualified);
        sqlx::query(&drop_staging_sql).execute(&mut *tx).await?;

        tx.commit().await?;

        debug!("MySQL: upserted {} rows to {}", row_count, qualified_target);
        Ok(row_count)
    }

    fn db_type(&self) -> &str {
        "mysql"
    }

    async fn close(&self) {
        self.pool.close().await;
    }

    fn supports_direct_copy(&self) -> bool {
        false
    }
}

/// Bind a SqlValue to a sqlx query.
fn bind_value<'q>(
    query: sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    value: &'q SqlValue<'_>,
) -> sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    match value {
        SqlValue::Null(_) => query.bind(None::<String>),
        SqlValue::Bool(b) => query.bind(*b),
        SqlValue::I16(i) => query.bind(*i),
        SqlValue::I32(i) => query.bind(*i),
        SqlValue::I64(i) => query.bind(*i),
        SqlValue::F32(f) => query.bind(*f),
        SqlValue::F64(f) => query.bind(*f),
        SqlValue::Text(s) => query.bind(s.as_ref()),
        SqlValue::Bytes(b) => query.bind(b.as_ref()),
        SqlValue::Uuid(u) => query.bind(u.to_string()),
        SqlValue::Decimal(d) => query.bind(d.to_string()),
        SqlValue::DateTime(dt) => query.bind(*dt),
        SqlValue::DateTimeOffset(dto) => query.bind(dto.naive_utc()),
        SqlValue::Date(d) => query.bind(*d),
        SqlValue::Time(t) => query.bind(*t),
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
