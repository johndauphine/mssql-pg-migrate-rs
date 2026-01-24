//! MySQL/MariaDB source reader implementation.
//!
//! Implements the `SourceReader` trait for reading data from MySQL/MariaDB databases.
//! Uses mysql_async for connection pooling and async query execution.

use std::borrow::Cow;
use std::collections::HashMap;

use async_trait::async_trait;
use mysql_async::prelude::*;
use mysql_async::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Row as MySqlRow, SslOpts};
use tokio::sync::mpsc;
use tracing::{debug, info};

use crate::config::SourceConfig;
use crate::core::schema::{CheckConstraint, Column, ForeignKey, Index, Partition, Table};
use crate::core::traits::{ReadOptions, SourceReader};
use crate::core::value::{Batch, SqlNullType, SqlValue};
use crate::error::{MigrateError, Result};
// Import target::SqlValue for legacy transfer engine compatibility
use crate::target::{SqlNullType as TargetSqlNullType, SqlValue as TargetSqlValue};

/// MySQL/MariaDB source reader implementation.
pub struct MysqlReader {
    pool: Pool,
    #[allow(dead_code)]
    database: String,
}

impl MysqlReader {
    /// Create a new MySQL reader from configuration.
    pub async fn new(config: &SourceConfig, max_conns: usize) -> Result<Self> {
        // Default to Preferred SSL mode for source connections
        let ssl_opts = Some(SslOpts::default().with_danger_accept_invalid_certs(true));

        let mut builder = OptsBuilder::default()
            .ip_or_hostname(&config.host)
            .tcp_port(config.port)
            .db_name(Some(&config.database))
            .user(Some(&config.user))
            .pass(Some(&config.password))
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
            .map_err(|e| MigrateError::pool(e, "creating MySQL source pool"))?;

        conn.query_drop("SELECT 1")
            .await
            .map_err(|e| MigrateError::pool(e, "testing MySQL source connection"))?;

        drop(conn);

        info!(
            "Connected to MySQL source: {}:{}/{}",
            config.host, config.port, config.database
        );

        Ok(Self {
            pool,
            database: config.database.clone(),
        })
    }

    /// Load columns for a table.
    async fn load_columns(&self, table: &mut Table) -> Result<()> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        // Query column metadata
        let query = r#"
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                CASE
                    WHEN CHARACTER_MAXIMUM_LENGTH IS NULL THEN 0
                    WHEN CHARACTER_MAXIMUM_LENGTH > 2147483647 THEN -1
                    ELSE CHARACTER_MAXIMUM_LENGTH
                END AS max_length,
                COALESCE(NUMERIC_PRECISION, 0) AS num_precision,
                COALESCE(NUMERIC_SCALE, 0) AS num_scale,
                IF(IS_NULLABLE = 'YES', 1, 0) AS is_nullable,
                IF(EXTRA LIKE '%auto_increment%', 1, 0) AS is_identity,
                ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        "#;

        let rows: Vec<MySqlRow> = conn.exec(query, (&table.schema, &table.name)).await?;

        for row in rows {
            let col = Column {
                name: row.get::<String, _>("COLUMN_NAME").unwrap_or_default(),
                data_type: row.get::<String, _>("DATA_TYPE").unwrap_or_default(),
                max_length: row.get::<i32, _>("max_length").unwrap_or(0),
                precision: row.get::<i32, _>("num_precision").unwrap_or(0),
                scale: row.get::<i32, _>("num_scale").unwrap_or(0),
                is_nullable: row.get::<i32, _>("is_nullable").unwrap_or(0) == 1,
                is_identity: row.get::<i32, _>("is_identity").unwrap_or(0) == 1,
                ordinal_pos: row.get::<i32, _>("ORDINAL_POSITION").unwrap_or(0),
            };
            table.columns.push(col);
        }

        Ok(())
    }

    /// Load primary key columns for a table.
    async fn load_primary_key(&self, table: &mut Table) -> Result<()> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        let query = r#"
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
        "#;

        let rows: Vec<MySqlRow> = conn.exec(query, (&table.schema, &table.name)).await?;

        for row in rows {
            let col_name: String = row.get("COLUMN_NAME").unwrap_or_default();
            table.primary_key.push(col_name.clone());

            // Add to pk_columns
            if let Some(col) = table.columns.iter().find(|c| c.name == col_name) {
                table.pk_columns.push(col.clone());
            }
        }

        Ok(())
    }

    /// Quote a MySQL identifier.
    fn quote_ident(name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }

    /// Convert a MySQL row to SqlValue vector.
    fn row_to_values(row: &MySqlRow, columns: &[Column]) -> Vec<SqlValue<'static>> {
        columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let data_type = col.data_type.to_lowercase();

                // Try to get the value, return NULL if column doesn't exist or is null
                Self::convert_column_value(row, i, &data_type)
            })
            .collect()
    }

    /// Convert a single column value from MySQL row to SqlValue.
    fn convert_column_value(row: &MySqlRow, idx: usize, data_type: &str) -> SqlValue<'static> {
        // Check for NULL first
        let is_null = row
            .get_opt::<mysql_async::Value, _>(idx)
            .is_none_or(|opt| opt.map_or(true, |v| matches!(v, mysql_async::Value::NULL)));

        if is_null {
            return SqlValue::Null(Self::null_type_for(data_type));
        }

        // Convert based on data type
        match data_type {
            // Integer types
            "tinyint" => row
                .get::<i8, _>(idx)
                .map(|v| SqlValue::I16(v as i16))
                .unwrap_or(SqlValue::Null(SqlNullType::I16)),
            "smallint" => row
                .get::<i16, _>(idx)
                .map(SqlValue::I16)
                .unwrap_or(SqlValue::Null(SqlNullType::I16)),
            "mediumint" | "int" | "integer" => row
                .get::<i32, _>(idx)
                .map(SqlValue::I32)
                .unwrap_or(SqlValue::Null(SqlNullType::I32)),
            "bigint" => row
                .get::<i64, _>(idx)
                .map(SqlValue::I64)
                .unwrap_or(SqlValue::Null(SqlNullType::I64)),

            // Floating point
            "float" => row
                .get::<f32, _>(idx)
                .map(SqlValue::F32)
                .unwrap_or(SqlValue::Null(SqlNullType::F32)),
            "double" | "real" => row
                .get::<f64, _>(idx)
                .map(SqlValue::F64)
                .unwrap_or(SqlValue::Null(SqlNullType::F64)),

            // Decimal - mysql_async doesn't have rust_decimal FromValue, read as String and parse
            "decimal" | "numeric" => row
                .get::<String, _>(idx)
                .and_then(|s| s.parse::<rust_decimal::Decimal>().ok())
                .map(SqlValue::Decimal)
                .unwrap_or(SqlValue::Null(SqlNullType::Decimal)),

            // Boolean
            "bit" | "boolean" | "bool" => row
                .get::<bool, _>(idx)
                .map(SqlValue::Bool)
                .unwrap_or(SqlValue::Null(SqlNullType::Bool)),

            // String types
            "char" | "varchar" | "text" | "tinytext" | "mediumtext" | "longtext" | "enum"
            | "set" => row
                .get::<String, _>(idx)
                .map(|s| SqlValue::Text(Cow::Owned(s)))
                .unwrap_or(SqlValue::Null(SqlNullType::String)),

            // Binary types
            "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => row
                .get::<Vec<u8>, _>(idx)
                .map(|b| SqlValue::Bytes(Cow::Owned(b)))
                .unwrap_or(SqlValue::Null(SqlNullType::Bytes)),

            // Date/Time types
            "date" => row
                .get::<chrono::NaiveDate, _>(idx)
                .map(SqlValue::Date)
                .unwrap_or(SqlValue::Null(SqlNullType::Date)),
            "time" => row
                .get::<chrono::NaiveTime, _>(idx)
                .map(SqlValue::Time)
                .unwrap_or(SqlValue::Null(SqlNullType::Time)),
            "datetime" | "timestamp" => row
                .get::<chrono::NaiveDateTime, _>(idx)
                .map(SqlValue::DateTime)
                .unwrap_or(SqlValue::Null(SqlNullType::DateTime)),

            // JSON - store as text since SqlValue doesn't have a Json variant
            "json" => row
                .get::<String, _>(idx)
                .map(|s| SqlValue::Text(Cow::Owned(s)))
                .unwrap_or(SqlValue::Null(SqlNullType::String)),

            // Default to string
            _ => row
                .get::<String, _>(idx)
                .map(|s| SqlValue::Text(Cow::Owned(s)))
                .unwrap_or(SqlValue::Null(SqlNullType::String)),
        }
    }

    /// Get the appropriate null type for a MySQL data type.
    fn null_type_for(data_type: &str) -> SqlNullType {
        match data_type {
            "tinyint" | "smallint" => SqlNullType::I16,
            "mediumint" | "int" | "integer" => SqlNullType::I32,
            "bigint" => SqlNullType::I64,
            "float" => SqlNullType::F32,
            "double" | "real" => SqlNullType::F64,
            "decimal" | "numeric" => SqlNullType::Decimal,
            "bit" | "boolean" | "bool" => SqlNullType::Bool,
            "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => {
                SqlNullType::Bytes
            }
            "date" => SqlNullType::Date,
            "time" => SqlNullType::Time,
            "datetime" | "timestamp" => SqlNullType::DateTime,
            _ => SqlNullType::String,
        }
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

    /// Query rows with pre-computed column types for O(1) lookup.
    /// Compatible with legacy transfer engine API.
    /// Returns target::SqlValue for compatibility with transfer engine.
    pub async fn query_rows_fast(
        &self,
        sql: &str,
        _columns: &[String],
        col_types: &[String],
    ) -> Result<Vec<Vec<TargetSqlValue>>> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "query_rows_fast"))?;

        let rows: Vec<MySqlRow> = conn
            .query(sql)
            .await
            .map_err(|e| MigrateError::pool(e, "query_rows_fast"))?;

        let mut result = Vec::with_capacity(rows.len());

        for row in rows {
            let mut values = Vec::with_capacity(col_types.len());

            for (idx, data_type) in col_types.iter().enumerate() {
                let value = Self::convert_value_for_target(&row, idx, data_type);
                values.push(value);
            }

            result.push(values);
        }

        Ok(result)
    }

    /// Convert a MySQL row value to target::SqlValue for legacy API.
    fn convert_value_for_target(row: &MySqlRow, idx: usize, data_type: &str) -> TargetSqlValue {
        let data_type = data_type.to_lowercase();

        // Check for NULL first
        let is_null = row
            .get_opt::<mysql_async::Value, _>(idx)
            .is_none_or(|opt| opt.map_or(true, |v| matches!(v, mysql_async::Value::NULL)));

        if is_null {
            return TargetSqlValue::Null(Self::target_null_type_for(&data_type));
        }

        // Convert based on data type
        match data_type.as_str() {
            // Integer types
            "tinyint" => row
                .get::<i8, _>(idx)
                .map(|v| TargetSqlValue::I16(v as i16))
                .unwrap_or(TargetSqlValue::Null(TargetSqlNullType::I16)),
            "smallint" => row
                .get::<i16, _>(idx)
                .map(TargetSqlValue::I16)
                .unwrap_or(TargetSqlValue::Null(TargetSqlNullType::I16)),
            "mediumint" | "int" | "integer" => row
                .get::<i32, _>(idx)
                .map(TargetSqlValue::I32)
                .unwrap_or(TargetSqlValue::Null(TargetSqlNullType::I32)),
            "bigint" => row
                .get::<i64, _>(idx)
                .map(TargetSqlValue::I64)
                .unwrap_or(TargetSqlValue::Null(TargetSqlNullType::I64)),

            // Floating point
            "float" => row
                .get::<f32, _>(idx)
                .map(TargetSqlValue::F32)
                .unwrap_or(TargetSqlValue::Null(TargetSqlNullType::F32)),
            "double" | "real" => row
                .get::<f64, _>(idx)
                .map(TargetSqlValue::F64)
                .unwrap_or(TargetSqlValue::Null(TargetSqlNullType::F64)),

            // Decimal - mysql_async doesn't have rust_decimal FromValue, read as String and parse
            "decimal" | "numeric" => row
                .get::<String, _>(idx)
                .and_then(|s| s.parse::<rust_decimal::Decimal>().ok())
                .map(TargetSqlValue::Decimal)
                .unwrap_or(TargetSqlValue::Null(TargetSqlNullType::Decimal)),

            // Boolean
            "bit" | "boolean" | "bool" => row
                .get::<bool, _>(idx)
                .map(TargetSqlValue::Bool)
                .unwrap_or(TargetSqlValue::Null(TargetSqlNullType::Bool)),

            // String types
            "char" | "varchar" | "text" | "tinytext" | "mediumtext" | "longtext" | "enum"
            | "set" => row
                .get::<String, _>(idx)
                .map(TargetSqlValue::String)
                .unwrap_or(TargetSqlValue::Null(TargetSqlNullType::String)),

            // Binary types
            "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => row
                .get::<Vec<u8>, _>(idx)
                .map(TargetSqlValue::Bytes)
                .unwrap_or(TargetSqlValue::Null(TargetSqlNullType::Bytes)),

            // Date/Time types
            "date" => row
                .get::<chrono::NaiveDate, _>(idx)
                .map(TargetSqlValue::Date)
                .unwrap_or(TargetSqlValue::Null(TargetSqlNullType::Date)),
            "time" => row
                .get::<chrono::NaiveTime, _>(idx)
                .map(TargetSqlValue::Time)
                .unwrap_or(TargetSqlValue::Null(TargetSqlNullType::Time)),
            "datetime" | "timestamp" => row
                .get::<chrono::NaiveDateTime, _>(idx)
                .map(TargetSqlValue::DateTime)
                .unwrap_or(TargetSqlValue::Null(TargetSqlNullType::DateTime)),

            // UUID (MySQL stores as CHAR(36))
            "uuid" => row
                .get::<String, _>(idx)
                .and_then(|s| uuid::Uuid::parse_str(&s).ok())
                .map(TargetSqlValue::Uuid)
                .unwrap_or(TargetSqlValue::Null(TargetSqlNullType::Uuid)),

            // Default to string
            _ => row
                .get::<String, _>(idx)
                .map(TargetSqlValue::String)
                .unwrap_or(TargetSqlValue::Null(TargetSqlNullType::String)),
        }
    }

    /// Get target null type for a data type string.
    fn target_null_type_for(data_type: &str) -> TargetSqlNullType {
        match data_type {
            "tinyint" | "smallint" => TargetSqlNullType::I16,
            "mediumint" | "int" | "integer" => TargetSqlNullType::I32,
            "bigint" => TargetSqlNullType::I64,
            "float" => TargetSqlNullType::F32,
            "double" | "real" => TargetSqlNullType::F64,
            "decimal" | "numeric" => TargetSqlNullType::Decimal,
            "bit" | "boolean" | "bool" => TargetSqlNullType::Bool,
            "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => {
                TargetSqlNullType::Bytes
            }
            "date" => TargetSqlNullType::Date,
            "time" => TargetSqlNullType::Time,
            "datetime" | "timestamp" => TargetSqlNullType::DateTime,
            _ => TargetSqlNullType::String,
        }
    }
}

#[async_trait]
impl SourceReader for MysqlReader {
    async fn extract_schema(&self, schema: &str) -> Result<Vec<Table>> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        let query = r#"
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        "#;

        let rows: Vec<MySqlRow> = conn.exec(query, (schema,)).await?;

        let mut tables = Vec::new();

        for row in rows {
            let table_name: String = row.get("TABLE_NAME").unwrap_or_default();

            let mut table = Table {
                schema: schema.to_string(),
                name: table_name,
                columns: Vec::new(),
                primary_key: Vec::new(),
                pk_columns: Vec::new(),
                indexes: Vec::new(),
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
                row_count: 0,
                estimated_row_size: 0,
            };

            self.load_columns(&mut table).await?;
            self.load_primary_key(&mut table).await?;

            tables.push(table);
        }

        info!(
            "Extracted {} tables from MySQL schema '{}'",
            tables.len(),
            schema
        );
        Ok(tables)
    }

    async fn load_indexes(&self, table: &mut Table) -> Result<()> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        let query = r#"
            SELECT
                INDEX_NAME,
                GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS columns,
                IF(NON_UNIQUE = 0, 1, 0) AS is_unique
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
              AND INDEX_NAME != 'PRIMARY'
            GROUP BY INDEX_NAME, NON_UNIQUE
        "#;

        let rows: Vec<MySqlRow> = conn.exec(query, (&table.schema, &table.name)).await?;

        for row in rows {
            let name: String = row.get("INDEX_NAME").unwrap_or_default();
            let columns_str: String = row.get("columns").unwrap_or_default();
            let is_unique: i32 = row.get("is_unique").unwrap_or(0);

            let columns: Vec<String> = columns_str.split(',').map(|s| s.to_string()).collect();

            table.indexes.push(Index {
                name,
                columns,
                is_unique: is_unique == 1,
                is_clustered: false,
                include_cols: Vec::new(),
            });
        }

        debug!(
            "Loaded {} indexes for {}.{}",
            table.indexes.len(),
            table.schema,
            table.name
        );
        Ok(())
    }

    async fn load_foreign_keys(&self, table: &mut Table) -> Result<()> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        let query = r#"
            SELECT
                rc.CONSTRAINT_NAME,
                kcu.COLUMN_NAME,
                kcu.REFERENCED_TABLE_SCHEMA,
                kcu.REFERENCED_TABLE_NAME,
                kcu.REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
                AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND rc.TABLE_NAME = kcu.TABLE_NAME
            WHERE rc.CONSTRAINT_SCHEMA = ? AND rc.TABLE_NAME = ?
            ORDER BY rc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
        "#;

        let rows: Vec<MySqlRow> = conn.exec(query, (&table.schema, &table.name)).await?;

        // Group by constraint name
        let mut fk_map: HashMap<String, ForeignKey> = HashMap::new();

        for row in rows {
            let name: String = row.get("CONSTRAINT_NAME").unwrap_or_default();
            let column: String = row.get("COLUMN_NAME").unwrap_or_default();
            let ref_schema: String = row.get("REFERENCED_TABLE_SCHEMA").unwrap_or_default();
            let ref_table: String = row.get("REFERENCED_TABLE_NAME").unwrap_or_default();
            let ref_column: String = row.get("REFERENCED_COLUMN_NAME").unwrap_or_default();

            let fk = fk_map.entry(name.clone()).or_insert_with(|| ForeignKey {
                name,
                columns: Vec::new(),
                ref_schema,
                ref_table,
                ref_columns: Vec::new(),
                on_delete: String::new(),
                on_update: String::new(),
            });

            fk.columns.push(column);
            fk.ref_columns.push(ref_column);
        }

        table.foreign_keys = fk_map.into_values().collect();

        debug!(
            "Loaded {} foreign keys for {}.{}",
            table.foreign_keys.len(),
            table.schema,
            table.name
        );
        Ok(())
    }

    async fn load_check_constraints(&self, table: &mut Table) -> Result<()> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        // MySQL 8.0+ supports check constraints via INFORMATION_SCHEMA.CHECK_CONSTRAINTS
        let query = r#"
            SELECT
                cc.CONSTRAINT_NAME,
                cc.CHECK_CLAUSE
            FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc
            JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
                AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            WHERE tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ?
              AND tc.CONSTRAINT_TYPE = 'CHECK'
        "#;

        // Silently ignore if CHECK_CONSTRAINTS table doesn't exist (MySQL < 8.0)
        if let Ok(rows) = conn
            .exec::<MySqlRow, _, _>(query, (&table.schema, &table.name))
            .await
        {
            for row in rows {
                let name: String = row.get("CONSTRAINT_NAME").unwrap_or_default();
                let expression: String = row.get("CHECK_CLAUSE").unwrap_or_default();

                table.check_constraints.push(CheckConstraint {
                    name,
                    definition: expression,
                });
            }

            debug!(
                "Loaded {} check constraints for {}.{}",
                table.check_constraints.len(),
                table.schema,
                table.name
            );
        }

        Ok(())
    }

    fn read_table(&self, opts: ReadOptions) -> mpsc::Receiver<Result<Batch>> {
        let (tx, rx) = mpsc::channel(16);
        let pool = self.pool.clone();
        let batch_size = opts.batch_size;

        tokio::spawn(async move {
            let result = Self::read_table_impl(pool, opts, batch_size, tx.clone()).await;
            if let Err(e) = result {
                let _ = tx.send(Err(e)).await;
            }
        });

        rx
    }

    async fn get_partition_boundaries(
        &self,
        table: &Table,
        num_partitions: usize,
    ) -> Result<Vec<Partition>> {
        if table.primary_key.is_empty() || num_partitions <= 1 {
            return Ok(vec![Partition {
                table_name: table.full_name(),
                partition_id: 0,
                min_pk: None,
                max_pk: None,
                start_row: 0,
                end_row: table.row_count,
                row_count: table.row_count,
            }]);
        }

        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        let pk_col = &table.primary_key[0];
        let query = format!(
            "SELECT MIN({}) AS min_pk, MAX({}) AS max_pk FROM {}.{}",
            Self::quote_ident(pk_col),
            Self::quote_ident(pk_col),
            Self::quote_ident(&table.schema),
            Self::quote_ident(&table.name)
        );

        let row: Option<MySqlRow> = conn.query_first(&query).await?;

        match row {
            Some(row) => {
                let min_pk: Option<i64> = row.get("min_pk");
                let max_pk: Option<i64> = row.get("max_pk");

                match (min_pk, max_pk) {
                    (Some(min), Some(max)) if max > min => {
                        let range = max - min;
                        let partition_size = range / num_partitions as i64;
                        let rows_per_partition = table.row_count / num_partitions as i64;

                        let partitions: Vec<Partition> = (0..num_partitions)
                            .map(|i| {
                                let start = if i == 0 {
                                    None
                                } else {
                                    Some(min + (i as i64 * partition_size))
                                };
                                let end = if i == num_partitions - 1 {
                                    None
                                } else {
                                    Some(min + ((i + 1) as i64 * partition_size))
                                };
                                Partition {
                                    table_name: table.full_name(),
                                    partition_id: i as i32,
                                    min_pk: start,
                                    max_pk: end,
                                    start_row: i as i64 * rows_per_partition,
                                    end_row: if i == num_partitions - 1 {
                                        table.row_count
                                    } else {
                                        (i + 1) as i64 * rows_per_partition
                                    },
                                    row_count: rows_per_partition,
                                }
                            })
                            .collect();

                        Ok(partitions)
                    }
                    _ => Ok(vec![Partition {
                        table_name: table.full_name(),
                        partition_id: 0,
                        min_pk: None,
                        max_pk: None,
                        start_row: 0,
                        end_row: table.row_count,
                        row_count: table.row_count,
                    }]),
                }
            }
            None => Ok(vec![Partition {
                table_name: table.full_name(),
                partition_id: 0,
                min_pk: None,
                max_pk: None,
                start_row: 0,
                end_row: table.row_count,
                row_count: table.row_count,
            }]),
        }
    }

    async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        let query = format!(
            "SELECT COUNT(*) AS cnt FROM {}.{}",
            Self::quote_ident(schema),
            Self::quote_ident(table)
        );

        let count: Option<i64> = conn.query_first(&query).await?;
        Ok(count.unwrap_or(0))
    }

    async fn get_max_pk(&self, schema: &str, table: &str, pk_col: &str) -> Result<i64> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection"))?;

        let query = format!(
            "SELECT MAX({}) AS max_pk FROM {}.{}",
            Self::quote_ident(pk_col),
            Self::quote_ident(schema),
            Self::quote_ident(table)
        );

        let max_pk: Option<i64> = conn.query_first(&query).await?;

        max_pk.ok_or_else(|| MigrateError::Transfer {
            table: format!("{}.{}", schema, table),
            message: "Failed to get max PK value".to_string(),
        })
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

/// Represents a primary key value for keyset pagination.
/// Supports both numeric and text-based primary keys.
#[derive(Clone, Debug)]
enum LastPkValue {
    Int(i64),
    Text(String),
}

impl MysqlReader {
    async fn read_table_impl(
        pool: Pool,
        opts: ReadOptions,
        batch_size: usize,
        tx: mpsc::Sender<Result<Batch>>,
    ) -> Result<()> {
        let columns = &opts.columns;
        let col_list = columns
            .iter()
            .map(|c| Self::quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");

        let table_ref = format!(
            "{}.{}",
            Self::quote_ident(&opts.schema),
            Self::quote_ident(&opts.table)
        );

        // Track last PK value for keyset pagination
        let mut last_pk: Option<LastPkValue> = opts.min_pk.map(LastPkValue::Int);
        // Track offset for fallback pagination
        let mut offset: usize = 0;
        // Whether to use offset-based pagination
        let mut use_offset_pagination = false;

        // Get PK column name from pk_idx
        let pk_col: Option<&str> = opts
            .pk_idx
            .and_then(|idx| columns.get(idx).map(|s| s.as_str()));

        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MySQL connection for read"))?;

        loop {
            let mut query = format!("SELECT {} FROM {}", col_list, table_ref);

            let mut conditions = Vec::new();

            // Build keyset pagination condition
            if !use_offset_pagination {
                if let (Some(pk), Some(ref lpk)) = (pk_col, &last_pk) {
                    match lpk {
                        LastPkValue::Int(v) => {
                            conditions.push(format!("{} > {}", Self::quote_ident(pk), v));
                        }
                        LastPkValue::Text(v) => {
                            let escaped = v.replace('\'', "''");
                            conditions.push(format!("{} > '{}'", Self::quote_ident(pk), escaped));
                        }
                    }
                }
                if let (Some(pk), Some(mpk)) = (pk_col, opts.max_pk) {
                    conditions.push(format!("{} <= {}", Self::quote_ident(pk), mpk));
                }
            }

            // Add additional where clause if provided
            if let Some(ref where_clause) = opts.where_clause {
                conditions.push(where_clause.clone());
            }

            if !conditions.is_empty() {
                query.push_str(" WHERE ");
                query.push_str(&conditions.join(" AND "));
            }

            if let Some(pk) = pk_col {
                query.push_str(&format!(" ORDER BY {}", Self::quote_ident(pk)));
            }

            query.push_str(&format!(" LIMIT {}", batch_size));

            // Add OFFSET for fallback pagination
            if use_offset_pagination && offset > 0 {
                query.push_str(&format!(" OFFSET {}", offset));
            }

            let rows: Vec<MySqlRow> = conn.query(&query).await?;

            if rows.is_empty() {
                break;
            }

            // Build column metadata for conversion
            let col_meta: Vec<Column> = columns
                .iter()
                .enumerate()
                .map(|(i, name)| Column {
                    name: name.clone(),
                    data_type: opts.col_types.get(i).cloned().unwrap_or_default(),
                    max_length: 0,
                    precision: 0,
                    scale: 0,
                    is_nullable: true,
                    is_identity: false,
                    ordinal_pos: i as i32,
                })
                .collect();

            let batch_rows: Vec<Vec<SqlValue<'static>>> = rows
                .iter()
                .map(|row| Self::row_to_values(row, &col_meta))
                .collect();

            // Track last PK for keyset pagination
            let last_key = opts.pk_idx.and_then(|idx| {
                batch_rows.last().and_then(|row| match &row[idx] {
                    SqlValue::I64(v) => Some(SqlValue::I64(*v)),
                    SqlValue::I32(v) => Some(SqlValue::I64(*v as i64)),
                    SqlValue::I16(v) => Some(SqlValue::I64(*v as i64)),
                    SqlValue::Text(s) => Some(SqlValue::Text(s.clone())),
                    _ => None,
                })
            });

            // Update last_pk for next iteration
            let new_last_pk: Option<LastPkValue> = opts.pk_idx.and_then(|idx| {
                batch_rows.last().and_then(|row| match &row[idx] {
                    SqlValue::I64(v) => Some(LastPkValue::Int(*v)),
                    SqlValue::I32(v) => Some(LastPkValue::Int(*v as i64)),
                    SqlValue::I16(v) => Some(LastPkValue::Int(*v as i64)),
                    SqlValue::Text(s) => Some(LastPkValue::Text(s.to_string())),
                    _ => None,
                })
            });

            // Fall back to OFFSET if keyset pagination isn't possible
            if new_last_pk.is_none() && pk_col.is_some() && !use_offset_pagination {
                debug!(
                    "Table {}.{}: PK type not suitable for keyset pagination, falling back to OFFSET",
                    opts.schema, opts.table
                );
                use_offset_pagination = true;
            }

            if use_offset_pagination {
                offset += batch_rows.len();
            } else {
                last_pk = new_last_pk;
            }

            let is_last = batch_rows.len() < batch_size;
            let batch = Batch {
                rows: batch_rows,
                last_key,
                is_last,
            };

            if tx.send(Ok(batch)).await.is_err() {
                break; // Receiver dropped
            }

            if is_last {
                break;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_ident() {
        assert_eq!(MysqlReader::quote_ident("name"), "`name`");
        assert_eq!(MysqlReader::quote_ident("table`name"), "`table``name`");
    }

    #[test]
    fn test_null_type_for() {
        assert!(matches!(
            MysqlReader::null_type_for("int"),
            SqlNullType::I32
        ));
        assert!(matches!(
            MysqlReader::null_type_for("bigint"),
            SqlNullType::I64
        ));
        assert!(matches!(
            MysqlReader::null_type_for("varchar"),
            SqlNullType::String
        ));
        assert!(matches!(
            MysqlReader::null_type_for("blob"),
            SqlNullType::Bytes
        ));
    }
}
