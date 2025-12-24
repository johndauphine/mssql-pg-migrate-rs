//! MSSQL source database operations.

mod types;

pub use types::*;

use crate::config::SourceConfig;
use crate::error::{MigrateError, Result};
use crate::target::{SqlNullType, SqlValue};
use async_trait::async_trait;
use bb8::{Pool, PooledConnection};
use chrono::NaiveDateTime;
use tiberius::{AuthMethod, Client, Config, EncryptionLevel, Query, Row};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::{debug, info};
use uuid::Uuid;

/// Trait for source database operations.
#[async_trait]
pub trait SourcePool: Send + Sync {
    /// Extract schema information from the source database.
    async fn extract_schema(&self, schema: &str) -> Result<Vec<Table>>;

    /// Get partition boundaries for parallel reads.
    async fn get_partition_boundaries(
        &self,
        table: &Table,
        num_partitions: usize,
    ) -> Result<Vec<Partition>>;

    /// Load index metadata for a table.
    async fn load_indexes(&self, table: &mut Table) -> Result<()>;

    /// Load foreign key metadata for a table.
    async fn load_foreign_keys(&self, table: &mut Table) -> Result<()>;

    /// Load check constraint metadata for a table.
    async fn load_check_constraints(&self, table: &mut Table) -> Result<()>;

    /// Get the row count for a table.
    async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64>;

    /// Get the database type.
    fn db_type(&self) -> &str;

    /// Close all connections.
    async fn close(&self);
}

/// Connection manager for bb8 pool with tiberius.
#[derive(Clone)]
struct TiberiusConnectionManager {
    config: SourceConfig,
}

impl TiberiusConnectionManager {
    fn new(config: SourceConfig) -> Self {
        Self { config }
    }

    fn build_config(&self) -> Config {
        let mut config = Config::new();
        config.host(&self.config.host);
        config.port(self.config.port);
        config.database(&self.config.database);
        config.authentication(AuthMethod::sql_server(&self.config.user, &self.config.password));

        // Encryption settings
        match self.config.encrypt.to_lowercase().as_str() {
            "false" | "no" | "0" | "disable" => {
                config.encryption(EncryptionLevel::NotSupported);
            }
            _ => {
                if self.config.trust_server_cert {
                    config.trust_cert();
                }
                config.encryption(EncryptionLevel::Required);
            }
        }

        config
    }
}

#[async_trait]
impl bb8::ManageConnection for TiberiusConnectionManager {
    type Connection = Client<Compat<TcpStream>>;
    type Error = tiberius::error::Error;

    async fn connect(&self) -> std::result::Result<Self::Connection, Self::Error> {
        let config = self.build_config();
        let tcp = TcpStream::connect(config.get_addr())
            .await
            .map_err(|e| tiberius::error::Error::Io {
                kind: e.kind(),
                message: e.to_string(),
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

/// MSSQL source pool implementation with connection pooling.
pub struct MssqlPool {
    pool: Pool<TiberiusConnectionManager>,
    config: SourceConfig,
}

impl MssqlPool {
    /// Create a new MSSQL source pool with connection pooling.
    pub async fn new(config: SourceConfig) -> Result<Self> {
        Self::with_max_connections(config, 8).await
    }

    /// Create a new MSSQL source pool with specified max connections.
    pub async fn with_max_connections(config: SourceConfig, max_size: u32) -> Result<Self> {
        let manager = TiberiusConnectionManager::new(config.clone());
        let pool = Pool::builder()
            .max_size(max_size)
            .min_idle(Some(1))
            .build(manager)
            .await
            .map_err(|e| MigrateError::Pool(format!("Failed to create MSSQL pool: {}", e)))?;

        // Test connection
        {
            let mut conn = pool.get().await
                .map_err(|e| MigrateError::Pool(format!("Failed to get connection: {}", e)))?;

            conn.simple_query("SELECT 1")
                .await
                .map_err(|e| MigrateError::Source(e))?
                .into_row()
                .await
                .map_err(|e| MigrateError::Source(e))?;
        }

        info!(
            "Connected to MSSQL: {}:{}/{} (pool_size={})",
            config.host, config.port, config.database, max_size
        );

        Ok(Self { pool, config })
    }

    /// Get a pooled connection.
    async fn get_client(&self) -> Result<PooledConnection<'_, TiberiusConnectionManager>> {
        self.pool.get().await
            .map_err(|e| MigrateError::Pool(format!("Failed to get connection: {}", e)))
    }

    /// Load columns for a table.
    async fn load_columns(&self, client: &mut Client<Compat<TcpStream>>, table: &mut Table) -> Result<()> {
        let query = r#"
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                CAST(ISNULL(CHARACTER_MAXIMUM_LENGTH, 0) AS INT),
                CAST(ISNULL(NUMERIC_PRECISION, 0) AS INT),
                CAST(ISNULL(NUMERIC_SCALE, 0) AS INT),
                CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END,
                ISNULL(COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity'), 0),
                ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = @P1 AND TABLE_NAME = @P2
            ORDER BY ORDINAL_POSITION
        "#;

        let mut query = Query::new(query);
        query.bind(&table.schema);
        query.bind(&table.name);

        let stream = query.query(client).await.map_err(|e| MigrateError::Source(e))?;
        let rows = stream.into_first_result().await.map_err(|e| MigrateError::Source(e))?;

        for row in rows {
            let col = Column {
                name: row.get::<&str, _>(0).unwrap_or_default().to_string(),
                data_type: row.get::<&str, _>(1).unwrap_or_default().to_string(),
                max_length: row.get::<i32, _>(2).unwrap_or(0),
                precision: row.get::<i32, _>(3).unwrap_or(0),
                scale: row.get::<i32, _>(4).unwrap_or(0),
                is_nullable: row.get::<i32, _>(5).unwrap_or(0) == 1,
                is_identity: row.get::<i32, _>(6).unwrap_or(0) == 1,
                ordinal_pos: row.get::<i32, _>(7).unwrap_or(0),
            };
            table.columns.push(col);
        }

        debug!("Loaded {} columns for {}", table.columns.len(), table.full_name());
        Ok(())
    }

    /// Load primary key for a table.
    async fn load_primary_key(&self, client: &mut Client<Compat<TcpStream>>, table: &mut Table) -> Result<()> {
        let query = r#"
            SELECT c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
                ON c.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                AND c.TABLE_SCHEMA = tc.TABLE_SCHEMA
                AND c.TABLE_NAME = tc.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
              AND tc.TABLE_SCHEMA = @P1
              AND tc.TABLE_NAME = @P2
            ORDER BY c.ORDINAL_POSITION
        "#;

        let mut query = Query::new(query);
        query.bind(&table.schema);
        query.bind(&table.name);

        let stream = query.query(client).await.map_err(|e| MigrateError::Source(e))?;
        let rows = stream.into_first_result().await.map_err(|e| MigrateError::Source(e))?;

        for row in rows {
            let col_name: &str = row.get(0).unwrap_or_default();
            table.primary_key.push(col_name.to_string());
        }

        // Populate pk_columns with full metadata
        for pk_col in &table.primary_key {
            if let Some(col) = table.columns.iter().find(|c| &c.name == pk_col) {
                table.pk_columns.push(col.clone());
            }
        }

        debug!("Primary key for {}: {:?}", table.full_name(), table.primary_key);
        Ok(())
    }

    /// Query rows from the database and convert to SqlValue.
    pub async fn query_rows(
        &self,
        sql: &str,
        columns: &[String],
        table: &Table,
    ) -> Result<Vec<Vec<SqlValue>>> {
        let mut client = self.get_client().await?;

        let stream = client.simple_query(sql).await.map_err(|e| MigrateError::Source(e))?;
        let rows = stream.into_first_result().await.map_err(|e| MigrateError::Source(e))?;

        let mut result = Vec::with_capacity(rows.len());

        for row in rows {
            let mut values = Vec::with_capacity(columns.len());

            for (idx, col_name) in columns.iter().enumerate() {
                // Find column metadata
                let col_meta = table.columns.iter().find(|c| &c.name == col_name);
                let data_type = col_meta.map(|c| c.data_type.as_str()).unwrap_or("varchar");

                let value = convert_row_value(&row, idx, data_type);
                values.push(value);
            }

            result.push(values);
        }

        Ok(result)
    }

    /// Load row count for a table (fast approximate from sys.partitions).
    async fn load_row_count(&self, client: &mut Client<Compat<TcpStream>>, table: &mut Table) -> Result<()> {
        let query = r#"
            SELECT SUM(p.rows)
            FROM sys.partitions p
            JOIN sys.tables t ON p.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @P1 AND t.name = @P2 AND p.index_id IN (0, 1)
        "#;

        let mut query = Query::new(query);
        query.bind(&table.schema);
        query.bind(&table.name);

        let stream = query.query(client).await.map_err(|e| MigrateError::Source(e))?;
        let row = stream.into_row().await.map_err(|e| MigrateError::Source(e))?;

        if let Some(row) = row {
            table.row_count = row.get::<i64, _>(0).unwrap_or(0);
        }

        debug!("Row count for {}: {}", table.full_name(), table.row_count);
        Ok(())
    }
}

#[async_trait]
impl SourcePool for MssqlPool {
    async fn extract_schema(&self, schema: &str) -> Result<Vec<Table>> {
        let mut client = self.get_client().await?;

        let query = r#"
            SELECT
                t.TABLE_SCHEMA,
                t.TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES t
            WHERE t.TABLE_TYPE = 'BASE TABLE'
              AND t.TABLE_SCHEMA = @P1
            ORDER BY t.TABLE_NAME
        "#;

        let mut q = Query::new(query);
        q.bind(schema);

        let stream = q.query(&mut client).await.map_err(|e| MigrateError::Source(e))?;
        let rows = stream.into_first_result().await.map_err(|e| MigrateError::Source(e))?;

        let mut tables = Vec::new();
        for row in rows {
            let mut table = Table {
                schema: row.get::<&str, _>(0).unwrap_or_default().to_string(),
                name: row.get::<&str, _>(1).unwrap_or_default().to_string(),
                columns: Vec::new(),
                primary_key: Vec::new(),
                pk_columns: Vec::new(),
                row_count: 0,
                estimated_row_size: 0,
                indexes: Vec::new(),
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
            };

            // Load metadata
            self.load_columns(&mut client, &mut table).await?;
            self.load_primary_key(&mut client, &mut table).await?;
            self.load_row_count(&mut client, &mut table).await?;

            // Estimate row size (simple heuristic: sum of column sizes)
            table.estimated_row_size = table.columns.iter().map(|c| {
                match c.data_type.as_str() {
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
                        if c.max_length == -1 { 100 } else { c.max_length.min(100) as i64 }
                    }
                    _ => 8, // default
                }
            }).sum();

            tables.push(table);
        }

        info!("Extracted {} tables from schema '{}'", tables.len(), schema);
        Ok(tables)
    }

    async fn get_partition_boundaries(
        &self,
        table: &Table,
        num_partitions: usize,
    ) -> Result<Vec<Partition>> {
        if !table.has_single_pk() {
            return Err(MigrateError::SchemaExtraction(
                "Partitioning requires single-column PK".into(),
            ));
        }

        let pk_col = &table.primary_key[0];
        let mut client = self.get_client().await?;

        // Use NTILE to split into partitions - CAST all to BIGINT for consistent types
        let query = format!(
            r#"
            WITH numbered AS (
                SELECT CAST([{pk}] AS BIGINT) AS pk_val,
                       NTILE({n}) OVER (ORDER BY [{pk}]) as partition_id
                FROM [{schema}].[{table}]
            )
            SELECT CAST(partition_id AS BIGINT) AS partition_id,
                   MIN(pk_val) as min_pk,
                   MAX(pk_val) as max_pk,
                   CAST(COUNT(*) AS BIGINT) as row_count
            FROM numbered
            GROUP BY partition_id
            ORDER BY partition_id
            "#,
            pk = pk_col,
            n = num_partitions,
            schema = table.schema,
            table = table.name
        );

        let stream = client.simple_query(&query).await.map_err(|e| MigrateError::Source(e))?;
        let rows = stream.into_first_result().await.map_err(|e| MigrateError::Source(e))?;

        let mut partitions = Vec::new();
        for row in rows {
            // All values are now BIGINT
            let partition_id = row.get::<i64, _>(0).unwrap_or(0) as i32;
            let min_pk = row.get::<i64, _>(1);
            let max_pk = row.get::<i64, _>(2);
            let row_count = row.get::<i64, _>(3).unwrap_or(0);

            let partition = Partition {
                table_name: table.full_name(),
                partition_id,
                min_pk,
                max_pk,
                start_row: 0,
                end_row: 0,
                row_count,
            };
            partitions.push(partition);
        }

        debug!(
            "Created {} partitions for {}",
            partitions.len(),
            table.full_name()
        );
        Ok(partitions)
    }

    async fn load_indexes(&self, table: &mut Table) -> Result<()> {
        let mut client = self.get_client().await?;

        // Use subquery approach for ordered string aggregation
        let query = format!(r#"
            SELECT
                i.name AS index_name,
                i.is_unique,
                i.type_desc,
                STUFF((
                    SELECT ',' + c2.name
                    FROM sys.index_columns ic2
                    JOIN sys.columns c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id
                    WHERE ic2.object_id = i.object_id AND ic2.index_id = i.index_id AND ic2.is_included_column = 0
                    ORDER BY ic2.key_ordinal
                    FOR XML PATH('')
                ), 1, 1, '') AS columns,
                ISNULL(STUFF((
                    SELECT ',' + c2.name
                    FROM sys.index_columns ic2
                    JOIN sys.columns c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id
                    WHERE ic2.object_id = i.object_id AND ic2.index_id = i.index_id AND ic2.is_included_column = 1
                    ORDER BY ic2.key_ordinal
                    FOR XML PATH('')
                ), 1, 1, ''), '') AS include_columns
            FROM sys.indexes i
            JOIN sys.tables tb ON i.object_id = tb.object_id
            JOIN sys.schemas s ON tb.schema_id = s.schema_id
            WHERE s.name = @P1
              AND tb.name = @P2
              AND i.is_primary_key = 0
              AND i.type > 0
            ORDER BY i.name
        "#);

        let mut q = Query::new(&query);
        q.bind(&table.schema);
        q.bind(&table.name);

        let stream = q.query(&mut client).await.map_err(|e| MigrateError::Source(e))?;
        let rows = stream.into_first_result().await.map_err(|e| MigrateError::Source(e))?;

        for row in rows {
            let cols_str: &str = row.get(3).unwrap_or_default();
            let include_str: &str = row.get(4).unwrap_or_default();
            let type_desc: &str = row.get(2).unwrap_or_default();

            let index = Index {
                name: row.get::<&str, _>(0).unwrap_or_default().to_string(),
                is_unique: row.get::<bool, _>(1).unwrap_or(false),
                is_clustered: type_desc == "CLUSTERED",
                columns: cols_str.split(',').filter(|s| !s.is_empty()).map(String::from).collect(),
                include_cols: include_str.split(',').filter(|s| !s.is_empty()).map(String::from).collect(),
            };
            table.indexes.push(index);
        }

        debug!("Loaded {} indexes for {}", table.indexes.len(), table.full_name());
        Ok(())
    }

    async fn load_foreign_keys(&self, table: &mut Table) -> Result<()> {
        let mut client = self.get_client().await?;

        // Use STUFF/FOR XML PATH for ordered string aggregation
        let query = format!(r#"
            SELECT
                fk.name AS fk_name,
                STUFF((
                    SELECT ',' + pc2.name
                    FROM sys.foreign_key_columns fkc2
                    JOIN sys.columns pc2 ON fkc2.parent_object_id = pc2.object_id AND fkc2.parent_column_id = pc2.column_id
                    WHERE fkc2.constraint_object_id = fk.object_id
                    ORDER BY fkc2.constraint_column_id
                    FOR XML PATH('')
                ), 1, 1, '') AS parent_columns,
                rs.name AS ref_schema,
                rt.name AS ref_table,
                STUFF((
                    SELECT ',' + rc2.name
                    FROM sys.foreign_key_columns fkc2
                    JOIN sys.columns rc2 ON fkc2.referenced_object_id = rc2.object_id AND fkc2.referenced_column_id = rc2.column_id
                    WHERE fkc2.constraint_object_id = fk.object_id
                    ORDER BY fkc2.constraint_column_id
                    FOR XML PATH('')
                ), 1, 1, '') AS ref_columns,
                fk.delete_referential_action_desc,
                fk.update_referential_action_desc
            FROM sys.foreign_keys fk
            JOIN sys.tables pt ON fk.parent_object_id = pt.object_id
            JOIN sys.schemas ps ON pt.schema_id = ps.schema_id
            JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
            JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
            WHERE ps.name = @P1 AND pt.name = @P2
            ORDER BY fk.name
        "#);

        let mut q = Query::new(&query);
        q.bind(&table.schema);
        q.bind(&table.name);

        let stream = q.query(&mut client).await.map_err(|e| MigrateError::Source(e))?;
        let rows = stream.into_first_result().await.map_err(|e| MigrateError::Source(e))?;

        for row in rows {
            let cols_str: &str = row.get(1).unwrap_or_default();
            let ref_cols_str: &str = row.get(4).unwrap_or_default();

            let fk = ForeignKey {
                name: row.get::<&str, _>(0).unwrap_or_default().to_string(),
                columns: cols_str.split(',').filter(|s| !s.is_empty()).map(String::from).collect(),
                ref_schema: row.get::<&str, _>(2).unwrap_or_default().to_string(),
                ref_table: row.get::<&str, _>(3).unwrap_or_default().to_string(),
                ref_columns: ref_cols_str.split(',').filter(|s| !s.is_empty()).map(String::from).collect(),
                on_delete: row.get::<&str, _>(5).unwrap_or_default().to_string(),
                on_update: row.get::<&str, _>(6).unwrap_or_default().to_string(),
            };
            table.foreign_keys.push(fk);
        }

        debug!("Loaded {} foreign keys for {}", table.foreign_keys.len(), table.full_name());
        Ok(())
    }

    async fn load_check_constraints(&self, table: &mut Table) -> Result<()> {
        let mut client = self.get_client().await?;

        let query = r#"
            SELECT
                cc.name,
                cc.definition
            FROM sys.check_constraints cc
            JOIN sys.tables tb ON cc.parent_object_id = tb.object_id
            JOIN sys.schemas s ON tb.schema_id = s.schema_id
            WHERE s.name = @P1 AND tb.name = @P2
              AND cc.is_disabled = 0
            ORDER BY cc.name
        "#;

        let mut query = Query::new(query);
        query.bind(&table.schema);
        query.bind(&table.name);

        let stream = query.query(&mut client).await.map_err(|e| MigrateError::Source(e))?;
        let rows = stream.into_first_result().await.map_err(|e| MigrateError::Source(e))?;

        for row in rows {
            let constraint = CheckConstraint {
                name: row.get::<&str, _>(0).unwrap_or_default().to_string(),
                definition: row.get::<&str, _>(1).unwrap_or_default().to_string(),
            };
            table.check_constraints.push(constraint);
        }

        debug!(
            "Loaded {} check constraints for {}",
            table.check_constraints.len(),
            table.full_name()
        );
        Ok(())
    }

    async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        let mut client = self.get_client().await?;

        // CAST to BIGINT for consistent type handling
        let query = format!(
            "SELECT CAST(COUNT(*) AS BIGINT) FROM [{}].[{}]",
            schema, table
        );

        let stream = client.simple_query(&query).await.map_err(|e| MigrateError::Source(e))?;
        let row = stream.into_row().await.map_err(|e| MigrateError::Source(e))?;

        // SQL Server COUNT(*) returns int (i32), so get as i32 and convert to i64
        Ok(row
            .and_then(|r| r.get::<i32, _>(0))
            .map(|v| v as i64)
            .unwrap_or(0))
    }

    fn db_type(&self) -> &str {
        "mssql"
    }

    async fn close(&self) {
        // tiberius doesn't have explicit connection pooling,
        // each client is a single connection that drops when done
    }
}

/// Convert a row value to SqlValue based on the column type.
fn convert_row_value(row: &Row, idx: usize, data_type: &str) -> SqlValue {
    let dt = data_type.to_lowercase();

    match dt.as_str() {
        "bit" => {
            row.get::<bool, _>(idx)
                .map(SqlValue::Bool)
                .unwrap_or(SqlValue::Null(SqlNullType::Bool))
        }
        "tinyint" => {
            row.get::<u8, _>(idx)
                .map(|v| SqlValue::I16(v as i16))
                .unwrap_or(SqlValue::Null(SqlNullType::I16))
        }
        "smallint" => {
            row.get::<i16, _>(idx)
                .map(SqlValue::I16)
                .unwrap_or(SqlValue::Null(SqlNullType::I16))
        }
        "int" => {
            row.get::<i32, _>(idx)
                .map(SqlValue::I32)
                .unwrap_or(SqlValue::Null(SqlNullType::I32))
        }
        "bigint" => {
            row.get::<i64, _>(idx)
                .map(SqlValue::I64)
                .unwrap_or(SqlValue::Null(SqlNullType::I64))
        }
        "real" => {
            row.get::<f32, _>(idx)
                .map(SqlValue::F32)
                .unwrap_or(SqlValue::Null(SqlNullType::F32))
        }
        "float" => {
            row.get::<f64, _>(idx)
                .map(SqlValue::F64)
                .unwrap_or(SqlValue::Null(SqlNullType::F64))
        }
        "uniqueidentifier" => {
            row.get::<Uuid, _>(idx)
                .map(SqlValue::Uuid)
                .unwrap_or(SqlValue::Null(SqlNullType::Uuid))
        }
        "datetime" | "datetime2" | "smalldatetime" => {
            row.get::<NaiveDateTime, _>(idx)
                .map(SqlValue::DateTime)
                .unwrap_or(SqlValue::Null(SqlNullType::DateTime))
        }
        "date" => {
            // Tiberius returns date as NaiveDateTime, extract just the date part
            row.get::<NaiveDateTime, _>(idx)
                .map(|dt| SqlValue::Date(dt.date()))
                .unwrap_or(SqlValue::Null(SqlNullType::Date))
        }
        "time" => {
            // Tiberius returns time as NaiveDateTime, extract just the time part
            row.get::<NaiveDateTime, _>(idx)
                .map(|dt| SqlValue::Time(dt.time()))
                .unwrap_or(SqlValue::Null(SqlNullType::Time))
        }
        "binary" | "varbinary" | "image" => {
            row.get::<&[u8], _>(idx)
                .map(|v| SqlValue::Bytes(v.to_vec()))
                .unwrap_or(SqlValue::Null(SqlNullType::Bytes))
        }
        "decimal" | "numeric" | "money" | "smallmoney" => {
            // For decimal/numeric, try to get as string and parse
            // This is more reliable than trying to convert tiberius Numeric directly
            row.get::<&str, _>(idx)
                .and_then(|s| s.parse::<rust_decimal::Decimal>().ok())
                .map(SqlValue::Decimal)
                .or_else(|| {
                    // Fallback: try as f64
                    row.get::<f64, _>(idx).map(|f| {
                        rust_decimal::Decimal::try_from(f)
                            .map(SqlValue::Decimal)
                            .unwrap_or(SqlValue::F64(f))
                    })
                })
                .unwrap_or(SqlValue::Null(SqlNullType::Decimal))
        }
        _ => {
            // Default: treat as string (covers varchar, nvarchar, char, nchar, text, ntext, xml, etc.)
            row.get::<&str, _>(idx)
                .map(|s| SqlValue::String(s.to_string()))
                .unwrap_or(SqlValue::Null(SqlNullType::String))
        }
    }
}
