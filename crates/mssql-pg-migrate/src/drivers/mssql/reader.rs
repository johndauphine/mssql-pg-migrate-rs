//! MSSQL source reader implementation.
//!
//! Implements the `SourceReader` trait for reading data from MSSQL databases.
//! Uses Tiberius with bb8 connection pooling for high-performance data extraction.

use std::time::Duration;

use async_trait::async_trait;
use bb8::{Pool, PooledConnection};
use chrono::NaiveDateTime;
use tiberius::{AuthMethod as TiberiusAuthMethod, Client, Config, EncryptionLevel, Query, Row};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::{debug, info, warn};
use uuid::Uuid;

#[cfg(feature = "kerberos")]
use crate::config::AuthMethod as ConfigAuthMethod;
use crate::config::SourceConfig;
use crate::core::schema::{CheckConstraint, Column, ForeignKey, Index, Partition, Table};
use crate::core::traits::{ReadOptions, SourceReader};
use crate::core::value::{Batch, SqlNullType, SqlValue};
use crate::error::{MigrateError, Result};
use crate::source::DirectCopyEncoder;

/// Maximum TDS packet size (32767 bytes, ~32KB).
const TDS_MAX_PACKET_SIZE: u32 = 32767;

/// Connection acquisition timeout from pool (30 seconds).
const POOL_CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// Idle connection timeout (5 minutes).
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// Maximum connection lifetime (30 minutes).
const POOL_MAX_LIFETIME: Duration = Duration::from_secs(1800);

/// TCP keepalive interval (30 seconds).
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// Connection manager for bb8 pool with Tiberius.
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

        // Set authentication method based on config
        match self.config.auth {
            #[cfg(feature = "kerberos")]
            ConfigAuthMethod::Kerberos => {
                info!("Using Kerberos authentication via Tiberius GSSAPI for MSSQL source");
                config.authentication(TiberiusAuthMethod::Integrated);
            }
            _ => {
                config.authentication(TiberiusAuthMethod::sql_server(
                    &self.config.user,
                    &self.config.password,
                ));
            }
        }

        // Encryption settings
        if self.config.encrypt {
            if self.config.trust_server_cert {
                config.trust_cert();
            }
            config.encryption(EncryptionLevel::Required);
        } else {
            config.encryption(EncryptionLevel::NotSupported);
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

        // Enable TCP keepalives
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

/// MSSQL source reader implementation.
///
/// Provides high-performance data extraction from MSSQL databases using:
/// - Connection pooling via bb8
/// - Direct COPY encoding for PostgreSQL targets
/// - Keyset pagination for parallel reads
pub struct MssqlReader {
    pool: Pool<TiberiusConnectionManager>,
}

impl MssqlReader {
    /// Create a new MSSQL reader from configuration.
    pub async fn new(config: SourceConfig) -> Result<Self> {
        Self::with_pool_size(config, 8).await
    }

    /// Create a new MSSQL reader with specified pool size.
    pub async fn with_pool_size(config: SourceConfig, max_size: u32) -> Result<Self> {
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
            .map_err(|e| MigrateError::pool(e, "creating MSSQL connection pool"))?;

        // Test connection
        {
            let mut conn = pool
                .get()
                .await
                .map_err(|e| MigrateError::pool(e, "testing MSSQL connection"))?;
            conn.simple_query("SELECT 1").await?.into_row().await?;
        }

        info!(
            "Connected to MSSQL: {}:{}/{} (pool_size={})",
            config.host, config.port, config.database, max_size
        );

        Ok(Self { pool })
    }

    /// Get a pooled connection.
    async fn get_client(&self) -> Result<PooledConnection<'_, TiberiusConnectionManager>> {
        self.pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting MSSQL connection from pool"))
    }

    /// Load columns for a table.
    async fn load_columns(
        &self,
        client: &mut Client<Compat<TcpStream>>,
        table: &mut Table,
    ) -> Result<()> {
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

        let stream = query.query(client).await?;
        let rows = stream.into_first_result().await?;

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

        debug!(
            "Loaded {} columns for {}",
            table.columns.len(),
            table.full_name()
        );
        Ok(())
    }

    /// Load primary key for a table.
    async fn load_primary_key(
        &self,
        client: &mut Client<Compat<TcpStream>>,
        table: &mut Table,
    ) -> Result<()> {
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

        let stream = query.query(client).await?;
        let rows = stream.into_first_result().await?;

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

        debug!(
            "Primary key for {}: {:?}",
            table.full_name(),
            table.primary_key
        );
        Ok(())
    }

    /// Load row count for a table (fast approximate from sys.partitions).
    async fn load_row_count(
        &self,
        client: &mut Client<Compat<TcpStream>>,
        table: &mut Table,
    ) -> Result<()> {
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

        let stream = query.query(client).await?;
        let row = stream.into_row().await?;

        if let Some(row) = row {
            table.row_count = row.get::<i64, _>(0).unwrap_or(0);
        }

        debug!("Row count for {}: {}", table.full_name(), table.row_count);
        Ok(())
    }

    /// Query rows with pre-computed column types for O(1) lookup.
    pub async fn query_rows_fast(
        &self,
        sql: &str,
        col_types: &[String],
    ) -> Result<Vec<Vec<SqlValue<'static>>>> {
        let mut client = self.get_client().await?;
        let stream = client.simple_query(sql).await?;
        let rows = stream.into_first_result().await?;

        let mut result = Vec::with_capacity(rows.len());

        for row in rows {
            let mut values = Vec::with_capacity(col_types.len());

            for (idx, data_type) in col_types.iter().enumerate() {
                let value = convert_row_value(&row, idx, data_type);
                values.push(value);
            }

            result.push(values);
        }

        Ok(result)
    }

    /// Query rows and encode directly to PostgreSQL COPY binary format.
    ///
    /// This is the high-performance path that bypasses SqlValue entirely.
    /// Returns (encoded_bytes, first_pk, last_pk, row_count).
    pub async fn query_rows_direct_copy(
        &self,
        sql: &str,
        col_types: &[String],
        pk_idx: Option<usize>,
    ) -> Result<(bytes::Bytes, Option<i64>, Option<i64>, usize)> {
        use bytes::BytesMut;

        let mut client = self.get_client().await?;
        let stream = client.simple_query(sql).await?;
        let rows = stream.into_first_result().await?;

        let encoder = DirectCopyEncoder::new(col_types);
        let estimated_size = encoder.buffer_size_for_rows(rows.len());
        let mut buf = BytesMut::with_capacity(estimated_size);

        encoder.write_header(&mut buf);

        let mut first_pk: Option<i64> = None;
        let mut last_pk: Option<i64> = None;
        let row_count = rows.len();

        for (i, row) in rows.iter().enumerate() {
            let pk = encoder.encode_row(row, &mut buf, pk_idx);

            if pk_idx.is_some() {
                if i == 0 {
                    first_pk = pk;
                }
                if pk.is_some() {
                    last_pk = pk;
                }
            }
        }

        encoder.write_trailer(&mut buf);

        Ok((buf.freeze(), first_pk, last_pk, row_count))
    }

    /// Test the connection.
    pub async fn test_connection(&self) -> Result<()> {
        let mut client = self.get_client().await?;
        client.simple_query("SELECT 1").await?.into_row().await?;
        Ok(())
    }
}

#[async_trait]
impl SourceReader for MssqlReader {
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

        let stream = q.query(&mut client).await?;
        let rows = stream.into_first_result().await?;

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

            self.load_columns(&mut client, &mut table).await?;
            self.load_primary_key(&mut client, &mut table).await?;
            self.load_row_count(&mut client, &mut table).await?;

            // Estimate row size
            table.estimated_row_size = table.columns.iter().map(|c| estimate_column_size(c)).sum();

            tables.push(table);
        }

        info!("Extracted {} tables from schema '{}'", tables.len(), schema);
        Ok(tables)
    }

    async fn load_indexes(&self, table: &mut Table) -> Result<()> {
        let mut client = self.get_client().await?;

        let query = r#"
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
        "#;

        let mut q = Query::new(query);
        q.bind(&table.schema);
        q.bind(&table.name);

        let stream = q.query(&mut client).await?;
        let rows = stream.into_first_result().await?;

        for row in rows {
            let cols_str: &str = row.get(3).unwrap_or_default();
            let include_str: &str = row.get(4).unwrap_or_default();
            let type_desc: &str = row.get(2).unwrap_or_default();

            let index = Index {
                name: row.get::<&str, _>(0).unwrap_or_default().to_string(),
                is_unique: row.get::<bool, _>(1).unwrap_or(false),
                is_clustered: type_desc == "CLUSTERED",
                columns: cols_str
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect(),
                include_cols: include_str
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect(),
            };
            table.indexes.push(index);
        }

        debug!(
            "Loaded {} indexes for {}",
            table.indexes.len(),
            table.full_name()
        );
        Ok(())
    }

    async fn load_foreign_keys(&self, table: &mut Table) -> Result<()> {
        let mut client = self.get_client().await?;

        let query = r#"
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
        "#;

        let mut q = Query::new(query);
        q.bind(&table.schema);
        q.bind(&table.name);

        let stream = q.query(&mut client).await?;
        let rows = stream.into_first_result().await?;

        for row in rows {
            let cols_str: &str = row.get(1).unwrap_or_default();
            let ref_cols_str: &str = row.get(4).unwrap_or_default();

            let fk = ForeignKey {
                name: row.get::<&str, _>(0).unwrap_or_default().to_string(),
                columns: cols_str
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect(),
                ref_schema: row.get::<&str, _>(2).unwrap_or_default().to_string(),
                ref_table: row.get::<&str, _>(3).unwrap_or_default().to_string(),
                ref_columns: ref_cols_str
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect(),
                on_delete: row.get::<&str, _>(5).unwrap_or_default().to_string(),
                on_update: row.get::<&str, _>(6).unwrap_or_default().to_string(),
            };
            table.foreign_keys.push(fk);
        }

        debug!(
            "Loaded {} foreign keys for {}",
            table.foreign_keys.len(),
            table.full_name()
        );
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

        let stream = query.query(&mut client).await?;
        let rows = stream.into_first_result().await?;

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

    fn read_table(&self, opts: ReadOptions) -> mpsc::Receiver<Result<Batch>> {
        let (tx, rx) = mpsc::channel(16);
        let pool = self.pool.clone();
        let opts = opts.clone();

        tokio::spawn(async move {
            if let Err(e) = read_table_internal(pool, opts, tx.clone()).await {
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
        if !table.has_single_pk() {
            return Err(MigrateError::SchemaExtraction(
                "Partitioning requires single-column PK".into(),
            ));
        }

        let pk_col = &table.primary_key[0];
        let mut client = self.get_client().await?;

        let query = format!(
            r#"
            WITH numbered AS (
                SELECT CAST([{pk}] AS BIGINT) AS pk_val,
                       NTILE({n}) OVER (ORDER BY [{pk}]) as partition_id
                FROM [{schema}].[{table}] WITH (NOLOCK)
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

        let stream = client.simple_query(&query).await?;
        let rows = stream.into_first_result().await?;

        let mut partitions = Vec::new();
        for row in rows {
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

    async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        let mut client = self.get_client().await?;

        let query = format!(
            "SELECT CAST(COUNT(*) AS BIGINT) FROM [{}].[{}] WITH (NOLOCK)",
            schema, table
        );

        let stream = client.simple_query(&query).await?;
        let row = stream.into_row().await?;

        Ok(row.and_then(|r| r.get::<i64, _>(0)).unwrap_or(0))
    }

    async fn get_max_pk(&self, schema: &str, table: &str, pk_col: &str) -> Result<i64> {
        let mut client = self.get_client().await?;

        let query = format!(
            "SELECT CAST(MAX([{}]) AS BIGINT) FROM [{}].[{}] WITH (NOLOCK)",
            pk_col, schema, table
        );

        let stream = client.simple_query(&query).await?;
        let row = stream.into_row().await?;

        Ok(row.and_then(|r| r.get::<i64, _>(0)).unwrap_or(0))
    }

    fn db_type(&self) -> &str {
        "mssql"
    }

    async fn close(&self) {
        // bb8 pool handles cleanup automatically
    }

    fn supports_direct_copy(&self) -> bool {
        true
    }
}

/// Internal function to read table rows and send batches through channel.
async fn read_table_internal(
    pool: Pool<TiberiusConnectionManager>,
    opts: ReadOptions,
    tx: mpsc::Sender<Result<Batch>>,
) -> Result<()> {
    let mut client = pool
        .get()
        .await
        .map_err(|e| MigrateError::pool(e, "getting connection for read_table"))?;

    // Build SELECT query
    let cols = opts
        .columns
        .iter()
        .map(|c| format!("[{}]", c.replace(']', "]]")))
        .collect::<Vec<_>>()
        .join(", ");

    let mut sql = format!(
        "SELECT TOP {} {} FROM [{}].[{}] WITH (NOLOCK)",
        opts.batch_size, cols, opts.schema, opts.table
    );

    let mut conditions = Vec::new();

    if let (Some(pk_idx), Some(min_pk)) = (opts.pk_idx, opts.min_pk) {
        let pk_col = &opts.columns[pk_idx];
        conditions.push(format!("[{}] > {}", pk_col, min_pk));
    }

    if let (Some(pk_idx), Some(max_pk)) = (opts.pk_idx, opts.max_pk) {
        let pk_col = &opts.columns[pk_idx];
        conditions.push(format!("[{}] <= {}", pk_col, max_pk));
    }

    if let Some(ref where_clause) = opts.where_clause {
        if !where_clause.is_empty() {
            conditions.push(format!("({})", where_clause));
        }
    }

    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }

    if let Some(pk_idx) = opts.pk_idx {
        let pk_col = &opts.columns[pk_idx];
        sql.push_str(&format!(" ORDER BY [{}]", pk_col));
    }

    // Execute query in batches using keyset pagination
    let mut last_pk: Option<i64> = opts.min_pk;

    loop {
        let batch_sql = if let Some(lpk) = last_pk {
            if let Some(pk_idx) = opts.pk_idx {
                let pk_col = &opts.columns[pk_idx];
                sql.replace(
                    &format!("[{}] > {}", pk_col, opts.min_pk.unwrap_or(0)),
                    &format!("[{}] > {}", pk_col, lpk),
                )
            } else {
                sql.clone()
            }
        } else {
            sql.clone()
        };

        let stream = client.simple_query(&batch_sql).await?;
        let rows = stream.into_first_result().await?;

        if rows.is_empty() {
            // Send final empty batch to signal completion
            let _ = tx.send(Ok(Batch::empty_final())).await;
            break;
        }

        let mut batch_rows = Vec::with_capacity(rows.len());
        let mut batch_last_pk: Option<i64> = None;

        for row in rows {
            let mut values = Vec::with_capacity(opts.col_types.len());
            for (idx, data_type) in opts.col_types.iter().enumerate() {
                let value = convert_row_value(&row, idx, data_type);

                // Track last PK
                if Some(idx) == opts.pk_idx {
                    if let SqlValue::I32(v) = &value {
                        batch_last_pk = Some(*v as i64);
                    } else if let SqlValue::I64(v) = &value {
                        batch_last_pk = Some(*v);
                    }
                }

                values.push(value);
            }
            batch_rows.push(values);
        }

        last_pk = batch_last_pk;

        let is_last = batch_rows.len() < opts.batch_size;
        let mut batch = Batch::new(batch_rows);

        if let Some(lpk) = batch_last_pk {
            batch = batch.with_last_key(SqlValue::I64(lpk));
        }
        if is_last {
            batch = batch.mark_final();
        }

        if tx.send(Ok(batch)).await.is_err() {
            break; // Channel closed
        }

        if is_last {
            break;
        }
    }

    Ok(())
}

/// Convert a row value to SqlValue based on the column type.
fn convert_row_value(row: &Row, idx: usize, data_type: &str) -> SqlValue<'static> {
    use std::borrow::Cow;

    let dt = data_type.to_lowercase();

    match dt.as_str() {
        "bit" => row
            .get::<bool, _>(idx)
            .map(SqlValue::Bool)
            .unwrap_or(SqlValue::Null(SqlNullType::Bool)),
        "tinyint" => row
            .get::<u8, _>(idx)
            .map(|v| SqlValue::I16(v as i16))
            .unwrap_or(SqlValue::Null(SqlNullType::I16)),
        "smallint" => row
            .get::<i16, _>(idx)
            .map(SqlValue::I16)
            .unwrap_or(SqlValue::Null(SqlNullType::I16)),
        "int" => row
            .get::<i32, _>(idx)
            .map(SqlValue::I32)
            .unwrap_or(SqlValue::Null(SqlNullType::I32)),
        "bigint" => row
            .get::<i64, _>(idx)
            .map(SqlValue::I64)
            .unwrap_or(SqlValue::Null(SqlNullType::I64)),
        "real" => row
            .get::<f32, _>(idx)
            .map(SqlValue::F32)
            .unwrap_or(SqlValue::Null(SqlNullType::F32)),
        "float" => row
            .get::<f64, _>(idx)
            .map(SqlValue::F64)
            .unwrap_or(SqlValue::Null(SqlNullType::F64)),
        "uniqueidentifier" => row
            .get::<Uuid, _>(idx)
            .map(SqlValue::Uuid)
            .unwrap_or(SqlValue::Null(SqlNullType::Uuid)),
        "datetime" | "datetime2" | "smalldatetime" => row
            .get::<NaiveDateTime, _>(idx)
            .map(SqlValue::DateTime)
            .unwrap_or(SqlValue::Null(SqlNullType::DateTime)),
        "date" => row
            .get::<NaiveDateTime, _>(idx)
            .map(|dt| SqlValue::Date(dt.date()))
            .unwrap_or(SqlValue::Null(SqlNullType::Date)),
        "time" => row
            .get::<NaiveDateTime, _>(idx)
            .map(|dt| SqlValue::Time(dt.time()))
            .unwrap_or(SqlValue::Null(SqlNullType::Time)),
        "binary" | "varbinary" | "image" => row
            .get::<&[u8], _>(idx)
            .map(|v| SqlValue::Bytes(Cow::Owned(v.to_vec())))
            .unwrap_or(SqlValue::Null(SqlNullType::Bytes)),
        "decimal" | "numeric" | "money" | "smallmoney" => row
            .get::<rust_decimal::Decimal, _>(idx)
            .map(SqlValue::Decimal)
            .or_else(|| {
                row.get::<f64, _>(idx).map(|f| {
                    rust_decimal::Decimal::try_from(f)
                        .map(SqlValue::Decimal)
                        .unwrap_or(SqlValue::F64(f))
                })
            })
            .unwrap_or(SqlValue::Null(SqlNullType::Decimal)),
        _ => {
            // Default: treat as string
            row.get::<&str, _>(idx)
                .map(|s| SqlValue::Text(Cow::Owned(s.to_string())))
                .unwrap_or(SqlValue::Null(SqlNullType::String))
        }
    }
}

/// Estimate column size for row size estimation.
fn estimate_column_size(col: &Column) -> i64 {
    match col.data_type.as_str() {
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
            if col.max_length == -1 {
                100
            } else {
                col.max_length.min(100) as i64
            }
        }
        _ => 8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_column_size() {
        let col = Column {
            name: "id".to_string(),
            data_type: "int".to_string(),
            max_length: 0,
            precision: 10,
            scale: 0,
            is_nullable: false,
            is_identity: true,
            ordinal_pos: 1,
        };
        assert_eq!(estimate_column_size(&col), 4);

        let col = Column {
            name: "name".to_string(),
            data_type: "varchar".to_string(),
            max_length: 255,
            precision: 0,
            scale: 0,
            is_nullable: true,
            is_identity: false,
            ordinal_pos: 2,
        };
        assert_eq!(estimate_column_size(&col), 100);
    }
}
