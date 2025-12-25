//! PostgreSQL target database operations.

use crate::error::{MigrateError, Result};
use crate::source::{CheckConstraint, ForeignKey, Index, Table};
use crate::typemap::mssql_to_postgres;
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use chrono::Timelike;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use futures::{future::try_join_all, SinkExt};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_postgres::{types::ToSql, Config as PgConfig, NoTls};
use tracing::{debug, info, warn};

/// Target buffer size for COPY operations (~64KB like Go pgx).
/// This is the threshold at which we flush the buffer to reduce syscalls
/// while keeping memory bounded.
const COPY_SEND_BUF_SIZE: usize = 65536 - 5; // Account for packet header

/// Number of buffer chunks to queue for concurrent network I/O.
const COPY_CHANNEL_SIZE: usize = 4;

/// Trait for target database operations.
#[async_trait]
pub trait TargetPool: Send + Sync {
    /// Create a schema if it doesn't exist.
    async fn create_schema(&self, schema: &str) -> Result<()>;

    /// Create a table from metadata.
    async fn create_table(&self, table: &Table, target_schema: &str) -> Result<()>;

    /// Create a table with optional UNLOGGED.
    async fn create_table_unlogged(&self, table: &Table, target_schema: &str) -> Result<()>;

    /// Drop a table if it exists.
    async fn drop_table(&self, schema: &str, table: &str) -> Result<()>;

    /// Truncate a table.
    async fn truncate_table(&self, schema: &str, table: &str) -> Result<()>;

    /// Check if a table exists.
    async fn table_exists(&self, schema: &str, table: &str) -> Result<bool>;

    /// Create a primary key constraint.
    async fn create_primary_key(&self, table: &Table, target_schema: &str) -> Result<()>;

    /// Create an index.
    async fn create_index(&self, table: &Table, idx: &Index, target_schema: &str) -> Result<()>;

    /// Create a foreign key constraint.
    async fn create_foreign_key(
        &self,
        table: &Table,
        fk: &ForeignKey,
        target_schema: &str,
    ) -> Result<()>;

    /// Create a check constraint.
    async fn create_check_constraint(
        &self,
        table: &Table,
        chk: &CheckConstraint,
        target_schema: &str,
    ) -> Result<()>;

    /// Check if a table has a primary key.
    async fn has_primary_key(&self, schema: &str, table: &str) -> Result<bool>;

    /// Get the row count for a table.
    async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64>;

    /// Reset sequence to max value.
    async fn reset_sequence(&self, schema: &str, table: &Table) -> Result<()>;

    /// Set table to LOGGED mode.
    async fn set_table_logged(&self, schema: &str, table: &str) -> Result<()>;

    /// Set table to UNLOGGED mode.
    async fn set_table_unlogged(&self, schema: &str, table: &str) -> Result<()>;

    /// Write a chunk of rows using COPY protocol.
    async fn write_chunk(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        rows: Vec<Vec<SqlValue>>,
    ) -> Result<u64>;

    /// Upsert a chunk of rows.
    async fn upsert_chunk(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        pk_cols: &[String],
        rows: Vec<Vec<SqlValue>>,
    ) -> Result<u64>;

    /// Get the database type.
    fn db_type(&self) -> &str;

    /// Close all connections.
    async fn close(&self);
}

/// SQL value enum for type-safe row handling.
#[derive(Debug, Clone)]
pub enum SqlValue {
    Null(SqlNullType),
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
    Decimal(rust_decimal::Decimal),
    DateTime(chrono::NaiveDateTime),
    DateTimeOffset(chrono::DateTime<chrono::FixedOffset>),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
}

/// Type hint for NULL values to ensure correct PostgreSQL encoding.
#[derive(Debug, Clone, Copy)]
pub enum SqlNullType {
    Bool,
    I16,
    I32,
    I64,
    F32,
    F64,
    String,
    Bytes,
    Uuid,
    Decimal,
    DateTime,
    DateTimeOffset,
    Date,
    Time,
}

/// PostgreSQL target pool implementation.
pub struct PgPool {
    pool: Pool,
    /// Rows per COPY buffer flush.
    copy_buffer_rows: usize,
    /// Use binary COPY format.
    use_binary_copy: bool,
    /// Rows per upsert batch.
    upsert_batch_size: usize,
    /// Number of parallel upsert tasks.
    upsert_parallel_tasks: usize,
}

impl PgPool {
    /// Create a new PostgreSQL target pool.
    pub async fn new(
        config: &crate::config::TargetConfig,
        max_conns: usize,
        copy_buffer_rows: usize,
        use_binary_copy: bool,
        upsert_batch_size: usize,
        upsert_parallel_tasks: usize,
    ) -> Result<Self> {
        // Build tokio_postgres::Config
        let mut pg_config = PgConfig::new();
        pg_config.host(&config.host);
        pg_config.port(config.port);
        pg_config.dbname(&config.database);
        pg_config.user(&config.user);
        pg_config.password(&config.password);

        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };

        let mgr = Manager::from_config(pg_config, NoTls, mgr_config);
        let pool = Pool::builder(mgr)
            .max_size(max_conns)
            .build()
            .map_err(|e| MigrateError::pool(e.to_string(), "creating PostgreSQL connection pool"))?;

        // Test connection
        let client = pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e.to_string(), "testing PostgreSQL connection"))?;

        client
            .simple_query("SELECT 1")
            .await
            ?;

        info!(
            "Connected to PostgreSQL: {}:{}/{}",
            config.host, config.port, config.database
        );

        Ok(Self {
            pool,
            copy_buffer_rows,
            use_binary_copy,
            upsert_batch_size,
            upsert_parallel_tasks,
        })
    }

    /// Generate DDL for table creation.
    fn generate_ddl(&self, table: &Table, target_schema: &str, unlogged: bool) -> String {
        let unlogged_str = if unlogged { "UNLOGGED " } else { "" };

        let schema = Self::quote_ident(target_schema);
        let table_name = Self::quote_ident(&table.name);

        let mut ddl = format!("CREATE {}TABLE {}.{} (\n", unlogged_str, schema, table_name);

        for (i, col) in table.columns.iter().enumerate() {
            let pg_type =
                mssql_to_postgres(&col.data_type, col.max_length, col.precision, col.scale);

            let nullable = if col.is_nullable { "" } else { " NOT NULL" };

            let identity = if col.is_identity {
                " GENERATED BY DEFAULT AS IDENTITY"
            } else {
                ""
            };

            ddl.push_str(&format!(
                "    {} {}{}{}",
                Self::quote_ident(&col.name),
                pg_type,
                nullable,
                identity
            ));

            if i < table.columns.len() - 1 {
                ddl.push_str(",\n");
            } else {
                ddl.push('\n');
            }
        }

        ddl.push_str(")");
        ddl
    }

    /// Quote a PostgreSQL identifier.
    fn quote_ident(name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    /// Fully qualify a table name.
    fn qualify_table(schema: &str, table: &str) -> String {
        format!("{}.{}", Self::quote_ident(schema), Self::quote_ident(table))
    }

    /// Map SQL Server referential action to PostgreSQL.
    fn map_referential_action(action: &str) -> &str {
        match action.to_uppercase().as_str() {
            "CASCADE" => "CASCADE",
            "SET_NULL" => "SET NULL",
            "SET_DEFAULT" => "SET DEFAULT",
            "NO_ACTION" => "NO ACTION",
            _ => "NO ACTION",
        }
    }

    /// Convert SQL Server CHECK definition to PostgreSQL.
    fn convert_check_definition(def: &str) -> String {
        let mut result = def.to_string();

        // Replace [column] with "column"
        while let Some(start) = result.find('[') {
            if let Some(end) = result[start..].find(']') {
                let col_name = &result[start + 1..start + end];
                result = format!(
                    "{}\"{}\"{}",
                    &result[..start],
                    col_name,
                    &result[start + end + 1..]
                );
            } else {
                break;
            }
        }

        // Replace getdate() with CURRENT_TIMESTAMP
        result = result.replace("getdate()", "CURRENT_TIMESTAMP");
        result = result.replace("GETDATE()", "CURRENT_TIMESTAMP");

        result
    }
}

#[async_trait]
impl TargetPool for PgPool {
    async fn create_schema(&self, schema: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e.to_string(), "getting PostgreSQL connection"))?;

        let sql = format!("CREATE SCHEMA IF NOT EXISTS {}", Self::quote_ident(schema));
        client
            .execute(&sql, &[])
            .await
            ?;

        debug!("Created schema '{}'", schema);
        Ok(())
    }

    async fn create_table(&self, table: &Table, target_schema: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e.to_string(), "getting PostgreSQL connection"))?;

        let ddl = self.generate_ddl(table, target_schema, false);
        client
            .execute(&ddl, &[])
            .await
            ?;

        debug!("Created table {}.{}", target_schema, table.name);
        Ok(())
    }

    async fn create_table_unlogged(&self, table: &Table, target_schema: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e.to_string(), "getting PostgreSQL connection"))?;

        let ddl = self.generate_ddl(table, target_schema, true);
        client
            .execute(&ddl, &[])
            .await
            ?;

        debug!("Created UNLOGGED table {}.{}", target_schema, table.name);
        Ok(())
    }

    async fn drop_table(&self, schema: &str, table: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e.to_string(), "getting PostgreSQL connection"))?;

        let sql = format!(
            "DROP TABLE IF EXISTS {} CASCADE",
            Self::qualify_table(schema, table)
        );
        client
            .execute(&sql, &[])
            .await
            ?;

        debug!("Dropped table {}.{}", schema, table);
        Ok(())
    }

    async fn truncate_table(&self, schema: &str, table: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e.to_string(), "getting PostgreSQL connection"))?;

        let sql = format!("TRUNCATE TABLE {}", Self::qualify_table(schema, table));
        client
            .execute(&sql, &[])
            .await
            ?;

        debug!("Truncated table {}.{}", schema, table);
        Ok(())
    }

    async fn table_exists(&self, schema: &str, table: &str) -> Result<bool> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e.to_string(), "getting PostgreSQL connection"))?;

        let row = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2
                )",
                &[&schema, &table],
            )
            .await
            ?;

        Ok(row.get(0))
    }

    async fn create_primary_key(&self, table: &Table, target_schema: &str) -> Result<()> {
        if table.primary_key.is_empty() {
            return Ok(());
        }

        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e.to_string(), "getting PostgreSQL connection"))?;

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

        client
            .execute(&sql, &[])
            .await
            ?;

        debug!("Created primary key for {}.{}", target_schema, table.name);
        Ok(())
    }

    async fn create_index(&self, table: &Table, idx: &Index, target_schema: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e.to_string(), "getting PostgreSQL connection"))?;

        let cols: Vec<String> = idx.columns.iter().map(|c| Self::quote_ident(c)).collect();

        let unique = if idx.is_unique { "UNIQUE " } else { "" };

        let mut idx_name = format!("idx_{}_{}", table.name, idx.name);
        if idx_name.len() > 63 {
            idx_name.truncate(63);
        }

        let mut sql = format!(
            "CREATE {}INDEX IF NOT EXISTS {} ON {} ({})",
            unique,
            Self::quote_ident(&idx_name),
            Self::qualify_table(target_schema, &table.name),
            cols.join(", ")
        );

        if !idx.include_cols.is_empty() {
            let include_cols: Vec<String> = idx
                .include_cols
                .iter()
                .map(|c| Self::quote_ident(c))
                .collect();
            sql.push_str(&format!(" INCLUDE ({})", include_cols.join(", ")));
        }

        client
            .execute(&sql, &[])
            .await
            ?;

        debug!(
            "Created index {} for {}.{}",
            idx_name, target_schema, table.name
        );
        Ok(())
    }

    async fn create_foreign_key(
        &self,
        table: &Table,
        fk: &ForeignKey,
        target_schema: &str,
    ) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e.to_string(), "getting PostgreSQL connection"))?;

        let cols: Vec<String> = fk.columns.iter().map(|c| Self::quote_ident(c)).collect();
        let ref_cols: Vec<String> = fk
            .ref_columns
            .iter()
            .map(|c| Self::quote_ident(c))
            .collect();

        let on_delete = Self::map_referential_action(&fk.on_delete);
        let on_update = Self::map_referential_action(&fk.on_update);

        let mut fk_name = format!("fk_{}_{}", table.name, fk.name);
        if fk_name.len() > 63 {
            fk_name.truncate(63);
        }

        let sql = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({}) ON DELETE {} ON UPDATE {}",
            Self::qualify_table(target_schema, &table.name),
            Self::quote_ident(&fk_name),
            cols.join(", "),
            Self::qualify_table(target_schema, &fk.ref_table),
            ref_cols.join(", "),
            on_delete,
            on_update
        );

        client
            .execute(&sql, &[])
            .await
            ?;

        debug!(
            "Created foreign key {} for {}.{}",
            fk_name, target_schema, table.name
        );
        Ok(())
    }

    async fn create_check_constraint(
        &self,
        table: &Table,
        chk: &CheckConstraint,
        target_schema: &str,
    ) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e.to_string(), "getting PostgreSQL connection"))?;

        let definition = Self::convert_check_definition(&chk.definition);

        let mut chk_name = format!("chk_{}_{}", table.name, chk.name);
        if chk_name.len() > 63 {
            chk_name.truncate(63);
        }

        let sql = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} CHECK {}",
            Self::qualify_table(target_schema, &table.name),
            Self::quote_ident(&chk_name),
            definition
        );

        client
            .execute(&sql, &[])
            .await
            ?;

        debug!(
            "Created check constraint {} for {}.{}",
            chk_name, target_schema, table.name
        );
        Ok(())
    }

    async fn has_primary_key(&self, schema: &str, table: &str) -> Result<bool> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e.to_string(), "getting PostgreSQL connection"))?;

        let row = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM information_schema.table_constraints
                    WHERE constraint_type = 'PRIMARY KEY'
                    AND table_schema = $1
                    AND table_name = $2
                )",
                &[&schema, &table],
            )
            .await
            ?;

        Ok(row.get(0))
    }

    async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e.to_string(), "getting PostgreSQL connection"))?;

        let sql = format!(
            "SELECT COUNT(*) FROM {}",
            Self::qualify_table(schema, table)
        );
        let row = client
            .query_one(&sql, &[])
            .await
            ?;

        Ok(row.get(0))
    }

    async fn reset_sequence(&self, schema: &str, table: &Table) -> Result<()> {
        // Find identity column
        let identity_col = table.columns.iter().find(|c| c.is_identity);
        let identity_col = match identity_col {
            Some(col) => col,
            None => return Ok(()),
        };

        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e.to_string(), "getting PostgreSQL connection"))?;

        // Get max value (cast to bigint for consistent type)
        let sql = format!(
            "SELECT COALESCE(MAX({})::bigint, 0) FROM {}",
            Self::quote_ident(&identity_col.name),
            Self::qualify_table(schema, &table.name)
        );
        let row = client
            .query_one(&sql, &[])
            .await
            ?;
        let max_val: i64 = row.get(0);

        if max_val == 0 {
            return Ok(());
        }

        // Try ALTER TABLE ... RESTART WITH
        let sql = format!(
            "ALTER TABLE {} ALTER COLUMN {} RESTART WITH {}",
            Self::qualify_table(schema, &table.name),
            Self::quote_ident(&identity_col.name),
            max_val + 1
        );

        if client.execute(&sql, &[]).await.is_err() {
            // Fall back to setval for SERIAL columns
            let sql = format!(
                "SELECT setval(pg_get_serial_sequence('{}', '{}'), {})",
                Self::qualify_table(schema, &table.name),
                identity_col.name,
                max_val
            );
            client
                .execute(&sql, &[])
                .await
                ?;
        }

        debug!(
            "Reset sequence for {}.{}.{}",
            schema, table.name, identity_col.name
        );
        Ok(())
    }

    async fn set_table_logged(&self, schema: &str, table: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e.to_string(), "getting PostgreSQL connection"))?;

        let sql = format!(
            "ALTER TABLE {} SET LOGGED",
            Self::qualify_table(schema, table)
        );
        client
            .execute(&sql, &[])
            .await
            ?;

        debug!("Set table {}.{} to LOGGED", schema, table);
        Ok(())
    }

    async fn set_table_unlogged(&self, schema: &str, table: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e.to_string(), "getting PostgreSQL connection"))?;

        let sql = format!(
            "ALTER TABLE {} SET UNLOGGED",
            Self::qualify_table(schema, table)
        );
        client
            .execute(&sql, &[])
            .await
            ?;

        debug!("Set table {}.{} to UNLOGGED", schema, table);
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

        // Use batched INSERT statements
        // TODO: Implement COPY protocol for better performance
        let count = self.insert_batch(schema, table, cols, rows).await?;
        Ok(count)
    }

    async fn upsert_chunk(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        pk_cols: &[String],
        rows: Vec<Vec<SqlValue>>,
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        if pk_cols.is_empty() {
            return Err(MigrateError::NoPrimaryKey(table.to_string()));
        }

        // Build prepared statement SQL once
        let upsert_sql = Arc::new(build_prepared_upsert_sql(schema, table, cols, pk_cols));

        // Split rows into batches for parallel processing
        let batches: Vec<Vec<Vec<SqlValue>>> = rows
            .chunks(self.upsert_batch_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        // Limit concurrent tasks to avoid overwhelming the connection pool
        let max_parallel = self.upsert_parallel_tasks.min(batches.len());

        // Process batches in parallel groups
        let mut total = 0u64;
        for batch_group in batches.chunks(max_parallel) {
            let futures: Vec<_> = batch_group
                .iter()
                .map(|batch| {
                    let pool = self.pool.clone();
                    let sql = Arc::clone(&upsert_sql);
                    let batch = batch.clone();
                    let schema = schema.to_string();
                    let table = table.to_string();

                    async move {
                        let client = pool
                            .get()
                            .await
                            .map_err(|e| MigrateError::pool(e.to_string(), "getting PostgreSQL connection"))?;

                        // Prepare statement for this connection
                        let stmt = client
                            .prepare(&sql)
                            .await
                            ?;

                        let mut batch_total = 0u64;

                        for row in &batch {
                            let params: Vec<Box<dyn ToSql + Sync + Send>> =
                                row.iter().map(sql_value_to_boxed_param).collect();

                            let param_refs: Vec<&(dyn ToSql + Sync)> = params
                                .iter()
                                .map(|p| p.as_ref() as &(dyn ToSql + Sync))
                                .collect();

                            match client.execute(&stmt, &param_refs).await {
                                Ok(_) => batch_total += 1,
                                Err(e) => {
                                    let preview: Vec<String> = row
                                        .iter()
                                        .take(5)
                                        .map(|v| format!("{:?}", v))
                                        .collect();
                                    let preview_str = preview.join(", ");
                                    let suffix = if row.len() > 5 { "..." } else { "" };
                                    tracing::error!(
                                        "Upsert failed for {}.{}: {} | Row preview: [{}{}]",
                                        schema, table, e, preview_str, suffix
                                    );
                                    return Err(e.into());
                                }
                            }
                        }

                        Ok::<u64, MigrateError>(batch_total)
                    }
                })
                .collect();

            let results = try_join_all(futures).await?;
            total += results.iter().sum::<u64>();
        }

        Ok(total)
    }

    fn db_type(&self) -> &str {
        "postgres"
    }

    async fn close(&self) {
        // Pool will be closed when dropped
    }
}

impl PgPool {
    /// Insert rows using PostgreSQL COPY protocol for maximum performance.
    async fn insert_batch(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        rows: Vec<Vec<SqlValue>>,
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        if self.use_binary_copy {
            self.insert_batch_binary(schema, table, cols, rows).await
        } else {
            self.insert_batch_text(schema, table, cols, rows).await
        }
    }

    /// Insert rows using text COPY format.
    async fn insert_batch_text(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        rows: Vec<Vec<SqlValue>>,
    ) -> Result<u64> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e.to_string(), format!("getting connection for COPY to {}.{}", schema, table)))?;

        // Build column list
        let col_list: String = cols
            .iter()
            .map(|c| pg_quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");

        // COPY statement with text format
        let copy_stmt = format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT text)",
            pg_qualify_table(schema, table),
            col_list
        );

        let sink = client
            .copy_in(&copy_stmt)
            .await
            ?;

        futures::pin_mut!(sink);

        // Build data in chunks to avoid huge memory allocation
        let copy_buffer_rows = self.copy_buffer_rows;
        let mut buf = BytesMut::with_capacity(1024 * 1024); // 1MB buffer
        let row_count = rows.len();

        for (i, row) in rows.into_iter().enumerate() {
            // Build tab-separated line
            for (j, value) in row.iter().enumerate() {
                if j > 0 {
                    buf.put_u8(b'\t');
                }
                let text = sql_value_to_copy_text(value);
                buf.extend_from_slice(text.as_bytes());
            }
            buf.put_u8(b'\n');

            // Flush buffer periodically
            if (i + 1) % copy_buffer_rows == 0 || i + 1 == row_count {
                sink.send(buf.split().freeze())
                    .await
                    .map_err(|e| MigrateError::Transfer {
                        table: format!("{}.{}", schema, table),
                        message: format!("COPY send failed: {}", e),
                    })?;
            }
        }

        // Finalize COPY
        let copied = sink.finish().await?;

        Ok(copied)
    }

    /// Insert rows using binary COPY format with optimized batching.
    ///
    /// Optimizations (inspired by Go pgx):
    /// 1. Adaptive buffer sizing - flushes based on buffer size (~64KB), not row count
    /// 2. Concurrent serialize/send - overlaps serialization with network I/O
    /// 3. Tracks largest row to prevent buffer overflow
    async fn insert_batch_binary(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        rows: Vec<Vec<SqlValue>>,
    ) -> Result<u64> {
        use std::time::Instant;

        let row_count = rows.len();
        if row_count == 0 {
            return Ok(0);
        }

        let t0 = Instant::now();

        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e.to_string(), format!("getting connection for binary COPY to {}.{}", schema, table)))?;
        let t_conn = t0.elapsed();

        // Build column list
        let col_list: String = cols
            .iter()
            .map(|c| pg_quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");

        // COPY statement with binary format
        let copy_stmt = format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT binary)",
            pg_qualify_table(schema, table),
            col_list
        );

        let t1 = Instant::now();
        let sink = client
            .copy_in(&copy_stmt)
            .await
            ?;
        let t_copy_init = t1.elapsed();

        futures::pin_mut!(sink);

        // Create channel for concurrent serialize/send
        let (chunk_tx, mut chunk_rx) = mpsc::channel::<Bytes>(COPY_CHANNEL_SIZE);

        let num_cols = cols.len() as i16;
        let table_name = pg_qualify_table(schema, table);
        let table_name_clone = table_name.clone();

        // Spawn serializer task (producer)
        let serializer_handle = tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(COPY_SEND_BUF_SIZE * 2);

            // Write binary COPY header
            buf.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
            buf.put_i32(0); // Flags: no OIDs
            buf.put_i32(0); // Header extension length: 0

            // Track largest row for adaptive flushing
            let mut largest_row_len: usize = 0;

            for row in rows {
                let row_start = buf.len();

                // Field count (16-bit)
                buf.put_i16(num_cols);

                // Write each field
                for value in row.iter() {
                    write_binary_value(&mut buf, value);
                }

                // Track largest row size for buffer management
                let row_len = buf.len() - row_start;
                if row_len > largest_row_len {
                    largest_row_len = row_len;
                }

                // Adaptive flush: send when buffer approaches target size
                // Leave room for the largest row we've seen
                if buf.len() > COPY_SEND_BUF_SIZE.saturating_sub(largest_row_len) {
                    if chunk_tx.send(buf.split().freeze()).await.is_err() {
                        return Err(MigrateError::Transfer {
                            table: table_name_clone.clone(),
                            message: "Receiver dropped".into(),
                        });
                    }
                }
            }

            // Write trailer: -1 as 16-bit signed integer
            buf.put_i16(-1);

            // Send remaining data
            if !buf.is_empty() {
                if chunk_tx.send(buf.freeze()).await.is_err() {
                    return Err(MigrateError::Transfer {
                        table: table_name_clone.clone(),
                        message: "Receiver dropped (final)".into(),
                    });
                }
            }

            Ok::<usize, MigrateError>(largest_row_len)
        });

        // Consumer: send chunks to PostgreSQL concurrently with serialization
        let t2 = Instant::now();
        let mut send_count = 0usize;

        while let Some(chunk) = chunk_rx.recv().await {
            sink.send(chunk).await.map_err(|e| MigrateError::Transfer {
                table: table_name.clone(),
                message: format!("Binary COPY send failed: {}", e),
            })?;
            send_count += 1;
        }
        let t_send = t2.elapsed();

        // Wait for serializer to complete and check for errors
        let largest_row = match serializer_handle.await {
            Ok(Ok(size)) => size,
            Ok(Err(e)) => return Err(e),
            Err(e) => {
                return Err(MigrateError::Transfer {
                    table: table_name,
                    message: format!("Serializer task panicked: {}", e),
                })
            }
        };

        // Finalize COPY
        let t3 = Instant::now();
        let copied = sink.finish().await?;
        let t_finish = t3.elapsed();

        // Log timing breakdown for chunks with significant data
        if row_count > 1000 {
            debug!(
                "PROFILE write: {} rows in {} chunks - conn={:?}, copy_init={:?}, send={:?}, finish={:?}, largest_row={}B",
                row_count, send_count, t_conn, t_copy_init, t_send, t_finish, largest_row
            );
        }

        Ok(copied)
    }

    /// Stream rows directly to PostgreSQL using COPY protocol.
    ///
    /// This method accepts a receiver channel, allowing rows to be streamed
    /// from the source without buffering the entire dataset in memory.
    /// Uses concurrent serialization and network I/O for maximum throughput.
    pub async fn write_chunk_streaming(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        mut row_rx: mpsc::Receiver<Vec<SqlValue>>,
    ) -> Result<u64> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e.to_string(), "getting PostgreSQL connection"))?;

        // Build column list
        let col_list: String = cols
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");

        // COPY statement with binary format
        let copy_stmt = format!(
            "COPY \"{}\".\"{}\" ({}) FROM STDIN WITH (FORMAT binary)",
            schema, table, col_list
        );

        let sink = client
            .copy_in(&copy_stmt)
            .await
            ?;

        futures::pin_mut!(sink);

        // Create channel for concurrent serialize/send
        let (chunk_tx, mut chunk_rx) = mpsc::channel::<Bytes>(COPY_CHANNEL_SIZE);

        let num_cols = cols.len() as i16;
        let table_name = format!("{}.{}", schema, table);
        let table_name_clone = table_name.clone();

        // Spawn serializer task that reads from row_rx
        let serializer_handle = tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(COPY_SEND_BUF_SIZE * 2);

            // Write binary COPY header
            buf.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
            buf.put_i32(0); // Flags: no OIDs
            buf.put_i32(0); // Header extension length: 0

            let mut largest_row_len: usize = 0;
            let mut row_count: u64 = 0;

            while let Some(row) = row_rx.recv().await {
                let row_start = buf.len();

                buf.put_i16(num_cols);
                for value in row.iter() {
                    write_binary_value(&mut buf, value);
                }

                let row_len = buf.len() - row_start;
                if row_len > largest_row_len {
                    largest_row_len = row_len;
                }
                row_count += 1;

                // Adaptive flush
                if buf.len() > COPY_SEND_BUF_SIZE.saturating_sub(largest_row_len) {
                    if chunk_tx.send(buf.split().freeze()).await.is_err() {
                        return Err(MigrateError::Transfer {
                            table: table_name_clone.clone(),
                            message: "Receiver dropped".into(),
                        });
                    }
                }
            }

            // Write trailer
            buf.put_i16(-1);

            if !buf.is_empty() {
                if chunk_tx.send(buf.freeze()).await.is_err() {
                    return Err(MigrateError::Transfer {
                        table: table_name_clone.clone(),
                        message: "Receiver dropped (final)".into(),
                    });
                }
            }

            Ok::<u64, MigrateError>(row_count)
        });

        // Consumer: send chunks to PostgreSQL
        while let Some(chunk) = chunk_rx.recv().await {
            sink.send(chunk).await.map_err(|e| MigrateError::Transfer {
                table: table_name.clone(),
                message: format!("Binary COPY send failed: {}", e),
            })?;
        }

        // Wait for serializer
        let row_count = match serializer_handle.await {
            Ok(Ok(count)) => count,
            Ok(Err(e)) => return Err(e),
            Err(e) => {
                return Err(MigrateError::Transfer {
                    table: table_name,
                    message: format!("Serializer task panicked: {}", e),
                })
            }
        };

        // Finalize COPY
        let copied = sink.finish().await?;

        if copied != row_count {
            warn!(
                "Row count mismatch for {}: serialized {} but PostgreSQL reported {}",
                table_name, row_count, copied
            );
        }

        Ok(copied)
    }
}

/// Write a value in PostgreSQL binary format.
fn write_binary_value(buf: &mut BytesMut, value: &SqlValue) {
    match value {
        SqlValue::Null(_) => {
            // NULL is represented as -1 length
            buf.put_i32(-1);
        }
        SqlValue::Bool(b) => {
            buf.put_i32(1); // length
            buf.put_u8(if *b { 1 } else { 0 });
        }
        SqlValue::I16(n) => {
            buf.put_i32(2); // length
            buf.put_i16(*n);
        }
        SqlValue::I32(n) => {
            buf.put_i32(4); // length
            buf.put_i32(*n);
        }
        SqlValue::I64(n) => {
            buf.put_i32(8); // length
            buf.put_i64(*n);
        }
        SqlValue::F32(n) => {
            buf.put_i32(4); // length
            buf.put_f32(*n);
        }
        SqlValue::F64(n) => {
            buf.put_i32(8); // length
            buf.put_f64(*n);
        }
        SqlValue::String(s) => {
            let bytes = s.as_bytes();
            buf.put_i32(bytes.len() as i32);
            buf.extend_from_slice(bytes);
        }
        SqlValue::Bytes(b) => {
            buf.put_i32(b.len() as i32);
            buf.extend_from_slice(b);
        }
        SqlValue::Uuid(u) => {
            buf.put_i32(16); // UUID is always 16 bytes
            buf.extend_from_slice(u.as_bytes());
        }
        SqlValue::Decimal(d) => {
            // PostgreSQL NUMERIC binary format is complex, fallback to text representation
            // which PostgreSQL will parse. This is still faster than text COPY for other types.
            let s = d.to_string();
            let bytes = s.as_bytes();
            buf.put_i32(bytes.len() as i32);
            buf.extend_from_slice(bytes);
        }
        SqlValue::DateTime(dt) => {
            // PostgreSQL timestamp is microseconds since 2000-01-01 00:00:00
            // Chrono's timestamp is microseconds since 1970-01-01
            // Difference: 30 years = 946684800 seconds
            const POSTGRES_EPOCH_OFFSET: i64 = 946_684_800_000_000; // microseconds
            let micros = dt.and_utc().timestamp_micros() - POSTGRES_EPOCH_OFFSET;
            buf.put_i32(8);
            buf.put_i64(micros);
        }
        SqlValue::DateTimeOffset(dt) => {
            // PostgreSQL timestamptz is also stored as microseconds since 2000-01-01 UTC
            const POSTGRES_EPOCH_OFFSET: i64 = 946_684_800_000_000;
            let micros = dt.timestamp_micros() - POSTGRES_EPOCH_OFFSET;
            buf.put_i32(8);
            buf.put_i64(micros);
        }
        SqlValue::Date(d) => {
            // PostgreSQL date is days since 2000-01-01
            // Calculate days from 2000-01-01
            let epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
            let days = (*d - epoch).num_days() as i32;
            buf.put_i32(4);
            buf.put_i32(days);
        }
        SqlValue::Time(t) => {
            // PostgreSQL time is microseconds since midnight
            let micros =
                t.num_seconds_from_midnight() as i64 * 1_000_000 + t.nanosecond() as i64 / 1000;
            buf.put_i32(8);
            buf.put_i64(micros);
        }
    }
}

/// Convert SqlValue to text format for COPY.
/// Escapes special characters: backslash, tab, newline, carriage return.
fn sql_value_to_copy_text(value: &SqlValue) -> String {
    match value {
        SqlValue::Null(_) => "\\N".to_string(),
        SqlValue::Bool(b) => if *b { "t" } else { "f" }.to_string(),
        SqlValue::I16(n) => n.to_string(),
        SqlValue::I32(n) => n.to_string(),
        SqlValue::I64(n) => n.to_string(),
        SqlValue::F32(n) => n.to_string(),
        SqlValue::F64(n) => n.to_string(),
        SqlValue::String(s) => escape_copy_text(s),
        SqlValue::Bytes(b) => format!("\\\\x{}", hex::encode(b)),
        SqlValue::Uuid(u) => u.to_string(),
        SqlValue::Decimal(d) => d.to_string(),
        SqlValue::DateTime(dt) => dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
        SqlValue::DateTimeOffset(dt) => dt.to_rfc3339(),
        SqlValue::Date(d) => d.to_string(),
        SqlValue::Time(t) => t.to_string(),
    }
}

/// Escape special characters for COPY text format.
fn escape_copy_text(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => result.push_str("\\\\"),
            '\t' => result.push_str("\\t"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            _ => result.push(c),
        }
    }
    result
}

fn pg_quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn pg_qualify_table(schema: &str, table: &str) -> String {
    format!("{}.{}", pg_quote_ident(schema), pg_quote_ident(table))
}

/// Convert SqlValue to a boxed ToSql parameter with native types.
/// Uses native PostgreSQL types for better performance with prepared statements.
fn sql_value_to_boxed_param(value: &SqlValue) -> Box<dyn ToSql + Sync + Send> {
    match value {
        SqlValue::Null(null_type) => {
            // Return properly typed NULL for each type
            match null_type {
                SqlNullType::Bool => Box::new(None::<bool>),
                SqlNullType::I16 => Box::new(None::<i16>),
                SqlNullType::I32 => Box::new(None::<i32>),
                SqlNullType::I64 => Box::new(None::<i64>),
                SqlNullType::F32 => Box::new(None::<f32>),
                SqlNullType::F64 => Box::new(None::<f64>),
                SqlNullType::String => Box::new(None::<String>),
                SqlNullType::Bytes => Box::new(None::<Vec<u8>>),
                SqlNullType::Uuid => Box::new(None::<uuid::Uuid>),
                SqlNullType::Decimal => Box::new(None::<rust_decimal::Decimal>),
                SqlNullType::DateTime => Box::new(None::<chrono::NaiveDateTime>),
                SqlNullType::DateTimeOffset => Box::new(None::<chrono::NaiveDateTime>),
                SqlNullType::Date => Box::new(None::<chrono::NaiveDate>),
                SqlNullType::Time => Box::new(None::<chrono::NaiveTime>),
            }
        }
        SqlValue::Bool(b) => Box::new(*b),
        SqlValue::I16(n) => Box::new(*n),
        SqlValue::I32(n) => Box::new(*n),
        SqlValue::I64(n) => Box::new(*n),
        SqlValue::F32(n) => Box::new(*n),
        SqlValue::F64(n) => Box::new(*n),
        SqlValue::String(s) => Box::new(s.clone()),
        SqlValue::Bytes(b) => Box::new(b.clone()),
        SqlValue::Uuid(u) => Box::new(*u),
        SqlValue::Decimal(d) => Box::new(*d),
        SqlValue::DateTime(dt) => Box::new(*dt),
        SqlValue::DateTimeOffset(dt) => Box::new(dt.naive_utc()),
        SqlValue::Date(d) => Box::new(*d),
        SqlValue::Time(t) => Box::new(*t),
    }
}

/// Build prepared UPSERT SQL statement with parameter placeholders.
fn build_prepared_upsert_sql(
    schema: &str,
    table: &str,
    cols: &[String],
    pk_cols: &[String],
) -> String {
    let col_list: String = cols
        .iter()
        .map(|c| pg_quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    let pk_list: String = pk_cols
        .iter()
        .map(|c| pg_quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    // Build parameter placeholders ($1, $2, ...)
    let placeholders: String = (1..=cols.len())
        .map(|i| format!("${}", i))
        .collect::<Vec<_>>()
        .join(", ");

    // Build UPDATE SET clause (exclude PK columns)
    let update_cols: Vec<String> = cols
        .iter()
        .filter(|c| !pk_cols.contains(c))
        .map(|c| format!("{} = EXCLUDED.{}", pg_quote_ident(c), pg_quote_ident(c)))
        .collect();

    // Build change detection WHERE clause
    let change_detection: Vec<String> = cols
        .iter()
        .filter(|c| !pk_cols.contains(c))
        .map(|c| {
            format!(
                "{}.{} IS DISTINCT FROM EXCLUDED.{}",
                pg_quote_ident(table),
                pg_quote_ident(c),
                pg_quote_ident(c)
            )
        })
        .collect();

    if update_cols.is_empty() {
        // PK-only table, just INSERT ... ON CONFLICT DO NOTHING
        format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING",
            pg_qualify_table(schema, table),
            col_list,
            placeholders,
            pk_list
        )
    } else {
        format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {} WHERE {}",
            pg_qualify_table(schema, table),
            col_list,
            placeholders,
            pk_list,
            update_cols.join(", "),
            change_detection.join(" OR ")
        )
    }
}
