//! PostgreSQL target writer implementation.
//!
//! Implements the `TargetWriter` trait for writing data to PostgreSQL databases.
//! Uses deadpool-postgres for connection pooling and binary COPY protocol.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use chrono::Timelike;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use futures::SinkExt;
use rustls::ClientConfig;
use tokio_postgres::Config as PgConfig;
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{debug, info, warn};

use crate::config::TargetConfig;
use crate::core::schema::{CheckConstraint, Column, ForeignKey, Index, Table};
use crate::core::traits::{TargetWriter, TypeMapper};
use crate::core::value::{Batch, SqlNullType, SqlValue};
use crate::error::{MigrateError, Result};
use crate::target::{SqlNullType as TargetSqlNullType, SqlValue as TargetSqlValue, UpsertWriter};
use std::borrow::Cow;

/// Connection pool timeout.
const POOL_CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// PostgreSQL target writer implementation.
pub struct PostgresWriter {
    pool: Pool,
    type_mapper: Option<Arc<dyn TypeMapper>>,
}

impl PostgresWriter {
    /// Create a new PostgreSQL writer from configuration.
    pub async fn new(config: &TargetConfig, max_conns: usize) -> Result<Self> {
        let mut pg_config = PgConfig::new();
        pg_config.host(&config.host);
        pg_config.port(config.port);
        pg_config.dbname(&config.database);
        pg_config.user(&config.user);
        pg_config.password(&config.password);

        // Connection options for reliability
        pg_config.keepalives(true);
        pg_config.keepalives_idle(Duration::from_secs(30));
        pg_config.connect_timeout(POOL_CONNECTION_TIMEOUT);

        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };

        let ssl_mode = config.ssl_mode.as_str();
        let pool = match ssl_mode.to_lowercase().as_str() {
            "disable" => {
                warn!("PostgreSQL TLS is disabled. Credentials will be transmitted in plaintext.");
                let mgr = Manager::from_config(pg_config, tokio_postgres::NoTls, mgr_config);
                Pool::builder(mgr)
                    .max_size(max_conns)
                    .build()
                    .map_err(|e| MigrateError::pool(e, "creating PostgreSQL target pool"))?
            }
            _ => {
                let tls_config = Self::build_tls_config(ssl_mode)?;
                let tls_connector = MakeRustlsConnect::new(tls_config);
                let mgr = Manager::from_config(pg_config, tls_connector, mgr_config);
                Pool::builder(mgr)
                    .max_size(max_conns)
                    .build()
                    .map_err(|e| MigrateError::pool(e, "creating PostgreSQL target pool"))?
            }
        };

        // Test connection
        let client = pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "testing PostgreSQL target connection"))?;
        client.simple_query("SELECT 1").await?;

        info!(
            "Connected to PostgreSQL target: {}:{}/{}",
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

    /// Build TLS configuration.
    fn build_tls_config(ssl_mode: &str) -> Result<ClientConfig> {
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let config = match ssl_mode {
            "require" => {
                warn!("ssl_mode=require: TLS enabled but server certificate is not verified.");
                ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(NoVerifier))
                    .with_no_client_auth()
            }
            "verify-ca" | "verify-full" => {
                info!("ssl_mode={}: certificate verification enabled", ssl_mode);
                ClientConfig::builder()
                    .with_root_certificates(root_store)
                    .with_no_client_auth()
            }
            other => {
                return Err(MigrateError::Config(format!(
                    "Invalid ssl_mode '{}'. Valid options: disable, require, verify-ca, verify-full",
                    other
                )));
            }
        };

        Ok(config)
    }

    /// Quote a PostgreSQL identifier.
    fn quote_ident(name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    /// Qualify a table name with schema.
    fn qualify_table(schema: &str, table: &str) -> String {
        format!("{}.{}", Self::quote_ident(schema), Self::quote_ident(table))
    }

    /// Map a source type to PostgreSQL type.
    fn map_type(&self, col: &Column) -> String {
        if let Some(mapper) = &self.type_mapper {
            let mapping = mapper.map_column(col);
            if let Some(warning) = &mapping.warning {
                warn!("Column {}: {}", col.name, warning);
            }
            mapping.target_type
        } else {
            // No mapper - assume source is PostgreSQL
            format_postgres_type(&col.data_type, col.max_length, col.precision, col.scale)
        }
    }

    /// Generate table DDL.
    fn generate_ddl(&self, table: &Table, target_schema: &str, unlogged: bool) -> String {
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

        let unlogged_str = if unlogged { "UNLOGGED " } else { "" };

        format!(
            "CREATE {}TABLE {} (\n    {}\n)",
            unlogged_str,
            Self::qualify_table(target_schema, &table.name),
            col_defs.join(",\n    ")
        )
    }

    // === Backward-compatible methods for legacy transfer engine ===

    /// Get the underlying connection pool for state backend creation.
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Test the database connection.
    pub async fn test_connection(&self) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "testing PostgreSQL connection"))?;
        client.simple_query("SELECT 1").await?;
        Ok(())
    }

    /// Write rows using old SqlValue type (backward compatibility).
    pub async fn write_chunk(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        rows: Vec<Vec<TargetSqlValue>>,
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        // Convert old SqlValue to new SqlValue
        let converted_rows: Vec<Vec<SqlValue<'static>>> = rows
            .into_iter()
            .map(|row| row.into_iter().map(convert_target_to_new_sql_value).collect())
            .collect();

        let batch = Batch::new(converted_rows);

        // Call the new write_batch via TargetWriter trait
        TargetWriter::write_batch(self, schema, table, cols, batch).await
    }

    /// Upsert rows using old SqlValue type (backward compatibility).
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_chunk(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        pk_cols: &[String],
        rows: Vec<Vec<TargetSqlValue>>,
        writer_id: usize,
        partition_id: Option<i32>,
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        // Convert old SqlValue to new SqlValue
        let converted_rows: Vec<Vec<SqlValue<'static>>> = rows
            .into_iter()
            .map(|row| row.into_iter().map(convert_target_to_new_sql_value).collect())
            .collect();

        let batch = Batch::new(converted_rows);

        // Call the new upsert_batch via TargetWriter trait
        TargetWriter::upsert_batch(self, schema, table, cols, pk_cols, batch, writer_id, partition_id).await
    }

    /// Get an upsert writer for streaming upserts (backward compatibility).
    pub async fn get_upsert_writer(
        &self,
        schema: &str,
        table: &str,
        writer_id: usize,
        partition_id: Option<i32>,
    ) -> Result<Box<dyn UpsertWriter>> {
        // Return a wrapper that adapts the new writer to the old UpsertWriter trait
        Ok(Box::new(PostgresUpsertWriterAdapter {
            pool: self.pool.clone(),
            schema: schema.to_string(),
            table: table.to_string(),
            writer_id,
            partition_id,
        }))
    }
}

/// Convert old target::SqlValue to new core::value::SqlValue.
fn convert_target_to_new_sql_value(value: TargetSqlValue) -> SqlValue<'static> {
    match value {
        TargetSqlValue::Null(nt) => SqlValue::Null(match nt {
            TargetSqlNullType::Bool => SqlNullType::Bool,
            TargetSqlNullType::I16 => SqlNullType::I16,
            TargetSqlNullType::I32 => SqlNullType::I32,
            TargetSqlNullType::I64 => SqlNullType::I64,
            TargetSqlNullType::F32 => SqlNullType::F32,
            TargetSqlNullType::F64 => SqlNullType::F64,
            TargetSqlNullType::String => SqlNullType::String,
            TargetSqlNullType::Bytes => SqlNullType::Bytes,
            TargetSqlNullType::Uuid => SqlNullType::Uuid,
            TargetSqlNullType::DateTime => SqlNullType::DateTime,
            TargetSqlNullType::Date => SqlNullType::Date,
            TargetSqlNullType::Time => SqlNullType::Time,
            TargetSqlNullType::Decimal => SqlNullType::Decimal,
            TargetSqlNullType::DateTimeOffset => SqlNullType::DateTimeOffset,
        }),
        TargetSqlValue::Bool(b) => SqlValue::Bool(b),
        TargetSqlValue::I16(i) => SqlValue::I16(i),
        TargetSqlValue::I32(i) => SqlValue::I32(i),
        TargetSqlValue::I64(i) => SqlValue::I64(i),
        TargetSqlValue::F32(f) => SqlValue::F32(f),
        TargetSqlValue::F64(f) => SqlValue::F64(f),
        TargetSqlValue::String(s) => SqlValue::Text(Cow::Owned(s)),
        TargetSqlValue::Bytes(b) => SqlValue::Bytes(Cow::Owned(b)),
        TargetSqlValue::Uuid(u) => SqlValue::Uuid(u),
        TargetSqlValue::DateTime(dt) => SqlValue::DateTime(dt),
        TargetSqlValue::Date(d) => SqlValue::Date(d),
        TargetSqlValue::Time(t) => SqlValue::Time(t),
        TargetSqlValue::Decimal(d) => SqlValue::Decimal(d),
        TargetSqlValue::CompressedText { compressed, original_len } => {
            SqlValue::CompressedText {
                compressed,
                original_len,
            }
        }
        TargetSqlValue::DateTimeOffset(dto) => SqlValue::DateTimeOffset(dto),
    }
}

/// Adapter to make PostgresWriter work with the old UpsertWriter trait.
struct PostgresUpsertWriterAdapter {
    pool: Pool,
    schema: String,
    table: String,
    writer_id: usize,
    partition_id: Option<i32>,
}

#[async_trait]
impl UpsertWriter for PostgresUpsertWriterAdapter {
    async fn upsert_chunk(
        &mut self,
        cols: &[String],
        pk_cols: &[String],
        rows: Vec<Vec<TargetSqlValue>>,
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        // Convert old SqlValue to new SqlValue
        let converted_rows: Vec<Vec<SqlValue<'static>>> = rows
            .into_iter()
            .map(|row| row.into_iter().map(convert_target_to_new_sql_value).collect())
            .collect();

        let batch = Batch::new(converted_rows);
        let row_count = batch.rows.len() as u64;

        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        // Create staging table name
        let staging_name = match self.partition_id {
            Some(pid) => format!("_staging_{}_p{}_{}", self.table, pid, self.writer_id),
            None => format!("_staging_{}_{}", self.table, self.writer_id),
        };

        let qualified_target = format!(
            "{}.{}",
            quote_ident(&self.schema),
            quote_ident(&self.table)
        );
        // Temp tables are in session's temp schema, not the target schema
        let staging_quoted = quote_ident(&staging_name);

        // Create temp table (no ON COMMIT DROP - we'll drop it manually)
        let create_staging = format!(
            "CREATE TEMP TABLE IF NOT EXISTS {} (LIKE {} INCLUDING DEFAULTS)",
            quote_ident(&staging_name),
            qualified_target
        );
        client.execute(&create_staging, &[]).await?;

        // Truncate staging
        let truncate_staging = format!("TRUNCATE TABLE {}", quote_ident(&staging_name));
        client.execute(&truncate_staging, &[]).await?;

        // Write batch to staging using COPY
        let col_list: Vec<String> = cols.iter().map(|c| quote_ident(c)).collect();
        let copy_sql = format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT BINARY)",
            quote_ident(&staging_name),
            col_list.join(", ")
        );

        let sink = client
            .copy_in(&copy_sql)
            .await
            .map_err(|e| MigrateError::transfer(&self.table, format!("initiating COPY: {}", e)))?;

        let mut buf = BytesMut::with_capacity(batch.rows.len() * 512);

        // Write header
        buf.put_slice(b"PGCOPY\n\xff\r\n\0");
        buf.put_i32(0); // flags
        buf.put_i32(0); // extension area length

        // Write rows
        for row in &batch.rows {
            buf.put_i16(row.len() as i16);
            for val in row {
                write_binary_value(&mut buf, val);
            }
        }

        // Write trailer
        buf.put_i16(-1);

        // Use pin_mut for the sink
        tokio::pin!(sink);
        let data = buf.freeze();
        sink.send(data)
            .await
            .map_err(|e| MigrateError::transfer(&self.table, format!("sending COPY data: {}", e)))?;
        sink.finish()
            .await
            .map_err(|e| MigrateError::transfer(&self.table, format!("finishing COPY: {}", e)))?;

        // Merge into target
        let pk_list: Vec<String> = pk_cols.iter().map(|c| quote_ident(c)).collect();
        let update_cols: Vec<String> = cols
            .iter()
            .filter(|c| !pk_cols.contains(c))
            .map(|c| format!("{} = EXCLUDED.{}", quote_ident(c), quote_ident(c)))
            .collect();

        let merge_sql = if update_cols.is_empty() {
            format!(
                "INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT ({}) DO NOTHING",
                qualified_target,
                col_list.join(", "),
                col_list.join(", "),
                staging_quoted,
                pk_list.join(", ")
            )
        } else {
            format!(
                "INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT ({}) DO UPDATE SET {}",
                qualified_target,
                col_list.join(", "),
                col_list.join(", "),
                staging_quoted,
                pk_list.join(", "),
                update_cols.join(", ")
            )
        };

        client.execute(&merge_sql, &[]).await?;

        // Drop staging table to free temp memory
        let drop_staging = format!("DROP TABLE IF EXISTS {}", staging_quoted);
        client.execute(&drop_staging, &[]).await.ok();

        Ok(row_count)
    }
}

/// Quote a PostgreSQL identifier.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[async_trait]
impl TargetWriter for PostgresWriter {
    async fn create_schema(&self, schema: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        let sql = format!("CREATE SCHEMA IF NOT EXISTS {}", Self::quote_ident(schema));
        client.execute(&sql, &[]).await?;

        debug!("Created schema '{}'", schema);
        Ok(())
    }

    async fn create_table(&self, table: &Table, target_schema: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        let ddl = self.generate_ddl(table, target_schema, false);
        client.execute(&ddl, &[]).await?;

        debug!("Created table {}.{}", target_schema, table.name);
        Ok(())
    }

    async fn create_table_unlogged(&self, table: &Table, target_schema: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        let ddl = self.generate_ddl(table, target_schema, true);
        client.execute(&ddl, &[]).await?;

        debug!("Created UNLOGGED table {}.{}", target_schema, table.name);
        Ok(())
    }

    async fn drop_table(&self, schema: &str, table: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        let sql = format!(
            "DROP TABLE IF EXISTS {} CASCADE",
            Self::qualify_table(schema, table)
        );
        client.execute(&sql, &[]).await?;

        debug!("Dropped table {}.{}", schema, table);
        Ok(())
    }

    async fn table_exists(&self, schema: &str, table: &str) -> Result<bool> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        let sql = r#"
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )
        "#;

        let row = client.query_one(sql, &[&schema, &table]).await?;
        Ok(row.get::<_, bool>(0))
    }

    async fn create_primary_key(&self, table: &Table, target_schema: &str) -> Result<()> {
        if table.primary_key.is_empty() {
            return Ok(());
        }

        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        let pk_cols: Vec<String> = table
            .primary_key
            .iter()
            .map(|c| Self::quote_ident(c))
            .collect();
        let pk_name = format!("pk_{}_{}", target_schema, table.name);

        let sql = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} PRIMARY KEY ({})",
            Self::qualify_table(target_schema, &table.name),
            Self::quote_ident(&pk_name),
            pk_cols.join(", ")
        );

        client.execute(&sql, &[]).await?;
        debug!("Created primary key on {}.{}", target_schema, table.name);
        Ok(())
    }

    async fn create_index(&self, table: &Table, idx: &Index, target_schema: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        let idx_cols: Vec<String> = idx.columns.iter().map(|c| Self::quote_ident(c)).collect();
        let unique = if idx.is_unique { "UNIQUE " } else { "" };

        let sql = format!(
            "CREATE {}INDEX {} ON {} ({})",
            unique,
            Self::quote_ident(&idx.name),
            Self::qualify_table(target_schema, &table.name),
            idx_cols.join(", ")
        );

        client.execute(&sql, &[]).await?;
        debug!(
            "Created index {} on {}.{}",
            idx.name, target_schema, table.name
        );
        Ok(())
    }

    async fn drop_non_pk_indexes(&self, schema: &str, table: &str) -> Result<Vec<String>> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        let query = r#"
            SELECT i.relname
            FROM pg_catalog.pg_index ix
            JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
            JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = $1 AND t.relname = $2 AND NOT ix.indisprimary
        "#;

        let rows = client.query(query, &[&schema, &table]).await?;
        let mut dropped_indexes = Vec::new();

        for row in rows {
            let name: String = row.get(0);
            let drop_sql = format!(
                "DROP INDEX IF EXISTS {}.{}",
                Self::quote_ident(schema),
                Self::quote_ident(&name)
            );
            client.execute(&drop_sql, &[]).await?;
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
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

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

        client.execute(&sql, &[]).await?;
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
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        let sql = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} {}",
            Self::qualify_table(target_schema, &table.name),
            Self::quote_ident(&chk.name),
            convert_check_definition(&chk.definition)
        );

        client.execute(&sql, &[]).await?;
        debug!(
            "Created check constraint {} on {}.{}",
            chk.name, target_schema, table.name
        );
        Ok(())
    }

    async fn has_primary_key(&self, schema: &str, table: &str) -> Result<bool> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        let sql = r#"
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_index ix
                JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = $1 AND t.relname = $2 AND ix.indisprimary
            )
        "#;

        let row = client.query_one(sql, &[&schema, &table]).await?;
        Ok(row.get::<_, bool>(0))
    }

    async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        let sql = format!(
            "SELECT COUNT(*)::int8 FROM {}",
            Self::qualify_table(schema, table)
        );
        let row = client.query_one(&sql, &[]).await?;
        Ok(row.get::<_, i64>(0))
    }

    async fn reset_sequence(&self, schema: &str, table: &Table) -> Result<()> {
        if table.primary_key.is_empty() {
            return Ok(());
        }

        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        // Try to find and reset sequence
        for pk_col in &table.primary_key {
            let seq_query = format!(
                "SELECT pg_get_serial_sequence('{}', '{}')",
                Self::qualify_table(schema, &table.name),
                pk_col
            );

            if let Ok(row) = client.query_one(&seq_query, &[]).await {
                if let Some(seq_name) = row.get::<_, Option<String>>(0) {
                    let reset_query = format!(
                        "SELECT setval('{}', COALESCE((SELECT MAX({}) FROM {}), 1), true)",
                        seq_name,
                        Self::quote_ident(pk_col),
                        Self::qualify_table(schema, &table.name)
                    );
                    client.execute(&reset_query, &[]).await?;
                    debug!("Reset sequence {} for {}.{}", seq_name, schema, table.name);
                }
            }
        }

        Ok(())
    }

    async fn set_table_logged(&self, schema: &str, table: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        let sql = format!(
            "ALTER TABLE {} SET LOGGED",
            Self::qualify_table(schema, table)
        );
        client.execute(&sql, &[]).await?;

        debug!("Set table {}.{} to LOGGED", schema, table);
        Ok(())
    }

    async fn set_table_unlogged(&self, schema: &str, table: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        let sql = format!(
            "ALTER TABLE {} SET UNLOGGED",
            Self::qualify_table(schema, table)
        );
        client.execute(&sql, &[]).await?;

        debug!("Set table {}.{} to UNLOGGED", schema, table);
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

        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        // Build COPY statement
        let col_list: Vec<String> = cols.iter().map(|c| Self::quote_ident(c)).collect();
        let copy_sql = format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT BINARY)",
            qualified_table,
            col_list.join(", ")
        );

        // Get sink for binary COPY
        let sink = client
            .copy_in(&copy_sql)
            .await
            .map_err(|e| MigrateError::transfer(&qualified_table, format!("COPY init: {}", e)))?;

        // Build binary data
        let mut buf = BytesMut::with_capacity(rows.len() * 256);

        // Write PGCOPY header: signature + flags + extension
        buf.put_slice(b"PGCOPY\n\xff\r\n\0");
        buf.put_i32(0); // flags
        buf.put_i32(0); // extension area length

        // Write rows
        for row in &rows {
            // Column count
            buf.put_i16(row.len() as i16);

            for value in row {
                write_binary_value(&mut buf, value);
            }
        }

        // Write trailer
        buf.put_i16(-1);

        // Send data
        let data = buf.freeze();
        tokio::pin!(sink);

        use futures::SinkExt;
        sink.send(data)
            .await
            .map_err(|e| MigrateError::transfer(&qualified_table, format!("COPY send: {}", e)))?;

        sink.finish()
            .await
            .map_err(|e| MigrateError::transfer(&qualified_table, format!("COPY finish: {}", e)))?;

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

        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        // Create staging table
        let staging_name = match partition_id {
            Some(pid) => format!("_staging_{}_p{}_{}", table, pid, writer_id),
            None => format!("_staging_{}_{}", table, writer_id),
        };
        let staging_qualified = Self::qualify_table(schema, &staging_name);

        // Drop if exists, create staging
        let drop_sql = format!("DROP TABLE IF EXISTS {}", staging_qualified);
        client.execute(&drop_sql, &[]).await?;

        let create_sql = format!(
            "CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS)",
            Self::quote_ident(&staging_name),
            qualified_target
        );
        client.execute(&create_sql, &[]).await?;

        // COPY to staging
        let col_list: Vec<String> = cols.iter().map(|c| Self::quote_ident(c)).collect();
        let copy_sql = format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT TEXT)",
            Self::quote_ident(&staging_name),
            col_list.join(", ")
        );

        let sink = client.copy_in(&copy_sql).await?;

        // Write rows as text (simpler for staging)
        let mut text_buf = String::with_capacity(rows.len() * 256);
        for row in &rows {
            for (i, value) in row.iter().enumerate() {
                if i > 0 {
                    text_buf.push('\t');
                }
                text_buf.push_str(&value_to_text(value));
            }
            text_buf.push('\n');
        }

        tokio::pin!(sink);
        use futures::SinkExt;
        sink.send(bytes::Bytes::from(text_buf)).await?;
        sink.finish().await?;

        // Build upsert SQL
        let pk_col_list: Vec<String> = pk_cols.iter().map(|c| Self::quote_ident(c)).collect();
        let update_cols: Vec<String> = cols
            .iter()
            .filter(|c| !pk_cols.contains(c))
            .map(|c| {
                format!(
                    "{} = EXCLUDED.{}",
                    Self::quote_ident(c),
                    Self::quote_ident(c)
                )
            })
            .collect();

        let upsert_sql = if update_cols.is_empty() {
            format!(
                "INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT ({}) DO NOTHING",
                qualified_target,
                col_list.join(", "),
                col_list.join(", "),
                Self::quote_ident(&staging_name),
                pk_col_list.join(", ")
            )
        } else {
            format!(
                "INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT ({}) DO UPDATE SET {}",
                qualified_target,
                col_list.join(", "),
                col_list.join(", "),
                Self::quote_ident(&staging_name),
                pk_col_list.join(", "),
                update_cols.join(", ")
            )
        };

        client.execute(&upsert_sql, &[]).await?;

        // Drop staging
        client
            .execute(
                &format!("DROP TABLE IF EXISTS {}", Self::quote_ident(&staging_name)),
                &[],
            )
            .await?;

        Ok(row_count)
    }

    fn db_type(&self) -> &str {
        "postgres"
    }

    async fn close(&self) {
        // deadpool handles cleanup
    }
}

/// Write a SqlValue as PostgreSQL binary format.
fn write_binary_value(buf: &mut BytesMut, value: &SqlValue<'_>) {
    match value {
        SqlValue::Null(_) => {
            buf.put_i32(-1);
        }
        SqlValue::Bool(b) => {
            buf.put_i32(1);
            buf.put_u8(if *b { 1 } else { 0 });
        }
        SqlValue::I16(i) => {
            buf.put_i32(2);
            buf.put_i16(*i);
        }
        SqlValue::I32(i) => {
            buf.put_i32(4);
            buf.put_i32(*i);
        }
        SqlValue::I64(i) => {
            buf.put_i32(8);
            buf.put_i64(*i);
        }
        SqlValue::F32(f) => {
            buf.put_i32(4);
            buf.put_f32(*f);
        }
        SqlValue::F64(f) => {
            buf.put_i32(8);
            buf.put_f64(*f);
        }
        SqlValue::Text(s) => {
            let bytes = s.as_bytes();
            buf.put_i32(bytes.len() as i32);
            buf.put_slice(bytes);
        }
        SqlValue::CompressedText { compressed, .. } => {
            // Decompress LZ4 data
            match lz4_flex::decompress_size_prepended(compressed) {
                Ok(decompressed) => {
                    buf.put_i32(decompressed.len() as i32);
                    buf.put_slice(&decompressed);
                }
                Err(_) => {
                    buf.put_i32(0);
                }
            }
        }
        SqlValue::Bytes(b) => {
            buf.put_i32(b.len() as i32);
            buf.put_slice(b);
        }
        SqlValue::Uuid(u) => {
            buf.put_i32(16);
            buf.put_slice(u.as_bytes());
        }
        SqlValue::Decimal(d) => {
            // PostgreSQL NUMERIC binary format:
            // - ndigits (i16): number of base-10000 digits
            // - weight (i16): weight of first digit (position above decimal)
            // - sign (i16): 0x0000=positive, 0x4000=negative
            // - dscale (i16): display scale (decimal places)
            // - digits (i16[]): base-10000 digits
            encode_decimal_binary(buf, d);
        }
        SqlValue::DateTime(dt) => {
            // PostgreSQL timestamp: microseconds since 2000-01-01
            let epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
            let micros = (*dt - epoch).num_microseconds().unwrap_or(0);
            buf.put_i32(8);
            buf.put_i64(micros);
        }
        SqlValue::DateTimeOffset(dto) => {
            let epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
            let micros = (dto.naive_utc() - epoch).num_microseconds().unwrap_or(0);
            buf.put_i32(8);
            buf.put_i64(micros);
        }
        SqlValue::Date(d) => {
            // Days since 2000-01-01
            let epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
            let days = (*d - epoch).num_days() as i32;
            buf.put_i32(4);
            buf.put_i32(days);
        }
        SqlValue::Time(t) => {
            // Microseconds since midnight
            let micros =
                t.num_seconds_from_midnight() as i64 * 1_000_000 + (t.nanosecond() / 1000) as i64;
            buf.put_i32(8);
            buf.put_i64(micros);
        }
    }
}

/// Encode a Decimal value into PostgreSQL binary NUMERIC format.
///
/// PostgreSQL NUMERIC binary format:
/// - ndigits (i16): number of base-10000 digits
/// - weight (i16): weight of first digit (position of first digit above decimal, minus 1)
/// - sign (i16): 0x0000=positive, 0x4000=negative
/// - dscale (i16): display scale (decimal places)
/// - digits (i16[]): array of base-10000 digits
fn encode_decimal_binary(buf: &mut BytesMut, d: &rust_decimal::Decimal) {
    const NUMERIC_POS: i16 = 0x0000;
    const NUMERIC_NEG: i16 = 0x4000;

    // Handle zero
    if d.is_zero() {
        buf.put_i32(8); // 4 i16 header fields
        buf.put_i16(0); // ndigits
        buf.put_i16(0); // weight
        buf.put_i16(NUMERIC_POS); // sign
        buf.put_i16(d.scale() as i16); // dscale
        return;
    }

    let sign = if d.is_sign_negative() {
        NUMERIC_NEG
    } else {
        NUMERIC_POS
    };
    let scale = d.scale();
    let dscale = scale as i16;

    // Use string representation to properly handle decimal positioning
    // This correctly handles cases like 0.01 where mantissa=1 but we need "01"
    let abs_str = d.abs().to_string();

    // Split into integer and fractional parts
    let (int_part, frac_part) = if let Some(dot_pos) = abs_str.find('.') {
        (&abs_str[..dot_pos], &abs_str[dot_pos + 1..])
    } else {
        (abs_str.as_str(), "")
    };

    // For base-10000, we need to group digits from the decimal point
    // Integer part: group from right to left (from decimal point) in groups of 4
    // Fractional part: group from left to right (from decimal point) in groups of 4

    // Process integer part: pad on LEFT to multiple of 4
    let mut int_digits: Vec<i16> = Vec::new();
    let int_part_clean = int_part.trim_start_matches('0');
    if !int_part_clean.is_empty() {
        let padded_len = ((int_part_clean.len() + 3) / 4) * 4;
        let padded = format!("{:0>width$}", int_part_clean, width = padded_len);
        for chunk in padded.as_bytes().chunks(4) {
            let s = std::str::from_utf8(chunk).unwrap();
            int_digits.push(s.parse::<i16>().unwrap());
        }
    }

    // Process fractional part: pad on RIGHT to multiple of 4
    let mut frac_digits: Vec<i16> = Vec::new();
    if !frac_part.is_empty() {
        let padded_len = ((frac_part.len() + 3) / 4) * 4;
        let mut padded = frac_part.to_string();
        while padded.len() < padded_len {
            padded.push('0');
        }
        for chunk in padded.as_bytes().chunks(4) {
            let s = std::str::from_utf8(chunk).unwrap();
            frac_digits.push(s.parse::<i16>().unwrap());
        }
    }

    // Calculate weight
    let num_int_groups = int_digits.len() as i16;
    let weight = if num_int_groups > 0 {
        num_int_groups - 1
    } else {
        // All fractional - find how many leading zero groups
        // e.g., 0.0001 -> frac_part="0001" -> first group is 1 -> weight=-1
        // e.g., 0.00000001 -> frac_part="00000001" -> groups ["0000","0001"] -> first nonzero at index 1 -> weight=-2
        let mut leading_zero_groups = 0i16;
        for &digit in &frac_digits {
            if digit == 0 {
                leading_zero_groups += 1;
            } else {
                break;
            }
        }
        -(leading_zero_groups + 1)
    };

    // Combine digits
    let mut base10000_digits: Vec<i16> = Vec::new();
    base10000_digits.extend(int_digits);
    base10000_digits.extend(frac_digits);

    // Remove trailing zeros from the digit array (PostgreSQL does this)
    while base10000_digits.len() > 1 && *base10000_digits.last().unwrap() == 0 {
        base10000_digits.pop();
    }

    // Remove leading zeros (but keep at least one digit)
    // Note: weight is NOT adjusted because it represents the position of the first non-zero digit
    while base10000_digits.len() > 1 && base10000_digits[0] == 0 {
        base10000_digits.remove(0);
    }

    let ndigits = base10000_digits.len() as i16;

    // Write length: 8 bytes header + 2 bytes per digit
    let len = 8 + (ndigits as i32 * 2);
    buf.put_i32(len);

    // Write header
    buf.put_i16(ndigits);
    buf.put_i16(weight);
    buf.put_i16(sign);
    buf.put_i16(dscale);

    // Write digits
    for digit in base10000_digits {
        buf.put_i16(digit);
    }
}

/// Convert SqlValue to text for COPY.
fn value_to_text(value: &SqlValue<'_>) -> String {
    match value {
        SqlValue::Null(_) => "\\N".to_string(),
        SqlValue::Bool(b) => if *b { "t" } else { "f" }.to_string(),
        SqlValue::I16(i) => i.to_string(),
        SqlValue::I32(i) => i.to_string(),
        SqlValue::I64(i) => i.to_string(),
        SqlValue::F32(f) => f.to_string(),
        SqlValue::F64(f) => f.to_string(),
        SqlValue::Text(s) => escape_copy_text(s),
        SqlValue::CompressedText { compressed, .. } => {
            // Decompress LZ4 data
            match lz4_flex::decompress_size_prepended(compressed) {
                Ok(decompressed) => escape_copy_text(&String::from_utf8_lossy(&decompressed)),
                Err(_) => String::new(),
            }
        }
        SqlValue::Bytes(b) => format!("\\\\x{}", hex::encode(b.as_ref())),
        SqlValue::Uuid(u) => u.to_string(),
        SqlValue::Decimal(d) => d.to_string(),
        SqlValue::DateTime(dt) => dt.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
        SqlValue::DateTimeOffset(dto) => dto.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string(),
        SqlValue::Date(d) => d.format("%Y-%m-%d").to_string(),
        SqlValue::Time(t) => t.format("%H:%M:%S%.f").to_string(),
    }
}

/// Escape text for PostgreSQL COPY.
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

/// Format PostgreSQL type.
fn format_postgres_type(data_type: &str, max_length: i32, precision: i32, scale: i32) -> String {
    let lower = data_type.to_lowercase();
    match lower.as_str() {
        "numeric" | "decimal" => {
            if precision > 0 {
                format!("numeric({},{})", precision, scale)
            } else {
                "numeric".to_string()
            }
        }
        "varchar" | "character varying" => {
            if max_length > 0 {
                format!("varchar({})", max_length)
            } else {
                "text".to_string()
            }
        }
        "char" | "character" | "bpchar" => {
            if max_length > 0 {
                format!("char({})", max_length)
            } else {
                "char".to_string()
            }
        }
        _ => data_type.to_string(),
    }
}

/// Map referential action.
fn map_referential_action(action: &str) -> &str {
    match action.to_uppercase().as_str() {
        "CASCADE" => "CASCADE",
        "SET_NULL" => "SET NULL",
        "SET_DEFAULT" => "SET DEFAULT",
        "NO_ACTION" => "NO ACTION",
        _ => "NO ACTION",
    }
}

/// Convert check definition.
fn convert_check_definition(def: &str) -> String {
    let mut result = def.to_string();
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
    result = result.replace("getdate()", "CURRENT_TIMESTAMP");
    result = result.replace("GETDATE()", "CURRENT_TIMESTAMP");
    result
}

/// Certificate verifier.
#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_copy_text() {
        assert_eq!(escape_copy_text("hello"), "hello");
        assert_eq!(escape_copy_text("tab\there"), "tab\\there");
        assert_eq!(escape_copy_text("new\nline"), "new\\nline");
    }

    #[test]
    fn test_format_postgres_type() {
        assert_eq!(format_postgres_type("varchar", 255, 0, 0), "varchar(255)");
        assert_eq!(format_postgres_type("varchar", 0, 0, 0), "text");
        assert_eq!(format_postgres_type("numeric", 0, 10, 2), "numeric(10,2)");
    }

    /// Helper to extract NUMERIC header from encoded buffer
    fn parse_numeric_header(buf: &[u8]) -> (i32, i16, i16, i16, i16, Vec<i16>) {
        use bytes::Buf;
        let mut cursor = std::io::Cursor::new(buf);
        let len = cursor.get_i32();
        let ndigits = cursor.get_i16();
        let weight = cursor.get_i16();
        let sign = cursor.get_i16();
        let dscale = cursor.get_i16();
        let mut digits = Vec::new();
        for _ in 0..ndigits {
            digits.push(cursor.get_i16());
        }
        (len, ndigits, weight, sign, dscale, digits)
    }

    #[test]
    fn test_encode_decimal_binary_zero() {
        let mut buf = BytesMut::new();
        let d = rust_decimal::Decimal::ZERO;
        encode_decimal_binary(&mut buf, &d);

        let (len, ndigits, weight, sign, dscale, digits) = parse_numeric_header(&buf);
        assert_eq!(len, 8); // Header only
        assert_eq!(ndigits, 0);
        assert_eq!(weight, 0);
        assert_eq!(sign, 0x0000); // Positive
        assert_eq!(dscale, 0);
        assert!(digits.is_empty());
    }

    #[test]
    fn test_encode_decimal_binary_simple_integer() {
        let mut buf = BytesMut::new();
        let d: rust_decimal::Decimal = "12345".parse().unwrap();
        encode_decimal_binary(&mut buf, &d);

        let (len, ndigits, weight, sign, dscale, digits) = parse_numeric_header(&buf);
        assert_eq!(len, 8 + 4); // Header + 2 digits
        assert_eq!(ndigits, 2); // "1" and "2345" in base-10000
        assert_eq!(weight, 1); // First digit in 10000s place
        assert_eq!(sign, 0x0000); // Positive
        assert_eq!(dscale, 0);
        assert_eq!(digits, vec![1, 2345]);
    }

    #[test]
    fn test_encode_decimal_binary_with_fraction() {
        let mut buf = BytesMut::new();
        let d: rust_decimal::Decimal = "123.45".parse().unwrap();
        encode_decimal_binary(&mut buf, &d);

        let (len, ndigits, weight, sign, dscale, digits) = parse_numeric_header(&buf);
        assert_eq!(ndigits, 2); // "123" and "4500" in base-10000
        assert_eq!(weight, 0); // First digit in 1s place (0-9999)
        assert_eq!(sign, 0x0000); // Positive
        assert_eq!(dscale, 2);
        assert_eq!(digits, vec![123, 4500]);
    }

    #[test]
    fn test_encode_decimal_binary_negative() {
        let mut buf = BytesMut::new();
        let d: rust_decimal::Decimal = "-456.78".parse().unwrap();
        encode_decimal_binary(&mut buf, &d);

        let (_len, ndigits, weight, sign, dscale, digits) = parse_numeric_header(&buf);
        assert_eq!(ndigits, 2);
        assert_eq!(weight, 0);
        assert_eq!(sign, 0x4000); // Negative
        assert_eq!(dscale, 2);
        assert_eq!(digits, vec![456, 7800]);
    }

    #[test]
    fn test_encode_decimal_binary_small_fraction() {
        // 0.01 should be: weight=-1, digits=[100]
        let mut buf = BytesMut::new();
        let d: rust_decimal::Decimal = "0.01".parse().unwrap();
        encode_decimal_binary(&mut buf, &d);

        let (_len, ndigits, weight, sign, dscale, digits) = parse_numeric_header(&buf);
        assert_eq!(ndigits, 1);
        assert_eq!(weight, -1); // First digit in 0.0001-0.9999 range
        assert_eq!(sign, 0x0000);
        assert_eq!(dscale, 2);
        assert_eq!(digits, vec![100]); // 0.01 = 100/10000
    }

    #[test]
    fn test_encode_decimal_binary_very_small() {
        // 0.0001 should be: weight=-1, digits=[1]
        let mut buf = BytesMut::new();
        let d: rust_decimal::Decimal = "0.0001".parse().unwrap();
        encode_decimal_binary(&mut buf, &d);

        let (_len, ndigits, weight, _sign, dscale, digits) = parse_numeric_header(&buf);
        assert_eq!(ndigits, 1);
        assert_eq!(weight, -1);
        assert_eq!(dscale, 4);
        assert_eq!(digits, vec![1]); // 0.0001 = 1/10000
    }

    #[test]
    fn test_encode_decimal_binary_tiny() {
        // 0.0000000001 should be: weight=-3, digits=[100]
        let mut buf = BytesMut::new();
        let d: rust_decimal::Decimal = "0.0000000001".parse().unwrap();
        encode_decimal_binary(&mut buf, &d);

        let (_len, ndigits, weight, _sign, dscale, digits) = parse_numeric_header(&buf);
        assert_eq!(ndigits, 1);
        assert_eq!(weight, -3); // 10000^-3 range
        assert_eq!(dscale, 10);
        assert_eq!(digits, vec![100]); // 0.0000000001 = 100 * 10000^-3
    }

    #[test]
    fn test_encode_decimal_binary_large() {
        // Large number: 12345678901234
        let mut buf = BytesMut::new();
        let d: rust_decimal::Decimal = "12345678901234".parse().unwrap();
        encode_decimal_binary(&mut buf, &d);

        let (_len, ndigits, weight, _sign, dscale, digits) = parse_numeric_header(&buf);
        assert_eq!(ndigits, 4);
        assert_eq!(weight, 3); // 10000^3 = 10^12 range
        assert_eq!(dscale, 0);
        // 12345678901234 = 12*10000^3 + 3456*10000^2 + 7890*10000^1 + 1234*10000^0
        assert_eq!(digits, vec![12, 3456, 7890, 1234]);
    }
}
