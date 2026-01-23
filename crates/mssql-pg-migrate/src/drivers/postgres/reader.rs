//! PostgreSQL source reader implementation.
//!
//! Implements the `SourceReader` trait for reading data from PostgreSQL databases.
//! Uses deadpool-postgres for connection pooling and supports binary COPY protocol.

use std::borrow::Cow;
use std::sync::Arc;

use async_trait::async_trait;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use futures::StreamExt;
use rustls::ClientConfig;
use tokio::sync::mpsc;
use tokio_postgres::Config as PgConfig;
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{debug, info, warn};

use crate::config::SourceConfig;
use crate::core::schema::{CheckConstraint, Column, ForeignKey, Index, Partition, Table};
use crate::core::traits::{ReadOptions, SourceReader};
use crate::core::value::{Batch, SqlNullType, SqlValue};
use crate::error::{MigrateError, Result};
use crate::source::BinaryRowParser;

/// PostgreSQL source reader implementation.
pub struct PostgresReader {
    pool: Pool,
}

impl PostgresReader {
    /// Create a new PostgreSQL reader from configuration.
    pub async fn new(config: &SourceConfig, max_conns: usize) -> Result<Self> {
        let mut pg_config = PgConfig::new();
        pg_config.host(&config.host);
        pg_config.port(config.port);
        pg_config.dbname(&config.database);
        pg_config.user(&config.user);
        pg_config.password(&config.password);

        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };

        let ssl_mode = "require";
        let pool = match ssl_mode.to_lowercase().as_str() {
            "disable" => {
                warn!("PostgreSQL TLS is disabled. Credentials will be transmitted in plaintext.");
                let mgr = Manager::from_config(pg_config, tokio_postgres::NoTls, mgr_config);
                Pool::builder(mgr)
                    .max_size(max_conns)
                    .build()
                    .map_err(|e| MigrateError::pool(e, "creating PostgreSQL source pool"))?
            }
            _ => {
                let tls_config = Self::build_tls_config(ssl_mode)?;
                let tls_connector = MakeRustlsConnect::new(tls_config);
                let mgr = Manager::from_config(pg_config, tls_connector, mgr_config);
                Pool::builder(mgr)
                    .max_size(max_conns)
                    .build()
                    .map_err(|e| MigrateError::pool(e, "creating PostgreSQL source pool"))?
            }
        };

        // Test connection
        let client = pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "testing PostgreSQL source connection"))?;
        client.simple_query("SELECT 1").await?;

        info!(
            "Connected to PostgreSQL source: {}:{}/{}",
            config.host, config.port, config.database
        );

        Ok(Self { pool })
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

    /// Load columns for a table.
    async fn load_columns(&self, table: &mut Table) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting connection for load_columns"))?;

        let query = r#"
            SELECT
                column_name,
                udt_name,
                COALESCE(character_maximum_length, 0)::int4,
                COALESCE(numeric_precision, 0)::int4,
                COALESCE(numeric_scale, 0)::int4,
                CASE WHEN is_nullable = 'YES' THEN true ELSE false END,
                COALESCE(
                    (SELECT true FROM pg_catalog.pg_class c
                     JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                     WHERE n.nspname = columns.table_schema
                       AND c.relname = columns.table_name
                       AND a.attname = columns.column_name
                       AND a.attidentity IN ('a', 'd')),
                    false
                ) AS is_identity,
                ordinal_position::int4
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
        "#;

        let rows = client.query(query, &[&table.schema, &table.name]).await?;

        for row in rows {
            let col = Column {
                name: row.get::<_, String>(0),
                data_type: row.get::<_, String>(1),
                max_length: row.get::<_, i32>(2),
                precision: row.get::<_, i32>(3),
                scale: row.get::<_, i32>(4),
                is_nullable: row.get::<_, bool>(5),
                is_identity: row.get::<_, bool>(6),
                ordinal_pos: row.get::<_, i32>(7),
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
    async fn load_primary_key(&self, table: &mut Table) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting connection for load_primary_key"))?;

        let query = r#"
            SELECT a.attname
            FROM pg_catalog.pg_constraint c
            JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid
            WHERE n.nspname = $1
              AND t.relname = $2
              AND c.contype = 'p'
              AND a.attnum = ANY(c.conkey)
            ORDER BY array_position(c.conkey, a.attnum)
        "#;

        let rows = client.query(query, &[&table.schema, &table.name]).await?;

        for row in rows {
            let col_name: String = row.get(0);
            table.primary_key.push(col_name.clone());

            if let Some(col) = table.columns.iter().find(|c| c.name == col_name) {
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

    /// Load row count for a table.
    async fn load_row_count(&self, table: &mut Table) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting connection for load_row_count"))?;

        let query = r#"
            SELECT COALESCE(c.reltuples, 0)::int8
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 AND c.relname = $2
        "#;

        let row = client
            .query_one(query, &[&table.schema, &table.name])
            .await?;
        table.row_count = row.get::<_, i64>(0);

        debug!("Row count for {}: {}", table.full_name(), table.row_count);
        Ok(())
    }

    /// Read rows using PostgreSQL COPY TO BINARY format.
    #[allow(clippy::too_many_arguments)]
    pub async fn copy_rows_binary(
        &self,
        schema: &str,
        table: &str,
        columns: &[String],
        col_types: &[String],
        pk_col: Option<&str>,
        min_pk: Option<i64>,
        max_pk: Option<i64>,
        tx: mpsc::Sender<Vec<Vec<SqlValue<'static>>>>,
        batch_size: usize,
    ) -> Result<i64> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting connection for copy_rows_binary"))?;

        let query = build_copy_query(schema, table, columns, pk_col, min_pk, max_pk);
        debug!("COPY query: {}", query);

        let copy_stream = client.copy_out(&query).await.map_err(|e| {
            MigrateError::transfer(
                format!("{}.{}", schema, table),
                format!("initiating COPY: {}", e),
            )
        })?;

        let mut parser = BinaryRowParser::from_type_names(col_types);
        let mut batch = Vec::with_capacity(batch_size);
        let mut total_rows = 0i64;

        tokio::pin!(copy_stream);

        while let Some(data) = copy_stream.next().await {
            let bytes = data.map_err(|e| {
                MigrateError::transfer(
                    format!("{}.{}", schema, table),
                    format!("reading COPY data: {}", e),
                )
            })?;

            parser.extend(&bytes);

            while let Some(row) = parser.next_row()? {
                // Convert old SqlValue to new SqlValue
                let converted_row: Vec<SqlValue<'static>> =
                    row.into_iter().map(convert_old_sql_value).collect();
                batch.push(converted_row);
                total_rows += 1;

                if batch.len() >= batch_size {
                    if tx.send(std::mem::take(&mut batch)).await.is_err() {
                        return Err(MigrateError::transfer(
                            format!("{}.{}", schema, table),
                            "channel closed while sending COPY batch".to_string(),
                        ));
                    }
                    batch = Vec::with_capacity(batch_size);
                }
            }
        }

        if !batch.is_empty() && tx.send(batch).await.is_err() {
            return Err(MigrateError::transfer(
                format!("{}.{}", schema, table),
                "channel closed while sending final COPY batch".to_string(),
            ));
        }

        Ok(total_rows)
    }
}

#[async_trait]
impl SourceReader for PostgresReader {
    async fn extract_schema(&self, schema: &str) -> Result<Vec<Table>> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting connection for extract_schema"))?;

        let query = r#"
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema = $1
            ORDER BY table_name
        "#;

        let rows = client.query(query, &[&schema]).await?;

        let mut tables = Vec::new();
        for row in rows {
            let mut table = Table {
                schema: row.get::<_, String>(0),
                name: row.get::<_, String>(1),
                columns: Vec::new(),
                primary_key: Vec::new(),
                pk_columns: Vec::new(),
                row_count: 0,
                estimated_row_size: 0,
                indexes: Vec::new(),
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
            };

            self.load_columns(&mut table).await?;
            self.load_primary_key(&mut table).await?;
            self.load_row_count(&mut table).await?;

            table.estimated_row_size = table.columns.iter().map(estimate_column_size).sum();
            tables.push(table);
        }

        info!("Extracted {} tables from schema '{}'", tables.len(), schema);
        Ok(tables)
    }

    async fn load_indexes(&self, table: &mut Table) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting connection for load_indexes"))?;

        let query = r#"
            SELECT
                i.relname AS index_name,
                ix.indisunique,
                am.amname = 'btree' AND ix.indisclustered AS is_clustered,
                array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) AS columns
            FROM pg_catalog.pg_index ix
            JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
            JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_catalog.pg_am am ON am.oid = i.relam
            JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE n.nspname = $1
              AND t.relname = $2
              AND NOT ix.indisprimary
            GROUP BY i.relname, ix.indisunique, am.amname, ix.indisclustered
            ORDER BY i.relname
        "#;

        let rows = client.query(query, &[&table.schema, &table.name]).await?;

        for row in rows {
            let columns: Vec<String> = row.get(3);
            let index = Index {
                name: row.get(0),
                is_unique: row.get(1),
                is_clustered: row.get(2),
                columns,
                include_cols: Vec::new(),
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
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting connection for load_foreign_keys"))?;

        let query = r#"
            SELECT
                c.conname AS fk_name,
                array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum)) AS columns,
                rn.nspname AS ref_schema,
                rt.relname AS ref_table,
                array_agg(ra.attname ORDER BY array_position(c.confkey, ra.attnum)) AS ref_columns,
                CASE c.confdeltype
                    WHEN 'a' THEN 'NO_ACTION'
                    WHEN 'r' THEN 'RESTRICT'
                    WHEN 'c' THEN 'CASCADE'
                    WHEN 'n' THEN 'SET_NULL'
                    WHEN 'd' THEN 'SET_DEFAULT'
                    ELSE 'NO_ACTION'
                END AS on_delete,
                CASE c.confupdtype
                    WHEN 'a' THEN 'NO_ACTION'
                    WHEN 'r' THEN 'RESTRICT'
                    WHEN 'c' THEN 'CASCADE'
                    WHEN 'n' THEN 'SET_NULL'
                    WHEN 'd' THEN 'SET_DEFAULT'
                    ELSE 'NO_ACTION'
                END AS on_update
            FROM pg_catalog.pg_constraint c
            JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_catalog.pg_class rt ON rt.oid = c.confrelid
            JOIN pg_catalog.pg_namespace rn ON rn.oid = rt.relnamespace
            JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
            JOIN pg_catalog.pg_attribute ra ON ra.attrelid = rt.oid AND ra.attnum = ANY(c.confkey)
            WHERE n.nspname = $1
              AND t.relname = $2
              AND c.contype = 'f'
            GROUP BY c.conname, rn.nspname, rt.relname, c.confdeltype, c.confupdtype
            ORDER BY c.conname
        "#;

        let rows = client.query(query, &[&table.schema, &table.name]).await?;

        for row in rows {
            let fk = ForeignKey {
                name: row.get(0),
                columns: row.get(1),
                ref_schema: row.get(2),
                ref_table: row.get(3),
                ref_columns: row.get(4),
                on_delete: row.get(5),
                on_update: row.get(6),
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
        let client =
            self.pool.get().await.map_err(|e| {
                MigrateError::pool(e, "getting connection for load_check_constraints")
            })?;

        let query = r#"
            SELECT c.conname, pg_get_constraintdef(c.oid)
            FROM pg_catalog.pg_constraint c
            JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = $1 AND t.relname = $2 AND c.contype = 'c'
            ORDER BY c.conname
        "#;

        let rows = client.query(query, &[&table.schema, &table.name]).await?;

        for row in rows {
            let constraint = CheckConstraint {
                name: row.get(0),
                definition: row.get(1),
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
        let client =
            self.pool.get().await.map_err(|e| {
                MigrateError::pool(e, "getting connection for partition boundaries")
            })?;

        let query = format!(
            r#"
            WITH numbered AS (
                SELECT CAST({pk} AS BIGINT) AS pk_val,
                       NTILE({n}) OVER (ORDER BY {pk}) as partition_id
                FROM {schema}.{table}
            )
            SELECT partition_id::int8,
                   MIN(pk_val)::int8 as min_pk,
                   MAX(pk_val)::int8 as max_pk,
                   COUNT(*)::int8 as row_count
            FROM numbered
            GROUP BY partition_id
            ORDER BY partition_id
            "#,
            pk = quote_ident(pk_col),
            n = num_partitions,
            schema = quote_ident(&table.schema),
            table = quote_ident(&table.name)
        );

        let rows = client.query(&query, &[]).await?;

        let mut partitions = Vec::new();
        for row in rows {
            let partition_id: i64 = row.get(0);
            let min_pk: Option<i64> = row.try_get(1).ok();
            let max_pk: Option<i64> = row.try_get(2).ok();
            let row_count: i64 = row.get(3);

            let partition = Partition {
                table_name: table.full_name(),
                partition_id: partition_id as i32,
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
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting connection for get_row_count"))?;

        let query = format!(
            "SELECT COUNT(*)::int8 FROM {}.{}",
            quote_ident(schema),
            quote_ident(table)
        );

        let row = client.query_one(&query, &[]).await?;
        Ok(row.get::<_, i64>(0))
    }

    async fn get_max_pk(&self, schema: &str, table: &str, pk_col: &str) -> Result<i64> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting connection for get_max_pk"))?;

        let query = format!(
            "SELECT COALESCE(MAX({})::bigint, 0) FROM {}.{}",
            quote_ident(pk_col),
            quote_ident(schema),
            quote_ident(table)
        );

        let row = client.query_one(&query, &[]).await?;
        Ok(row.get::<_, i64>(0))
    }

    fn db_type(&self) -> &str {
        "postgres"
    }

    async fn close(&self) {
        // deadpool handles cleanup automatically
    }

    fn supports_direct_copy(&self) -> bool {
        true
    }
}

/// Internal function to read table rows.
async fn read_table_internal(
    pool: Pool,
    opts: ReadOptions,
    tx: mpsc::Sender<Result<Batch>>,
) -> Result<()> {
    let client = pool
        .get()
        .await
        .map_err(|e| MigrateError::pool(e, "getting connection for read_table"))?;

    // Build SELECT query
    let cols = opts
        .columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    let mut sql = format!(
        "SELECT {} FROM {}.{}",
        cols,
        quote_ident(&opts.schema),
        quote_ident(&opts.table)
    );

    let mut conditions = Vec::new();

    if let (Some(pk_idx), Some(min_pk)) = (opts.pk_idx, opts.min_pk) {
        let pk_col = &opts.columns[pk_idx];
        conditions.push(format!("{} > {}", quote_ident(pk_col), min_pk));
    }

    if let (Some(pk_idx), Some(max_pk)) = (opts.pk_idx, opts.max_pk) {
        let pk_col = &opts.columns[pk_idx];
        conditions.push(format!("{} <= {}", quote_ident(pk_col), max_pk));
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
        sql.push_str(&format!(" ORDER BY {}", quote_ident(pk_col)));
    }

    sql.push_str(&format!(" LIMIT {}", opts.batch_size));

    // Execute query
    let rows = client.query(&sql, &[]).await?;

    if rows.is_empty() {
        let _ = tx.send(Ok(Batch::empty_final())).await;
        return Ok(());
    }

    let mut batch_rows = Vec::with_capacity(rows.len());

    for row in rows {
        let mut values = Vec::with_capacity(opts.col_types.len());
        for (idx, data_type) in opts.col_types.iter().enumerate() {
            let value = convert_pg_row_value(&row, idx, data_type);
            values.push(value);
        }
        batch_rows.push(values);
    }

    let is_last = batch_rows.len() < opts.batch_size;
    let mut batch = Batch::new(batch_rows);
    if is_last {
        batch = batch.mark_final();
    }

    let _ = tx.send(Ok(batch)).await;
    Ok(())
}

/// Quote a PostgreSQL identifier.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Build a PostgreSQL COPY query.
fn build_copy_query(
    schema: &str,
    table: &str,
    columns: &[String],
    pk_col: Option<&str>,
    min_pk: Option<i64>,
    max_pk: Option<i64>,
) -> String {
    let col_list = columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    let table_ref = format!("{}.{}", quote_ident(schema), quote_ident(table));

    match (pk_col, min_pk, max_pk) {
        (Some(pk), Some(min), Some(max)) => {
            format!(
                "COPY (SELECT {} FROM {} WHERE {} >= {} AND {} <= {} ORDER BY {}) TO STDOUT (FORMAT BINARY)",
                col_list, table_ref, quote_ident(pk), min, quote_ident(pk), max, quote_ident(pk)
            )
        }
        (Some(pk), Some(min), None) => {
            format!(
                "COPY (SELECT {} FROM {} WHERE {} >= {} ORDER BY {}) TO STDOUT (FORMAT BINARY)",
                col_list,
                table_ref,
                quote_ident(pk),
                min,
                quote_ident(pk)
            )
        }
        (Some(pk), None, Some(max)) => {
            format!(
                "COPY (SELECT {} FROM {} WHERE {} <= {} ORDER BY {}) TO STDOUT (FORMAT BINARY)",
                col_list,
                table_ref,
                quote_ident(pk),
                max,
                quote_ident(pk)
            )
        }
        _ => {
            format!(
                "COPY (SELECT {} FROM {}) TO STDOUT (FORMAT BINARY)",
                col_list, table_ref
            )
        }
    }
}

/// Estimate the size of a PostgreSQL column.
fn estimate_column_size(col: &Column) -> i64 {
    match col.data_type.as_str() {
        "int2" | "smallint" => 2,
        "int4" | "integer" | "int" => 4,
        "int8" | "bigint" => 8,
        "float4" | "real" => 4,
        "float8" | "double precision" => 8,
        "bool" | "boolean" => 1,
        "uuid" => 16,
        "date" => 4,
        "time" | "timetz" => 8,
        "timestamp" | "timestamptz" => 8,
        "varchar" | "character varying" | "text" | "char" | "character" => {
            if col.max_length > 0 {
                col.max_length.min(100) as i64
            } else {
                100
            }
        }
        "bytea" => 100,
        "numeric" | "decimal" => 16,
        "json" | "jsonb" => 100,
        _ => 8,
    }
}

/// Convert a PostgreSQL row value to SqlValue.
fn convert_pg_row_value(
    row: &tokio_postgres::Row,
    idx: usize,
    data_type: &str,
) -> SqlValue<'static> {
    let dt = data_type.to_lowercase();

    match dt.as_str() {
        "bool" | "boolean" => row
            .try_get::<_, bool>(idx)
            .ok()
            .map(SqlValue::Bool)
            .unwrap_or(SqlValue::Null(SqlNullType::Bool)),
        "int2" | "smallint" => row
            .try_get::<_, i16>(idx)
            .ok()
            .map(SqlValue::I16)
            .unwrap_or(SqlValue::Null(SqlNullType::I16)),
        "int4" | "integer" | "int" => row
            .try_get::<_, i32>(idx)
            .ok()
            .map(SqlValue::I32)
            .unwrap_or(SqlValue::Null(SqlNullType::I32)),
        "int8" | "bigint" => row
            .try_get::<_, i64>(idx)
            .ok()
            .map(SqlValue::I64)
            .unwrap_or(SqlValue::Null(SqlNullType::I64)),
        "float4" | "real" => row
            .try_get::<_, f32>(idx)
            .ok()
            .map(SqlValue::F32)
            .unwrap_or(SqlValue::Null(SqlNullType::F32)),
        "float8" | "double precision" => row
            .try_get::<_, f64>(idx)
            .ok()
            .map(SqlValue::F64)
            .unwrap_or(SqlValue::Null(SqlNullType::F64)),
        "uuid" => row
            .try_get::<_, uuid::Uuid>(idx)
            .ok()
            .map(SqlValue::Uuid)
            .unwrap_or(SqlValue::Null(SqlNullType::Uuid)),
        "timestamp" | "timestamp without time zone" => row
            .try_get::<_, chrono::NaiveDateTime>(idx)
            .ok()
            .map(SqlValue::DateTime)
            .unwrap_or(SqlValue::Null(SqlNullType::DateTime)),
        "timestamptz" | "timestamp with time zone" => row
            .try_get::<_, chrono::DateTime<chrono::FixedOffset>>(idx)
            .ok()
            .map(SqlValue::DateTimeOffset)
            .unwrap_or(SqlValue::Null(SqlNullType::DateTimeOffset)),
        "date" => row
            .try_get::<_, chrono::NaiveDate>(idx)
            .ok()
            .map(SqlValue::Date)
            .unwrap_or(SqlValue::Null(SqlNullType::Date)),
        "time" | "time without time zone" => row
            .try_get::<_, chrono::NaiveTime>(idx)
            .ok()
            .map(SqlValue::Time)
            .unwrap_or(SqlValue::Null(SqlNullType::Time)),
        "bytea" => row
            .try_get::<_, Vec<u8>>(idx)
            .ok()
            .map(|b| SqlValue::Bytes(Cow::Owned(b)))
            .unwrap_or(SqlValue::Null(SqlNullType::Bytes)),
        "numeric" | "decimal" => row
            .try_get::<_, rust_decimal::Decimal>(idx)
            .ok()
            .map(SqlValue::Decimal)
            .unwrap_or(SqlValue::Null(SqlNullType::Decimal)),
        "json" | "jsonb" => row
            .try_get::<_, serde_json::Value>(idx)
            .ok()
            .map(|v| SqlValue::Text(Cow::Owned(v.to_string())))
            .unwrap_or(SqlValue::Null(SqlNullType::String)),
        _ => row
            .try_get::<_, String>(idx)
            .ok()
            .map(|s| SqlValue::Text(Cow::Owned(s)))
            .unwrap_or(SqlValue::Null(SqlNullType::String)),
    }
}

/// Convert old SqlValue to new SqlValue.
fn convert_old_sql_value(old: crate::target::SqlValue) -> SqlValue<'static> {
    use crate::target::SqlNullType as OldNullType;
    use crate::target::SqlValue as OldSqlValue;

    match old {
        OldSqlValue::Null(null_type) => {
            let new_null = match null_type {
                OldNullType::Bool => SqlNullType::Bool,
                OldNullType::I16 => SqlNullType::I16,
                OldNullType::I32 => SqlNullType::I32,
                OldNullType::I64 => SqlNullType::I64,
                OldNullType::F32 => SqlNullType::F32,
                OldNullType::F64 => SqlNullType::F64,
                OldNullType::String => SqlNullType::String,
                OldNullType::Bytes => SqlNullType::Bytes,
                OldNullType::Uuid => SqlNullType::Uuid,
                OldNullType::Decimal => SqlNullType::Decimal,
                OldNullType::DateTime => SqlNullType::DateTime,
                OldNullType::DateTimeOffset => SqlNullType::DateTimeOffset,
                OldNullType::Date => SqlNullType::Date,
                OldNullType::Time => SqlNullType::Time,
            };
            SqlValue::Null(new_null)
        }
        OldSqlValue::Bool(b) => SqlValue::Bool(b),
        OldSqlValue::I16(i) => SqlValue::I16(i),
        OldSqlValue::I32(i) => SqlValue::I32(i),
        OldSqlValue::I64(i) => SqlValue::I64(i),
        OldSqlValue::F32(f) => SqlValue::F32(f),
        OldSqlValue::F64(f) => SqlValue::F64(f),
        OldSqlValue::String(s) => SqlValue::Text(Cow::Owned(s)),
        OldSqlValue::Bytes(b) => SqlValue::Bytes(Cow::Owned(b)),
        OldSqlValue::Uuid(u) => SqlValue::Uuid(u),
        OldSqlValue::Decimal(d) => SqlValue::Decimal(d),
        OldSqlValue::DateTime(dt) => SqlValue::DateTime(dt),
        OldSqlValue::DateTimeOffset(dto) => SqlValue::DateTimeOffset(dto),
        OldSqlValue::Date(d) => SqlValue::Date(d),
        OldSqlValue::Time(t) => SqlValue::Time(t),
    }
}

/// Certificate verifier that accepts any certificate.
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
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_ident() {
        assert_eq!(quote_ident("users"), "\"users\"");
        assert_eq!(quote_ident("user\"table"), "\"user\"\"table\"");
    }

    #[test]
    fn test_estimate_column_size() {
        let col = Column {
            name: "id".to_string(),
            data_type: "int4".to_string(),
            max_length: 0,
            precision: 0,
            scale: 0,
            is_nullable: false,
            is_identity: false,
            ordinal_pos: 1,
        };
        assert_eq!(estimate_column_size(&col), 4);
    }
}
