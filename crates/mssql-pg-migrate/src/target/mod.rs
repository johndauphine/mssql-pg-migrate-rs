//! PostgreSQL target database operations.

use crate::error::{MigrateError, Result};
use crate::source::{CheckConstraint, ForeignKey, Index, Table};
use crate::typemap::mssql_to_postgres;
use async_trait::async_trait;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::{types::ToSql, Config as PgConfig, NoTls};
use tracing::{debug, info};

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
    Null,
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

/// PostgreSQL target pool implementation.
pub struct PgPool {
    pool: Pool,
    config: crate::config::TargetConfig,
}

impl PgPool {
    /// Create a new PostgreSQL target pool.
    pub async fn new(config: &crate::config::TargetConfig, max_conns: usize) -> Result<Self> {
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
            .map_err(|e| MigrateError::Pool(format!("Failed to create pool: {}", e)))?;

        // Test connection
        let client = pool
            .get()
            .await
            .map_err(|e| MigrateError::Pool(format!("Failed to get connection: {}", e)))?;

        client
            .simple_query("SELECT 1")
            .await
            .map_err(|e| MigrateError::Target(e))?;

        info!(
            "Connected to PostgreSQL: {}:{}/{}",
            config.host, config.port, config.database
        );

        Ok(Self {
            pool,
            config: config.clone(),
        })
    }

    /// Generate DDL for table creation.
    fn generate_ddl(&self, table: &Table, target_schema: &str, unlogged: bool) -> String {
        let unlogged_str = if unlogged { "UNLOGGED " } else { "" };

        let mut ddl = format!(
            "CREATE {}TABLE \"{}\".\"{}\" (\n",
            unlogged_str, target_schema, table.name
        );

        for (i, col) in table.columns.iter().enumerate() {
            let pg_type = mssql_to_postgres(&col.data_type, col.max_length, col.precision, col.scale);

            let nullable = if col.is_nullable { "" } else { " NOT NULL" };

            let identity = if col.is_identity {
                " GENERATED BY DEFAULT AS IDENTITY"
            } else {
                ""
            };

            ddl.push_str(&format!("    \"{}\" {}{}{}", col.name, pg_type, nullable, identity));

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
        format!("\"{}\".\"{}\"", schema, table)
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
            .map_err(|e| MigrateError::Pool(e.to_string()))?;

        let sql = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", schema);
        client
            .execute(&sql, &[])
            .await
            .map_err(|e| MigrateError::Target(e))?;

        debug!("Created schema '{}'", schema);
        Ok(())
    }

    async fn create_table(&self, table: &Table, target_schema: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::Pool(e.to_string()))?;

        let ddl = self.generate_ddl(table, target_schema, false);
        client
            .execute(&ddl, &[])
            .await
            .map_err(|e| MigrateError::Target(e))?;

        debug!("Created table {}.{}", target_schema, table.name);
        Ok(())
    }

    async fn create_table_unlogged(&self, table: &Table, target_schema: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::Pool(e.to_string()))?;

        let ddl = self.generate_ddl(table, target_schema, true);
        client
            .execute(&ddl, &[])
            .await
            .map_err(|e| MigrateError::Target(e))?;

        debug!("Created UNLOGGED table {}.{}", target_schema, table.name);
        Ok(())
    }

    async fn drop_table(&self, schema: &str, table: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::Pool(e.to_string()))?;

        let sql = format!(
            "DROP TABLE IF EXISTS {} CASCADE",
            Self::qualify_table(schema, table)
        );
        client
            .execute(&sql, &[])
            .await
            .map_err(|e| MigrateError::Target(e))?;

        debug!("Dropped table {}.{}", schema, table);
        Ok(())
    }

    async fn truncate_table(&self, schema: &str, table: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::Pool(e.to_string()))?;

        let sql = format!("TRUNCATE TABLE {}", Self::qualify_table(schema, table));
        client
            .execute(&sql, &[])
            .await
            .map_err(|e| MigrateError::Target(e))?;

        debug!("Truncated table {}.{}", schema, table);
        Ok(())
    }

    async fn table_exists(&self, schema: &str, table: &str) -> Result<bool> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::Pool(e.to_string()))?;

        let row = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2
                )",
                &[&schema, &table],
            )
            .await
            .map_err(|e| MigrateError::Target(e))?;

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
            .map_err(|e| MigrateError::Pool(e.to_string()))?;

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
            .map_err(|e| MigrateError::Target(e))?;

        debug!("Created primary key for {}.{}", target_schema, table.name);
        Ok(())
    }

    async fn create_index(
        &self,
        table: &Table,
        idx: &Index,
        target_schema: &str,
    ) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::Pool(e.to_string()))?;

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
            .map_err(|e| MigrateError::Target(e))?;

        debug!("Created index {} for {}.{}", idx_name, target_schema, table.name);
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
            .map_err(|e| MigrateError::Pool(e.to_string()))?;

        let cols: Vec<String> = fk.columns.iter().map(|c| Self::quote_ident(c)).collect();
        let ref_cols: Vec<String> = fk.ref_columns.iter().map(|c| Self::quote_ident(c)).collect();

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
            .map_err(|e| MigrateError::Target(e))?;

        debug!("Created foreign key {} for {}.{}", fk_name, target_schema, table.name);
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
            .map_err(|e| MigrateError::Pool(e.to_string()))?;

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
            .map_err(|e| MigrateError::Target(e))?;

        debug!("Created check constraint {} for {}.{}", chk_name, target_schema, table.name);
        Ok(())
    }

    async fn has_primary_key(&self, schema: &str, table: &str) -> Result<bool> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::Pool(e.to_string()))?;

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
            .map_err(|e| MigrateError::Target(e))?;

        Ok(row.get(0))
    }

    async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::Pool(e.to_string()))?;

        let sql = format!("SELECT COUNT(*) FROM {}", Self::qualify_table(schema, table));
        let row = client
            .query_one(&sql, &[])
            .await
            .map_err(|e| MigrateError::Target(e))?;

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
            .map_err(|e| MigrateError::Pool(e.to_string()))?;

        // Get max value
        let sql = format!(
            "SELECT COALESCE(MAX({}), 0) FROM {}",
            Self::quote_ident(&identity_col.name),
            Self::qualify_table(schema, &table.name)
        );
        let row = client
            .query_one(&sql, &[])
            .await
            .map_err(|e| MigrateError::Target(e))?;
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
                .map_err(|e| MigrateError::Target(e))?;
        }

        debug!("Reset sequence for {}.{}.{}", schema, table.name, identity_col.name);
        Ok(())
    }

    async fn set_table_logged(&self, schema: &str, table: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::Pool(e.to_string()))?;

        let sql = format!(
            "ALTER TABLE {} SET LOGGED",
            Self::qualify_table(schema, table)
        );
        client
            .execute(&sql, &[])
            .await
            .map_err(|e| MigrateError::Target(e))?;

        debug!("Set table {}.{} to LOGGED", schema, table);
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

        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::Pool(e.to_string()))?;

        // Build column list
        let col_list: String = cols.iter().map(|c| Self::quote_ident(c)).collect::<Vec<_>>().join(", ");

        // Use COPY for bulk insert (text format for simplicity)
        let copy_stmt = format!(
            "COPY {}.\"{}\" ({}) FROM STDIN",
            Self::quote_ident(schema),
            table,
            col_list
        );

        let sink = client
            .copy_in(&copy_stmt)
            .await
            .map_err(|e| MigrateError::Target(e))?;

        let writer = tokio_postgres::binary_copy::BinaryCopyInWriter::new(
            sink,
            &[], // Will use text format instead
        );

        // For simplicity, we'll use a text-based COPY
        // In production, use binary format for better performance
        let mut data = String::new();
        for row in &rows {
            let values: Vec<String> = row.iter().map(|v| sql_value_to_copy_text(v)).collect();
            data.push_str(&values.join("\t"));
            data.push('\n');
        }

        // Actually, let's use simple INSERT for now (COPY requires more complex handling)
        // TODO: Implement proper COPY protocol for performance
        drop(writer);

        // Use batched INSERT instead
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

        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::Pool(e.to_string()))?;

        // PostgreSQL has a limit of ~65535 parameters per query
        const MAX_PARAMS: usize = 65000;
        let batch_size = MAX_PARAMS / cols.len();
        let batch_size = batch_size.max(1).min(rows.len());

        let mut total = 0u64;

        for chunk in rows.chunks(batch_size) {
            let (sql, params) = build_upsert_sql(schema, table, cols, pk_cols, chunk);

            let params_refs: Vec<&(dyn ToSql + Sync)> = params
                .iter()
                .map(|p| p.as_ref() as &(dyn ToSql + Sync))
                .collect();

            client
                .execute(&sql, &params_refs)
                .await
                .map_err(|e| MigrateError::Target(e))?;

            total += chunk.len() as u64;
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
    /// Insert rows using batched INSERT statements.
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

        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::Pool(e.to_string()))?;

        const MAX_PARAMS: usize = 65000;
        let batch_size = MAX_PARAMS / cols.len();
        let batch_size = batch_size.max(1).min(rows.len());

        let mut total = 0u64;

        for chunk in rows.chunks(batch_size) {
            let (sql, params) = build_insert_sql(schema, table, cols, chunk);

            let params_refs: Vec<&(dyn ToSql + Sync)> = params
                .iter()
                .map(|p| p.as_ref() as &(dyn ToSql + Sync))
                .collect();

            client
                .execute(&sql, &params_refs)
                .await
                .map_err(|e| MigrateError::Target(e))?;

            total += chunk.len() as u64;
        }

        Ok(total)
    }
}

/// Convert SqlValue to text format for COPY.
fn sql_value_to_copy_text(value: &SqlValue) -> String {
    match value {
        SqlValue::Null => "\\N".to_string(),
        SqlValue::Bool(b) => if *b { "t" } else { "f" }.to_string(),
        SqlValue::I16(n) => n.to_string(),
        SqlValue::I32(n) => n.to_string(),
        SqlValue::I64(n) => n.to_string(),
        SqlValue::F32(n) => n.to_string(),
        SqlValue::F64(n) => n.to_string(),
        SqlValue::String(s) => s.replace('\t', "\\t").replace('\n', "\\n"),
        SqlValue::Bytes(b) => format!("\\x{}", hex::encode(b)),
        SqlValue::Uuid(u) => u.to_string(),
        SqlValue::Decimal(d) => d.to_string(),
        SqlValue::DateTime(dt) => dt.to_string(),
        SqlValue::DateTimeOffset(dt) => dt.to_rfc3339(),
        SqlValue::Date(d) => d.to_string(),
        SqlValue::Time(t) => t.to_string(),
    }
}

/// Build INSERT SQL with parameters.
fn build_insert_sql(
    schema: &str,
    table: &str,
    cols: &[String],
    rows: &[Vec<SqlValue>],
) -> (String, Vec<Box<dyn ToSql + Sync + Send>>) {
    let col_list: String = cols
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");

    let mut placeholders = Vec::new();
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let mut idx = 1;

    for row in rows {
        let row_placeholders: Vec<String> = row
            .iter()
            .map(|_| {
                let p = format!("${}", idx);
                idx += 1;
                p
            })
            .collect();
        placeholders.push(format!("({})", row_placeholders.join(", ")));

        for value in row {
            params.push(sql_value_to_param(value));
        }
    }

    let sql = format!(
        "INSERT INTO \"{}\".\"{}\" ({}) VALUES {}",
        schema,
        table,
        col_list,
        placeholders.join(", ")
    );

    (sql, params)
}

/// Build UPSERT SQL with parameters.
fn build_upsert_sql(
    schema: &str,
    table: &str,
    cols: &[String],
    pk_cols: &[String],
    rows: &[Vec<SqlValue>],
) -> (String, Vec<Box<dyn ToSql + Sync + Send>>) {
    let col_list: String = cols
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");

    let pk_list: String = pk_cols
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");

    // Build UPDATE SET clause (exclude PK columns)
    let update_cols: Vec<String> = cols
        .iter()
        .filter(|c| !pk_cols.contains(c))
        .map(|c| format!("\"{}\" = EXCLUDED.\"{}\"", c, c))
        .collect();

    let mut placeholders = Vec::new();
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let mut idx = 1;

    for row in rows {
        let row_placeholders: Vec<String> = row
            .iter()
            .map(|_| {
                let p = format!("${}", idx);
                idx += 1;
                p
            })
            .collect();
        placeholders.push(format!("({})", row_placeholders.join(", ")));

        for value in row {
            params.push(sql_value_to_param(value));
        }
    }

    // Build change detection WHERE clause
    let change_detection: Vec<String> = cols
        .iter()
        .filter(|c| !pk_cols.contains(c))
        .map(|c| format!("\"{}\" IS DISTINCT FROM EXCLUDED.\"{}\"", c, c))
        .collect();

    let sql = if update_cols.is_empty() {
        // PK-only table, just INSERT ... ON CONFLICT DO NOTHING
        format!(
            "INSERT INTO \"{}\".\"{}\" ({}) VALUES {} ON CONFLICT ({}) DO NOTHING",
            schema,
            table,
            col_list,
            placeholders.join(", "),
            pk_list
        )
    } else {
        format!(
            "INSERT INTO \"{}\".\"{}\" ({}) VALUES {} ON CONFLICT ({}) DO UPDATE SET {} WHERE {}",
            schema,
            table,
            col_list,
            placeholders.join(", "),
            pk_list,
            update_cols.join(", "),
            change_detection.join(" OR ")
        )
    };

    (sql, params)
}

/// Convert SqlValue to a boxed ToSql parameter.
fn sql_value_to_param(value: &SqlValue) -> Box<dyn ToSql + Sync + Send> {
    match value {
        SqlValue::Null => Box::new(None::<i32>),
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
        SqlValue::DateTimeOffset(dt) => Box::new(*dt),
        SqlValue::Date(d) => Box::new(*d),
        SqlValue::Time(t) => Box::new(*t),
    }
}
