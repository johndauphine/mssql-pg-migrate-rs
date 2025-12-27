//! PostgreSQL target database operations.

use crate::error::{MigrateError, Result};
use crate::source::{CheckConstraint, ForeignKey, Index, PkValue, Table};
use crate::typemap::mssql_to_postgres;
use crate::verify::{CompositePk, CompositeRowHashMap, RowRange};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use chrono::Timelike;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use futures::SinkExt;
use md5::Md5;
use md5::Digest;
use rustls::ClientConfig;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_postgres::Config as PgConfig;
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{debug, info, warn};

/// Target buffer size for COPY operations (~64KB like Go pgx).
/// This is the threshold at which we flush the buffer to reduce syscalls
/// while keeping memory bounded.
const COPY_SEND_BUF_SIZE: usize = 65536 - 5; // Account for packet header

/// Number of buffer chunks to queue for concurrent network I/O.
const COPY_CHANNEL_SIZE: usize = 4;

/// Minimum row count for logging detailed upsert profiling information.
const UPSERT_PROFILING_ROW_THRESHOLD: u64 = 10_000;

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

    /// Drop all non-PK indexes on a table.
    async fn drop_non_pk_indexes(&self, schema: &str, table: &str) -> Result<Vec<String>>;

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
    /// `writer_id` is used to create a unique staging table per writer to avoid conflicts.
    /// `row_hash_column` is the configured column name for row hash detection (if present in cols).
    async fn upsert_chunk(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        pk_cols: &[String],
        rows: Vec<Vec<SqlValue>>,
        writer_id: usize,
        row_hash_column: Option<&str>,
    ) -> Result<u64>;

    /// Upsert a chunk of rows (used when hash-based change detection pre-filtered rows).
    /// This is identical to upsert_chunk but exists for API clarity - hash filtering
    /// reduces network transfer, while the DB-side WHERE clause prevents unnecessary writes.
    async fn upsert_chunk_with_hash(
        &self,
        schema: &str,
        table: &str,
        cols: &[String],
        pk_cols: &[String],
        rows: Vec<Vec<SqlValue>>,
        writer_id: usize,
        row_hash_column: Option<&str>,
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

/// Calculate MD5 hash of non-PK column values for change detection.
///
/// The hash is calculated using pipe-separated values with consistent formatting:
/// - NULL values are represented as `\N`
/// - Floats use fixed precision to ensure hash stability
/// - All types are converted to their string representation
///
/// Returns a 32-character lowercase hex string.
pub fn calculate_row_hash(row: &[SqlValue], pk_indices: &[usize]) -> String {
    let mut hasher = Md5::new();
    let mut first = true;

    for (idx, value) in row.iter().enumerate() {
        // Skip primary key columns - they identify the row, not the content
        if pk_indices.contains(&idx) {
            continue;
        }

        // Use pipe as separator between values
        if !first {
            hasher.update(b"|");
        }
        first = false;

        match value {
            SqlValue::Null(_) => hasher.update(b"\\N"),
            SqlValue::Bool(b) => hasher.update(if *b { b"t" } else { b"f" }),
            SqlValue::I16(n) => hasher.update(n.to_string().as_bytes()),
            SqlValue::I32(n) => hasher.update(n.to_string().as_bytes()),
            SqlValue::I64(n) => hasher.update(n.to_string().as_bytes()),
            SqlValue::F32(n) => hasher.update(format!("{:.6}", n).as_bytes()),
            SqlValue::F64(n) => hasher.update(format!("{:.15}", n).as_bytes()),
            SqlValue::String(s) => hasher.update(s.as_bytes()),
            SqlValue::Bytes(b) => hasher.update(b),
            SqlValue::Uuid(u) => hasher.update(u.to_string().as_bytes()),
            SqlValue::Decimal(d) => hasher.update(d.to_string().as_bytes()),
            SqlValue::DateTime(dt) => {
                hasher.update(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string().as_bytes())
            }
            SqlValue::DateTimeOffset(dt) => hasher.update(dt.to_rfc3339().as_bytes()),
            SqlValue::Date(d) => hasher.update(d.to_string().as_bytes()),
            SqlValue::Time(t) => hasher.update(t.to_string().as_bytes()),
        }
    }

    format!("{:x}", hasher.finalize())
}

/// Convert a PostgreSQL row value to SqlValue.
///
/// Used by fetch_rows_for_range to convert query results to a consistent type.
fn convert_pg_value(row: &tokio_postgres::Row, idx: usize) -> SqlValue {
    // Try each type in order of likelihood
    // Note: PostgreSQL doesn't have column type info in Row without metadata,
    // so we try types in a reasonable order

    // Integers
    if let Ok(v) = row.try_get::<_, i64>(idx) {
        return SqlValue::I64(v);
    }
    if let Ok(v) = row.try_get::<_, i32>(idx) {
        return SqlValue::I32(v);
    }
    if let Ok(v) = row.try_get::<_, i16>(idx) {
        return SqlValue::I16(v);
    }

    // String
    if let Ok(v) = row.try_get::<_, String>(idx) {
        return SqlValue::String(v);
    }

    // Boolean
    if let Ok(v) = row.try_get::<_, bool>(idx) {
        return SqlValue::Bool(v);
    }

    // Floats
    if let Ok(v) = row.try_get::<_, f64>(idx) {
        return SqlValue::F64(v);
    }
    if let Ok(v) = row.try_get::<_, f32>(idx) {
        return SqlValue::F32(v);
    }

    // UUID
    if let Ok(v) = row.try_get::<_, uuid::Uuid>(idx) {
        return SqlValue::Uuid(v);
    }

    // Decimal
    if let Ok(v) = row.try_get::<_, rust_decimal::Decimal>(idx) {
        return SqlValue::Decimal(v);
    }

    // Date/Time types
    if let Ok(v) = row.try_get::<_, chrono::NaiveDateTime>(idx) {
        return SqlValue::DateTime(v);
    }
    if let Ok(v) = row.try_get::<_, chrono::NaiveDate>(idx) {
        return SqlValue::Date(v);
    }
    if let Ok(v) = row.try_get::<_, chrono::NaiveTime>(idx) {
        return SqlValue::Time(v);
    }
    if let Ok(v) = row.try_get::<_, chrono::DateTime<chrono::FixedOffset>>(idx) {
        return SqlValue::DateTimeOffset(v);
    }

    // Binary
    if let Ok(v) = row.try_get::<_, Vec<u8>>(idx) {
        return SqlValue::Bytes(v);
    }

    // Check for NULL - try to get as Option<String> which works for most types
    if let Ok(None) = row.try_get::<_, Option<String>>(idx) {
        return SqlValue::Null(SqlNullType::String);
    }
    if let Ok(None) = row.try_get::<_, Option<i64>>(idx) {
        return SqlValue::Null(SqlNullType::I64);
    }

    // Default to null if we can't determine the type
    SqlValue::Null(SqlNullType::String)
}

/// Certificate verifier that accepts any certificate.
/// Used for ssl_mode=require which needs TLS but doesn't verify certificates.
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

/// PostgreSQL target pool implementation.
pub struct PgPool {
    pool: Pool,
    /// Rows per COPY buffer flush.
    copy_buffer_rows: usize,
    /// Use binary COPY format.
    use_binary_copy: bool,
}

impl PgPool {
    /// Create a new PostgreSQL target pool.
    pub async fn new(
        config: &crate::config::TargetConfig,
        max_conns: usize,
        copy_buffer_rows: usize,
        use_binary_copy: bool,
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

        // Create pool based on ssl_mode
        let pool = match config.ssl_mode.to_lowercase().as_str() {
            "disable" => {
                warn!("PostgreSQL TLS is disabled. Credentials will be transmitted in plaintext.");
                let mgr = Manager::from_config(pg_config, tokio_postgres::NoTls, mgr_config);
                Pool::builder(mgr).max_size(max_conns).build().map_err(|e| {
                    MigrateError::pool(e, "creating PostgreSQL connection pool")
                })?
            }
            ssl_mode => {
                // Build TLS config based on ssl_mode
                let tls_config = Self::build_tls_config(ssl_mode)?;
                let tls_connector = MakeRustlsConnect::new(tls_config);
                let mgr = Manager::from_config(pg_config, tls_connector, mgr_config);
                let pool = Pool::builder(mgr).max_size(max_conns).build().map_err(|e| {
                    MigrateError::pool(e, "creating PostgreSQL connection pool")
                })?;
                info!("PostgreSQL TLS enabled (ssl_mode={})", ssl_mode);
                pool
            }
        };

        // Test connection
        let client = pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "testing PostgreSQL connection"))?;

        client.simple_query("SELECT 1").await?;

        info!(
            "Connected to PostgreSQL: {}:{}/{}",
            config.host, config.port, config.database
        );

        Ok(Self {
            pool,
            copy_buffer_rows,
            use_binary_copy,
        })
    }

    /// Build TLS configuration based on ssl_mode.
    fn build_tls_config(ssl_mode: &str) -> Result<ClientConfig> {
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let config = match ssl_mode {
            "require" => {
                // TLS required but no certificate verification
                // This is less secure but matches PostgreSQL "require" semantics
                warn!(
                    "ssl_mode=require: TLS enabled but server certificate is not verified. \
                     Consider using 'verify-full' for production."
                );
                ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(NoVerifier))
                    .with_no_client_auth()
            }
            "verify-ca" => {
                // NOTE: In this implementation, 'verify-ca' behaves like 'verify-full':
                // the server certificate is validated against trusted CAs and the
                // server hostname is also verified against the certificate.
                // This is stricter than PostgreSQL's 'verify-ca' semantics but safer.
                info!(
                    "ssl_mode=verify-ca: certificate and hostname verification enabled \
                     (same behavior as verify-full in this implementation)"
                );
                ClientConfig::builder()
                    .with_root_certificates(root_store.clone())
                    .with_no_client_auth()
            }
            "verify-full" => {
                // Full certificate and hostname verification using system root CAs
                info!("ssl_mode=verify-full: full certificate and hostname verification enabled");
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

    /// Generate DDL for table creation.
    fn generate_ddl(&self, table: &Table, target_schema: &str, unlogged: bool) -> String {
        self.generate_ddl_with_options(table, target_schema, unlogged, None)
    }

    /// Generate DDL for table creation with optional row_hash column.
    fn generate_ddl_with_options(
        &self,
        table: &Table,
        target_schema: &str,
        unlogged: bool,
        row_hash_column: Option<&str>,
    ) -> String {
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

            // Add comma if more columns follow or if we're adding row_hash
            if i < table.columns.len() - 1 || row_hash_column.is_some() {
                ddl.push_str(",\n");
            } else {
                ddl.push('\n');
            }
        }

        // Add row_hash column if requested (for upsert mode with hash detection)
        if let Some(hash_col) = row_hash_column {
            ddl.push_str(&format!("    {} VARCHAR(32)\n", Self::quote_ident(hash_col)));
        }

        ddl.push_str(")");
        ddl
    }

    /// Quote a PostgreSQL identifier, escaping embedded double quotes.
    ///
    /// SQL identifiers (table names, column names, schema names) cannot be passed as
    /// parameters in prepared statements - only data values can be parameterized.
    /// This is a fundamental limitation of SQL, not a design choice.
    ///
    /// To safely construct dynamic SQL with identifiers, we:
    /// 1. Wrap identifiers in double quotes `"name"` (PostgreSQL's quoting mechanism)
    /// 2. Escape embedded quotes by doubling them: `"` → `""`
    ///
    /// This prevents SQL injection through identifier names while allowing dynamic
    /// table/column selection required for a generic migration tool.
    fn quote_ident(name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    /// Fully qualify a table name with schema.
    ///
    /// See [`Self::quote_ident`] for details on identifier escaping.
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
    ///
    /// This does simple bracket-to-quote conversion for column identifiers.
    /// Complex cases (nested brackets, brackets in strings) may not convert correctly.
    fn convert_check_definition(def: &str) -> String {
        // Detect complex cases that may not parse correctly
        Self::warn_if_complex_constraint(def);

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

    /// Check for complex constraint patterns that may not convert correctly.
    fn warn_if_complex_constraint(def: &str) {
        // Check for nested brackets like [[column]]
        if def.contains("[[") || def.contains("]]") {
            warn!(
                "CHECK constraint contains nested brackets which may not convert correctly: {}",
                Self::truncate_for_log(def)
            );
            return;
        }

        // Check for brackets inside string literals (approximate detection)
        // Look for patterns like '[something]' inside quotes
        let mut in_string = false;
        let mut has_bracket_in_string = false;
        let chars: Vec<char> = def.chars().collect();

        for (i, &c) in chars.iter().enumerate() {
            if c == '\'' {
                // Handle escaped quotes ''
                if i + 1 < chars.len() && chars[i + 1] == '\'' {
                    continue;
                }
                in_string = !in_string;
            } else if in_string && (c == '[' || c == ']') {
                has_bracket_in_string = true;
                break;
            }
        }

        if has_bracket_in_string {
            warn!(
                "CHECK constraint contains brackets inside string literals which may not convert correctly: {}",
                Self::truncate_for_log(def)
            );
        }
    }

    /// Truncate a constraint definition for logging (avoid huge log entries).
    fn truncate_for_log(s: &str) -> String {
        const MAX_LEN: usize = 100;
        if s.len() > MAX_LEN {
            format!("{}...", &s[..MAX_LEN])
        } else {
            s.to_string()
        }
    }

    /// Ensure the row_hash column exists on a table.
    /// Returns true if the column was added, false if it already existed.
    pub async fn ensure_row_hash_column(
        &self,
        schema: &str,
        table: &str,
        row_hash_column: &str,
    ) -> Result<bool> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        // Check if column exists
        let check_sql = r#"
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
            )
        "#;

        let row = client
            .query_one(check_sql, &[&schema, &table, &row_hash_column])
            .await?;
        let exists: bool = row.get(0);

        if !exists {
            let alter_sql = format!(
                "ALTER TABLE {} ADD COLUMN {} VARCHAR(32)",
                Self::qualify_table(schema, table),
                Self::quote_ident(row_hash_column)
            );
            client.execute(&alter_sql, &[]).await?;
            info!(
                "Added {} column to {}.{}",
                row_hash_column, schema, table
            );
            return Ok(true);
        }

        Ok(false)
    }

    /// Fetch existing row hashes for a PK range.
    /// Returns HashMap of serialized PK -> MD5 hash string.
    ///
    /// Supports both single and composite primary keys. For composite PKs,
    /// the key is serialized as "pk1|pk2|pk3".
    pub async fn fetch_row_hashes(
        &self,
        schema: &str,
        table: &str,
        pk_cols: &[String],
        row_hash_col: &str,
        min_pk: Option<i64>,
        max_pk: Option<i64>,
    ) -> Result<HashMap<String, String>> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection for hash fetch"))?;

        // Build PK column select list
        let pk_select: Vec<String> = pk_cols.iter().map(|c| Self::quote_ident(c)).collect();

        let mut query = format!(
            "SELECT {}, {} FROM {} WHERE {} IS NOT NULL",
            pk_select.join(", "),
            Self::quote_ident(row_hash_col),
            Self::qualify_table(schema, table),
            Self::quote_ident(row_hash_col)
        );

        // For single integer PK, we can apply range conditions
        // For composite PKs, we skip range filtering (fetch all hashes)
        if pk_cols.len() == 1 {
            let pk_col = &pk_cols[0];
            let mut conditions = Vec::new();
            if let Some(min) = min_pk {
                conditions.push(format!("{} >= {}", Self::quote_ident(pk_col), min));
            }
            if let Some(max) = max_pk {
                conditions.push(format!("{} <= {}", Self::quote_ident(pk_col), max));
            }
            if !conditions.is_empty() {
                query.push_str(" AND ");
                query.push_str(&conditions.join(" AND "));
            }
        }

        let rows = client.query(&query, &[]).await?;

        let mut hashes = HashMap::with_capacity(rows.len());
        let num_pk_cols = pk_cols.len();

        for row in rows {
            // Build serialized PK key from all PK columns
            // Handles all types that could reasonably be primary keys
            let pk_key: String = (0..num_pk_cols)
                .map(|i| {
                    // Try different types for PK columns (ordered by likelihood)
                    if let Ok(v) = row.try_get::<_, i64>(i) {
                        v.to_string()
                    } else if let Ok(v) = row.try_get::<_, i32>(i) {
                        v.to_string()
                    } else if let Ok(v) = row.try_get::<_, i16>(i) {
                        v.to_string()
                    } else if let Ok(v) = row.try_get::<_, String>(i) {
                        v
                    } else if let Ok(v) = row.try_get::<_, uuid::Uuid>(i) {
                        v.to_string()
                    } else if let Ok(v) = row.try_get::<_, rust_decimal::Decimal>(i) {
                        v.to_string()
                    } else if let Ok(v) = row.try_get::<_, chrono::NaiveDate>(i) {
                        v.to_string()
                    } else if let Ok(v) = row.try_get::<_, chrono::NaiveDateTime>(i) {
                        v.to_string()
                    } else if let Ok(v) = row.try_get::<_, bool>(i) {
                        if v { "1" } else { "0" }.to_string()
                    } else if let Ok(v) = row.try_get::<_, Vec<u8>>(i) {
                        hex::encode(v)
                    } else {
                        "UNKNOWN".to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join("|");

            let hash: String = row.get(num_pk_cols);
            hashes.insert(pk_key, hash);
        }

        debug!(
            "Fetched {} existing hashes from {}.{} (pk range {:?} to {:?})",
            hashes.len(),
            schema,
            table,
            min_pk,
            max_pk
        );

        Ok(hashes)
    }

    /// Create a table with the row_hash column included.
    /// Used for upsert mode with hash-based change detection.
    pub async fn create_table_with_hash(
        &self,
        table: &Table,
        target_schema: &str,
        row_hash_column: &str,
    ) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        let ddl = self.generate_ddl_with_options(table, target_schema, false, Some(row_hash_column));
        client.execute(&ddl, &[]).await?;

        debug!(
            "Created table {}.{} with {} column",
            target_schema, table.name, row_hash_column
        );
        Ok(())
    }
}

#[async_trait]
impl TargetPool for PgPool {
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

    async fn truncate_table(&self, schema: &str, table: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        let sql = format!("TRUNCATE TABLE {}", Self::qualify_table(schema, table));
        client.execute(&sql, &[]).await?;

        debug!("Truncated table {}.{}", schema, table);
        Ok(())
    }

    async fn table_exists(&self, schema: &str, table: &str) -> Result<bool> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        let row = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2
                )",
                &[&schema, &table],
            )
            .await?;

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
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

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

        client.execute(&sql, &[]).await?;

        debug!("Created primary key for {}.{}", target_schema, table.name);
        Ok(())
    }

    async fn create_index(&self, table: &Table, idx: &Index, target_schema: &str) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

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

        client.execute(&sql, &[]).await?;

        debug!(
            "Created index {} for {}.{}",
            idx_name, target_schema, table.name
        );
        Ok(())
    }

    async fn drop_non_pk_indexes(&self, schema: &str, table: &str) -> Result<Vec<String>> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        // Query to find all non-PK indexes on the table
        let sql = r#"
            SELECT i.relname AS index_name
            FROM pg_index idx
            JOIN pg_class i ON i.oid = idx.indexrelid
            JOIN pg_class t ON t.oid = idx.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = $1
              AND t.relname = $2
              AND NOT idx.indisprimary
              AND NOT idx.indisunique
        "#;

        let rows = client.query(sql, &[&schema, &table]).await?;

        let mut dropped = Vec::new();
        for row in rows {
            let index_name: String = row.get(0);
            let drop_sql = format!(
                "DROP INDEX IF EXISTS {} CASCADE",
                Self::qualify_table(schema, &index_name)
            );
            client.execute(&drop_sql, &[]).await?;
            debug!("Dropped index {}.{}", schema, index_name);
            dropped.push(index_name);
        }

        Ok(dropped)
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

        client.execute(&sql, &[]).await?;

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
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

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

        client.execute(&sql, &[]).await?;

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
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

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
            .await?;

        Ok(row.get(0))
    }

    async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        let sql = format!(
            "SELECT COUNT(*) FROM {}",
            Self::qualify_table(schema, table)
        );
        let row = client.query_one(&sql, &[]).await?;

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
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

        // Get max value (cast to bigint for consistent type)
        let sql = format!(
            "SELECT COALESCE(MAX({})::bigint, 0) FROM {}",
            Self::quote_ident(&identity_col.name),
            Self::qualify_table(schema, &table.name)
        );
        let row = client.query_one(&sql, &[]).await?;
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
            client.execute(&sql, &[]).await?;
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

    /// Upsert rows using staging table approach for better performance.
    ///
    /// Instead of row-by-row INSERT...ON CONFLICT, this:
    /// 1. Creates a temp staging table (UNLOGGED for speed)
    /// 2. COPY rows into staging using fast binary protocol
    /// 3. Single INSERT...SELECT...ON CONFLICT to merge into target
    /// 4. Drop staging table
    ///
    /// This is 2-3x faster than row-by-row because:
    /// - COPY is much faster than individual INSERTs
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
        row_hash_column: Option<&str>,
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

        // Use per-writer staging table to avoid conflicts between parallel writers.
        // Each writer gets its own staging table that persists for the session,
        // reducing catalog churn while avoiding race conditions.
        let staging_table = format!("_staging_{}_{}_{}", schema, table, writer_id);

        let client = self.pool.get().await.map_err(|e| {
            MigrateError::pool(e, "getting PostgreSQL connection for upsert")
        })?;

        // 1. Create temp staging table if not exists, then truncate for reuse
        // This reduces system catalog churn by reusing the same table across batches
        let create_staging_sql = format!(
            "CREATE TEMP TABLE IF NOT EXISTS {} (LIKE {} INCLUDING DEFAULTS)",
            pg_quote_ident(&staging_table),
            pg_qualify_table(schema, table)
        );
        client.execute(&create_staging_sql, &[]).await.map_err(|e| {
            MigrateError::transfer(
                format!("{}.{}", schema, table),
                format!("creating staging table: {}", e),
            )
        })?;

        // Truncate to clear any previous batch data (faster than DROP/CREATE)
        let truncate_sql = format!("TRUNCATE TABLE {}", pg_quote_ident(&staging_table));
        client.execute(&truncate_sql, &[]).await.map_err(|e| {
            MigrateError::transfer(
                format!("{}.{}", schema, table),
                format!("truncating staging table: {}", e),
            )
        })?;

        let staging_time = chunk_start.elapsed();
        let copy_start = Instant::now();

        // 2. COPY rows into staging table using binary format
        let col_list: String = cols
            .iter()
            .map(|c| pg_quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");

        let copy_stmt = format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT binary)",
            pg_quote_ident(&staging_table),
            col_list
        );

        let sink = client.copy_in(&copy_stmt).await.map_err(|e| {
            MigrateError::transfer(
                format!("{}.{}", schema, table),
                format!("initiating COPY to staging: {}", e),
            )
        })?;

        futures::pin_mut!(sink);

        // Build binary COPY data
        let mut buf = BytesMut::with_capacity(COPY_SEND_BUF_SIZE * 2);

        // Write binary COPY header
        buf.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
        buf.put_i32(0); // Flags: no OIDs
        buf.put_i32(0); // Header extension length: 0

        let num_cols = cols.len() as i16;

        for row in &rows {
            // Field count (16-bit)
            buf.put_i16(num_cols);

            // Write each field
            for value in row.iter() {
                write_binary_value(&mut buf, value);
            }

            // Flush when buffer is large enough
            if buf.len() > COPY_SEND_BUF_SIZE {
                sink.send(buf.split().freeze()).await.map_err(|e| {
                    MigrateError::transfer(
                        format!("{}.{}", schema, table),
                        format!("sending COPY data to staging: {}", e),
                    )
                })?;
            }
        }

        // Write trailer (-1 as field count indicates end)
        buf.put_i16(-1);

        // Send remaining data
        if !buf.is_empty() {
            sink.send(buf.freeze()).await.map_err(|e| {
                MigrateError::transfer(
                    format!("{}.{}", schema, table),
                    format!("sending final COPY data to staging: {}", e),
                )
            })?;
        }

        // Finish COPY
        sink.finish().await.map_err(|e| {
            MigrateError::transfer(
                format!("{}.{}", schema, table),
                format!("finishing COPY to staging: {}", e),
            )
        })?;

        let copy_time = copy_start.elapsed();
        let merge_start = Instant::now();

        // 3. Merge staging into target with single INSERT...SELECT...ON CONFLICT
        let merge_sql =
            build_staging_merge_sql(schema, table, &staging_table, cols, pk_cols, row_hash_column);
        client.execute(&merge_sql, &[]).await.map_err(|e| {
            MigrateError::transfer(
                format!("{}.{}", schema, table),
                format!("merging staging to target: {}", e),
            )
        })?;

        let merge_time = merge_start.elapsed();
        let total_time = chunk_start.elapsed();

        // Log detailed timing for profiling (only for larger chunks to avoid spam)
        if row_count >= UPSERT_PROFILING_ROW_THRESHOLD {
            tracing::debug!(
                "{}.{}: upsert chunk {} rows - staging: {:?}, copy: {:?}, merge: {:?}, total: {:?}",
                schema,
                table,
                row_count,
                staging_time,
                copy_time,
                merge_time,
                total_time
            );
        }

        // Note: Staging table is NOT dropped here - it's reused across batches
        // to reduce system catalog churn. It will be automatically dropped when
        // the underlying PostgreSQL session/connection is closed (not merely when
        // the connection is returned to the pool).

        Ok(row_count)
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
        // Hash filtering reduces network transfer, but we still use the DB-side
        // WHERE clause to prevent unnecessary writes (WAL, triggers, etc.)
        self.upsert_chunk(schema, table, cols, pk_cols, rows, writer_id, row_hash_column)
            .await
    }

    fn db_type(&self) -> &str {
        "postgres"
    }

    async fn close(&self) {
        // Pool will be closed when dropped
    }
}

impl PgPool {
    /// Test the connection to the PostgreSQL database.
    pub async fn test_connection(&self) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "testing PostgreSQL connection"))?;
        client.simple_query("SELECT 1").await?;
        Ok(())
    }
}

// Batch verification methods
impl PgPool {
    /// Execute a count query and return row_count.
    ///
    /// Used for Tier 1/2 quick verification.
    pub async fn execute_count_query(
        &self,
        query: &str,
        min_pk: i64,
        max_pk: i64,
    ) -> Result<i64> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "executing count query"))?;

        let row = client
            .query_one(query, &[&min_pk, &max_pk])
            .await
            .map_err(MigrateError::Target)?;

        let row_count: i64 = row.get(0);

        Ok(row_count)
    }

    /// Fetch row hashes for a PK range (Tier 3 verification).
    ///
    /// Returns a map of PK -> MD5 hash for comparison with source.
    pub async fn fetch_row_hashes_for_verify(
        &self,
        query: &str,
        min_pk: i64,
        max_pk: i64,
    ) -> Result<std::collections::HashMap<i64, String>> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "fetching row hashes"))?;

        let rows = client
            .query(query, &[&min_pk, &max_pk])
            .await
            .map_err(MigrateError::Target)?;

        let mut hashes = std::collections::HashMap::new();
        for row in rows {
            let pk: i64 = row.get(0);
            let hash: Option<String> = row.get(1);
            if let Some(h) = hash {
                hashes.insert(pk, h);
            }
        }

        Ok(hashes)
    }

    /// Get PK boundaries (min, max, count) for a table.
    pub async fn get_pk_bounds(&self, query: &str) -> Result<(i64, i64, i64)> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting PK bounds"))?;

        let row = client
            .query_one(query, &[])
            .await
            .map_err(MigrateError::Target)?;

        let min_pk: Option<i64> = row.get(0);
        let max_pk: Option<i64> = row.get(1);
        let row_count: i64 = row.get(2);

        Ok((min_pk.unwrap_or(0), max_pk.unwrap_or(0), row_count))
    }

    /// Delete rows by primary key values.
    ///
    /// Used for sync operations to remove rows that exist in target but not source.
    pub async fn delete_rows_by_pks(
        &self,
        schema: &str,
        table: &str,
        pk_column: &str,
        pks: &[i64],
    ) -> Result<i64> {
        if pks.is_empty() {
            return Ok(0);
        }

        let client = self.pool.get().await.map_err(|e| {
            MigrateError::pool(e, format!("getting connection for DELETE on {}.{}", schema, table))
        })?;

        let mut total_deleted = 0i64;

        // Batch deletions to avoid query size limits
        for chunk in pks.chunks(1000) {
            // Build parameterized query
            let params: Vec<String> = (1..=chunk.len()).map(|i| format!("${}", i)).collect();
            let param_list = params.join(", ");

            let query = format!(
                "DELETE FROM {}.{} WHERE {} IN ({})",
                pg_quote_ident(schema),
                pg_quote_ident(table),
                pg_quote_ident(pk_column),
                param_list
            );

            // Convert PKs to references for parameter binding
            let pk_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = chunk
                .iter()
                .map(|pk| pk as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();

            let rows_deleted = client
                .execute_raw(&query, pk_refs)
                .await
                .map_err(MigrateError::Target)?;

            total_deleted += rows_deleted as i64;
        }

        Ok(total_deleted)
    }

    /// Fetch rows for a PK range (for Rust-side hashing).
    ///
    /// Returns rows with all columns for the specified PK range, ordered by PK.
    /// Used by verify module to compute hashes in Rust instead of SQL.
    pub async fn fetch_rows_for_range(
        &self,
        schema: &str,
        table: &str,
        columns: &[String],
        pk_column: &str,
        min_pk: i64,
        max_pk: i64,
    ) -> Result<Vec<Vec<SqlValue>>> {
        let client = self.pool.get().await.map_err(|e| {
            MigrateError::pool(
                e,
                format!("getting connection for fetch_rows_for_range on {}.{}", schema, table),
            )
        })?;

        // Build column list with proper quoting
        let col_list = columns
            .iter()
            .map(|c| pg_quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");

        // Cast pk_column to BIGINT for comparison with i64 parameters
        let query = format!(
            "SELECT {} FROM {}.{} WHERE {}::BIGINT >= $1 AND {}::BIGINT < $2 ORDER BY {}",
            col_list,
            pg_quote_ident(schema),
            pg_quote_ident(table),
            pg_quote_ident(pk_column),
            pg_quote_ident(pk_column),
            pg_quote_ident(pk_column)
        );

        let rows = client
            .query(&query, &[&min_pk, &max_pk])
            .await
            .map_err(MigrateError::Target)?;

        let mut result = Vec::with_capacity(rows.len());

        for row in rows {
            let mut values = Vec::with_capacity(columns.len());
            for i in 0..columns.len() {
                let value = convert_pg_value(&row, i);
                values.push(value);
            }
            result.push(values);
        }

        Ok(result)
    }

    // ========================================================================
    // Universal PK Verification Methods (ROW_NUMBER-based)
    // ========================================================================

    /// Get total row count for a table.
    ///
    /// Used for Tier 1 initial partitioning.
    pub async fn get_total_row_count(&self, query: &str) -> Result<i64> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "getting total row count"))?;

        let row = client
            .query_one(query, &[])
            .await
            .map_err(MigrateError::Target)?;

        let row_count: i64 = row.get(0);
        Ok(row_count)
    }

    /// Execute NTILE partition query and return partition row counts.
    ///
    /// Returns a vector of (partition_id, row_count) tuples for Tier 1.
    pub async fn execute_ntile_partition_query(&self, query: &str) -> Result<Vec<(i64, i64)>> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "executing NTILE partition query"))?;

        let rows = client
            .query(query, &[])
            .await
            .map_err(MigrateError::Target)?;

        let mut partitions = Vec::new();
        for row in rows {
            let partition_id: i64 = row.get(0);
            let row_count: i64 = row.get(1);
            partitions.push((partition_id, row_count));
        }

        Ok(partitions)
    }

    /// Execute count query using ROW_NUMBER range (Tier 2 verification).
    ///
    /// Used for tables with any PK type (int, uuid, string, composite).
    pub async fn execute_count_query_with_rownum(
        &self,
        query: &str,
        range: &RowRange,
    ) -> Result<i64> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "executing count query with rownum"))?;

        let row = client
            .query_one(query, &[&range.start_row, &range.end_row])
            .await
            .map_err(MigrateError::Target)?;

        let row_count: i64 = row.get(0);
        Ok(row_count)
    }

    /// Fetch row hashes using ROW_NUMBER range with composite PK support (Tier 3).
    ///
    /// Returns a map of CompositePk -> MD5 hash for comparison with source.
    /// Supports any PK type including composite keys.
    pub async fn fetch_row_hashes_with_rownum(
        &self,
        query: &str,
        range: &RowRange,
        pk_column_count: usize,
    ) -> Result<CompositeRowHashMap> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MigrateError::pool(e, "fetching row hashes with rownum"))?;

        let rows = client
            .query(query, &[&range.start_row, &range.end_row])
            .await
            .map_err(MigrateError::Target)?;

        let mut hashes = std::collections::HashMap::new();
        for row in rows {
            // Extract PK columns (first pk_column_count columns)
            let mut pk_values = Vec::with_capacity(pk_column_count);
            for i in 0..pk_column_count {
                let pk_value = extract_pk_value_from_pg(&row, i);
                pk_values.push(pk_value);
            }

            // Last column is the row_hash
            let hash_idx = pk_column_count;
            let hash: Option<String> = row.get(hash_idx);

            if let Some(h) = hash {
                hashes.insert(CompositePk::new(pk_values), h);
            }
        }

        Ok(hashes)
    }

    /// Delete rows by composite primary key values.
    ///
    /// Used for sync operations to remove rows that exist in target but not source.
    pub async fn delete_rows_by_composite_pks(
        &self,
        schema: &str,
        table: &str,
        pk_columns: &[String],
        pks: &[CompositePk],
    ) -> Result<i64> {
        if pks.is_empty() {
            return Ok(0);
        }

        let client = self.pool.get().await.map_err(|e| {
            MigrateError::pool(
                e,
                format!(
                    "getting connection for DELETE on {}.{}",
                    schema, table
                ),
            )
        })?;

        let mut total_deleted = 0i64;

        // Batch deletions to avoid query size limits
        for chunk in pks.chunks(500) {
            let where_clause = if pk_columns.len() == 1 {
                // Single PK column - use IN clause
                let pk_col = pg_quote_ident(&pk_columns[0]);
                let pk_list = chunk
                    .iter()
                    .map(|pk| pk.values()[0].to_sql_literal())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{} IN ({})", pk_col, pk_list)
            } else {
                // Composite PK - use OR of tuple comparisons
                let conditions: Vec<String> = chunk
                    .iter()
                    .map(|pk| {
                        let parts: Vec<String> = pk_columns
                            .iter()
                            .zip(pk.values())
                            .map(|(col, val)| {
                                format!("{} = {}", pg_quote_ident(col), val.to_sql_literal())
                            })
                            .collect();
                        format!("({})", parts.join(" AND "))
                    })
                    .collect();
                conditions.join(" OR ")
            };

            let query = format!(
                "DELETE FROM {}.{} WHERE {}",
                pg_quote_ident(schema),
                pg_quote_ident(table),
                where_clause
            );

            let rows_deleted = client
                .execute(&query, &[])
                .await
                .map_err(MigrateError::Target)?;

            total_deleted += rows_deleted as i64;
        }

        Ok(total_deleted)
    }
}

/// Extract a PkValue from a PostgreSQL row at the given index.
fn extract_pk_value_from_pg(row: &tokio_postgres::Row, idx: usize) -> PkValue {
    // Try integer types first
    if let Ok(v) = row.try_get::<_, i64>(idx) {
        return PkValue::Int(v);
    }
    if let Ok(v) = row.try_get::<_, i32>(idx) {
        return PkValue::Int(v as i64);
    }
    // Try UUID
    if let Ok(v) = row.try_get::<_, uuid::Uuid>(idx) {
        return PkValue::Uuid(v);
    }
    // Fallback to string
    if let Ok(v) = row.try_get::<_, String>(idx) {
        return PkValue::String(v);
    }
    // Default to empty string if nothing matched
    PkValue::String(String::new())
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
        let client = self.pool.get().await.map_err(|e| {
            MigrateError::pool(e, format!("getting connection for COPY to {}.{}", schema, table))
        })?;

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

        let sink = client.copy_in(&copy_stmt).await?;

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

        let client = self.pool.get().await.map_err(|e| {
            MigrateError::pool(e, format!("getting connection for binary COPY to {}.{}", schema, table))
        })?;
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
        let sink = client.copy_in(&copy_stmt).await?;
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
            .map_err(|e| MigrateError::pool(e, "getting PostgreSQL connection"))?;

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

        let sink = client.copy_in(&copy_stmt).await?;

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

/// Build SQL to merge from staging table into target table.
/// Uses INSERT ... SELECT ... ON CONFLICT DO UPDATE with change detection.
///
/// If `row_hash_column` is provided and present in `cols`, uses it for efficient change detection
/// (single column compare). Otherwise falls back to comparing all non-PK columns with IS DISTINCT FROM.
fn build_staging_merge_sql(
    schema: &str,
    table: &str,
    staging_table: &str,
    cols: &[String],
    pk_cols: &[String],
    row_hash_column: Option<&str>,
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

    // Build UPDATE SET clause (exclude PK columns)
    let update_cols: Vec<String> = cols
        .iter()
        .filter(|c| !pk_cols.contains(c))
        .map(|c| format!("{} = EXCLUDED.{}", pg_quote_ident(c), pg_quote_ident(c)))
        .collect();

    if update_cols.is_empty() {
        // PK-only table, just INSERT ... ON CONFLICT DO NOTHING
        format!(
            "INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT ({}) DO NOTHING",
            pg_qualify_table(schema, table),
            col_list,
            col_list,
            pg_quote_ident(staging_table),
            pk_list
        )
    } else {
        // Use row_hash for change detection if available (much faster than comparing all columns)
        let hash_col_in_cols = row_hash_column.filter(|h| cols.iter().any(|c| c == *h));

        let change_detection = if let Some(hash_col) = hash_col_in_cols {
            // Single column comparison using pre-computed hash
            format!(
                "{}.{} IS DISTINCT FROM EXCLUDED.{}",
                pg_quote_ident(table),
                pg_quote_ident(hash_col),
                pg_quote_ident(hash_col)
            )
        } else {
            // Fallback: compare all non-PK columns
            cols.iter()
                .filter(|c| !pk_cols.contains(c))
                .map(|c| {
                    format!(
                        "{}.{} IS DISTINCT FROM EXCLUDED.{}",
                        pg_quote_ident(table),
                        pg_quote_ident(c),
                        pg_quote_ident(c)
                    )
                })
                .collect::<Vec<_>>()
                .join(" OR ")
        };

        format!(
            "INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT ({}) DO UPDATE SET {} WHERE {}",
            pg_qualify_table(schema, table),
            col_list,
            col_list,
            pg_quote_ident(staging_table),
            pk_list,
            update_cols.join(", "),
            change_detection
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_row_hash_basic() {
        // Test with simple string values, PK at index 0
        let row = vec![
            SqlValue::I64(1),                         // PK - should be excluded
            SqlValue::String("hello".to_string()),
            SqlValue::I32(42),
        ];
        let pk_indices = vec![0];

        let hash = calculate_row_hash(&row, &pk_indices);

        // Hash should be 32 hex characters
        assert_eq!(hash.len(), 32);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_calculate_row_hash_excludes_pk() {
        // Two rows with same non-PK data but different PKs should have same hash
        let row1 = vec![
            SqlValue::I64(1),                         // PK
            SqlValue::String("data".to_string()),
        ];
        let row2 = vec![
            SqlValue::I64(999),                       // Different PK
            SqlValue::String("data".to_string()),
        ];
        let pk_indices = vec![0];

        let hash1 = calculate_row_hash(&row1, &pk_indices);
        let hash2 = calculate_row_hash(&row2, &pk_indices);

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_calculate_row_hash_detects_changes() {
        // Two rows with same PK but different data should have different hashes
        let row1 = vec![
            SqlValue::I64(1),
            SqlValue::String("value1".to_string()),
        ];
        let row2 = vec![
            SqlValue::I64(1),
            SqlValue::String("value2".to_string()),
        ];
        let pk_indices = vec![0];

        let hash1 = calculate_row_hash(&row1, &pk_indices);
        let hash2 = calculate_row_hash(&row2, &pk_indices);

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_calculate_row_hash_null_handling() {
        // NULL values should be handled consistently
        let row1 = vec![
            SqlValue::I64(1),
            SqlValue::Null(SqlNullType::String),
        ];
        let row2 = vec![
            SqlValue::I64(2),
            SqlValue::Null(SqlNullType::String),
        ];
        let pk_indices = vec![0];

        let hash1 = calculate_row_hash(&row1, &pk_indices);
        let hash2 = calculate_row_hash(&row2, &pk_indices);

        // Same data (NULL) should produce same hash
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_calculate_row_hash_null_vs_empty_string() {
        // NULL and empty string should have different hashes
        let row1 = vec![
            SqlValue::I64(1),
            SqlValue::Null(SqlNullType::String),
        ];
        let row2 = vec![
            SqlValue::I64(1),
            SqlValue::String("".to_string()),
        ];
        let pk_indices = vec![0];

        let hash1 = calculate_row_hash(&row1, &pk_indices);
        let hash2 = calculate_row_hash(&row2, &pk_indices);

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_calculate_row_hash_composite_pk() {
        // Test with composite primary key
        let row = vec![
            SqlValue::I64(1),                         // PK part 1
            SqlValue::I64(2),                         // PK part 2
            SqlValue::String("data".to_string()),
        ];
        let pk_indices = vec![0, 1];

        let hash = calculate_row_hash(&row, &pk_indices);

        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn test_calculate_row_hash_all_types() {
        // Test with all supported types
        let row = vec![
            SqlValue::I64(1),                                                          // PK
            SqlValue::Bool(true),
            SqlValue::I16(16),
            SqlValue::I32(32),
            SqlValue::I64(64),
            SqlValue::F32(1.5),
            SqlValue::F64(2.5),
            SqlValue::String("text".to_string()),
            SqlValue::Bytes(vec![1, 2, 3]),
            SqlValue::Uuid(uuid::Uuid::nil()),
            SqlValue::Decimal(rust_decimal::Decimal::new(12345, 2)),
            SqlValue::Date(chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()),
            SqlValue::Time(chrono::NaiveTime::from_hms_opt(12, 30, 45).unwrap()),
        ];
        let pk_indices = vec![0];

        let hash = calculate_row_hash(&row, &pk_indices);

        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn test_calculate_row_hash_float_precision() {
        // Floats should use fixed precision for hash stability
        let row1 = vec![
            SqlValue::I64(1),
            SqlValue::F64(1.0 / 3.0),  // 0.333333...
        ];
        let row2 = vec![
            SqlValue::I64(1),
            SqlValue::F64(1.0 / 3.0),  // Same calculation
        ];
        let pk_indices = vec![0];

        let hash1 = calculate_row_hash(&row1, &pk_indices);
        let hash2 = calculate_row_hash(&row2, &pk_indices);

        // Same float value should produce same hash
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_calculate_row_hash_order_matters() {
        // Column order should affect hash
        let row1 = vec![
            SqlValue::I64(1),
            SqlValue::String("a".to_string()),
            SqlValue::String("b".to_string()),
        ];
        let row2 = vec![
            SqlValue::I64(1),
            SqlValue::String("b".to_string()),
            SqlValue::String("a".to_string()),
        ];
        let pk_indices = vec![0];

        let hash1 = calculate_row_hash(&row1, &pk_indices);
        let hash2 = calculate_row_hash(&row2, &pk_indices);

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_calculate_row_hash_deterministic() {
        // Same input should always produce same hash
        let row = vec![
            SqlValue::I64(42),
            SqlValue::String("test".to_string()),
            SqlValue::I32(123),
        ];
        let pk_indices = vec![0];

        let hash1 = calculate_row_hash(&row, &pk_indices);
        let hash2 = calculate_row_hash(&row, &pk_indices);
        let hash3 = calculate_row_hash(&row, &pk_indices);

        assert_eq!(hash1, hash2);
        assert_eq!(hash2, hash3);
    }

    #[test]
    fn test_build_staging_merge_sql_with_change_detection() {
        let cols = vec!["id".to_string(), "name".to_string(), "value".to_string()];
        let pk_cols = vec!["id".to_string()];

        // Should always include change detection WHERE clause (fallback to column comparison)
        let sql = build_staging_merge_sql(
            "public",
            "users",
            "_staging_users",
            &cols,
            &pk_cols,
            None,
        );
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("IS DISTINCT FROM"));
        assert!(sql.contains("DO UPDATE SET"));
        // Should compare name and value columns
        assert!(sql.contains(r#""name" IS DISTINCT FROM"#));
        assert!(sql.contains(r#""value" IS DISTINCT FROM"#));
    }

    #[test]
    fn test_build_staging_merge_sql_pk_only_table() {
        // For PK-only tables, should use DO NOTHING (no columns to update)
        let cols = vec!["id".to_string()];
        let pk_cols = vec!["id".to_string()];

        let sql = build_staging_merge_sql(
            "public",
            "lookup",
            "_staging_lookup",
            &cols,
            &pk_cols,
            None,
        );
        assert!(sql.contains("DO NOTHING"));
        assert!(!sql.contains("DO UPDATE"));
    }

    #[test]
    fn test_build_staging_merge_sql_with_row_hash_optimization() {
        // When row_hash column is present, should use single column comparison
        let cols = vec![
            "id".to_string(),
            "name".to_string(),
            "value".to_string(),
            "row_hash".to_string(),
        ];
        let pk_cols = vec!["id".to_string()];

        let sql = build_staging_merge_sql(
            "public",
            "users",
            "_staging_users",
            &cols,
            &pk_cols,
            Some("row_hash"),
        );
        assert!(sql.contains("WHERE"));
        // Should use row_hash for change detection
        assert!(sql.contains(r#""row_hash" IS DISTINCT FROM EXCLUDED."row_hash""#));
        // Should NOT compare individual columns in WHERE clause
        assert!(!sql.contains(r#""name" IS DISTINCT FROM EXCLUDED."name""#));
        assert!(!sql.contains(r#""value" IS DISTINCT FROM EXCLUDED."value""#));
    }

    #[test]
    fn test_build_staging_merge_sql_with_custom_hash_column() {
        // When using a custom hash column name
        let cols = vec![
            "id".to_string(),
            "name".to_string(),
            "_hash".to_string(),
        ];
        let pk_cols = vec!["id".to_string()];

        let sql = build_staging_merge_sql(
            "public",
            "users",
            "_staging_users",
            &cols,
            &pk_cols,
            Some("_hash"),
        );
        // Should use custom hash column
        assert!(sql.contains(r#""_hash" IS DISTINCT FROM EXCLUDED."_hash""#));
        assert!(!sql.contains(r#""name" IS DISTINCT FROM EXCLUDED."name""#));
    }

    #[test]
    fn test_build_staging_merge_sql_hash_column_not_in_cols() {
        // When row_hash_column is specified but not in cols, should fallback
        let cols = vec!["id".to_string(), "name".to_string(), "value".to_string()];
        let pk_cols = vec!["id".to_string()];

        let sql = build_staging_merge_sql(
            "public",
            "users",
            "_staging_users",
            &cols,
            &pk_cols,
            Some("row_hash"), // specified but not in cols
        );
        // Should fallback to comparing all non-PK columns
        assert!(sql.contains(r#""name" IS DISTINCT FROM EXCLUDED."name""#));
        assert!(sql.contains(r#""value" IS DISTINCT FROM EXCLUDED."value""#));
        // Should NOT reference row_hash since it's not in cols
        assert!(!sql.contains("row_hash"));
    }
}
