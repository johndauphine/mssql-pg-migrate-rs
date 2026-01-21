//! PostgreSQL SQL dialect (Strategy pattern).
//!
//! Provides PostgreSQL-specific SQL syntax for identifier quoting, query building,
//! and parameter placeholders.

use crate::core::traits::{Dialect, SelectQueryOptions};

/// PostgreSQL dialect implementation.
///
/// Implements the Strategy pattern for SQL syntax differences.
#[derive(Debug, Clone, Default)]
pub struct PostgresDialect;

impl PostgresDialect {
    /// Create a new PostgreSQL dialect instance.
    pub fn new() -> Self {
        Self
    }
}

impl Dialect for PostgresDialect {
    fn name(&self) -> &str {
        "postgres"
    }

    fn quote_ident(&self, name: &str) -> String {
        // PostgreSQL uses double quotes for identifier quoting
        // Handle names that contain double quotes by doubling them
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    fn build_select_query(&self, opts: &SelectQueryOptions) -> String {
        let cols = if opts.columns.is_empty() {
            "*".to_string()
        } else {
            opts.columns
                .iter()
                .map(|c| self.quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let table = format!(
            "{}.{}",
            self.quote_ident(&opts.schema),
            self.quote_ident(&opts.table)
        );

        let mut sql = format!("SELECT {} FROM {}", cols, table);

        // Build WHERE clause
        let mut conditions = Vec::new();

        // Keyset pagination
        if let (Some(pk), Some(min_pk)) = (&opts.pk_col, opts.min_pk) {
            conditions.push(format!("{} > {}", self.quote_ident(pk), min_pk));
        }

        if let (Some(pk), Some(max_pk)) = (&opts.pk_col, opts.max_pk) {
            conditions.push(format!("{} <= {}", self.quote_ident(pk), max_pk));
        }

        // Custom WHERE clause
        if let Some(ref where_clause) = opts.where_clause {
            if !where_clause.is_empty() {
                conditions.push(format!("({})", where_clause));
            }
        }

        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        // Ordering by PK for keyset pagination
        if let Some(ref pk) = opts.pk_col {
            sql.push_str(&format!(" ORDER BY {}", self.quote_ident(pk)));
        }

        // PostgreSQL uses LIMIT for row limiting
        if let Some(limit) = opts.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        sql
    }

    fn build_upsert_query(
        &self,
        target_table: &str,
        staging_table: &str,
        columns: &[String],
        pk_columns: &[String],
    ) -> String {
        // PostgreSQL uses INSERT ... ON CONFLICT for upsert operations
        let quoted_cols = columns
            .iter()
            .map(|c| self.quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");

        let source_cols = columns
            .iter()
            .map(|c| format!("{}.{}", staging_table, self.quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");

        let conflict_cols = pk_columns
            .iter()
            .map(|c| self.quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");

        // Non-PK columns for UPDATE
        let non_pk_cols: Vec<_> = columns
            .iter()
            .filter(|c| !pk_columns.contains(c))
            .collect();

        let mut sql = format!(
            "INSERT INTO {} ({}) SELECT {} FROM {}",
            target_table, quoted_cols, source_cols, staging_table
        );

        // ON CONFLICT clause
        sql.push_str(&format!(" ON CONFLICT ({}) DO", conflict_cols));

        if non_pk_cols.is_empty() {
            // No non-PK columns to update - just ignore duplicates
            sql.push_str(" NOTHING");
        } else {
            // UPDATE non-PK columns
            let update_set = non_pk_cols
                .iter()
                .map(|c| format!("{} = EXCLUDED.{}", self.quote_ident(c), self.quote_ident(c)))
                .collect::<Vec<_>>()
                .join(", ");

            sql.push_str(&format!(" UPDATE SET {}", update_set));
        }

        sql
    }

    fn param_placeholder(&self, index: usize) -> String {
        // PostgreSQL uses $1, $2, etc. (1-based)
        format!("${}", index)
    }

    fn build_keyset_where(&self, pk_col: &str, last_pk: i64) -> String {
        format!("{} > {}", self.quote_ident(pk_col), last_pk)
    }

    fn build_row_number_query(
        &self,
        inner_query: &str,
        pk_col: &str,
        start_row: i64,
        end_row: i64,
    ) -> String {
        // PostgreSQL ROW_NUMBER with CTE for pagination
        // Note: PostgreSQL also supports OFFSET/LIMIT but ROW_NUMBER is more flexible
        format!(
            r#"WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (ORDER BY {}) AS __rn
    FROM ({}) AS __inner
)
SELECT * FROM numbered WHERE __rn >= {} AND __rn <= {}"#,
            self.quote_ident(pk_col),
            inner_query,
            start_row,
            end_row
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_ident() {
        let dialect = PostgresDialect::new();
        assert_eq!(dialect.quote_ident("name"), "\"name\"");
        assert_eq!(dialect.quote_ident("table\"name"), "\"table\"\"name\"");
        assert_eq!(dialect.quote_ident("Users"), "\"Users\"");
    }

    #[test]
    fn test_param_placeholder() {
        let dialect = PostgresDialect::new();
        assert_eq!(dialect.param_placeholder(1), "$1");
        assert_eq!(dialect.param_placeholder(10), "$10");
    }

    #[test]
    fn test_build_select_query_simple() {
        let dialect = PostgresDialect::new();
        let opts = SelectQueryOptions {
            schema: "public".to_string(),
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            pk_col: None,
            min_pk: None,
            max_pk: None,
            where_clause: None,
            limit: None,
        };

        let sql = dialect.build_select_query(&opts);
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"public\".\"users\""
        );
    }

    #[test]
    fn test_build_select_query_with_keyset() {
        let dialect = PostgresDialect::new();
        let opts = SelectQueryOptions {
            schema: "public".to_string(),
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            pk_col: Some("id".to_string()),
            min_pk: Some(100),
            max_pk: Some(200),
            where_clause: None,
            limit: Some(1000),
        };

        let sql = dialect.build_select_query(&opts);
        assert!(sql.contains("\"id\" > 100"));
        assert!(sql.contains("\"id\" <= 200"));
        assert!(sql.contains("ORDER BY \"id\""));
        assert!(sql.contains("LIMIT 1000"));
    }

    #[test]
    fn test_build_select_query_with_where() {
        let dialect = PostgresDialect::new();
        let opts = SelectQueryOptions {
            schema: "public".to_string(),
            table: "users".to_string(),
            columns: vec!["id".to_string()],
            pk_col: Some("id".to_string()),
            min_pk: Some(0),
            max_pk: None,
            where_clause: Some("active = true".to_string()),
            limit: None,
        };

        let sql = dialect.build_select_query(&opts);
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("\"id\" > 0"));
        assert!(sql.contains("(active = true)"));
    }

    #[test]
    fn test_build_upsert_query() {
        let dialect = PostgresDialect::new();
        let sql = dialect.build_upsert_query(
            "\"public\".\"users\"",
            "_staging_users",
            &["id".to_string(), "name".to_string(), "email".to_string()],
            &["id".to_string()],
        );

        assert!(sql.contains("INSERT INTO \"public\".\"users\""));
        assert!(sql.contains("(\"id\", \"name\", \"email\")"));
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("FROM _staging_users"));
        assert!(sql.contains("ON CONFLICT (\"id\") DO"));
        assert!(sql.contains("UPDATE SET"));
        assert!(sql.contains("\"name\" = EXCLUDED.\"name\""));
        assert!(sql.contains("\"email\" = EXCLUDED.\"email\""));
    }

    #[test]
    fn test_build_upsert_query_pk_only() {
        let dialect = PostgresDialect::new();
        // Table with only PK columns - should use DO NOTHING
        let sql = dialect.build_upsert_query(
            "\"public\".\"id_table\"",
            "_staging_id_table",
            &["id".to_string()],
            &["id".to_string()],
        );

        assert!(sql.contains("ON CONFLICT (\"id\") DO NOTHING"));
        assert!(!sql.contains("UPDATE SET"));
    }

    #[test]
    fn test_build_keyset_where() {
        let dialect = PostgresDialect::new();
        assert_eq!(
            dialect.build_keyset_where("id", 100),
            "\"id\" > 100"
        );
    }

    #[test]
    fn test_build_row_number_query() {
        let dialect = PostgresDialect::new();
        let sql = dialect.build_row_number_query(
            "SELECT * FROM \"public\".\"users\"",
            "id",
            1,
            1000,
        );

        assert!(sql.contains("ROW_NUMBER() OVER (ORDER BY \"id\")"));
        assert!(sql.contains("__rn >= 1"));
        assert!(sql.contains("__rn <= 1000"));
    }
}
