//! MySQL/MariaDB SQL dialect (Strategy pattern).
//!
//! Provides MySQL-specific SQL syntax for identifier quoting, query building,
//! and parameter placeholders.

use crate::core::traits::{Dialect, SelectQueryOptions};

/// MySQL/MariaDB dialect implementation.
///
/// Implements the Strategy pattern for SQL syntax differences.
/// Compatible with MySQL 5.7+, 8.0+, and MariaDB 10.2+.
#[derive(Debug, Clone, Default)]
pub struct MysqlDialect;

impl MysqlDialect {
    /// Create a new MySQL dialect instance.
    pub fn new() -> Self {
        Self
    }
}

impl Dialect for MysqlDialect {
    fn name(&self) -> &str {
        "mysql"
    }

    fn quote_ident(&self, name: &str) -> String {
        // MySQL uses backticks for identifier quoting
        // Handle names that contain backticks by doubling them
        format!("`{}`", name.replace('`', "``"))
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

        // MySQL uses LIMIT for row limiting
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
        // MySQL uses INSERT ... SELECT ... ON DUPLICATE KEY UPDATE
        // This requires that the target table has a PRIMARY KEY or UNIQUE index

        // All columns for INSERT
        let insert_cols = columns
            .iter()
            .map(|c| self.quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");

        // Select from staging table
        let select_cols = columns
            .iter()
            .map(|c| format!("s.{}", self.quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");

        // Non-PK columns for UPDATE
        let non_pk_cols: Vec<_> = columns.iter().filter(|c| !pk_columns.contains(c)).collect();

        let update_set = non_pk_cols
            .iter()
            .map(|c| {
                format!(
                    "{} = VALUES({})",
                    self.quote_ident(c),
                    self.quote_ident(c)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        // Build INSERT ... SELECT ... ON DUPLICATE KEY UPDATE
        let mut sql = format!(
            "INSERT INTO {} ({}) SELECT {} FROM {} AS s",
            target_table, insert_cols, select_cols, staging_table
        );

        // ON DUPLICATE KEY UPDATE (only if there are non-PK columns)
        if !non_pk_cols.is_empty() {
            sql.push_str(&format!(" ON DUPLICATE KEY UPDATE {}", update_set));
        } else {
            // If only PK columns, use INSERT IGNORE to skip duplicates
            sql = format!(
                "INSERT IGNORE INTO {} ({}) SELECT {} FROM {} AS s",
                target_table, insert_cols, select_cols, staging_table
            );
        }

        sql
    }

    fn param_placeholder(&self, index: usize) -> String {
        // SQLx MySQL uses positional placeholders with $N (1-based)
        // But native MySQL uses ?, so we support both styles
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
        // MySQL 8.0+ supports ROW_NUMBER()
        // For older versions, use LIMIT/OFFSET with a subquery
        // We'll use the MySQL 8.0+ syntax with ROW_NUMBER()
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
        let dialect = MysqlDialect::new();
        assert_eq!(dialect.quote_ident("name"), "`name`");
        assert_eq!(dialect.quote_ident("table`name"), "`table``name`");
        assert_eq!(dialect.quote_ident("Users"), "`Users`");
    }

    #[test]
    fn test_param_placeholder() {
        let dialect = MysqlDialect::new();
        assert_eq!(dialect.param_placeholder(1), "$1");
        assert_eq!(dialect.param_placeholder(10), "$10");
    }

    #[test]
    fn test_build_select_query_simple() {
        let dialect = MysqlDialect::new();
        let opts = SelectQueryOptions {
            schema: "mydb".to_string(),
            table: "Users".to_string(),
            columns: vec!["Id".to_string(), "Name".to_string()],
            pk_col: None,
            min_pk: None,
            max_pk: None,
            where_clause: None,
            limit: None,
        };

        let sql = dialect.build_select_query(&opts);
        assert!(sql.contains("SELECT `Id`, `Name` FROM `mydb`.`Users`"));
    }

    #[test]
    fn test_build_select_query_with_keyset() {
        let dialect = MysqlDialect::new();
        let opts = SelectQueryOptions {
            schema: "mydb".to_string(),
            table: "Users".to_string(),
            columns: vec!["Id".to_string(), "Name".to_string()],
            pk_col: Some("Id".to_string()),
            min_pk: Some(100),
            max_pk: Some(200),
            where_clause: None,
            limit: Some(1000),
        };

        let sql = dialect.build_select_query(&opts);
        assert!(sql.contains("LIMIT 1000"));
        assert!(sql.contains("`Id` > 100"));
        assert!(sql.contains("`Id` <= 200"));
        assert!(sql.contains("ORDER BY `Id`"));
    }

    #[test]
    fn test_build_select_query_with_where() {
        let dialect = MysqlDialect::new();
        let opts = SelectQueryOptions {
            schema: "mydb".to_string(),
            table: "Users".to_string(),
            columns: vec!["Id".to_string()],
            pk_col: Some("Id".to_string()),
            min_pk: Some(0),
            max_pk: None,
            where_clause: Some("Active = 1".to_string()),
            limit: None,
        };

        let sql = dialect.build_select_query(&opts);
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("`Id` > 0"));
        assert!(sql.contains("(Active = 1)"));
    }

    #[test]
    fn test_build_upsert_query() {
        let dialect = MysqlDialect::new();
        let sql = dialect.build_upsert_query(
            "`mydb`.`Users`",
            "`mydb`.`_staging_Users`",
            &["Id".to_string(), "Name".to_string(), "Email".to_string()],
            &["Id".to_string()],
        );

        assert!(sql.contains("INSERT INTO `mydb`.`Users`"));
        assert!(sql.contains("SELECT s.`Id`, s.`Name`, s.`Email`"));
        assert!(sql.contains("FROM `mydb`.`_staging_Users` AS s"));
        assert!(sql.contains("ON DUPLICATE KEY UPDATE"));
        assert!(sql.contains("`Name` = VALUES(`Name`)"));
        assert!(sql.contains("`Email` = VALUES(`Email`)"));
    }

    #[test]
    fn test_build_upsert_query_pk_only() {
        let dialect = MysqlDialect::new();
        // Table with only PK columns - use INSERT IGNORE
        let sql = dialect.build_upsert_query(
            "`mydb`.`IdTable`",
            "`mydb`.`_staging_IdTable`",
            &["Id".to_string()],
            &["Id".to_string()],
        );

        // Should use INSERT IGNORE instead of ON DUPLICATE KEY UPDATE
        assert!(sql.contains("INSERT IGNORE INTO"));
        assert!(!sql.contains("ON DUPLICATE KEY UPDATE"));
    }

    #[test]
    fn test_build_keyset_where() {
        let dialect = MysqlDialect::new();
        assert_eq!(dialect.build_keyset_where("Id", 100), "`Id` > 100");
    }

    #[test]
    fn test_build_row_number_query() {
        let dialect = MysqlDialect::new();
        let sql = dialect.build_row_number_query("SELECT * FROM `mydb`.`Users`", "Id", 1, 1000);

        assert!(sql.contains("ROW_NUMBER() OVER (ORDER BY `Id`)"));
        assert!(sql.contains("__rn >= 1"));
        assert!(sql.contains("__rn <= 1000"));
    }
}
