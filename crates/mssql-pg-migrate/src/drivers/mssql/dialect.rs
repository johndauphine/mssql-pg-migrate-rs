//! MSSQL SQL dialect (Strategy pattern).
//!
//! Provides MSSQL-specific SQL syntax for identifier quoting, query building,
//! and parameter placeholders.

use crate::core::traits::{Dialect, SelectQueryOptions};

/// Microsoft SQL Server dialect implementation.
///
/// Implements the Strategy pattern for SQL syntax differences.
#[derive(Debug, Clone, Default)]
pub struct MssqlDialect;

impl MssqlDialect {
    /// Create a new MSSQL dialect instance.
    pub fn new() -> Self {
        Self
    }
}

impl Dialect for MssqlDialect {
    fn name(&self) -> &str {
        "mssql"
    }

    fn quote_ident(&self, name: &str) -> String {
        // MSSQL uses square brackets for identifier quoting
        // Handle names that contain closing brackets by doubling them
        format!("[{}]", name.replace(']', "]]"))
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

        let mut sql = format!("SELECT {} FROM {} WITH (NOLOCK)", cols, table);

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

        // MSSQL uses TOP for limiting rows in ordered queries
        // Note: For OFFSET/FETCH, we'd need SQL Server 2012+
        // But for keyset pagination, TOP works well
        if let Some(limit) = opts.limit {
            // Insert TOP after SELECT
            sql = sql.replacen("SELECT ", &format!("SELECT TOP {} ", limit), 1);
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
        // MSSQL uses MERGE for upsert operations
        let target_alias = "t";
        let source_alias = "s";

        // Join condition on primary keys
        let join_condition = pk_columns
            .iter()
            .map(|pk| {
                format!(
                    "{}.{} = {}.{}",
                    target_alias,
                    self.quote_ident(pk),
                    source_alias,
                    self.quote_ident(pk)
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");

        // Non-PK columns for UPDATE
        let non_pk_cols: Vec<_> = columns
            .iter()
            .filter(|c| !pk_columns.contains(c))
            .collect();

        let update_set = non_pk_cols
            .iter()
            .map(|c| {
                format!(
                    "{}.{} = {}.{}",
                    target_alias,
                    self.quote_ident(c),
                    source_alias,
                    self.quote_ident(c)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        // All columns for INSERT
        let insert_cols = columns
            .iter()
            .map(|c| self.quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");

        let insert_vals = columns
            .iter()
            .map(|c| format!("{}.{}", source_alias, self.quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");

        // Build MERGE statement
        let mut sql = format!(
            "MERGE {} AS {} USING {} AS {} ON {}",
            target_table, target_alias, staging_table, source_alias, join_condition
        );

        // WHEN MATCHED - UPDATE (only if there are non-PK columns)
        if !non_pk_cols.is_empty() {
            sql.push_str(&format!(" WHEN MATCHED THEN UPDATE SET {}", update_set));
        }

        // WHEN NOT MATCHED - INSERT
        sql.push_str(&format!(
            " WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
            insert_cols, insert_vals
        ));

        // MSSQL MERGE requires semicolon terminator
        sql.push(';');

        sql
    }

    fn param_placeholder(&self, index: usize) -> String {
        // MSSQL uses @P1, @P2, etc. (1-based)
        format!("@P{}", index)
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
        // MSSQL ROW_NUMBER with CTE for pagination
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
        let dialect = MssqlDialect::new();
        assert_eq!(dialect.quote_ident("name"), "[name]");
        assert_eq!(dialect.quote_ident("table]name"), "[table]]name]");
        assert_eq!(dialect.quote_ident("Users"), "[Users]");
    }

    #[test]
    fn test_param_placeholder() {
        let dialect = MssqlDialect::new();
        assert_eq!(dialect.param_placeholder(1), "@P1");
        assert_eq!(dialect.param_placeholder(10), "@P10");
    }

    #[test]
    fn test_build_select_query_simple() {
        let dialect = MssqlDialect::new();
        let opts = SelectQueryOptions {
            schema: "dbo".to_string(),
            table: "Users".to_string(),
            columns: vec!["Id".to_string(), "Name".to_string()],
            pk_col: None,
            min_pk: None,
            max_pk: None,
            where_clause: None,
            limit: None,
        };

        let sql = dialect.build_select_query(&opts);
        assert!(sql.contains("SELECT [Id], [Name] FROM [dbo].[Users]"));
        assert!(sql.contains("WITH (NOLOCK)"));
    }

    #[test]
    fn test_build_select_query_with_keyset() {
        let dialect = MssqlDialect::new();
        let opts = SelectQueryOptions {
            schema: "dbo".to_string(),
            table: "Users".to_string(),
            columns: vec!["Id".to_string(), "Name".to_string()],
            pk_col: Some("Id".to_string()),
            min_pk: Some(100),
            max_pk: Some(200),
            where_clause: None,
            limit: Some(1000),
        };

        let sql = dialect.build_select_query(&opts);
        assert!(sql.contains("TOP 1000"));
        assert!(sql.contains("[Id] > 100"));
        assert!(sql.contains("[Id] <= 200"));
        assert!(sql.contains("ORDER BY [Id]"));
    }

    #[test]
    fn test_build_select_query_with_where() {
        let dialect = MssqlDialect::new();
        let opts = SelectQueryOptions {
            schema: "dbo".to_string(),
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
        assert!(sql.contains("[Id] > 0"));
        assert!(sql.contains("(Active = 1)"));
    }

    #[test]
    fn test_build_upsert_query() {
        let dialect = MssqlDialect::new();
        let sql = dialect.build_upsert_query(
            "[dbo].[Users]",
            "#staging_Users",
            &["Id".to_string(), "Name".to_string(), "Email".to_string()],
            &["Id".to_string()],
        );

        assert!(sql.contains("MERGE [dbo].[Users] AS t"));
        assert!(sql.contains("USING #staging_Users AS s"));
        assert!(sql.contains("ON t.[Id] = s.[Id]"));
        assert!(sql.contains("WHEN MATCHED THEN UPDATE SET"));
        assert!(sql.contains("t.[Name] = s.[Name]"));
        assert!(sql.contains("t.[Email] = s.[Email]"));
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"));
        assert!(sql.ends_with(';'));
    }

    #[test]
    fn test_build_upsert_query_pk_only() {
        let dialect = MssqlDialect::new();
        // Table with only PK columns - no UPDATE SET clause
        let sql = dialect.build_upsert_query(
            "[dbo].[IdTable]",
            "#staging_IdTable",
            &["Id".to_string()],
            &["Id".to_string()],
        );

        // Should not have "WHEN MATCHED" because there's nothing to update
        assert!(!sql.contains("WHEN MATCHED"));
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"));
    }

    #[test]
    fn test_build_keyset_where() {
        let dialect = MssqlDialect::new();
        assert_eq!(
            dialect.build_keyset_where("Id", 100),
            "[Id] > 100"
        );
    }

    #[test]
    fn test_build_row_number_query() {
        let dialect = MssqlDialect::new();
        let sql = dialect.build_row_number_query(
            "SELECT * FROM [dbo].[Users]",
            "Id",
            1,
            1000,
        );

        assert!(sql.contains("ROW_NUMBER() OVER (ORDER BY [Id])"));
        assert!(sql.contains("__rn >= 1"));
        assert!(sql.contains("__rn <= 1000"));
    }
}
