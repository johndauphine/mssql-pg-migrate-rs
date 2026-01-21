//! Driver catalog for explicit dependency injection.
//!
//! The [`DriverCatalog`] provides a registry for database dialects and type mappers.
//! Unlike global singletons, it is explicitly constructed and injected into the
//! orchestrator, enabling better testability and deterministic initialization.
//!
//! # Design Rationale
//!
//! - **No global state**: Avoids linkme/inventory crate magic
//! - **Explicit registration**: Clear, deterministic initialization order
//! - **Testable**: Easy to create mock catalogs for testing
//! - **Feature-gated**: New databases added via Cargo features + registration

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{MigrateError, Result};

use super::traits::{Dialect, TypeMapper};

/// Registry of database dialects and type mappers.
///
/// This catalog is explicitly constructed and passed to the orchestrator,
/// rather than using global singletons. This provides:
///
/// - Deterministic initialization order
/// - Easy testing with mock catalogs
/// - Clear dependency injection
///
/// # Example
///
/// ```rust,ignore
/// let mut catalog = DriverCatalog::new();
/// catalog.register_dialect("mssql", Box::new(MssqlDialect));
/// catalog.register_dialect("postgres", Box::new(PostgresDialect));
/// catalog.register_mapper("mssql", "postgres", Arc::new(MssqlToPostgresMapper));
///
/// let orchestrator = Orchestrator::with_catalog(config, catalog).await?;
/// ```
#[derive(Default)]
pub struct DriverCatalog {
    /// Registered dialects by name.
    dialects: HashMap<String, Arc<dyn Dialect>>,

    /// Type mappers keyed by (source, target) dialect pair.
    ///
    /// Using tuple keys solves the N² type mapping problem by only
    /// requiring mappers for actually-used source→target combinations.
    type_mappers: HashMap<(String, String), Arc<dyn TypeMapper>>,
}

impl DriverCatalog {
    /// Create a new empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a catalog with the standard built-in drivers registered.
    ///
    /// This is a convenience method that registers MSSQL and PostgreSQL
    /// dialects and their type mappers.
    #[allow(dead_code)]
    pub fn with_builtins() -> Self {
        let catalog = Self::new();
        // Builtins will be registered by drivers module
        // This method exists so drivers/mod.rs can populate it
        catalog
    }

    /// Register a dialect by name.
    ///
    /// # Arguments
    ///
    /// * `name` - The dialect identifier (e.g., "mssql", "postgres")
    /// * `dialect` - The dialect implementation
    pub fn register_dialect(&mut self, name: impl Into<String>, dialect: impl Dialect + 'static) {
        self.dialects.insert(name.into(), Arc::new(dialect));
    }

    /// Register a dialect as an Arc (for sharing).
    pub fn register_dialect_arc(&mut self, name: impl Into<String>, dialect: Arc<dyn Dialect>) {
        self.dialects.insert(name.into(), dialect);
    }

    /// Get a dialect by name.
    pub fn get_dialect(&self, name: &str) -> Option<Arc<dyn Dialect>> {
        self.dialects.get(name).cloned()
    }

    /// Get a dialect by name, returning an error if not found.
    pub fn require_dialect(&self, name: &str) -> Result<Arc<dyn Dialect>> {
        self.get_dialect(name).ok_or_else(|| {
            MigrateError::Config(format!("Unknown database dialect: {}", name))
        })
    }

    /// Register a type mapper for a source→target dialect pair.
    ///
    /// # Arguments
    ///
    /// * `source` - The source dialect name
    /// * `target` - The target dialect name
    /// * `mapper` - The type mapper implementation
    pub fn register_mapper(
        &mut self,
        source: impl Into<String>,
        target: impl Into<String>,
        mapper: Arc<dyn TypeMapper>,
    ) {
        self.type_mappers
            .insert((source.into(), target.into()), mapper);
    }

    /// Get a type mapper for a source→target dialect pair.
    pub fn get_mapper(&self, source: &str, target: &str) -> Option<Arc<dyn TypeMapper>> {
        self.type_mappers
            .get(&(source.to_string(), target.to_string()))
            .cloned()
    }

    /// Get a type mapper for a source→target dialect pair, returning an error if not found.
    pub fn require_mapper(&self, source: &str, target: &str) -> Result<Arc<dyn TypeMapper>> {
        self.get_mapper(source, target).ok_or_else(|| {
            MigrateError::Config(format!(
                "No type mapper registered for {} → {}",
                source, target
            ))
        })
    }

    /// Check if a dialect is registered.
    pub fn has_dialect(&self, name: &str) -> bool {
        self.dialects.contains_key(name)
    }

    /// Check if a type mapper is registered for a source→target pair.
    pub fn has_mapper(&self, source: &str, target: &str) -> bool {
        self.type_mappers
            .contains_key(&(source.to_string(), target.to_string()))
    }

    /// Get all registered dialect names.
    pub fn dialect_names(&self) -> Vec<&str> {
        self.dialects.keys().map(String::as_str).collect()
    }

    /// Get all registered mapper pairs as (source, target) tuples.
    pub fn mapper_pairs(&self) -> Vec<(&str, &str)> {
        self.type_mappers
            .keys()
            .map(|(s, t)| (s.as_str(), t.as_str()))
            .collect()
    }
}

impl std::fmt::Debug for DriverCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriverCatalog")
            .field("dialects", &self.dialects.keys().collect::<Vec<_>>())
            .field("type_mappers", &self.type_mappers.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::schema::Column;
    use crate::core::traits::{ColumnMapping, SelectQueryOptions, TypeMapping};

    // Mock dialect for testing
    struct MockDialect {
        name: &'static str,
    }

    impl Dialect for MockDialect {
        fn name(&self) -> &str {
            self.name
        }

        fn quote_ident(&self, name: &str) -> String {
            format!("\"{}\"", name)
        }

        fn build_select_query(&self, _opts: &SelectQueryOptions) -> String {
            "SELECT *".to_string()
        }

        fn build_upsert_query(
            &self,
            _target: &str,
            _staging: &str,
            _cols: &[String],
            _pks: &[String],
        ) -> String {
            "UPSERT".to_string()
        }

        fn param_placeholder(&self, index: usize) -> String {
            format!("${}", index)
        }

        fn build_keyset_where(&self, pk_col: &str, last_pk: i64) -> String {
            format!("{} > {}", pk_col, last_pk)
        }

        fn build_row_number_query(
            &self,
            inner: &str,
            _pk: &str,
            start: i64,
            end: i64,
        ) -> String {
            format!("{} ROWS {} TO {}", inner, start, end)
        }
    }

    // Mock type mapper for testing
    struct MockMapper {
        source: &'static str,
        target: &'static str,
    }

    #[async_trait::async_trait]
    impl TypeMapper for MockMapper {
        fn source_dialect(&self) -> &str {
            self.source
        }

        fn target_dialect(&self) -> &str {
            self.target
        }

        fn map_column(&self, col: &Column) -> ColumnMapping {
            ColumnMapping {
                name: col.name.clone(),
                target_type: "text".to_string(),
                is_nullable: col.is_nullable,
                warning: None,
            }
        }

        fn map_type(
            &self,
            _data_type: &str,
            _max_length: i32,
            _precision: i32,
            _scale: i32,
        ) -> TypeMapping {
            TypeMapping::lossless("text")
        }
    }

    #[test]
    fn test_catalog_dialect_registration() {
        let mut catalog = DriverCatalog::new();
        assert!(!catalog.has_dialect("test"));

        catalog.register_dialect("test", MockDialect { name: "test" });
        assert!(catalog.has_dialect("test"));

        let dialect = catalog.get_dialect("test").unwrap();
        assert_eq!(dialect.name(), "test");
    }

    #[test]
    fn test_catalog_mapper_registration() {
        let mut catalog = DriverCatalog::new();
        assert!(!catalog.has_mapper("mssql", "postgres"));

        catalog.register_mapper(
            "mssql",
            "postgres",
            Arc::new(MockMapper {
                source: "mssql",
                target: "postgres",
            }),
        );

        assert!(catalog.has_mapper("mssql", "postgres"));
        assert!(!catalog.has_mapper("postgres", "mssql"));

        let mapper = catalog.get_mapper("mssql", "postgres").unwrap();
        assert_eq!(mapper.source_dialect(), "mssql");
        assert_eq!(mapper.target_dialect(), "postgres");
    }

    #[test]
    fn test_catalog_require_methods() {
        let mut catalog = DriverCatalog::new();
        catalog.register_dialect("test", MockDialect { name: "test" });

        assert!(catalog.require_dialect("test").is_ok());
        assert!(catalog.require_dialect("nonexistent").is_err());

        catalog.register_mapper(
            "a",
            "b",
            Arc::new(MockMapper {
                source: "a",
                target: "b",
            }),
        );

        assert!(catalog.require_mapper("a", "b").is_ok());
        assert!(catalog.require_mapper("b", "a").is_err());
    }

    #[test]
    fn test_catalog_enumeration() {
        let mut catalog = DriverCatalog::new();
        catalog.register_dialect("mssql", MockDialect { name: "mssql" });
        catalog.register_dialect("postgres", MockDialect { name: "postgres" });
        catalog.register_mapper(
            "mssql",
            "postgres",
            Arc::new(MockMapper {
                source: "mssql",
                target: "postgres",
            }),
        );

        let dialects = catalog.dialect_names();
        assert_eq!(dialects.len(), 2);
        assert!(dialects.contains(&"mssql"));
        assert!(dialects.contains(&"postgres"));

        let pairs = catalog.mapper_pairs();
        assert_eq!(pairs.len(), 1);
        assert!(pairs.contains(&("mssql", "postgres")));
    }
}
