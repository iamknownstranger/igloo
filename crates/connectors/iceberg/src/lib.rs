//! Iceberg connector for Igloo
//!
//! Provides integration with Apache Iceberg tables for distributed query processing.

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};
use iceberg_datafusion::IcebergTableProvider;
use std::sync::Arc;
use tracing::{info, warn};

pub mod catalog;
pub mod table_provider;

pub use catalog::IcebergCatalog;
pub use table_provider::IglooIcebergTableProvider;

/// Configuration for Iceberg connector
#[derive(Debug, Clone)]
pub struct IcebergConfig {
    pub catalog_uri: String,
    pub warehouse_path: String,
    pub catalog_type: CatalogType,
}

#[derive(Debug, Clone)]
pub enum CatalogType {
    Rest,
    Hive,
    Memory,
    FileSystem,
}

impl Default for IcebergConfig {
    fn default() -> Self {
        Self {
            catalog_uri: "memory://".to_string(),
            warehouse_path: "./warehouse".to_string(),
            catalog_type: CatalogType::Memory,
        }
    }
}

/// Main Iceberg connector struct
pub struct IcebergConnector {
    config: IcebergConfig,
    catalog: Arc<dyn Catalog>,
}

impl IcebergConnector {
    /// Create a new Iceberg connector with the given configuration
    pub async fn new(config: IcebergConfig) -> Result<Self> {
        info!("Initializing Iceberg connector with config: {:?}", config);
        
        let catalog = Self::create_catalog(&config).await
            .context("Failed to create Iceberg catalog")?;

        Ok(Self {
            config,
            catalog,
        })
    }

    /// Create an Iceberg catalog based on configuration
    async fn create_catalog(config: &IcebergConfig) -> Result<Arc<dyn Catalog>> {
        match config.catalog_type {
            CatalogType::Memory => {
                info!("Creating in-memory Iceberg catalog");
                let catalog = iceberg::catalog::memory::MemoryCatalog::new();
                Ok(Arc::new(catalog))
            }
            CatalogType::FileSystem => {
                info!("Creating filesystem-based Iceberg catalog at: {}", config.warehouse_path);
                let catalog = iceberg::catalog::filesystem::FileSystemCatalog::new(
                    &config.warehouse_path
                ).await.context("Failed to create filesystem catalog")?;
                Ok(Arc::new(catalog))
            }
            _ => {
                warn!("Catalog type {:?} not yet implemented, falling back to memory", config.catalog_type);
                let catalog = iceberg::catalog::memory::MemoryCatalog::new();
                Ok(Arc::new(catalog))
            }
        }
    }

    /// Get a table by name
    pub async fn get_table(&self, namespace: &str, table_name: &str) -> Result<Arc<dyn TableProvider>> {
        info!("Loading Iceberg table: {}.{}", namespace, table_name);
        
        let table_ident = TableIdent::new(vec![namespace.to_string()], table_name.to_string());
        
        let table = self.catalog
            .load_table(&table_ident)
            .await
            .context(format!("Failed to load table {}.{}", namespace, table_name))?;

        let table_provider = IcebergTableProvider::try_new(table)
            .await
            .context("Failed to create Iceberg table provider")?;

        Ok(Arc::new(table_provider))
    }

    /// List all tables in a namespace
    pub async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        info!("Listing tables in namespace: {}", namespace);
        
        let namespace_ident = iceberg::NamespaceIdent::new(vec![namespace.to_string()]);
        
        let tables = self.catalog
            .list_tables(&namespace_ident)
            .await
            .context(format!("Failed to list tables in namespace {}", namespace))?;

        let table_names: Vec<String> = tables
            .into_iter()
            .map(|ident| ident.name().to_string())
            .collect();

        info!("Found {} tables in namespace {}", table_names.len(), namespace);
        Ok(table_names)
    }

    /// Create a new Iceberg table (for testing purposes)
    pub async fn create_table(
        &self,
        namespace: &str,
        table_name: &str,
        schema: arrow::datatypes::SchemaRef,
    ) -> Result<Arc<dyn TableProvider>> {
        info!("Creating Iceberg table: {}.{}", namespace, table_name);
        
        let table_ident = TableIdent::new(vec![namespace.to_string()], table_name.to_string());
        let namespace_ident = iceberg::NamespaceIdent::new(vec![namespace.to_string()]);
        
        // Ensure namespace exists
        if !self.catalog.namespace_exists(&namespace_ident).await? {
            self.catalog.create_namespace(&namespace_ident, Default::default()).await
                .context("Failed to create namespace")?;
        }

        // Convert Arrow schema to Iceberg schema
        let iceberg_schema = iceberg::spec::Schema::try_from(schema.as_ref())
            .context("Failed to convert Arrow schema to Iceberg schema")?;

        // Create table
        let table = self.catalog
            .create_table(&table_ident, iceberg_schema)
            .await
            .context("Failed to create Iceberg table")?;

        let table_provider = IcebergTableProvider::try_new(table)
            .await
            .context("Failed to create table provider for new table")?;

        Ok(Arc::new(table_provider))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_iceberg_connector_creation() {
        let config = IcebergConfig::default();
        let connector = IcebergConnector::new(config).await;
        assert!(connector.is_ok());
    }

    #[tokio::test]
    async fn test_create_and_list_tables() {
        let config = IcebergConfig::default();
        let connector = IcebergConnector::new(config).await.unwrap();

        // Create a test schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));

        // Create a table
        let result = connector.create_table("test_namespace", "test_table", schema).await;
        assert!(result.is_ok());

        // List tables
        let tables = connector.list_tables("test_namespace").await.unwrap();
        assert!(tables.contains(&"test_table".to_string()));
    }
}