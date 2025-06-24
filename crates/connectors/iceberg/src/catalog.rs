//! Iceberg catalog integration for Igloo

use anyhow::Result;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// Wrapper around Iceberg catalog for Igloo-specific functionality
pub struct IcebergCatalog {
    inner: Arc<dyn Catalog>,
}

impl IcebergCatalog {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { inner: catalog }
    }

    /// Get the underlying catalog
    pub fn inner(&self) -> &Arc<dyn Catalog> {
        &self.inner
    }

    /// List all namespaces
    pub async fn list_namespaces(&self) -> Result<Vec<String>> {
        info!("Listing all namespaces");
        let namespaces = self.inner.list_namespaces(None).await?;
        let namespace_names: Vec<String> = namespaces
            .into_iter()
            .map(|ns| ns.to_string())
            .collect();
        Ok(namespace_names)
    }

    /// Get namespace properties
    pub async fn get_namespace_properties(&self, namespace: &str) -> Result<HashMap<String, String>> {
        let namespace_ident = NamespaceIdent::new(vec![namespace.to_string()]);
        let properties = self.inner.get_namespace(&namespace_ident).await?;
        Ok(properties)
    }

    /// Check if a table exists
    pub async fn table_exists(&self, namespace: &str, table_name: &str) -> Result<bool> {
        let table_ident = TableIdent::new(vec![namespace.to_string()], table_name.to_string());
        let exists = self.inner.table_exists(&table_ident).await?;
        Ok(exists)
    }
}