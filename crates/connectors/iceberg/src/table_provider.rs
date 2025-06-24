//! Custom table provider for Iceberg tables in Igloo

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use iceberg::table::Table;
use iceberg_datafusion::IcebergTableProvider;
use std::sync::Arc;
use tracing::{info, warn};

/// Igloo-specific wrapper around IcebergTableProvider
pub struct IglooIcebergTableProvider {
    inner: IcebergTableProvider,
    table: Table,
}

impl IglooIcebergTableProvider {
    pub async fn try_new(table: Table) -> Result<Self> {
        info!("Creating Igloo Iceberg table provider for table: {}", table.identifier());
        
        let inner = IcebergTableProvider::try_new(table.clone()).await?;
        
        Ok(Self { inner, table })
    }

    /// Get the underlying Iceberg table
    pub fn table(&self) -> &Table {
        &self.table
    }

    /// Get table metadata
    pub fn metadata(&self) -> &iceberg::table::TableMetadata {
        self.table.metadata()
    }

    /// Scan the table with optional filters and projections
    pub async fn scan_with_filters(
        &self,
        ctx: &SessionContext,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        info!("Scanning Iceberg table with {} filters", filters.len());
        
        // Create a DataFrame from the table provider
        let df = ctx.read_table(Arc::new(self.inner.clone()))?;
        
        // Apply filters
        let mut filtered_df = df;
        for filter in filters {
            filtered_df = filtered_df.filter(filter.clone())?;
        }
        
        // Apply projection if specified
        if let Some(proj_indices) = projection {
            let schema = self.inner.schema();
            let projected_fields: Vec<Expr> = proj_indices
                .iter()
                .map(|&i| col(schema.field(i).name()))
                .collect();
            filtered_df = filtered_df.select(projected_fields)?;
        }
        
        // Apply limit if specified
        if let Some(limit_count) = limit {
            filtered_df = filtered_df.limit(0, Some(limit_count))?;
        }
        
        // Collect results
        let batches = filtered_df.collect().await?;
        info!("Scanned {} record batches from Iceberg table", batches.len());
        
        Ok(batches)
    }
}

// Delegate TableProvider implementation to the inner provider
#[async_trait::async_trait]
impl TableProvider for IglooIcebergTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }

    async fn scan(
        &self,
        state: &datafusion::execution::context::SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn datafusion::execution::ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }
}