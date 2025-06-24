//! Join engine for combining data from Parquet and Iceberg tables

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion::execution::context::SessionContext;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use igloo_connector_iceberg::{IcebergConnector, IcebergConfig};
use std::sync::Arc;
use tracing::{info, warn};

/// Configuration for join operations
#[derive(Debug, Clone)]
pub struct JoinConfig {
    pub parquet_path: String,
    pub iceberg_config: IcebergConfig,
    pub join_type: JoinType,
    pub join_keys: Vec<(String, String)>, // (parquet_column, iceberg_column)
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl From<JoinType> for datafusion::logical_expr::JoinType {
    fn from(join_type: JoinType) -> Self {
        match join_type {
            JoinType::Inner => datafusion::logical_expr::JoinType::Inner,
            JoinType::Left => datafusion::logical_expr::JoinType::Left,
            JoinType::Right => datafusion::logical_expr::JoinType::Right,
            JoinType::Full => datafusion::logical_expr::JoinType::Full,
        }
    }
}

/// Engine for performing joins between Parquet and Iceberg tables
pub struct JoinEngine {
    ctx: SessionContext,
    iceberg_connector: IcebergConnector,
}

impl JoinEngine {
    /// Create a new join engine
    pub async fn new(iceberg_config: IcebergConfig) -> Result<Self> {
        info!("Initializing join engine");
        
        let ctx = SessionContext::new();
        let iceberg_connector = IcebergConnector::new(iceberg_config).await
            .context("Failed to create Iceberg connector")?;

        Ok(Self {
            ctx,
            iceberg_connector,
        })
    }

    /// Register a Parquet table from a file or directory
    pub async fn register_parquet_table(
        &self,
        table_name: &str,
        path: &str,
        schema: Option<arrow::datatypes::SchemaRef>,
    ) -> Result<()> {
        info!("Registering Parquet table '{}' from path: {}", table_name, path);

        let table_path = ListingTableUrl::parse(path)
            .context("Failed to parse Parquet table path")?;

        let mut config_builder = ListingTableConfig::new(table_path)
            .with_listing_options(
                ListingOptions::new(Arc::new(ParquetFormat::default()))
                    .with_file_extension(".parquet")
            );

        if let Some(schema_ref) = schema {
            config_builder = config_builder.with_schema(schema_ref);
        }

        let config = config_builder;
        let table = ListingTable::try_new(config)
            .context("Failed to create Parquet listing table")?;

        self.ctx.register_table(table_name, Arc::new(table))
            .context("Failed to register Parquet table with DataFusion context")?;

        info!("Successfully registered Parquet table '{}'", table_name);
        Ok(())
    }

    /// Register an Iceberg table
    pub async fn register_iceberg_table(
        &self,
        table_name: &str,
        namespace: &str,
        iceberg_table_name: &str,
    ) -> Result<()> {
        info!("Registering Iceberg table '{}' from {}.{}", table_name, namespace, iceberg_table_name);

        let table_provider = self.iceberg_connector
            .get_table(namespace, iceberg_table_name)
            .await
            .context("Failed to get Iceberg table")?;

        self.ctx.register_table(table_name, table_provider)
            .context("Failed to register Iceberg table with DataFusion context")?;

        info!("Successfully registered Iceberg table '{}'", table_name);
        Ok(())
    }

    /// Perform a join between Parquet and Iceberg tables
    pub async fn join_tables(
        &self,
        parquet_table: &str,
        iceberg_table: &str,
        config: &JoinConfig,
    ) -> Result<Vec<RecordBatch>> {
        info!(
            "Performing {:?} join between Parquet table '{}' and Iceberg table '{}'",
            config.join_type, parquet_table, iceberg_table
        );

        if config.join_keys.is_empty() {
            return Err(anyhow::anyhow!("Join keys cannot be empty"));
        }

        // Build the join SQL query
        let join_conditions: Vec<String> = config.join_keys
            .iter()
            .map(|(parquet_col, iceberg_col)| {
                format!("{}.{} = {}.{}", parquet_table, parquet_col, iceberg_table, iceberg_col)
            })
            .collect();

        let join_condition = join_conditions.join(" AND ");
        
        let join_type_sql = match config.join_type {
            JoinType::Inner => "INNER JOIN",
            JoinType::Left => "LEFT JOIN",
            JoinType::Right => "RIGHT JOIN",
            JoinType::Full => "FULL OUTER JOIN",
        };

        let sql = format!(
            "SELECT * FROM {} {} {} ON {}",
            parquet_table, join_type_sql, iceberg_table, join_condition
        );

        info!("Executing join SQL: {}", sql);

        let df = self.ctx.sql(&sql).await
            .context("Failed to execute join SQL")?;

        let batches = df.collect().await
            .context("Failed to collect join results")?;

        info!("Join completed successfully, returned {} batches", batches.len());
        Ok(batches)
    }

    /// Perform a more complex join with custom projections and filters
    pub async fn join_tables_advanced(
        &self,
        parquet_table: &str,
        iceberg_table: &str,
        config: &JoinConfig,
        parquet_filters: Option<Vec<Expr>>,
        iceberg_filters: Option<Vec<Expr>>,
        projections: Option<Vec<String>>,
    ) -> Result<Vec<RecordBatch>> {
        info!("Performing advanced join with custom filters and projections");

        // Get DataFrames for both tables
        let mut parquet_df = self.ctx.table(parquet_table).await
            .context("Failed to get Parquet table DataFrame")?;
        
        let mut iceberg_df = self.ctx.table(iceberg_table).await
            .context("Failed to get Iceberg table DataFrame")?;

        // Apply filters if provided
        if let Some(filters) = parquet_filters {
            for filter in filters {
                parquet_df = parquet_df.filter(filter)
                    .context("Failed to apply filter to Parquet table")?;
            }
        }

        if let Some(filters) = iceberg_filters {
            for filter in filters {
                iceberg_df = iceberg_df.filter(filter)
                    .context("Failed to apply filter to Iceberg table")?;
            }
        }

        // Build join expressions
        let join_exprs: Vec<(Expr, Expr)> = config.join_keys
            .iter()
            .map(|(parquet_col, iceberg_col)| {
                (col(parquet_col), col(iceberg_col))
            })
            .collect();

        if join_exprs.is_empty() {
            return Err(anyhow::anyhow!("Join expressions cannot be empty"));
        }

        // Perform the join
        let mut joined_df = parquet_df.join(
            iceberg_df,
            config.join_type.clone().into(),
            &join_exprs,
            None,
        ).context("Failed to perform DataFrame join")?;

        // Apply projections if provided
        if let Some(proj_cols) = projections {
            let proj_exprs: Vec<Expr> = proj_cols
                .into_iter()
                .map(|col_name| col(col_name))
                .collect();
            joined_df = joined_df.select(proj_exprs)
                .context("Failed to apply projections")?;
        }

        // Collect results
        let batches = joined_df.collect().await
            .context("Failed to collect advanced join results")?;

        info!("Advanced join completed successfully, returned {} batches", batches.len());
        Ok(batches)
    }

    /// Get table schema information
    pub async fn get_table_schema(&self, table_name: &str) -> Result<arrow::datatypes::SchemaRef> {
        let df = self.ctx.table(table_name).await
            .context("Failed to get table DataFrame")?;
        Ok(df.schema())
    }

    /// List all registered tables
    pub fn list_tables(&self) -> Vec<String> {
        self.ctx.catalog("datafusion").unwrap()
            .schema("public").unwrap()
            .table_names()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::fs;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn create_test_parquet_file(temp_dir: &TempDir) -> Result<String> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )?;

        let file_path = temp_dir.path().join("test.parquet");
        let file = fs::File::create(&file_path)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(file_path.to_string_lossy().to_string())
    }

    #[tokio::test]
    async fn test_join_engine_creation() {
        let config = IcebergConfig::default();
        let engine = JoinEngine::new(config).await;
        assert!(engine.is_ok());
    }

    #[tokio::test]
    async fn test_register_parquet_table() {
        let temp_dir = TempDir::new().unwrap();
        let parquet_path = create_test_parquet_file(&temp_dir).await.unwrap();
        
        let config = IcebergConfig::default();
        let engine = JoinEngine::new(config).await.unwrap();
        
        let result = engine.register_parquet_table("test_parquet", &parquet_path, None).await;
        assert!(result.is_ok());
        
        let tables = engine.list_tables();
        assert!(tables.contains(&"test_parquet".to_string()));
    }

    #[tokio::test]
    async fn test_get_table_schema() {
        let temp_dir = TempDir::new().unwrap();
        let parquet_path = create_test_parquet_file(&temp_dir).await.unwrap();
        
        let config = IcebergConfig::default();
        let engine = JoinEngine::new(config).await.unwrap();
        
        engine.register_parquet_table("test_parquet", &parquet_path, None).await.unwrap();
        
        let schema = engine.get_table_schema("test_parquet").await.unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }
}