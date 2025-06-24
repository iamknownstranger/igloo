//! Integration tests for Parquet-Iceberg join operations

use anyhow::Result;
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use igloo_connector_iceberg::IcebergConfig;
use igloo_engine::{JoinConfig, JoinEngine, JoinType};
use parquet::arrow::ArrowWriter;
use std::fs;
use std::sync::Arc;
use tempfile::TempDir;

/// Create a test Parquet file with sample data
async fn create_test_parquet_file(temp_dir: &TempDir, file_name: &str) -> Result<String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("customer_name", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana", "Eve"])),
            Arc::new(StringArray::from(vec!["US", "EU", "US", "APAC", "EU"])),
        ],
    )?;

    let file_path = temp_dir.path().join(file_name);
    let file = fs::File::create(&file_path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(file_path.to_string_lossy().to_string())
}

/// Create a test Iceberg table with sample data
async fn create_test_iceberg_table(
    engine: &JoinEngine,
    table_name: &str,
) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("order_amount", DataType::Float64, false),
        Field::new("order_date", DataType::Utf8, false),
    ]));

    // For this test, we'll create a simple in-memory Iceberg table
    // In a real scenario, this would involve writing to actual Iceberg format
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 1, 4])),
            Arc::new(Float64Array::from(vec![100.0, 250.0, 75.0, 150.0, 300.0])),
            Arc::new(StringArray::from(vec![
                "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"
            ])),
        ],
    )?;

    // Register as a memory table for testing purposes
    let ctx = engine.session_context();
    let mem_table = datafusion::catalog::MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table(table_name, Arc::new(mem_table))?;

    Ok(())
}

#[tokio::test]
async fn test_parquet_iceberg_inner_join() -> Result<()> {
    // Setup
    let temp_dir = TempDir::new()?;
    let parquet_path = create_test_parquet_file(&temp_dir, "customers.parquet").await?;
    
    let iceberg_config = IcebergConfig::default();
    let engine = JoinEngine::new(iceberg_config).await?;
    
    // Register tables
    engine.register_parquet_table("customers", &parquet_path, None).await?;
    create_test_iceberg_table(&engine, "orders").await?;
    
    // Configure join
    let join_config = JoinConfig {
        parquet_path: parquet_path.clone(),
        iceberg_config: IcebergConfig::default(),
        join_type: JoinType::Inner,
        join_keys: vec![("customer_id".to_string(), "customer_id".to_string())],
    };
    
    // Perform join
    let results = engine.join_tables("customers", "orders", &join_config).await?;
    
    // Verify results
    assert!(!results.is_empty(), "Join should return results");
    
    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    assert!(total_rows > 0, "Join should return at least one row");
    
    // Verify schema contains columns from both tables
    let schema = &results[0].schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    
    assert!(field_names.contains(&"customer_name"), "Should contain customer_name from Parquet");
    assert!(field_names.contains(&"order_amount"), "Should contain order_amount from Iceberg");
    
    println!("Inner join test passed with {} total rows", total_rows);
    Ok(())
}

#[tokio::test]
async fn test_parquet_iceberg_left_join() -> Result<()> {
    // Setup
    let temp_dir = TempDir::new()?;
    let parquet_path = create_test_parquet_file(&temp_dir, "customers.parquet").await?;
    
    let iceberg_config = IcebergConfig::default();
    let engine = JoinEngine::new(iceberg_config).await?;
    
    // Register tables
    engine.register_parquet_table("customers", &parquet_path, None).await?;
    create_test_iceberg_table(&engine, "orders").await?;
    
    // Configure left join
    let join_config = JoinConfig {
        parquet_path: parquet_path.clone(),
        iceberg_config: IcebergConfig::default(),
        join_type: JoinType::Left,
        join_keys: vec![("customer_id".to_string(), "customer_id".to_string())],
    };
    
    // Perform join
    let results = engine.join_tables("customers", "orders", &join_config).await?;
    
    // Verify results
    assert!(!results.is_empty(), "Left join should return results");
    
    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    
    // Left join should return all customers (5), even those without orders
    assert!(total_rows >= 5, "Left join should return at least 5 rows (all customers)");
    
    println!("Left join test passed with {} total rows", total_rows);
    Ok(())
}

#[tokio::test]
async fn test_advanced_join_with_filters() -> Result<()> {
    // Setup
    let temp_dir = TempDir::new()?;
    let parquet_path = create_test_parquet_file(&temp_dir, "customers.parquet").await?;
    
    let iceberg_config = IcebergConfig::default();
    let engine = JoinEngine::new(iceberg_config).await?;
    
    // Register tables
    engine.register_parquet_table("customers", &parquet_path, None).await?;
    create_test_iceberg_table(&engine, "orders").await?;
    
    // Configure join with filters
    let join_config = JoinConfig {
        parquet_path: parquet_path.clone(),
        iceberg_config: IcebergConfig::default(),
        join_type: JoinType::Inner,
        join_keys: vec![("customer_id".to_string(), "customer_id".to_string())],
    };
    
    // Define filters
    let parquet_filters = Some(vec![
        col("region").eq(lit("US"))
    ]);
    
    let iceberg_filters = Some(vec![
        col("order_amount").gt(lit(100.0))
    ]);
    
    let projections = Some(vec![
        "customer_name".to_string(),
        "region".to_string(),
        "order_amount".to_string(),
    ]);
    
    // Perform advanced join
    let results = engine.join_tables_advanced(
        "customers",
        "orders",
        &join_config,
        parquet_filters,
        iceberg_filters,
        projections,
    ).await?;
    
    // Verify results
    assert!(!results.is_empty(), "Advanced join should return results");
    
    let schema = &results[0].schema();
    assert_eq!(schema.fields().len(), 3, "Should have exactly 3 projected columns");
    
    // Verify projected columns
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(field_names.contains(&"customer_name"));
    assert!(field_names.contains(&"region"));
    assert!(field_names.contains(&"order_amount"));
    
    println!("Advanced join with filters test passed");
    Ok(())
}

#[tokio::test]
async fn test_schema_compatibility() -> Result<()> {
    // Setup
    let temp_dir = TempDir::new()?;
    let parquet_path = create_test_parquet_file(&temp_dir, "customers.parquet").await?;
    
    let iceberg_config = IcebergConfig::default();
    let engine = JoinEngine::new(iceberg_config).await?;
    
    // Register tables
    engine.register_parquet_table("customers", &parquet_path, None).await?;
    create_test_iceberg_table(&engine, "orders").await?;
    
    // Get schemas
    let parquet_schema = engine.get_table_schema("customers").await?;
    let iceberg_schema = engine.get_table_schema("orders").await?;
    
    // Verify schemas
    assert!(parquet_schema.fields().len() > 0, "Parquet table should have fields");
    assert!(iceberg_schema.fields().len() > 0, "Iceberg table should have fields");
    
    // Check for common join key
    let parquet_has_customer_id = parquet_schema.fields()
        .iter()
        .any(|f| f.name() == "customer_id");
    let iceberg_has_customer_id = iceberg_schema.fields()
        .iter()
        .any(|f| f.name() == "customer_id");
    
    assert!(parquet_has_customer_id, "Parquet table should have customer_id field");
    assert!(iceberg_has_customer_id, "Iceberg table should have customer_id field");
    
    println!("Schema compatibility test passed");
    Ok(())
}

#[tokio::test]
async fn test_join_performance_metrics() -> Result<()> {
    use std::time::Instant;
    
    // Setup
    let temp_dir = TempDir::new()?;
    let parquet_path = create_test_parquet_file(&temp_dir, "customers.parquet").await?;
    
    let iceberg_config = IcebergConfig::default();
    let engine = JoinEngine::new(iceberg_config).await?;
    
    // Register tables
    engine.register_parquet_table("customers", &parquet_path, None).await?;
    create_test_iceberg_table(&engine, "orders").await?;
    
    // Configure join
    let join_config = JoinConfig {
        parquet_path: parquet_path.clone(),
        iceberg_config: IcebergConfig::default(),
        join_type: JoinType::Inner,
        join_keys: vec![("customer_id".to_string(), "customer_id".to_string())],
    };
    
    // Measure join performance
    let start = Instant::now();
    let results = engine.join_tables("customers", "orders", &join_config).await?;
    let duration = start.elapsed();
    
    // Verify results and performance
    assert!(!results.is_empty(), "Join should return results");
    
    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    println!("Join completed in {:?} with {} rows", duration, total_rows);
    
    // Performance should be reasonable for small test data
    assert!(duration.as_millis() < 5000, "Join should complete within 5 seconds for test data");
    
    Ok(())
}