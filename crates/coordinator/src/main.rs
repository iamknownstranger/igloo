use anyhow::Result;
use arrow_flight::flight_service_server::FlightServiceServer;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::print_batches;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use igloo_api::IglooFlightSqlService;
use igloo_common::catalog::MemoryCatalog;
use igloo_connector_iceberg::IcebergConfig;
use igloo_engine::{JoinConfig, JoinEngine, JoinType, QueryEngine};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tonic::transport::Server;
use tracing::{info, warn};

mod service;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    
    info!("Starting Igloo Coordinator with Parquet-Iceberg join capabilities");

    // 1. Instantiate the query engine and catalog
    let engine = Arc::new(QueryEngine::new());
    let mut catalog = MemoryCatalog::default();

    // 2. Register the CSV as a table in the catalog (existing functionality)
    let csv_relative_path = "crates/connectors/filesystem/test_data.csv";
    let base_path = std::env::current_dir()?;
    let csv_abs_path = base_path.join(Path::new(csv_relative_path));

    let table_path_url_str = format!("file://{}", csv_abs_path.display());
    let table_path = ListingTableUrl::parse(&table_path_url_str)?;

    let config = ListingTableConfig::new(table_path)
        .with_schema(Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Int64, false),
            Field::new("col_b", DataType::Utf8, false),
        ])))
        .with_listing_options(
            ListingOptions::new(Arc::new(CsvFormat::default().with_has_header(true)))
                .with_file_extension(".csv"),
        );

    let table_provider = Arc::new(ListingTable::try_new(config)?);
    catalog.register_table("test_table".to_string(), table_provider);
    info!("Registered test_table with the catalog");

    // 3. Register tables from catalog with the engine
    for (name, table) in catalog.tables.iter() {
        engine.register_table(name, table.clone())?;
        info!("Registered table '{}' with the query engine", name);
    }

    // 4. Execute a simple query on existing CSV data
    let sql = "SELECT col_a, col_b FROM test_table LIMIT 5;";
    info!("Executing SQL: {}", sql);
    let results = engine.execute(sql).await;

    info!("Query Results:");
    match print_batches(&results) {
        Ok(_) => {}
        Err(e) => warn!("Error printing batches: {}", e),
    }

    // 5. Demonstrate Parquet-Iceberg join capabilities
    info!("Demonstrating Parquet-Iceberg join capabilities");
    
    let iceberg_config = IcebergConfig::default();
    let join_engine = JoinEngine::new(iceberg_config.clone()).await?;
    
    // Create sample Parquet data for demonstration
    if let Err(e) = demonstrate_join_capabilities(&join_engine).await {
        warn!("Failed to demonstrate join capabilities: {}", e);
    }

    // 6. Start the Flight SQL server
    let addr: SocketAddr = "127.0.0.1:50051".parse()?;
    let flight_service = IglooFlightSqlService::new(engine.clone(), Arc::new(catalog));
    info!("Coordinator Flight SQL listening on {}", addr);

    Server::builder()
        .add_service(FlightServiceServer::new(flight_service))
        .serve_with_shutdown(addr, async {
            tokio::signal::ctrl_c().await.expect("failed to listen for event");
            info!("Shutting down coordinator gracefully...");
        })
        .await?;

    Ok(())
}

async fn demonstrate_join_capabilities(join_engine: &JoinEngine) -> Result<()> {
    info!("Creating sample data for join demonstration");
    
    // Create sample Parquet data in memory
    use arrow::array::{Int64Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use datafusion::catalog::MemTable;
    use std::sync::Arc;

    // Sample customer data (simulating Parquet)
    let customer_schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("customer_name", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
    ]));

    let customer_batch = RecordBatch::try_new(
        customer_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana", "Eve"])),
            Arc::new(StringArray::from(vec!["US", "EU", "US", "APAC", "EU"])),
        ],
    )?;

    // Sample order data (simulating Iceberg)
    let order_schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("order_amount", DataType::Int64, false),
        Field::new("order_date", DataType::Utf8, false),
    ]));

    let order_batch = RecordBatch::try_new(
        order_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 1, 4])),
            Arc::new(Int64Array::from(vec![100, 250, 75, 150, 300])),
            Arc::new(StringArray::from(vec![
                "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"
            ])),
        ],
    )?;

    // Register tables with the join engine
    let customer_table = MemTable::try_new(customer_schema, vec![vec![customer_batch]])?;
    let order_table = MemTable::try_new(order_schema, vec![vec![order_batch]])?;

    let ctx = join_engine.session_context();
    ctx.register_table("customers", Arc::new(customer_table))?;
    ctx.register_table("orders", Arc::new(order_table))?;

    // Configure and execute join
    let join_config = JoinConfig {
        parquet_path: "memory://customers".to_string(),
        iceberg_config: IcebergConfig::default(),
        join_type: JoinType::Inner,
        join_keys: vec![("customer_id".to_string(), "customer_id".to_string())],
    };

    info!("Executing inner join between customers and orders");
    let join_results = join_engine.join_tables("customers", "orders", &join_config).await?;

    info!("Join Results:");
    match print_batches(&join_results) {
        Ok(_) => {}
        Err(e) => warn!("Error printing join results: {}", e),
    }

    let total_rows: usize = join_results.iter().map(|batch| batch.num_rows()).sum();
    info!("Join completed successfully with {} total rows", total_rows);

    // Demonstrate advanced join with filters
    info!("Demonstrating advanced join with filters");
    
    use datafusion::prelude::*;
    
    let parquet_filters = Some(vec![col("region").eq(lit("US"))]);
    let iceberg_filters = Some(vec![col("order_amount").gt(lit(100))]);
    let projections = Some(vec![
        "customer_name".to_string(),
        "region".to_string(),
        "order_amount".to_string(),
    ]);

    let advanced_results = join_engine.join_tables_advanced(
        "customers",
        "orders",
        &join_config,
        parquet_filters,
        iceberg_filters,
        projections,
    ).await?;

    info!("Advanced Join Results (US customers with orders > 100):");
    match print_batches(&advanced_results) {
        Ok(_) => {}
        Err(e) => warn!("Error printing advanced join results: {}", e),
    }

    Ok(())
}