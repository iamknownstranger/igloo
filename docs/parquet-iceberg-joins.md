# Parquet-Iceberg Join Operations in Igloo

This document provides comprehensive documentation for the Parquet-Iceberg join functionality in Igloo.

## Overview

Igloo's Parquet-Iceberg join feature enables efficient join operations between data stored in Parquet files and Apache Iceberg tables. This capability bridges the gap between different storage formats commonly used in modern data architectures.

## Architecture

### Components

1. **JoinEngine**: Core engine responsible for orchestrating join operations
2. **IcebergConnector**: Handles connections to Iceberg catalogs and tables
3. **ParquetTableProvider**: Manages access to Parquet files through DataFusion
4. **Schema Compatibility Layer**: Ensures proper schema alignment between formats

### Data Flow

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│   Parquet   │    │ JoinEngine   │    │   Iceberg   │
│    Files    │───▶│              │◀───│   Tables    │
└─────────────┘    │              │    └─────────────┘
                   │  DataFusion  │
                   │   Executor   │
                   └──────┬───────┘
                          │
                          ▼
                   ┌─────────────┐
                   │   Results   │
                   └─────────────┘
```

## Features

### Supported Join Types

- **INNER JOIN**: Returns only matching records from both tables
- **LEFT JOIN**: Returns all records from Parquet table, matched records from Iceberg
- **RIGHT JOIN**: Returns all records from Iceberg table, matched records from Parquet
- **FULL OUTER JOIN**: Returns all records from both tables

### Advanced Capabilities

- **Predicate Pushdown**: Filters applied at source level for optimal performance
- **Projection Pushdown**: Only required columns are read from storage
- **Schema Evolution**: Handles schema differences between formats
- **Parallel Execution**: Leverages DataFusion's parallel processing capabilities

## Usage Examples

### Basic Join Configuration

```rust
use igloo_engine::{JoinEngine, JoinConfig, JoinType};
use igloo_connector_iceberg::IcebergConfig;

let iceberg_config = IcebergConfig {
    catalog_uri: "memory://".to_string(),
    warehouse_path: "./warehouse".to_string(),
    catalog_type: CatalogType::Memory,
};

let engine = JoinEngine::new(iceberg_config).await?;

let join_config = JoinConfig {
    parquet_path: "/path/to/data.parquet".to_string(),
    iceberg_config: IcebergConfig::default(),
    join_type: JoinType::Inner,
    join_keys: vec![("id".to_string(), "id".to_string())],
};
```

### Registering Tables

```rust
// Register Parquet table
engine.register_parquet_table(
    "customers", 
    "/data/customers.parquet", 
    None // Optional schema
).await?;

// Register Iceberg table
engine.register_iceberg_table(
    "orders", 
    "warehouse_namespace", 
    "orders_table"
).await?;
```

### Executing Joins

```rust
// Simple join
let results = engine.join_tables(
    "customers", 
    "orders", 
    &join_config
).await?;

// Advanced join with filters
let parquet_filters = Some(vec![col("region").eq(lit("US"))]);
let iceberg_filters = Some(vec![col("amount").gt(lit(100.0))]);
let projections = Some(vec![
    "customer_name".to_string(),
    "order_amount".to_string(),
]);

let results = engine.join_tables_advanced(
    "customers",
    "orders",
    &join_config,
    parquet_filters,
    iceberg_filters,
    projections,
).await?;
```

## Configuration Options

### IcebergConfig

```rust
pub struct IcebergConfig {
    pub catalog_uri: String,      // Catalog connection URI
    pub warehouse_path: String,   // Warehouse root path
    pub catalog_type: CatalogType, // Catalog implementation type
}

pub enum CatalogType {
    Rest,        // REST catalog
    Hive,        // Hive metastore
    Memory,      // In-memory (testing)
    FileSystem,  // Filesystem-based
}
```

### JoinConfig

```rust
pub struct JoinConfig {
    pub parquet_path: String,           // Path to Parquet data
    pub iceberg_config: IcebergConfig,  // Iceberg configuration
    pub join_type: JoinType,            // Type of join operation
    pub join_keys: Vec<(String, String)>, // Join key pairs
}
```

## Performance Considerations

### Optimization Strategies

1. **Predicate Pushdown**: Apply filters early to reduce data movement
2. **Projection Pushdown**: Select only necessary columns
3. **Partition Pruning**: Skip irrelevant partitions in both formats
4. **Columnar Processing**: Leverage Arrow's columnar format for efficiency

### Best Practices

- Use appropriate join keys with good selectivity
- Apply filters before joins when possible
- Consider data locality for distributed deployments
- Monitor memory usage for large datasets

## Schema Compatibility

### Automatic Schema Alignment

The join engine automatically handles:
- Data type conversions between compatible types
- Column name mapping
- Nullable vs non-nullable field differences

### Supported Type Mappings

| Parquet Type | Iceberg Type | Arrow Type |
|--------------|--------------|------------|
| INT32        | INTEGER      | Int32      |
| INT64        | LONG         | Int64      |
| FLOAT        | FLOAT        | Float32    |
| DOUBLE       | DOUBLE       | Float64    |
| BYTE_ARRAY   | STRING       | Utf8       |
| BOOLEAN      | BOOLEAN      | Boolean    |

## Error Handling

### Common Error Scenarios

1. **Schema Incompatibility**: Mismatched join key types
2. **Missing Tables**: Referenced tables not found
3. **Catalog Connection**: Iceberg catalog unavailable
4. **File Access**: Parquet files not accessible

### Error Recovery

```rust
match engine.join_tables("customers", "orders", &config).await {
    Ok(results) => {
        // Process successful results
        println!("Join completed with {} batches", results.len());
    }
    Err(e) => {
        // Handle specific error types
        match e.downcast_ref::<DataFusionError>() {
            Some(DataFusionError::SchemaError(msg)) => {
                eprintln!("Schema error: {}", msg);
            }
            Some(DataFusionError::IoError(io_err)) => {
                eprintln!("IO error: {}", io_err);
            }
            _ => {
                eprintln!("General error: {}", e);
            }
        }
    }
}
```

## Testing

### Unit Tests

Run unit tests for individual components:

```bash
cargo test -p igloo-connector-iceberg
cargo test -p igloo-engine join_engine
```

### Integration Tests

Run comprehensive integration tests:

```bash
cargo test -p igloo-engine --test integration_join_test
```

### Performance Tests

Run performance benchmarks:

```bash
cargo test -p igloo-engine --test integration_join_test test_join_performance_metrics -- --nocapture
```

## Limitations and Known Issues

### Current Limitations

1. **Catalog Support**: Limited to Memory and FileSystem catalogs
2. **Complex Types**: Nested types not fully supported
3. **Large Datasets**: Memory usage scales with data size
4. **Concurrent Access**: Limited concurrent catalog operations

### Planned Improvements

- REST and Hive catalog support
- Streaming join operations for large datasets
- Enhanced schema evolution handling
- Improved error messages and diagnostics

## Troubleshooting

### Common Issues

**Issue**: "Table not found" error
**Solution**: Ensure tables are properly registered before joining

**Issue**: Schema mismatch errors
**Solution**: Verify join key types are compatible

**Issue**: Out of memory errors
**Solution**: Use filters to reduce dataset size or increase available memory

### Debug Mode

Enable debug logging for detailed execution information:

```rust
use tracing_subscriber;

tracing_subscriber::fmt()
    .with_max_level(tracing::Level::DEBUG)
    .init();
```

## Future Enhancements

### Roadmap Items

- [ ] Support for Delta Lake tables
- [ ] Streaming join operations
- [ ] Advanced join algorithms (hash, sort-merge)
- [ ] Query result caching
- [ ] Distributed join execution
- [ ] Schema registry integration

### Community Contributions

We welcome contributions in the following areas:
- Additional catalog implementations
- Performance optimizations
- Enhanced error handling
- Documentation improvements
- Test coverage expansion