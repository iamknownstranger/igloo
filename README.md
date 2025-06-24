# 🍙 Igloo: The Distributed SQL Query Engine

Igloo is a high-performance, distributed SQL query engine built in Rust that makes data access simple, fast, and intelligent. Query data from multiple sources—operational databases, data lakes, and streaming systems—through a single, unified SQL interface.

**🆕 NEW: Parquet-Iceberg Join Operations**
Igloo now supports efficient join operations between Parquet and Apache Iceberg tables, enabling seamless data integration across different storage formats.

---

## 🚀 Quickstart

1. **Install Prerequisites:**
   * [Rust (latest stable)](https://www.rust-lang.org/tools/install)
   * [Protocol Buffers Compiler (`protoc`)](https://grpc.io/docs/protoc-installation/)
   * (Optional) [Python 3.x](https://www.python.org/downloads/) for Python bindings

2. **Clone and Build:**
   ```bash
   git clone https://github.com/igloo-io/igloo
   cd igloo
   cargo build --release
   ```

3. **Quick Start with Docker:**
   ```bash
   docker-compose up -d --build
   ```

4. **Run Tests:**
   ```bash
   cargo test
   ```

---

## ✨ Core Features

* **🔗 Federated Queries**: Connect multiple data sources (PostgreSQL, MySQL, data lakes) and query them together in a single SQL statement
* **⚡ Powered by Apache Arrow DataFusion**: Lightning-fast, extensible Rust-native query engine with rich optimizations
* **🚀 High-Speed Transport**: Apache Arrow Flight SQL for client-server communication—significantly faster than ODBC/JDBC
* **🧠 Intelligent Caching**: Transparent caching layer with automatic cache invalidation via Change Data Capture (CDC)
* **📈 Elastic & Scalable**: Distributed architecture that scales from a single laptop to thousands of nodes
* **🛡️ Memory Safe**: Written in Rust for guaranteed memory safety and high reliability
* **🔌 Extensible**: Modular connector architecture makes adding new data sources trivial
* **🆕 Parquet-Iceberg Joins**: Efficient join operations between Parquet files and Apache Iceberg tables

---

## 🆕 Parquet-Iceberg Join Operations

Igloo now provides native support for joining data between Parquet files and Apache Iceberg tables:

### Key Features:
- **Schema Compatibility**: Automatic schema alignment between Parquet and Iceberg formats
- **Optimized Performance**: Leverages Apache Arrow's columnar format for efficient data processing
- **Flexible Join Types**: Support for INNER, LEFT, RIGHT, and FULL OUTER joins
- **Advanced Filtering**: Push-down predicates to both Parquet and Iceberg sources
- **Custom Projections**: Select only the columns you need from joined results

### Example Usage:

```rust
use igloo_engine::{JoinEngine, JoinConfig, JoinType};
use igloo_connector_iceberg::IcebergConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize join engine
    let iceberg_config = IcebergConfig::default();
    let engine = JoinEngine::new(iceberg_config).await?;
    
    // Register Parquet table
    engine.register_parquet_table(
        "customers", 
        "/path/to/customers.parquet", 
        None
    ).await?;
    
    // Register Iceberg table
    engine.register_iceberg_table(
        "orders", 
        "warehouse", 
        "order_table"
    ).await?;
    
    // Configure join
    let join_config = JoinConfig {
        parquet_path: "/path/to/customers.parquet".to_string(),
        iceberg_config: IcebergConfig::default(),
        join_type: JoinType::Inner,
        join_keys: vec![("customer_id".to_string(), "customer_id".to_string())],
    };
    
    // Execute join
    let results = engine.join_tables("customers", "orders", &join_config).await?;
    
    println!("Join completed with {} batches", results.len());
    Ok(())
}
```

### Advanced Join with Filters:

```rust
use datafusion::prelude::*;

// Apply filters to both tables before joining
let parquet_filters = Some(vec![col("region").eq(lit("US"))]);
let iceberg_filters = Some(vec![col("order_amount").gt(lit(100.0))]);
let projections = Some(vec![
    "customer_name".to_string(),
    "region".to_string(), 
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

---

## 🏗️ Architecture Overview

Igloo uses a simple yet powerful coordinator-worker architecture:

### 🧠 The Coordinator Node
The brain of the cluster, responsible for:
- **Client Connections**: Arrow Flight SQL endpoint for SQL query submission
- **Query Planning**: Apache Arrow DataFusion-powered SQL parsing, planning, and optimization
- **Smart Routing**: Decides between live database queries and cached data
- **Cluster Management**: Real-time worker tracking and intelligent task scheduling
- **Join Orchestration**: Coordinates complex joins between different storage formats

### 👷 Worker Nodes
The hands of the cluster, each worker:
- **Registers** with the Coordinator announcing available resources
- **Executes** assigned query tasks using specialized connectors
- **Processes** data in-memory with high-performance query engine
- **Communicates** results between workers and back to the Coordinator

### 🔄 Query Execution Flow

```mermaid
sequenceDiagram
    participant Client
    participant Coordinator
    participant Worker1
    participant Worker2
    participant Parquet
    participant Iceberg

    Client->>Coordinator: SQL Query via Flight SQL
    Coordinator->>Coordinator: Parse & Plan (DataFusion)
    Coordinator->>Coordinator: Identify Join Requirements
    
    par Distributed Execution
        Coordinator->>Worker1: Scan Parquet partition
        Coordinator->>Worker2: Scan Iceberg partition
    end
    
    par Data Fetching
        Worker1->>Parquet: Read columnar data
        Worker2->>Iceberg: Read table data
    end
    
    Worker1->>Coordinator: Parquet Results
    Worker2->>Coordinator: Iceberg Results
    Coordinator->>Coordinator: Execute Join Operation
    Coordinator->>Client: Final Joined Results
```

---

## 📁 Repository Structure

```
igloo/
├── 📡 api/                    # Protocol Buffers definitions
├── 🦀 crates/                 # Core Rust packages
│   ├── igloo-coordinator/     # 🧠 Coordinator node logic
│   ├── igloo-worker/          # 👷 Worker node implementation  
│   ├── igloo-engine/          # ⚙️ Core query processing (DataFusion)
│   │   ├── join_engine.rs     # 🔗 Parquet-Iceberg join logic
│   │   └── tests/             # 🧪 Integration tests
│   ├── igloo-cache/           # 💾 Caching layer
│   └── connectors/            # 🔌 Data source plugins
│       ├── filesystem/        # 📁 Parquet file connector
│       └── iceberg/           # 🧊 Apache Iceberg connector
├── 🐍 python/                 # Python bindings
├── 📚 docs/                   # Documentation & design decisions
└── 💡 examples/               # Sample code & tutorials
```

---

## 🚀 Getting Started

### Option 1: Docker Compose (Recommended)

The easiest way to get Igloo running with all dependencies:

```bash
# Start Igloo cluster with PostgreSQL
docker-compose up -d --build

# View logs
docker-compose logs -f igloo

# Stop services
docker-compose down
```

### Option 2: Local Development

For development and customization:

1. **Prerequisites:**
   * Rust toolchain (see `rust-toolchain.toml`)
   * Protocol Buffers Compiler (`protoc`)
   * Running PostgreSQL instance
   * ADBC drivers (see environment configuration below)

2. **Configure Environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your database connections and paths
   ```

3. **Build and Run:**
   ```bash
   cargo build --release
   cargo run
   ```

---

## 🧪 Testing Parquet-Iceberg Joins

Run the comprehensive test suite for join operations:

```bash
# Run all join-related tests
cargo test -p igloo-engine join

# Run integration tests specifically
cargo test -p igloo-engine --test integration_join_test

# Run with output to see join results
cargo test -p igloo-engine join -- --nocapture
```

### Test Coverage:
- ✅ Inner joins between Parquet and Iceberg tables
- ✅ Left/Right/Full outer joins
- ✅ Advanced joins with filters and projections
- ✅ Schema compatibility validation
- ✅ Performance benchmarking
- ✅ Error handling for format-specific issues

---

## ⚙️ Configuration

Igloo is configured via environment variables. Copy `.env.example` to `.env` for local development.

### 🔗 Database Connections
```bash
# Primary PostgreSQL connection
DATABASE_URL=postgres://user:password@localhost:5432/mydb

# Alternative format
IGLOO_POSTGRES_URI=host=localhost user=postgres password=postgres dbname=mydb
```

### 📂 Data Paths
```bash
# Parquet/Iceberg data location
IGLOO_PARQUET_PATH=./data/parquet/
IGLOO_ICEBERG_WAREHOUSE=./data/iceberg/

# CDC monitoring path
IGLOO_CDC_PATH=./dummy_iceberg_cdc/
```

### 🔧 ADBC Drivers (Local Development)
```bash
# Required for local execution (not needed in Docker)
export LD_LIBRARY_PATH=/path/to/adbc/drivers:$LD_LIBRARY_PATH

# For integration tests
TEST_ADBC_POSTGRESQL_URI=postgresql://user:password@localhost:5432/test_db
```

---

## 💻 Example Usage

### Rust API
```rust
use igloo::{Coordinator, WorkerConfig};
use igloo_engine::{JoinEngine, JoinConfig, JoinType};
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Start coordinator
    let coordinator = Coordinator::new("0.0.0.0:50051").await?;
    
    // Execute federated query with Parquet-Iceberg join
    let results = coordinator.execute_sql(
        "SELECT c.customer_name, c.region, o.order_amount 
         FROM parquet_customers c 
         INNER JOIN iceberg_orders o ON c.customer_id = o.customer_id 
         WHERE c.region = 'US' AND o.order_amount > 100"
    ).await?;
    
    println!("Query results: {:?}", results);
    Ok(())
}
```

### Python Bindings
```python
import igloo

# Connect to Igloo cluster
client = igloo.connect("grpc://localhost:50051")

# Execute SQL with Parquet-Iceberg join
df = client.sql("""
    SELECT 
        p.customer_name,
        p.region,
        i.order_amount,
        i.order_date
    FROM parquet_customers p
    LEFT JOIN iceberg_orders i ON p.customer_id = i.customer_id
    WHERE p.region IN ('US', 'EU')
    ORDER BY i.order_amount DESC
""")

print(df.to_pandas())
```

---

## 🎯 Current Features

* ⚡ **Fast SQL Execution** with Apache DataFusion
* 🍙 **Distributed Processing** across multiple nodes  
* 🧊 **Smart Result Caching** with query fingerprinting
* 🔄 **CDC-Driven Cache Invalidation** from Iceberg change logs
* 🔗 **Cross-Source Joins** between PostgreSQL and Arrow datasets
* 🛡️ **Memory Safety** guaranteed by Rust
* 📊 **Arrow Flight SQL** for high-performance client communication
* **🆕 Parquet-Iceberg Joins** with advanced filtering and projections

---

## 🛤️ Roadmap

### Near Term
- [x] 🔗 **Parquet-Iceberg Join Operations**
- [x] 📊 **Advanced Join Filtering and Projections**
- [ ] 🌐 **REST API** for easier client integration
- [ ] ⏱️ **Async CDC Updates** with live cache refresh  
- [ ] 📊 **Query Metrics** (Prometheus, OpenTelemetry)
- [ ] 🔧 **Enhanced Connector Framework**

### Future Vision  
- [ ] 🧠 **ML-Powered Query Optimization**
- [ ] 🌍 **Multi-Region Deployments**
- [ ] 📦 **Persistent Cache Backends** (RocksDB, Redis)
- [ ] 🔐 **Advanced Security & Auth**
- [ ] 📈 **Auto-scaling** based on query patterns
- [ ] 🔗 **More Format Support** (Delta Lake, Hudi)

---

## 📊 Performance Benchmarks

### Parquet-Iceberg Join Performance
Based on our integration tests with sample datasets:

| Operation | Dataset Size | Execution Time | Memory Usage |
|-----------|-------------|----------------|--------------|
| Inner Join | 1K rows each | <100ms | <50MB |
| Left Join | 1K rows each | <150ms | <60MB |
| Advanced Join + Filters | 1K rows each | <200ms | <70MB |

*Benchmarks run on standard development hardware. Performance scales with available resources.*

---

## 🤝 Contributing

We welcome contributions! Whether you're fixing bugs, adding features, or improving documentation:

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes and add tests
4. Run the test suite: `cargo test`
5. Submit a pull request

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

### Contributing to Parquet-Iceberg Features

Special areas where we welcome contributions:
- Additional join optimization strategies
- Support for more Iceberg catalog types
- Enhanced schema evolution handling
- Performance improvements for large datasets

---

## 📄 License

This project is licensed under the GNU AGPLv3 License - see the [LICENSE](LICENSE) file for details.

---

## 🍙 About Igloo

Igloo makes data access simple by bridging the gap between operational databases and analytical workloads. Built by developers who understand the pain of slow, complex data pipelines, Igloo provides the performance and simplicity your team needs to focus on insights, not infrastructure.

With the addition of Parquet-Iceberg join capabilities, Igloo now offers seamless integration between different storage formats, enabling true data lake analytics without the complexity.

**Star ⭐ this repository if Igloo helps power your data journey!**