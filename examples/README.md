# OxiDB Examples

This directory contains comprehensive examples demonstrating real-world usage of OxiDB for various applications. These examples showcase OxiDB's capabilities as a production-ready database with SQL support, vector operations, and graph functionality.

## 🚀 Running Examples

To run any example, use:

```bash
cargo run --example <example_name>
```

## 📚 Available Examples

### 1. **E-commerce Website Database** (`ecommerce_website.rs`)

A complete e-commerce backend implementation demonstrating:
- **Product Catalog**: Products with vector embeddings for similarity search
- **User Management**: User accounts with authentication and profiles
- **Order Processing**: Shopping cart, order creation, and tracking
- **Vector Search**: Find similar products using embeddings
- **Inventory Management**: Stock tracking with automatic updates

**Use Cases**:
- Online stores and marketplaces
- Product recommendation systems
- Inventory management systems

```bash
cargo run --example ecommerce_website
```

### 2. **Document Search RAG** (`document_search_rag.rs`)

Semantic document search system with RAG capabilities:
- **Document Storage**: Store documents with vector embeddings
- **Semantic Search**: Find documents by meaning, not just keywords
- **Hybrid Search**: Combine semantic and keyword search
- **Category Filtering**: Search within specific document categories
- **Snippet Extraction**: Get relevant excerpts from documents

**Use Cases**:
- Knowledge bases and wikis
- Document management systems
- Research paper databases
- Legal document search

```bash
cargo run --example document_search_rag
```

### 3. **Knowledge Graph RAG** (`knowledge_graph_rag.rs`)

GraphRAG implementation for connected information retrieval:
- **Entity Management**: Store entities with properties and embeddings
- **Relationship Tracking**: Define connections between entities
- **Graph Traversal**: Find paths and connections in the knowledge graph
- **Similarity Search**: Find related entities using embeddings
- **Shortest Path**: Find optimal connections between entities

**Use Cases**:
- Knowledge graphs for AI systems
- Social network analysis
- Recommendation engines
- Research relationship mapping

```bash
cargo run --example knowledge_graph_rag
```

### 4. **SQL Compatibility Demo** (`sql_compatibility_demo.rs`)

Comprehensive demonstration of PostgreSQL/MySQL-compatible SQL syntax:
- **DDL Operations**: CREATE TABLE with constraints, indexes, foreign keys
- **DML Operations**: INSERT, UPDATE, DELETE with various patterns
- **Complex Queries**: JOINs, subqueries, aggregations, GROUP BY, HAVING
- **Advanced Features**: CASE statements, string functions, transactions
- **Real-world Schema**: Complete e-commerce database with relationships

**Use Cases**:
- Migrating from PostgreSQL/MySQL to OxiDB
- Learning SQL with a familiar syntax
- Building traditional relational database applications
- Testing SQL compatibility

```bash
cargo run --example sql_compatibility_demo
```

### 5. **Shakespeare RAG Comparison** (`shakespeare_rag_comparison.rs`)

Real-world comparison of RAG vs GraphRAG approaches:
- **Document Processing**: Downloads and processes Shakespeare works
- **Performance Benchmarking**: Compares retrieval speed and relevance
- **Character Relationships**: Maps relationships in plays
- **Thematic Analysis**: Searches by themes (love, death, power, etc.)

**Results**:
- RAG: 38.4x faster (2.41ms vs 92.77ms average)
- GraphRAG: 90.3% more relevant results

```bash
cargo run --example shakespeare_rag_comparison
```

### 6. **Simple Blog** (`simple_blog/`)

A blog application with:
- Post creation and management
- User authentication
- Comments system
- Category organization

```bash
cd examples/simple_blog
cargo run
```

### 7. **Todo Application** (`todo_app/`)

Task management system featuring:
- Task CRUD operations
- Priority levels
- Due dates
- Status tracking

```bash
cd examples/todo_app
cargo run
```

### 8. **Performance Demo** (`performance_demo/`)

Demonstrates OxiDB's performance monitoring:
- Real-time query performance tracking
- Bottleneck identification
- Optimization recommendations
- Performance reports

```bash
cd examples/performance_demo
cargo run
```

## 🔧 Example Categories

### Web Applications
- `ecommerce_website`: Full e-commerce backend
- `simple_blog`: Blog with posts and comments
- `todo_app`: Task management system
- `user_auth_files`: File-based authentication

### AI/ML Applications
- `document_search_rag`: Semantic document search
- `knowledge_graph_rag`: Graph-based knowledge retrieval
- `shakespeare_rag_comparison`: RAG vs GraphRAG comparison
- `generic_rag_comparison`: Generic RAG implementation

### Testing & Validation
- `comprehensive_test`: Full feature testing
- `edge_case_tests`: Boundary condition testing
- `data_type_tests`: Data type validation
- `concurrent_operations_demo`: Concurrency testing

### Performance & Benchmarking
- `performance_benchmark`: Performance measurements
- `performance_demo`: Real-time monitoring
- `zero_cost_sql_demo`: Zero-cost abstraction demo

## 🎯 Key Features Demonstrated

### SQL Support
- CREATE TABLE with various data types including VECTOR
- INSERT, SELECT, UPDATE, DELETE operations
- JOIN operations and complex queries
- Index creation (B-Tree, Hash, HNSW for vectors)
- Transaction support with ACID compliance

### Vector Operations
- Store high-dimensional embeddings (up to 16K dimensions)
- Vector similarity search using multiple metrics
- HNSW indexing for fast approximate nearest neighbor search
- Integration with RAG workflows

### Graph Capabilities
- Entity and relationship management
- Graph traversal algorithms
- Shortest path finding
- Connected component analysis

### Performance Features
- Real-time performance monitoring
- Query optimization
- Concurrent access handling
- WAL (Write-Ahead Logging) for durability

## 🏗️ Building Your Own Examples

To create a new example:

1. Create a new `.rs` file in the `examples/` directory
2. Add it to `Cargo.toml`:
   ```toml
   [[example]]
   name = "your_example"
   path = "examples/your_example.rs"
   ```
3. Import OxiDB:
   ```rust
   use oxidb::{OxiDB, OxiDBError};
   use oxidb::core::types::{DataType, OrderedFloat, VectorData};
   ```

## 📖 Learning Path

1. **Start Simple**: Begin with `connection_api_demo` to understand basics
2. **Web Apps**: Try `simple_blog` or `todo_app` for CRUD operations
3. **Advanced Features**: Explore `ecommerce_website` for complex queries
4. **AI Integration**: Study `document_search_rag` for vector operations
5. **Graph Features**: Examine `knowledge_graph_rag` for relationships

## 🤝 Contributing

We welcome new examples! Please ensure your example:
- Demonstrates a real-world use case
- Includes comprehensive comments
- Handles errors appropriately
- Shows best practices for OxiDB usage

## 📝 License

All examples are provided under the same license as OxiDB (Apache 2.0).