# OxiDB Database Examples - Familiar Patterns from Popular Databases

This directory contains comprehensive examples demonstrating how to use OxiDB with patterns familiar to developers coming from MySQL, PostgreSQL, MongoDB, and other popular databases.

## 🎯 Overview

These examples showcase OxiDB's versatility by demonstrating database patterns that developers already know and love from other systems:

- **MySQL-style** - E-commerce operations, transactions, relational patterns
- **PostgreSQL-style** - Advanced analytics, window functions, CTEs, data warehousing
- **MongoDB-style** - Document storage, JSON queries, nested operations

## 📁 Example Files

### 🛒 MySQL-Style E-commerce (`mysql_style_ecommerce.rs`)

**What it demonstrates:**
- Familiar MySQL DDL (Data Definition Language) patterns
- E-commerce database schema design
- CRUD operations with complex relationships
- Transaction management
- Business logic implementation
- Inventory tracking and order management

**Key Features:**
```sql
-- MySQL-style table creation
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Complex business queries
SELECT c.first_name, c.last_name, c.loyalty_points,
       COUNT(o.id) as total_orders,
       SUM(o.total_amount) as lifetime_value
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
GROUP BY c.id
ORDER BY lifetime_value DESC;
```

**Business Scenarios Covered:**
- Customer registration and authentication
- Product catalog management with categories
- Shopping cart operations
- Order processing with transactions
- Inventory tracking and restock alerts
- Sales reporting and analytics

### 🐘 PostgreSQL-Style Analytics (`postgresql_analytics_demo.rs`)

**What it demonstrates:**
- Advanced analytical queries with window functions
- Common Table Expressions (CTEs) for complex logic
- Time-series analysis and reporting
- Data warehousing patterns (star schema, slowly changing dimensions)
- Statistical functions and aggregations
- OLAP-style multi-dimensional analysis

**Key Features:**
```sql
-- Window functions for running totals
SELECT 
    full_date,
    daily_revenue,
    SUM(daily_revenue) OVER (ORDER BY full_date ROWS UNBOUNDED PRECEDING) as running_total,
    AVG(daily_revenue) OVER (ORDER BY full_date ROWS 6 PRECEDING) as seven_day_avg,
    RANK() OVER (ORDER BY daily_revenue DESC) as revenue_rank
FROM daily_sales;

-- Complex CTEs for cohort analysis
WITH customer_cohorts AS (
    SELECT customer_id, MIN(transaction_date) as first_purchase_date
    FROM transactions GROUP BY customer_id
),
cohort_data AS (
    SELECT cohort_month, COUNT(*) as active_customers
    FROM customer_cohorts 
    GROUP BY DATE_TRUNC('month', first_purchase_date)
)
SELECT * FROM cohort_data;
```

**Analytics Scenarios Covered:**
- Customer lifetime value analysis
- Cohort retention analysis
- RFM (Recency, Frequency, Monetary) customer segmentation
- Time-series analysis with gap filling
- Market basket analysis
- Multi-dimensional CUBE/ROLLUP operations
- Statistical aggregations and percentiles

### 🍃 MongoDB-Style Documents (`mongodb_style_document_demo.rs`)

**What it demonstrates:**
- Document-oriented data modeling
- JSON field queries and updates
- Nested document operations
- Array operations and containment checks
- Aggregation pipeline patterns
- Full-text search in documents
- Geospatial data handling

**Key Features:**
```sql
-- Document storage with JSON
CREATE TABLE users (
    _id VARCHAR(50) PRIMARY KEY,
    document JSON NOT NULL,
    email VARCHAR(100) GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.email')) STORED,
    username VARCHAR(50) GENERATED ALWAYS AS (JSON_EXTRACT(document, '$.username')) STORED
);

-- Complex nested queries
SELECT _id,
       JSON_EXTRACT(document, '$.profile.firstName') as first_name,
       JSON_EXTRACT(document, '$.profile.location.city') as city,
       JSON_EXTRACT(document, '$.roles') as roles
FROM users
WHERE JSON_CONTAINS(JSON_EXTRACT(document, '$.roles'), '"admin"')
  AND JSON_EXTRACT(document, '$.profile.location.state') = 'CA';

-- Array operations
UPDATE users
SET document = JSON_ARRAY_APPEND(JSON_EXTRACT(document, '$.roles'), '$', 'beta_tester')
WHERE _id = 'user_001';
```

**Document Scenarios Covered:**
- User profiles with nested preferences
- Product catalogs with flexible attributes
- Order documents with embedded line items
- Blog posts with rich metadata
- Geospatial location data
- Full-text search across document fields

## 🚀 How to Run Examples

### Prerequisites

1. **Build OxiDB** (if not already done):
   ```bash
   cargo build --release
   ```

2. **Install Dependencies**:
   ```bash
   cargo update
   ```

### Running Individual Examples

Each example is self-contained and can be run directly:

```bash
# MySQL-style E-commerce
cargo run --example mysql_style_ecommerce

# PostgreSQL-style Analytics  
cargo run --example postgresql_analytics_demo

# MongoDB-style Documents
cargo run --example mongodb_style_document_demo
```

### Alternative: Direct Compilation

If the examples aren't set up as Cargo examples, you can compile them directly:

```bash
# Compile and run MySQL example
rustc examples/mysql_style_ecommerce.rs --extern oxidb=target/release/liboxidb.rlib \
    -L target/release/deps -o mysql_example && ./mysql_example

# Similar for other examples
rustc examples/postgresql_analytics_demo.rs --extern oxidb=target/release/liboxidb.rlib \
    -L target/release/deps -o postgres_example && ./postgres_example
```

## 📊 Example Outputs

### MySQL E-commerce Output
```
🛒 MySQL-Style E-commerce Database Demo
============================================================

🧹 Cleaning up existing tables...
✓ Tables cleaned up

🏗️ Creating database schema (MySQL-style)...
✓ Created users table
✓ Created customers table
✓ Created categories table
✓ Created products table
✓ Created order_items table
✅ Database schema created successfully!

👥 Customer Operations (MySQL-style)
========================================
📝 Customer Registration:
✓ User registered successfully
✓ Customer profile created
```

### PostgreSQL Analytics Output
```
🐘 PostgreSQL-Style Analytics & Data Warehousing Demo
=================================================================

🏗️ Setting up Analytics Schema (PostgreSQL-style)...
✓ Created time_dimension table
✓ Created sales_events table
✓ Created customer_transactions table

🪟 Window Functions (PostgreSQL Advanced Analytics)
=======================================================
📈 Running Totals & Moving Averages:
✓ Running totals and moving averages calculated

👑 Customer Ranking & Segmentation:
✓ Customer ranking and segmentation completed
```

### MongoDB Documents Output
```
🍃 MongoDB-Style Document Database Demo
==================================================

📄 Setting up Document Collections...
✓ Created users collection
✓ Created products collection
✓ Created orders collection

🔍 Document Query Operations (MongoDB-style)
==================================================
📋 Basic Document Queries:
✓ Found active users

🪆 Nested Document Operations
===================================
✏️ Updating Nested Fields:
✓ Updated user preferences and login info
```

## 🎯 Key Learning Points

### 1. **Familiar SQL Patterns**
- All examples use SQL syntax that developers already know
- Standard DDL, DML, and query patterns
- Familiar functions and operators

### 2. **Advanced Features**
- Window functions for analytics
- CTEs for complex logic
- JSON operations for document storage
- Full-text search capabilities

### 3. **Real-World Scenarios**
- Complete business logic implementations
- Production-ready patterns
- Performance considerations
- Data integrity and constraints

### 4. **Cross-Database Compatibility**
- MySQL-style AUTO_INCREMENT and constraints
- PostgreSQL-style advanced analytics
- MongoDB-style document operations
- All in one unified system

## 📚 Additional Resources

### Related Examples in This Repository
- `sql_compatibility_demo.rs` - Basic SQL compatibility
- `comprehensive_sql_demo.rs` - SQL feature overview
- `ecommerce_website.rs` - Web application patterns
- `knowledge_graph_rag.rs` - Graph database patterns

### Documentation
- [OxiDB SQL Reference](../docs/sql_reference.md)
- [JSON Functions Guide](../docs/json_functions.md)
- [Performance Tuning](../docs/performance.md)

## 🔧 Customization

Each example is designed to be easily customizable:

1. **Schema Modifications**: Adapt table structures for your use case
2. **Business Logic**: Modify queries for your specific requirements
3. **Data Generation**: Adjust sample data to match your domain
4. **Performance Tuning**: Add indexes and optimize queries

## 🤝 Contributing

Found an issue or want to add more examples? 

1. **Add New Patterns**: Create examples for other database systems (Oracle, SQL Server, etc.)
2. **Extend Scenarios**: Add more complex business logic
3. **Performance Examples**: Add benchmarking and optimization examples
4. **Integration Examples**: Show how to integrate with web frameworks

## 📈 Next Steps

After running these examples:

1. **Adapt for Your Use Case**: Modify schemas and queries for your specific needs
2. **Performance Testing**: Run with larger datasets to test performance
3. **Integration**: Integrate patterns into your applications
4. **Advanced Features**: Explore OxiDB's unique features like GraphRAG

---

**💡 Pro Tip**: These examples are designed to be familiar to developers from different database backgrounds. Choose the patterns that match your team's expertise and gradually explore others!