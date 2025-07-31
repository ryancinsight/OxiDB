//! SQL Compatibility Demo
//! 
//! This example demonstrates OxiDB's SQL compatibility with PostgreSQL and MySQL-like syntax.
//! It shows various SQL features including:
//! - DDL (Data Definition Language): CREATE, ALTER, DROP
//! - DML (Data Manipulation Language): INSERT, UPDATE, DELETE, SELECT
//! - Complex queries: JOINs, subqueries, aggregations
//! - Transactions and constraints

use oxidb::{OxiDB, OxiDBError};
use oxidb::core::types::{DataType, OrderedFloat};
use chrono::{DateTime, Utc};
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== SQL Compatibility Demo ===\n");
    
    let db = OxiDB::open("sql_demo.db")?;
    
    // Clean up any existing tables
    let _ = db.execute_sql("DROP TABLE IF EXISTS order_items");
    let _ = db.execute_sql("DROP TABLE IF EXISTS orders");
    let _ = db.execute_sql("DROP TABLE IF EXISTS products");
    let _ = db.execute_sql("DROP TABLE IF EXISTS customers");
    
    println!("1. Creating Tables (PostgreSQL/MySQL compatible syntax)");
    println!("{}", "=".repeat(50));
    
    // Create customers table (similar to both PostgreSQL and MySQL)
    let create_customers = r#"
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            phone TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT true
        )
    "#;
    db.execute_sql(create_customers)?;
    println!("✓ Created customers table");
    
    // Create products table with various data types
    let create_products = r#"
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            sku TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            price FLOAT NOT NULL CHECK (price >= 0),
            cost FLOAT DEFAULT 0.0,
            stock INTEGER DEFAULT 0 CHECK (stock >= 0),
            category TEXT,
            tags TEXT,  -- JSON array stored as text
            features VECTOR[128],  -- OxiDB-specific vector type
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    "#;
    db.execute_sql(create_products)?;
    println!("✓ Created products table");
    
    // Create orders table with foreign key reference
    let create_orders = r#"
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            order_date TEXT DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'pending',
            total_amount FLOAT DEFAULT 0.0,
            shipping_address TEXT,
            notes TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        )
    "#;
    db.execute_sql(create_orders)?;
    println!("✓ Created orders table");
    
    // Create order_items table (many-to-many relationship)
    let create_order_items = r#"
        CREATE TABLE order_items (
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            unit_price FLOAT NOT NULL,
            discount FLOAT DEFAULT 0.0,
            PRIMARY KEY (order_id, product_id),
            FOREIGN KEY (order_id) REFERENCES orders(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
    "#;
    db.execute_sql(create_order_items)?;
    println!("✓ Created order_items table");
    
    // Create indexes for better performance
    println!("\n2. Creating Indexes");
    println!("{}", "=".repeat(50));
    
    db.execute_sql("CREATE INDEX idx_customers_email ON customers(email)")?;
    db.execute_sql("CREATE INDEX idx_products_category ON products(category)")?;
    db.execute_sql("CREATE INDEX idx_orders_customer ON orders(customer_id)")?;
    db.execute_sql("CREATE INDEX idx_orders_status ON orders(status)")?;
    println!("✓ Created indexes for optimized queries");
    
    // Insert sample data
    println!("\n3. Inserting Data (Various SQL patterns)");
    println!("{}", "=".repeat(50));
    
    // Insert customers
    let customers = vec![
        "INSERT INTO customers (id, email, name, phone) VALUES (1, 'john@example.com', 'John Doe', '+1-555-0101')",
        "INSERT INTO customers (id, email, name, phone) VALUES (2, 'jane@example.com', 'Jane Smith', '+1-555-0102')",
        "INSERT INTO customers (id, email, name, phone) VALUES (3, 'bob@example.com', 'Bob Johnson', NULL)",
        "INSERT INTO customers (id, email, name, is_active) VALUES (4, 'alice@example.com', 'Alice Brown', false)",
    ];
    
    for sql in customers {
        db.execute_sql(sql)?;
    }
    println!("✓ Inserted 4 customers");
    
    // Insert products with various data types
    let products = vec![
        "INSERT INTO products (id, sku, name, description, price, cost, stock, category, tags) 
         VALUES (1, 'LAPTOP-001', 'Professional Laptop', 'High-performance laptop for developers', 1299.99, 800.00, 25, 'Electronics', '[\"laptop\",\"computer\",\"portable\"]')",
        
        "INSERT INTO products (id, sku, name, description, price, cost, stock, category, tags) 
         VALUES (2, 'MOUSE-001', 'Wireless Mouse', 'Ergonomic wireless mouse', 29.99, 12.50, 150, 'Electronics', '[\"mouse\",\"wireless\",\"accessory\"]')",
        
        "INSERT INTO products (id, sku, name, description, price, cost, stock, category, tags) 
         VALUES (3, 'DESK-001', 'Standing Desk', 'Adjustable height standing desk', 599.99, 350.00, 10, 'Furniture', '[\"desk\",\"furniture\",\"ergonomic\"]')",
        
        "INSERT INTO products (id, sku, name, description, price, cost, stock, category) 
         VALUES (4, 'CHAIR-001', 'Ergonomic Chair', 'Comfortable office chair', 399.99, 200.00, 15, 'Furniture')",
        
        "INSERT INTO products (id, sku, name, description, price, stock, category) 
         VALUES (5, 'MONITOR-001', '4K Monitor', '27-inch 4K display', 499.99, 8, 'Electronics')",
    ];
    
    for sql in products {
        db.execute_sql(sql)?;
    }
    println!("✓ Inserted 5 products");
    
    // Insert orders
    let orders = vec![
        "INSERT INTO orders (id, customer_id, status, total_amount, shipping_address) 
         VALUES (1, 1, 'completed', 1329.98, '123 Main St, Anytown, USA')",
        
        "INSERT INTO orders (id, customer_id, status, total_amount) 
         VALUES (2, 2, 'processing', 999.98)",
        
        "INSERT INTO orders (id, customer_id, status, total_amount, notes) 
         VALUES (3, 1, 'pending', 29.99, 'Gift wrapping requested')",
    ];
    
    for sql in orders {
        db.execute_sql(sql)?;
    }
    println!("✓ Inserted 3 orders");
    
    // Insert order items
    let order_items = vec![
        "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (1, 1, 1, 1299.99)",
        "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (1, 2, 1, 29.99)",
        "INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount) VALUES (2, 3, 1, 599.99, 0.0)",
        "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (2, 4, 1, 399.99)",
        "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (3, 2, 1, 29.99)",
    ];
    
    for sql in order_items {
        db.execute_sql(sql)?;
    }
    println!("✓ Inserted 5 order items");
    
    // Demonstrate various SELECT queries
    println!("\n4. SELECT Queries (PostgreSQL/MySQL compatible)");
    println!("{}", "=".repeat(50));
    
    // Simple SELECT
    println!("\n-- Simple SELECT with WHERE clause:");
    let result = db.execute_sql("SELECT name, email FROM customers WHERE is_active = true")?;
    print_results(&result);
    
    // SELECT with ORDER BY and LIMIT
    println!("\n-- SELECT with ORDER BY and LIMIT:");
    let result = db.execute_sql("SELECT name, price FROM products ORDER BY price DESC LIMIT 3")?;
    print_results(&result);
    
    // SELECT with aggregation
    println!("\n-- Aggregation functions:");
    let result = db.execute_sql("SELECT category, COUNT(*) as count, AVG(price) as avg_price, MAX(price) as max_price FROM products GROUP BY category")?;
    print_results(&result);
    
    // JOIN queries
    println!("\n-- INNER JOIN (customer orders):");
    let result = db.execute_sql(r#"
        SELECT c.name, o.id as order_id, o.status, o.total_amount
        FROM customers c
        INNER JOIN orders o ON c.id = o.customer_id
        ORDER BY o.total_amount DESC
    "#)?;
    print_results(&result);
    
    // Complex JOIN with multiple tables
    println!("\n-- Multi-table JOIN (order details):");
    let result = db.execute_sql(r#"
        SELECT 
            c.name as customer,
            p.name as product,
            oi.quantity,
            oi.unit_price,
            (oi.quantity * oi.unit_price) as line_total
        FROM order_items oi
        INNER JOIN orders o ON oi.order_id = o.id
        INNER JOIN customers c ON o.customer_id = c.id
        INNER JOIN products p ON oi.product_id = p.id
        WHERE o.status = 'completed'
    "#)?;
    print_results(&result);
    
    // Subquery example
    println!("\n-- Subquery (customers with orders):");
    let result = db.execute_sql(r#"
        SELECT name, email
        FROM customers
        WHERE id IN (SELECT DISTINCT customer_id FROM orders)
    "#)?;
    print_results(&result);
    
    // UPDATE examples
    println!("\n5. UPDATE Operations");
    println!("{}", "=".repeat(50));
    
    // Update single record
    db.execute_sql("UPDATE products SET stock = stock - 1 WHERE id = 1")?;
    println!("✓ Updated laptop stock (decreased by 1)");
    
    // Update with calculation
    db.execute_sql("UPDATE products SET price = price * 1.1 WHERE category = 'Electronics'")?;
    println!("✓ Increased electronics prices by 10%");
    
    // Update multiple fields
    db.execute_sql(r#"
        UPDATE orders 
        SET status = 'shipped', 
            notes = 'Shipped via express delivery'
        WHERE id = 2
    "#)?;
    println!("✓ Updated order status and notes");
    
    // Conditional update
    db.execute_sql(r#"
        UPDATE customers 
        SET is_active = false 
        WHERE id NOT IN (SELECT DISTINCT customer_id FROM orders)
    "#)?;
    println!("✓ Deactivated customers without orders");
    
    // Transaction example
    println!("\n6. Transaction Example");
    println!("{}", "=".repeat(50));
    
    // Start transaction
    db.execute_sql("BEGIN TRANSACTION")?;
    println!("✓ Started transaction");
    
    // Create a new order within transaction
    db.execute_sql("INSERT INTO orders (id, customer_id, status) VALUES (4, 3, 'pending')")?;
    db.execute_sql("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (4, 5, 2, 499.99)")?;
    db.execute_sql("UPDATE orders SET total_amount = 999.98 WHERE id = 4")?;
    db.execute_sql("UPDATE products SET stock = stock - 2 WHERE id = 5")?;
    
    // Commit transaction
    db.execute_sql("COMMIT")?;
    println!("✓ Committed transaction (new order created)");
    
    // Advanced queries
    println!("\n7. Advanced SQL Features");
    println!("{}", "=".repeat(50));
    
    // CASE statement
    println!("\n-- CASE statement (price categories):");
    let result = db.execute_sql(r#"
        SELECT 
            name,
            price,
            CASE 
                WHEN price < 100 THEN 'Budget'
                WHEN price < 500 THEN 'Mid-range'
                ELSE 'Premium'
            END as price_category
        FROM products
        ORDER BY price
    "#)?;
    print_results(&result);
    
    // HAVING clause
    println!("\n-- GROUP BY with HAVING:");
    let result = db.execute_sql(r#"
        SELECT 
            category,
            COUNT(*) as product_count,
            AVG(price) as avg_price
        FROM products
        GROUP BY category
        HAVING COUNT(*) > 1
    "#)?;
    print_results(&result);
    
    // String functions
    println!("\n-- String functions:");
    let result = db.execute_sql(r#"
        SELECT 
            UPPER(name) as upper_name,
            LENGTH(email) as email_length,
            SUBSTR(phone, 1, 6) as area_code
        FROM customers
        WHERE phone IS NOT NULL
    "#)?;
    print_results(&result);
    
    // Date/time operations (simulated)
    println!("\n-- Date operations:");
    let result = db.execute_sql(r#"
        SELECT 
            id,
            order_date,
            status
        FROM orders
        WHERE order_date >= '2024-01-01'
        ORDER BY order_date DESC
    "#)?;
    print_results(&result);
    
    // Clean up
    println!("\n8. Cleanup Operations");
    println!("{}", "=".repeat(50));
    
    // Delete with JOIN (delete order items for pending orders)
    db.execute_sql(r#"
        DELETE FROM order_items
        WHERE order_id IN (
            SELECT id FROM orders WHERE status = 'pending'
        )
    "#)?;
    println!("✓ Deleted items from pending orders");
    
    // Drop tables in correct order (respecting foreign keys)
    db.execute_sql("DROP TABLE order_items")?;
    db.execute_sql("DROP TABLE orders")?;
    db.execute_sql("DROP TABLE products")?;
    db.execute_sql("DROP TABLE customers")?;
    println!("✓ Dropped all tables");
    
    println!("\n✅ SQL Compatibility Demo completed successfully!");
    println!("\nThis demo shows OxiDB's compatibility with common SQL patterns");
    println!("found in PostgreSQL and MySQL, making migration easier.");
    
    Ok(())
}

// Helper function to print query results
fn print_results(result: &oxidb::QueryResult) {
    if result.rows.is_empty() {
        println!("(No results)");
        return;
    }
    
    // Print column headers
    let headers: Vec<String> = result.schema.iter()
        .map(|(name, _)| name.clone())
        .collect();
    println!("{}", headers.join(" | "));
    println!("{}", "-".repeat(headers.join(" | ").len()));
    
    // Print rows
    for row in &result.rows {
        let values: Vec<String> = row.iter()
            .map(|val| format_value(val))
            .collect();
        println!("{}", values.join(" | "));
    }
    println!("({} rows)", result.rows.len());
}

fn format_value(val: &DataType) -> String {
    match val {
        DataType::Integer(i) => i.to_string(),
        DataType::Float(f) => format!("{:.2}", f.0),
        DataType::String(s) => s.clone(),
        DataType::Boolean(b) => b.to_string(),
        DataType::Null => "NULL".to_string(),
        _ => format!("{:?}", val),
    }
}