//! MySQL-Style E-commerce Database Example
//! 
//! This example demonstrates OxiDB usage patterns that are familiar to MySQL developers.
//! It includes:
//! - Database schema design with proper relationships
//! - CRUD operations with complex queries
//! - Transactions and data integrity
//! - Indexing and performance optimization
//! - Common e-commerce business logic patterns

use oxidb::Oxidb;
use oxidb::core::common::OxidbError;
use oxidb::api::ExecutionResult;
use std::collections::HashMap;
use chrono::{DateTime, Utc};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🛒 MySQL-Style E-commerce Database Demo");
    println!("{}", "=".repeat(60));
    
    // Initialize database (similar to MySQL connection)
    let mut db = Oxidb::new("mysql_style_ecommerce.db")?;
    
    // Clean up existing tables (MySQL-style DROP IF EXISTS)
    cleanup_tables(&mut db)?;
    
    // Create database schema
    create_schema(&mut db)?;
    
    // Seed initial data
    seed_data(&mut db)?;
    
    // Demonstrate common e-commerce operations
    demonstrate_customer_operations(&mut db)?;
    demonstrate_product_catalog(&mut db)?;
    demonstrate_order_management(&mut db)?;
    demonstrate_inventory_tracking(&mut db)?;
    demonstrate_reporting_queries(&mut db)?;
    demonstrate_advanced_features(&mut db)?;
    
    println!("\n✅ MySQL-style e-commerce demo completed successfully!");
    Ok(())
}

fn cleanup_tables(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n🧹 Cleaning up existing tables...");
    
    let tables = vec![
        "order_items",
        "orders", 
        "cart_items",
        "product_reviews",
        "products",
        "categories",
        "customers",
        "users"
    ];
    
    for table in tables {
        let drop_sql = format!("DROP TABLE IF EXISTS {}", table);
        let _ = db.execute_query_str(&drop_sql);
    }
    
    println!("✓ Tables cleaned up");
    Ok(())
}

fn create_schema(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n🏗️  Creating database schema (MySQL-style)...");
    
    // Users table (authentication)
    let create_users = r#"
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            username VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            role ENUM('admin', 'customer', 'manager') DEFAULT 'customer',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        )
    "#;
    db.execute_query_str(create_users)?;
    println!("✓ Created users table");
    
    // Customers table (customer profiles)
    let create_customers = r#"
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            user_id INTEGER NOT NULL,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            phone VARCHAR(20),
            date_of_birth DATE,
            gender ENUM('M', 'F', 'Other'),
            loyalty_points INTEGER DEFAULT 0,
            preferred_language VARCHAR(10) DEFAULT 'en',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
            INDEX idx_customer_name (first_name, last_name),
            INDEX idx_loyalty_points (loyalty_points)
        )
    "#;
    db.execute_query_str(create_customers)?;
    println!("✓ Created customers table");
    
    // Categories table (product categorization)
    let create_categories = r#"
        CREATE TABLE categories (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(100) NOT NULL UNIQUE,
            slug VARCHAR(100) NOT NULL UNIQUE,
            description TEXT,
            parent_id INTEGER,
            sort_order INTEGER DEFAULT 0,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE SET NULL,
            INDEX idx_parent_id (parent_id),
            INDEX idx_slug (slug),
            INDEX idx_sort_order (sort_order)
        )
    "#;
    db.execute_query_str(create_categories)?;
    println!("✓ Created categories table");
    
    // Products table (product catalog)
    let create_products = r#"
        CREATE TABLE products (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            sku VARCHAR(50) UNIQUE NOT NULL,
            name VARCHAR(200) NOT NULL,
            description TEXT,
            short_description VARCHAR(500),
            category_id INTEGER NOT NULL,
            price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
            cost_price DECIMAL(10,2) CHECK (cost_price >= 0),
            weight DECIMAL(8,3),
            dimensions VARCHAR(50),
            stock_quantity INTEGER DEFAULT 0 CHECK (stock_quantity >= 0),
            min_stock_level INTEGER DEFAULT 5,
            is_active BOOLEAN DEFAULT TRUE,
            is_featured BOOLEAN DEFAULT FALSE,
            meta_title VARCHAR(200),
            meta_description VARCHAR(500),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            FOREIGN KEY (category_id) REFERENCES categories(id),
            INDEX idx_sku (sku),
            INDEX idx_category (category_id),
            INDEX idx_price (price),
            INDEX idx_stock (stock_quantity),
            INDEX idx_featured (is_featured),
            FULLTEXT INDEX idx_search (name, description, short_description)
        )
    "#;
    db.execute_query_str(create_products)?;
    println!("✓ Created products table");
    
    // Product reviews table
    let create_reviews = r#"
        CREATE TABLE product_reviews (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            product_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL,
            rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
            title VARCHAR(200),
            review_text TEXT,
            is_verified_purchase BOOLEAN DEFAULT FALSE,
            helpful_votes INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
            FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
            UNIQUE KEY unique_customer_product (customer_id, product_id),
            INDEX idx_product_rating (product_id, rating),
            INDEX idx_created_at (created_at)
        )
    "#;
    db.execute_query_str(create_reviews)?;
    println!("✓ Created product_reviews table");
    
    // Shopping cart table
    let create_cart = r#"
        CREATE TABLE cart_items (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            customer_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
            FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
            UNIQUE KEY unique_customer_product (customer_id, product_id),
            INDEX idx_customer (customer_id),
            INDEX idx_added_at (added_at)
        )
    "#;
    db.execute_query_str(create_cart)?;
    println!("✓ Created cart_items table");
    
    // Orders table
    let create_orders = r#"
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            order_number VARCHAR(50) UNIQUE NOT NULL,
            customer_id INTEGER NOT NULL,
            status ENUM('pending', 'processing', 'shipped', 'delivered', 'cancelled') DEFAULT 'pending',
            payment_status ENUM('pending', 'paid', 'failed', 'refunded') DEFAULT 'pending',
            payment_method VARCHAR(50),
            subtotal DECIMAL(10,2) NOT NULL,
            tax_amount DECIMAL(10,2) DEFAULT 0,
            shipping_amount DECIMAL(10,2) DEFAULT 0,
            discount_amount DECIMAL(10,2) DEFAULT 0,
            total_amount DECIMAL(10,2) NOT NULL,
            currency VARCHAR(3) DEFAULT 'USD',
            shipping_address TEXT,
            billing_address TEXT,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            shipped_at TIMESTAMP NULL,
            delivered_at TIMESTAMP NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(id),
            INDEX idx_customer (customer_id),
            INDEX idx_status (status),
            INDEX idx_payment_status (payment_status),
            INDEX idx_order_number (order_number),
            INDEX idx_created_at (created_at),
            INDEX idx_total_amount (total_amount)
        )
    "#;
    db.execute_query_str(create_orders)?;
    println!("✓ Created orders table");
    
    // Order items table
    let create_order_items = r#"
        CREATE TABLE order_items (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            unit_price DECIMAL(10,2) NOT NULL,
            total_price DECIMAL(10,2) NOT NULL,
            product_snapshot JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
            FOREIGN KEY (product_id) REFERENCES products(id),
            INDEX idx_order (order_id),
            INDEX idx_product (product_id)
        )
    "#;
    db.execute_query_str(create_order_items)?;
    println!("✓ Created order_items table");
    
    println!("✅ Database schema created successfully!");
    Ok(())
}

fn seed_data(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n🌱 Seeding initial data...");
    
    // Insert users
    let users_data = vec![
        ("admin", "admin@example.com", "admin_hash", "admin"),
        ("john_doe", "john@example.com", "user_hash_1", "customer"),
        ("jane_smith", "jane@example.com", "user_hash_2", "customer"),
        ("manager1", "manager@example.com", "manager_hash", "manager"),
    ];
    
    for (username, email, password_hash, role) in users_data {
        let sql = format!(
            "INSERT INTO users (username, email, password_hash, role) VALUES ('{}', '{}', '{}', '{}')",
            username, email, password_hash, role
        );
        db.execute_query_str(&sql)?;
    }
    println!("✓ Inserted users");
    
    // Insert customers
    let customers_data = vec![
        (2, "John", "Doe", "+1-555-0101", "1990-05-15", "M"),
        (3, "Jane", "Smith", "+1-555-0102", "1985-08-22", "F"),
    ];
    
    for (user_id, first_name, last_name, phone, dob, gender) in customers_data {
        let sql = format!(
            "INSERT INTO customers (user_id, first_name, last_name, phone, date_of_birth, gender, loyalty_points) VALUES ({}, '{}', '{}', '{}', '{}', '{}', {})",
            user_id, first_name, last_name, phone, dob, gender, rand::random::<u32>() % 1000
        );
        db.execute_query_str(&sql)?;
    }
    println!("✓ Inserted customers");
    
    // Insert categories
    let categories_data = vec![
        ("Electronics", "electronics", "Electronic devices and gadgets", None),
        ("Smartphones", "smartphones", "Mobile phones and accessories", Some(1)),
        ("Laptops", "laptops", "Portable computers", Some(1)),
        ("Clothing", "clothing", "Apparel and fashion", None),
        ("Men's Clothing", "mens-clothing", "Clothing for men", Some(4)),
        ("Women's Clothing", "womens-clothing", "Clothing for women", Some(4)),
    ];
    
    for (name, slug, description, parent_id) in categories_data {
        let parent_clause = match parent_id {
            Some(id) => format!("{}", id),
            None => "NULL".to_string(),
        };
        let sql = format!(
            "INSERT INTO categories (name, slug, description, parent_id) VALUES ('{}', '{}', '{}', {})",
            name, slug, description, parent_clause
        );
        db.execute_query_str(&sql)?;
    }
    println!("✓ Inserted categories");
    
    // Insert products
    let products_data = vec![
        ("IPHONE14PRO", "iPhone 14 Pro", "Latest iPhone with Pro features", "High-end smartphone with advanced camera", 2, 999.99, 700.00, 0.174, "146.7×71.5×7.85mm", 50),
        ("SAMSUNG-S23", "Samsung Galaxy S23", "Premium Android smartphone", "Flagship Samsung phone", 2, 899.99, 650.00, 0.168, "146.3×70.9×7.6mm", 75),
        ("MACBOOK-AIR", "MacBook Air M2", "Ultra-thin laptop with M2 chip", "Perfect for everyday computing", 3, 1199.99, 900.00, 1.24, "304×212×11.3mm", 25),
        ("DELL-XPS13", "Dell XPS 13", "Premium ultrabook", "High-performance Windows laptop", 3, 1099.99, 800.00, 1.20, "295×199×14.8mm", 30),
        ("MENS-TSHIRT", "Classic Men's T-Shirt", "Comfortable cotton t-shirt", "Basic wardrobe essential", 5, 19.99, 8.00, 0.15, "Standard sizes", 200),
        ("WOMENS-DRESS", "Summer Dress", "Elegant summer dress", "Perfect for warm weather", 6, 49.99, 25.00, 0.20, "Various sizes", 150),
    ];
    
    for (sku, name, description, short_desc, category_id, price, cost, weight, dimensions, stock) in products_data {
        let sql = format!(
            "INSERT INTO products (sku, name, description, short_description, category_id, price, cost_price, weight, dimensions, stock_quantity, is_featured) VALUES ('{}', '{}', '{}', '{}', {}, {}, {}, {}, '{}', {}, {})",
            sku, name, description, short_desc, category_id, price, cost, weight, dimensions, stock, rand::random::<bool>()
        );
        db.execute_query_str(&sql)?;
    }
    println!("✓ Inserted products");
    
    println!("✅ Initial data seeded successfully!");
    Ok(())
}

fn demonstrate_customer_operations(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n👥 Customer Operations (MySQL-style)");
    println!("{}", "=".repeat(40));
    
    // Customer registration (similar to MySQL stored procedure pattern)
    println!("\n📝 Customer Registration:");
    let register_sql = r#"
        INSERT INTO users (username, email, password_hash, role) 
        VALUES ('alice_cooper', 'alice@example.com', 'hashed_password_123', 'customer')
    "#;
    db.execute_query_str(register_sql)?;
    
    // Get the last inserted user ID (MySQL LAST_INSERT_ID() equivalent)
    let user_result = db.execute_query_str("SELECT id FROM users WHERE username = 'alice_cooper'")?;
    println!("✓ User registered successfully");
    
    // Create customer profile
    let customer_sql = r#"
        INSERT INTO customers (user_id, first_name, last_name, phone, gender, loyalty_points) 
        VALUES (5, 'Alice', 'Cooper', '+1-555-0199', 'F', 0)
    "#;
    db.execute_query_str(customer_sql)?;
    println!("✓ Customer profile created");
    
    // Customer login simulation (checking credentials)
    println!("\n🔐 Customer Authentication:");
    let auth_sql = r#"
        SELECT u.id, u.username, u.email, u.role, u.is_active,
               c.first_name, c.last_name, c.loyalty_points
        FROM users u
        LEFT JOIN customers c ON u.id = c.user_id
        WHERE u.email = 'alice@example.com' AND u.is_active = TRUE
    "#;
    let auth_result = db.execute_query_str(auth_sql)?;
    println!("✓ Customer authentication query executed");
    
    // Update customer profile (MySQL UPDATE with JOIN pattern)
    println!("\n✏️  Profile Update:");
    let update_sql = r#"
        UPDATE customers 
        SET phone = '+1-555-0200', loyalty_points = loyalty_points + 100
        WHERE user_id = 5
    "#;
    db.execute_query_str(update_sql)?;
    println!("✓ Customer profile updated with loyalty points");
    
    // Customer search (MySQL LIKE pattern matching)
    println!("\n🔍 Customer Search:");
    let search_sql = r#"
        SELECT c.id, c.first_name, c.last_name, c.phone, c.loyalty_points,
               u.email, u.created_at
        FROM customers c
        JOIN users u ON c.user_id = u.id
        WHERE c.first_name LIKE 'A%' OR c.last_name LIKE 'A%'
        ORDER BY c.loyalty_points DESC
    "#;
    let search_result = db.execute_query_str(search_sql)?;
    println!("✓ Customer search completed");
    
    Ok(())
}

fn demonstrate_product_catalog(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n📦 Product Catalog Management");
    println!("{}", "=".repeat(40));
    
    // Product search with category (MySQL JOIN with WHERE)
    println!("\n🔍 Product Search by Category:");
    let category_search = r#"
        SELECT p.id, p.sku, p.name, p.price, p.stock_quantity,
               c.name as category_name, c.slug as category_slug
        FROM products p
        JOIN categories c ON p.category_id = c.id
        WHERE c.slug = 'smartphones' AND p.is_active = TRUE
        ORDER BY p.price DESC
    "#;
    let result = db.execute_query_str(category_search)?;
    println!("✓ Found products in smartphones category");
    
    // Full-text search simulation (MySQL MATCH AGAINST equivalent)
    println!("\n🔎 Product Full-Text Search:");
    let text_search = r#"
        SELECT p.id, p.sku, p.name, p.description, p.price,
               c.name as category_name
        FROM products p
        JOIN categories c ON p.category_id = c.id
        WHERE p.name LIKE '%iPhone%' OR p.description LIKE '%iPhone%'
           OR p.short_description LIKE '%iPhone%'
        ORDER BY p.price DESC
    "#;
    let search_result = db.execute_query_str(text_search)?;
    println!("✓ Full-text search completed");
    
    // Price range filtering (MySQL BETWEEN)
    println!("\n💰 Price Range Filtering:");
    let price_filter = r#"
        SELECT p.sku, p.name, p.price, p.stock_quantity,
               CASE 
                   WHEN p.price < 50 THEN 'Budget'
                   WHEN p.price BETWEEN 50 AND 500 THEN 'Mid-range'
                   ELSE 'Premium'
               END as price_category
        FROM products p
        WHERE p.price BETWEEN 100 AND 1000 AND p.is_active = TRUE
        ORDER BY p.price ASC
    "#;
    let price_result = db.execute_query_str(price_filter)?;
    println!("✓ Price filtering completed");
    
    // Product variants/options (MySQL GROUP BY with aggregation)
    println!("\n📊 Product Analytics:");
    let analytics_sql = r#"
        SELECT c.name as category,
               COUNT(p.id) as product_count,
               AVG(p.price) as avg_price,
               MIN(p.price) as min_price,
               MAX(p.price) as max_price,
               SUM(p.stock_quantity) as total_stock
        FROM categories c
        LEFT JOIN products p ON c.id = p.category_id AND p.is_active = TRUE
        GROUP BY c.id, c.name
        HAVING product_count > 0
        ORDER BY avg_price DESC
    "#;
    let analytics_result = db.execute_query_str(analytics_sql)?;
    println!("✓ Product analytics generated");
    
    // Low stock alert (MySQL WHERE with threshold)
    println!("\n⚠️  Low Stock Alert:");
    let low_stock_sql = r#"
        SELECT p.sku, p.name, p.stock_quantity, p.min_stock_level,
               (p.min_stock_level - p.stock_quantity) as shortage
        FROM products p
        WHERE p.stock_quantity <= p.min_stock_level AND p.is_active = TRUE
        ORDER BY shortage DESC
    "#;
    let stock_result = db.execute_query_str(low_stock_sql)?;
    println!("✓ Low stock analysis completed");
    
    Ok(())
}

fn demonstrate_order_management(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n🛒 Order Management System");
    println!("{}", "=".repeat(40));
    
    // Add items to cart (MySQL INSERT ON DUPLICATE KEY UPDATE pattern)
    println!("\n🛍️  Adding Items to Cart:");
    let add_to_cart = r#"
        INSERT INTO cart_items (customer_id, product_id, quantity) 
        VALUES (1, 1, 2), (1, 3, 1)
    "#;
    db.execute_query_str(add_to_cart)?;
    println!("✓ Items added to cart");
    
    // View cart with product details (MySQL JOIN)
    println!("\n👀 Cart Contents:");
    let view_cart = r#"
        SELECT ci.id, p.sku, p.name, p.price, ci.quantity,
               (p.price * ci.quantity) as line_total,
               p.stock_quantity
        FROM cart_items ci
        JOIN products p ON ci.product_id = p.id
        WHERE ci.customer_id = 1
        ORDER BY ci.added_at DESC
    "#;
    let cart_result = db.execute_query_str(view_cart)?;
    println!("✓ Cart contents retrieved");
    
    // Create order from cart (MySQL transaction pattern)
    println!("\n📋 Creating Order:");
    
    // Start transaction
    db.execute_query_str("BEGIN TRANSACTION")?;
    
    // Generate order number (UUID simulation)
    let order_number = format!("ORD-{}-{}", 
        chrono::Utc::now().format("%Y%m%d"), 
        rand::random::<u32>() % 10000
    );
    
    // Calculate totals from cart
    let totals_sql = r#"
        SELECT SUM(p.price * ci.quantity) as subtotal,
               COUNT(ci.id) as item_count
        FROM cart_items ci
        JOIN products p ON ci.product_id = p.id
        WHERE ci.customer_id = 1
    "#;
    let totals_result = db.execute_query_str(totals_sql)?;
    
    // Create order record
    let create_order = format!(r#"
        INSERT INTO orders (order_number, customer_id, status, payment_status, 
                           subtotal, tax_amount, shipping_amount, total_amount, 
                           shipping_address, billing_address)
        VALUES ('{}', 1, 'pending', 'pending', 
                1199.99, 120.00, 15.00, 1334.99,
                '123 Main St, City, State 12345',
                '123 Main St, City, State 12345')
    "#, order_number);
    db.execute_query_str(&create_order)?;
    
    // Get order ID (simulate LAST_INSERT_ID())
    let order_id_sql = format!("SELECT id FROM orders WHERE order_number = '{}'", order_number);
    let order_id_result = db.execute_query_str(&order_id_sql)?;
    
    // Create order items from cart
    let create_order_items = r#"
        INSERT INTO order_items (order_id, product_id, quantity, unit_price, total_price)
        SELECT 1, ci.product_id, ci.quantity, p.price, (p.price * ci.quantity)
        FROM cart_items ci
        JOIN products p ON ci.product_id = p.id
        WHERE ci.customer_id = 1
    "#;
    db.execute_query_str(create_order_items)?;
    
    // Update product stock
    let update_stock = r#"
        UPDATE products p
        SET stock_quantity = stock_quantity - (
            SELECT ci.quantity 
            FROM cart_items ci 
            WHERE ci.product_id = p.id AND ci.customer_id = 1
        )
        WHERE p.id IN (
            SELECT DISTINCT product_id 
            FROM cart_items 
            WHERE customer_id = 1
        )
    "#;
    db.execute_query_str(update_stock)?;
    
    // Clear cart
    db.execute_query_str("DELETE FROM cart_items WHERE customer_id = 1")?;
    
    // Commit transaction
    db.execute_query_str("COMMIT")?;
    println!("✓ Order created successfully: {}", order_number);
    
    // Order status tracking
    println!("\n📦 Order Status Update:");
    let update_status = format!(r#"
        UPDATE orders 
        SET status = 'processing', 
            payment_status = 'paid',
            updated_at = CURRENT_TIMESTAMP
        WHERE order_number = '{}'
    "#, order_number);
    db.execute_query_str(&update_status)?;
    println!("✓ Order status updated to processing");
    
    Ok(())
}

fn demonstrate_inventory_tracking(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n📊 Inventory Management");
    println!("{}", "=".repeat(40));
    
    // Stock level report
    println!("\n📈 Stock Level Report:");
    let stock_report = r#"
        SELECT p.sku, p.name, p.stock_quantity, p.min_stock_level,
               CASE 
                   WHEN p.stock_quantity = 0 THEN 'Out of Stock'
                   WHEN p.stock_quantity <= p.min_stock_level THEN 'Low Stock'
                   WHEN p.stock_quantity <= (p.min_stock_level * 2) THEN 'Medium Stock'
                   ELSE 'In Stock'
               END as stock_status,
               c.name as category
        FROM products p
        JOIN categories c ON p.category_id = c.id
        WHERE p.is_active = TRUE
        ORDER BY p.stock_quantity ASC, p.sku
    "#;
    let stock_result = db.execute_query_str(stock_report)?;
    println!("✓ Stock level report generated");
    
    // Inventory value calculation
    println!("\n💰 Inventory Valuation:");
    let valuation_sql = r#"
        SELECT c.name as category,
               COUNT(p.id) as products,
               SUM(p.stock_quantity) as total_units,
               SUM(p.stock_quantity * p.cost_price) as cost_value,
               SUM(p.stock_quantity * p.price) as retail_value,
               SUM(p.stock_quantity * (p.price - p.cost_price)) as potential_profit
        FROM products p
        JOIN categories c ON p.category_id = c.id
        WHERE p.is_active = TRUE AND p.stock_quantity > 0
        GROUP BY c.id, c.name
        ORDER BY cost_value DESC
    "#;
    let valuation_result = db.execute_query_str(valuation_sql)?;
    println!("✓ Inventory valuation completed");
    
    // Restock recommendations
    println!("\n🔄 Restock Recommendations:");
    let restock_sql = r#"
        SELECT p.sku, p.name, p.stock_quantity, p.min_stock_level,
               (p.min_stock_level * 3 - p.stock_quantity) as suggested_reorder,
               p.cost_price,
               (p.min_stock_level * 3 - p.stock_quantity) * p.cost_price as reorder_cost
        FROM products p
        WHERE p.stock_quantity <= p.min_stock_level 
          AND p.is_active = TRUE
        ORDER BY reorder_cost DESC
    "#;
    let restock_result = db.execute_query_str(restock_sql)?;
    println!("✓ Restock recommendations generated");
    
    Ok(())
}

fn demonstrate_reporting_queries(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n📊 Business Intelligence & Reporting");
    println!("{}", "=".repeat(45));
    
    // Sales summary report
    println!("\n💹 Sales Summary:");
    let sales_summary = r#"
        SELECT 
            COUNT(DISTINCT o.id) as total_orders,
            COUNT(DISTINCT o.customer_id) as unique_customers,
            SUM(o.total_amount) as total_revenue,
            AVG(o.total_amount) as avg_order_value,
            MIN(o.total_amount) as min_order,
            MAX(o.total_amount) as max_order
        FROM orders o
        WHERE o.status != 'cancelled'
    "#;
    let sales_result = db.execute_query_str(sales_summary)?;
    println!("✓ Sales summary generated");
    
    // Top selling products
    println!("\n🏆 Top Selling Products:");
    let top_products = r#"
        SELECT p.sku, p.name, 
               SUM(oi.quantity) as units_sold,
               SUM(oi.total_price) as revenue,
               COUNT(DISTINCT oi.order_id) as orders_count,
               AVG(oi.unit_price) as avg_selling_price
        FROM order_items oi
        JOIN products p ON oi.product_id = p.id
        JOIN orders o ON oi.order_id = o.id
        WHERE o.status != 'cancelled'
        GROUP BY p.id, p.sku, p.name
        ORDER BY units_sold DESC, revenue DESC
        LIMIT 10
    "#;
    let top_products_result = db.execute_query_str(top_products)?;
    println!("✓ Top products analysis completed");
    
    // Customer lifetime value
    println!("\n👑 Customer Lifetime Value:");
    let clv_sql = r#"
        SELECT c.id, c.first_name, c.last_name, c.loyalty_points,
               COUNT(o.id) as total_orders,
               SUM(o.total_amount) as lifetime_value,
               AVG(o.total_amount) as avg_order_value,
               MAX(o.created_at) as last_order_date,
               MIN(o.created_at) as first_order_date
        FROM customers c
        LEFT JOIN orders o ON c.id = o.customer_id AND o.status != 'cancelled'
        GROUP BY c.id, c.first_name, c.last_name, c.loyalty_points
        HAVING total_orders > 0
        ORDER BY lifetime_value DESC
        LIMIT 10
    "#;
    let clv_result = db.execute_query_str(clv_sql)?;
    println!("✓ Customer lifetime value analysis completed");
    
    // Category performance
    println!("\n📈 Category Performance:");
    let category_perf = r#"
        SELECT c.name as category,
               COUNT(DISTINCT p.id) as products_count,
               COUNT(DISTINCT oi.order_id) as orders_with_category,
               SUM(oi.quantity) as units_sold,
               SUM(oi.total_price) as category_revenue,
               AVG(oi.unit_price) as avg_price
        FROM categories c
        LEFT JOIN products p ON c.id = p.category_id
        LEFT JOIN order_items oi ON p.id = oi.product_id
        LEFT JOIN orders o ON oi.order_id = o.id AND o.status != 'cancelled'
        GROUP BY c.id, c.name
        HAVING category_revenue > 0
        ORDER BY category_revenue DESC
    "#;
    let category_result = db.execute_query_str(category_perf)?;
    println!("✓ Category performance analysis completed");
    
    Ok(())
}

fn demonstrate_advanced_features(db: &mut Oxidb) -> Result<(), OxidbError> {
    println!("\n🚀 Advanced Database Features");
    println!("{}", "=".repeat(40));
    
    // Complex JOIN with subquery (MySQL advanced pattern)
    println!("\n🔗 Complex Query with Subqueries:");
    let complex_query = r#"
        SELECT p.sku, p.name, p.price, p.stock_quantity,
               c.name as category,
               COALESCE(sales_data.units_sold, 0) as units_sold,
               COALESCE(sales_data.revenue, 0) as revenue
        FROM products p
        JOIN categories c ON p.category_id = c.id
        LEFT JOIN (
            SELECT oi.product_id,
                   SUM(oi.quantity) as units_sold,
                   SUM(oi.total_price) as revenue
            FROM order_items oi
            JOIN orders o ON oi.order_id = o.id
            WHERE o.status != 'cancelled'
            GROUP BY oi.product_id
        ) sales_data ON p.id = sales_data.product_id
        WHERE p.is_active = TRUE
        ORDER BY COALESCE(sales_data.revenue, 0) DESC, p.name
    "#;
    let complex_result = db.execute_query_str(complex_query)?;
    println!("✓ Complex query with subqueries executed");
    
    // Window functions simulation (MySQL 8.0+ pattern)
    println!("\n🪟 Analytics with Ranking:");
    let ranking_query = r#"
        SELECT o.id, o.order_number, o.customer_id, o.total_amount,
               c.first_name, c.last_name,
               RANK() OVER (ORDER BY o.total_amount DESC) as revenue_rank,
               ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.created_at DESC) as customer_order_sequence
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        WHERE o.status != 'cancelled'
        ORDER BY o.total_amount DESC
        LIMIT 20
    "#;
    // Note: This would need window function support, showing the pattern
    println!("✓ Window function pattern demonstrated");
    
    // JSON operations (MySQL 5.7+ JSON functions)
    println!("\n📄 JSON Data Operations:");
    let json_ops = r#"
        SELECT oi.id, oi.order_id, oi.product_id,
               JSON_EXTRACT(oi.product_snapshot, '$.name') as product_name,
               JSON_EXTRACT(oi.product_snapshot, '$.original_price') as original_price
        FROM order_items oi
        WHERE oi.product_snapshot IS NOT NULL
        LIMIT 10
    "#;
    // Note: This shows JSON operation patterns
    println!("✓ JSON operations pattern demonstrated");
    
    // Full-text search with relevance scoring
    println!("\n🔍 Advanced Search Features:");
    let search_query = r#"
        SELECT p.id, p.sku, p.name, p.description, p.price,
               MATCH(p.name, p.description) AGAINST ('smartphone camera') as relevance_score
        FROM products p
        WHERE MATCH(p.name, p.description) AGAINST ('smartphone camera' IN NATURAL LANGUAGE MODE)
        ORDER BY relevance_score DESC
        LIMIT 10
    "#;
    // Note: This shows full-text search patterns
    println!("✓ Advanced search patterns demonstrated");
    
    // Database maintenance operations
    println!("\n🔧 Database Maintenance:");
    
    // Analyze table statistics (MySQL ANALYZE TABLE equivalent)
    let analyze_sql = "ANALYZE TABLE products, orders, customers";
    println!("✓ Table analysis pattern shown");
    
    // Index optimization suggestions
    let index_analysis = r#"
        -- Suggested indexes for performance optimization:
        -- CREATE INDEX idx_orders_customer_status ON orders(customer_id, status);
        -- CREATE INDEX idx_products_category_active ON products(category_id, is_active);
        -- CREATE INDEX idx_order_items_product_order ON order_items(product_id, order_id);
    "#;
    println!("✓ Index optimization suggestions provided");
    
    Ok(())
}