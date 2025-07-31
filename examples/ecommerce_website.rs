//! E-commerce Website Database Example
//! 
//! This example demonstrates using OxiDB as a backend database for an e-commerce website.
//! It includes:
//! - Product catalog with vector embeddings for similarity search
//! - User management with authentication
//! - Order processing and tracking
//! - Shopping cart functionality
//! - Product recommendations using vector similarity

use oxidb::{Connection, OxidbError, QueryResult};
use oxidb::core::types::{DataType, OrderedFloat, HashableVectorData, VectorData};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Product {
    id: String,
    name: String,
    description: String,
    price: f64,
    category: String,
    stock: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    embedding: Option<Vec<f32>>, // Vector embedding for similarity search
    tags: Vec<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct User {
    id: String,
    email: String,
    name: String,
    password_hash: String,
    shipping_address: Address,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Address {
    street: String,
    city: String,
    state: String,
    zip: String,
    country: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    id: String,
    user_id: String,
    items: Vec<OrderItem>,
    total: f64,
    status: OrderStatus,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderItem {
    product_id: String,
    quantity: i64,
    price_at_purchase: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum OrderStatus {
    Pending,
    Processing,
    Shipped,
    Delivered,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShoppingCart {
    user_id: String,
    items: Vec<CartItem>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CartItem {
    product_id: String,
    quantity: i64,
}

struct EcommerceDB {
    db: Connection,
}

impl EcommerceDB {
    fn new(db_path: &str) -> Result<Self, OxidbError> {
        let mut db = Connection::open(db_path)?;
        
        // Create tables for our e-commerce data
        db.execute("CREATE TABLE IF NOT EXISTS products (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            price FLOAT NOT NULL,
            category TEXT,
            stock INTEGER DEFAULT 0,
            embedding VECTOR[384],
            tags TEXT,
            created_at TEXT,
            updated_at TEXT
        )")?;
        
        db.execute("CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            shipping_address TEXT,
            created_at TEXT
        )")?;
        
        db.execute("CREATE TABLE IF NOT EXISTS orders (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            items TEXT NOT NULL,
            total FLOAT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT,
            updated_at TEXT
        )")?;
        
        db.execute("CREATE TABLE IF NOT EXISTS shopping_carts (
            user_id TEXT PRIMARY KEY,
            items TEXT NOT NULL,
            updated_at TEXT
        )")?;
        
        // Create indexes for better performance
        db.execute("CREATE INDEX IF NOT EXISTS idx_products_category ON products(category)")?;
        db.execute("CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id)")?;
        db.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)")?;
        
        Ok(EcommerceDB { db })
    }
    
    // Product management
    fn add_product(&mut self, product: &Product) -> Result<(), OxidbError> {
        let tags_json = serde_json::to_string(&product.tags).unwrap();
        let embedding_vector = if let Some(emb) = &product.embedding {
            format!("[{}]", emb.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(","))
        } else {
            "NULL".to_string()
        };
        
        let sql = format!(
            "INSERT INTO products (id, name, description, price, category, stock, embedding, tags, created_at, updated_at) 
             VALUES ('{}', '{}', '{}', {}, '{}', {}, {}, '{}', '{}', '{}')",
            product.id,
            product.name.replace("'", "''"),
            product.description.replace("'", "''"),
            product.price,
            product.category,
            product.stock,
            embedding_vector,
            tags_json.replace("'", "''"),
            product.created_at.to_rfc3339(),
            product.updated_at.to_rfc3339()
        );
        
        self.db.execute(&sql)?;
        Ok(())
    }
    
    fn get_product(&mut self, product_id: &str) -> Result<Option<Product>, OxidbError> {
        let sql = format!("SELECT * FROM products WHERE id = '{}'", product_id);
        let result = self.db.execute(&sql)?;
        
        match result {
            QueryResult::Data(data) => {
                if let Some(row) = data.rows().next() {
                    Ok(Some(self.row_to_product_from_row(row)?))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }
    
    fn search_products_by_category(&mut self, category: &str) -> Result<Vec<Product>, OxidbError> {
        let sql = format!("SELECT * FROM products WHERE category = '{}'", category);
        let result = self.db.execute(&sql)?;
        
        result.rows.iter()
            .map(|row| self.row_to_product(row))
            .collect()
    }
    
    fn find_similar_products(&mut self, product_id: &str, limit: usize) -> Result<Vec<Product>, OxidbError> {
        // First get the product's embedding
        let product = self.get_product(product_id)?
            .ok_or_else(|| OxidbError::KeyNotFound)?;
        
        if let Some(embedding) = product.embedding {
            // Use vector similarity search to find similar products
            let embedding_str = format!("[{}]", embedding.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(","));
            
            // This would use OxiDB's vector similarity capabilities
            let sql = format!(
                "SELECT * FROM products 
                 WHERE id != '{}' AND embedding IS NOT NULL
                 ORDER BY vector_distance(embedding, {}) ASC
                 LIMIT {}",
                product_id, embedding_str, limit
            );
            
            let result = self.db.execute(&sql)?;
            result.rows.iter()
                .map(|row| self.row_to_product(row))
                .collect()
        } else {
            // Fallback to category-based recommendations
            self.search_products_by_category(&product.category)?
                .into_iter()
                .filter(|p| p.id != product_id)
                .take(limit)
                .collect::<Vec<_>>()
                .into()
        }
    }
    
    // User management
    fn create_user(&mut self, user: &User) -> Result<(), OxidbError> {
        let address_json = serde_json::to_string(&user.shipping_address).unwrap();
        
        let sql = format!(
            "INSERT INTO users (id, email, name, password_hash, shipping_address, created_at) 
             VALUES ('{}', '{}', '{}', '{}', '{}', '{}')",
            user.id,
            user.email,
            user.name.replace("'", "''"),
            user.password_hash,
            address_json.replace("'", "''"),
            user.created_at.to_rfc3339()
        );
        
        self.db.execute(&sql)?;
        Ok(())
    }
    
    fn get_user_by_email(&mut self, email: &str) -> Result<Option<User>, OxidbError> {
        let sql = format!("SELECT * FROM users WHERE email = '{}'", email);
        let result = self.db.execute(&sql)?;
        
        if let Some(row) = result.rows.first() {
            Ok(Some(self.row_to_user(row)?))
        } else {
            Ok(None)
        }
    }
    
    // Order management
    fn create_order(&mut self, order: &Order) -> Result<(), OxidbError> {
        let items_json = serde_json::to_string(&order.items).unwrap();
        let status_str = serde_json::to_string(&order.status).unwrap();
        
        let sql = format!(
            "INSERT INTO orders (id, user_id, items, total, status, created_at, updated_at) 
             VALUES ('{}', '{}', '{}', {}, {}, '{}', '{}')",
            order.id,
            order.user_id,
            items_json.replace("'", "''"),
            order.total,
            status_str,
            order.created_at.to_rfc3339(),
            order.updated_at.to_rfc3339()
        );
        
        self.db.execute(&sql)?;
        
        // Update product stock
        for item in &order.items {
            self.update_product_stock(&item.product_id, -item.quantity)?;
        }
        
        Ok(())
    }
    
    fn get_user_orders(&mut self, user_id: &str) -> Result<Vec<Order>, OxidbError> {
        let sql = format!("SELECT * FROM orders WHERE user_id = '{}'", user_id);
        let result = self.db.execute(&sql)?;
        
        result.rows.iter()
            .map(|row| self.row_to_order(row))
            .collect()
    }
    
    // Shopping cart
    fn update_cart(&mut self, cart: &ShoppingCart) -> Result<(), OxidbError> {
        let items_json = serde_json::to_string(&cart.items).unwrap();
        
        let sql = format!(
            "INSERT OR REPLACE INTO shopping_carts (user_id, items, updated_at) 
             VALUES ('{}', '{}', '{}')",
            cart.user_id,
            items_json.replace("'", "''"),
            cart.updated_at.to_rfc3339()
        );
        
        self.db.execute(&sql)?;
        Ok(())
    }
    
    fn get_cart(&mut self, user_id: &str) -> Result<Option<ShoppingCart>, OxidbError> {
        let sql = format!("SELECT * FROM shopping_carts WHERE user_id = '{}'", user_id);
        let result = self.db.execute(&sql)?;
        
        if let Some(row) = result.rows.first() {
            Ok(Some(self.row_to_cart(row)?))
        } else {
            Ok(None)
        }
    }
    
    // Helper methods
    fn update_product_stock(&mut self, product_id: &str, quantity_change: i64) -> Result<(), OxidbError> {
        let sql = format!(
            "UPDATE products SET stock = stock + {} WHERE id = '{}'",
            quantity_change, product_id
        );
        self.db.execute(&sql)?;
        Ok(())
    }
    
    fn row_to_product(&mut self, row: &[DataType]) -> Result<Product, OxidbError> {
        // Parse row data into Product struct
        // This is a simplified version - in production you'd want more robust parsing
        Ok(Product {
            id: self.get_string(&row[0])?,
            name: self.get_string(&row[1])?,
            description: self.get_string(&row[2])?,
            price: self.get_float(&row[3])?,
            category: self.get_string(&row[4])?,
            stock: self.get_integer(&row[5])?,
            embedding: self.get_vector(&row[6])?,
            tags: serde_json::from_str(&self.get_string(&row[7])?).unwrap_or_default(),
            created_at: DateTime::parse_from_rfc3339(&self.get_string(&row[8])?)
                .unwrap()
                .with_timezone(&Utc),
            updated_at: DateTime::parse_from_rfc3339(&self.get_string(&row[9])?)
                .unwrap()
                .with_timezone(&Utc),
        })
    }
    
    fn row_to_user(&mut self, row: &[DataType]) -> Result<User, OxidbError> {
        Ok(User {
            id: self.get_string(&row[0])?,
            email: self.get_string(&row[1])?,
            name: self.get_string(&row[2])?,
            password_hash: self.get_string(&row[3])?,
            shipping_address: serde_json::from_str(&self.get_string(&row[4])?).unwrap(),
            created_at: DateTime::parse_from_rfc3339(&self.get_string(&row[5])?)
                .unwrap()
                .with_timezone(&Utc),
        })
    }
    
    fn row_to_order(&mut self, row: &[DataType]) -> Result<Order, OxidbError> {
        Ok(Order {
            id: self.get_string(&row[0])?,
            user_id: self.get_string(&row[1])?,
            items: serde_json::from_str(&self.get_string(&row[2])?).unwrap(),
            total: self.get_float(&row[3])?,
            status: serde_json::from_str(&self.get_string(&row[4])?).unwrap(),
            created_at: DateTime::parse_from_rfc3339(&self.get_string(&row[5])?)
                .unwrap()
                .with_timezone(&Utc),
            updated_at: DateTime::parse_from_rfc3339(&self.get_string(&row[6])?)
                .unwrap()
                .with_timezone(&Utc),
        })
    }
    
    fn row_to_cart(&mut self, row: &[DataType]) -> Result<ShoppingCart, OxidbError> {
        Ok(ShoppingCart {
            user_id: self.get_string(&row[0])?,
            items: serde_json::from_str(&self.get_string(&row[1])?).unwrap(),
            updated_at: DateTime::parse_from_rfc3339(&self.get_string(&row[2])?)
                .unwrap()
                .with_timezone(&Utc),
        })
    }
    
    fn get_string(&mut self, data: &DataType) -> Result<String, OxidbError> {
        match data {
            DataType::String(s) => Ok(s.clone()),
            DataType::Null => Ok(String::new()),
            _ => Err(OxidbError::TypeMismatch),
        }
    }
    
    fn get_float(&mut self, data: &DataType) -> Result<f64, OxidbError> {
        match data {
            DataType::Float(f) => Ok(f.0),
            DataType::Integer(i) => Ok(*i as f64),
            _ => Err(OxidbError::TypeMismatch),
        }
    }
    
    fn get_integer(&mut self, data: &DataType) -> Result<i64, OxidbError> {
        match data {
            DataType::Integer(i) => Ok(*i),
            _ => Err(OxidbError::TypeMismatch),
        }
    }
    
    fn get_vector(&mut self, data: &DataType) -> Result<Option<Vec<f32>>, OxidbError> {
        match data {
            DataType::Vector(v) => Ok(Some(v.0.data.clone())),
            DataType::Null => Ok(None),
            _ => Err(OxidbError::TypeMismatch),
        }
    }
}

// Example usage
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== E-commerce Website Database Example ===\n");
    
    // Initialize database
    let db = EcommerceDB::new("ecommerce.db")?;
    
    // Create sample products
    let products = vec![
        Product {
            id: "prod_001".to_string(),
            name: "Wireless Headphones".to_string(),
            description: "High-quality Bluetooth headphones with noise cancellation".to_string(),
            price: 149.99,
            category: "Electronics".to_string(),
            stock: 50,
            embedding: Some(vec![0.1, 0.2, 0.3, 0.4, 0.5]), // Simplified embedding
            tags: vec!["audio".to_string(), "wireless".to_string(), "bluetooth".to_string()],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        },
        Product {
            id: "prod_002".to_string(),
            name: "Smart Watch".to_string(),
            description: "Fitness tracking smartwatch with heart rate monitor".to_string(),
            price: 299.99,
            category: "Electronics".to_string(),
            stock: 30,
            embedding: Some(vec![0.2, 0.3, 0.4, 0.5, 0.6]), // Similar to headphones
            tags: vec!["wearable".to_string(), "fitness".to_string(), "smart".to_string()],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        },
        Product {
            id: "prod_003".to_string(),
            name: "Running Shoes".to_string(),
            description: "Comfortable running shoes with advanced cushioning".to_string(),
            price: 89.99,
            category: "Sports".to_string(),
            stock: 100,
            embedding: Some(vec![0.7, 0.8, 0.9, 0.1, 0.2]), // Different category
            tags: vec!["footwear".to_string(), "running".to_string(), "athletic".to_string()],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        },
    ];
    
    // Add products to database
    println!("Adding products to database...");
    for product in &products {
        db.add_product(product)?;
        println!("Added: {} - ${}", product.name, product.price);
    }
    
    // Create a user
    let user = User {
        id: "user_001".to_string(),
        email: "john.doe@example.com".to_string(),
        name: "John Doe".to_string(),
        password_hash: "hashed_password_here".to_string(),
        shipping_address: Address {
            street: "123 Main St".to_string(),
            city: "San Francisco".to_string(),
            state: "CA".to_string(),
            zip: "94105".to_string(),
            country: "USA".to_string(),
        },
        created_at: Utc::now(),
    };
    
    println!("\nCreating user account...");
    db.create_user(&user)?;
    println!("User created: {}", user.email);
    
    // Add items to shopping cart
    let cart = ShoppingCart {
        user_id: user.id.clone(),
        items: vec![
            CartItem {
                product_id: "prod_001".to_string(),
                quantity: 2,
            },
            CartItem {
                product_id: "prod_002".to_string(),
                quantity: 1,
            },
        ],
        updated_at: Utc::now(),
    };
    
    println!("\nUpdating shopping cart...");
    db.update_cart(&cart)?;
    println!("Cart updated with {} items", cart.items.len());
    
    // Create an order
    let order = Order {
        id: "order_001".to_string(),
        user_id: user.id.clone(),
        items: vec![
            OrderItem {
                product_id: "prod_001".to_string(),
                quantity: 2,
                price_at_purchase: 149.99,
            },
            OrderItem {
                product_id: "prod_002".to_string(),
                quantity: 1,
                price_at_purchase: 299.99,
            },
        ],
        total: 599.97,
        status: OrderStatus::Processing,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    
    println!("\nCreating order...");
    db.create_order(&order)?;
    println!("Order created: {} - Total: ${}", order.id, order.total);
    
    // Search products by category
    println!("\nSearching for Electronics...");
    let electronics = db.search_products_by_category("Electronics")?;
    for product in &electronics {
        println!("- {} (${}) - {} in stock", product.name, product.price, product.stock);
    }
    
    // Find similar products
    println!("\nFinding products similar to Wireless Headphones...");
    let similar = db.find_similar_products("prod_001", 3)?;
    for product in &similar {
        println!("- {} ({})", product.name, product.category);
    }
    
    // Get user orders
    println!("\nUser order history:");
    let user_orders = db.get_user_orders(&user.id)?;
    for order in &user_orders {
        println!("- Order {}: ${} - Status: {:?}", order.id, order.total, order.status);
    }
    
    Ok(())
}