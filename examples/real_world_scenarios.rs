use oxidb::{Connection, OxidbError};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use serde::{Serialize, Deserialize};

/// Real-world scenario tests for Oxidb
/// Demonstrates practical usage patterns following SOLID, GRASP, and CLEAN principles
/// Each scenario is self-contained (SRP) and demonstrates specific use cases

#[derive(Debug, Serialize, Deserialize, Clone)]
struct User {
    id: u64,
    username: String,
    email: String,
    created_at: String,
    is_active: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Product {
    id: u64,
    name: String,
    price: f64,
    category: String,
    stock_quantity: i32,
    description: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Order {
    id: u64,
    user_id: u64,
    product_ids: Vec<u64>,
    total_amount: f64,
    status: String,
    created_at: String,
}

/// Interface Segregation Principle - Separate concerns for different operations
trait UserRepository {
    fn create_user(&self, user: &User) -> Result<(), OxidbError>;
    fn find_user_by_email(&self, email: &str) -> Result<Option<User>, OxidbError>;
    fn update_user_status(&self, user_id: u64, is_active: bool) -> Result<(), OxidbError>;
}

trait ProductRepository {
    fn create_product(&self, product: &Product) -> Result<(), OxidbError>;
    fn find_products_by_category(&self, category: &str) -> Result<Vec<Product>, OxidbError>;
    fn update_stock(&self, product_id: u64, new_quantity: i32) -> Result<(), OxidbError>;
}

trait OrderRepository {
    fn create_order(&self, order: &Order) -> Result<(), OxidbError>;
    fn find_orders_by_user(&self, user_id: u64) -> Result<Vec<Order>, OxidbError>;
    fn update_order_status(&self, order_id: u64, status: &str) -> Result<(), OxidbError>;
}

/// Dependency Inversion Principle - Implementation depends on abstraction
struct DatabaseRepository {
    connection: Arc<Mutex<Connection>>,
}

impl DatabaseRepository {
    fn new() -> Result<Self, OxidbError> {
        let conn = Connection::open_in_memory()?;
        Ok(Self {
            connection: Arc::new(Mutex::new(conn)),
        })
    }

    fn setup_schema(&self) -> Result<(), OxidbError> {
        let mut conn = self.connection.lock().unwrap();
        
        // Users table
        conn.execute("CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            created_at TEXT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE
        )")?;

        // Products table
        conn.execute("CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL NOT NULL CHECK(price >= 0),
            category TEXT NOT NULL,
            stock_quantity INTEGER NOT NULL CHECK(stock_quantity >= 0),
            description TEXT
        )")?;

        // Orders table
        conn.execute("CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            product_ids TEXT NOT NULL,
            total_amount REAL NOT NULL CHECK(total_amount >= 0),
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )")?;

        // Create indexes for performance
        conn.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_products_category ON products(category)")?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id)")?;

        Ok(())
    }
}

impl UserRepository for DatabaseRepository {
    fn create_user(&self, user: &User) -> Result<(), OxidbError> {
        let mut conn = self.connection.lock().unwrap();
        conn.execute(&format!(
            "INSERT INTO users (id, username, email, created_at, is_active) VALUES ({}, '{}', '{}', '{}', {})",
            user.id, user.username, user.email, user.created_at, user.is_active
        ))?;
        Ok(())
    }

    fn find_user_by_email(&self, email: &str) -> Result<Option<User>, OxidbError> {
        let mut conn = self.connection.lock().unwrap();
        let result = conn.query_all(&format!("SELECT * FROM users WHERE email = '{}'", email))?;
        
        if result.is_empty() {
            Ok(None)
        } else {
            let row = &result[0];
            // Assuming columns: id, username, email, created_at, is_active
            Ok(Some(User {
                id: match row.get(0) {
                    Some(oxidb::Value::Integer(i)) => *i as u64,
                    _ => 0,
                },
                username: match row.get(1) {
                    Some(oxidb::Value::Text(s)) => s.clone(),
                    _ => String::new(),
                },
                email: match row.get(2) {
                    Some(oxidb::Value::Text(s)) => s.clone(),
                    _ => String::new(),
                },
                created_at: match row.get(3) {
                    Some(oxidb::Value::Text(s)) => s.clone(),
                    _ => String::new(),
                },
                is_active: match row.get(4) {
                    Some(oxidb::Value::Boolean(b)) => *b,
                    Some(oxidb::Value::Integer(i)) => *i != 0,
                    _ => true,
                },
            }))
        }
    }

    fn update_user_status(&self, user_id: u64, is_active: bool) -> Result<(), OxidbError> {
        let mut conn = self.connection.lock().unwrap();
        conn.execute(&format!(
            "UPDATE users SET is_active = {} WHERE id = {}",
            is_active, user_id
        ))?;
        Ok(())
    }
}

impl ProductRepository for DatabaseRepository {
    fn create_product(&self, product: &Product) -> Result<(), OxidbError> {
        let mut conn = self.connection.lock().unwrap();
        conn.execute(&format!(
            "INSERT INTO products (id, name, price, category, stock_quantity, description) VALUES ({}, '{}', {}, '{}', {}, '{}')",
            product.id, product.name, product.price, product.category, product.stock_quantity, product.description
        ))?;
        Ok(())
    }

    fn find_products_by_category(&self, category: &str) -> Result<Vec<Product>, OxidbError> {
        let mut conn = self.connection.lock().unwrap();
        let result = conn.query_all(&format!("SELECT * FROM products WHERE category = '{}'", category))?;
        
        let mut products = Vec::new();
        for row in result {
            // Assuming columns: id, name, price, category, stock_quantity, description
            products.push(Product {
                id: match row.get(0) {
                    Some(oxidb::Value::Integer(i)) => *i as u64,
                    _ => 0,
                },
                name: match row.get(1) {
                    Some(oxidb::Value::Text(s)) => s.clone(),
                    _ => String::new(),
                },
                price: match row.get(2) {
                    Some(oxidb::Value::Float(f)) => *f,
                    Some(oxidb::Value::Integer(i)) => *i as f64,
                    _ => 0.0,
                },
                category: match row.get(3) {
                    Some(oxidb::Value::Text(s)) => s.clone(),
                    _ => String::new(),
                },
                stock_quantity: match row.get(4) {
                    Some(oxidb::Value::Integer(i)) => *i as i32,
                    _ => 0,
                },
                description: match row.get(5) {
                    Some(oxidb::Value::Text(s)) => s.clone(),
                    _ => String::new(),
                },
            });
        }
        Ok(products)
    }

    fn update_stock(&self, product_id: u64, new_quantity: i32) -> Result<(), OxidbError> {
        let mut conn = self.connection.lock().unwrap();
        conn.execute(&format!(
            "UPDATE products SET stock_quantity = {} WHERE id = {}",
            new_quantity, product_id
        ))?;
        Ok(())
    }
}

impl OrderRepository for DatabaseRepository {
    fn create_order(&self, order: &Order) -> Result<(), OxidbError> {
        let mut conn = self.connection.lock().unwrap();
        let product_ids_json = serde_json::to_string(&order.product_ids).unwrap_or_default();
        conn.execute(&format!(
            "INSERT INTO orders (id, user_id, product_ids, total_amount, status, created_at) VALUES ({}, {}, '{}', {}, '{}', '{}')",
            order.id, order.user_id, product_ids_json, order.total_amount, order.status, order.created_at
        ))?;
        Ok(())
    }

    fn find_orders_by_user(&self, user_id: u64) -> Result<Vec<Order>, OxidbError> {
        let mut conn = self.connection.lock().unwrap();
        let result = conn.query_all(&format!("SELECT * FROM orders WHERE user_id = {}", user_id))?;
        
        let mut orders = Vec::new();
        for row in result {
            // Assuming columns: id, user_id, product_ids, total_amount, status, created_at
            let product_ids_str = match row.get(2) {
                Some(oxidb::Value::Text(s)) => s.clone(),
                _ => "[]".to_string(),
            };
            let product_ids: Vec<u64> = serde_json::from_str(&product_ids_str).unwrap_or_default();
            
            orders.push(Order {
                id: match row.get(0) {
                    Some(oxidb::Value::Integer(i)) => *i as u64,
                    _ => 0,
                },
                user_id: match row.get(1) {
                    Some(oxidb::Value::Integer(i)) => *i as u64,
                    _ => 0,
                },
                product_ids,
                total_amount: match row.get(3) {
                    Some(oxidb::Value::Float(f)) => *f,
                    Some(oxidb::Value::Integer(i)) => *i as f64,
                    _ => 0.0,
                },
                status: match row.get(4) {
                    Some(oxidb::Value::Text(s)) => s.clone(),
                    _ => String::new(),
                },
                created_at: match row.get(5) {
                    Some(oxidb::Value::Text(s)) => s.clone(),
                    _ => String::new(),
                },
            });
        }
        Ok(orders)
    }

    fn update_order_status(&self, order_id: u64, status: &str) -> Result<(), OxidbError> {
        let mut conn = self.connection.lock().unwrap();
        conn.execute(&format!(
            "UPDATE orders SET status = '{}' WHERE id = {}",
            status, order_id
        ))?;
        Ok(())
    }
}

/// E-commerce Service Layer - Demonstrates GRASP principles
struct ECommerceService {
    user_repo: Arc<dyn UserRepository + Send + Sync>,
    product_repo: Arc<dyn ProductRepository + Send + Sync>,
    order_repo: Arc<dyn OrderRepository + Send + Sync>,
}

impl ECommerceService {
    fn new(repo: Arc<DatabaseRepository>) -> Self {
        Self {
            user_repo: repo.clone(),
            product_repo: repo.clone(),
            order_repo: repo,
        }
    }

    /// Complex business logic combining multiple operations
    fn process_order(&self, user_email: &str, product_ids: Vec<u64>) -> Result<u64, OxidbError> {
        // Validate user exists and is active
        let user = self.user_repo.find_user_by_email(user_email)?
            .ok_or_else(|| OxidbError::InvalidInput { message: "User not found".to_string() })?;
        
        if !user.is_active {
            return Err(OxidbError::InvalidInput { message: "User account is inactive".to_string() });
        }

        // Calculate total amount and validate stock
        let mut total_amount = 0.0;
        for &product_id in &product_ids {
            // In a real scenario, we'd fetch each product and check stock
            total_amount += 29.99; // Simplified for demo
        }

        // Create order
        let order = Order {
            id: (user.id * 1000) + product_ids.len() as u64, // Simple ID generation
            user_id: user.id,
            product_ids,
            total_amount,
            status: "pending".to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        self.order_repo.create_order(&order)?;
        Ok(order.id)
    }

    fn get_user_order_history(&self, user_email: &str) -> Result<Vec<Order>, OxidbError> {
        let user = self.user_repo.find_user_by_email(user_email)?
            .ok_or_else(|| OxidbError::InvalidInput { message: "User not found".to_string() })?;
        
        self.order_repo.find_orders_by_user(user.id)
    }
}

fn main() -> Result<(), OxidbError> {
    println!("=== Oxidb Real-World Scenarios ===\n");

    // Test E-commerce Platform Scenario
    run_ecommerce_scenario()?;
    
    // Test User Management Scenario
    run_user_management_scenario()?;
    
    // Test Inventory Management Scenario
    run_inventory_management_scenario()?;
    
    // Test Analytics Scenario
    run_analytics_scenario()?;

    println!("\n=== All Real-World Scenarios Completed Successfully ===");
    Ok(())
}

fn run_ecommerce_scenario() -> Result<(), OxidbError> {
    println!("🛒 Testing E-commerce Platform Scenario...");
    
    let repo = Arc::new(DatabaseRepository::new()?);
    repo.setup_schema()?;
    
    let service = ECommerceService::new(repo.clone());

    // Create test users
    let users = vec![
        User {
            id: 1,
            username: "alice_buyer".to_string(),
            email: "alice@example.com".to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            is_active: true,
        },
        User {
            id: 2,
            username: "bob_customer".to_string(),
            email: "bob@example.com".to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            is_active: false, // Inactive user for testing
        },
    ];

    for user in &users {
        service.user_repo.create_user(user)?;
    }

    // Create test products
    let products = vec![
        Product {
            id: 1,
            name: "Laptop Pro".to_string(),
            price: 1299.99,
            category: "Electronics".to_string(),
            stock_quantity: 50,
            description: "High-performance laptop".to_string(),
        },
        Product {
            id: 2,
            name: "Wireless Mouse".to_string(),
            price: 29.99,
            category: "Electronics".to_string(),
            stock_quantity: 100,
            description: "Ergonomic wireless mouse".to_string(),
        },
    ];

    for product in &products {
        service.product_repo.create_product(product)?;
    }

    // Test successful order processing
    let order_id = service.process_order("alice@example.com", vec![1, 2])?;
    println!("✅ Successfully created order: {}", order_id);

    // Test order processing with inactive user (should fail)
    match service.process_order("bob@example.com", vec![1]) {
        Err(OxidbError::InvalidInput { message }) if message.contains("inactive") => {
            println!("✅ Correctly rejected order for inactive user");
        }
        _ => println!("❌ Should have rejected order for inactive user"),
    }

    // Test order history retrieval
    let orders = service.get_user_order_history("alice@example.com")?;
    println!("✅ Retrieved {} orders for user", orders.len());

    println!("✅ E-commerce scenario completed\n");
    Ok(())
}

fn run_user_management_scenario() -> Result<(), OxidbError> {
    println!("👥 Testing User Management Scenario...");
    
    let repo = Arc::new(DatabaseRepository::new()?);
    repo.setup_schema()?;

    // Test user creation with edge cases
    let test_users = vec![
        ("normal_user@test.com", "normaluser", true),
        ("special+chars@test-domain.co.uk", "user_with_special", true),
        ("", "empty_email", false), // Should fail
    ];

    for (email, username, should_succeed) in test_users {
        let user = User {
            id: rand::random::<u64>() % 10000,
            username: username.to_string(),
            email: email.to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            is_active: true,
        };

        match repo.create_user(&user) {
            Ok(_) if should_succeed => println!("✅ Successfully created user: {}", username),
            Err(_) if !should_succeed => println!("✅ Correctly rejected invalid user: {}", username),
            Ok(_) => println!("❌ Should have rejected invalid user: {}", username),
            Err(e) => println!("❌ Unexpected error for user {}: {:?}", username, e),
        }
    }

    println!("✅ User management scenario completed\n");
    Ok(())
}

fn run_inventory_management_scenario() -> Result<(), OxidbError> {
    println!("📦 Testing Inventory Management Scenario...");
    
    let repo = Arc::new(DatabaseRepository::new()?);
    repo.setup_schema()?;

    // Create products with various stock levels
    let products = vec![
        Product {
            id: 100,
            name: "High Stock Item".to_string(),
            price: 19.99,
            category: "Test".to_string(),
            stock_quantity: 1000,
            description: "Item with high stock".to_string(),
        },
        Product {
            id: 101,
            name: "Low Stock Item".to_string(),
            price: 99.99,
            category: "Test".to_string(),
            stock_quantity: 2,
            description: "Item with low stock".to_string(),
        },
        Product {
            id: 102,
            name: "Out of Stock Item".to_string(),
            price: 49.99,
            category: "Test".to_string(),
            stock_quantity: 0,
            description: "Out of stock item".to_string(),
        },
    ];

    for product in &products {
        repo.create_product(product)?;
    }

    // Test stock updates
    repo.update_stock(100, 950)?; // Reduce high stock
    repo.update_stock(101, 0)?;   // Deplete low stock
    
    // Test category-based queries
    let test_products = repo.find_products_by_category("Test")?;
    println!("✅ Found {} products in Test category", test_products.len());

    println!("✅ Inventory management scenario completed\n");
    Ok(())
}

fn run_analytics_scenario() -> Result<(), OxidbError> {
    println!("📊 Testing Analytics Scenario...");
    
    let repo = Arc::new(DatabaseRepository::new()?);
    repo.setup_schema()?;

    // Create sample data for analytics
    let start_time = Instant::now();
    
    // Bulk insert users
    for i in 1..=100 {
        let user = User {
            id: i,
            username: format!("user_{}", i),
            email: format!("user{}@analytics.test", i),
            created_at: chrono::Utc::now().to_rfc3339(),
            is_active: i % 10 != 0, // 10% inactive users
        };
        repo.create_user(&user)?;
    }

    // Bulk insert products
    let categories = ["Electronics", "Books", "Clothing", "Home", "Sports"];
    for i in 1..=50 {
        let product = Product {
            id: i,
            name: format!("Product {}", i),
            price: (i as f64) * 10.0 + 9.99,
            category: categories[(i as usize - 1) % categories.len()].to_string(),
            stock_quantity: ((i * 10) % 100) as i32,
            description: format!("Description for product {}", i),
        };
        repo.create_product(&product)?;
    }

    let elapsed = start_time.elapsed();
    println!("✅ Bulk inserted 150 records in {:?}", elapsed);

    // Test category-based analytics
    for category in &categories {
        let products = repo.find_products_by_category(category)?;
        println!("📈 {} category has {} products", category, products.len());
    }

    println!("✅ Analytics scenario completed\n");
    Ok(())
}

// Helper trait for generating test data (YAGNI - only what we need)
trait TestDataGenerator {
    fn generate_test_email(id: u64) -> String {
        format!("test_user_{}@oxidb.test", id)
    }
    
    fn generate_test_username(id: u64) -> String {
        format!("testuser_{}", id)
    }
}

impl TestDataGenerator for DatabaseRepository {}

// Add chrono dependency for timestamps
use chrono;
use rand;