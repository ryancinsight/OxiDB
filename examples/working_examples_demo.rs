use oxidb::{Connection, OxidbError, QueryResult};
use std::time::Instant;

fn main() -> Result<(), OxidbError> {
    println!("=== Oxidb Working Examples Demo ===\n");

    // Test 1: Basic CRUD Operations
    println!("--- Test 1: Basic CRUD Operations ---");
    test_basic_crud()?;
    
    // Test 2: Data Types Support
    println!("\n--- Test 2: Data Types Support ---");
    test_data_types()?;
    
    // Test 3: Transaction Management
    println!("\n--- Test 3: Transaction Management ---");
    test_transactions()?;
    
    // Test 4: Query Operations
    println!("\n--- Test 4: Query Operations ---");
    test_queries()?;
    
    // Test 5: Performance Test
    println!("\n--- Test 5: Performance Test ---");
    test_performance()?;
    
    // Test 6: File Persistence
    println!("\n--- Test 6: File Persistence ---");
    test_file_persistence()?;

    println!("\n🎉 All working examples completed successfully! 🎉");
    Ok(())
}

fn test_basic_crud() -> Result<(), OxidbError> {
    let mut conn = Connection::open_in_memory()?;
    
    // Create table
    let create_sql = "CREATE TABLE demo_users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
    conn.execute(create_sql)?;
    println!("✓ Created demo_users table");
    
    // Insert data
    conn.execute("INSERT INTO demo_users (id, name, age) VALUES (1, 'Alice', 30)")?;
    conn.execute("INSERT INTO demo_users (id, name, age) VALUES (2, 'Bob', 25)")?;
    conn.execute("INSERT INTO demo_users (id, name, age) VALUES (3, 'Charlie', 35)")?;
    println!("✓ Inserted 3 users");
    
    // Select data
    let result = conn.execute("SELECT * FROM demo_users")?;
    if let QueryResult::Data(data) = result {
        println!("✓ Retrieved {} users from database", data.row_count());
        for (i, row) in data.rows().enumerate() {
            println!("  User {}: {:?}", i + 1, row);
        }
    }
    
    // Update data
    conn.execute("UPDATE demo_users SET age = 31 WHERE name = 'Alice'")?;
    println!("✓ Updated Alice's age");
    
    // Delete data
    conn.execute("DELETE FROM demo_users WHERE id = 3")?;
    println!("✓ Deleted Charlie");
    
    // Verify final state
    let result = conn.execute("SELECT COUNT(*) FROM demo_users")?;
    if let QueryResult::Data(data) = result {
        let rows: Vec<_> = data.rows().collect();
        if let Some(row) = rows.first() {
            println!("✓ Final user count: {:?}", row);
        }
    }
    
    Ok(())
}

fn test_data_types() -> Result<(), OxidbError> {
    let mut conn = Connection::open_in_memory()?;
    
    // Create table with various data types
    let create_sql = "CREATE TABLE data_demo (
        id INTEGER PRIMARY KEY,
        text_field TEXT,
        integer_field INTEGER,
        float_field FLOAT,
        boolean_field BOOLEAN
    )";
    conn.execute(create_sql)?;
    println!("✓ Created data_demo table with various types");
    
    // Insert data with different types
    conn.execute("INSERT INTO data_demo VALUES (1, 'Hello World', 42, 3.14159, true)")?;
    conn.execute("INSERT INTO data_demo VALUES (2, 'Test String', -100, 2.718, false)")?;
    conn.execute("INSERT INTO data_demo VALUES (3, NULL, 0, 0.0, true)")?;
    println!("✓ Inserted data with various types including NULL");
    
    // Query and display
    let result = conn.execute("SELECT * FROM data_demo")?;
    if let QueryResult::Data(data) = result {
        println!("✓ Retrieved {} records with mixed data types", data.row_count());
        for (i, row) in data.rows().enumerate() {
            println!("  Record {}: {:?}", i + 1, row);
        }
    }
    
    Ok(())
}

fn test_transactions() -> Result<(), OxidbError> {
    let mut conn = Connection::open_in_memory()?;
    
    // Create table
    conn.execute("CREATE TABLE tx_demo (id INTEGER PRIMARY KEY, balance FLOAT)")?;
    conn.execute("INSERT INTO tx_demo VALUES (1, 1000.0)")?;
    conn.execute("INSERT INTO tx_demo VALUES (2, 500.0)")?;
    println!("✓ Created accounts with initial balances");
    
    // Test successful transaction
    conn.begin_transaction()?;
    conn.execute("UPDATE tx_demo SET balance = balance - 100 WHERE id = 1")?;
    conn.execute("UPDATE tx_demo SET balance = balance + 100 WHERE id = 2")?;
    conn.commit()?;
    println!("✓ Successfully transferred 100 between accounts");
    
    // Verify balances
    let result = conn.execute("SELECT id, balance FROM tx_demo ORDER BY id")?;
    if let QueryResult::Data(data) = result {
        for row in data.rows() {
            println!("  Account balance: {:?}", row);
        }
    }
    
    // Test rollback
    conn.begin_transaction()?;
    conn.execute("UPDATE tx_demo SET balance = balance - 1000 WHERE id = 1")?;
    conn.rollback()?;
    println!("✓ Successfully rolled back large withdrawal");
    
    // Verify balances unchanged
    let result = conn.execute("SELECT id, balance FROM tx_demo ORDER BY id")?;
    if let QueryResult::Data(data) = result {
        println!("✓ Balances after rollback:");
        for row in data.rows() {
            println!("  Account balance: {:?}", row);
        }
    }
    
    Ok(())
}

fn test_queries() -> Result<(), OxidbError> {
    let mut conn = Connection::open_in_memory()?;
    
    // Create and populate table
    conn.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price FLOAT, category TEXT)")?;
    conn.execute("INSERT INTO products VALUES (1, 'Laptop', 999.99, 'Electronics')")?;
    conn.execute("INSERT INTO products VALUES (2, 'Mouse', 29.99, 'Electronics')")?;
    conn.execute("INSERT INTO products VALUES (3, 'Book', 19.99, 'Education')")?;
    conn.execute("INSERT INTO products VALUES (4, 'Pen', 2.99, 'Office')")?;
    println!("✓ Created products table with sample data");
    
    // Test WHERE clause
    let result = conn.execute("SELECT * FROM products WHERE price > 20")?;
    if let QueryResult::Data(data) = result {
        println!("✓ Found {} products over $20", data.row_count());
    }
    
    // Test ORDER BY
    let result = conn.execute("SELECT name, price FROM products ORDER BY price DESC")?;
    if let QueryResult::Data(data) = result {
        println!("✓ Products ordered by price (descending):");
        for row in data.rows() {
            println!("  {:?}", row);
        }
    }
    
    // Test aggregation
    let result = conn.execute("SELECT COUNT(*) as total_products FROM products")?;
    if let QueryResult::Data(data) = result {
        let rows: Vec<_> = data.rows().collect();
        if let Some(row) = rows.first() {
            println!("✓ Total products: {:?}", row);
        }
    }
    
    Ok(())
}

fn test_performance() -> Result<(), OxidbError> {
    let mut conn = Connection::open_in_memory()?;
    
    // Create table for performance test
    conn.execute("CREATE TABLE perf_test (id INTEGER PRIMARY KEY, data TEXT, value FLOAT)")?;
    println!("✓ Created performance test table");
    
    // Insert many records
    let start = Instant::now();
    conn.begin_transaction()?;
    
    for i in 1..=1000 {
        let sql = format!("INSERT INTO perf_test VALUES ({}, 'Record{}', {})", i, i, i as f64 * 1.5);
        conn.execute(&sql)?;
    }
    
    conn.commit()?;
    let duration = start.elapsed();
    println!("✓ Inserted 1000 records in {:?} ({:.2} records/sec)", 
             duration, 1000.0 / duration.as_secs_f64());
    
    // Query performance test
    let start = Instant::now();
    let result = conn.execute("SELECT COUNT(*) FROM perf_test WHERE value > 500")?;
    let duration = start.elapsed();
    
    if let QueryResult::Data(data) = result {
        let rows: Vec<_> = data.rows().collect();
        if let Some(row) = rows.first() {
            println!("✓ Query completed in {:?}, result: {:?}", duration, row);
        }
    }
    
    Ok(())
}

fn test_file_persistence() -> Result<(), OxidbError> {
    let db_path = format!("test_persistence_{}.db", std::process::id());
    
    // Create file-based database and add data
    {
        let mut conn = Connection::open(&db_path)?;
        conn.execute("CREATE TABLE persistent_data (id INTEGER PRIMARY KEY, message TEXT)")?;
        conn.execute("INSERT INTO persistent_data VALUES (1, 'Hello from file!')")?;
        conn.execute("INSERT INTO persistent_data VALUES (2, 'This data persists')")?;
        println!("✓ Created file database and inserted data");
    } // Connection closes here, data should be saved
    
    // Reopen database and verify data persists
    {
        let mut conn = Connection::open(&db_path)?;
        let result = conn.execute("SELECT * FROM persistent_data")?;
        if let QueryResult::Data(data) = result {
            println!("✓ Reopened database, found {} persistent records", data.row_count());
            for row in data.rows() {
                println!("  Persistent data: {:?}", row);
            }
        }
    }
    
    // Clean up test file
    if std::path::Path::new(&db_path).exists() {
        std::fs::remove_file(&db_path).ok();
        println!("✓ Cleaned up test database file");
    }
    
    Ok(())
}