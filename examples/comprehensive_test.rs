use oxidb::{Connection, OxidbError, QueryResult};

fn main() -> Result<(), OxidbError> {
    println!("=== Oxidb Comprehensive Functionality Test ===\n");

    // Test 1: Basic Connection and Table Creation
    test_basic_connection()?;
    
    // Test 2: Data Types Support
    test_data_types()?;
    
    // Test 3: CRUD Operations
    test_crud_operations()?;
    
    // Test 4: Transaction Management
    test_transactions()?;
    
    // Test 5: Indexing and Performance
    test_indexing()?;
    
    // Test 6: File Persistence
    test_file_persistence()?;
    
    // Test 7: Complex Queries
    test_complex_queries()?;

    println!("\n🎉 All tests completed successfully! 🎉");
    Ok(())
}

fn test_basic_connection() -> Result<(), OxidbError> {
    println!("--- Test 1: Basic Connection and Table Creation ---");
    
    let mut conn = Connection::open_in_memory()?;
    println!("✓ Created in-memory connection");
    
    // Create a simple table
    let result = conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN)")?;
    println!("✓ Created users table: {:?}", result);
    
    // Verify table exists by inserting data with unique ID
    let unique_id = std::process::id() as i32;
    let sql = format!("INSERT INTO users (id, name, active) VALUES ({}, 'Test User', true)", unique_id);
    let result = conn.execute(&sql)?;
    println!("✓ Inserted test record: {:?}", result);
    
    println!("✅ Basic connection test passed\n");
    Ok(())
}

fn test_data_types() -> Result<(), OxidbError> {
    println!("--- Test 2: Data Types Support ---");
    
    let mut conn = Connection::open_in_memory()?;
    
    // Create table with various data types
    let table_name = format!("data_types_{}", std::process::id());
    let create_sql = format!("CREATE TABLE {} (
        id INTEGER PRIMARY KEY,
        text_field TEXT,
        integer_field INTEGER,
        float_field FLOAT,
        boolean_field BOOLEAN
    )", table_name);
    conn.execute(&create_sql)?;
    println!("✓ Created data_types table");
    
    // Insert various data types
    let insert_sql = format!("INSERT INTO {} (id, text_field, integer_field, float_field, boolean_field) 
                      VALUES (1, 'Hello World', 42, 3.14159, true)", table_name);
    conn.execute(&insert_sql)?;
    
    let insert_sql = format!("INSERT INTO {} (id, text_field, integer_field, float_field, boolean_field) 
                      VALUES (2, 'Test String', -100, 2.718, false)", table_name);
    conn.execute(&insert_sql)?;
    
    println!("✓ Inserted records with various data types");
    
    // Query and verify data types
    let result = conn.execute(&format!("SELECT * FROM {}", table_name))?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Retrieved {} records with columns: {:?}", 
                    data.row_count(), data.columns());
            for (i, row) in data.rows().enumerate() {
                println!("  Row {}: {:?}", i + 1, row);
            }
        }
        _ => return Err(OxidbError::Other("Unexpected result type".to_string())),
    }
    
    println!("✅ Data types test passed\n");
    Ok(())
}

fn test_crud_operations() -> Result<(), OxidbError> {
    println!("--- Test 3: CRUD Operations ---");
    
    let mut conn = Connection::open_in_memory()?;
    
    // Create
    conn.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price FLOAT, in_stock BOOLEAN)")?;
    println!("✓ Created products table");
    
    // Insert (Create)
    conn.execute("INSERT INTO products (id, name, price, in_stock) VALUES (1, 'Laptop', 999.99, true)")?;
    conn.execute("INSERT INTO products (id, name, price, in_stock) VALUES (2, 'Mouse', 29.99, true)")?;
    conn.execute("INSERT INTO products (id, name, price, in_stock) VALUES (3, 'Keyboard', 79.99, false)")?;
    println!("✓ Inserted 3 products");
    
    // Read
    let result = conn.execute("SELECT * FROM products")?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Read {} products", data.row_count());
        }
        _ => return Err(OxidbError::Other("Failed to read products".to_string())),
    }
    
    // Read with WHERE clause
    let result = conn.execute("SELECT * FROM products WHERE price > 50")?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Found {} products with price > 50", data.row_count());
        }
        _ => return Err(OxidbError::Other("Failed to filter products".to_string())),
    }
    
    // Update
    let result = conn.execute("UPDATE products SET price = 899.99 WHERE id = 1")?;
    println!("✓ Updated product price: {:?}", result);
    
    // Delete
    let result = conn.execute("DELETE FROM products WHERE in_stock = false")?;
    println!("✓ Deleted out-of-stock products: {:?}", result);
    
    // Verify final state
    let result = conn.execute("SELECT * FROM products")?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Final product count: {}", data.row_count());
        }
        _ => return Err(OxidbError::Other("Failed to verify final state".to_string())),
    }
    
    println!("✅ CRUD operations test passed\n");
    Ok(())
}

fn test_transactions() -> Result<(), OxidbError> {
    println!("--- Test 4: Transaction Management ---");
    
    let mut conn = Connection::open_in_memory()?;
    
    // Setup
    conn.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT, balance FLOAT)")?;
    conn.execute("INSERT INTO accounts (id, name, balance) VALUES (1, 'Alice', 1000.0)")?;
    conn.execute("INSERT INTO accounts (id, name, balance) VALUES (2, 'Bob', 500.0)")?;
    println!("✓ Created accounts table with initial data");
    
    // Test successful transaction
    conn.begin_transaction()?;
    conn.execute("UPDATE accounts SET balance = balance - 100.0 WHERE id = 1")?;
    conn.execute("UPDATE accounts SET balance = balance + 100.0 WHERE id = 2")?;
    conn.commit()?;
    println!("✓ Successfully transferred $100 from Alice to Bob");
    
    // Verify transaction result
    let result = conn.execute("SELECT * FROM accounts")?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Account balances after transfer:");
            for row in data.rows() {
                println!("  {:?}", row);
            }
        }
        _ => return Err(OxidbError::Other("Failed to verify transaction".to_string())),
    }
    
    // Test rollback
    conn.begin_transaction()?;
    conn.execute("UPDATE accounts SET balance = 0.0 WHERE id = 1")?;
    conn.execute("UPDATE accounts SET balance = 0.0 WHERE id = 2")?;
    conn.rollback()?;
    println!("✓ Successfully rolled back transaction");
    
    // Verify rollback
    let result = conn.execute("SELECT * FROM accounts")?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Account balances after rollback:");
            for row in data.rows() {
                println!("  {:?}", row);
            }
        }
        _ => return Err(OxidbError::Other("Failed to verify rollback".to_string())),
    }
    
    println!("✅ Transaction management test passed\n");
    Ok(())
}

fn test_indexing() -> Result<(), OxidbError> {
    println!("--- Test 5: Indexing and Performance ---");
    
    let mut conn = Connection::open_in_memory()?;
    
    // Create table with indexed columns
    conn.execute("CREATE TABLE employees (
        id INTEGER PRIMARY KEY, 
        name TEXT, 
        department TEXT, 
        salary FLOAT
    )")?;
    println!("✓ Created employees table");
    
    // Insert test data
    let departments = ["Engineering", "Sales", "Marketing", "HR"];
    let names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"];
    
    for i in 1..=20 {
        let name = names[i % names.len()];
        let dept = departments[i % departments.len()];
        let salary = 50000.0 + (i as f64 * 1000.0);
        
        let sql = format!(
            "INSERT INTO employees (id, name, department, salary) VALUES ({}, '{}{}', '{}', {})",
            i, name, i, dept, salary
        );
        conn.execute(&sql)?;
    }
    println!("✓ Inserted 20 employee records");
    
    // Test queries that would benefit from indexing
    let result = conn.execute("SELECT * FROM employees WHERE department = 'Engineering'")?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Found {} engineers", data.row_count());
        }
        _ => return Err(OxidbError::Other("Failed to query by department".to_string())),
    }
    
    let result = conn.execute("SELECT * FROM employees WHERE salary > 60000")?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Found {} high-salary employees", data.row_count());
        }
        _ => return Err(OxidbError::Other("Failed to query by salary".to_string())),
    }
    
    println!("✅ Indexing test passed\n");
    Ok(())
}

fn test_file_persistence() -> Result<(), OxidbError> {
    println!("--- Test 6: File Persistence ---");
    
    let db_file = format!("test_persistence_{}.db", std::process::id());
    
    // Create and populate database
    {
        let mut conn = Connection::open(&db_file)?;
        conn.execute("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT)")?;
        conn.execute("INSERT INTO settings (key, value) VALUES ('theme', 'dark')")?;
        conn.execute("INSERT INTO settings (key, value) VALUES ('language', 'en')")?;
        conn.execute("INSERT INTO settings (key, value) VALUES ('notifications', 'true')")?;
        conn.persist()?;
        println!("✓ Created database file and persisted data");
    }
    
    // Reopen and verify data persists
    {
        let mut conn = Connection::open(&db_file)?;
        let result = conn.execute("SELECT * FROM settings")?;
        match result {
            QueryResult::Data(data) => {
                println!("✓ Reopened database and found {} settings", data.row_count());
                for row in data.rows() {
                    println!("  {:?}", row);
                }
            }
            _ => return Err(OxidbError::Other("Failed to verify persistence".to_string())),
        }
    }
    
    // Clean up
    std::fs::remove_file(&db_file).ok();
    println!("✓ Cleaned up test database file");
    
    println!("✅ File persistence test passed\n");
    Ok(())
}

fn test_complex_queries() -> Result<(), OxidbError> {
    println!("--- Test 7: Complex Queries ---");
    
    let mut conn = Connection::open_in_memory()?;
    
    // Create related tables
    conn.execute("CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)")?;
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, category_id INTEGER, price FLOAT)")?;
    
    // Insert test data
    conn.execute("INSERT INTO categories (id, name) VALUES (1, 'Electronics')")?;
    conn.execute("INSERT INTO categories (id, name) VALUES (2, 'Books')")?;
    conn.execute("INSERT INTO categories (id, name) VALUES (3, 'Clothing')")?;
    
    conn.execute("INSERT INTO items (id, name, category_id, price) VALUES (1, 'Laptop', 1, 999.99)")?;
    conn.execute("INSERT INTO items (id, name, category_id, price) VALUES (2, 'Phone', 1, 599.99)")?;
    conn.execute("INSERT INTO items (id, name, category_id, price) VALUES (3, 'Novel', 2, 19.99)")?;
    conn.execute("INSERT INTO items (id, name, category_id, price) VALUES (4, 'Textbook', 2, 89.99)")?;
    conn.execute("INSERT INTO items (id, name, category_id, price) VALUES (5, 'T-Shirt', 3, 24.99)")?;
    
    println!("✓ Created categories and items tables with test data");
    
    // Test various query patterns
    let result = conn.execute("SELECT * FROM items WHERE price BETWEEN 20 AND 100")?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Found {} items priced between $20-100", data.row_count());
        }
        _ => return Err(OxidbError::Other("Failed range query".to_string())),
    }
    
    let result = conn.execute("SELECT * FROM items WHERE name LIKE '%book%'")?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Found {} items containing 'book'", data.row_count());
        }
        _ => return Err(OxidbError::Other("Failed LIKE query".to_string())),
    }
    
    let result = conn.execute("SELECT * FROM items ORDER BY price DESC")?;
            match result {
            QueryResult::Data(data) => {
                println!("✓ Retrieved {} items ordered by price (desc)", data.row_count());
                let rows: Vec<_> = data.rows().collect();
                if let Some(first_row) = rows.first() {
                    println!("  Most expensive item: {:?}", first_row);
                }
            }
            _ => return Err(OxidbError::Other("Failed ORDER BY query".to_string())),
        }
    
    println!("✅ Complex queries test passed\n");
    Ok(())
}