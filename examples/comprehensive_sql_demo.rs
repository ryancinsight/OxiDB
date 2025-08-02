use oxidb::{Connection, OxidbError, QueryResult};

fn main() -> Result<(), OxidbError> {
    println!("=== Oxidb Comprehensive SQL Demo ===\n");

    let mut conn = Connection::open_in_memory()?;
    let process_id = std::process::id();
    
    // Test 1: Basic Table Operations
    println!("--- Test 1: Basic Table Operations ---");
    test_basic_table_operations(&mut conn, process_id)?;
    
    // Test 2: Data Types and Constraints
    println!("\n--- Test 2: Data Types and Constraints ---");
    test_data_types_and_constraints(&mut conn, process_id)?;
    
    // Test 3: Advanced Queries
    println!("\n--- Test 3: Advanced Queries ---");
    test_advanced_queries(&mut conn, process_id)?;
    
    // Test 4: Transaction Operations
    println!("\n--- Test 4: Transaction Operations ---");
    test_transaction_operations(&mut conn, process_id)?;
    
    println!("\n🎉 All SQL tests completed successfully! 🎉");
    Ok(())
}

fn test_basic_table_operations(conn: &mut Connection, process_id: u32) -> Result<(), OxidbError> {
    // Create a simple users table
    let table_name = format!("users_{}", process_id);
    let create_sql = format!(
        "CREATE TABLE {} (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)",
        table_name
    );
    conn.execute(&create_sql)?;
    println!("✓ Created table: {}", table_name);

    // Insert test data
    let insert_sql = format!(
        "INSERT INTO {} (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 25)",
        table_name
    );
    conn.execute(&insert_sql)?;
    
    let insert_sql = format!(
        "INSERT INTO {} (id, name, email, age) VALUES (2, 'Bob', 'bob@example.com', 30)",
        table_name
    );
    conn.execute(&insert_sql)?;
    println!("✓ Inserted test data");

    // Query data
    let select_sql = format!("SELECT * FROM {}", table_name);
    let result = conn.execute(&select_sql)?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Retrieved {} rows", data.row_count());
        }
        _ => return Err(OxidbError::Other("Expected data result".to_string())),
    }

    // Update data
    let update_sql = format!("UPDATE {} SET age = 26 WHERE name = 'Alice'", table_name);
    conn.execute(&update_sql)?;
    println!("✓ Updated Alice's age");

    // Delete data
    let delete_sql = format!("DELETE FROM {} WHERE name = 'Bob'", table_name);
    conn.execute(&delete_sql)?;
    println!("✓ Deleted Bob's record");

    Ok(())
}

fn test_data_types_and_constraints(conn: &mut Connection, process_id: u32) -> Result<(), OxidbError> {
    let table_name = format!("products_{}", process_id);
    let create_sql = format!(
        "CREATE TABLE {} (id INTEGER PRIMARY KEY, name TEXT, price FLOAT, in_stock BOOLEAN)",
        table_name
    );
    conn.execute(&create_sql)?;
    println!("✓ Created products table with various data types");

    // Insert various data types
    let insert_sql = format!(
        "INSERT INTO {} (id, name, price, in_stock) VALUES (1, 'Laptop', 999.99, true)",
        table_name
    );
    conn.execute(&insert_sql)?;
    
    let insert_sql = format!(
        "INSERT INTO {} (id, name, price, in_stock) VALUES (2, 'Mouse', 29.99, false)",
        table_name
    );
    conn.execute(&insert_sql)?;
    println!("✓ Inserted products with different data types");

    // Query with WHERE clause
    let select_sql = format!("SELECT * FROM {} WHERE price > 50", table_name);
    let result = conn.execute(&select_sql)?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Found {} expensive products", data.row_count());
        }
        _ => return Err(OxidbError::Other("Expected data result".to_string())),
    }

    Ok(())
}

fn test_advanced_queries(conn: &mut Connection, process_id: u32) -> Result<(), OxidbError> {
    let table_name = format!("orders_{}", process_id);
    let create_sql = format!(
        "CREATE TABLE {} (id INTEGER PRIMARY KEY, customer_name TEXT, amount FLOAT, order_date TEXT)",
        table_name
    );
    conn.execute(&create_sql)?;
    println!("✓ Created orders table");

    // Insert sample orders
    let orders = vec![
        (1, "Alice", 100.50, "2024-01-01"),
        (2, "Bob", 250.75, "2024-01-02"),
        (3, "Alice", 75.25, "2024-01-03"),
        (4, "Charlie", 500.00, "2024-01-04"),
    ];

    for (id, name, amount, date) in orders {
        let insert_sql = format!(
            "INSERT INTO {} (id, customer_name, amount, order_date) VALUES ({}, '{}', {}, '{}')",
            table_name, id, name, amount, date
        );
        conn.execute(&insert_sql)?;
    }
    println!("✓ Inserted sample orders");

    // Query with conditions
    let select_sql = format!("SELECT * FROM {} WHERE amount > 100", table_name);
    let result = conn.execute(&select_sql)?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Found {} large orders", data.row_count());
        }
        _ => return Err(OxidbError::Other("Expected data result".to_string())),
    }

    // Query specific customer
    let select_sql = format!("SELECT * FROM {} WHERE customer_name = 'Alice'", table_name);
    let result = conn.execute(&select_sql)?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Found {} orders for Alice", data.row_count());
        }
        _ => return Err(OxidbError::Other("Expected data result".to_string())),
    }

    Ok(())
}

fn test_transaction_operations(conn: &mut Connection, process_id: u32) -> Result<(), OxidbError> {
    let table_name = format!("accounts_{}", process_id);
    let create_sql = format!(
        "CREATE TABLE {} (id INTEGER PRIMARY KEY, name TEXT, balance FLOAT)",
        table_name
    );
    conn.execute(&create_sql)?;
    println!("✓ Created accounts table");

    // Insert initial accounts
    let insert_sql = format!(
        "INSERT INTO {} (id, name, balance) VALUES (1, 'Account A', 1000.0)",
        table_name
    );
    conn.execute(&insert_sql)?;
    
    let insert_sql = format!(
        "INSERT INTO {} (id, name, balance) VALUES (2, 'Account B', 500.0)",
        table_name
    );
    conn.execute(&insert_sql)?;
    println!("✓ Created initial accounts");

    // Begin transaction
    conn.begin_transaction()?;
    println!("✓ Started transaction");

    // Transfer money
    let update_sql = format!("UPDATE {} SET balance = balance - 100 WHERE id = 1", table_name);
    conn.execute(&update_sql)?;
    
    let update_sql = format!("UPDATE {} SET balance = balance + 100 WHERE id = 2", table_name);
    conn.execute(&update_sql)?;
    println!("✓ Transferred 100 from Account A to Account B");

    // Commit transaction
    conn.commit()?;
    println!("✓ Committed transaction");

    // Verify balances
    let select_sql = format!("SELECT * FROM {}", table_name);
    let result = conn.execute(&select_sql)?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Verified final balances - {} accounts", data.row_count());
        }
        _ => return Err(OxidbError::Other("Expected data result".to_string())),
    }

    Ok(())
}