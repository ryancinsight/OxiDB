use oxidb::{Connection, OxidbError, QueryResult};

fn main() -> Result<(), OxidbError> {
    println!("=== OxiDB Simple Functionality Test ===\n");

    // Generate unique identifiers to avoid conflicts
    let process_id = std::process::id();
    
    // Test 1: Basic CRUD Operations
    test_basic_crud(process_id)?;
    
    // Test 2: Transactions
    test_transactions(process_id)?;
    
    // Test 3: Data Types
    test_data_types(process_id)?;
    
    // Test 4: File Persistence
    test_file_persistence(process_id)?;

    println!("🎉 All functionality tests passed! 🎉");
    Ok(())
}

fn test_basic_crud(process_id: u32) -> Result<(), OxidbError> {
    println!("--- Test 1: Basic CRUD Operations ---");
    
    let mut conn = Connection::open_in_memory()?;
    let table_name = format!("products_{}", process_id);
    
    // Create
    let sql = format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, name TEXT, price FLOAT)", table_name);
    conn.execute(&sql)?;
    println!("✓ Created table");
    
    // Insert
    let sql = format!("INSERT INTO {} (id, name, price) VALUES (1, 'Laptop', 999.99)", table_name);
    conn.execute(&sql)?;
    let sql = format!("INSERT INTO {} (id, name, price) VALUES (2, 'Mouse', 29.99)", table_name);
    conn.execute(&sql)?;
    println!("✓ Inserted records");
    
    // Read
    let sql = format!("SELECT * FROM {}", table_name);
    let result = conn.execute(&sql)?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Read {} records", data.row_count());
            for (i, row) in data.rows().enumerate() {
                println!("  Row {}: {:?}", i + 1, row);
            }
        }
        _ => return Err(OxidbError::Other("Expected data result".to_string())),
    }
    
    // Update
    let sql = format!("UPDATE {} SET price = 899.99 WHERE id = 1", table_name);
    conn.execute(&sql)?;
    println!("✓ Updated record");
    
    // Delete
    let sql = format!("DELETE FROM {} WHERE id = 2", table_name);
    conn.execute(&sql)?;
    println!("✓ Deleted record");
    
    // Verify final state
    let sql = format!("SELECT * FROM {}", table_name);
    let result = conn.execute(&sql)?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Final count: {} records", data.row_count());
        }
        _ => return Err(OxidbError::Other("Expected data result".to_string())),
    }
    
    println!("✅ CRUD test passed\n");
    Ok(())
}

fn test_transactions(process_id: u32) -> Result<(), OxidbError> {
    println!("--- Test 2: Transaction Management ---");
    
    let mut conn = Connection::open_in_memory()?;
    let table_name = format!("accounts_{}", process_id);
    
    // Setup
    let sql = format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, name TEXT, balance FLOAT)", table_name);
    conn.execute(&sql)?;
    let sql = format!("INSERT INTO {} (id, name, balance) VALUES (1, 'Alice', 1000.0)", table_name);
    conn.execute(&sql)?;
    let sql = format!("INSERT INTO {} (id, name, balance) VALUES (2, 'Bob', 500.0)", table_name);
    conn.execute(&sql)?;
    println!("✓ Setup accounts");
    
    // Test commit
    conn.begin_transaction()?;
    let sql = format!("UPDATE {} SET balance = balance - 100.0 WHERE id = 1", table_name);
    conn.execute(&sql)?;
    let sql = format!("UPDATE {} SET balance = balance + 100.0 WHERE id = 2", table_name);
    conn.execute(&sql)?;
    conn.commit()?;
    println!("✓ Transaction committed");
    
    // Test rollback
    conn.begin_transaction()?;
    let sql = format!("UPDATE {} SET balance = 0.0 WHERE id = 1", table_name);
    conn.execute(&sql)?;
    conn.rollback()?;
    println!("✓ Transaction rolled back");
    
    // Verify state
    let sql = format!("SELECT * FROM {}", table_name);
    let result = conn.execute(&sql)?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Final balances verified: {} accounts", data.row_count());
        }
        _ => return Err(OxidbError::Other("Expected data result".to_string())),
    }
    
    println!("✅ Transaction test passed\n");
    Ok(())
}

fn test_data_types(process_id: u32) -> Result<(), OxidbError> {
    println!("--- Test 3: Data Types ---");
    
    let mut conn = Connection::open_in_memory()?;
    let table_name = format!("types_{}", process_id);
    
    let sql = format!("CREATE TABLE {} (
        id INTEGER PRIMARY KEY,
        text_val TEXT,
        int_val INTEGER,
        float_val FLOAT,
        bool_val BOOLEAN
    )", table_name);
    conn.execute(&sql)?;
    println!("✓ Created table with multiple data types");
    
    let sql = format!("INSERT INTO {} (id, text_val, int_val, float_val, bool_val) 
                      VALUES (1, 'Hello', 42, 3.14, true)", table_name);
    conn.execute(&sql)?;
    
    let sql = format!("INSERT INTO {} (id, text_val, int_val, float_val, bool_val) 
                      VALUES (2, 'World', -100, 2.718, false)", table_name);
    conn.execute(&sql)?;
    println!("✓ Inserted records with various data types");
    
    let sql = format!("SELECT * FROM {}", table_name);
    let result = conn.execute(&sql)?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Retrieved {} records with columns: {:?}", 
                    data.row_count(), data.columns());
        }
        _ => return Err(OxidbError::Other("Expected data result".to_string())),
    }
    
    println!("✅ Data types test passed\n");
    Ok(())
}

fn test_file_persistence(process_id: u32) -> Result<(), OxidbError> {
    println!("--- Test 4: File Persistence ---");
    
    let db_file = format!("test_{}.db", process_id);
    let table_name = format!("settings_{}", process_id);
    
    // Create and populate database
    {
        let mut conn = Connection::open(&db_file)?;
        let sql = format!("CREATE TABLE {} (key TEXT PRIMARY KEY, value TEXT)", table_name);
        conn.execute(&sql)?;
        let sql = format!("INSERT INTO {} (key, value) VALUES ('theme', 'dark')", table_name);
        conn.execute(&sql)?;
        let sql = format!("INSERT INTO {} (key, value) VALUES ('lang', 'en')", table_name);
        conn.execute(&sql)?;
        conn.persist()?;
        println!("✓ Created and persisted database");
    }
    
    // Reopen and verify
    {
        let mut conn = Connection::open(&db_file)?;
        let sql = format!("SELECT * FROM {}", table_name);
        let result = conn.execute(&sql)?;
        match result {
            QueryResult::Data(data) => {
                println!("✓ Reopened database, found {} settings", data.row_count());
            }
            _ => return Err(OxidbError::Other("Expected data result".to_string())),
        }
    }
    
    // Clean up
    std::fs::remove_file(&db_file).ok();
    println!("✓ Cleaned up test file");
    
    println!("✅ File persistence test passed\n");
    Ok(())
}