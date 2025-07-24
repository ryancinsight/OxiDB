use oxidb::Connection;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== OxiDB Error Handling and Edge Cases Demo ===\n");

    test_connection_errors()?;
    test_sql_syntax_errors()?;
    test_constraint_violations()?;
    test_transaction_errors()?;
    test_edge_cases()?;

    println!("\n✅ All error handling tests completed successfully! ✅");
    Ok(())
}

fn test_connection_errors() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Test 1: Connection Errors ---");
    
    let mut conn = Connection::open_in_memory()?;
    println!("✓ Successfully created valid in-memory connection");
    
    match conn.execute("CREATE TABLE test (id INTEGER)") {
        Ok(_) => println!("✓ Successfully executed query on valid connection"),
        Err(e) => println!("✗ Unexpected error on valid connection: {:?}", e),
    }
    
    println!("✅ Connection error tests completed\n");
    Ok(())
}

fn test_sql_syntax_errors() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Test 2: SQL Syntax Errors ---");
    
    let mut conn = Connection::open_in_memory()?;
    
    let invalid_queries = [
        ("Empty query", ""),
        ("Invalid CREATE", "CREATE TABEL users (id INTEGER)"),
        ("Missing FROM", "SELECT * users"),
        ("Invalid INSERT", "INSERT users VALUES (1)"),
    ];
    
    for (test_name, query) in &invalid_queries {
        match conn.execute(query) {
            Ok(_) => println!("⚠ Expected error for {}, but query succeeded", test_name),
            Err(e) => println!("✓ Expected error for {}: {:?}", test_name, e),
        }
    }
    
    println!("✅ SQL syntax error tests completed\n");
    Ok(())
}

fn test_constraint_violations() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Test 3: Constraint Violations ---");
    
    let mut conn = Connection::open_in_memory()?;
    conn.execute("CREATE TABLE constrained_table (id INTEGER PRIMARY KEY, unique_field TEXT)")?;
    
    conn.execute("INSERT INTO constrained_table (id, unique_field) VALUES (1, 'unique1')")?;
    println!("✓ Successfully inserted valid record");
    
    match conn.execute("INSERT INTO constrained_table (id, unique_field) VALUES (1, 'unique2')") {
        Ok(_) => println!("⚠ Expected constraint violation, but query succeeded"),
        Err(e) => println!("✓ Expected constraint violation: {:?}", e),
    }
    
    println!("✅ Constraint violation tests completed\n");
    Ok(())
}

fn test_transaction_errors() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Test 4: Transaction Errors ---");
    
    let mut conn = Connection::open_in_memory()?;
    conn.execute("CREATE TABLE tx_test (id INTEGER PRIMARY KEY, value INTEGER)")?;
    
    conn.begin_transaction()?;
    println!("✓ Started transaction");
    
    match conn.begin_transaction() {
        Ok(_) => println!("⚠ Expected error for nested transaction, but it succeeded"),
        Err(e) => println!("✓ Expected error for nested transaction: {:?}", e),
    }
    
    conn.commit()?;
    println!("✓ Committed transaction");
    
    match conn.commit() {
        Ok(_) => println!("⚠ Expected error for commit without transaction, but it succeeded"),
        Err(e) => println!("✓ Expected error for commit without transaction: {:?}", e),
    }
    
    println!("✅ Transaction error tests completed\n");
    Ok(())
}

fn test_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Test 5: Edge Cases ---");
    
    let mut conn = Connection::open_in_memory()?;
    
    conn.execute("CREATE TABLE string_test (id INTEGER, data TEXT)")?;
    let long_string = "x".repeat(1000);
    match conn.execute(&format!("INSERT INTO string_test (id, data) VALUES (1, '{}')", long_string)) {
        Ok(_) => println!("✓ Successfully inserted long string"),
        Err(e) => println!("⚠ Error inserting long string: {:?}", e),
    }
    
    conn.execute("CREATE TABLE numeric_test (id INTEGER, value FLOAT)")?;
    match conn.execute("INSERT INTO numeric_test VALUES (1, 0.0)") {
        Ok(_) => println!("✓ Successfully handled zero value"),
        Err(e) => println!("⚠ Error with zero value: {:?}", e),
    }
    
    println!("✅ Edge case tests completed\n");
    Ok(())
}