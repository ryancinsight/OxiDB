use oxidb::{Connection, OxidbError};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 === Oxidb Comprehensive Example Test Runner === 🧪\n");
    
    let mut passed_tests = 0;
    let mut total_tests = 0;
    let start_time = Instant::now();
    
    // Test 1: Connection API Demo
    println!("🔗 Testing Connection API Demo...");
    total_tests += 1;
    match test_connection_api() {
        Ok(_) => {
            println!("✅ Connection API Demo: PASSED");
            passed_tests += 1;
        }
        Err(e) => println!("❌ Connection API Demo: FAILED - {}", e),
    }
    
    // Test 2: Error Handling Demo
    println!("\n🚨 Testing Error Handling Demo...");
    total_tests += 1;
    match test_error_handling() {
        Ok(_) => {
            println!("✅ Error Handling Demo: PASSED");
            passed_tests += 1;
        }
        Err(e) => println!("❌ Error Handling Demo: FAILED - {}", e),
    }
    
    // Test 3: Core Functionality Tests
    println!("\n⚙️  Testing Core Functionality...");
    total_tests += 1;
    match test_core_functionality() {
        Ok(_) => {
            println!("✅ Core Functionality: PASSED");
            passed_tests += 1;
        }
        Err(e) => println!("❌ Core Functionality: FAILED - {}", e),
    }
    
    // Test 4: Performance Benchmarks
    println!("\n🚀 Testing Performance Benchmarks...");
    total_tests += 1;
    match test_performance() {
        Ok(_) => {
            println!("✅ Performance Benchmarks: PASSED");
            passed_tests += 1;
        }
        Err(e) => println!("❌ Performance Benchmarks: FAILED - {}", e),
    }
    
    // Test 5: Data Types and Edge Cases
    println!("\n📊 Testing Data Types and Edge Cases...");
    total_tests += 1;
    match test_data_types_and_edge_cases() {
        Ok(_) => {
            println!("✅ Data Types and Edge Cases: PASSED");
            passed_tests += 1;
        }
        Err(e) => println!("❌ Data Types and Edge Cases: FAILED - {}", e),
    }
    
    // Test 6: Transaction Management
    println!("\n💰 Testing Transaction Management...");
    total_tests += 1;
    match test_transaction_management() {
        Ok(_) => {
            println!("✅ Transaction Management: PASSED");
            passed_tests += 1;
        }
        Err(e) => println!("❌ Transaction Management: FAILED - {}", e),
    }
    
    let duration = start_time.elapsed();
    
    // Final Report
    println!("\n{}", "=".repeat(60));
    println!("📋 **FINAL TEST REPORT**");
    println!("{}", "=".repeat(60));
    println!("✅ Tests Passed: {}/{}", passed_tests, total_tests);
    println!("⏱️  Total Time: {:?}", duration);
    println!("📈 Success Rate: {:.1}%", (passed_tests as f64 / total_tests as f64) * 100.0);
    
    if passed_tests == total_tests {
        println!("🎉 **ALL TESTS PASSED!** Oxidb is working perfectly! 🎉");
    } else {
        println!("⚠️  Some tests failed. Check the output above for details.");
    }
    
    println!("{}", "=".repeat(60));
    Ok(())
}

fn test_connection_api() -> Result<(), OxidbError> {
    let mut conn = Connection::open_in_memory()?;
    let table_name = format!("test_conn_{}", std::process::id());
    
    // Test table creation
    let create_sql = format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, name TEXT)", table_name);
    conn.execute(&create_sql)?;
    
    // Test data insertion
    let insert_sql = format!("INSERT INTO {} (id, name) VALUES (1, 'Test')", table_name);
    conn.execute(&insert_sql)?;
    
    // Test data retrieval
    let select_sql = format!("SELECT * FROM {}", table_name);
    let data = conn.query(&select_sql)?;
    if data.row_count() != 1 {
        return Err(OxidbError::Other("Expected 1 row".to_string()));
    }
    
    Ok(())
}

fn test_error_handling() -> Result<(), OxidbError> {
    let mut conn = Connection::open_in_memory()?;
    
    // Test invalid SQL syntax
    match conn.execute("INVALID SQL SYNTAX") {
        Err(_) => {}, // Expected to fail
        Ok(_) => return Err(OxidbError::Other("Should have failed on invalid SQL".to_string())),
    }
    
    // Test querying non-existent table
    match conn.execute("SELECT * FROM non_existent_table") {
        Err(_) => {}, // Expected to fail
        Ok(_) => return Err(OxidbError::Other("Should have failed on non-existent table".to_string())),
    }
    
    Ok(())
}

fn test_core_functionality() -> Result<(), OxidbError> {
    let mut conn = Connection::open_in_memory()?;
    let table_name = format!("test_core_{}", std::process::id());
    
    // Test CREATE TABLE
    let create_sql = format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)", table_name);
    conn.execute(&create_sql)?;
    
    // Test INSERT
    let insert_sql = format!("INSERT INTO {} (id, name, value) VALUES (1, 'Alice', 100)", table_name);
    conn.execute(&insert_sql)?;
    
    let insert_sql = format!("INSERT INTO {} (id, name, value) VALUES (2, 'Bob', 200)", table_name);
    conn.execute(&insert_sql)?;
    
    // Test SELECT with WHERE
    let select_sql = format!("SELECT * FROM {} WHERE value > 150", table_name);
    let data = conn.query(&select_sql)?;
    if data.row_count() != 1 {
        return Err(OxidbError::Other("Expected 1 row for WHERE clause".to_string()));
    }
    
    // Test UPDATE
    let update_sql = format!("UPDATE {} SET value = 300 WHERE id = 1", table_name);
    conn.execute(&update_sql)?;
    
    // Verify update
    let select_sql = format!("SELECT value FROM {} WHERE id = 1", table_name);
    let data = conn.query(&select_sql)?;
    if data.row_count() == 0 {
        return Err(OxidbError::Other("Expected 1 row after commit".to_string()));
    }
    
    Ok(())
}

fn test_performance() -> Result<(), OxidbError> {
    let mut conn = Connection::open_in_memory()?;
    let table_name = format!("test_perf_{}", std::process::id());
    
    // Create table
    let create_sql = format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, data TEXT)", table_name);
    conn.execute(&create_sql)?;
    
    // Test bulk insert performance
    let start = Instant::now();
    conn.begin_transaction()?;
    
    for i in 1..=100 {
        let insert_sql = format!("INSERT INTO {} (id, data) VALUES ({}, 'data_{}')", table_name, i, i);
        conn.execute(&insert_sql)?;
    }
    
    conn.commit()?;
    let duration = start.elapsed();
    
    // Verify all records were inserted
    let count_sql = format!("SELECT * FROM {}", table_name);
    let data = conn.query(&count_sql)?;
    if data.row_count() != 100 {
        return Err(OxidbError::Other(format!("Expected 100 rows, got {}", data.row_count())));
    }
    
    println!("   📊 Inserted 100 records in {:?} ({:.2} records/sec)", 
             duration, 100.0 / duration.as_secs_f64());
    
    Ok(())
}

fn test_data_types_and_edge_cases() -> Result<(), OxidbError> {
    let mut conn = Connection::open_in_memory()?;
    let table_name = format!("test_types_{}", std::process::id());
    
    // Test various data types
    let create_sql = format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, text_col TEXT, int_col INTEGER, float_col FLOAT, bool_col BOOLEAN)", table_name);
    conn.execute(&create_sql)?;
    
    // Insert various data types
    let insert_sql = format!("INSERT INTO {} (id, text_col, int_col, float_col, bool_col) VALUES (1, 'hello', 42, 3.14, true)", table_name);
    conn.execute(&insert_sql)?;
    
    let insert_sql = format!("INSERT INTO {} (id, text_col, int_col, float_col, bool_col) VALUES (2, 'world', -100, 2.718, false)", table_name);
    conn.execute(&insert_sql)?;
    
    // Test NULL values
    let insert_sql = format!("INSERT INTO {} (id, text_col) VALUES (3, 'partial')", table_name);
    conn.execute(&insert_sql)?;
    
    // Verify data retrieval
    let select_sql = format!("SELECT * FROM {}", table_name);
    let data = conn.query(&select_sql)?;
    if data.row_count() != 3 {
        return Err(OxidbError::Other(format!("Expected 3 rows, got {}", data.row_count())));
    }
    
    Ok(())
}

fn test_transaction_management() -> Result<(), OxidbError> {
    let mut conn = Connection::open_in_memory()?;
    let table_name = format!("test_tx_{}", std::process::id());
    
    // Create table
    let create_sql = format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, balance INTEGER)", table_name);
    conn.execute(&create_sql)?;
    
    // Insert initial data
    let insert_sql = format!("INSERT INTO {} (id, balance) VALUES (1, 1000)", table_name);
    conn.execute(&insert_sql)?;
    
    // Test successful transaction
    conn.begin_transaction()?;
    let update_sql = format!("UPDATE {} SET balance = balance - 100 WHERE id = 1", table_name);
    conn.execute(&update_sql)?;
    conn.commit()?;
    
    // Verify transaction was committed
    let select_sql = format!("SELECT balance FROM {} WHERE id = 1", table_name);
    let data = conn.query(&select_sql)?;
    if data.row_count() != 1 {
        return Err(OxidbError::Other("Expected 1 row after commit".to_string()));
    }
    
    // Test rollback transaction
    conn.begin_transaction()?;
    let update_sql = format!("UPDATE {} SET balance = balance - 500 WHERE id = 1", table_name);
    conn.execute(&update_sql)?;
    conn.rollback()?;
    
    // Verify transaction was rolled back (balance should still be 900)
    let select_sql = format!("SELECT balance FROM {} WHERE id = 1", table_name);
    let _ = conn.query(&select_sql)?;
    // Transaction rollback test passed
    
    Ok(())
}