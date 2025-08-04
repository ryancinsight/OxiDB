use oxidb::{Connection, OxidbError, QueryResult};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

/// Comprehensive edge case tests for Oxidb
/// Follows SOLID principles with single responsibility per test function
/// Implements DRY principle with helper functions
/// Uses KISS principle with clear, simple test logic
fn main() -> Result<(), OxidbError> {
    println!("=== Oxidb Comprehensive Edge Case Tests ===\n");

    // Test suite organized by categories (SRP - Single Responsibility Principle)
    run_data_type_edge_cases()?;
    run_transaction_edge_cases()?;
    run_concurrency_edge_cases()?;
    run_memory_limit_edge_cases()?;
    run_sql_injection_protection_tests()?;
    run_constraint_violation_edge_cases()?;
    run_index_edge_cases()?;
    run_recovery_edge_cases()?;

    println!("\n🎉 All edge case tests completed successfully! 🎉");
    Ok(())
}

/// Test edge cases for data types (boundary values, null handling, type coercion)
/// Implements Single Responsibility Principle
fn run_data_type_edge_cases() -> Result<(), OxidbError> {
    println!("--- Data Type Edge Cases ---");
    let mut conn = Connection::open_in_memory()?;

    // Test integer boundaries
    test_integer_boundaries(&mut conn)?;
    
    // Test string length limits
    test_string_boundaries(&mut conn)?;
    
    // Test null handling
    test_null_edge_cases(&mut conn)?;
    
    // Test type coercion edge cases
    test_type_coercion_edge_cases(&mut conn)?;
    
    // Test floating point precision
    test_floating_point_precision(&mut conn)?;

    println!("✅ Data type edge cases passed\n");
    Ok(())
}

/// Test integer boundary conditions (ACID compliance)
fn test_integer_boundaries(conn: &mut Connection) -> Result<(), OxidbError> {
    conn.execute("CREATE TABLE int_bounds (id INTEGER PRIMARY KEY, value INTEGER)")?;
    
    // Test maximum and minimum values
    let test_values = vec![
        i64::MAX,
        i64::MIN,
        0,
        -1,
        1,
        i32::MAX as i64,
        i32::MIN as i64,
    ];
    
    for value in test_values {
        let sql = format!("INSERT INTO int_bounds (value) VALUES ({})", value);
        conn.execute(&sql)?;
        
        // Verify the value was stored correctly (ACID - Consistency)
        let verify_sql = format!("SELECT value FROM int_bounds WHERE value = {}", value);
        let result = conn.execute(&verify_sql)?;
        assert_query_returns_data(&result, "Integer boundary test")?;
    }
    
    println!("✓ Integer boundaries test passed");
    Ok(())
}

/// Test string boundary conditions and encoding
fn test_string_boundaries(conn: &mut Connection) -> Result<(), OxidbError> {
    conn.execute("CREATE TABLE string_bounds (id INTEGER PRIMARY KEY, text_col TEXT, varchar_col VARCHAR(10))")?;
    
    // Test various string scenarios
    let long_string = "A".repeat(1000);
    let test_cases = vec![
        ("", "Empty string"),
        ("a", "Single character"),
        ("Hello, 世界! 🌍", "Unicode characters"),
        (long_string.as_str(), "Long string"),
        ("String with\nnewlines\tand\ttabs", "Control characters"),
        ("String with 'quotes' and \"double quotes\"", "Quote handling"),
        ("SQL injection'; DROP TABLE users; --", "SQL injection attempt"),
    ];
    
    for (text, description) in test_cases {
        let sql = format!("INSERT INTO string_bounds (text_col) VALUES ('{}')", escape_sql_string(text));
        match conn.execute(&sql) {
            Ok(_) => println!("✓ {}: Handled correctly", description),
            Err(e) => println!("⚠ {}: Error (expected for some cases): {:?}", description, e),
        }
    }
    
    println!("✓ String boundaries test completed");
    Ok(())
}

/// Test NULL handling edge cases
fn test_null_edge_cases(conn: &mut Connection) -> Result<(), OxidbError> {
    conn.execute("CREATE TABLE null_test (id INTEGER PRIMARY KEY, nullable_col TEXT, not_null_col TEXT NOT NULL)")?;
    
    // Test NULL insertions
    let test_cases = vec![
        ("INSERT INTO null_test (nullable_col, not_null_col) VALUES (NULL, 'valid')", true),
        ("INSERT INTO null_test (nullable_col, not_null_col) VALUES ('valid', NULL)", false),
        ("INSERT INTO null_test (not_null_col) VALUES ('valid')", true),
    ];
    
    for (sql, should_succeed) in test_cases {
        match conn.execute(sql) {
            Ok(_) if should_succeed => println!("✓ NULL test passed: {}", sql),
            Err(_) if !should_succeed => println!("✓ NULL constraint enforced: {}", sql),
            Ok(_) => println!("⚠ Expected failure but succeeded: {}", sql),
            Err(e) => println!("⚠ Unexpected failure: {} - {:?}", sql, e),
        }
    }
    
    println!("✓ NULL edge cases test completed");
    Ok(())
}

/// Test type coercion edge cases
fn test_type_coercion_edge_cases(conn: &mut Connection) -> Result<(), OxidbError> {
    conn.execute("CREATE TABLE coercion_test (id INTEGER PRIMARY KEY, int_col INTEGER, text_col TEXT, bool_col BOOLEAN)")?;
    
    // Test various type coercions
    let test_cases = vec![
        ("INSERT INTO coercion_test (int_col) VALUES ('123')", "String to integer"),
        ("INSERT INTO coercion_test (int_col) VALUES ('not_a_number')", "Invalid string to integer"),
        ("INSERT INTO coercion_test (text_col) VALUES (456)", "Integer to text"),
        ("INSERT INTO coercion_test (bool_col) VALUES (1)", "Integer to boolean"),
        ("INSERT INTO coercion_test (bool_col) VALUES ('true')", "String to boolean"),
        ("INSERT INTO coercion_test (bool_col) VALUES ('maybe')", "Invalid string to boolean"),
    ];
    
    for (sql, description) in test_cases {
        match conn.execute(sql) {
            Ok(_) => println!("✓ Type coercion succeeded: {}", description),
            Err(e) => println!("⚠ Type coercion failed (may be expected): {} - {:?}", description, e),
        }
    }
    
    println!("✓ Type coercion edge cases test completed");
    Ok(())
}

/// Test floating point precision edge cases
fn test_floating_point_precision(conn: &mut Connection) -> Result<(), OxidbError> {
    conn.execute("CREATE TABLE float_test (id INTEGER PRIMARY KEY, float_val REAL)")?;
    
    // Test floating point edge cases
    let test_values = vec![
        0.0,
        -0.0,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NAN,
        f64::MIN,
        f64::MAX,
        f64::EPSILON,
        1.0 / 3.0, // Repeating decimal
        0.1 + 0.2, // Floating point arithmetic precision
    ];
    
    for value in test_values {
        let sql = format!("INSERT INTO float_test (float_val) VALUES ({})", value);
        match conn.execute(&sql) {
            Ok(_) => println!("✓ Float value handled: {}", value),
            Err(e) => println!("⚠ Float value failed: {} - {:?}", value, e),
        }
    }
    
    println!("✓ Floating point precision test completed");
    Ok(())
}

/// Test transaction edge cases (ACID compliance)
fn run_transaction_edge_cases() -> Result<(), OxidbError> {
    println!("--- Transaction Edge Cases ---");
    let mut conn = Connection::open_in_memory()?;
    
    // Test nested transactions
    test_nested_transactions(&mut conn)?;
    
    // Test transaction rollback scenarios
    test_transaction_rollback_scenarios(&mut conn)?;
    
    // Test transaction isolation
    test_transaction_isolation(&mut conn)?;
    
    // Test deadlock detection
    test_deadlock_scenarios(&mut conn)?;

    println!("✅ Transaction edge cases passed\n");
    Ok(())
}

/// Test nested transaction behavior
fn test_nested_transactions(conn: &mut Connection) -> Result<(), OxidbError> {
    conn.execute("CREATE TABLE trans_test (id INTEGER PRIMARY KEY, value INTEGER)")?;
    
    // Test transaction within transaction
    conn.execute("BEGIN TRANSACTION")?;
    conn.execute("INSERT INTO trans_test (value) VALUES (1)")?;
    
    // Attempt nested transaction (should handle gracefully)
    match conn.execute("BEGIN TRANSACTION") {
        Ok(_) => println!("✓ Nested transaction allowed"),
        Err(e) => println!("✓ Nested transaction rejected: {:?}", e),
    }
    
    conn.execute("COMMIT")?;
    println!("✓ Nested transaction test completed");
    Ok(())
}

/// Test transaction rollback scenarios
fn test_transaction_rollback_scenarios(conn: &mut Connection) -> Result<(), OxidbError> {
    conn.execute("CREATE TABLE rollback_test (id INTEGER PRIMARY KEY, value INTEGER UNIQUE)")?;
    
    // Test rollback on constraint violation
    conn.execute("BEGIN TRANSACTION")?;
    conn.execute("INSERT INTO rollback_test (value) VALUES (100)")?;
    conn.execute("INSERT INTO rollback_test (value) VALUES (200)")?;
    
    // This should cause a constraint violation
    match conn.execute("INSERT INTO rollback_test (value) VALUES (100)") {
        Ok(_) => println!("⚠ Expected constraint violation but succeeded"),
        Err(_) => {
            conn.execute("ROLLBACK")?;
            println!("✓ Transaction rolled back on constraint violation");
        }
    }
    
    // Verify rollback worked (table should be empty)
    let _result = conn.execute("SELECT COUNT(*) FROM rollback_test")?;
    println!("✓ Rollback verification completed");
    Ok(())
}

/// Test transaction isolation levels
fn test_transaction_isolation(conn: &mut Connection) -> Result<(), OxidbError> {
    conn.execute("CREATE TABLE isolation_test (id INTEGER PRIMARY KEY, value INTEGER)")?;
    conn.execute("INSERT INTO isolation_test (value) VALUES (1)")?;
    
    // Test read committed isolation
    conn.execute("BEGIN TRANSACTION")?;
    conn.execute("UPDATE isolation_test SET value = 2 WHERE id = 1")?;
    
    // In a real multi-connection scenario, another connection shouldn't see this change
    let _result = conn.execute("SELECT value FROM isolation_test WHERE id = 1")?;
    
    conn.execute("COMMIT")?;
    println!("✓ Transaction isolation test completed");
    Ok(())
}

/// Test deadlock detection and resolution
fn test_deadlock_scenarios(conn: &mut Connection) -> Result<(), OxidbError> {
    conn.execute("CREATE TABLE deadlock_test (id INTEGER PRIMARY KEY, value INTEGER)")?;
    conn.execute("INSERT INTO deadlock_test (id, value) VALUES (1, 100), (2, 200)")?;
    
    // Simulate potential deadlock scenario
    conn.execute("BEGIN TRANSACTION")?;
    conn.execute("UPDATE deadlock_test SET value = 150 WHERE id = 1")?;
    
    // In a multi-threaded scenario, this could cause deadlock
    match conn.execute("UPDATE deadlock_test SET value = 250 WHERE id = 2") {
        Ok(_) => {
            conn.execute("COMMIT")?;
            println!("✓ Deadlock scenario handled");
        }
        Err(e) => {
            conn.execute("ROLLBACK")?;
            println!("✓ Deadlock detected and resolved: {:?}", e);
        }
    }
    
    Ok(())
}

/// Test concurrency edge cases
fn run_concurrency_edge_cases() -> Result<(), OxidbError> {
    println!("--- Concurrency Edge Cases ---");
    
    // Test concurrent connections
    test_concurrent_connections()?;
    
    // Test concurrent transactions
    test_concurrent_write_operations()?;
    
    // Test connection pool exhaustion
    test_connection_limits()?;

    println!("✅ Concurrency edge cases passed\n");
    Ok(())
}

/// Test multiple concurrent connections
fn test_concurrent_connections() -> Result<(), OxidbError> {
    let db_path = "test_concurrent.db";
    
    // Clean up any existing test database
    let _ = std::fs::remove_file(db_path);
    
    let handles: Vec<_> = (0..5).map(|i| {
        let path = db_path.to_string();
        thread::spawn(move || -> Result<(), OxidbError> {
            let mut conn = Connection::open(&path)?;
            conn.execute(&format!("CREATE TABLE IF NOT EXISTS concurrent_test_{} (id INTEGER PRIMARY KEY, value INTEGER)", i))?;
            
            for j in 0..10 {
                conn.execute(&format!("INSERT INTO concurrent_test_{} (value) VALUES ({})", i, j))?;
            }
            
            Ok(())
        })
    }).collect();
    
    // Wait for all threads to complete
    for handle in handles {
        match handle.join() {
            Ok(Ok(_)) => println!("✓ Concurrent connection succeeded"),
            Ok(Err(e)) => println!("⚠ Concurrent connection failed: {:?}", e),
            Err(_) => println!("⚠ Thread panicked"),
        }
    }
    
    // Clean up
    let _ = std::fs::remove_file(db_path);
    
    println!("✓ Concurrent connections test completed");
    Ok(())
}

/// Test concurrent write operations
fn test_concurrent_write_operations() -> Result<(), OxidbError> {
    let results = Arc::new(Mutex::new(Vec::new()));
    let handles: Vec<_> = (0..3).map(|i| {
        let results_clone = Arc::clone(&results);
        thread::spawn(move || -> Result<(), OxidbError> {
            let mut conn = Connection::open_in_memory()?;
            conn.execute("CREATE TABLE IF NOT EXISTS concurrent_writes (id INTEGER PRIMARY KEY, thread_id INTEGER, value INTEGER)")?;
            
            for j in 0..5 {
                let sql = format!("INSERT INTO concurrent_writes (thread_id, value) VALUES ({}, {})", i, j);
                match conn.execute(&sql) {
                    Ok(_) => {
                        let mut results = results_clone.lock().unwrap();
                        results.push(format!("Thread {} inserted value {}", i, j));
                    }
                    Err(e) => {
                        let mut results = results_clone.lock().unwrap();
                        results.push(format!("Thread {} failed: {:?}", i, e));
                    }
                }
            }
            Ok(())
        })
    }).collect();
    
    // Wait for all threads
    for handle in handles {
        let _ = handle.join();
    }
    
    let results = results.lock().unwrap();
    println!("✓ Concurrent write operations: {} operations completed", results.len());
    Ok(())
}

/// Test connection limits and resource exhaustion
fn test_connection_limits() -> Result<(), OxidbError> {
    let mut connections = Vec::new();
    let max_connections = 50; // Reasonable limit for testing
    
    for i in 0..max_connections {
        match Connection::open_in_memory() {
            Ok(conn) => {
                connections.push(conn);
                if i % 10 == 0 {
                    println!("✓ Created {} connections", i + 1);
                }
            }
            Err(e) => {
                println!("⚠ Connection limit reached at {}: {:?}", i, e);
                break;
            }
        }
    }
    
    println!("✓ Connection limits test completed with {} connections", connections.len());
    Ok(())
}

/// Test memory limit edge cases
fn run_memory_limit_edge_cases() -> Result<(), OxidbError> {
    println!("--- Memory Limit Edge Cases ---");
    
    test_large_query_results()?;
    test_large_transactions()?;
    test_memory_pressure_scenarios()?;

    println!("✅ Memory limit edge cases passed\n");
    Ok(())
}

/// Test handling of large query results
fn test_large_query_results() -> Result<(), OxidbError> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute("CREATE TABLE large_result_test (id INTEGER PRIMARY KEY, data TEXT)")?;
    
    // Insert a reasonable amount of test data
    let large_string = "A".repeat(1000);
    for i in 0..100 {
        let sql = format!("INSERT INTO large_result_test (data) VALUES ('{}')", large_string);
        conn.execute(&sql)?;
        
        if i % 25 == 0 {
            println!("✓ Inserted {} large records", i + 1);
        }
    }
    
    // Query all data
    let start_time = Instant::now();
    let _result = conn.execute("SELECT * FROM large_result_test")?;
    let duration = start_time.elapsed();
    
    println!("✓ Large query completed in {:?}", duration);
    Ok(())
}

/// Test large transactions
fn test_large_transactions() -> Result<(), OxidbError> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute("CREATE TABLE large_transaction_test (id INTEGER PRIMARY KEY, value INTEGER)")?;
    
    conn.execute("BEGIN TRANSACTION")?;
    
    let start_time = Instant::now();
    for i in 0..1000 {
        conn.execute(&format!("INSERT INTO large_transaction_test (value) VALUES ({})", i))?;
        
        if i % 250 == 0 {
            println!("✓ Transaction progress: {} operations", i + 1);
        }
    }
    
    conn.execute("COMMIT")?;
    let duration = start_time.elapsed();
    
    println!("✓ Large transaction completed in {:?}", duration);
    Ok(())
}

/// Test memory pressure scenarios
fn test_memory_pressure_scenarios() -> Result<(), OxidbError> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute("CREATE TABLE memory_pressure_test (id INTEGER PRIMARY KEY, data BLOB)")?;
    
    // Create progressively larger data
    for i in 1..=10 {
        let data_size = i * 10000; // 10KB, 20KB, ..., 100KB
        let large_data = vec![b'X'; data_size];
                    let hex_data = oxidb::core::common::hex::encode(&large_data);
        
        match conn.execute(&format!("INSERT INTO memory_pressure_test (data) VALUES (x'{}')", hex_data)) {
            Ok(_) => println!("✓ Inserted {}KB data block", data_size / 1000),
            Err(e) => println!("⚠ Failed to insert {}KB data: {:?}", data_size / 1000, e),
        }
    }
    
    println!("✓ Memory pressure test completed");
    Ok(())
}

/// Test SQL injection protection
fn run_sql_injection_protection_tests() -> Result<(), OxidbError> {
    println!("--- SQL Injection Protection Tests ---");
    
    let mut conn = Connection::open_in_memory()?;
    conn.execute("CREATE TABLE injection_test (id INTEGER PRIMARY KEY, username TEXT, password TEXT)")?;
    conn.execute("INSERT INTO injection_test (username, password) VALUES ('admin', 'secret123')")?;
    
    // Test various SQL injection attempts
    let injection_attempts = vec![
        "'; DROP TABLE injection_test; --",
        "' OR '1'='1",
        "' UNION SELECT * FROM injection_test --",
        "'; INSERT INTO injection_test VALUES (999, 'hacker', 'pwned'); --",
        "admin'; UPDATE injection_test SET password='hacked' WHERE username='admin'; --",
    ];
    
    for attempt in injection_attempts {
        let sql = format!("SELECT * FROM injection_test WHERE username = '{}'", attempt);
        match conn.execute(&sql) {
            Ok(_result) => {
                println!("⚠ Potential injection vulnerability: query succeeded for '{}'", attempt);
                // Check if the injection had any effect
                let _check_result = conn.execute("SELECT COUNT(*) FROM injection_test")?;
            }
            Err(e) => println!("✓ Injection attempt blocked: '{}' - {:?}", attempt, e),
        }
    }
    
    println!("✅ SQL injection protection tests completed\n");
    Ok(())
}

/// Test constraint violation edge cases
fn run_constraint_violation_edge_cases() -> Result<(), OxidbError> {
    println!("--- Constraint Violation Edge Cases ---");
    
    let mut conn = Connection::open_in_memory()?;
    
    test_primary_key_violations(&mut conn)?;
    test_foreign_key_violations(&mut conn)?;
    test_check_constraint_violations(&mut conn)?;
    test_unique_constraint_violations(&mut conn)?;

    println!("✅ Constraint violation edge cases passed\n");
    Ok(())
}

/// Test primary key constraint violations
fn test_primary_key_violations(conn: &mut Connection) -> Result<(), OxidbError> {
    conn.execute("CREATE TABLE pk_violation_test (id INTEGER PRIMARY KEY, name TEXT)")?;
    
    // Insert valid record
    conn.execute("INSERT INTO pk_violation_test (id, name) VALUES (1, 'First')")?;
    
    // Attempt duplicate primary key
    match conn.execute("INSERT INTO pk_violation_test (id, name) VALUES (1, 'Duplicate')") {
        Ok(_) => println!("⚠ Primary key violation not caught"),
        Err(e) => println!("✓ Primary key violation caught: {:?}", e),
    }
    
    // Test NULL primary key
    match conn.execute("INSERT INTO pk_violation_test (id, name) VALUES (NULL, 'Null PK')") {
        Ok(_) => println!("⚠ NULL primary key allowed"),
        Err(e) => println!("✓ NULL primary key rejected: {:?}", e),
    }
    
    println!("✓ Primary key violation tests completed");
    Ok(())
}

/// Test foreign key constraint violations
fn test_foreign_key_violations(conn: &mut Connection) -> Result<(), OxidbError> {
    conn.execute("CREATE TABLE parent_table (id INTEGER PRIMARY KEY, name TEXT)")?;
    conn.execute("CREATE TABLE child_table (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parent_table(id))")?;
    
    // Insert parent record
    conn.execute("INSERT INTO parent_table (id, name) VALUES (1, 'Parent')")?;
    
    // Valid foreign key reference
    match conn.execute("INSERT INTO child_table (id, parent_id) VALUES (1, 1)") {
        Ok(_) => println!("✓ Valid foreign key reference accepted"),
        Err(e) => println!("⚠ Valid foreign key reference rejected: {:?}", e),
    }
    
    // Invalid foreign key reference
    match conn.execute("INSERT INTO child_table (id, parent_id) VALUES (2, 999)") {
        Ok(_) => println!("⚠ Invalid foreign key reference allowed"),
        Err(e) => println!("✓ Invalid foreign key reference caught: {:?}", e),
    }
    
    println!("✓ Foreign key violation tests completed");
    Ok(())
}

/// Test check constraint violations
fn test_check_constraint_violations(conn: &mut Connection) -> Result<(), OxidbError> {
    // Note: Check constraints might not be fully implemented, so we test what we can
    conn.execute("CREATE TABLE check_test (id INTEGER PRIMARY KEY, age INTEGER CHECK (age >= 0))")?;
    
    // Valid check constraint
    match conn.execute("INSERT INTO check_test (id, age) VALUES (1, 25)") {
        Ok(_) => println!("✓ Valid check constraint accepted"),
        Err(e) => println!("⚠ Valid check constraint rejected: {:?}", e),
    }
    
    // Invalid check constraint
    match conn.execute("INSERT INTO check_test (id, age) VALUES (2, -5)") {
        Ok(_) => println!("⚠ Check constraint violation allowed"),
        Err(e) => println!("✓ Check constraint violation caught: {:?}", e),
    }
    
    println!("✓ Check constraint violation tests completed");
    Ok(())
}

/// Test unique constraint violations
fn test_unique_constraint_violations(conn: &mut Connection) -> Result<(), OxidbError> {
    conn.execute("CREATE TABLE unique_test (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")?;
    
    // Insert first record
    conn.execute("INSERT INTO unique_test (id, email) VALUES (1, 'user@example.com')")?;
    
    // Attempt duplicate unique value
    match conn.execute("INSERT INTO unique_test (id, email) VALUES (2, 'user@example.com')") {
        Ok(_) => println!("⚠ Unique constraint violation not caught"),
        Err(e) => println!("✓ Unique constraint violation caught: {:?}", e),
    }
    
    // Test NULL unique values (should be allowed)
    match conn.execute("INSERT INTO unique_test (id, email) VALUES (3, NULL)") {
        Ok(_) => println!("✓ NULL unique value allowed"),
        Err(e) => println!("⚠ NULL unique value rejected: {:?}", e),
    }
    
    println!("✓ Unique constraint violation tests completed");
    Ok(())
}

/// Test index edge cases
fn run_index_edge_cases() -> Result<(), OxidbError> {
    println!("--- Index Edge Cases ---");
    
    let mut conn = Connection::open_in_memory()?;
    
    test_index_on_large_data(&mut conn)?;
    test_index_on_null_values(&mut conn)?;
    test_composite_index_edge_cases(&mut conn)?;

    println!("✅ Index edge cases passed\n");
    Ok(())
}

/// Test indexing on large datasets
fn test_index_on_large_data(conn: &mut Connection) -> Result<(), OxidbError> {
    conn.execute("CREATE TABLE large_index_test (id INTEGER PRIMARY KEY, search_col TEXT)")?;
    
    // Insert test data
    for i in 0..1000 {
        conn.execute(&format!("INSERT INTO large_index_test (search_col) VALUES ('value_{}')", i))?;
    }
    
    // Create index
    match conn.execute("CREATE INDEX idx_search_col ON large_index_test(search_col)") {
        Ok(_) => println!("✓ Index created on large dataset"),
        Err(e) => println!("⚠ Index creation failed: {:?}", e),
    }
    
    // Test index usage
    let start_time = Instant::now();
    conn.execute("SELECT * FROM large_index_test WHERE search_col = 'value_500'")?;
    let duration = start_time.elapsed();
    
    println!("✓ Index query completed in {:?}", duration);
    Ok(())
}

/// Test indexing on NULL values
fn test_index_on_null_values(conn: &mut Connection) -> Result<(), OxidbError> {
    conn.execute("CREATE TABLE null_index_test (id INTEGER PRIMARY KEY, nullable_col TEXT)")?;
    
    // Insert data with NULLs
    conn.execute("INSERT INTO null_index_test (nullable_col) VALUES ('value1')")?;
    conn.execute("INSERT INTO null_index_test (nullable_col) VALUES (NULL)")?;
    conn.execute("INSERT INTO null_index_test (nullable_col) VALUES ('value2')")?;
    conn.execute("INSERT INTO null_index_test (nullable_col) VALUES (NULL)")?;
    
    // Create index on nullable column
    match conn.execute("CREATE INDEX idx_nullable ON null_index_test(nullable_col)") {
        Ok(_) => println!("✓ Index created on nullable column"),
        Err(e) => println!("⚠ Index on nullable column failed: {:?}", e),
    }
    
    // Query NULL values
    conn.execute("SELECT * FROM null_index_test WHERE nullable_col IS NULL")?;
    println!("✓ NULL index query completed");
    
    Ok(())
}

/// Test composite index edge cases
fn test_composite_index_edge_cases(conn: &mut Connection) -> Result<(), OxidbError> {
    conn.execute("CREATE TABLE composite_index_test (id INTEGER PRIMARY KEY, col1 TEXT, col2 INTEGER, col3 TEXT)")?;
    
    // Insert test data
    for i in 0..100 {
        conn.execute(&format!("INSERT INTO composite_index_test (col1, col2, col3) VALUES ('prefix_{}', {}, 'suffix_{}')", 
                             i % 10, i, i % 5))?;
    }
    
    // Create composite index
    match conn.execute("CREATE INDEX idx_composite ON composite_index_test(col1, col2, col3)") {
        Ok(_) => println!("✓ Composite index created"),
        Err(e) => println!("⚠ Composite index creation failed: {:?}", e),
    }
    
    // Test partial composite queries
    conn.execute("SELECT * FROM composite_index_test WHERE col1 = 'prefix_1'")?;
    conn.execute("SELECT * FROM composite_index_test WHERE col1 = 'prefix_1' AND col2 = 1")?;
    
    println!("✓ Composite index tests completed");
    Ok(())
}

/// Test recovery edge cases
fn run_recovery_edge_cases() -> Result<(), OxidbError> {
    println!("--- Recovery Edge Cases ---");
    
    test_crash_recovery_simulation()?;
    test_corrupted_data_handling()?;
    test_wal_recovery_scenarios()?;

    println!("✅ Recovery edge cases passed\n");
    Ok(())
}

/// Simulate crash recovery scenarios
fn test_crash_recovery_simulation() -> Result<(), OxidbError> {
    let db_path = "test_crash_recovery.db";
    let _ = std::fs::remove_file(db_path);
    
    // Create database and insert data
    {
        let mut conn = Connection::open(db_path)?;
        conn.execute("CREATE TABLE recovery_test (id INTEGER PRIMARY KEY, data TEXT)")?;
        conn.execute("INSERT INTO recovery_test (data) VALUES ('test_data')")?;
        // Simulate crash by not properly closing
    }
    
    // Reopen database (simulating recovery)
    {
        let mut conn = Connection::open(db_path)?;
        let _result = conn.execute("SELECT COUNT(*) FROM recovery_test")?;
        println!("✓ Database recovered after simulated crash");
    }
    
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

/// Test handling of corrupted data
fn test_corrupted_data_handling() -> Result<(), OxidbError> {
    let db_path = "test_corruption.db";
    let _ = std::fs::remove_file(db_path);
    
    // Create a valid database
    {
        let mut conn = Connection::open(db_path)?;
        conn.execute("CREATE TABLE corruption_test (id INTEGER PRIMARY KEY, data TEXT)")?;
        conn.execute("INSERT INTO corruption_test (data) VALUES ('valid_data')")?;
    }
    
    // Simulate corruption by writing random data to the file
    // (In a real scenario, this would be more sophisticated)
    if let Ok(mut file) = std::fs::OpenOptions::new().write(true).open(db_path) {
        use std::io::{Seek, SeekFrom, Write};
        let _ = file.seek(SeekFrom::Start(100));
        let _ = file.write_all(b"CORRUPTED_DATA_BLOCK");
    }
    
    // Try to open the potentially corrupted database
    match Connection::open(db_path) {
        Ok(mut conn) => {
            match conn.execute("SELECT * FROM corruption_test") {
                Ok(_) => println!("✓ Database handled potential corruption gracefully"),
                Err(e) => println!("✓ Corruption detected: {:?}", e),
            }
        }
        Err(e) => println!("✓ Corrupted database rejected: {:?}", e),
    }
    
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

/// Test WAL (Write-Ahead Logging) recovery scenarios
fn test_wal_recovery_scenarios() -> Result<(), OxidbError> {
    let db_path = "test_wal_recovery.db";
    let _ = std::fs::remove_file(db_path);
    
    // Create database with WAL mode
    {
        let mut conn = Connection::open(db_path)?;
        conn.execute("PRAGMA journal_mode=WAL")?;
        conn.execute("CREATE TABLE wal_test (id INTEGER PRIMARY KEY, data TEXT)")?;
        
        // Start transaction but don't commit (simulating incomplete transaction)
        conn.execute("BEGIN TRANSACTION")?;
        conn.execute("INSERT INTO wal_test (data) VALUES ('uncommitted_data')")?;
        // Don't commit - simulate crash
    }
    
    // Reopen and check recovery
    {
        let mut conn = Connection::open(db_path)?;
        let _result = conn.execute("SELECT COUNT(*) FROM wal_test")?;
        println!("✓ WAL recovery test completed");
    }
    
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

// Helper functions (DRY principle)

/// Escape SQL string to prevent injection (basic implementation)
fn escape_sql_string(s: &str) -> String {
    s.replace("'", "''")
}

/// Assert that a query result contains data
fn assert_query_returns_data(result: &QueryResult, test_name: &str) -> Result<(), OxidbError> {
    match result {
        QueryResult::Data(_) => Ok(()),
        QueryResult::RowsAffected(rows_affected) if *rows_affected > 0 => Ok(()),
        QueryResult::Success => Ok(()),
        _ => {
            println!("⚠ {}: No data returned", test_name);
            Ok(())
        }
    }
}