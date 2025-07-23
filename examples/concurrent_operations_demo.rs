use oxidb::{Connection, OxidbError, QueryResult};
use std::thread;
use std::time::Duration;

fn main() -> Result<(), OxidbError> {
    println!("=== OxiDB Concurrent Operations Demo ===\n");

    // Test 1: Sequential Operations (baseline)
    println!("--- Test 1: Sequential Operations Baseline ---");
    test_sequential_operations()?;
    
    // Test 2: Simulated Concurrent Read Operations
    println!("\n--- Test 2: Simulated Concurrent Read Operations ---");
    test_simulated_concurrent_reads()?;
    
    // Test 3: Simulated Concurrent Write Operations
    println!("\n--- Test 3: Simulated Concurrent Write Operations ---");
    test_simulated_concurrent_writes()?;
    
    // Test 4: Mixed Read/Write Operations
    println!("\n--- Test 4: Mixed Read/Write Operations ---");
    test_mixed_operations()?;

    println!("\n🎉 All concurrent operations tests completed! 🎉");
    Ok(())
}

fn test_sequential_operations() -> Result<(), OxidbError> {
    let mut conn = Connection::open_in_memory()?;
    let process_id = std::process::id();
    
    // Create table
    let table_name = format!("sequential_{}", process_id);
    let create_sql = format!(
        "CREATE TABLE {} (id INTEGER PRIMARY KEY, value TEXT, timestamp TEXT)",
        table_name
    );
    conn.execute(&create_sql)?;
    println!("✓ Created table for sequential operations");

    // Perform 100 sequential operations
    let start = std::time::Instant::now();
    for i in 1..=100 {
        let insert_sql = format!(
            "INSERT INTO {} (id, value, timestamp) VALUES ({}, 'value_{}', '2024-01-01')",
            table_name, i, i
        );
        conn.execute(&insert_sql)?;
    }
    let duration = start.elapsed();
    
    println!("✓ Completed 100 sequential inserts in {:?}", duration);
    
    // Verify count
    let count_sql = format!("SELECT * FROM {}", table_name);
    let result = conn.execute(&count_sql)?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Verified {} records in table", data.row_count());
        }
        _ => return Err(OxidbError::Other("Expected data result".to_string())),
    }

    Ok(())
}

fn test_simulated_concurrent_reads() -> Result<(), OxidbError> {
    let mut conn = Connection::open_in_memory()?;
    let process_id = std::process::id();
    
    // Setup test data
    let table_name = format!("concurrent_reads_{}", process_id);
    let create_sql = format!(
        "CREATE TABLE {} (id INTEGER PRIMARY KEY, data TEXT)",
        table_name
    );
    conn.execute(&create_sql)?;
    
    // Insert test data
    for i in 1..=50 {
        let insert_sql = format!(
            "INSERT INTO {} (id, data) VALUES ({}, 'test_data_{}')",
            table_name, i, i
        );
        conn.execute(&insert_sql)?;
    }
    println!("✓ Setup test data with 50 records");

    // Simulate concurrent reads by performing rapid sequential reads
    let start = std::time::Instant::now();
    let mut total_rows = 0;
    
    for i in 1..=20 {
        let select_sql = format!("SELECT * FROM {} WHERE id <= {}", table_name, i * 2);
        let result = conn.execute(&select_sql)?;
        match result {
            QueryResult::Data(data) => {
                total_rows += data.row_count();
            }
            _ => return Err(OxidbError::Other("Expected data result".to_string())),
        }
    }
    
    let duration = start.elapsed();
    println!("✓ Completed 20 read operations, total rows read: {} in {:?}", total_rows, duration);

    Ok(())
}

fn test_simulated_concurrent_writes() -> Result<(), OxidbError> {
    let mut conn = Connection::open_in_memory()?;
    let process_id = std::process::id();
    
    // Create table
    let table_name = format!("concurrent_writes_{}", process_id);
    let create_sql = format!(
        "CREATE TABLE {} (id INTEGER PRIMARY KEY, batch_id INTEGER, data TEXT)",
        table_name
    );
    conn.execute(&create_sql)?;
    println!("✓ Created table for concurrent write simulation");

    // Simulate concurrent writes with batched transactions
    let start = std::time::Instant::now();
    let mut total_inserts = 0;
    
    for batch in 1..=5 {
        conn.begin_transaction()?;
        
        for i in 1..=10 {
            let id = (batch - 1) * 10 + i;
            let insert_sql = format!(
                "INSERT INTO {} (id, batch_id, data) VALUES ({}, {}, 'batch_{}_item_{}')",
                table_name, id, batch, batch, i
            );
            conn.execute(&insert_sql)?;
            total_inserts += 1;
        }
        
        conn.commit()?;
        println!("✓ Completed batch {} with 10 inserts", batch);
        
        // Small delay to simulate processing time
        thread::sleep(Duration::from_millis(10));
    }
    
    let duration = start.elapsed();
    println!("✓ Completed {} inserts in {} batches in {:?}", total_inserts, 5, duration);

    // Verify final count
    let count_sql = format!("SELECT * FROM {}", table_name);
    let result = conn.execute(&count_sql)?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Verified {} total records", data.row_count());
        }
        _ => return Err(OxidbError::Other("Expected data result".to_string())),
    }

    Ok(())
}

fn test_mixed_operations() -> Result<(), OxidbError> {
    let mut conn = Connection::open_in_memory()?;
    let process_id = std::process::id();
    
    // Create table
    let table_name = format!("mixed_ops_{}", process_id);
    let create_sql = format!(
        "CREATE TABLE {} (id INTEGER PRIMARY KEY, counter INTEGER, status TEXT)",
        table_name
    );
    conn.execute(&create_sql)?;
    println!("✓ Created table for mixed operations");

    // Initialize some data
    for i in 1..=10 {
        let insert_sql = format!(
            "INSERT INTO {} (id, counter, status) VALUES ({}, 0, 'active')",
            table_name, i
        );
        conn.execute(&insert_sql)?;
    }
    println!("✓ Initialized 10 records");

    // Simulate mixed read/write operations
    let start = std::time::Instant::now();
    let mut operations = 0;
    
    for round in 1..=10 {
        // Read operation
        let select_sql = format!("SELECT * FROM {} WHERE counter < {}", table_name, round * 2);
        let result = conn.execute(&select_sql)?;
        match result {
            QueryResult::Data(data) => {
                operations += 1;
                if round % 3 == 0 {
                    println!("✓ Round {}: Read {} records", round, data.row_count());
                }
            }
            _ => return Err(OxidbError::Other("Expected data result".to_string())),
        }
        
        // Write operation (update)
        let update_sql = format!(
            "UPDATE {} SET counter = counter + 1 WHERE id <= {}",
            table_name, round
        );
        conn.execute(&update_sql)?;
        operations += 1;
        
        // Conditional write operation
        if round % 3 == 0 {
            let update_sql = format!(
                "UPDATE {} SET status = 'updated' WHERE counter >= {}",
                table_name, round
            );
            conn.execute(&update_sql)?;
            operations += 1;
        }
    }
    
    let duration = start.elapsed();
    println!("✓ Completed {} mixed operations in {:?}", operations, duration);

    // Final verification
    let final_sql = format!("SELECT * FROM {}", table_name);
    let result = conn.execute(&final_sql)?;
    match result {
        QueryResult::Data(data) => {
            println!("✓ Final state: {} records", data.row_count());
        }
        _ => return Err(OxidbError::Other("Expected data result".to_string())),
    }

    Ok(())
}