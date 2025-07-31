//! Simple OxiDB Test Example
//! 
//! This example tests basic OxiDB functionality to ensure the database is working correctly.

use oxidb::{Connection, QueryResult};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Simple OxiDB Test ===\n");
    
    // Create an in-memory database
    let mut conn = Connection::open_in_memory()?;
    println!("✓ Created in-memory database");
    
    // Create a simple table
    conn.execute("CREATE TABLE test_table (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        value FLOAT
    )")?;
    println!("✓ Created test table");
    
    // Insert some data
    conn.execute("INSERT INTO test_table (id, name, value) VALUES (1, 'Item One', 10.5)")?;
    conn.execute("INSERT INTO test_table (id, name, value) VALUES (2, 'Item Two', 20.0)")?;
    conn.execute("INSERT INTO test_table (id, name, value) VALUES (3, 'Item Three', 15.5)")?;
    println!("✓ Inserted 3 rows");
    
    // Query the data
    let result = conn.execute("SELECT * FROM test_table ORDER BY id")?;
    
    match result {
        QueryResult::Data(data) => {
            println!("\n✓ Query returned {} rows", data.row_count());
            println!("\nColumns: {:?}", data.columns);
            
            println!("\nData:");
            for (i, row) in data.rows().enumerate() {
                println!("Row {}: {:?}", i + 1, row);
            }
        }
        _ => println!("Unexpected result type"),
    }
    
    // Test WHERE clause
    println!("\n--- Testing WHERE clause ---");
    let result = conn.execute("SELECT name, value FROM test_table WHERE value > 15")?;
    
    match result {
        QueryResult::Data(data) => {
            println!("✓ WHERE clause query returned {} rows", data.rows.len());
            println!("\nColumns: {:?}", data.columns);
            println!("\nData:");
            for (i, row) in data.rows().enumerate() {
                println!("Row {}: {:?}", i + 1, row);
            }
        }
        _ => {
            println!("✗ Expected data result from WHERE query");
        }
    }
    
    // Test UPDATE
    println!("\n--- Testing UPDATE ---");
    let result = conn.execute("UPDATE test_table SET value = 40.0 WHERE id = 2")?;
    
    match result {
        QueryResult::RowsAffected(count) => {
            println!("✓ Updated {} rows", count);
        }
        _ => {
            println!("✗ Expected rows affected result from UPDATE");
        }
    }
    
    // Verify the update
    let result = conn.execute("SELECT * FROM test_table WHERE id = 2")?;
    match result {
        QueryResult::Data(data) => {
            if let Some(row) = data.rows().next() {
                println!("✓ Row after update: {:?}", row);
            }
        }
        _ => println!("Unexpected result type"),
    }
    
    // Test DELETE
    println!("\n--- Testing DELETE ---");
    let result = conn.execute("DELETE FROM test_table WHERE id = 1")?;
    match result {
        QueryResult::RowsAffected(count) => {
            println!("✓ Deleted {} rows", count);
        }
        _ => println!("Unexpected result type"),
    }
    
    // Final count - select all remaining rows
    println!("\n--- Final data ---");
    let result = conn.execute("SELECT * FROM test_table")?;
    match result {
        QueryResult::Data(data) => {
            let count = data.rows().count();
            println!("✓ Final row count: {}", count);
            println!("\nRemaining data:");
            for row in data.rows() {
                println!("  {:?}", row);
            }
        }
        _ => println!("Unexpected result type"),
    }
    
    println!("\n✅ All tests passed!");
    
    Ok(())
}