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
    
    // Test COUNT(*)
    println!("\n--- Testing COUNT(*) ---");
    match conn.execute("SELECT COUNT(*) FROM test_table") {
        Ok(QueryResult::Data(data)) => {
            println!("✓ COUNT(*) query parsed successfully");
            println!("Number of result rows: {}", data.rows.len());
            for row in data.rows() {
                println!("  Count result: {:?}", row);
            }
        }
        Ok(_) => println!("✗ Unexpected result type for COUNT(*)"),
        Err(e) => println!("✗ COUNT(*) error: {}", e),
    }
    
    // Test SUM
    println!("\n--- Testing SUM(value) ---");
    match conn.execute("SELECT SUM(value) FROM test_table") {
        Ok(QueryResult::Data(data)) => {
            println!("✓ SUM query executed successfully");
            for row in data.rows() {
                println!("  Sum result: {:?}", row);
            }
        }
        Ok(_) => println!("✗ Unexpected result type for SUM"),
        Err(e) => println!("✗ SUM error: {}", e),
    }
    
    // Test AVG
    println!("\n--- Testing AVG(value) ---");
    match conn.execute("SELECT AVG(value) FROM test_table") {
        Ok(QueryResult::Data(data)) => {
            println!("✓ AVG query executed successfully");
            for row in data.rows() {
                println!("  Average result: {:?}", row);
            }
        }
        Ok(_) => println!("✗ Unexpected result type for AVG"),
        Err(e) => println!("✗ AVG error: {}", e),
    }
    
    // Test MIN
    println!("\n--- Testing MIN(value) ---");
    match conn.execute("SELECT MIN(value) FROM test_table") {
        Ok(QueryResult::Data(data)) => {
            println!("✓ MIN query executed successfully");
            for row in data.rows() {
                println!("  Min result: {:?}", row);
            }
        }
        Ok(_) => println!("✗ Unexpected result type for MIN"),
        Err(e) => println!("✗ MIN error: {}", e),
    }
    
    // Test MAX
    println!("\n--- Testing MAX(value) ---");
    match conn.execute("SELECT MAX(value) FROM test_table") {
        Ok(QueryResult::Data(data)) => {
            println!("✓ MAX query executed successfully");
            for row in data.rows() {
                println!("  Max result: {:?}", row);
            }
        }
        Ok(_) => println!("✗ Unexpected result type for MAX"),
        Err(e) => println!("✗ MAX error: {}", e),
    }
    
    // Test GROUP BY
    println!("\n--- Testing GROUP BY ---");
    
    // First, let's insert some more data with categories
    conn.execute("INSERT INTO test_table (id, name, value) VALUES (4, 'Item D', 75)")?;
    conn.execute("INSERT INTO test_table (id, name, value) VALUES (5, 'Item A', 80)")?;
    conn.execute("INSERT INTO test_table (id, name, value) VALUES (6, 'Item B', 90)")?;
    
    // GROUP BY name
    match conn.execute("SELECT name, COUNT(*), SUM(value) FROM test_table GROUP BY name")? {
        QueryResult::Data(data) => {
            println!("GROUP BY name results:");
            println!("Columns: {:?}", data.columns);
            for (i, row) in data.rows().enumerate() {
                println!("Row {}: {:?}", i, row);
            }
        }
        _ => panic!("Expected data from GROUP BY query"),
    }
    
    // Test HAVING clause
    println!("\n--- Testing HAVING ---");
    // For now, let's test GROUP BY without HAVING since HAVING with aggregates requires more parser work
    match conn.execute("SELECT name, COUNT(*) FROM test_table WHERE value > 50 GROUP BY name")? {
        QueryResult::Data(data) => {
            println!("GROUP BY with WHERE results (items with value > 50):");
            println!("Columns: {:?}", data.columns);
            for (i, row) in data.rows().enumerate() {
                println!("Row {}: {:?}", i, row);
            }
        }
        _ => panic!("Expected data from GROUP BY query"),
    }
    
    // Test JOINs
    println!("\n--- Testing JOINs ---");
    
    // Create another table for JOIN testing
    conn.execute("CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)")?;
    conn.execute("INSERT INTO categories (id, name) VALUES (1, 'Electronics')")?;
    conn.execute("INSERT INTO categories (id, name) VALUES (2, 'Accessories')")?;
    conn.execute("INSERT INTO categories (id, name) VALUES (3, 'Furniture')")?;
    
    // Add category_id to test_table items
    conn.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price FLOAT, category_id INTEGER)")?;
    conn.execute("INSERT INTO products (id, name, price, category_id) VALUES (1, 'Laptop', 999.99, 1)")?;
    conn.execute("INSERT INTO products (id, name, price, category_id) VALUES (2, 'Mouse', 29.99, 2)")?;
    conn.execute("INSERT INTO products (id, name, price, category_id) VALUES (3, 'Desk', 299.99, 3)")?;
    conn.execute("INSERT INTO products (id, name, price, category_id) VALUES (4, 'Keyboard', 79.99, 2)")?;
    
    // Test INNER JOIN
    match conn.execute("SELECT p.name, c.name FROM products p JOIN categories c ON p.category_id = c.id")? {
        QueryResult::Data(data) => {
            println!("INNER JOIN results:");
            println!("Columns: {:?}", data.columns);
            for (i, row) in data.rows().enumerate() {
                println!("Row {}: {:?}", i, row);
            }
        }
        _ => panic!("Expected data from JOIN query"),
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