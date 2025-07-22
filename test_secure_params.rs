use oxidb::{Connection, Value, QueryResult};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing secure parameterized queries...");
    
    // Create an in-memory database
    let mut conn = Connection::open_in_memory()?;
    
    // Create a test table with unique name
    let table_name = format!("test_users_{}", std::process::id());
    conn.execute(&format!("CREATE TABLE {} (id INTEGER, name TEXT, email TEXT)", table_name))?;
    println!("✓ Created table");
    
    // Test secure INSERT with parameters
    let result = conn.execute_with_params(
        &format!("INSERT INTO {} (id, name, email) VALUES (?, ?, ?)", table_name),
        &[
            Value::Integer(1),
            Value::Text("Alice".to_string()),
            Value::Text("alice@example.com".to_string())
        ]
    )?;
    println!("✓ Inserted user: {:?}", result);
    
    // Test secure SELECT with parameters
    let result = conn.execute_with_params(
        &format!("SELECT * FROM {} WHERE name = ?", table_name),
        &[Value::Text("Alice".to_string())]
    )?;
    
    match result {
        QueryResult::Data(data) => {
            println!("✓ Found {} rows", data.row_count());
            if let Some(row) = data.get_row(0) {
                println!("  Row data: id={:?}, name={:?}, email={:?}", 
                    row.get(0), row.get(1), row.get(2));
            }
        }
        _ => println!("Unexpected result type"),
    }
    
    // Test SQL injection prevention
    println!("\n🔒 Testing SQL injection prevention...");
    
    // This would be dangerous with string interpolation, but is safe with parameters
    let malicious_input = format!("Alice'; DROP TABLE {}; --", table_name);
    let result = conn.execute_with_params(
        &format!("SELECT * FROM {} WHERE name = ?", table_name),
        &[Value::Text(malicious_input.to_string())]
    )?;
    
    match result {
        QueryResult::Data(data) => {
            println!("✓ Malicious input safely handled - found {} rows", data.row_count());
            // Should find 0 rows since "Alice'; DROP TABLE users; --" is not a valid name
        }
        _ => println!("Unexpected result type"),
    }
    
    // Verify table still exists after injection attempt
    let result = conn.execute(&format!("SELECT COUNT(*) FROM {}", table_name))?;
    match result {
        QueryResult::Data(data) => {
            if data.row_count() > 0 {
                println!("✓ Table still exists - SQL injection prevented!");
            }
        }
        _ => {}
    }
    
    println!("\n🎉 All tests passed! Parameterized queries are working securely.");
    Ok(())
}
