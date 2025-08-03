use oxidb::{Connection, OxidbError, QueryResult};

fn main() -> Result<(), OxidbError> {
    println!("=== Oxidb Connection API Demo ===\n");

    // Create an in-memory database connection
    let mut conn = Connection::open_in_memory()?;
    println!("✓ Opened in-memory database connection");

    // Create a table with explicit IDs (with unique name to avoid conflicts)
    let table_name = format!("demo_users_{}", std::process::id());
    let create_sql =
        format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", table_name);
    let result = conn.execute(&create_sql)?;
    println!("✓ Created table: {:?}", result);

    // Insert some data with explicit IDs
    conn.begin_transaction()?;
    println!("✓ Started transaction");

    let insert_sql = format!("INSERT INTO {} (id, name, age) VALUES (1, 'Alice', 30)", table_name);
    let result = conn.execute(&insert_sql)?;
    println!("✓ Inserted Alice: {:?}", result);

    let insert_sql = format!("INSERT INTO {} (id, name, age) VALUES (2, 'Bob', 25)", table_name);
    let result = conn.execute(&insert_sql)?;
    println!("✓ Inserted Bob: {:?}", result);

    let insert_sql =
        format!("INSERT INTO {} (id, name, age) VALUES (3, 'Charlie', 35)", table_name);
    let result = conn.execute(&insert_sql)?;
    println!("✓ Inserted Charlie: {:?}", result);

    conn.commit()?;
    println!("✓ Committed transaction");

    // Query the data
    let select_sql = format!("SELECT * FROM {}", table_name);
    let result = conn.execute(&select_sql)?;
    println!("✓ Query result: {:?}", result);

    match result {
        QueryResult::Data(data) => {
            println!("\nQuery Results:");
            println!("Columns: {:?}", data.columns());
            println!("Row count: {}", data.row_count());

            for (i, row) in data.rows().enumerate() {
                println!("Row {}: {:?}", i + 1, row);
            }
        }
        _ => println!("Unexpected result type"),
    }

    // Test rollback
    println!("\n--- Testing Rollback ---");
    conn.begin_transaction()?;

    let insert_sql = format!("INSERT INTO {} (id, name, age) VALUES (4, 'David', 40)", table_name);
    let result = conn.execute(&insert_sql)?;
    println!("✓ Inserted David (will be rolled back): {:?}", result);

    conn.rollback()?;
    println!("✓ Rolled back transaction");

    // Verify rollback worked - use simple SELECT * instead of COUNT(*)
    let verify_sql = format!("SELECT * FROM {}", table_name);
    let result = conn.execute(&verify_sql)?;
    println!("✓ Records after rollback: {:?}", result);
    
    match result {
        QueryResult::Data(data) => {
            println!("Record count after rollback: {}", data.row_count());
        }
        _ => println!("Unexpected result type for verification"),
    }

    // Test file-based database
    println!("\n--- Testing File-based Database ---");
    let db_name = format!("demo_{}.db", std::process::id());
    let mut file_conn = Connection::open(&db_name)?;
    println!("✓ Opened file-based database connection");

    let products_table = format!("products_{}", std::process::id());
    let create_sql =
        format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, name TEXT, price FLOAT)", products_table);
    file_conn.execute(&create_sql)?;

    let insert_sql =
        format!("INSERT INTO {} (id, name, price) VALUES (1, 'Laptop', 999.99)", products_table);
    file_conn.execute(&insert_sql)?;

    let insert_sql =
        format!("INSERT INTO {} (id, name, price) VALUES (2, 'Mouse', 29.99)", products_table);
    file_conn.execute(&insert_sql)?;

    let select_sql = format!("SELECT * FROM {}", products_table);
    let result = file_conn.execute(&select_sql)?;
    println!("✓ Products query: {:?}", result);

    // Persist to ensure data is saved
    file_conn.persist()?;
    println!("✓ Data persisted to disk");

    println!("\n=== Demo completed successfully! ===");
    Ok(())
}
