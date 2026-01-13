//! Performance Monitoring Demonstration for Oxidb
//!
//! This example demonstrates the comprehensive performance monitoring capabilities
//! of Oxidb, showing how to track query performance, analyze bottlenecks, and
//! get optimization recommendations.

use oxidb::{Connection, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Oxidb Performance Monitoring Demonstration");
    println!("============================================");

    // Create an in-memory database connection
    let mut conn = Connection::open_in_memory()?;

    // Create a sample table
    println!("\n📊 Setting up sample data...");
    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)",
    )?;
    conn.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT, amount FLOAT)",
    )?;

    // Insert sample data to simulate various workloads
    println!("📝 Inserting sample data...");

    // Simulate a batch insert workload
    conn.begin_transaction()?;
    for i in 1..=100 {
        let params = [
            Value::Integer(i),
            Value::Text(format!("User{}", i)),
            Value::Text(format!("user{}@example.com", i)),
            Value::Integer(20 + (i % 50)),
        ];
        conn.execute_with_params(
            "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)",
            &params,
        )?;
    }
    conn.commit()?;

    // Simulate various query patterns
    println!("🔍 Executing various query patterns...");

    // Simple SELECT queries
    for _ in 0..10 {
        conn.execute("SELECT * FROM users WHERE age > 30")?;
    }

    // More complex queries with JOINs (simulated)
    for i in 1..=50 {
        let params = [
            Value::Integer(i),
            Value::Integer((i % 100) + 1),
            Value::Text(format!("Product{}", i % 10)),
            Value::Float(19.99 + (i as f64 * 0.5)),
        ];
        conn.execute_with_params(
            "INSERT INTO orders (id, user_id, product, amount) VALUES (?, ?, ?, ?)",
            &params,
        )?;
    }

    // Simulate some UPDATE operations
    for i in 1..=10 {
        let params = [Value::Integer(i + 30), Value::Integer(i)];
        conn.execute_with_params("UPDATE users SET age = ? WHERE id = ?", &params)?;
    }

    // Simulate some DELETE operations
    conn.execute("DELETE FROM users WHERE age > 65")?;

    // Generate and display performance report
    println!("\n�� Generating Performance Report...");
    println!("===================================");

    let report = conn.get_performance_report()?;
    println!("{}", report);

    // Demonstrate performance tracking over time
    println!("\n⏱️  Performance Tracking Example:");
    println!("  Executing a series of queries to show performance variation...");

    let queries = [
        "SELECT COUNT(*) FROM users",
        "SELECT * FROM users WHERE age BETWEEN 25 AND 35",
        "SELECT name, email FROM users ORDER BY name",
        "SELECT AVG(age) FROM users",
        "SELECT * FROM orders WHERE amount > 50.0",
    ];

    for (i, query) in queries.iter().enumerate() {
        let start = std::time::Instant::now();
        let result = conn.execute(query)?;
        let duration = start.elapsed();

        match result {
            oxidb::QueryResult::Data(data) => {
                println!("  Query {}: {:?} - {} rows returned", i + 1, duration, data.row_count());
            }
            oxidb::QueryResult::RowsAffected(count) => {
                println!("  Query {}: {:?} - {} rows affected", i + 1, duration, count);
            }
            _ => {
                println!("  Query {}: {:?} - operation completed", i + 1, duration);
            }
        }
    }

    // Final performance report
    println!("\n📊 Final Performance Summary:");
    let final_report = conn.get_performance_report()?;
    println!("{}", final_report);

    println!("\n✅ Performance monitoring demonstration completed!");
    println!("   This shows how Oxidb provides comprehensive performance insights");
    println!("   for production database monitoring and optimization.");

    Ok(())
}
