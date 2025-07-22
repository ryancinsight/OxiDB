//! Performance Monitoring Demonstration for OxiDB
//!
//! This example demonstrates the comprehensive performance monitoring capabilities
//! of OxiDB, showing how to track query performance, analyze bottlenecks, and
//! get optimization recommendations.

use oxidb::{Connection, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 OxiDB Performance Monitoring Demonstration");
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

    // Display query performance metrics
    println!("\n🔍 Query Performance Analysis:");
    println!("  • Total Queries Executed: {}", report.query_analysis.total_queries);
    println!("  • Average Execution Time: {:?}", report.query_analysis.average_execution_time);
    println!("  • Fastest Query: {:?}", report.query_analysis.fastest_query_time);
    println!("  • Slowest Query: {:?}", report.query_analysis.slowest_query_time);
    println!("  • Queries Per Second: {:.2}", report.query_analysis.queries_per_second);
    println!(
        "  • Slow Queries Detected: {}",
        if report.query_analysis.slow_queries_detected { "Yes" } else { "No" }
    );

    // Display transaction performance metrics
    println!("\n💼 Transaction Performance Analysis:");
    println!("  • Total Transactions: {}", report.transaction_analysis.total_transactions);
    println!("  • Average Duration: {:?}", report.transaction_analysis.average_duration);
    println!("  • Commit Rate: {:.1}%", report.transaction_analysis.commit_rate * 100.0);
    println!("  • Abort Rate: {:.1}%", report.transaction_analysis.abort_rate * 100.0);

    // Display storage performance metrics
    println!("\n💾 Storage Performance Analysis:");
    println!("  • Total Bytes Read: {} bytes", report.storage_analysis.total_bytes_read);
    println!("  • Total Bytes Written: {} bytes", report.storage_analysis.total_bytes_written);
    println!("  • Total I/O Operations: {}", report.storage_analysis.total_io_operations);
    println!("  • Average I/O Duration: {:?}", report.storage_analysis.average_io_duration);
    println!("  • Read/Write Ratio: {:.2}", report.storage_analysis.read_write_ratio);

    // Display bottleneck analysis
    println!("\n⚠️  Bottleneck Analysis:");
    println!("  • Severity Level: {:?}", report.bottlenecks.severity);
    if report.bottlenecks.bottlenecks.is_empty() {
        println!("  • No significant bottlenecks detected! 🎉");
    } else {
        for bottleneck in &report.bottlenecks.bottlenecks {
            println!("  • {}", bottleneck);
        }
    }

    // Display optimization recommendations
    println!("\n💡 Optimization Recommendations:");
    for (i, recommendation) in report.recommendations.iter().enumerate() {
        println!("  {}. {}", i + 1, recommendation);
    }

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
    println!("  • Total Queries: {}", final_report.query_analysis.total_queries);
    println!("  • Average Performance: {:?}", final_report.query_analysis.average_execution_time);

    println!("\n✅ Performance monitoring demonstration completed!");
    println!("   This shows how OxiDB provides comprehensive performance insights");
    println!("   for production database monitoring and optimization.");

    Ok(())
}
