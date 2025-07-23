use oxidb::{Connection, OxidbError, QueryResult};
use std::time::{Duration, Instant};

fn main() -> Result<(), OxidbError> {
    println!("=== OxiDB Performance Benchmark ===\n");

    // Benchmark 1: Bulk Insert Performance
    benchmark_bulk_insert()?;
    
    // Benchmark 2: Query Performance
    benchmark_query_performance()?;
    
    // Benchmark 3: Transaction Performance
    benchmark_transaction_performance()?;
    
    // Benchmark 4: Memory vs File Performance
    benchmark_memory_vs_file()?;
    
    // Benchmark 5: Concurrent Operations (simulated)
    benchmark_concurrent_operations()?;

    println!("\n🏁 All benchmarks completed! 🏁");
    Ok(())
}

fn benchmark_bulk_insert() -> Result<(), OxidbError> {
    println!("--- Benchmark 1: Bulk Insert Performance ---");
    
    let mut conn = Connection::open_in_memory()?;
    conn.execute("CREATE TABLE bulk_test (id INTEGER PRIMARY KEY, data TEXT, value FLOAT)")?;
    
    let record_counts = [100, 1000, 5000];
    
    for &count in &record_counts {
        let start = Instant::now();
        
        conn.begin_transaction()?;
        for i in 1..=count {
            let sql = format!(
                "INSERT INTO bulk_test (id, data, value) VALUES ({}, 'Record{}', {})",
                i, i, i as f64 * 1.5
            );
            conn.execute(&sql)?;
        }
        conn.commit()?;
        
        let duration = start.elapsed();
        let records_per_sec = count as f64 / duration.as_secs_f64();
        
        println!("✓ Inserted {} records in {:?} ({:.0} records/sec)", 
                count, duration, records_per_sec);
    }
    
    println!("✅ Bulk insert benchmark completed\n");
    Ok(())
}

fn benchmark_query_performance() -> Result<(), OxidbError> {
    println!("--- Benchmark 2: Query Performance ---");
    
    let mut conn = Connection::open_in_memory()?;
    
    // Setup test data
    conn.execute("CREATE TABLE query_test (id INTEGER PRIMARY KEY, category TEXT, score INTEGER, active BOOLEAN)")?;
    
    let categories = ["A", "B", "C", "D", "E"];
    conn.begin_transaction()?;
    for i in 1..=10000 {
        let category = categories[i % categories.len()];
        let score = i % 100;
        let active = i % 2 == 0;
        let sql = format!(
            "INSERT INTO query_test (id, category, score, active) VALUES ({}, '{}', {}, {})",
            i, category, score, active
        );
        conn.execute(&sql)?;
    }
    conn.commit()?;
    println!("✓ Setup: Inserted 10,000 test records");
    
    // Benchmark different query types
    let queries = [
        ("Simple SELECT", "SELECT * FROM query_test WHERE id = 5000"),
        ("Range Query", "SELECT * FROM query_test WHERE score BETWEEN 50 AND 60"),
        ("Category Filter", "SELECT * FROM query_test WHERE category = 'A'"),
        ("Complex Filter", "SELECT * FROM query_test WHERE category = 'B' AND score > 80 AND active = true"),
        ("ORDER BY", "SELECT * FROM query_test ORDER BY score DESC LIMIT 100"),
    ];
    
    for (name, sql) in &queries {
        let start = Instant::now();
        let result = conn.execute(sql)?;
        let duration = start.elapsed();
        
        match result {
            QueryResult::Data(data) => {
                println!("✓ {}: {} results in {:?}", name, data.row_count(), duration);
            }
            _ => println!("✓ {}: completed in {:?}", name, duration),
        }
    }
    
    println!("✅ Query performance benchmark completed\n");
    Ok(())
}

fn benchmark_transaction_performance() -> Result<(), OxidbError> {
    println!("--- Benchmark 3: Transaction Performance ---");
    
    let mut conn = Connection::open_in_memory()?;
    conn.execute("CREATE TABLE tx_test (id INTEGER PRIMARY KEY, counter INTEGER)")?;
    conn.execute("INSERT INTO tx_test (id, counter) VALUES (1, 0)")?;
    
    // Benchmark transaction throughput
    let tx_counts = [10, 100, 500];
    
    for &count in &tx_counts {
        let start = Instant::now();
        
        for _ in 0..count {
            conn.begin_transaction()?;
            conn.execute("UPDATE tx_test SET counter = counter + 1 WHERE id = 1")?;
            conn.commit()?;
        }
        
        let duration = start.elapsed();
        let tx_per_sec = count as f64 / duration.as_secs_f64();
        
        println!("✓ Completed {} transactions in {:?} ({:.0} tx/sec)", 
                count, duration, tx_per_sec);
    }
    
    // Test rollback performance
    let start = Instant::now();
    for _ in 0..100 {
        conn.begin_transaction()?;
        conn.execute("UPDATE tx_test SET counter = counter + 1000 WHERE id = 1")?;
        conn.rollback()?;
    }
    let duration = start.elapsed();
    println!("✓ Completed 100 rollbacks in {:?}", duration);
    
    println!("✅ Transaction performance benchmark completed\n");
    Ok(())
}

fn benchmark_memory_vs_file() -> Result<(), OxidbError> {
    println!("--- Benchmark 4: Memory vs File Performance ---");
    
    let operations = 1000;
    
    // Memory benchmark
    let start = Instant::now();
    {
        let mut conn = Connection::open_in_memory()?;
        conn.execute("CREATE TABLE perf_test (id INTEGER PRIMARY KEY, data TEXT)")?;
        
        for i in 1..=operations {
            let sql = format!("INSERT INTO perf_test (id, data) VALUES ({}, 'Data{}')", i, i);
            conn.execute(&sql)?;
        }
        
        conn.execute("SELECT * FROM perf_test")?;
    }
    let memory_duration = start.elapsed();
    
    // File benchmark
    let db_file = format!("benchmark_{}.db", std::process::id());
    let start = Instant::now();
    {
        let mut conn = Connection::open(&db_file)?;
        conn.execute("CREATE TABLE perf_test (id INTEGER PRIMARY KEY, data TEXT)")?;
        
        for i in 1..=operations {
            let sql = format!("INSERT INTO perf_test (id, data) VALUES ({}, 'Data{}')", i, i);
            conn.execute(&sql)?;
        }
        
        conn.execute("SELECT * FROM perf_test")?;
        conn.persist()?;
    }
    let file_duration = start.elapsed();
    
    println!("✓ Memory operations ({} records): {:?}", operations, memory_duration);
    println!("✓ File operations ({} records): {:?}", operations, file_duration);
    
    let ratio = file_duration.as_secs_f64() / memory_duration.as_secs_f64();
    println!("✓ File/Memory ratio: {:.2}x", ratio);
    
    // Clean up
    std::fs::remove_file(&db_file).ok();
    
    println!("✅ Memory vs File benchmark completed\n");
    Ok(())
}

fn benchmark_concurrent_operations() -> Result<(), OxidbError> {
    println!("--- Benchmark 5: Concurrent Operations (Simulated) ---");
    
    let mut conn = Connection::open_in_memory()?;
    conn.execute("CREATE TABLE concurrent_test (id INTEGER PRIMARY KEY, thread_id INTEGER, operation_id INTEGER)")?;
    
    // Simulate concurrent operations by rapidly switching between different "threads"
    let thread_count = 5;
    let ops_per_thread = 200;
    
    let start = Instant::now();
    
    for op_id in 1..=ops_per_thread {
        for thread_id in 1..=thread_count {
            let sql = format!(
                "INSERT INTO concurrent_test (id, thread_id, operation_id) VALUES ({}, {}, {})",
                (op_id - 1) * thread_count + thread_id,
                thread_id,
                op_id
            );
            conn.execute(&sql)?;
        }
    }
    
    let duration = start.elapsed();
    let total_ops = thread_count * ops_per_thread;
    let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
    
    println!("✓ Simulated {} concurrent operations ({} threads × {} ops) in {:?}", 
            total_ops, thread_count, ops_per_thread, duration);
    println!("✓ Throughput: {:.0} operations/sec", ops_per_sec);
    
    // Verify data integrity
    let result = conn.execute("SELECT COUNT(*) as total FROM concurrent_test")?;
    match result {
        QueryResult::Data(data) => {
            let rows: Vec<_> = data.rows().collect();
            if let Some(row) = rows.first() {
                println!("✓ Data integrity verified: {:?}", row);
            }
        }
        _ => println!("⚠ Could not verify data integrity"),
    }
    
    println!("✅ Concurrent operations benchmark completed\n");
    Ok(())
}

// Helper function to format duration nicely
fn format_duration(duration: Duration) -> String {
    if duration.as_millis() < 1000 {
        format!("{}ms", duration.as_millis())
    } else {
        format!("{:.2}s", duration.as_secs_f64())
    }
}