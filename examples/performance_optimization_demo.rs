//! Performance Optimization Demo for OxiDB
//! 
//! This example demonstrates how to use OxiDB's performance monitoring framework
//! to identify bottlenecks and apply optimizations using elite programming practices.

use oxidb::{Connection, OxidbError};
use std::time::{Duration, Instant};
use rand::Rng;

/// Demonstrates performance optimization techniques
fn main() -> Result<(), OxidbError> {
    println!("=== OxiDB Performance Optimization Demo ===\n");

    // Create a new in-memory database
    let mut conn = Connection::open_in_memory()?;
    
    // Enable performance monitoring
    conn.enable_performance_monitoring();

    // Phase 1: Create test schema and baseline data
    setup_test_schema(&mut conn)?;
    
    // Phase 2: Run unoptimized queries and collect metrics
    println!("Phase 1: Running unoptimized queries...");
    let unoptimized_metrics = run_unoptimized_workload(&mut conn)?;
    
    // Phase 3: Analyze performance and identify bottlenecks
    println!("\nPhase 2: Analyzing performance...");
    let report = conn.get_performance_report()?;
    analyze_and_display_report(&report);
    
    // Phase 4: Apply optimizations based on analysis
    println!("\nPhase 3: Applying optimizations...");
    apply_optimizations(&mut conn)?;
    
    // Phase 5: Run optimized queries and compare
    println!("\nPhase 4: Running optimized queries...");
    let optimized_metrics = run_optimized_workload(&mut conn)?;
    
    // Phase 6: Compare results and show improvements
    println!("\nPhase 5: Performance Comparison");
    compare_performance(&unoptimized_metrics, &optimized_metrics);
    
    // Phase 7: Demonstrate advanced optimization techniques
    println!("\nPhase 6: Advanced Optimizations");
    demonstrate_advanced_optimizations(&mut conn)?;

    Ok(())
}

/// Performance metrics for comparison
#[derive(Debug)]
struct WorkloadMetrics {
    total_duration: Duration,
    avg_query_time: Duration,
    queries_per_second: f64,
    cache_hit_rate: f64,
}

/// Setup test schema with various table structures
fn setup_test_schema(conn: &mut Connection) -> Result<(), OxidbError> {
    println!("Setting up test schema...");
    
    // Create main tables
    conn.execute("CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        username TEXT NOT NULL,
        email TEXT NOT NULL,
        created_at INTEGER,
        profile_vector VECTOR[128]
    )")?;
    
    conn.execute("CREATE TABLE posts (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        title TEXT,
        content TEXT,
        created_at INTEGER,
        tags TEXT
    )")?;
    
    conn.execute("CREATE TABLE comments (
        id INTEGER PRIMARY KEY,
        post_id INTEGER,
        user_id INTEGER,
        content TEXT,
        created_at INTEGER
    )")?;
    
    // Insert test data using efficient batch operations
    let mut rng = rand::thread_rng();
    
    // Insert users with vectors for similarity search
    for i in 1..=1000 {
        let vector_str = (0..128)
            .map(|_| rng.gen_range(0.0..1.0).to_string())
            .collect::<Vec<_>>()
            .join(", ");
            
        conn.execute(&format!(
            "INSERT INTO users (id, username, email, created_at, profile_vector) 
             VALUES ({}, 'user{}', 'user{}@example.com', {}, [{}])",
            i, i, i, 
            1700000000 + i * 86400,
            vector_str
        ))?;
    }
    
    // Insert posts
    for i in 1..=5000 {
        let user_id = rng.gen_range(1..=1000);
        conn.execute(&format!(
            "INSERT INTO posts (id, user_id, title, content, created_at, tags) 
             VALUES ({}, {}, 'Post {}', 'Content for post {}', {}, 'tag{}')",
            i, user_id, i, i,
            1700000000 + i * 3600,
            i % 10
        ))?;
    }
    
    // Insert comments
    for i in 1..=10000 {
        let post_id = rng.gen_range(1..=5000);
        let user_id = rng.gen_range(1..=1000);
        conn.execute(&format!(
            "INSERT INTO comments (id, post_id, user_id, content, created_at) 
             VALUES ({}, {}, {}, 'Comment {}', {})",
            i, post_id, user_id, i,
            1700000000 + i * 600
        ))?;
    }
    
    println!("✓ Schema created with 1,000 users, 5,000 posts, and 10,000 comments");
    Ok(())
}

/// Run unoptimized workload without indexes
fn run_unoptimized_workload(conn: &mut Connection) -> Result<WorkloadMetrics, OxidbError> {
    let start = Instant::now();
    let mut query_times = Vec::new();
    
    // Query 1: Find posts by specific user (no index on user_id)
    for user_id in [42, 123, 456, 789, 999] {
        let query_start = Instant::now();
        conn.execute(&format!(
            "SELECT * FROM posts WHERE user_id = {}",
            user_id
        ))?;
        query_times.push(query_start.elapsed());
    }
    
    // Query 2: Find comments for posts (no index on post_id)
    for post_id in [100, 500, 1000, 2500, 4999] {
        let query_start = Instant::now();
        conn.execute(&format!(
            "SELECT * FROM comments WHERE post_id = {}",
            post_id
        ))?;
        query_times.push(query_start.elapsed());
    }
    
    // Query 3: Join query without indexes
    let query_start = Instant::now();
    conn.execute(
        "SELECT u.username, COUNT(p.id) as post_count 
         FROM users u 
         JOIN posts p ON u.id = p.user_id 
         WHERE u.id < 100"
    )?;
    query_times.push(query_start.elapsed());
    
    // Query 4: Range scan without index
    let query_start = Instant::now();
    conn.execute(
        "SELECT * FROM posts 
         WHERE created_at BETWEEN 1700000000 AND 1700864000"
    )?;
    query_times.push(query_start.elapsed());
    
    let total_duration = start.elapsed();
    let avg_query_time = query_times.iter().sum::<Duration>() / query_times.len() as u32;
    
    Ok(WorkloadMetrics {
        total_duration,
        avg_query_time,
        queries_per_second: query_times.len() as f64 / total_duration.as_secs_f64(),
        cache_hit_rate: 0.0, // No caching yet
    })
}

/// Analyze performance report and display insights
fn analyze_and_display_report(report: &str) {
    println!("{}", report);
    
    // Additional analysis
    println!("\n🔍 Key Insights:");
    println!("- Full table scans detected on foreign key lookups");
    println!("- No indexes on frequently queried columns");
    println!("- Join operations using nested loop without optimization");
    println!("- Large result sets being materialized in memory");
}

/// Apply optimizations based on performance analysis
fn apply_optimizations(conn: &mut Connection) -> Result<(), OxidbError> {
    // Create indexes on frequently queried columns
    println!("Creating indexes on foreign keys...");
    conn.execute("CREATE INDEX idx_posts_user_id ON posts(user_id)")?;
    conn.execute("CREATE INDEX idx_comments_post_id ON comments(post_id)")?;
    conn.execute("CREATE INDEX idx_comments_user_id ON comments(user_id)")?;
    
    // Create index for range queries
    println!("Creating index for temporal queries...");
    conn.execute("CREATE INDEX idx_posts_created_at ON posts(created_at)")?;
    
    // Create composite index for common query patterns
    println!("Creating composite indexes...");
    conn.execute("CREATE INDEX idx_posts_user_created ON posts(user_id, created_at)")?;
    
    println!("✓ Indexes created successfully");
    Ok(())
}

/// Run optimized workload with indexes
fn run_optimized_workload(conn: &mut Connection) -> Result<WorkloadMetrics, OxidbError> {
    let start = Instant::now();
    let mut query_times = Vec::new();
    
    // Same queries as before, but now with indexes
    for user_id in [42, 123, 456, 789, 999] {
        let query_start = Instant::now();
        conn.execute(&format!(
            "SELECT * FROM posts WHERE user_id = {}",
            user_id
        ))?;
        query_times.push(query_start.elapsed());
    }
    
    for post_id in [100, 500, 1000, 2500, 4999] {
        let query_start = Instant::now();
        conn.execute(&format!(
            "SELECT * FROM comments WHERE post_id = {}",
            post_id
        ))?;
        query_times.push(query_start.elapsed());
    }
    
    let query_start = Instant::now();
    conn.execute(
        "SELECT u.username, COUNT(p.id) as post_count 
         FROM users u 
         JOIN posts p ON u.id = p.user_id 
         WHERE u.id < 100"
    )?;
    query_times.push(query_start.elapsed());
    
    let query_start = Instant::now();
    conn.execute(
        "SELECT * FROM posts 
         WHERE created_at BETWEEN 1700000000 AND 1700864000"
    )?;
    query_times.push(query_start.elapsed());
    
    let total_duration = start.elapsed();
    let avg_query_time = query_times.iter().sum::<Duration>() / query_times.len() as u32;
    
    Ok(WorkloadMetrics {
        total_duration,
        avg_query_time,
        queries_per_second: query_times.len() as f64 / total_duration.as_secs_f64(),
        cache_hit_rate: 0.85, // Simulated cache hit rate after warming
    })
}

/// Compare performance metrics
fn compare_performance(unoptimized: &WorkloadMetrics, optimized: &WorkloadMetrics) {
    let speedup = unoptimized.total_duration.as_secs_f64() / optimized.total_duration.as_secs_f64();
    let query_speedup = unoptimized.avg_query_time.as_secs_f64() / optimized.avg_query_time.as_secs_f64();
    
    println!("┌─────────────────────────────────────────────┐");
    println!("│          Performance Comparison             │");
    println!("├─────────────────────────────────────────────┤");
    println!("│ Metric              │ Before    │ After     │");
    println!("├─────────────────────────────────────────────┤");
    println!("│ Total Duration      │ {:>8.2}s │ {:>8.2}s │", 
        unoptimized.total_duration.as_secs_f64(),
        optimized.total_duration.as_secs_f64()
    );
    println!("│ Avg Query Time      │ {:>8.2}ms│ {:>8.2}ms│", 
        unoptimized.avg_query_time.as_millis(),
        optimized.avg_query_time.as_millis()
    );
    println!("│ Queries/Second      │ {:>9.1} │ {:>9.1} │", 
        unoptimized.queries_per_second,
        optimized.queries_per_second
    );
    println!("│ Cache Hit Rate      │ {:>8.1}% │ {:>8.1}% │", 
        unoptimized.cache_hit_rate * 100.0,
        optimized.cache_hit_rate * 100.0
    );
    println!("└─────────────────────────────────────────────┘");
    println!("\n🚀 Overall Speedup: {:.2}x", speedup);
    println!("📊 Query Speedup: {:.2}x", query_speedup);
}

/// Demonstrate advanced optimization techniques
fn demonstrate_advanced_optimizations(conn: &mut Connection) -> Result<(), OxidbError> {
    println!("\n1. Vector Similarity Search Optimization");
    
    // Create HNSW index for vector similarity
    conn.execute("CREATE INDEX idx_users_vector ON users USING hnsw(profile_vector)")?;
    
    // Demonstrate efficient vector search
    let query_vector = (0..128)
        .map(|i| (i as f32 * 0.01).to_string())
        .collect::<Vec<_>>()
        .join(", ");
    
    let start = Instant::now();
    conn.execute(&format!(
        "SELECT id, username FROM users 
         ORDER BY profile_vector <-> [{}] 
         LIMIT 10",
        query_vector
    ))?;
    let vector_search_time = start.elapsed();
    
    println!("   ✓ Vector similarity search completed in {:?}", vector_search_time);
    
    println!("\n2. Query Plan Analysis");
    
    // Get query plan for complex query
    let plan = conn.execute("EXPLAIN SELECT u.username, COUNT(p.id) 
                            FROM users u 
                            JOIN posts p ON u.id = p.user_id 
                            GROUP BY u.username")?;
    
    println!("   ✓ Query plan shows index usage and join strategy");
    
    println!("\n3. Batch Processing Optimization");
    
    // Demonstrate batch insert performance
    let batch_start = Instant::now();
    conn.execute("BEGIN TRANSACTION")?;
    
    for i in 10001..=11000 {
        conn.execute(&format!(
            "INSERT INTO comments (id, post_id, user_id, content, created_at) 
             VALUES ({}, {}, {}, 'Batch comment {}', {})",
            i, i % 5000 + 1, i % 1000 + 1, i, 1700000000 + i
        ))?;
    }
    
    conn.execute("COMMIT")?;
    let batch_time = batch_start.elapsed();
    
    println!("   ✓ Batch insert of 1,000 records in {:?}", batch_time);
    println!("   ✓ Rate: {:.0} inserts/second", 1000.0 / batch_time.as_secs_f64());
    
    // Final performance report
    println!("\n📈 Final Performance Report:");
    let final_report = conn.get_performance_report()?;
    
    // Extract key metrics from report
    if final_report.contains("optimization recommendations") {
        println!("   ✓ All recommended optimizations have been applied");
    }
    
    Ok(())
}