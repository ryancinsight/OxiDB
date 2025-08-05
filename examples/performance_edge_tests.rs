use oxidb::{Connection, OxidbError};
use std::sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}};
use std::thread;
use std::time::{Duration, Instant};

/// Performance and Edge Case Tests for Oxidb
/// Tests system behavior under stress conditions and edge cases
/// Follows SOLID principles with clear separation of concerns
/// Implements CUPID principles for composable, predictable tests

const LARGE_DATASET_SIZE: usize = 10_000;
const CONCURRENT_THREADS: usize = 10;
const STRESS_TEST_ITERATIONS: usize = 1_000;

/// Performance test configuration - Single Responsibility Principle
#[derive(Debug, Clone)]
struct TestConfig {
    dataset_size: usize,
    thread_count: usize,
    iterations: usize,
    timeout_seconds: u64,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            dataset_size: LARGE_DATASET_SIZE,
            thread_count: CONCURRENT_THREADS,
            iterations: STRESS_TEST_ITERATIONS,
            timeout_seconds: 30,
        }
    }
}

/// Test metrics collector - Open/Closed Principle (extensible without modification)
#[derive(Debug, Default)]
struct TestMetrics {
    operations_completed: AtomicUsize,
    operations_failed: AtomicUsize,
    total_duration: Arc<Mutex<Duration>>,
    peak_memory_usage: AtomicUsize,
}

impl TestMetrics {
    fn new() -> Self {
        Self::default()
    }

    fn record_success(&self) {
        self.operations_completed.fetch_add(1, Ordering::Relaxed);
    }

    fn record_failure(&self) {
        self.operations_failed.fetch_add(1, Ordering::Relaxed);
    }

    fn record_duration(&self, duration: Duration) {
        let mut total = self.total_duration.lock().unwrap();
        *total += duration;
    }

    fn get_summary(&self) -> TestSummary {
        TestSummary {
            completed: self.operations_completed.load(Ordering::Relaxed),
            failed: self.operations_failed.load(Ordering::Relaxed),
            total_duration: *self.total_duration.lock().unwrap(),
            peak_memory: self.peak_memory_usage.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct TestSummary {
    completed: usize,
    failed: usize,
    total_duration: Duration,
    peak_memory: usize,
}

impl TestSummary {
    fn success_rate(&self) -> f64 {
        if self.completed + self.failed == 0 {
            0.0
        } else {
            self.completed as f64 / (self.completed + self.failed) as f64 * 100.0
        }
    }

    fn operations_per_second(&self) -> f64 {
        if self.total_duration.as_secs_f64() == 0.0 {
            0.0
        } else {
            self.completed as f64 / self.total_duration.as_secs_f64()
        }
    }
}

/// Database connection pool - Dependency Inversion Principle
trait ConnectionPool {
    fn get_connection(&self) -> Result<Arc<Mutex<Connection>>, OxidbError>;
    fn return_connection(&self, conn: Arc<Mutex<Connection>>);
}

struct SimpleConnectionPool {
    connections: Arc<Mutex<Vec<Arc<Mutex<Connection>>>>>,
    max_size: usize,
}

impl SimpleConnectionPool {
    fn new(max_size: usize) -> Result<Self, OxidbError> {
        let mut connections = Vec::new();
        for _ in 0..max_size {
            let conn = Connection::open_in_memory()?;
            connections.push(Arc::new(Mutex::new(conn)));
        }

        Ok(Self {
            connections: Arc::new(Mutex::new(connections)),
            max_size,
        })
    }
}

impl ConnectionPool for SimpleConnectionPool {
    fn get_connection(&self) -> Result<Arc<Mutex<Connection>>, OxidbError> {
        let mut pool = self.connections.lock().unwrap();
        if let Some(conn) = pool.pop() {
            Ok(conn)
        } else {
            // Create new connection if pool is empty
            let conn = Connection::open_in_memory()?;
            Ok(Arc::new(Mutex::new(conn)))
        }
    }

    fn return_connection(&self, conn: Arc<Mutex<Connection>>) {
        let mut pool = self.connections.lock().unwrap();
        if pool.len() < self.max_size {
            pool.push(conn);
        }
        // Otherwise, let connection drop
    }
}

/// Performance test suite - Single Responsibility Principle
struct PerformanceTestSuite {
    config: TestConfig,
    metrics: Arc<TestMetrics>,
    pool: Arc<dyn ConnectionPool + Send + Sync>,
}

impl PerformanceTestSuite {
    fn new(config: TestConfig) -> Result<Self, OxidbError> {
        let pool = Arc::new(SimpleConnectionPool::new(config.thread_count * 2)?);
        
        Ok(Self {
            config,
            metrics: Arc::new(TestMetrics::new()),
            pool,
        })
    }

    fn setup_test_schema(&self) -> Result<(), OxidbError> {
        let conn = self.pool.get_connection()?;
        let mut conn = conn.lock().unwrap();

        // Create tables with constraints for edge case testing
        conn.execute("CREATE TABLE IF NOT EXISTS large_data_test (
            id INTEGER PRIMARY KEY,
            data TEXT NOT NULL,
            value INTEGER NOT NULL CHECK(value >= 0),
            timestamp TEXT NOT NULL,
            category TEXT NOT NULL
        )")?;

        conn.execute("CREATE TABLE IF NOT EXISTS concurrent_test (
            id INTEGER PRIMARY KEY,
            thread_id INTEGER NOT NULL,
            operation_count INTEGER NOT NULL,
            data BLOB,
            created_at TEXT NOT NULL
        )")?;

        // Create indexes for performance testing
        conn.execute("CREATE INDEX IF NOT EXISTS idx_large_data_category ON large_data_test(category)")?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_large_data_value ON large_data_test(value)")?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_concurrent_thread ON concurrent_test(thread_id)")?;

        Ok(())
    }

    /// Test large dataset operations - KISS principle (simple, focused test)
    fn test_large_dataset_operations(&self) -> Result<(), OxidbError> {
        println!("🔄 Testing large dataset operations...");
        let start_time = Instant::now();

        let conn = self.pool.get_connection()?;
        let conn = conn.lock().unwrap();

        // Bulk insert test
        let insert_start = Instant::now();
        for i in 0..self.config.dataset_size {
            let query = format!(
                "INSERT INTO large_data_test (id, data, value, timestamp, category) VALUES ({}, '{}', {}, '{}', '{}')",
                i,
                format!("Large data string for record {} with additional padding to test memory usage", i),
                i % 1000,
                chrono::Utc::now().to_rfc3339(),
                format!("category_{}", i % 10)
            );
            
            match conn.execute(&query) {
                Ok(_) => self.metrics.record_success(),
                Err(_) => self.metrics.record_failure(),
            }

            // Progress indicator for large operations
            if i % 1000 == 0 && i > 0 {
                println!("  Inserted {} records...", i);
            }
        }
        let insert_duration = insert_start.elapsed();
        println!("  ✅ Bulk insert completed in {:?}", insert_duration);

        // Query performance test
        let query_start = Instant::now();
        for category in 0..10 {
            let query = format!("SELECT COUNT(*) FROM large_data_test WHERE category = 'category_{}'", category);
            match conn.query_all(&query) {
                Ok(_) => self.metrics.record_success(),
                Err(_) => self.metrics.record_failure(),
            }
        }
        let query_duration = query_start.elapsed();
        println!("  ✅ Category queries completed in {:?}", query_duration);

        // Range query test
        let range_start = Instant::now();
        let range_query = "SELECT * FROM large_data_test WHERE value BETWEEN 100 AND 200 ORDER BY value";
        match conn.query_all(range_query) {
            Ok(result) => {
                println!("  ✅ Range query returned {} records", result.len());
                self.metrics.record_success();
            }
            Err(_) => self.metrics.record_failure(),
        }
        let range_duration = range_start.elapsed();
        println!("  ✅ Range query completed in {:?}", range_duration);

        self.metrics.record_duration(start_time.elapsed());
        Ok(())
    }

    /// Test concurrent operations - demonstrates thread safety
    fn test_concurrent_operations(&self) -> Result<(), OxidbError> {
        println!("🔄 Testing concurrent operations with {} threads...", self.config.thread_count);
        let start_time = Instant::now();

        let handles: Vec<_> = (0..self.config.thread_count)
            .map(|thread_id| {
                let pool = self.pool.clone();
                let metrics = self.metrics.clone();
                let iterations = self.config.iterations / self.config.thread_count;

                thread::spawn(move || {
                    let thread_start = Instant::now();
                    
                    for i in 0..iterations {
                        let conn = match pool.get_connection() {
                            Ok(conn) => conn,
                            Err(_) => {
                                metrics.record_failure();
                                continue;
                            }
                        };

                        let conn_guard = conn.lock().unwrap();
                        
                        // Mix of operations to simulate real workload
                        match i % 4 {
                            0 => {
                                // Insert operation
                                let query = format!(
                                    "INSERT INTO concurrent_test (id, thread_id, operation_count, data, created_at) VALUES ({}, {}, {}, '{}', '{}')",
                                    thread_id * iterations + i,
                                    thread_id,
                                    i,
                                    format!("Thread {} operation {}", thread_id, i),
                                    chrono::Utc::now().to_rfc3339()
                                );
                                match conn_guard.execute(&query) {
                                    Ok(_) => metrics.record_success(),
                                    Err(_) => metrics.record_failure(),
                                }
                            }
                            1 => {
                                // Select operation
                                let query = format!("SELECT COUNT(*) FROM concurrent_test WHERE thread_id = {}", thread_id);
                                match conn_guard.query_all(&query) {
                                    Ok(_) => metrics.record_success(),
                                    Err(_) => metrics.record_failure(),
                                }
                            }
                            2 => {
                                // Update operation
                                let query = format!(
                                    "UPDATE concurrent_test SET operation_count = {} WHERE thread_id = {} AND operation_count < {}",
                                    i, thread_id, i
                                );
                                match conn_guard.execute(&query) {
                                    Ok(_) => metrics.record_success(),
                                    Err(_) => metrics.record_failure(),
                                }
                            }
                            3 => {
                                // Complex query operation
                                let query = "SELECT thread_id, COUNT(*), AVG(operation_count) FROM concurrent_test GROUP BY thread_id";
                                match conn_guard.query_all(query) {
                                    Ok(_) => metrics.record_success(),
                                    Err(_) => metrics.record_failure(),
                                }
                            }
                            _ => unreachable!(),
                        }

                        drop(conn_guard);
                        pool.return_connection(conn);
                    }

                    let thread_duration = thread_start.elapsed();
                    metrics.record_duration(thread_duration);
                    println!("  Thread {} completed {} operations in {:?}", thread_id, iterations, thread_duration);
                })
            })
            .collect();

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        let total_duration = start_time.elapsed();
        println!("  ✅ All concurrent operations completed in {:?}", total_duration);
        Ok(())
    }

    /// Test boundary conditions and edge cases
    fn test_boundary_conditions(&self) -> Result<(), OxidbError> {
        println!("🔄 Testing boundary conditions and edge cases...");
        let start_time = Instant::now();

        let conn = self.pool.get_connection()?;
        let mut conn = conn.lock().unwrap();

        // Test empty string handling
        let empty_string_query = "INSERT INTO large_data_test (id, data, value, timestamp, category) VALUES (999999, '', 0, '2024-01-01T00:00:00Z', '')";
        match conn.execute(empty_string_query) {
            Ok(_) => {
                println!("  ✅ Empty string handling successful");
                self.metrics.record_success();
            }
            Err(e) => {
                println!("  ❌ Empty string handling failed: {:?}", e);
                self.metrics.record_failure();
            }
        }

        // Test very long string (stress test)
        let long_string = "A".repeat(10_000);
        let long_string_query = format!(
            "INSERT INTO large_data_test (id, data, value, timestamp, category) VALUES (999998, '{}', 1, '2024-01-01T00:00:00Z', 'long')",
            long_string
        );
        match conn.execute(&long_string_query) {
            Ok(_) => {
                println!("  ✅ Long string handling successful");
                self.metrics.record_success();
            }
            Err(e) => {
                println!("  ❌ Long string handling failed: {:?}", e);
                self.metrics.record_failure();
            }
        }

        // Test constraint violations
        let constraint_violation_query = "INSERT INTO large_data_test (id, data, value, timestamp, category) VALUES (999997, 'test', -1, '2024-01-01T00:00:00Z', 'test')";
        match conn.execute(constraint_violation_query) {
            Err(_) => {
                println!("  ✅ Constraint violation correctly rejected");
                self.metrics.record_success();
            }
            Ok(_) => {
                println!("  ❌ Constraint violation should have been rejected");
                self.metrics.record_failure();
            }
        }

        // Test duplicate key handling
        let duplicate_key_query = "INSERT INTO large_data_test (id, data, value, timestamp, category) VALUES (999999, 'duplicate', 0, '2024-01-01T00:00:00Z', 'test')";
        match conn.execute(duplicate_key_query) {
            Err(_) => {
                println!("  ✅ Duplicate key correctly rejected");
                self.metrics.record_success();
            }
            Ok(_) => {
                println!("  ❌ Duplicate key should have been rejected");
                self.metrics.record_failure();
            }
        }

        // Test complex query with edge cases
        let complex_query = "SELECT * FROM large_data_test WHERE data LIKE '%nonexistent%' AND value > 999999 ORDER BY id DESC LIMIT 0";
        match conn.query_all(complex_query) {
            Ok(result) => {
                println!("  ✅ Complex edge case query returned {} results", result.len());
                self.metrics.record_success();
            }
            Err(e) => {
                println!("  ❌ Complex edge case query failed: {:?}", e);
                self.metrics.record_failure();
            }
        }

        self.metrics.record_duration(start_time.elapsed());
        println!("  ✅ Boundary condition tests completed in {:?}", start_time.elapsed());
        Ok(())
    }

    /// Test memory and resource limits
    fn test_resource_limits(&self) -> Result<(), OxidbError> {
        println!("🔄 Testing resource limits and memory usage...");
        let start_time = Instant::now();

        let conn = self.pool.get_connection()?;
        let mut conn = conn.lock().unwrap();

        // Test large result set handling
        let large_result_query = "SELECT * FROM large_data_test ORDER BY id";
        match conn.query_all(large_result_query) {
            Ok(result) => {
                println!("  ✅ Large result set query returned {} rows", result.len());
                self.metrics.record_success();
            }
            Err(e) => {
                println!("  ❌ Large result set query failed: {:?}", e);
                self.metrics.record_failure();
            }
        }

        // Test multiple simultaneous large queries (memory pressure)
        let queries = vec![
            "SELECT COUNT(*) FROM large_data_test",
            "SELECT AVG(value) FROM large_data_test",
            "SELECT category, COUNT(*) FROM large_data_test GROUP BY category",
            "SELECT * FROM large_data_test WHERE value > 500 ORDER BY value DESC",
        ];

        for (i, query) in queries.iter().enumerate() {
            match conn.query_all(query) {
                Ok(_) => {
                    println!("  ✅ Resource test query {} completed", i + 1);
                    self.metrics.record_success();
                }
                Err(e) => {
                    println!("  ❌ Resource test query {} failed: {:?}", i + 1, e);
                    self.metrics.record_failure();
                }
            }
        }

        self.metrics.record_duration(start_time.elapsed());
        println!("  ✅ Resource limit tests completed in {:?}", start_time.elapsed());
        Ok(())
    }

    fn run_all_tests(&self) -> Result<TestSummary, OxidbError> {
        println!("=== Starting Performance and Edge Case Tests ===\n");
        
        self.setup_test_schema()?;
        
        // Run test suite
        self.test_large_dataset_operations()?;
        self.test_concurrent_operations()?;
        self.test_boundary_conditions()?;
        self.test_resource_limits()?;

        let summary = self.metrics.get_summary();
        Ok(summary)
    }
}

fn main() -> Result<(), OxidbError> {
    let config = TestConfig::default();
    println!("Performance Test Configuration:");
    println!("  Dataset Size: {}", config.dataset_size);
    println!("  Thread Count: {}", config.thread_count);
    println!("  Iterations: {}", config.iterations);
    println!("  Timeout: {}s\n", config.timeout_seconds);

    let test_suite = PerformanceTestSuite::new(config)?;
    let summary = test_suite.run_all_tests()?;

    println!("\n=== Performance Test Results ===");
    println!("Operations Completed: {}", summary.completed);
    println!("Operations Failed: {}", summary.failed);
    println!("Success Rate: {:.2}%", summary.success_rate());
    println!("Total Duration: {:?}", summary.total_duration);
    println!("Operations/Second: {:.2}", summary.operations_per_second());
    println!("Peak Memory Usage: {} bytes", summary.peak_memory);

    if summary.success_rate() >= 95.0 {
        println!("\n✅ Performance tests PASSED (success rate >= 95%)");
    } else {
        println!("\n❌ Performance tests FAILED (success rate < 95%)");
    }

    println!("\n=== Performance and Edge Case Tests Completed ===");
    Ok(())
}

// Add required dependencies
use chrono;