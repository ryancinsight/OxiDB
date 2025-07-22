//! Performance metrics collection and tracking

use std::time::Duration;
use std::collections::HashMap;

/// Comprehensive performance metrics for database operations
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    /// Query execution metrics
    pub query_metrics: QueryMetrics,
    /// Transaction performance metrics
    pub transaction_metrics: TransactionMetrics,
    /// Storage and I/O metrics
    pub storage_metrics: StorageMetrics,
}

impl PerformanceMetrics {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            query_metrics: QueryMetrics::new(),
            transaction_metrics: TransactionMetrics::new(),
            storage_metrics: StorageMetrics::new(),
        }
    }

    /// Record a query execution
    pub fn record_query(&mut self, query: &str, duration: Duration, rows_affected: u64) {
        self.query_metrics.record_execution(query, duration, rows_affected);
    }

    /// Record a transaction
    pub fn record_transaction(&mut self, duration: Duration, operations: u32) {
        self.transaction_metrics.record_transaction(duration, operations);
    }

    /// Record storage operation
    pub fn record_storage_operation(&mut self, operation: &str, duration: Duration, bytes: u64) {
        self.storage_metrics.record_operation(operation, duration, bytes);
    }
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Query execution performance metrics
#[derive(Debug, Clone)]
pub struct QueryMetrics {
    /// Total number of queries executed
    pub total_queries: u64,
    /// Total execution time across all queries
    pub total_execution_time: Duration,
    /// Average query execution time
    pub average_execution_time: Duration,
    /// Slowest query execution time
    pub max_execution_time: Duration,
    /// Fastest query execution time
    pub min_execution_time: Duration,
    /// Query execution times by query type
    pub execution_times_by_type: HashMap<String, Vec<Duration>>,
    /// Total rows affected across all queries
    pub total_rows_affected: u64,
}

impl QueryMetrics {
    /// Create new query metrics
    pub fn new() -> Self {
        Self {
            total_queries: 0,
            total_execution_time: Duration::ZERO,
            average_execution_time: Duration::ZERO,
            max_execution_time: Duration::ZERO,
            min_execution_time: Duration::MAX,
            execution_times_by_type: HashMap::new(),
            total_rows_affected: 0,
        }
    }

    /// Record a query execution
    pub fn record_execution(&mut self, query: &str, duration: Duration, rows_affected: u64) {
        self.total_queries += 1;
        self.total_execution_time += duration;
        self.total_rows_affected += rows_affected;

        // Update min/max times
        if duration > self.max_execution_time {
            self.max_execution_time = duration;
        }
        if duration < self.min_execution_time {
            self.min_execution_time = duration;
        }

        // Update average
        self.average_execution_time = self.total_execution_time / self.total_queries as u32;

        // Track by query type
        let query_type = self.extract_query_type(query);
        self.execution_times_by_type
            .entry(query_type)
            .or_insert_with(Vec::new)
            .push(duration);
    }

    /// Extract query type from SQL string
    fn extract_query_type(&self, query: &str) -> String {
        let trimmed = query.trim().to_uppercase();
        if trimmed.starts_with("SELECT") {
            "SELECT".to_string()
        } else if trimmed.starts_with("INSERT") {
            "INSERT".to_string()
        } else if trimmed.starts_with("UPDATE") {
            "UPDATE".to_string()
        } else if trimmed.starts_with("DELETE") {
            "DELETE".to_string()
        } else if trimmed.starts_with("CREATE") {
            "CREATE".to_string()
        } else if trimmed.starts_with("DROP") {
            "DROP".to_string()
        } else {
            "OTHER".to_string()
        }
    }
}

impl Default for QueryMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Transaction performance metrics
#[derive(Debug, Clone)]
pub struct TransactionMetrics {
    /// Total number of transactions
    pub total_transactions: u64,
    /// Total transaction duration
    pub total_duration: Duration,
    /// Average transaction duration
    pub average_duration: Duration,
    /// Number of committed transactions
    pub committed_transactions: u64,
    /// Number of aborted transactions
    pub aborted_transactions: u64,
}

impl TransactionMetrics {
    /// Create new transaction metrics
    pub fn new() -> Self {
        Self {
            total_transactions: 0,
            total_duration: Duration::ZERO,
            average_duration: Duration::ZERO,
            committed_transactions: 0,
            aborted_transactions: 0,
        }
    }

    /// Record a transaction
    pub fn record_transaction(&mut self, duration: Duration, _operations: u32) {
        self.total_transactions += 1;
        self.total_duration += duration;
        self.average_duration = self.total_duration / self.total_transactions as u32;
        self.committed_transactions += 1; // Assume committed for now
    }
}

impl Default for TransactionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Storage and I/O performance metrics
#[derive(Debug, Clone)]
pub struct StorageMetrics {
    /// Total bytes read
    pub total_bytes_read: u64,
    /// Total bytes written
    pub total_bytes_written: u64,
    /// Total I/O operations
    pub total_io_operations: u64,
    /// Average I/O operation duration
    pub average_io_duration: Duration,
}

impl StorageMetrics {
    /// Create new storage metrics
    pub fn new() -> Self {
        Self {
            total_bytes_read: 0,
            total_bytes_written: 0,
            total_io_operations: 0,
            average_io_duration: Duration::ZERO,
        }
    }

    /// Record a storage operation
    pub fn record_operation(&mut self, operation: &str, duration: Duration, bytes: u64) {
        self.total_io_operations += 1;
        
        if operation.contains("read") {
            self.total_bytes_read += bytes;
        } else if operation.contains("write") {
            self.total_bytes_written += bytes;
        }

        // Update average duration
        self.average_io_duration = 
            (self.average_io_duration * (self.total_io_operations - 1) as u32 + duration) 
            / self.total_io_operations as u32;
    }
}

impl Default for StorageMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_metrics() {
        let mut metrics = QueryMetrics::new();
        metrics.record_execution("SELECT * FROM users", Duration::from_millis(100), 5);
        
        assert_eq!(metrics.total_queries, 1);
        assert_eq!(metrics.total_rows_affected, 5);
        assert_eq!(metrics.average_execution_time, Duration::from_millis(100));
    }

    #[test]
    fn test_query_type_extraction() {
        let metrics = QueryMetrics::new();
        assert_eq!(metrics.extract_query_type("SELECT * FROM users"), "SELECT");
        assert_eq!(metrics.extract_query_type("INSERT INTO users VALUES (1)"), "INSERT");
        assert_eq!(metrics.extract_query_type("UPDATE users SET name = 'test'"), "UPDATE");
    }

    #[test]
    fn test_performance_metrics() {
        let mut metrics = PerformanceMetrics::new();
        metrics.record_query("SELECT * FROM test", Duration::from_millis(50), 10);
        
        assert_eq!(metrics.query_metrics.total_queries, 1);
        assert_eq!(metrics.query_metrics.total_rows_affected, 10);
    }
}
