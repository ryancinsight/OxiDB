//! Performance analytics and reporting

use super::metrics::PerformanceMetrics;
use std::time::Duration;
use std::fmt;

/// Performance analyzer for generating insights and reports
#[derive(Debug)]
pub struct PerformanceAnalyzer {
    /// Threshold for slow query detection (default: 1 second)
    slow_query_threshold: Duration,
}

impl PerformanceAnalyzer {
    /// Create a new performance analyzer
    #[must_use]
    pub const fn new() -> Self {
        Self { slow_query_threshold: Duration::from_secs(1) }
    }

    /// Create analyzer with custom slow query threshold
    #[must_use]
    pub const fn with_threshold(threshold: Duration) -> Self {
        Self { slow_query_threshold: threshold }
    }

    /// Analyze performance metrics and generate a report
    #[must_use]
    pub fn analyze(&self, metrics: &PerformanceMetrics) -> PerformanceReport {
        let query_analysis = self.analyze_queries(metrics);
        let transaction_analysis = self.analyze_transactions(metrics);
        let storage_analysis = self.analyze_storage(metrics);
        let bottlenecks = self.identify_bottlenecks(metrics);

        PerformanceReport {
            query_analysis,
            transaction_analysis,
            storage_analysis,
            bottlenecks,
            recommendations: self.generate_recommendations(metrics),
        }
    }

    /// Analyze query performance
    fn analyze_queries(&self, metrics: &PerformanceMetrics) -> QueryAnalysis {
        let query_metrics = &metrics.query_metrics;

        QueryAnalysis {
            total_queries: query_metrics.total_queries,
            average_execution_time: query_metrics.average_execution_time,
            slowest_query_time: query_metrics.max_execution_time,
            fastest_query_time: if query_metrics.min_execution_time == Duration::MAX {
                Duration::ZERO
            } else {
                query_metrics.min_execution_time
            },
            queries_per_second: if query_metrics.total_execution_time.as_secs() > 0 {
                query_metrics.total_queries as f64
                    / query_metrics.total_execution_time.as_secs_f64()
            } else {
                0.0
            },
            slow_queries_detected: query_metrics.max_execution_time > self.slow_query_threshold,
        }
    }

    /// Analyze transaction performance
    fn analyze_transactions(&self, metrics: &PerformanceMetrics) -> TransactionAnalysis {
        let tx_metrics = &metrics.transaction_metrics;

        TransactionAnalysis {
            total_transactions: tx_metrics.total_transactions,
            average_duration: tx_metrics.average_duration,
            commit_rate: if tx_metrics.total_transactions > 0 {
                tx_metrics.committed_transactions as f64 / tx_metrics.total_transactions as f64
            } else {
                0.0
            },
            abort_rate: if tx_metrics.total_transactions > 0 {
                tx_metrics.aborted_transactions as f64 / tx_metrics.total_transactions as f64
            } else {
                0.0
            },
        }
    }

    /// Analyze storage performance
    fn analyze_storage(&self, metrics: &PerformanceMetrics) -> StorageAnalysis {
        let storage_metrics = &metrics.storage_metrics;

        StorageAnalysis {
            total_bytes_read: storage_metrics.total_bytes_read,
            total_bytes_written: storage_metrics.total_bytes_written,
            total_io_operations: storage_metrics.total_io_operations,
            average_io_duration: storage_metrics.average_io_duration,
            read_write_ratio: if storage_metrics.total_bytes_written > 0 {
                storage_metrics.total_bytes_read as f64 / storage_metrics.total_bytes_written as f64
            } else {
                0.0
            },
        }
    }

    /// Identify performance bottlenecks
    fn identify_bottlenecks(&self, metrics: &PerformanceMetrics) -> BottleneckAnalysis {
        let mut bottlenecks = Vec::new();
        let mut severity = BottleneckSeverity::Low;

        // Check for slow queries
        if metrics.query_metrics.max_execution_time > self.slow_query_threshold {
            bottlenecks.push(format!(
                "Slow query detected: {:.2}s execution time",
                metrics.query_metrics.max_execution_time.as_secs_f64()
            ));
            severity = BottleneckSeverity::High;
        }

        // Check transaction abort rate
        let abort_rate = if metrics.transaction_metrics.total_transactions > 0 {
            metrics.transaction_metrics.aborted_transactions as f64
                / metrics.transaction_metrics.total_transactions as f64
        } else {
            0.0
        };

        if abort_rate > 0.1 {
            bottlenecks.push(format!("High transaction abort rate: {:.1}%", abort_rate * 100.0));
            severity = std::cmp::max(severity, BottleneckSeverity::Medium);
        }

        // Check I/O performance
        if metrics.storage_metrics.average_io_duration > Duration::from_millis(100) {
            bottlenecks.push(format!(
                "Slow I/O operations: {:.2}ms average",
                metrics.storage_metrics.average_io_duration.as_millis()
            ));
            severity = std::cmp::max(severity, BottleneckSeverity::Medium);
        }

        BottleneckAnalysis { bottlenecks, severity }
    }

    /// Generate performance recommendations
    fn generate_recommendations(&self, metrics: &PerformanceMetrics) -> Vec<String> {
        let mut recommendations = Vec::new();

        // Query optimization recommendations
        if metrics.query_metrics.max_execution_time > self.slow_query_threshold {
            recommendations.push("Consider adding indexes for slow queries".to_string());
            recommendations
                .push("Review query execution plans for optimization opportunities".to_string());
        }

        // Transaction recommendations
        let abort_rate = if metrics.transaction_metrics.total_transactions > 0 {
            metrics.transaction_metrics.aborted_transactions as f64
                / metrics.transaction_metrics.total_transactions as f64
        } else {
            0.0
        };

        if abort_rate > 0.05 {
            recommendations
                .push("High abort rate detected - consider reducing transaction scope".to_string());
        }

        // Storage recommendations
        if metrics.storage_metrics.average_io_duration > Duration::from_millis(50) {
            recommendations
                .push("Consider using faster storage or optimizing I/O patterns".to_string());
        }

        if recommendations.is_empty() {
            recommendations
                .push("Performance looks good - no immediate optimizations needed".to_string());
        }

        recommendations
    }
}

impl Default for PerformanceAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

/// Comprehensive performance report
#[derive(Debug, Clone)]
pub struct PerformanceReport {
    /// Query performance analysis
    pub query_analysis: QueryAnalysis,
    /// Transaction performance analysis
    pub transaction_analysis: TransactionAnalysis,
    /// Storage performance analysis
    pub storage_analysis: StorageAnalysis,
    /// Identified bottlenecks
    pub bottlenecks: BottleneckAnalysis,
    /// Performance recommendations
    pub recommendations: Vec<String>,
}

/// Query performance analysis
#[derive(Debug, Clone)]
pub struct QueryAnalysis {
    /// Total number of queries executed
    pub total_queries: u64,
    /// Average query execution time
    pub average_execution_time: Duration,
    /// Slowest query execution time
    pub slowest_query_time: Duration,
    /// Fastest query execution time
    pub fastest_query_time: Duration,
    /// Queries per second throughput
    pub queries_per_second: f64,
    /// Whether slow queries were detected
    pub slow_queries_detected: bool,
}

/// Transaction performance analysis
#[derive(Debug, Clone)]
pub struct TransactionAnalysis {
    /// Total number of transactions
    pub total_transactions: u64,
    /// Average transaction duration
    pub average_duration: Duration,
    /// Commit rate (0.0 to 1.0)
    pub commit_rate: f64,
    /// Abort rate (0.0 to 1.0)
    pub abort_rate: f64,
}

/// Storage performance analysis
#[derive(Debug, Clone)]
pub struct StorageAnalysis {
    /// Total bytes read
    pub total_bytes_read: u64,
    /// Total bytes written
    pub total_bytes_written: u64,
    /// Total I/O operations
    pub total_io_operations: u64,
    /// Average I/O operation duration
    pub average_io_duration: Duration,
    /// Read to write ratio
    pub read_write_ratio: f64,
}

/// Bottleneck analysis results
#[derive(Debug, Clone)]
pub struct BottleneckAnalysis {
    /// Identified bottlenecks
    pub bottlenecks: Vec<String>,
    /// Overall severity level
    pub severity: BottleneckSeverity,
}

/// Bottleneck severity levels
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum BottleneckSeverity {
    /// Low impact on performance
    Low,
    /// Medium impact on performance
    Medium,
    /// High impact on performance
    High,
    /// Critical performance issue
    Critical,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::performance::metrics::PerformanceMetrics;

    #[test]
    fn test_performance_analyzer() {
        let analyzer = PerformanceAnalyzer::new();
        let mut metrics = PerformanceMetrics::new();

        // Add some test data
        metrics.record_query("SELECT * FROM users", Duration::from_millis(100), 5);
        metrics.record_transaction(Duration::from_millis(200), 3);

        let report = analyzer.analyze(&metrics);

        assert_eq!(report.query_analysis.total_queries, 1);
        assert_eq!(report.transaction_analysis.total_transactions, 1);
        assert!(!report.recommendations.is_empty());
    }

    #[test]
    fn test_slow_query_detection() {
        let analyzer = PerformanceAnalyzer::with_threshold(Duration::from_millis(50));
        let mut metrics = PerformanceMetrics::new();

        // Add a slow query
        metrics.record_query("SELECT * FROM large_table", Duration::from_millis(100), 1000);

        let report = analyzer.analyze(&metrics);

        assert!(report.query_analysis.slow_queries_detected);
        assert!(report.bottlenecks.severity >= BottleneckSeverity::High);
    }
}

impl fmt::Display for PerformanceReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "=== Performance Report ===")?;
        writeln!(f)?;
        
        // Query Analysis
        writeln!(f, "Query Performance:")?;
        writeln!(f, "  Total Queries: {}", self.query_analysis.total_queries)?;
        writeln!(f, "  Average Execution Time: {:?}", self.query_analysis.average_execution_time)?;
        writeln!(f, "  Slowest Query: {:?}", self.query_analysis.slowest_query_time)?;
        writeln!(f, "  Fastest Query: {:?}", self.query_analysis.fastest_query_time)?;
        writeln!(f, "  Throughput: {:.2} queries/second", self.query_analysis.queries_per_second)?;
        if self.query_analysis.slow_queries_detected {
            writeln!(f, "  ⚠️  Slow queries detected!")?;
        }
        writeln!(f)?;
        
        // Transaction Analysis
        writeln!(f, "Transaction Performance:")?;
        writeln!(f, "  Total Transactions: {}", self.transaction_analysis.total_transactions)?;
        writeln!(f, "  Average Duration: {:?}", self.transaction_analysis.average_duration)?;
        writeln!(f, "  Commit Rate: {:.1}%", self.transaction_analysis.commit_rate * 100.0)?;
        writeln!(f, "  Abort Rate: {:.1}%", self.transaction_analysis.abort_rate * 100.0)?;
        writeln!(f)?;
        
        // Storage Analysis
        writeln!(f, "Storage I/O:")?;
        writeln!(f, "  Bytes Read: {}", format_bytes(self.storage_analysis.total_bytes_read))?;
        writeln!(f, "  Bytes Written: {}", format_bytes(self.storage_analysis.total_bytes_written))?;
        writeln!(f, "  Total I/O Operations: {}", self.storage_analysis.total_io_operations)?;
        writeln!(f, "  Average I/O Duration: {:?}", self.storage_analysis.average_io_duration)?;
        writeln!(f, "  Read/Write Ratio: {:.2}:1", self.storage_analysis.read_write_ratio)?;
        writeln!(f)?;
        
        // Bottlenecks
        writeln!(f, "Bottleneck Analysis:")?;
        writeln!(f, "  Severity: {:?}", self.bottlenecks.severity)?;
        for bottleneck in &self.bottlenecks.bottlenecks {
            writeln!(f, "  - {}", bottleneck)?;
        }
        writeln!(f)?;
        
        // Recommendations
        if !self.recommendations.is_empty() {
            writeln!(f, "Recommendations:")?;
            for recommendation in &self.recommendations {
                writeln!(f, "  • {}", recommendation)?;
            }
        }
        
        Ok(())
    }
}

/// Format bytes into human-readable string
fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;
    
    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }
    
    format!("{:.2} {}", size, UNITS[unit_index])
}
