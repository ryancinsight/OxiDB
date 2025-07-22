//! Performance Monitoring and Analysis Framework for `OxiDB`

pub mod metrics;
pub mod profiler; 
pub mod analytics;
pub mod monitor;

pub use metrics::{PerformanceMetrics, QueryMetrics};
pub use profiler::{PerformanceProfiler, ProfiledOperation};
pub use analytics::{PerformanceAnalyzer, PerformanceReport};
pub use monitor::{PerformanceMonitor, MonitoringConfig};

use std::time::Duration;
use std::sync::{Arc, RwLock};

/// Global performance tracking context for the database instance
#[derive(Debug, Clone)]
pub struct PerformanceContext {
    /// Shared metrics collector
    pub metrics: Arc<RwLock<PerformanceMetrics>>,
    /// Performance profiler for detailed analysis
    pub profiler: Arc<RwLock<PerformanceProfiler>>,
    /// Real-time performance monitor
    pub monitor: Arc<RwLock<PerformanceMonitor>>,
    /// Configuration for monitoring behavior
    pub config: MonitoringConfig,
}

impl PerformanceContext {
    /// Create a new performance context with default configuration
    #[must_use] pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(PerformanceMetrics::new())),
            profiler: Arc::new(RwLock::new(PerformanceProfiler::new())),
            monitor: Arc::new(RwLock::new(PerformanceMonitor::new())),
            config: MonitoringConfig::default(),
        }
    }

    /// Record a query execution with performance metrics
    pub fn record_query(&self, query: &str, duration: Duration, rows_affected: u64) -> Result<(), crate::core::common::OxidbError> {
        if let Ok(mut metrics) = self.metrics.write() {
            metrics.record_query(query, duration, rows_affected);
        }
        Ok(())
    }

    /// Generate a comprehensive performance report
    pub fn generate_report(&self) -> Result<PerformanceReport, crate::core::common::OxidbError> {
        let metrics = self.metrics.read()
            .map_err(|e| crate::core::common::OxidbError::Internal(format!("Failed to read metrics: {e}")))?;
        
        let analyzer = PerformanceAnalyzer::new();
        Ok(analyzer.analyze(&metrics))
    }
}

impl Default for PerformanceContext {
    fn default() -> Self {
        Self::new()
    }
}
