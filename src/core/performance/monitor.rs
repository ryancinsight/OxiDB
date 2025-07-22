//! Real-time performance monitoring and alerting

use std::time::Duration;

/// Real-time performance monitor
#[derive(Debug)]
pub struct PerformanceMonitor {
    /// Current monitoring configuration
    config: MonitoringConfig,
    /// Active alerts
    active_alerts: Vec<String>,
}

impl PerformanceMonitor {
    /// Create a new performance monitor
    #[must_use] pub fn new() -> Self {
        Self {
            config: MonitoringConfig::default(),
            active_alerts: Vec::new(),
        }
    }

    /// Create monitor with custom configuration
    #[must_use] pub const fn with_config(config: MonitoringConfig) -> Self {
        Self {
            config,
            active_alerts: Vec::new(),
        }
    }

    /// Check for performance alerts
    #[must_use] pub fn check_alerts(&self) -> Vec<String> {
        self.active_alerts.clone()
    }

    /// Add a new alert
    pub fn add_alert(&mut self, message: String, level: AlertLevel) {
        if level >= self.config.min_alert_level {
            self.active_alerts.push(format!("[{level:?}] {message}"));
        }
    }

    /// Clear all alerts
    pub fn clear_alerts(&mut self) {
        self.active_alerts.clear();
    }

    /// Update monitoring configuration
    pub fn update_config(&mut self, config: MonitoringConfig) {
        self.config = config;
    }
}

impl Default for PerformanceMonitor {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for performance monitoring
#[derive(Debug, Clone)]
pub struct MonitoringConfig {
    /// Whether to enable detailed profiling
    pub enable_profiling: bool,
    /// Whether to enable real-time monitoring
    pub enable_monitoring: bool,
    /// Minimum alert level to report
    pub min_alert_level: AlertLevel,
    /// Slow query threshold
    pub slow_query_threshold: Duration,
    /// High memory usage threshold (in MB)
    pub memory_threshold_mb: u64,
    /// High CPU usage threshold (as percentage)
    pub cpu_threshold_percent: f64,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enable_profiling: false,
            enable_monitoring: true,
            min_alert_level: AlertLevel::Warning,
            slow_query_threshold: Duration::from_secs(1),
            memory_threshold_mb: 1024, // 1GB
            cpu_threshold_percent: 80.0,
        }
    }
}

/// Alert severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AlertLevel {
    /// Informational message
    Info,
    /// Warning - attention needed
    Warning,
    /// Error - immediate attention required
    Error,
    /// Critical - system stability at risk
    Critical,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_performance_monitor() {
        let mut monitor = PerformanceMonitor::new();
        
        monitor.add_alert("Test alert".to_string(), AlertLevel::Warning);
        let alerts = monitor.check_alerts();
        
        assert_eq!(alerts.len(), 1);
        assert!(alerts[0].contains("Test alert"));
    }

    #[test]
    fn test_alert_filtering() {
        let config = MonitoringConfig {
            min_alert_level: AlertLevel::Error,
            ..Default::default()
        };
        let mut monitor = PerformanceMonitor::with_config(config);
        
        // This should be filtered out
        monitor.add_alert("Warning message".to_string(), AlertLevel::Warning);
        // This should be included
        monitor.add_alert("Error message".to_string(), AlertLevel::Error);
        
        let alerts = monitor.check_alerts();
        assert_eq!(alerts.len(), 1);
        assert!(alerts[0].contains("Error message"));
    }

    #[test]
    fn test_monitoring_config() {
        let config = MonitoringConfig::default();
        
        assert!(config.enable_monitoring);
        assert!(!config.enable_profiling);
        assert_eq!(config.min_alert_level, AlertLevel::Warning);
        assert_eq!(config.slow_query_threshold, Duration::from_secs(1));
    }
}
