//! Performance profiling and detailed operation tracking

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Performance profiler for detailed operation analysis
#[derive(Debug)]
pub struct PerformanceProfiler {
    /// Active profiled operations
    active_operations: HashMap<String, Instant>,
    /// Completed operation profiles
    completed_profiles: Vec<ProfileResult>,
}

impl PerformanceProfiler {
    /// Create a new profiler
    #[must_use]
    pub fn new() -> Self {
        Self { active_operations: HashMap::new(), completed_profiles: Vec::new() }
    }

    /// Start profiling an operation
    pub fn start_operation(&mut self, operation: &str) -> ProfiledOperation {
        let start_time = Instant::now();
        self.active_operations.insert(operation.to_string(), start_time);
        ProfiledOperation { operation: operation.to_string(), start_time }
    }

    /// Record an operation completion
    pub fn record_operation(&mut self, operation: &str, duration: Duration) {
        let result =
            ProfileResult { operation: operation.to_string(), duration, timestamp: Instant::now() };
        self.completed_profiles.push(result);
    }

    /// Get profiling results
    #[must_use]
    pub fn get_results(&self) -> &[ProfileResult] {
        &self.completed_profiles
    }

    /// Clear profiling data
    pub fn clear(&mut self) {
        self.active_operations.clear();
        self.completed_profiles.clear();
    }
}

impl Default for PerformanceProfiler {
    fn default() -> Self {
        Self::new()
    }
}

/// A profiled operation that tracks timing
#[derive(Debug)]
pub struct ProfiledOperation {
    /// Operation name
    pub operation: String,
    /// Start time
    pub start_time: Instant,
}

impl ProfiledOperation {
    /// Complete the profiled operation
    #[must_use]
    pub fn complete(self) -> ProfileResult {
        ProfileResult {
            operation: self.operation,
            duration: self.start_time.elapsed(),
            timestamp: Instant::now(),
        }
    }
}

/// Result of a profiled operation
#[derive(Debug, Clone)]
pub struct ProfileResult {
    /// Operation name
    pub operation: String,
    /// Duration of the operation
    pub duration: Duration,
    /// Timestamp when completed
    pub timestamp: Instant,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_profiler_operation() {
        let mut profiler = PerformanceProfiler::new();
        let operation_guard = profiler.start_operation("test_op");

        thread::sleep(Duration::from_millis(10));
        let result = operation_guard.complete();

        assert_eq!(result.operation, "test_op");
        assert!(result.duration >= Duration::from_millis(10));
    }

    #[test]
    fn test_profiler_recording() {
        let mut profiler = PerformanceProfiler::new();
        profiler.record_operation("test", Duration::from_millis(100));

        let results = profiler.get_results();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].operation, "test");
        assert_eq!(results[0].duration, Duration::from_millis(100));
    }
}
