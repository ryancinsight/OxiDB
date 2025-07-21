//! Result and Error Utilities
//! 
//! This module provides common utilities for handling Results and Errors
//! throughout the Oxidb codebase, following DRY principles.

use crate::core::common::OxidbError;
use std::fmt::Debug;

/// A trait for converting various error types into `OxidbError`
/// Follows SOLID's Dependency Inversion Principle by depending on abstractions
pub trait IntoOxidbError<T> {
    /// Convert the result into a Result<T, OxidbError>
    fn into_oxidb_error(self) -> Result<T, OxidbError>;
}

impl<T, E> IntoOxidbError<T> for Result<T, E>
where
    E: Into<OxidbError>,
{
    fn into_oxidb_error(self) -> Result<T, OxidbError> {
        self.map_err(Into::into)
    }
}

/// Extension trait for Result to provide common operations
/// Follows SOLID's Interface Segregation Principle
pub trait ResultExt<T, E> {
    /// Map error with context information
    fn with_context<F>(self, f: F) -> Result<T, OxidbError>
    where
        F: FnOnce() -> String;
    
    /// Convert to OxidbError with a static context
    fn with_static_context(self, context: &'static str) -> Result<T, OxidbError>;
}

impl<T, E> ResultExt<T, E> for Result<T, E>
where
    E: Debug,
{
    fn with_context<F>(self, f: F) -> Result<T, OxidbError>
    where
        F: FnOnce() -> String,
    {
        self.map_err(|e| OxidbError::Other(format!("{}: {:?}", f(), e)))
    }
    
    fn with_static_context(self, context: &'static str) -> Result<T, OxidbError> {
        self.map_err(|e| OxidbError::Other(format!("{}: {:?}", context, e)))
    }
}

/// Safe unwrapping utilities for testing
/// Follows YAGNI - only implement what's needed for tests
#[cfg(test)]
pub trait TestResultExt<T> {
    /// Unwrap for tests with better error messages
    fn unwrap_test(self) -> T;
    
    /// Unwrap with custom test message
    fn unwrap_test_with_msg(self, msg: &str) -> T;
}

#[cfg(test)]
impl<T> TestResultExt<T> for Result<T, OxidbError> {
    fn unwrap_test(self) -> T {
        match self {
            Ok(val) => val,
            Err(e) => panic!("Test failed with error: {:?}", e),
        }
    }
    
    fn unwrap_test_with_msg(self, msg: &str) -> T {
        match self {
            Ok(val) => val,
            Err(e) => panic!("Test failed: {}: {:?}", msg, e),
        }
    }
}

#[cfg(test)]
impl<T> TestResultExt<T> for Option<T> {
    fn unwrap_test(self) -> T {
        match self {
            Some(val) => val,
            None => panic!("Test failed: Option was None"),
        }
    }
    
    fn unwrap_test_with_msg(self, msg: &str) -> T {
        match self {
            Some(val) => val,
            None => panic!("Test failed: {}: Option was None", msg),
        }
    }
}

/// Retry utility for operations that may fail transiently
/// Follows KISS principle - simple retry logic
pub fn retry_with_backoff<T, F>(
    mut operation: F,
    max_attempts: usize,
    base_delay_ms: u64,
) -> Result<T, OxidbError>
where
    F: FnMut() -> Result<T, OxidbError>,
{
    let mut attempts = 0;
    loop {
        attempts += 1;
        match operation() {
            Ok(result) => return Ok(result),
            Err(e) if attempts >= max_attempts => return Err(e),
            Err(_) => {
                let delay = base_delay_ms * 2_u64.pow((attempts - 1) as u32);
                std::thread::sleep(std::time::Duration::from_millis(delay));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_into_oxidb_error() {
        let io_error: io::Result<i32> = Err(io::Error::new(io::ErrorKind::NotFound, "test"));
        let result = io_error.into_oxidb_error();
        assert!(result.is_err());
    }

    #[test]
    fn test_with_context() {
        let result: Result<i32, &str> = Err("test error");
        let contextual_result = result.with_context(|| "operation failed".to_string());
        assert!(contextual_result.is_err());
        if let Err(e) = contextual_result {
            assert!(format!("{:?}", e).contains("operation failed"));
        }
    }

    #[test]
    fn test_retry_with_backoff() {
        let mut attempt_count = 0;
        let result = retry_with_backoff(
            || {
                attempt_count += 1;
                if attempt_count < 3 {
                    Err(OxidbError::Other("temporary failure".to_string()))
                } else {
                    Ok(42)
                }
            },
            3,
            1,
        );
        assert_eq!(result.unwrap_test(), 42);
        assert_eq!(attempt_count, 3);
    }
}