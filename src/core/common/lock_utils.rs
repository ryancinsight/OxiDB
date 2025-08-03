//! Lock error handling utilities following DRY principle
//!
//! This module provides common lock error conversions to reduce code duplication.

use crate::core::common::OxidbError;
use std::sync::{MutexGuard, PoisonError, RwLockReadGuard, RwLockWriteGuard};

/// Convert a poisoned mutex error to OxidbError with a generic message
pub fn lock_poisoned<T>(_: PoisonError<MutexGuard<T>>) -> OxidbError {
    OxidbError::LockTimeout("Lock poisoned".to_string())
}

/// Convert a poisoned mutex error to OxidbError for store locks
pub fn store_lock_poisoned<T>(_: PoisonError<MutexGuard<T>>) -> OxidbError {
    OxidbError::LockTimeout("Failed to lock store".to_string())
}

/// Convert a poisoned read lock error to OxidbError
pub fn read_lock_poisoned<T>(_: PoisonError<RwLockReadGuard<T>>) -> OxidbError {
    OxidbError::LockTimeout("Failed to acquire read lock".to_string())
}

/// Convert a poisoned write lock error to OxidbError
pub fn write_lock_poisoned<T>(_: PoisonError<RwLockWriteGuard<T>>) -> OxidbError {
    OxidbError::LockTimeout("Failed to acquire write lock".to_string())
}

/// Convert a poisoned write lock error with context
pub fn write_lock_poisoned_with_context<T>(
    context: &str,
) -> impl Fn(PoisonError<RwLockWriteGuard<T>>) -> OxidbError + '_ {
    move |_| OxidbError::LockTimeout(format!("Failed to acquire write lock: {}", context))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    
    #[test]
    fn test_lock_error_messages() {
        // Create a mutex and poison it
        let mutex = Mutex::new(42);
        let _guard = mutex.lock().unwrap();
        
        // The error messages should be as expected
        assert_eq!(
            lock_poisoned::<i32>(PoisonError::new(MutexGuard::new(&mutex))).to_string(),
            "Lock Timeout: Lock poisoned"
        );
        
        assert_eq!(
            store_lock_poisoned::<i32>(PoisonError::new(MutexGuard::new(&mutex))).to_string(),
            "Lock Timeout: Failed to lock store"
        );
    }
}