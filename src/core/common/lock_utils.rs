//! Lock error handling utilities following DRY principle
//!
//! This module provides common lock error conversions to reduce code duplication.

use crate::core::common::OxidbError;
use std::sync::{MutexGuard, PoisonError, RwLockReadGuard, RwLockWriteGuard};

/// Error message constants to avoid string allocations
const LOCK_POISONED_MSG: &str = "Lock poisoned";
const STORE_LOCK_FAILED_MSG: &str = "Failed to lock store";
const READ_LOCK_FAILED_MSG: &str = "Failed to acquire read lock";
const WRITE_LOCK_FAILED_MSG: &str = "Failed to acquire write lock";

/// Convert a poisoned mutex error to OxidbError with a generic message
pub fn lock_poisoned<T>(_: PoisonError<MutexGuard<T>>) -> OxidbError {
    OxidbError::LockTimeout(LOCK_POISONED_MSG.to_string())
}

/// Convert a poisoned mutex error to OxidbError for store locks
pub fn store_lock_poisoned<T>(_: PoisonError<MutexGuard<T>>) -> OxidbError {
    OxidbError::LockTimeout(STORE_LOCK_FAILED_MSG.to_string())
}

/// Convert a poisoned read lock error to OxidbError
pub fn read_lock_poisoned<T>(_: PoisonError<RwLockReadGuard<T>>) -> OxidbError {
    OxidbError::LockTimeout(READ_LOCK_FAILED_MSG.to_string())
}

/// Convert a poisoned write lock error to OxidbError
pub fn write_lock_poisoned<T>(_: PoisonError<RwLockWriteGuard<T>>) -> OxidbError {
    OxidbError::LockTimeout(WRITE_LOCK_FAILED_MSG.to_string())
}

/// Convert a poisoned write lock error with context
pub fn write_lock_poisoned_with_context<T>(
    context: &str,
) -> impl Fn(PoisonError<RwLockWriteGuard<T>>) -> OxidbError + '_ {
    move |_| OxidbError::LockTimeout(format!("Failed to acquire write lock: {context}"))
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
        // Create a guard by locking the mutex
        let guard = mutex.lock().unwrap();

        assert_eq!(
            lock_poisoned::<i32>(PoisonError::new(guard)).to_string(),
            "Lock Timeout: Lock poisoned"
        );

        // Lock again for the second test
        let guard2 = mutex.lock().unwrap();
        assert_eq!(
            store_lock_poisoned::<i32>(PoisonError::new(guard2)).to_string(),
            "Lock Timeout: Failed to lock store"
        );
    }
}
