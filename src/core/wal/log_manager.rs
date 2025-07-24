// src/core/wal/log_manager.rs
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::core::common::types::Lsn; // Assuming Lsn is u64, or define it here/common

/// Manages the allocation of Log Sequence Numbers (LSNs).
/// LSNs are unique and monotonically increasing.
#[derive(Debug)]
pub struct LogManager {
    /// Atomic counter for generating unique Log Sequence Numbers
    lsn_counter: Arc<AtomicU64>,
    // Potentially, a handle to the WalWriter or other components if needed later.
}

impl LogManager {
    /// Creates a new `LogManager`.
    /// LSNs will start from 0.
    #[must_use]
    pub fn new() -> Self {
        Self { lsn_counter: Arc::new(AtomicU64::new(0)) }
    }

    /// Allocates and returns a new LSN.
    #[must_use]
    pub fn next_lsn(&self) -> Lsn {
        // For now, LSN is u64. A specific Lsn type might be introduced later.
        self.lsn_counter.fetch_add(1, Ordering::SeqCst)
    }

    /// Returns the current LSN without incrementing it.
    /// Useful for knowing what the next LSN *would be* or the last assigned one (if called after `next_lsn`).
    /// Note: another thread might have already incremented it.
    #[must_use]
    pub fn current_lsn(&self) -> Lsn {
        self.lsn_counter.load(Ordering::SeqCst)
    }
}

impl Default for LogManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_lsn_starts_from_zero_and_increments() {
        let log_manager = LogManager::new();
        assert_eq!(log_manager.next_lsn(), 0);
        assert_eq!(log_manager.next_lsn(), 1);
        assert_eq!(log_manager.next_lsn(), 2);
    }

    #[test]
    fn test_current_lsn() {
        let log_manager = LogManager::new();
        assert_eq!(log_manager.current_lsn(), 0);
        let _ = log_manager.next_lsn();
        assert_eq!(log_manager.current_lsn(), 1);
        let _ = log_manager.next_lsn();
        assert_eq!(log_manager.current_lsn(), 2);
    }
}
