//! Recovery Tables
//!
//! This module implements the Transaction Table and Dirty Page Table used during
//! the ARIES recovery process. These tables are built during the Analysis phase
//! and used throughout the recovery process.

use crate::core::common::types::ids::PageId;
use crate::core::common::types::{Lsn, TransactionId};
use crate::core::recovery::types::TransactionInfo;
use std::collections::HashMap;

/// The Transaction Table tracks the state of all transactions during recovery.
///
/// This table is built during the Analysis phase by scanning the WAL and is used
/// during the Undo phase to determine which transactions need to be rolled back.
#[derive(Debug, Clone)]
pub struct TransactionTable {
    /// Map from transaction ID to transaction information
    transactions: HashMap<TransactionId, TransactionInfo>,
}

impl TransactionTable {
    /// Creates a new empty transaction table.
    pub fn new() -> Self {
        Self { transactions: HashMap::new() }
    }

    /// Adds or updates a transaction in the table.
    pub fn insert(&mut self, tx_info: TransactionInfo) {
        self.transactions.insert(tx_info.tx_id, tx_info);
    }

    /// Gets transaction information by ID.
    pub fn get(&self, tx_id: &TransactionId) -> Option<&TransactionInfo> {
        self.transactions.get(tx_id)
    }

    /// Gets mutable transaction information by ID.
    pub fn get_mut(&mut self, tx_id: &TransactionId) -> Option<&mut TransactionInfo> {
        self.transactions.get_mut(tx_id)
    }

    /// Removes a transaction from the table.
    pub fn remove(&mut self, tx_id: &TransactionId) -> Option<TransactionInfo> {
        self.transactions.remove(tx_id)
    }

    /// Returns true if the transaction table contains the given transaction ID.
    pub fn contains(&self, tx_id: &TransactionId) -> bool {
        self.transactions.contains_key(tx_id)
    }

    /// Returns the number of transactions in the table.
    pub fn len(&self) -> usize {
        self.transactions.len()
    }

    /// Returns true if the transaction table is empty.
    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }

    /// Returns an iterator over all transactions.
    pub fn iter(&self) -> impl Iterator<Item = (&TransactionId, &TransactionInfo)> {
        self.transactions.iter()
    }

    /// Returns an iterator over all active transactions that need to be undone.
    pub fn active_transactions(&self) -> impl Iterator<Item = (&TransactionId, &TransactionInfo)> {
        self.transactions.iter().filter(|(_, tx_info)| tx_info.needs_undo())
    }

    /// Updates the last LSN for a transaction, creating it if it doesn't exist.
    pub fn update_transaction(&mut self, tx_id: TransactionId, lsn: Lsn) {
        match self.transactions.get_mut(&tx_id) {
            Some(tx_info) => tx_info.update_last_lsn(lsn),
            None => {
                let tx_info = TransactionInfo::new_active(tx_id, lsn);
                self.transactions.insert(tx_id, tx_info);
            }
        }
    }

    /// Marks a transaction as committed.
    pub fn commit_transaction(&mut self, tx_id: &TransactionId) -> bool {
        if let Some(tx_info) = self.transactions.get_mut(tx_id) {
            tx_info.commit();
            true
        } else {
            false
        }
    }

    /// Marks a transaction as aborted.
    pub fn abort_transaction(&mut self, tx_id: &TransactionId) -> bool {
        if let Some(tx_info) = self.transactions.get_mut(tx_id) {
            tx_info.abort();
            true
        } else {
            false
        }
    }

    /// Clears all transactions from the table.
    pub fn clear(&mut self) {
        self.transactions.clear();
    }
}

impl Default for TransactionTable {
    fn default() -> Self {
        Self::new()
    }
}

/// Information about a dirty page during recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirtyPageInfo {
    /// The page ID
    pub page_id: PageId,
    /// The LSN of the first log record that dirtied this page
    pub recovery_lsn: Lsn,
}

impl DirtyPageInfo {
    /// Creates a new DirtyPageInfo.
    pub fn new(page_id: PageId, recovery_lsn: Lsn) -> Self {
        Self { page_id, recovery_lsn }
    }
}

/// The Dirty Page Table tracks pages that may need to be redone during recovery.
///
/// This table is built during the Analysis phase and used during the Redo phase
/// to determine which pages need to have their changes reapplied.
#[derive(Debug, Clone)]
pub struct DirtyPageTable {
    /// Map from page ID to dirty page information
    pages: HashMap<PageId, DirtyPageInfo>,
}

impl DirtyPageTable {
    /// Creates a new empty dirty page table.
    pub fn new() -> Self {
        Self { pages: HashMap::new() }
    }

    /// Adds or updates a dirty page in the table.
    ///
    /// If the page is already in the table, the recovery LSN is updated only
    /// if the new LSN is smaller (earlier) than the existing one.
    pub fn insert(&mut self, page_id: PageId, recovery_lsn: Lsn) {
        match self.pages.get_mut(&page_id) {
            Some(page_info) => {
                if recovery_lsn < page_info.recovery_lsn {
                    page_info.recovery_lsn = recovery_lsn;
                }
            }
            None => {
                let page_info = DirtyPageInfo::new(page_id, recovery_lsn);
                self.pages.insert(page_id, page_info);
            }
        }
    }

    /// Gets dirty page information by page ID.
    pub fn get(&self, page_id: &PageId) -> Option<&DirtyPageInfo> {
        self.pages.get(page_id)
    }

    /// Removes a page from the dirty page table.
    pub fn remove(&mut self, page_id: &PageId) -> Option<DirtyPageInfo> {
        self.pages.remove(page_id)
    }

    /// Returns true if the dirty page table contains the given page ID.
    pub fn contains(&self, page_id: &PageId) -> bool {
        self.pages.contains_key(page_id)
    }

    /// Returns the number of dirty pages in the table.
    pub fn len(&self) -> usize {
        self.pages.len()
    }

    /// Returns true if the dirty page table is empty.
    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    /// Returns an iterator over all dirty pages.
    pub fn iter(&self) -> impl Iterator<Item = (&PageId, &DirtyPageInfo)> {
        self.pages.iter()
    }

    /// Returns the minimum recovery LSN across all dirty pages.
    ///
    /// This is used to determine the starting point for the Redo phase.
    pub fn min_recovery_lsn(&self) -> Option<Lsn> {
        self.pages.values().map(|page_info| page_info.recovery_lsn).min()
    }

    /// Returns all page IDs that have a recovery LSN less than or equal to the given LSN.
    pub fn pages_to_redo(&self, max_lsn: Lsn) -> Vec<PageId> {
        self.pages
            .values()
            .filter(|page_info| page_info.recovery_lsn <= max_lsn)
            .map(|page_info| page_info.page_id)
            .collect()
    }

    /// Clears all pages from the table.
    pub fn clear(&mut self) {
        self.pages.clear();
    }
}

impl Default for DirtyPageTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::common::types::ids::PageId;
    use crate::core::common::types::TransactionId;
    use crate::core::recovery::types::TransactionState;

    #[test]
    fn test_transaction_table_basic_operations() {
        let mut table = TransactionTable::new();
        let tx_id = TransactionId(123);
        let lsn = 100;

        // Test insertion
        let tx_info = TransactionInfo::new_active(tx_id, lsn);
        table.insert(tx_info.clone());
        assert_eq!(table.len(), 1);
        assert!(table.contains(&tx_id));

        // Test retrieval
        let retrieved = table.get(&tx_id).unwrap();
        assert_eq!(retrieved.tx_id, tx_id);
        assert_eq!(retrieved.last_lsn, lsn);
        assert_eq!(retrieved.state, TransactionState::Active);

        // Test removal
        let removed = table.remove(&tx_id).unwrap();
        assert_eq!(removed.tx_id, tx_id);
        assert_eq!(table.len(), 0);
        assert!(!table.contains(&tx_id));
    }

    #[test]
    fn test_transaction_table_update_operations() {
        let mut table = TransactionTable::new();
        let tx_id = TransactionId(123);

        // Test update_transaction creates new transaction
        table.update_transaction(tx_id, 100);
        assert_eq!(table.len(), 1);
        let tx_info = table.get(&tx_id).unwrap();
        assert_eq!(tx_info.last_lsn, 100);
        assert_eq!(tx_info.state, TransactionState::Active);

        // Test update_transaction updates existing transaction
        table.update_transaction(tx_id, 200);
        assert_eq!(table.len(), 1);
        let tx_info = table.get(&tx_id).unwrap();
        assert_eq!(tx_info.last_lsn, 200);

        // Test commit_transaction
        assert!(table.commit_transaction(&tx_id));
        let tx_info = table.get(&tx_id).unwrap();
        assert_eq!(tx_info.state, TransactionState::Committed);
        assert!(!tx_info.needs_undo());

        // Test commit_transaction on non-existent transaction
        let non_existent_tx = TransactionId(999);
        assert!(!table.commit_transaction(&non_existent_tx));
    }

    #[test]
    fn test_transaction_table_active_transactions() {
        let mut table = TransactionTable::new();

        // Add active transaction
        let active_tx = TransactionId(1);
        table.insert(TransactionInfo::new_active(active_tx, 100));

        // Add committed transaction
        let committed_tx = TransactionId(2);
        table.insert(TransactionInfo::new_committed(committed_tx, 200));

        // Add aborted transaction
        let aborted_tx = TransactionId(3);
        table.insert(TransactionInfo::new_aborted(aborted_tx, 300));

        let active_txs: Vec<_> = table.active_transactions().collect();
        assert_eq!(active_txs.len(), 1);
        assert_eq!(active_txs[0].0, &active_tx);
    }

    #[test]
    fn test_dirty_page_table_basic_operations() {
        let mut table = DirtyPageTable::new();
        let page_id = PageId(123);
        let recovery_lsn = 100;

        // Test insertion
        table.insert(page_id, recovery_lsn);
        assert_eq!(table.len(), 1);
        assert!(table.contains(&page_id));

        // Test retrieval
        let page_info = table.get(&page_id).unwrap();
        assert_eq!(page_info.page_id, page_id);
        assert_eq!(page_info.recovery_lsn, recovery_lsn);

        // Test removal
        let removed = table.remove(&page_id).unwrap();
        assert_eq!(removed.page_id, page_id);
        assert_eq!(table.len(), 0);
        assert!(!table.contains(&page_id));
    }

    #[test]
    fn test_dirty_page_table_lsn_updates() {
        let mut table = DirtyPageTable::new();
        let page_id = PageId(123);

        // Insert with initial LSN
        table.insert(page_id, 200);
        assert_eq!(table.get(&page_id).unwrap().recovery_lsn, 200);

        // Insert with smaller LSN (should update)
        table.insert(page_id, 100);
        assert_eq!(table.get(&page_id).unwrap().recovery_lsn, 100);

        // Insert with larger LSN (should not update)
        table.insert(page_id, 300);
        assert_eq!(table.get(&page_id).unwrap().recovery_lsn, 100);
    }

    #[test]
    fn test_dirty_page_table_min_recovery_lsn() {
        let mut table = DirtyPageTable::new();

        // Empty table should return None
        assert_eq!(table.min_recovery_lsn(), None);

        // Add pages with different recovery LSNs
        table.insert(PageId(1), 300);
        table.insert(PageId(2), 100);
        table.insert(PageId(3), 200);

        // Should return the minimum LSN
        assert_eq!(table.min_recovery_lsn(), Some(100));
    }

    #[test]
    fn test_dirty_page_table_pages_to_redo() {
        let mut table = DirtyPageTable::new();

        table.insert(PageId(1), 100);
        table.insert(PageId(2), 200);
        table.insert(PageId(3), 300);

        let pages_to_redo = table.pages_to_redo(200);
        assert_eq!(pages_to_redo.len(), 2);
        assert!(pages_to_redo.contains(&PageId(1)));
        assert!(pages_to_redo.contains(&PageId(2)));
        assert!(!pages_to_redo.contains(&PageId(3)));
    }
}
