use crate::core::transaction::{Transaction, TransactionState};
use std::collections::HashMap;

#[derive(Debug)]
pub struct TransactionManager {
    active_transactions: HashMap<u64, Transaction>,
    next_transaction_id: u64,
    current_active_transaction_id: Option<u64>,
    committed_tx_ids: Vec<u64>, // Added field
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionManager {
    pub fn new() -> Self {
        TransactionManager {
            active_transactions: HashMap::new(),
            next_transaction_id: 1,
            current_active_transaction_id: None,
            committed_tx_ids: Vec::new(), // Initialize new field
        }
    }

    pub fn generate_tx_id(&mut self) -> u64 {
        let id = self.next_transaction_id;
        self.next_transaction_id += 1;
        id
    }

    pub fn begin_transaction(&mut self) -> Transaction {
        let id = self.generate_tx_id();
        let transaction = Transaction::new(id);
        self.active_transactions.insert(id, transaction.clone());
        self.current_active_transaction_id = Some(id);
        transaction
    }

    pub fn get_active_transaction(&self) -> Option<&Transaction> {
        self.current_active_transaction_id.and_then(|id| self.active_transactions.get(&id))
    }

    pub fn get_active_transaction_mut(&mut self) -> Option<&mut Transaction> {
        self.current_active_transaction_id.and_then(move |id| self.active_transactions.get_mut(&id))
    }

    pub fn current_active_transaction_id(&self) -> Option<u64> {
        self.current_active_transaction_id
    }

    pub fn commit_transaction(&mut self) {
        if let Some(id) = self.current_active_transaction_id.take() {
            // take() sets current_active_transaction_id to None
            if let Some(mut transaction) = self.active_transactions.remove(&id) {
                transaction.set_state(TransactionState::Committed);
                self.committed_tx_ids.push(id); // Add to committed list
                                                // The transaction (and its undo_log) is removed from active_transactions.
                                                // If it were to be kept for inspection, its undo_log should be cleared here.
            }
            // current_active_transaction_id is already None due to take()
        }
    }

    pub fn is_committed(&self, tx_id: u64) -> bool {
        // Assumes committed_tx_ids is sorted because tx IDs are monotonic and pushed in order.
        self.committed_tx_ids.binary_search(&tx_id).is_ok()
    }

    pub fn get_committed_tx_ids_snapshot(&self) -> Vec<u64> {
        self.committed_tx_ids.clone()
    }

    // Method to explicitly add a transaction ID to the committed list.
    // This is useful for auto-commit scenarios handled outside of begin/commit commands.
    pub fn add_committed_tx_id(&mut self, tx_id: u64) {
        // Could add a check to ensure it's not already there or respect order,
        // but for auto-commit ID 0, it's likely fine.
        // For simplicity, just push. If order matters for binary_search in is_committed,
        // and auto-commit IDs can be arbitrary, this might need sorting or a Set.
        // Given current usage (ID 0), direct push and then sort/dedup if needed is an option.
        // Or, ensure `is_committed` handles potential disorder if non-monotonic IDs are added.
        if !self.committed_tx_ids.contains(&tx_id) {
            // Avoid duplicates for sanity
            self.committed_tx_ids.push(tx_id);
            // If order is strictly required for is_committed's binary_search, sort here.
            // self.committed_tx_ids.sort_unstable();
        }
    }

    pub fn get_oldest_active_tx_id(&self) -> Option<u64> {
        self.active_transactions.values().map(|tx| tx.id).min()
    }

    pub fn get_next_transaction_id_peek(&self) -> u64 {
        self.next_transaction_id
    }

    pub fn rollback_transaction(&mut self) {
        if let Some(id) = self.current_active_transaction_id.take() {
            // take() sets current_active_transaction_id to None
            if let Some(mut transaction) = self.active_transactions.remove(&id) {
                transaction.set_state(TransactionState::Aborted);
                // The transaction (and its undo_log) is removed.
                // Executor is responsible for using the undo_log before this.
            }
            // current_active_transaction_id is already None due to take()
        }
    }
}
