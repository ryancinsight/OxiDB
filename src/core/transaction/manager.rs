use std::collections::HashMap;
use crate::core::transaction::{Transaction, TransactionState};

#[derive(Debug)]
pub struct TransactionManager {
    active_transactions: HashMap<u64, Transaction>,
    next_transaction_id: u64,
    current_active_transaction_id: Option<u64>,
}

impl TransactionManager {
    pub fn new() -> Self {
        TransactionManager {
            active_transactions: HashMap::new(),
            next_transaction_id: 1,
            current_active_transaction_id: None,
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

    pub fn commit_transaction(&mut self) {
        if let Some(id) = self.current_active_transaction_id.take() { // take() sets current_active_transaction_id to None
            if let Some(mut transaction) = self.active_transactions.remove(&id) {
                transaction.set_state(TransactionState::Committed);
                // The transaction (and its undo_log) is removed from active_transactions.
                // If it were to be kept for inspection, its undo_log should be cleared here.
            }
            // current_active_transaction_id is already None due to take()
        }
    }

    pub fn rollback_transaction(&mut self) {
        if let Some(id) = self.current_active_transaction_id.take() { // take() sets current_active_transaction_id to None
            if let Some(mut transaction) = self.active_transactions.remove(&id) {
                transaction.set_state(TransactionState::Aborted);
                // The transaction (and its undo_log) is removed.
                // Executor is responsible for using the undo_log before this.
            }
            // current_active_transaction_id is already None due to take()
        }
    }
}
