use super::{ExecutionResult, QueryExecutor};
use crate::core::common::OxidbError; // Changed
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::transaction::transaction::{Transaction, UndoOperation}; // Removed TransactionState
use std::collections::{HashMap, HashSet}; // Use super to refer to parent mod

impl<S: KeyValueStore<Vec<u8>, Vec<u8>>> QueryExecutor<S> {
    pub(crate) fn handle_begin_transaction(&mut self) -> Result<ExecutionResult, OxidbError> { // Changed
        self.transaction_manager.begin_transaction();
        Ok(ExecutionResult::Success)
    }

    pub(crate) fn handle_commit_transaction(&mut self) -> Result<ExecutionResult, OxidbError> { // Changed
        if let Some(active_tx) = self.transaction_manager.get_active_transaction_mut() {
            let tx_id_to_release = active_tx.id;
            active_tx.redo_log.clear();
            active_tx.undo_log.clear();

            let commit_entry = crate::core::storage::engine::wal::WalEntry::TransactionCommit {
                transaction_id: tx_id_to_release,
            };
            self.store.write().unwrap().log_wal_entry(&commit_entry)?;

            self.lock_manager.release_locks(tx_id_to_release);
            self.transaction_manager.commit_transaction();
            Ok(ExecutionResult::Success)
        } else {
            Err(OxidbError::NoActiveTransaction) // Changed
        }
    }

    pub(crate) fn handle_rollback_transaction(&mut self) -> Result<ExecutionResult, OxidbError> { // Changed
        if let Some(active_tx) = self.transaction_manager.get_active_transaction_mut() {
            let tx_id_to_release = active_tx.id;

            let temp_transaction_for_undo = Transaction::new(tx_id_to_release);

            for undo_op in active_tx.undo_log.iter().rev() {
                match undo_op {
                    UndoOperation::RevertInsert { key } => {
                        self.store.write().unwrap().delete(key, &temp_transaction_for_undo)?;
                    }
                    UndoOperation::RevertUpdate { key, old_value } => {
                        self.store.write().unwrap().put(
                            key.clone(),
                            old_value.clone(),
                            &temp_transaction_for_undo,
                        )?;
                    }
                    UndoOperation::RevertDelete { key, old_value } => {
                        self.store.write().unwrap().put(
                            key.clone(),
                            old_value.clone(),
                            &temp_transaction_for_undo,
                        )?;
                    }
                    UndoOperation::IndexRevertInsert { index_name, key, value_for_index } => {
                        let mut indexed_values_map = HashMap::new();
                        indexed_values_map.insert(index_name.clone(), value_for_index.clone());
                        self.index_manager.on_delete_data(&indexed_values_map, key)?;
                    }
                    UndoOperation::IndexRevertDelete { index_name, key, old_value_for_index } => {
                        let mut indexed_values_map = HashMap::new();
                        indexed_values_map.insert(index_name.clone(), old_value_for_index.clone());
                        self.index_manager.on_insert_data(&indexed_values_map, key)?;
                    }
                }
            }
            active_tx.undo_log.clear();
            active_tx.redo_log.clear();

            let rollback_entry = crate::core::storage::engine::wal::WalEntry::TransactionRollback {
                transaction_id: tx_id_to_release,
            };
            self.store.write().unwrap().log_wal_entry(&rollback_entry)?;

            self.lock_manager.release_locks(tx_id_to_release);
            self.transaction_manager.rollback_transaction();
            Ok(ExecutionResult::Success)
        } else {
            Err(OxidbError::NoActiveTransaction) // Changed
        }
    }

    pub(crate) fn handle_vacuum(&mut self) -> Result<ExecutionResult, OxidbError> { // Changed
        let low_water_mark = self
            .transaction_manager
            .get_oldest_active_tx_id()
            .unwrap_or_else(|| self.transaction_manager.get_next_transaction_id_peek());

        let committed_ids: HashSet<u64> =
            self.transaction_manager.get_committed_tx_ids_snapshot().into_iter().collect();

        self.store.write().unwrap().gc(low_water_mark, &committed_ids)?;
        Ok(ExecutionResult::Success)
    }
}
