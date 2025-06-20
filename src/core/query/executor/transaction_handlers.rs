use super::{ExecutionResult, QueryExecutor};
use crate::core::common::OxidbError; // Changed
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::transaction::transaction::{Transaction, UndoOperation}; // Removed TransactionState
use std::collections::{HashMap, HashSet}; // Use super to refer to parent mod

impl<S: KeyValueStore<Vec<u8>, Vec<u8>>> QueryExecutor<S> {
    pub(crate) fn handle_begin_transaction(&mut self) -> Result<ExecutionResult, OxidbError> {
        self.transaction_manager.begin_transaction()?; // Use ? to handle the Result
        Ok(ExecutionResult::Success)
    }

    pub(crate) fn handle_commit_transaction(&mut self) -> Result<ExecutionResult, OxidbError> {
        // Changed
        if let Some(active_tx) = self.transaction_manager.get_active_transaction_mut() {
            let tx_id_to_release = active_tx.id;
            active_tx.redo_log.clear();
            active_tx.undo_log.clear();

            let lsn = self.log_manager.next_lsn();
            active_tx.prev_lsn = lsn; // Update active transaction's prev_lsn

            // Physical WAL entry for commit (uses u64 for transaction_id)
            let commit_entry = crate::core::storage::engine::wal::WalEntry::TransactionCommit {
                lsn,
                transaction_id: tx_id_to_release.0, // tx_id_to_release is TransactionId, .0 gives u64
            };
            self.store.write().unwrap().log_wal_entry(&commit_entry)?;

            self.lock_manager.release_locks(tx_id_to_release.0); // Pass u64 for release_locks
            self.transaction_manager.commit_transaction().map_err(OxidbError::Io)?;
            Ok(ExecutionResult::Success)
        } else {
            Err(OxidbError::NoActiveTransaction) // Changed
        }
    }

    pub(crate) fn handle_rollback_transaction(&mut self) -> Result<ExecutionResult, OxidbError> {
        // Changed
        let tx_id_to_release = if let Some(tx) = self.transaction_manager.get_active_transaction() {
            tx.id
        } else {
            return Err(OxidbError::NoActiveTransaction);
        };

        // For rollback, the committed_ids set should be those transactions committed *before* this one.
        // The current transaction (tx_id_to_release) is NOT committed.
        let committed_ids_for_undo: HashSet<u64> = self.transaction_manager
            .get_committed_tx_ids_snapshot()
            .into_iter()
            .map(|id| id.0) // Convert TransactionId to u64
            .filter(|&id| id != tx_id_to_release.0) // Ensure current tx_id is not in this set
            .collect();

        if let Some(active_tx) = self.transaction_manager.get_active_transaction_mut() {
            // Ensure active_tx.id is indeed tx_id_to_release (it should be, barring concurrent modification)
            assert_eq!(active_tx.id, tx_id_to_release, "Mismatch in transaction ID during rollback prep");

            eprintln!("[QueryExecutor::handle_rollback_transaction] Rolling back TX ID: {:?}, Undo Log: {:?}", tx_id_to_release, active_tx.undo_log);
            let temp_transaction_for_undo = Transaction::new(tx_id_to_release);

            for undo_op in active_tx.undo_log.iter().rev() {
                match undo_op {
                    UndoOperation::RevertInsert { key } => {
                        let lsn = self.log_manager.next_lsn();
                        self.store.write().unwrap().delete(key, &temp_transaction_for_undo, lsn, &committed_ids_for_undo)?;
                    }
                    UndoOperation::RevertUpdate { key, old_value: _ } => { // old_value is used for index, not directly here for store
                        let lsn = self.log_manager.next_lsn();
                        // This delete operation finds the version created by temp_transaction_for_undo (the transaction being rolled back)
                        // and marks its expired_tx_id to its own transaction ID.
                        // This correctly invalidates the version created by the transaction being rolled back.
                        // The previously existing version (which was expired by this transaction) will become visible again
                        // because its expirer_tx_id points to a non-committed transaction.
                        self.store.write().unwrap().delete(
                            key,
                            &temp_transaction_for_undo, // The transaction being rolled back
                            lsn,
                            &committed_ids_for_undo,
                        )?;
                    }
                    UndoOperation::RevertDelete { key, old_value } => {
                        let lsn = self.log_manager.next_lsn();
                        self.store.write().unwrap().put(
                            key.clone(),
                            old_value.clone(),
                            &temp_transaction_for_undo,
                            lsn,
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
                    UndoOperation::IndexRevertUpdate { index_name, key, old_value_for_index, new_value_for_index } => {
                        // To revert an update in the index:
                        // 1. Delete the new value that was inserted.
                        let mut new_values_map = HashMap::new();
                        new_values_map.insert(index_name.clone(), new_value_for_index.clone());
                        self.index_manager.on_delete_data(&new_values_map, key)?;

                        // 2. Re-insert the old value.
                        let mut old_values_map = HashMap::new();
                        old_values_map.insert(index_name.clone(), old_value_for_index.clone());
                        self.index_manager.on_insert_data(&old_values_map, key)?;
                    }
                }
            }
            active_tx.undo_log.clear();
            active_tx.redo_log.clear();

            let lsn = self.log_manager.next_lsn();
            // Update prev_lsn of the transaction being rolled back.
            active_tx.prev_lsn = lsn; // Update actual transaction's prev_lsn

            // Physical WAL entry for rollback
            let rollback_entry = crate::core::storage::engine::wal::WalEntry::TransactionRollback {
                lsn,
                transaction_id: tx_id_to_release.0, // tx_id_to_release is TransactionId, .0 gives u64
            };
            self.store.write().unwrap().log_wal_entry(&rollback_entry)?;

            self.lock_manager.release_locks(tx_id_to_release.0); // Pass u64 for release_locks
            self.transaction_manager.abort_transaction().map_err(OxidbError::Io)?;
            Ok(ExecutionResult::Success)
        } else {
            Err(OxidbError::NoActiveTransaction) // Changed
        }
    }

    pub(crate) fn handle_vacuum(&mut self) -> Result<ExecutionResult, OxidbError> {
        // Changed
        let low_water_mark = self
            .transaction_manager
            .get_oldest_active_tx_id()
            .unwrap_or_else(|| self.transaction_manager.get_next_transaction_id_peek());

        // Map Vec<TransactionId> to HashSet<u64>
        let committed_ids: HashSet<u64> = self
            .transaction_manager
            .get_committed_tx_ids_snapshot()
            .into_iter()
            .map(|id| id.0) // Convert TransactionId to u64
            .collect();

        self.store.write().unwrap().gc(low_water_mark.0, &committed_ids)?; // Use low_water_mark.0
        Ok(ExecutionResult::Success)
    }
}
