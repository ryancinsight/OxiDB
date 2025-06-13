use super::{ExecutionResult, QueryExecutor};
use crate::core::common::OxidbError; // Changed
use crate::core::common::serialization::{deserialize_data_type, serialize_data_type};
use crate::core::query::commands::Key;
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::transaction::lock_manager::LockType;
use crate::core::transaction::transaction::{Transaction, TransactionState, UndoOperation};
use crate::core::types::DataType;
use std::collections::{HashMap, HashSet}; // Use super to refer to parent mod

impl<S: KeyValueStore<Vec<u8>, Vec<u8>>> QueryExecutor<S> {
    pub(crate) fn handle_insert(
        &mut self,
        key: Key,
        value: DataType,
    ) -> Result<ExecutionResult, OxidbError> { // Changed
        let committed_ids_snapshot: HashSet<u64> =
            self.transaction_manager.get_committed_tx_ids_snapshot().into_iter().collect();

        if let Some(active_tx_mut) = self.transaction_manager.get_active_transaction_mut() {
            let active_tx_id = active_tx_mut.id;
            self.lock_manager.acquire_lock(active_tx_id, &key, LockType::Exclusive)?;

            let current_value =
                self.store.read().unwrap().get(&key, active_tx_id, &committed_ids_snapshot)?;
            let undo_op = if let Some(old_val) = current_value {
                UndoOperation::RevertUpdate { key: key.clone(), old_value: old_val }
            // Or a specific RevertOverwrite
            } else {
                UndoOperation::RevertInsert { key: key.clone() }
            };
            active_tx_mut.undo_log.push(undo_op);

            let serialized_value = serialize_data_type(&value)?;
            let tx_for_store = Transaction {
                id: active_tx_id,
                state: active_tx_mut.state.clone(),
                last_lsn: active_tx_mut.last_lsn, // Copy last_lsn from the active transaction
                undo_log: Vec::new(),
                redo_log: Vec::new(),
            };
            let put_result = self.store.write().unwrap().put(
                key.clone(),
                serialized_value.clone(),
                &tx_for_store,
            );

            if put_result.is_ok() {
                let mut indexed_values_map = HashMap::new();
                indexed_values_map
                    .insert("default_value_index".to_string(), serialized_value.clone());
                if let Err(index_err) = self.index_manager.on_insert_data(&indexed_values_map, &key)
                {
                    eprintln!("Index insert failed: {:?}, store put was successful but transaction will be hard to rollback fully here.", index_err);
                    return Err(OxidbError::Index(format!( // Changed
                        "Failed to update index after insert: {}",
                        index_err
                    )));
                }
                active_tx_mut.undo_log.push(UndoOperation::IndexRevertInsert {
                    index_name: "default_value_index".to_string(),
                    key: key.clone(),
                    value_for_index: serialized_value.clone(),
                });
                Ok(ExecutionResult::Success)
            } else {
                put_result.map(|_| ExecutionResult::Success)
            }
        } else {
            // Auto-commit for Insert
            let auto_commit_tx_id = 0; // Using 0 for auto-commit "transaction"
            match self.lock_manager.acquire_lock(auto_commit_tx_id, &key, LockType::Exclusive) {
                Ok(()) => {
                    let serialized_value = serialize_data_type(&value)?;
                    let mut tx_for_store = Transaction::new(auto_commit_tx_id); // Create a temporary transaction
                    let put_result = self.store.write().unwrap().put(
                        key.clone(),
                        serialized_value.clone(),
                        &tx_for_store,
                    );

                    if put_result.is_ok() {
                        let value_for_index = serialized_value;
                        let mut indexed_values_map = HashMap::new();
                        indexed_values_map
                            .insert("default_value_index".to_string(), value_for_index);

                        if let Err(index_err) =
                            self.index_manager.on_insert_data(&indexed_values_map, &key)
                        {
                            eprintln!(
                                "Failed to update index after insert (auto-commit): {:?}",
                                index_err
                            );
                            // Inconsistency: data in store, not in index. For now, log and continue.
                        }

                        tx_for_store.set_state(TransactionState::Committed); // Mark temporary tx as committed
                                                                             // Log commit to WAL
                        let commit_entry =
                            crate::core::storage::engine::wal::WalEntry::TransactionCommit {
                                transaction_id: auto_commit_tx_id,
                            };
                        self.store.write().unwrap().log_wal_entry(&commit_entry)?;
                        self.transaction_manager.add_committed_tx_id(auto_commit_tx_id); // Add to committed list
                        self.lock_manager.release_locks(auto_commit_tx_id);
                        Ok(ExecutionResult::Success)
                    } else {
                        self.lock_manager.release_locks(auto_commit_tx_id);
                        // No need to change tx_for_store state to Aborted as it's ephemeral
                        Err(put_result.unwrap_err())
                    }
                }
                Err(lock_err) => Err(lock_err),
            }
        }
    }

    pub(crate) fn handle_delete(&mut self, key: Key) -> Result<ExecutionResult, OxidbError> { // Changed
        let current_operation_tx_id;
        let committed_ids_snapshot_for_get;

        if let Some(active_tx) = self.transaction_manager.get_active_transaction() {
            current_operation_tx_id = active_tx.id;
            committed_ids_snapshot_for_get =
                self.transaction_manager.get_committed_tx_ids_snapshot().into_iter().collect();
        } else {
            current_operation_tx_id = 0; // Auto-commit tx id
            committed_ids_snapshot_for_get =
                self.transaction_manager.get_committed_tx_ids_snapshot().into_iter().collect();
        }

        let old_value_opt = self.store.read().unwrap().get(
            &key,
            current_operation_tx_id,
            &committed_ids_snapshot_for_get,
        )?;

        if let Some(active_tx) = self.transaction_manager.get_active_transaction_mut() {
            self.lock_manager.acquire_lock(active_tx.id, &key, LockType::Exclusive)?;

            if let Some(ref old_value) = old_value_opt {
                active_tx.undo_log.push(UndoOperation::RevertDelete {
                    key: key.clone(),
                    old_value: old_value.clone(),
                });
            }

            let tx_for_store = active_tx.clone(); // Clone relevant parts for store operation
            let delete_result = self.store.write().unwrap().delete(&key, &tx_for_store);

            if let Ok(deleted) = delete_result {
                if deleted {
                    if let Some(old_serialized_value_for_index) = old_value_opt {
                        let mut indexed_values_map = HashMap::new();
                        indexed_values_map.insert(
                            "default_value_index".to_string(),
                            old_serialized_value_for_index.clone(),
                        );
                        if let Err(index_err) =
                            self.index_manager.on_delete_data(&indexed_values_map, &key)
                        {
                            eprintln!("Index delete failed: {:?}, store delete was successful but transaction will be hard to rollback fully here.", index_err);
                            return Err(OxidbError::Index(format!( // Changed
                                "Failed to update index after delete: {}",
                                index_err
                            )));
                        }
                        active_tx.undo_log.push(UndoOperation::IndexRevertDelete {
                            index_name: "default_value_index".to_string(),
                            key: key.clone(),
                            old_value_for_index: old_serialized_value_for_index.clone(),
                        });
                    }
                }
                Ok(ExecutionResult::Deleted(deleted))
            } else {
                delete_result.map(ExecutionResult::Deleted)
            }
        } else {
            // Auto-commit for Delete
            let auto_commit_tx_id = 0;
            match self.lock_manager.acquire_lock(auto_commit_tx_id, &key, LockType::Exclusive) {
                Ok(()) => {
                    let mut tx_for_store = Transaction::new(auto_commit_tx_id);
                    let delete_result = self.store.write().unwrap().delete(&key, &tx_for_store);

                    if let Ok(deleted) = delete_result {
                        if deleted {
                            if let Some(old_serialized_value) = old_value_opt {
                                let mut indexed_values_map = HashMap::new();
                                indexed_values_map.insert(
                                    "default_value_index".to_string(),
                                    old_serialized_value,
                                );

                                if let Err(index_err) =
                                    self.index_manager.on_delete_data(&indexed_values_map, &key)
                                {
                                    eprintln!(
                                        "Failed to update index after delete (auto-commit): {:?}",
                                        index_err
                                    );
                                }
                            }
                        }
                        tx_for_store.set_state(TransactionState::Committed);
                        let commit_entry =
                            crate::core::storage::engine::wal::WalEntry::TransactionCommit {
                                transaction_id: auto_commit_tx_id,
                            };
                        self.store.write().unwrap().log_wal_entry(&commit_entry)?;
                        self.transaction_manager.add_committed_tx_id(auto_commit_tx_id);
                        self.lock_manager.release_locks(auto_commit_tx_id);
                        Ok(ExecutionResult::Deleted(deleted))
                    } else {
                        self.lock_manager.release_locks(auto_commit_tx_id);
                        Err(delete_result.unwrap_err())
                    }
                }
                Err(lock_err) => Err(lock_err),
            }
        }
    }

    pub(crate) fn handle_find_by_index(
        &mut self,
        index_name: String,
        value: Vec<u8>, // This is the serialized form of the value being searched
    ) -> Result<ExecutionResult, OxidbError> { // Changed
        let candidate_keys = match self.index_manager.find_by_index(&index_name, &value) {
            Ok(Some(keys)) => keys,
            Ok(None) => Vec::new(),
            Err(e) => return Err(e),
        };

        if candidate_keys.is_empty() {
            return Ok(ExecutionResult::Values(Vec::new()));
        }

        let snapshot_id;
        let committed_ids_vec;

        if let Some(active_tx) = self.transaction_manager.get_active_transaction() {
            snapshot_id = active_tx.id;
            committed_ids_vec = self.transaction_manager.get_committed_tx_ids_snapshot();
            // Consider if a shared lock is needed for each key during read
        } else {
            snapshot_id = self.transaction_manager.generate_tx_id();
            committed_ids_vec = self.transaction_manager.get_committed_tx_ids_snapshot();
        }

        let committed_ids: HashSet<u64> =
            committed_ids_vec.into_iter().filter(|id| *id <= snapshot_id).collect();

        let mut results_vec = Vec::new();
        for primary_key in candidate_keys {
            // Potentially acquire shared lock for primary_key here if in transaction
            match self.store.read().unwrap().get(&primary_key, snapshot_id, &committed_ids) {
                Ok(Some(serialized_data_from_store)) => {
                    // The `value` parameter to this function is the serialized indexed field's value.
                    // If the index ("default_value_index") stores the entire serialized DataType,
                    // then `serialized_data_from_store` should indeed be compared with `value`.
                    // However, this relies on the specific indexing strategy.
                    // For "default_value_index", it's assumed it indexes the serialized DataType.
                    if serialized_data_from_store == value {
                        // This comparison logic might need adjustment based on what the index actually stores
                        match deserialize_data_type(&serialized_data_from_store) {
                            Ok(data_type) => results_vec.push(data_type),
                            Err(deserialize_err) => {
                                eprintln!(
                                    "Error deserializing data for key {:?}: {}",
                                    primary_key, deserialize_err
                                );
                                // Depending on strictness, might return Err(deserialize_err) or continue
                            }
                        }
                    }
                    // Else: Key from index points to data that doesn't match the indexed value anymore (e.g. updated).
                    // This can happen if the index is not perfectly in sync or if the query logic needs refinement.
                }
                Ok(None) => { /* Key from index not visible or gone under current snapshot, skip */
                }
                Err(e) => return Err(e), // This e is already OxidbError
            }
        }
        Ok(ExecutionResult::Values(results_vec))
    }
    pub(crate) fn handle_get(&mut self, key: Key) -> Result<ExecutionResult, OxidbError> { // Changed
        let snapshot_id;
        let committed_ids_vec;

        if let Some(active_tx) = self.transaction_manager.get_active_transaction() {
            snapshot_id = active_tx.id;
            committed_ids_vec = self.transaction_manager.get_committed_tx_ids_snapshot();
            self.lock_manager.acquire_lock(active_tx.id, &key, LockType::Shared)?;
        } else {
            snapshot_id = self.transaction_manager.generate_tx_id();
            committed_ids_vec = self.transaction_manager.get_committed_tx_ids_snapshot();
        }

        let committed_ids: HashSet<u64> =
            committed_ids_vec.into_iter().filter(|id| *id <= snapshot_id).collect();

        let get_result = self.store.read().unwrap().get(&key, snapshot_id, &committed_ids);
        match get_result {
            Ok(Some(bytes)) => match deserialize_data_type(&bytes) {
                Ok(data_type) => Ok(ExecutionResult::Value(Some(data_type))),
                Err(e) => Err(e),
            },
            Ok(None) => Ok(ExecutionResult::Value(None)),
            Err(e) => Err(e),
        }
    }
}
