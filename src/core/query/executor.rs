// src/core/query/executor.rs

use crate::core::common::error::DbError;
use crate::core::types::DataType;
use crate::core::common::serialization::{serialize_data_type, deserialize_data_type};
use crate::core::storage::engine::{SimpleFileKvStore, InMemoryKvStore};
use crate::core::query::commands::{Command, Key}; // Added Key import
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::indexing::manager::IndexManager; // Added for IndexManager
use std::path::PathBuf; // Added for PathBuf
use std::collections::{HashMap, HashSet}; // Added HashSet
use crate::core::transaction::{lock_manager::{LockManager, LockType}}; // Added LockType
use crate::core::transaction::manager::TransactionManager;
use crate::core::transaction::transaction::{Transaction, TransactionState, UndoOperation};

#[derive(Debug, PartialEq)]
pub enum ExecutionResult {
    Value(Option<DataType>),
    Success,
    Deleted(bool),
    Values(Vec<DataType>), // Changed from PrimaryKeys(Vec<Key>)
}

pub struct QueryExecutor<S: KeyValueStore<Vec<u8>, Vec<u8>>> {
    pub(crate) store: S,
    pub(crate) transaction_manager: TransactionManager,
    pub(crate) lock_manager: LockManager,
    pub(crate) index_manager: IndexManager, // Added index_manager field
}

impl<S: KeyValueStore<Vec<u8>, Vec<u8>>> QueryExecutor<S> {
    // Modified new method signature and body
    pub fn new(store: S, index_base_path: PathBuf) -> Result<Self, DbError> { // Added index_base_path and Result
        let mut index_manager = IndexManager::new(index_base_path)?;

        // Attempt to create a default index.
        // In a real system, this would be based on configuration or explicit commands.
        if index_manager.get_index("default_value_index").is_none() {
            index_manager.create_index("default_value_index".to_string(), "hash")
                .map_err(|e| DbError::IndexError(format!("Failed to create default_value_index: {}", e.to_string())))?;
        }

        let mut transaction_manager = TransactionManager::new();
        // Add transaction ID 0 as committed by default, representing the baseline disk state.
        transaction_manager.add_committed_tx_id(0);

        Ok(QueryExecutor {
            store,
            transaction_manager,
            lock_manager: LockManager::new(),
            index_manager,
        })
    }

    pub fn execute_command(&mut self, command: Command) -> Result<ExecutionResult, DbError> {
        match command {
            Command::Insert { key, value } => {
                // Determine committed snapshot *before* potentially acquiring mutable borrow for active_tx
                let committed_ids_snapshot: HashSet<u64> = self.transaction_manager.get_committed_tx_ids_snapshot().into_iter().collect();

                if let Some(active_tx_mut) = self.transaction_manager.get_active_transaction_mut() {
                    let active_tx_id = active_tx_mut.id; // Get ID before further borrows
                    self.lock_manager.acquire_lock(active_tx_id, &key, LockType::Exclusive)?;
                    
                    // For Insert, the "current_value" for undo log should see what was committed before this tx started.
                    // So, snapshot_id is active_tx.id, and committed_ids are those committed *before* this tx.
                    // The committed_ids_snapshot is taken before this operation, which is good.
                    // The active_tx_id is the snapshot view for this read-for-undo.
                    let current_value = self.store.get(&key, active_tx_id, &committed_ids_snapshot)?;
                    let undo_op = if let Some(old_val) = current_value {
                        UndoOperation::RevertUpdate { key: key.clone(), old_value: old_val }
                    } else {
                        UndoOperation::RevertInsert { key: key.clone() }
                    };
                    active_tx_mut.undo_log.push(undo_op);
                    
                    let serialized_value = serialize_data_type(&value)?;
                    // Clone the immutable parts of active_tx_mut for store operation
                    let tx_for_store = Transaction {
                        id: active_tx_id,
                        state: active_tx_mut.state.clone(),
                        undo_log: Vec::new(), // The store doesn't need the undo log for put
                        redo_log: Vec::new(), // Add missing redo_log field
                    };
                    let put_result = self.store.put(key.clone(), serialized_value.clone(), &tx_for_store);

                    if put_result.is_ok() {
                        // Perform index update immediately
                        let mut indexed_values_map = HashMap::new();
                        // Assuming "default_value_index" and serialized_value is what's indexed.
                        // This needs to be more generic if multiple indexes or different value parts are indexed.
                        indexed_values_map.insert("default_value_index".to_string(), serialized_value.clone());
                        if let Err(index_err) = self.index_manager.on_insert_data(&indexed_values_map, &key) {
                            // If index update fails, the transaction should ideally roll back the store.put.
                            // This is complex. For now, let's make it an error.
                            // A more robust system might try to undo the put or mark the transaction for rollback.
                            eprintln!("Index insert failed: {:?}, store put was successful but transaction will be hard to rollback fully here.", index_err);
                            return Err(DbError::IndexError(format!("Failed to update index after insert: {}", index_err)));
                        }
                        // Add to undo log for index operation
                        active_tx_mut.undo_log.push(UndoOperation::IndexRevertInsert {
                            index_name: "default_value_index".to_string(), // Assuming default index
                            key: key.clone(),
                            value_for_index: serialized_value.clone(),
                        });
                        Ok(ExecutionResult::Success)
                    } else {
                        put_result.map(|_| ExecutionResult::Success) // Propagate original error
                    }
                } else {
                    // Auto-commit for Insert
                    let auto_commit_tx_id = 0;
                    match self.lock_manager.acquire_lock(auto_commit_tx_id, &key, LockType::Exclusive) {
                        Ok(()) => {
                            let serialized_value = serialize_data_type(&value)?;
                            let mut tx_for_store = Transaction::new(auto_commit_tx_id);
                            let put_result = self.store.put(key.clone(), serialized_value.clone(), &tx_for_store);

                            if put_result.is_ok() {
                                // BEGIN INDEX UPDATE (auto-commit)
                                let value_for_index = serialized_value; // This is Vec<u8>
                                let mut indexed_values_map = HashMap::new();
                                indexed_values_map.insert("default_value_index".to_string(), value_for_index);

                                if let Err(index_err) = self.index_manager.on_insert_data(&indexed_values_map, &key) {
                                    eprintln!("Failed to update index after insert (auto-commit): {:?}", index_err);
                                    // This is auto-commit, so the store.put is already done.
                                    // If index fails, data is in store but not index. Inconsistency.
                                    // For now, log and continue.
                                }
                                // END INDEX UPDATE

                                tx_for_store.set_state(TransactionState::Committed);
                                let commit_entry = crate::core::storage::engine::wal::WalEntry::TransactionCommit { transaction_id: auto_commit_tx_id };
                                self.store.log_wal_entry(&commit_entry)?;
                                self.transaction_manager.add_committed_tx_id(auto_commit_tx_id); // Add to committed list
                                self.lock_manager.release_locks(auto_commit_tx_id);
                                Ok(ExecutionResult::Success)
                            } else {
                                self.lock_manager.release_locks(auto_commit_tx_id);
                                tx_for_store.set_state(TransactionState::Aborted);
                                Err(put_result.unwrap_err())
                            }
                        }
                        Err(lock_err) => Err(lock_err),
                    }
                }
            }
            Command::Get { key } => {
                let snapshot_id;
                let committed_ids_vec;

                if let Some(active_tx) = self.transaction_manager.get_active_transaction() { 
                    snapshot_id = active_tx.id;
                    committed_ids_vec = self.transaction_manager.get_committed_tx_ids_snapshot();
                    // Acquire shared lock for read consistency if in an active transaction
                    self.lock_manager.acquire_lock(active_tx.id, &key, LockType::Shared)?;
                } else { // Auto-commit for Get
                    // Auto-commit Get acts as its own short transaction.
                    // It should see all data committed up to the point it starts.
                    // For auto-commit reads, we typically don't acquire locks in this simplified model,
                    // or it would be a very short-lived lock if we did.
                    // The test "test_exclusive_lock_prevents_shared_read" implies locking even for reads within a TX.
                    // We generate a temporary ID for it to define its snapshot point.
                    // Note: This ID is not stored in TransactionManager's active/committed lists.
                    snapshot_id = self.transaction_manager.generate_tx_id();
                    committed_ids_vec = self.transaction_manager.get_committed_tx_ids_snapshot();
                }

                let committed_ids: HashSet<u64> = committed_ids_vec.into_iter().filter(|id| *id <= snapshot_id).collect();

                let get_result = self.store.get(&key, snapshot_id, &committed_ids);
                match get_result {
                    Ok(Some(bytes)) => {
                        match deserialize_data_type(&bytes) {
                            Ok(data_type) => Ok(ExecutionResult::Value(Some(data_type))),
                            Err(e) => Err(e),
                        }
                    }
                    Ok(None) => Ok(ExecutionResult::Value(None)),
                    Err(e) => Err(e),
                }
            }
            Command::Delete { key } => {
                let current_operation_tx_id;
                let committed_ids_snapshot_for_get;

                if let Some(active_tx) = self.transaction_manager.get_active_transaction() {
                    current_operation_tx_id = active_tx.id;
                    committed_ids_snapshot_for_get = self.transaction_manager.get_committed_tx_ids_snapshot().into_iter().collect();
                } else {
                    current_operation_tx_id = 0; // Auto-commit tx id
                    committed_ids_snapshot_for_get = self.transaction_manager.get_committed_tx_ids_snapshot().into_iter().collect();
                }

                // Fetch the value *before* deleting it from the store for index update.
                // This read should see what's visible to the current transaction before its own changes.
                let old_value_opt = self.store.get(&key, current_operation_tx_id, &committed_ids_snapshot_for_get)?;

                if let Some(active_tx) = self.transaction_manager.get_active_transaction_mut() {
                    self.lock_manager.acquire_lock(active_tx.id, &key, LockType::Exclusive)?;

                    if let Some(ref old_value) = old_value_opt { // Use ref here
                        active_tx.undo_log.push(UndoOperation::RevertDelete { key: key.clone(), old_value: old_value.clone() });
                    }
                    
                    let tx_for_store = active_tx.clone();
                    let delete_result = self.store.delete(&key, &tx_for_store);

                    if let Ok(deleted) = delete_result {
                        if deleted {
                            if let Some(old_serialized_value_for_index) = old_value_opt {
                                // Perform index update immediately
                                let mut indexed_values_map = HashMap::new();
                                // Assuming "default_value_index" and old_serialized_value_for_index is what was indexed.
                                indexed_values_map.insert("default_value_index".to_string(), old_serialized_value_for_index.clone());
                                if let Err(index_err) = self.index_manager.on_delete_data(&indexed_values_map, &key) {
                                    eprintln!("Index delete failed: {:?}, store delete was successful but transaction will be hard to rollback fully here.", index_err);
                                    return Err(DbError::IndexError(format!("Failed to update index after delete: {}", index_err)));
                                }
                                // Add to undo log for index operation
                                active_tx.undo_log.push(UndoOperation::IndexRevertDelete {
                                    index_name: "default_value_index".to_string(), // Assuming default index
                                    key: key.clone(),
                                    old_value_for_index: old_serialized_value_for_index.clone(),
                                });
                            }
                        }
                        Ok(ExecutionResult::Deleted(deleted))
                    } else {
                        delete_result.map(ExecutionResult::Deleted) // Propagate original error
                    }
                } else {
                    // Auto-commit for Delete
                    let auto_commit_tx_id = 0;
                    match self.lock_manager.acquire_lock(auto_commit_tx_id, &key, LockType::Exclusive) {
                        Ok(()) => {
                            let mut tx_for_store = Transaction::new(auto_commit_tx_id);
                            let delete_result = self.store.delete(&key, &tx_for_store);

                            if let Ok(deleted) = delete_result {
                                if deleted {
                                    if let Some(old_serialized_value) = old_value_opt {
                                        // BEGIN INDEX UPDATE (auto-commit)
                                        let mut indexed_values_map = HashMap::new();
                                        indexed_values_map.insert("default_value_index".to_string(), old_serialized_value);

                                        if let Err(index_err) = self.index_manager.on_delete_data(&indexed_values_map, &key) {
                                            eprintln!("Failed to update index after delete (auto-commit): {:?}", index_err);
                                        }
                                        // END INDEX UPDATE
                                    }
                                }
                                tx_for_store.set_state(TransactionState::Committed);
                                let commit_entry = crate::core::storage::engine::wal::WalEntry::TransactionCommit { transaction_id: auto_commit_tx_id };
                                self.store.log_wal_entry(&commit_entry)?;
                                self.transaction_manager.add_committed_tx_id(auto_commit_tx_id); // Add to committed list
                                self.lock_manager.release_locks(auto_commit_tx_id);
                                Ok(ExecutionResult::Deleted(deleted))
                            } else {
                                self.lock_manager.release_locks(auto_commit_tx_id);
                                tx_for_store.set_state(TransactionState::Aborted);
                                Err(delete_result.unwrap_err())
                            }
                        }
                        Err(lock_err) => Err(lock_err),
                    }
                }
            }
            Command::FindByIndex { index_name, value } => {
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
                } else {
                    snapshot_id = self.transaction_manager.generate_tx_id(); // Generate temporary ID for snapshot
                    committed_ids_vec = self.transaction_manager.get_committed_tx_ids_snapshot();
                }

                let committed_ids: HashSet<u64> = committed_ids_vec.into_iter().filter(|id| *id <= snapshot_id).collect();

                let mut results_vec = Vec::new();
                for primary_key in candidate_keys {
                    match self.store.get(&primary_key, snapshot_id, &committed_ids) {
                        Ok(Some(serialized_data_from_store)) => {
                            // `value` is the query parameter (serialized form of the indexed field).
                            // `serialized_data_from_store` is the serialized form of the entire DataType object.
                            // This check is only correct if the indexed value IS the entire serialized DataType.
                            // If only a specific field of DataType was indexed, this comparison needs refinement.
                            // For "default_value_index", we assume it indexed the serialized DataType.
                            if serialized_data_from_store == value {
                                match deserialize_data_type(&serialized_data_from_store) {
                                    Ok(data_type) => results_vec.push(data_type),
                                    Err(deserialize_err) => {
                                        // Log error or handle as appropriate for your application
                                        eprintln!("Error deserializing data for key {:?}: {}", primary_key, deserialize_err);
                                        // Depending on strictness, might return Err(deserialize_err) or continue
                                    }
                                }
                            }
                        }
                        Ok(None) => { /* Key from index not visible or gone under current snapshot, skip */ }
                        Err(e) => return Err(e), // Propagate store error
                    }
                }
                Ok(ExecutionResult::Values(results_vec))
            }
            Command::BeginTransaction => {
                self.transaction_manager.begin_transaction(); // Consider if a previous active tx should be auto-committed/rolled_back
                Ok(ExecutionResult::Success)
            }
            Command::CommitTransaction => {
                if let Some(active_tx) = self.transaction_manager.get_active_transaction_mut() {
                    let tx_id_to_release = active_tx.id;
                    // Index updates are now immediate, so redo_log for indexes is not processed here for commit.
                    // Redo log might still be relevant for WAL-based recovery if needed in future.
                    // For now, we primarily rely on undo log for rollback.
                    active_tx.redo_log.clear(); // Clear any redo log (though we stopped adding index ops to it)
                    active_tx.undo_log.clear(); // Undo log is cleared after successful commit.

                    // Log commit to WAL before releasing locks or finalizing commit in manager
                    let commit_entry = crate::core::storage::engine::wal::WalEntry::TransactionCommit { transaction_id: tx_id_to_release };
                    self.store.log_wal_entry(&commit_entry)?;
                    
                    self.lock_manager.release_locks(tx_id_to_release);
                    self.transaction_manager.commit_transaction(); // This will remove the tx from active list
                    Ok(ExecutionResult::Success)
                } else {
                    Err(DbError::NoActiveTransaction)
                }
            }
            Command::RollbackTransaction => {
                if let Some(mut active_tx) = self.transaction_manager.get_active_transaction_mut() {
                    let tx_id_to_release = active_tx.id;
                    
                    // Perform undo operations first
                    let temp_transaction_for_undo = Transaction::new(tx_id_to_release); // State is Active

                    for undo_op in active_tx.undo_log.iter().rev() { // Iterate in reverse
                        match undo_op {
                            UndoOperation::RevertInsert { key } => {
                                self.store.delete(key, &temp_transaction_for_undo)?;
                            }
                            UndoOperation::RevertUpdate { key, old_value } => {
                                self.store.put(key.clone(), old_value.clone(), &temp_transaction_for_undo)?;
                            }
                            UndoOperation::RevertDelete { key, old_value } => {
                                self.store.put(key.clone(), old_value.clone(), &temp_transaction_for_undo)?;
                            }
                            UndoOperation::IndexRevertInsert { index_name, key, value_for_index } => {
                                // To revert an index insert, we need to delete the entry from the index.
                                // The on_delete_data method needs a map of {index_name: value_that_was_indexed}.
                                let mut indexed_values_map = HashMap::new();
                                indexed_values_map.insert(index_name.clone(), value_for_index.clone());
                                self.index_manager.on_delete_data(&indexed_values_map, key)?;
                            }
                            UndoOperation::IndexRevertDelete { index_name, key, old_value_for_index } => {
                                // To revert an index delete, we need to add the entry back to the index.
                                let mut indexed_values_map = HashMap::new();
                                indexed_values_map.insert(index_name.clone(), old_value_for_index.clone());
                                self.index_manager.on_insert_data(&indexed_values_map, key)?;
                            }
                        }
                    }
                    active_tx.undo_log.clear(); // Clear after processing
                    active_tx.redo_log.clear(); // Also clear redo log on rollback

                    // Log rollback to WAL before releasing locks or finalizing rollback in manager
                    let rollback_entry = crate::core::storage::engine::wal::WalEntry::TransactionRollback { transaction_id: tx_id_to_release };
                    self.store.log_wal_entry(&rollback_entry)?;

                    self.lock_manager.release_locks(tx_id_to_release);
                    self.transaction_manager.rollback_transaction(); // This will remove the tx from active list
                    Ok(ExecutionResult::Success)
                } else {
                    Err(DbError::NoActiveTransaction)
                }
            }
            Command::Vacuum => {
                let low_water_mark = self.transaction_manager.get_oldest_active_tx_id()
                    .unwrap_or_else(|| self.transaction_manager.get_next_transaction_id_peek());

                let committed_ids: HashSet<u64> = self.transaction_manager.get_committed_tx_ids_snapshot().into_iter().collect();

                self.store.gc(low_water_mark, &committed_ids)?;
                Ok(ExecutionResult::Success)
            }
        }
    }
}

// Methods specific to QueryExecutor when the store is SimpleFileKvStore
impl QueryExecutor<SimpleFileKvStore> {
    pub fn persist(&mut self) -> Result<(), DbError> {
        self.store.save_to_disk()?; // Save main data
        self.index_manager.save_all_indexes() // Save all indexes
    }
}

