// src/core/query/executor.rs

use crate::core::common::error::DbError;
use crate::core::types::DataType;
use crate::core::common::serialization::{serialize_data_type, deserialize_data_type};
use crate::core::storage::engine::SimpleFileKvStore; // Removed InMemoryKvStore
use crate::core::query::commands::{Command, Key, SelectColumnSpec}; // Added Key import, SelectColumnSpec
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::indexing::manager::IndexManager; // Added for IndexManager
use std::path::PathBuf; // Added for PathBuf
use std::collections::{HashMap, HashSet}; // Added HashSet
use std::sync::Arc; // Added for Arc<IndexManager>
use crate::core::transaction::{lock_manager::{LockManager, LockType}}; // Added LockType
use crate::core::transaction::manager::TransactionManager;
use crate::core::transaction::transaction::{Transaction, TransactionState, UndoOperation};
use crate::core::optimizer::QueryPlanNode; // Added import
use crate::core::execution::ExecutionOperator; // Added import
use crate::core::execution::Tuple; // Added import

#[derive(Debug, PartialEq)]
pub enum ExecutionResult {
    Value(Option<DataType>),
    Success,
    Deleted(bool),
    Values(Vec<DataType>), // Changed from PrimaryKeys(Vec<Key>)
}

#[derive(Debug)]
pub struct QueryExecutor<S: KeyValueStore<Vec<u8>, Vec<u8>>> {
    pub(crate) store: S,
    pub(crate) transaction_manager: TransactionManager,
    pub(crate) lock_manager: LockManager,
    pub(crate) index_manager: Arc<IndexManager>, // Changed to Arc<IndexManager>
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
            index_manager: Arc::new(index_manager), // Wrap in Arc
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
            Command::Select { columns, source, condition } => {
                // NOTE: The 'source' (table name) is currently ignored as this executor operates
                // on a global key-value space. True table-like behavior would require
                // namespacing keys or a different storage model.

                // FUNDAMENTAL LIMITATION: The current KeyValueStore trait does not support
                // iterating over all keys or scanning. Without this, a full SELECT operation
                // (especially without a WHERE clause that maps to specific GETs) is not truly feasible.
                // The following implementation simulates the logic assuming such iteration is possible
                // but will operate on an empty dataset for now. This section needs to be revisited
                // when store iteration capabilities are added.

                // Placeholder for data that would come from iterating the store.
                // For actual testing, this would need to be populated by a mock store or specific test setup.
                let all_data_placeholder: Vec<(Key, DataType)> = Vec::new();
                // Example of how it might be populated if store iteration existed:
                // let all_keys = self.store.scan_all_keys_debug_only()?; // Hypothetical method
                // for key in all_keys {
                //     if let Some(value_bytes) = self.store.get(&key, /*snapshot_id*/, /*committed_ids*/)? {
                //         if let Ok(data_type) = deserialize_data_type(&value_bytes) {
                //             all_data_placeholder.push((key.clone(), data_type));
                //         }
                //     }
                // }


                let mut results: Vec<DataType> = Vec::new();

                for (_key, data_value) in all_data_placeholder { // Iterate over all data in the "table"
                    let mut matches_condition = true;
                    if let Some(ref cond) = condition {
                        matches_condition = false;
                        // Attempt to check condition against the data_value
                        // This is simplified. A real implementation needs to handle various DataType structures.
                        // If data_value is a Map, we look for cond.column.
                        if let DataType::Map(ref map_value) = data_value {
                            if let Some(field_value_from_map) = map_value.get(&cond.column.as_bytes().to_vec()) {
                                match compare_data_types(field_value_from_map, &cond.value, &cond.operator) {
                                    Ok(cmp_result) => matches_condition = cmp_result,
                                    Err(e) => { /* comparison error, treat as non-match or propagate */
                                        // For now, let's log and treat as non-match
                                        eprintln!("Condition comparison error: {}", e);
                                    }
                                }
                            } else {
                                // Column not found in map. If operator is e.g. IS NULL and column is absent,
                                // that might be a match depending on SQL dialect. For '=', it's not a match.
                                // This simple model assumes non-match if column is absent for most operators.
                            }
                        } else {
                            // data_value is not a Map. If condition.column is special (e.g. "_value" for scalar)
                            // we could compare directly. For now, assume conditions on non-maps are non-matches
                            // if a column is specified.
                            // If cond.column refers to the value itself (for non-map types), this logic would change.
                        }
                    }

                    if matches_condition {
                        match columns {
                            SelectColumnSpec::All => {
                                results.push(data_value.clone());
                            }
                            SelectColumnSpec::Specific(ref selected_columns) => {
                                if let DataType::Map(ref map_value) = data_value {
                                    let mut selected_map = crate::core::types::SimpleMap::new();
                                    for col_name in selected_columns {
                                        if let Some(val) = map_value.get(&col_name.as_bytes().to_vec()) {
                                            selected_map.insert(col_name.as_bytes().to_vec(), val.clone());
                                        }
                                        // If a selected column is not found, it's omitted from the result row.
                                    }
                                    results.push(DataType::Map(selected_map));
                                } else {
                                    // Trying to select specific columns from a non-Map DataType.
                                    // SQL behavior here can vary: error, nulls for columns, or skip row.
                                    // For now, if it's not a map, we can't select columns, so we push an empty map
                                    // or skip. Pushing an empty map might be misleading.
                                    // Let's push an empty map to indicate structure but no matching data.
                                    // results.push(DataType::Map(crate::core::types::SimpleMap::new()));
                                    // Alternative: skip this row if it doesn't conform.
                                    // Or, if only one column is selected and it's the value itself:
                                    if selected_columns.len() == 1 && selected_columns[0] == "_value" { // Hypothetical
                                         results.push(data_value.clone());
                                    } else {
                                        // results.push(DataType::Map(crate::core::types::SimpleMap::new())); // Or skip
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(ExecutionResult::Values(results))
            }
            Command::Update { source, assignments, condition } => {
                // NOTE: 'source' (table name) is currently ignored.
                // FUNDAMENTAL LIMITATION: Similar to SELECT, updating arbitrary rows without
                // a direct way to fetch them (e.g. via an index for the WHERE clause, or scanning)
                // is not fully supported due to KeyValueStore lacking key iteration.
                // The following logic assumes keys_to_update would be populated by such means.
                // For now, keys_to_update will be empty, so no actual updates will occur
                // unless this part is changed to work with specific keys (e.g. from a future index lookup).

                let keys_to_update: Vec<Key> = Vec::new(); // Placeholder
                let mut updated_count = 0; // To track how many items were actually changed.

                let active_tx_id_opt = self.transaction_manager.get_active_transaction().map(|tx| tx.id);

                for key in keys_to_update { // This loop is conceptual until key discovery is implemented
                    // Acquire exclusive lock for the key
                    let lock_tx_id = active_tx_id_opt.unwrap_or(0); // Use 0 for auto-commit lock
                    self.lock_manager.acquire_lock(lock_tx_id, &key, LockType::Exclusive)?;

                    let (snapshot_id, committed_ids_snapshot) = if let Some(active_tx) = self.transaction_manager.get_active_transaction() {
                        (active_tx.id, self.transaction_manager.get_committed_tx_ids_snapshot().into_iter().collect())
                    } else {
                        // For auto-commit, generate a temporary ID for snapshot consistency for the read.
                        (self.transaction_manager.generate_tx_id(), self.transaction_manager.get_committed_tx_ids_snapshot().into_iter().collect())
                    };

                    let current_value_bytes_opt = self.store.get(&key, snapshot_id, &committed_ids_snapshot)?;

                    if let Some(current_value_bytes) = current_value_bytes_opt {
                        let mut current_data_type = match deserialize_data_type(&current_value_bytes) {
                            Ok(dt) => dt,
                            Err(e) => {
                                if active_tx_id_opt.is_none() { self.lock_manager.release_locks(lock_tx_id); }
                                return Err(e); // Error deserializing, release lock if auto-commit
                            }
                        };

                        let mut matches_where = true;
                        if let Some(ref cond) = condition {
                            matches_where = false;
                            if let DataType::Map(ref map_value) = current_data_type {
                                if let Some(field_value_from_map) = map_value.get(&cond.column.as_bytes().to_vec()) {
                                    match compare_data_types(field_value_from_map, &cond.value, &cond.operator) {
                                        Ok(cmp_result) => matches_where = cmp_result,
                                        Err(e) => { /* comparison error, treat as non-match or log */
                                            eprintln!("Condition comparison error during UPDATE: {}", e);
                                        }
                                    }
                                }
                            } // else: condition on non-map, or column not found implies non-match for this logic.
                        }

                        if matches_where {
                            let original_value_for_undo = current_value_bytes.clone(); // Used for undo log

                            if let DataType::Map(ref mut map_data) = current_data_type {
                                for assignment in &assignments {
                                    map_data.insert(assignment.column.as_bytes().to_vec(), assignment.value.clone());
                                }
                            } else {
                                if !assignments.is_empty() {
                                    if active_tx_id_opt.is_none() { self.lock_manager.release_locks(lock_tx_id); }
                                    return Err(DbError::UnsupportedOperation(
                                        "Cannot apply field assignments to non-Map DataType".to_string()
                                    ));
                                }
                                // If assignments is empty and it's not a map, it's a no-op for this item's data.
                            }

                            let updated_value_bytes = match serialize_data_type(&current_data_type) {
                                Ok(bytes) => bytes,
                                Err(e) => {
                                    if active_tx_id_opt.is_none() { self.lock_manager.release_locks(lock_tx_id); }
                                    return Err(e);
                                }
                            };

                            if let Some(active_tx_mut) = self.transaction_manager.get_active_transaction_mut() {
                                active_tx_mut.undo_log.push(UndoOperation::RevertUpdate {
                                    key: key.clone(),
                                    old_value: original_value_for_undo.clone() // Clone here
                                });

                                // TODO: Index Undo/Redo logging for RevertUpdate
                                // This would involve:
                                // 1. Deserialize original_value_for_undo to get old indexed fields.
                                // 2. Knowing which fields are indexed.
                                // 3. Storing old and new indexed values in undo/redo logs for index.

                                // Index update logic begins
                                // For now, assume "default_value_index" indexes the entire serialized DataType.
                                // old_indexed_field_value_bytes is original_value_for_undo
                                // new_indexed_field_value_bytes is updated_value_bytes
                                if original_value_for_undo != updated_value_bytes {
                                    let mut old_map_for_index = HashMap::new();
                                    old_map_for_index.insert("default_value_index".to_string(), original_value_for_undo.clone());

                                    let mut new_map_for_index = HashMap::new();
                                    new_map_for_index.insert("default_value_index".to_string(), updated_value_bytes.clone());

                                    // on_update_data will call index.update(), which calls index.delete() and index.insert().
                                    // These sub-methods in HashIndex are NOT currently logging to undo_log.
                                    // This needs to be addressed: HashIndex::insert/delete should log to the *provided* transaction.
                                    // For now, QueryExecutor will log these assuming IndexManager doesn't.
                                    // This is a temporary fix for the subtask.
                                    // Correct way: Pass active_tx_mut to IndexManager methods.

                                    // Log undo for deleting the new value from index (reverting the insert part of update)
                                    active_tx_mut.undo_log.push(UndoOperation::IndexRevertInsert {
                                        index_name: "default_value_index".to_string(),
                                        key: key.clone(),
                                        value_for_index: updated_value_bytes.clone(),
                                    });
                                    // Log undo for inserting the old value back to index (reverting the delete part of update)
                                     active_tx_mut.undo_log.push(UndoOperation::IndexRevertDelete {
                                        index_name: "default_value_index".to_string(),
                                        key: key.clone(),
                                        old_value_for_index: original_value_for_undo.clone(),
                                    });

                                    self.index_manager.on_update_data(&old_map_for_index, &new_map_for_index, &key)?;
                                }
                                // Index update logic ends

                                let tx_for_store = active_tx_mut.clone_for_store();
                                self.store.put(key.clone(), updated_value_bytes.clone(), &tx_for_store)?;

                            } else { // Auto-commit mode
                                let old_indexed_field_value_bytes = original_value_for_undo.clone(); // Assuming this is what was indexed
                                let new_indexed_field_value_bytes = updated_value_bytes.clone();

                                if old_indexed_field_value_bytes != new_indexed_field_value_bytes {
                                    let mut old_map_for_index = HashMap::new();
                                    old_map_for_index.insert("default_value_index".to_string(), old_indexed_field_value_bytes);

                                    let mut new_map_for_index = HashMap::new();
                                    new_map_for_index.insert("default_value_index".to_string(), new_indexed_field_value_bytes);

                                    // TODO: For auto-commit, index changes are permanent immediately.
                                    // Error handling here is critical. If index update fails after store.put,
                                    // there's data inconsistency.
                                    self.index_manager.on_update_data(&old_map_for_index, &new_map_for_index, &key)?;
                                }

                                let mut tx_for_store = Transaction::new(0); // Auto-commit tx has ID 0
                                self.store.put(key.clone(), updated_value_bytes.clone(), &tx_for_store)?;

                                tx_for_store.set_state(TransactionState::Committed);
                                let commit_entry = crate::core::storage::engine::wal::WalEntry::TransactionCommit { transaction_id: 0 };
                                self.store.log_wal_entry(&commit_entry)?;
                                self.transaction_manager.add_committed_tx_id(0);
                            }
                            updated_count += 1;
                        }
                    } // else: key not found, nothing to update.

                    // Release lock if in auto-commit mode (per key)
                    if active_tx_id_opt.is_none() {
                        self.lock_manager.release_locks(lock_tx_id);
                    }
                } // End loop over keys_to_update

                // For now, always return Success. Could be changed to return updated_count.
                Ok(ExecutionResult::Success)
            }
        }
    }
}

/// Compares two DataType values based on a given operator.
///
/// Handles basic equality ("=") and inequality ("!="). For ordered comparisons
/// ("<", "<=", ">", ">="), it attempts to compare values of the same numeric type (Integer, Float)
/// or strings. Other cross-type comparisons or comparisons on non-ordered types
/// for these operators will result in an error.
///
/// Null handling:
/// - `val = NULL` or `val != NULL` is not standard SQL comparison. Use `IS NULL` or `IS NOT NULL`.
///   This function will treat `NULL = NULL` as true, and `NULL = <non-NULL>` as false.
///   For other operators, comparison with NULL generally results in an error or specific SQL null logic.
///   For simplicity here, any ordered comparison involving NULL will currently error.
fn compare_data_types(val1: &DataType, val2: &DataType, operator: &str) -> Result<bool, DbError> {
    match operator {
        "=" => Ok(val1 == val2),
        "!=" => Ok(val1 != val2),
        "<" | "<=" | ">" | ">=" => {
            // Ordered comparisons primarily for numbers and strings.
            // DataType needs to support PartialOrd for this to be more general.
            // Current DataType has PartialEq but not PartialOrd.
            // We need to implement comparison logic manually.
            match (val1, val2) {
                (DataType::Integer(i1), DataType::Integer(i2)) => match operator {
                    "<" => Ok(i1 < i2),
                    "<=" => Ok(i1 <= i2),
                    ">" => Ok(i1 > i2),
                    ">=" => Ok(i1 >= i2),
                    _ => unreachable!(), // Should be covered by outer match
                },
                (DataType::Float(f1), DataType::Float(f2)) => match operator {
                    "<" => Ok(f1 < f2),
                    "<=" => Ok(f1 <= f2),
                    ">" => Ok(f1 > f2),
                    ">=" => Ok(f1 >= f2),
                    _ => unreachable!(),
                },
                // Allow comparison between Integer and Float by promoting Integer to Float
                (DataType::Integer(i1), DataType::Float(f2)) => {
                    let f1 = *i1 as f64;
                    match operator {
                        "<" => Ok(f1 < *f2),
                        "<=" => Ok(f1 <= *f2),
                        ">" => Ok(f1 > *f2),
                        ">=" => Ok(f1 >= *f2),
                        _ => unreachable!(),
                    }
                }
                (DataType::Float(f1), DataType::Integer(i2)) => {
                    let f2 = *i2 as f64;
                    match operator {
                        "<" => Ok(*f1 < f2),
                        "<=" => Ok(*f1 <= f2),
                        ">" => Ok(*f1 > f2),
                        ">=" => Ok(*f1 >= f2),
                        _ => unreachable!(),
                    }
                }
                (DataType::String(s1), DataType::String(s2)) => match operator {
                    "<" => Ok(s1 < s2),
                    "<=" => Ok(s1 <= s2),
                    ">" => Ok(s1 > s2),
                    ">=" => Ok(s1 >= s2),
                    _ => unreachable!(),
                },
                // Ordered comparison with Null is tricky. SQL's NULL is not <, >, etc. to other values.
                // For simplicity, we'll error. Proper SQL would yield UNKNOWN, which then affects WHERE.
                (DataType::Null, _) | (_, DataType::Null) => Err(DbError::InvalidQuery(format!(
                    "Ordered comparison ('{}') with NULL is not supported directly. Use IS NULL or IS NOT NULL.",
                    operator
                ))),
                // Other type mismatches for ordered comparison
                _ => Err(DbError::TypeError(format!(
                    "Cannot apply ordered operator '{}' between {:?} and {:?}",
                    operator, val1, val2
                ))),
            }
        }
        _ => Err(DbError::InvalidQuery(format!("Unsupported operator: {}", operator))),
    }
}


// Methods specific to QueryExecutor when the store is SimpleFileKvStore
impl QueryExecutor<SimpleFileKvStore> {
    pub fn persist(&mut self) -> Result<(), DbError> {
        self.store.save_to_disk()?; // Save main data
        self.index_manager.save_all_indexes() // Save all indexes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::query::commands::{SqlCondition, SelectColumnSpec};
    use crate::core::types::{DataType, SimpleMap};
    use crate::core::storage::engine::InMemoryKvStore; // Using InMemoryKvStore for tests
    use std::path::PathBuf;
    use crate::core::query::commands::SqlAssignment; // For UPDATE tests

    // Helper to create a default QueryExecutor for tests
    fn create_test_executor() -> QueryExecutor<InMemoryKvStore> {
        let store = InMemoryKvStore::new();
        let index_path = PathBuf::from("test_indexes_executor_select"); // Unique path for tests
        // Clean up any old index files before test, if necessary (not strictly needed for InMemory)
        // std::fs::remove_dir_all(&index_path).ok();
        QueryExecutor::new(store, index_path).unwrap()
    }

    // Helper to create sample DataType::Map for testing SELECT
    fn create_sample_map_data(name: &str, age: i64, city: &str, active: bool) -> (Key, DataType) {
        let mut map = SimpleMap::new();
        map.insert("name".as_bytes().to_vec(), DataType::String(name.to_string()));
        map.insert("age".as_bytes().to_vec(), DataType::Integer(age));
        map.insert("city".as_bytes().to_vec(), DataType::String(city.to_string()));
        map.insert("is_active".as_bytes().to_vec(), DataType::Boolean(active));
        // Use name as key for simplicity in these tests
        (name.as_bytes().to_vec(), DataType::Map(map))
    }

    // Mocked version of execute_select for testing the logic without store iteration
    fn execute_select_logic(
        columns: SelectColumnSpec,
        condition: Option<SqlCondition>,
        all_data: Vec<(Key, DataType)> // Provide data directly
    ) -> Result<ExecutionResult, DbError> {
        let mut results: Vec<DataType> = Vec::new();

        for (_key, data_value) in all_data {
            let mut matches_condition = true;
            if let Some(ref cond) = condition {
                matches_condition = false;
                if let DataType::Map(ref map_value) = data_value {
                    if let Some(field_value_from_map) = map_value.get(&cond.column.as_bytes().to_vec()) {
                        match compare_data_types(field_value_from_map, &cond.value, &cond.operator) {
                            Ok(cmp_result) => matches_condition = cmp_result,
                            Err(e) => { eprintln!("Condition comparison error: {}", e); }
                        }
                    }
                }
            }

            if matches_condition {
                match columns {
                    SelectColumnSpec::All => {
                        results.push(data_value.clone());
                    }
                    SelectColumnSpec::Specific(ref selected_columns) => {
                        if let DataType::Map(ref map_value) = data_value {
                            let mut selected_map = SimpleMap::new();
                            for col_name in selected_columns {
                                if let Some(val) = map_value.get(&col_name.as_bytes().to_vec()) {
                                    selected_map.insert(col_name.as_bytes().to_vec(), val.clone());
                                }
                            }
                            if !selected_map.is_empty() || selected_columns.is_empty() { // Push if selected map has items or if no specific columns were asked (edge case)
                                results.push(DataType::Map(selected_map));
                            } else if selected_columns.iter().any(|c| map_value.contains_key(&c.as_bytes().to_vec())) {
                                // If some selected columns were present but all their values were e.g. filtered out by some logic
                                // or if the map was empty to begin with, push an empty map.
                                results.push(DataType::Map(selected_map));
                            }
                            // If specific columns are selected and NONE of them exist in the source map, we don't add an empty map for that row.
                        }
                        // If not a map, and specific columns are requested, this row is skipped.
                    }
                }
            }
        }
        Ok(ExecutionResult::Values(results))
    }


    #[test]
    fn test_select_all_no_condition() {
        let data = vec![
            create_sample_map_data("Alice", 30, "New York", true),
            create_sample_map_data("Bob", 24, "London", false),
        ];
        let expected_results = data.iter().map(|(_, val)| val.clone()).collect::<Vec<_>>();

        let result = execute_select_logic(SelectColumnSpec::All, None, data).unwrap();
        match result {
            ExecutionResult::Values(res_data) => assert_eq!(res_data, expected_results),
            _ => panic!("Expected Values result"),
        }
    }

    #[test]
    fn test_select_all_with_matching_condition() {
        let data = vec![
            create_sample_map_data("Alice", 30, "New York", true),
            create_sample_map_data("Bob", 24, "London", false),
            create_sample_map_data("Carol", 30, "Paris", true),
        ];
        let condition = Some(SqlCondition {
            column: "age".to_string(),
            operator: "=".to_string(),
            value: DataType::Integer(30),
        });
        // Expected: Alice and Carol
        let expected_data = vec![data[0].1.clone(), data[2].1.clone()];

        let result = execute_select_logic(SelectColumnSpec::All, condition, data).unwrap();
        match result {
            ExecutionResult::Values(res_data) => assert_eq!(res_data, expected_data),
            _ => panic!("Expected Values result"),
        }
    }

    #[test]
    fn test_select_all_with_string_condition() {
        let data = vec![
            create_sample_map_data("Alice", 30, "New York", true),
            create_sample_map_data("Bob", 24, "London", false),
        ];
        let condition = Some(SqlCondition {
            column: "city".to_string(),
            operator: "=".to_string(),
            value: DataType::String("London".to_string()),
        });
        let expected_data = vec![data[1].1.clone()]; // Bob

        let result = execute_select_logic(SelectColumnSpec::All, condition, data).unwrap();
         match result {
            ExecutionResult::Values(res_data) => assert_eq!(res_data, expected_data),
            _ => panic!("Expected Values result"),
        }
    }


    #[test]
    fn test_select_all_with_non_matching_condition() {
        let data = vec![
            create_sample_map_data("Alice", 30, "New York", true),
            create_sample_map_data("Bob", 24, "London", false),
        ];
        let condition = Some(SqlCondition {
            column: "age".to_string(),
            operator: "=".to_string(),
            value: DataType::Integer(100), // No one is 100
        });

        let result = execute_select_logic(SelectColumnSpec::All, condition, data).unwrap();
        match result {
            ExecutionResult::Values(res_data) => assert!(res_data.is_empty()),
            _ => panic!("Expected empty Values result"),
        }
    }

    #[test]
    fn test_select_specific_cols_no_condition() {
        let data = vec![
            create_sample_map_data("Alice", 30, "New York", true),
            create_sample_map_data("Bob", 24, "London", false),
        ];
        let columns = SelectColumnSpec::Specific(vec!["name".to_string(), "city".to_string()]);

        let mut expected_map1 = SimpleMap::new();
        expected_map1.insert("name".as_bytes().to_vec(), DataType::String("Alice".to_string()));
        expected_map1.insert("city".as_bytes().to_vec(), DataType::String("New York".to_string()));

        let mut expected_map2 = SimpleMap::new();
        expected_map2.insert("name".as_bytes().to_vec(), DataType::String("Bob".to_string()));
        expected_map2.insert("city".as_bytes().to_vec(), DataType::String("London".to_string()));

        let expected_data = vec![DataType::Map(expected_map1), DataType::Map(expected_map2)];

        let result = execute_select_logic(columns, None, data).unwrap();
        match result {
            ExecutionResult::Values(res_data) => assert_eq!(res_data, expected_data),
            _ => panic!("Expected Values result"),
        }
    }

    #[test]
    fn test_select_specific_cols_with_matching_condition() {
        let data = vec![
            create_sample_map_data("Alice", 30, "New York", true),
            create_sample_map_data("Bob", 24, "London", false),
            create_sample_map_data("Carol", 30, "Paris", true),
        ];
        let columns = SelectColumnSpec::Specific(vec!["name".to_string(), "is_active".to_string()]);
        let condition = Some(SqlCondition {
            column: "city".to_string(),
            operator: "=".to_string(),
            value: DataType::String("Paris".to_string()), // Carol
        });

        let mut expected_map_carol = SimpleMap::new();
        expected_map_carol.insert("name".as_bytes().to_vec(), DataType::String("Carol".to_string()));
        expected_map_carol.insert("is_active".as_bytes().to_vec(), DataType::Boolean(true));
        let expected_data = vec![DataType::Map(expected_map_carol)];

        let result = execute_select_logic(columns, condition, data).unwrap();
        match result {
            ExecutionResult::Values(res_data) => assert_eq!(res_data, expected_data),
            _ => panic!("Expected Values result"),
        }
    }

    #[test]
    fn test_select_specific_col_missing_in_some_rows() {
        let (_key_alice, data_alice) = create_sample_map_data("Alice", 30, "New York", true); // Has all fields

        let mut map_bob_incomplete = SimpleMap::new(); // Bob is missing 'city'
        map_bob_incomplete.insert("name".as_bytes().to_vec(), DataType::String("Bob".to_string()));
        map_bob_incomplete.insert("age".as_bytes().to_vec(), DataType::Integer(24));
        map_bob_incomplete.insert("is_active".as_bytes().to_vec(), DataType::Boolean(false));
        let data_bob = (_key_alice, DataType::Map(map_bob_incomplete)); // key doesn't matter for this test structure

        let data = vec![data_alice, data_bob];
        let columns = SelectColumnSpec::Specific(vec!["name".to_string(), "city".to_string()]);

        let mut expected_map_alice = SimpleMap::new();
        expected_map_alice.insert("name".as_bytes().to_vec(), DataType::String("Alice".to_string()));
        expected_map_alice.insert("city".as_bytes().to_vec(), DataType::String("New York".to_string()));

        let mut expected_map_bob = SimpleMap::new(); // Bob will only have name, city is missing
        expected_map_bob.insert("name".as_bytes().to_vec(), DataType::String("Bob".to_string()));

        // The current logic for Specific columns will only include an entry if the column was found.
        // If selected_map remains empty AND selected_columns was not empty, it implies the row itself
        // did not contain any of the specifically requested columns.
        // The test helper `execute_select_logic` was adjusted: "If specific columns are selected and NONE of them exist in the source map, we don't add an empty map for that row."
        // However, if SOME selected columns exist, a map is created.
        // So, Bob's entry will have 'name' but not 'city'.
        let expected_data = vec![DataType::Map(expected_map_alice), DataType::Map(expected_map_bob)];

        let result = execute_select_logic(columns, None, data).unwrap();
        match result {
            ExecutionResult::Values(res_data) => assert_eq!(res_data, expected_data),
            _ => panic!("Expected Values result"),
        }
    }

    #[test]
    fn test_select_with_greater_than_condition() {
        let data = vec![
            create_sample_map_data("Alice", 30, "New York", true),
            create_sample_map_data("Bob", 24, "London", false),
            create_sample_map_data("Carol", 35, "Paris", true),
        ];
        let condition = Some(SqlCondition {
            column: "age".to_string(),
            operator: ">".to_string(),
            value: DataType::Integer(25),
        });
        // Expected: Alice (30) and Carol (35)
        let expected_data = vec![data[0].1.clone(), data[2].1.clone()];

        let result = execute_select_logic(SelectColumnSpec::All, condition, data).unwrap();
        match result {
            ExecutionResult::Values(res_data) => assert_eq!(res_data, expected_data),
            _ => panic!("Expected Values result"),
        }
    }

    #[test]
    fn test_select_condition_on_boolean() {
        let data = vec![
            create_sample_map_data("Alice", 30, "New York", true),
            create_sample_map_data("Bob", 24, "London", false),
            create_sample_map_data("Carol", 35, "Paris", true),
        ];
        let condition = Some(SqlCondition {
            column: "is_active".to_string(),
            operator: "=".to_string(),
            value: DataType::Boolean(true),
        });
        // Expected: Alice and Carol
        let expected_data = vec![data[0].1.clone(), data[2].1.clone()];

        let result = execute_select_logic(SelectColumnSpec::All, condition, data).unwrap();
        match result {
            ExecutionResult::Values(res_data) => assert_eq!(res_data, expected_data),
            _ => panic!("Expected Values result"),
        }
    }

    // Test actual QueryExecutor path (will return empty due to no store iteration)
    #[test]
    fn test_executor_select_all_no_condition_empty_store() {
        let mut executor = create_test_executor();
        let command = Command::Select {
            columns: SelectColumnSpec::All,
            source: "any_table".to_string(), // Source is ignored for now
            condition: None,
        };
        let result = executor.execute_command(command).unwrap();
        match result {
            ExecutionResult::Values(data) => assert!(data.is_empty()),
            _ => panic!("Expected empty Values result from actual executor due to no store iteration"),
        }
    }

    // --- Tests for UPDATE command ---

    // Helper function to simulate the core logic of updating a single item.
    // This bypasses key discovery and direct store interaction for focused logic testing.
    fn apply_update_logic_to_item(
        initial_data: &DataType,
        assignments: &[SqlAssignment],
        condition: Option<&SqlCondition>,
    ) -> Result<Option<DataType>, DbError> {
        let mut current_data = initial_data.clone();

        // 1. Check condition
        let mut matches_where = true;
        if let Some(ref cond) = condition {
            matches_where = false;
            if let DataType::Map(ref map_value) = current_data {
                if let Some(field_value_from_map) = map_value.get(&cond.column.as_bytes().to_vec()) {
                    match compare_data_types(field_value_from_map, &cond.value, &cond.operator) {
                        Ok(cmp_result) => matches_where = cmp_result,
                        Err(e) => { eprintln!("Update test: Condition comparison error: {}", e); return Err(e); }
                    }
                } else { /* Column not in map, condition (usually) fails */ }
            } else { /* Condition on non-map, (usually) fails */ }
        }

        if !matches_where {
            return Ok(None); // Condition not met, item not updated
        }

        // 2. Apply assignments
        if let DataType::Map(ref mut map_data) = current_data {
            for assignment in assignments {
                map_data.insert(assignment.column.as_bytes().to_vec(), assignment.value.clone());
            }
        } else {
            // Cannot apply column-based assignments to non-Map type.
            if !assignments.is_empty() {
                return Err(DbError::UnsupportedOperation(
                    "Cannot apply field assignments to non-Map DataType".to_string(),
                ));
            }
        }
        Ok(Some(current_data))
    }

    #[test]
    fn test_update_apply_assignments_no_condition() {
        let (_key, initial_data) = create_sample_map_data("Alice", 30, "New York", true);
        let assignments = vec![
            SqlAssignment { column: "age".to_string(), value: DataType::Integer(31) },
            SqlAssignment { column: "city".to_string(), value: DataType::String("Boston".to_string()) },
        ];

        let updated_data_opt = apply_update_logic_to_item(&initial_data, &assignments, None).unwrap();
        assert!(updated_data_opt.is_some());
        let updated_data = updated_data_opt.unwrap();

        if let DataType::Map(map) = updated_data {
            assert_eq!(map.get("name".as_bytes()), Some(&DataType::String("Alice".to_string())));
            assert_eq!(map.get("age".as_bytes()), Some(&DataType::Integer(31)));
            assert_eq!(map.get("city".as_bytes()), Some(&DataType::String("Boston".to_string())));
            assert_eq!(map.get("is_active".as_bytes()), Some(&DataType::Boolean(true)));
        } else {
            panic!("Expected updated data to be a Map");
        }
    }

    #[test]
    fn test_update_condition_met_applies_assignments() {
        let (_key, initial_data) = create_sample_map_data("Bob", 24, "London", false);
        let assignments = vec![
            SqlAssignment { column: "is_active".to_string(), value: DataType::Boolean(true) },
        ];
        let condition = Some(SqlCondition {
            column: "name".to_string(),
            operator: "=".to_string(),
            value: DataType::String("Bob".to_string()),
        });

        let updated_data_opt = apply_update_logic_to_item(&initial_data, &assignments, Some(&condition)).unwrap();
        assert!(updated_data_opt.is_some());
        if let DataType::Map(map) = updated_data_opt.unwrap() {
            assert_eq!(map.get("is_active".as_bytes()), Some(&DataType::Boolean(true)));
            assert_eq!(map.get("age".as_bytes()), Some(&DataType::Integer(24))); // Age unchanged
        } else {
            panic!("Expected updated data to be a Map");
        }
    }

    #[test]
    fn test_update_condition_not_met_no_change() {
        let (_key, initial_data) = create_sample_map_data("Carol", 35, "Paris", true);
        let assignments = vec![
            SqlAssignment { column: "age".to_string(), value: DataType::Integer(36) },
        ];
        let condition = Some(SqlCondition {
            column: "city".to_string(),
            operator: "=".to_string(),
            value: DataType::String("London".to_string()), // Carol is in Paris
        });

        let updated_data_opt = apply_update_logic_to_item(&initial_data, &assignments, Some(&condition)).unwrap();
        assert!(updated_data_opt.is_none()); // No update should occur
    }

    #[test]
    fn test_update_on_non_map_type_fails_with_assignments() {
        let initial_data = DataType::String("just a string".to_string());
        let assignments = vec![
            SqlAssignment { column: "any".to_string(), value: DataType::Integer(1) },
        ];

        let result = apply_update_logic_to_item(&initial_data, &assignments, None);
        assert!(matches!(result, Err(DbError::UnsupportedOperation(_))));
    }

    #[test]
    fn test_update_on_non_map_type_no_assignments_no_condition() {
        // If there are no assignments, it's effectively a no-op on the data part.
        // The condition check would still apply.
        let initial_data = DataType::String("just a string".to_string());
        let assignments = vec![]; // No assignments

        let updated_data_opt = apply_update_logic_to_item(&initial_data, &assignments, None).unwrap();
        assert!(updated_data_opt.is_some());
        assert_eq!(updated_data_opt.unwrap(), initial_data); // Data remains unchanged
    }

    #[test]
    fn test_executor_update_empty_keys_to_update() {
        // This test confirms that if keys_to_update is empty (as it currently is
        // in the main executor.rs due to no store scan), no error occurs and it's a successful no-op.
        let mut executor = create_test_executor();
        let command = Command::Update {
            source: "any_table".to_string(),
            assignments: vec![SqlAssignment { column: "foo".to_string(), value: DataType::String("bar".to_string())}],
            condition: None,
        };
        let result = executor.execute_command(command).unwrap();
        assert_eq!(result, ExecutionResult::Success); // Should be success, 0 rows affected effectively.
    }

    // TODO: Tests for transaction and undo log behavior for UPDATE.
    // These would require setting up an active transaction in the executor,
    // then executing an UPDATE, and inspecting `executor.transaction_manager`
    // or specifically the active transaction's undo_log.
    // This is more involved as it requires deeper interaction with the executor's state.
    // Example sketch:
    // #[test]
    // fn test_update_within_transaction_adds_to_undo_log() {
    //     let mut executor = create_test_executor();
    //     executor.execute_command(Command::BeginTransaction).unwrap();
    //
    //     // Manually insert a piece of data to update for this test, since keys_to_update is empty.
    //     // This is a workaround.
    //     let test_key = "test_update_key".as_bytes().to_vec();
    //     let initial_map_data = create_sample_map_data("Test", 40, "TestCity", true).1;
    //     let serialized_initial = serialize_data_type(&initial_map_data).unwrap();
    //
    //     // We need to put this into the store in a way that our later UPDATE can "find" it.
    //     // This is the tricky part for testing the full UPDATE path.
    //     // For now, this test can only verify the logic if keys_to_update was populated.
    //
    //     // If we could populate keys_to_update:
    //     // executor.keys_to_update_for_test_only = vec![test_key.clone()]; // Hypothetical
    //
    //     let assignments = vec![SqlAssignment { column: "age".to_string(), value: DataType::Integer(41) }];
    //     let update_command = Command::Update {
    //         source: "test_table".to_string(),
    //         assignments,
    //         condition: None,
    //     };
    //     // executor.execute_command(update_command).unwrap(); // This will currently do nothing
    //
    //     // let active_tx = executor.transaction_manager.get_active_transaction().unwrap();
    //     // assert!(!active_tx.undo_log.is_empty());
    //     // if let Some(UndoOperation::RevertUpdate { key, old_value }) = active_tx.undo_log.last() {
    //     //     assert_eq!(key, &test_key);
    //     //     assert_eq!(*old_value, serialized_initial);
    //     // } else {
    //     //     panic!("Expected RevertUpdate in undo log");
    //     // }
    // }
}
