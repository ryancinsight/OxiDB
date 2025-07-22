use super::{ExecutionResult, QueryExecutor};
use crate::core::common::OxidbError;
use crate::core::transaction::Transaction; // Added this import
                                           // use crate::core::common::serialization::{deserialize_data_type}; // No longer needed here
use crate::core::common::types::TransactionId;
// Key removed
use crate::core::storage::engine::traits::KeyValueStore;
// LockType removed
// Transaction, TransactionState, UndoOperation removed
// DataType removed
use std::collections::HashSet; // HashMap removed

impl<S: KeyValueStore<Vec<u8>, Vec<u8>>> QueryExecutor<S> {
    // handle_insert, handle_delete, and handle_get were removed.
    // Only handle_find_by_index and other DDL-specific handlers should remain.

    /// Handles finding rows by an index lookup.
    /// It retrieves primary keys from the specified index for the given value,
    /// then fetches the actual row data from the store for those primary keys,
    /// considering transaction visibility.
    pub(crate) fn handle_find_by_index(
        &mut self,
        index_name: String,
        value: Vec<u8>, // This is the serialized form of the value being searched
    ) -> Result<ExecutionResult, OxidbError> {
        // Changed
        let option_keys = self
            .index_manager
            .read()
            .map_err(|e| {
                OxidbError::LockTimeout(format!(
                    "Failed to acquire read lock on index manager for find: {e}"
                ))
            })?
            .find_by_index(&index_name, &value)?; // Propagate error from find_by_index

        let candidate_keys: std::vec::Vec<std::vec::Vec<u8>> = option_keys.unwrap_or_default();

        if candidate_keys.is_empty() {
            return Ok(ExecutionResult::Values(Vec::new()));
        }

        let snapshot_id: TransactionId; // Explicitly TransactionId
        let committed_ids_vec: Vec<TransactionId>;

        if let Some(active_tx) = self.transaction_manager.get_active_transaction() {
            snapshot_id = active_tx.id;
            committed_ids_vec = self.transaction_manager.get_committed_tx_ids_snapshot();
        } else {
            // If no active transaction, generate_tx_id might not be what we want for a read snapshot.
            // Using 0 for auto-commit context often means "read latest committed".
            // However, the original logic used generate_tx_id. Let's stick to TransactionId(0) for unwrap_or consistency.
            // If this is for a read operation without a transaction, it should see all committed data.
            // A "snapshot_id" of 0 and an empty committed_ids (or all committed_ids if available) might be more appropriate
            // if TransactionId(0) is special. For now, let's assume it aligns with unwrap_or(TransactionId(0)).
            snapshot_id = self
                .transaction_manager
                .current_active_transaction_id()
                .unwrap_or(TransactionId(0));
            committed_ids_vec = self.transaction_manager.get_committed_tx_ids_snapshot();
        }

        // Filter committed_ids based on snapshot_id (both are TransactionId)
        // Then map to u64 for HashSet<u64> and for store.get()
        let committed_ids_for_store: HashSet<u64> = committed_ids_vec
            .into_iter()
            .filter(|id| *id <= snapshot_id) // Compare TransactionId with TransactionId
            .map(|id| id.0) // Convert TransactionId to u64
            .collect();

        let mut results_vec = Vec::new();
        for primary_key in candidate_keys {
            match self
                .store
                .read()
                .map_err(|e| {
                    OxidbError::LockTimeout(format!(
                        "Failed to acquire read lock on store for find by index: {e}"
                    ))
                })?
                .get(&primary_key, snapshot_id.0, &committed_ids_for_store)
            {
                // Corrected: removed extra parenthesis, this is the match block opening
                // Use snapshot_id.0 (u64)
                Ok(Some(serialized_data_from_store)) => {
                    // The `value` parameter to this function is the serialized indexed field's value.
                    // If the index ("default_value_index") stores the entire serialized DataType,
                    // then `serialized_data_from_store` should indeed be compared with `value`.
                    // However, this relies on the specific indexing strategy.
                    // For "default_value_index", it's assumed it indexes the serialized DataType.
                    if serialized_data_from_store == value {
                        // This comparison logic might need adjustment based on what the index actually stores
                        match crate::core::common::serialization::deserialize_data_type(
                            &serialized_data_from_store,
                        ) {
                            Ok(data_type) => results_vec.push(data_type),
                            Err(deserialize_err) => {
                                // deserialize_data_type already returns OxidbError.
                                // Log the original error context if needed, then propagate.
                                eprintln!(
                                    "Error deserializing data (via deserialize_data_type) for key {primary_key:?}: {deserialize_err}"
                                );
                                return Err(deserialize_err);
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
    // Removed handle_get from here

    /// Handles the creation of a new table.
    /// This involves storing the table's schema and creating any necessary indexes
    /// for primary or unique keys defined in the schema.
    pub(crate) fn handle_create_table(
        &mut self,
        table_name: String,
        columns: Vec<crate::core::types::schema::ColumnDef>,
    ) -> Result<ExecutionResult, OxidbError> {
        let schema_key = Self::schema_key(&table_name); // Use helper from QueryExecutor in mod.rs

        // Check if schema already exists (optional, depends on IF NOT EXISTS behavior)
        // For now, assume CREATE TABLE should fail if table (schema) already exists.
        // The get_schema method uses snapshot_id 0 and default committed_ids.
        if self.get_table_schema(&table_name)?.is_some() {
            return Err(OxidbError::AlreadyExists { name: format!("Table '{table_name}'") });
        }

        let schema_to_store = crate::core::types::schema::Schema::new(columns);

        // Serialize the Schema object. Assuming JSON serialization for now.
        let serialized_schema = serde_json::to_vec(&schema_to_store).map_err(|e| {
            OxidbError::Serialization(format!(
                "Failed to serialize schema for table '{table_name}': {e}"
            ))
        })?;

        // Use a system transaction (ID 0) for DDL operations like schema storage.
        // LSN generation for DDL is also important.
        let _system_tx = Transaction::new(TransactionId(0));
        let lsn = self.log_manager.next_lsn();

        // The schema itself is stored as a Vec<u8> value.
        // The `handle_insert` is for DataType values, so use store.put directly.
        // Use the current transaction context (which will be Tx0 if auto-committing)
        let current_tx = self.transaction_manager.get_active_transaction().map_or_else(
            || Transaction::new(TransactionId(0)),
            crate::core::transaction::Transaction::clone_for_store,
        ); // Fallback to new Tx0 if somehow none (should be set by execute_command)

        // Ensure prev_lsn is updated for the active transaction (likely Tx0)
        if let Some(active_tx_mut) = self.transaction_manager.get_active_transaction_mut() {
            active_tx_mut.prev_lsn = lsn;
        }

        self.store
            .write()
            .map_err(|e| {
                OxidbError::LockTimeout(format!(
                    "Failed to acquire write lock on store for create table: {e}"
                ))
            })?
            .put(
                schema_key,
                serialized_schema,
                &current_tx, // Use current_tx (which would be Tx0 in auto-commit)
                lsn,
            )?;

        // Iterate through columns to create indexes for primary key or unique columns
        for col_def in &schema_to_store.columns {
            if col_def.is_primary_key || col_def.is_unique {
                let index_name = format!("idx_{}_{}", table_name, col_def.name);
                // Using "hash" as the index type for simplicity, good for exact lookups.
                // The actual index implementation (e.g., BTree, Hash) would be determined by
                // the string passed here and handled by the IndexManager.
                match self
                    .index_manager
                    .write()
                    .map_err(|e| {
                        OxidbError::LockTimeout(format!(
                            "Failed to acquire write lock on index manager for create index: {e}"
                        ))
                    })?
                    .create_index(index_name.clone(), "hash")
                {
                    // Acquire write lock
                    Ok(()) => {
                        eprintln!("[Executor::handle_create_table] Successfully created index '{}' for table '{}', column '{}'.", index_name, table_name, col_def.name);
                    }
                    Err(OxidbError::Index(msg)) if msg.contains("already exists") => {
                        // This case might occur if an index with the same name somehow exists.
                        // For CREATE TABLE, this should ideally not happen if table names are unique
                        // and index naming convention is followed.
                        // We can choose to ignore this error or propagate it.
                        // For now, let's print a warning and continue, as the goal is to have the index.
                        eprintln!("[Executor::handle_create_table] Warning: Index '{index_name}' already exists. Assuming it's usable.");
                    }
                    Err(e) => {
                        // For other errors during index creation, propagate them.
                        return Err(OxidbError::Index(format!(
                            "Failed to create index '{}' for table '{}', column '{}': {}",
                            index_name, table_name, col_def.name, e
                        )));
                    }
                }
            }
        }

        // TODO: Persist schema changes and new index metadata immediately or rely on normal WAL/persist cycle?
        // For simplicity now, rely on normal cycle. Critical DDL might force persist.
        // IndexManager::create_index typically handles its own persistence for index metadata.

        // Auto-commit logic is now handled by QueryExecutor::execute_command wrapper.
        // No need for explicit commit logging to store's WAL here.
        // The wrapper will call handle_commit_transaction, which calls transaction_manager.commit_transaction(),
        // which logs LogRecord::CommitTransaction to TM's WAL.

        Ok(ExecutionResult::Success)
    }
}
