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

    pub(crate) fn handle_find_by_index(
        &mut self,
        index_name: String,
        value: Vec<u8>, // This is the serialized form of the value being searched
    ) -> Result<ExecutionResult, OxidbError> {
        // Changed
        let candidate_keys = match self.index_manager.find_by_index(&index_name, &value) {
            Ok(Some(keys)) => keys,
            Ok(None) => Vec::new(),
            Err(e) => return Err(e),
        };

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
            match self.store.read().unwrap().get(
                &primary_key,
                snapshot_id.0,
                &committed_ids_for_store,
            ) {
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
                                    "Error deserializing data (via deserialize_data_type) for key {:?}: {}",
                                    primary_key, deserialize_err
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
            return Err(OxidbError::AlreadyExists { name: format!("Table '{}'", table_name) });
        }

        let schema_to_store = crate::core::types::schema::Schema::new(columns);

        // Serialize the Schema object. Assuming JSON serialization for now.
        let serialized_schema = serde_json::to_vec(&schema_to_store).map_err(|e|
            OxidbError::Serialization(format!("Failed to serialize schema for table '{}': {}", table_name, e))
        )?;

        // Use a system transaction (ID 0) for DDL operations like schema storage.
        // LSN generation for DDL is also important.
        let system_tx = Transaction::new(TransactionId(0));
        let lsn = self.log_manager.next_lsn();

        // The schema itself is stored as a Vec<u8> value.
        // The `handle_insert` is for DataType values, so use store.put directly.
        self.store.write().unwrap().put(
            schema_key,
            serialized_schema,
            &system_tx,
            lsn,
        )?;

        // TODO: Persist schema changes immediately or rely on normal WAL/persist cycle?
        // For simplicity now, rely on normal cycle. Critical DDL might force persist.

        Ok(ExecutionResult::Success)
    }
}
