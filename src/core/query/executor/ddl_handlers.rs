use super::{ExecutionResult, QueryExecutor};
use crate::core::common::OxidbError;
use crate::core::transaction::Transaction;
use crate::core::common::types::TransactionId;
use crate::core::storage::engine::traits::KeyValueStore;

impl<S: KeyValueStore<Vec<u8>, Vec<u8>>> QueryExecutor<S> {

    /* Legacy method - commented out to enforce SQL-only API
    pub(crate) fn handle_find_by_index(...) -> ...
    */

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

    /// Handles dropping a table.
    /// This removes the table's schema, drops associated indexes, and deletes table data.
    pub(crate) fn handle_drop_table(
        &mut self,
        table_name: String,
        if_exists: bool,
    ) -> Result<ExecutionResult, OxidbError> {
        // 1. Check if table exists (get schema)
        let schema_arc_opt = self.get_table_schema(&table_name)?;

        let schema_arc = match schema_arc_opt {
            Some(s) => s,
            None => {
                if if_exists {
                    return Ok(ExecutionResult::Success);
                } else {
                    return Err(OxidbError::TableNotFound(table_name));
                }
            }
        };

        let schema = schema_arc.as_ref();

        // 2. Drop associated indexes
        {
            let mut index_manager = self.index_manager.write().map_err(|e| {
                OxidbError::LockTimeout(format!("Failed to acquire write lock on index manager for drop table: {e}"))
            })?;

            // Iterate over columns to find indexes created for PKs or Unique constraints
            for col_def in &schema.columns {
                if col_def.is_primary_key || col_def.is_unique {
                    let index_name = format!("idx_{}_{}", table_name, col_def.name);

                    if let Err(e) = index_manager.drop_index(&index_name) {
                         // Log warning but continue? Or fail?
                         // If index file deletion fails, it might be an issue.
                         // But if index is just not found in map, it's fine.
                         eprintln!("Warning: Failed to drop index '{index_name}': {e}");
                    }
                }
            }
        }

        // 3. Delete all data rows associated with the table
        let prefix = format!("{}_", table_name).into_bytes();

        {
            let mut store = self.store.write().map_err(|e| {
                OxidbError::LockTimeout(format!("Failed to acquire write lock on store for drop table: {e}"))
            })?;

            // Scan all keys and identify those matching the table prefix
            // Note: This is inefficient for large stores but required given current KeyValueStore trait limitations
            let keys_to_delete: Vec<Vec<u8>> = store.scan()?
                .into_iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .map(|(k, _)| k)
                .collect();

            // Use a system transaction (ID 0) for these deletions
            let tx = Transaction::new(TransactionId(0));
            let lsn = self.log_manager.next_lsn();
            let committed_ids = self.transaction_manager.get_committed_tx_ids_snapshot().into_iter().map(|id| id.0).collect();

            for key in keys_to_delete {
                 store.delete(&key, &tx, lsn, &committed_ids)?;
            }

            // Delete the schema key
            let schema_key = Self::schema_key(&table_name);
            store.delete(&schema_key, &tx, lsn, &committed_ids)?;
        }

        // 4. Clean up auto-increment state
        // Need to find and remove keys in `auto_increment_state` that start with "{table_name}."
        let keys_to_remove: Vec<String> = self.auto_increment_state.keys()
            .filter(|k| k.starts_with(&format!("{}.", table_name)))
            .cloned()
            .collect();

        for k in keys_to_remove {
            self.auto_increment_state.remove(&k);
        }

        Ok(ExecutionResult::Success)
    }
}
