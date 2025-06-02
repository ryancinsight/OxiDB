// src/core/query/executor.rs

use crate::core::common::error::DbError;
use crate::core::query::commands::Command;
use crate::core::storage::engine::traits::KeyValueStore;

#[derive(Debug, PartialEq)] // Add PartialEq for testing
pub enum ExecutionResult {
    Value(Option<Vec<u8>>), // For Get operations, carries the potential value
    Success,                // For Insert operations or other successful non-value returning ops
    Deleted(bool),          // For Delete operations, true if deleted, false if not found
}

/// Executes a given command against the provided key-value store.
///
/// # Arguments
///
/// * `store`: A mutable reference to a type implementing `KeyValueStore`.
/// * `command`: The `Command` to execute.
///
/// # Returns
///
/// * `Ok(ExecutionResult)` describing the outcome of the operation.
/// * `Err(DbError)` if any error occurs during store operation.
pub fn execute_command<S: KeyValueStore<Vec<u8>, Vec<u8>>>(
    store: &mut S,
    command: Command,
) -> Result<ExecutionResult, DbError> {
    match command {
        Command::Insert { key, value } => {
            store.put(key, value)?;
            Ok(ExecutionResult::Success)
        }
        Command::Get { key } => {
            store.get(&key).map(ExecutionResult::Value)
        }
        Command::Delete { key } => {
            store.delete(&key).map(ExecutionResult::Deleted)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::query::commands::{Command, Key, Value};
    use crate::core::storage::engine::simple_file_kv_store::SimpleFileKvStore;
    use tempfile::NamedTempFile;

    fn create_temp_store() -> SimpleFileKvStore {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        SimpleFileKvStore::new(temp_file.path()).expect("Failed to create SimpleFileKvStore")
    }

    #[test]
    fn test_insert_and_get() {
        let mut store = create_temp_store();
        let key: Key = b"test_key_1".to_vec();
        let value: Value = b"test_value_1".to_vec();

        // Insert
        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        let result = execute_command(&mut store, insert_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Success);

        // Get
        let get_command = Command::Get { key: key.clone() };
        let result = execute_command(&mut store, get_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Value(Some(value)));
    }

    #[test]
    fn test_get_non_existent() {
        let mut store = create_temp_store();
        let key: Key = b"non_existent_key".to_vec();

        let get_command = Command::Get { key: key.clone() };
        let result = execute_command(&mut store, get_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Value(None));
    }

    #[test]
    fn test_insert_delete_get() {
        let mut store = create_temp_store();
        let key: Key = b"test_key_2".to_vec();
        let value: Value = b"test_value_2".to_vec();

        // Insert
        let insert_command = Command::Insert { key: key.clone(), value: value.clone() };
        execute_command(&mut store, insert_command).unwrap();

        // Delete
        let delete_command = Command::Delete { key: key.clone() };
        let result = execute_command(&mut store, delete_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Deleted(true));

        // Get (should be Value(None))
        let get_command = Command::Get { key: key.clone() };
        let result = execute_command(&mut store, get_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Value(None));
    }

    #[test]
    fn test_delete_non_existent() {
        let mut store = create_temp_store();
        let key: Key = b"non_existent_delete_key".to_vec();

        let delete_command = Command::Delete { key: key.clone() };
        let result = execute_command(&mut store, delete_command);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::Deleted(false));
    }

    #[test]
    fn test_insert_update_get() {
        let mut store = create_temp_store();
        let key: Key = b"test_key_3".to_vec();
        let value1: Value = b"initial_value".to_vec();
        let value2: Value = b"updated_value".to_vec();

        // Insert initial value
        let insert_command1 = Command::Insert { key: key.clone(), value: value1.clone() };
        assert_eq!(execute_command(&mut store, insert_command1).unwrap(), ExecutionResult::Success);

        // Get initial value
        let get_command1 = Command::Get { key: key.clone() };
        assert_eq!(execute_command(&mut store, get_command1).unwrap(), ExecutionResult::Value(Some(value1)));

        // Insert new value (update)
        let insert_command2 = Command::Insert { key: key.clone(), value: value2.clone() };
        assert_eq!(execute_command(&mut store, insert_command2).unwrap(), ExecutionResult::Success);

        // Get updated value
        let get_command2 = Command::Get { key: key.clone() };
        assert_eq!(execute_command(&mut store, get_command2).unwrap(), ExecutionResult::Value(Some(value2)));
    }

    #[test]
    fn test_delete_results() { // Renamed from test_delete_returns_ok_none
        let mut store = create_temp_store();
        let key: Key = b"delete_me".to_vec();
        let value: Value = b"some_data".to_vec();

        // Insert
        let insert_cmd = Command::Insert { key: key.clone(), value: value.clone() };
        execute_command(&mut store, insert_cmd).expect("Insert failed");

        // Delete (item exists)
        let delete_cmd_exists = Command::Delete { key: key.clone() };
        let result_exists = execute_command(&mut store, delete_cmd_exists);
        
        assert!(result_exists.is_ok(), "Delete operation (existing) failed: {:?}", result_exists.err());
        assert_eq!(result_exists.unwrap(), ExecutionResult::Deleted(true), "Delete operation (existing) should return Deleted(true)");

        // Verify it's actually gone
        let get_cmd = Command::Get { key: key.clone() };
        let get_result = execute_command(&mut store, get_cmd);
        assert_eq!(get_result.unwrap(), ExecutionResult::Value(None), "Key should be Value(None) after deletion");

        // Delete (item doesn't exist)
        let delete_cmd_not_exists = Command::Delete { key: b"does_not_exist".to_vec() };
        let result_not_exists = execute_command(&mut store, delete_cmd_not_exists);

        assert!(result_not_exists.is_ok(), "Delete operation (non-existing) failed: {:?}", result_not_exists.err());
        assert_eq!(result_not_exists.unwrap(), ExecutionResult::Deleted(false), "Delete operation (non-existing) should return Deleted(false)");
    }
}
