// File: src/core/query/executor/command_handlers.rs
// This file now primarily serves as a dispatcher for command execution
// to more specialized handler modules.

use super::{ExecutionResult, QueryExecutor};
use crate::core::common::OxidbError;
use crate::core::query::commands::Command;
use crate::core::storage::engine::traits::KeyValueStore;
// Removed unused Transaction and DataType imports that were moved to executor/mod.rs
// use crate::core::transaction::Transaction;
// use crate::core::types::DataType;

// The main execute_command method, dispatching to handlers in submodules.
impl<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> QueryExecutor<S> {
    pub fn execute_command(&mut self, command: Command) -> Result<ExecutionResult, OxidbError> {
        match command {
            Command::Insert { key, value } => self.handle_insert(key, value),
            Command::Get { key } => self.handle_get(key),
            Command::Delete { key } => self.handle_delete(key),
            Command::FindByIndex { index_name, value } => {
                self.handle_find_by_index(index_name, value)
            }

            Command::BeginTransaction => self.handle_begin_transaction(),
            Command::CommitTransaction => self.handle_commit_transaction(),
            Command::RollbackTransaction => self.handle_rollback_transaction(),
            Command::Vacuum => self.handle_vacuum(),

            Command::Select { columns, source, condition } => {
                self.handle_select(columns, source, condition)
            }
            Command::Update { source, assignments, condition } => {
                self.handle_update(source, assignments, condition)
            }
            Command::CreateTable { table_name: _, columns: _ } => {
                // For now, this is a no-op to allow CREATE TABLE statements to parse
                // and be "executed" successfully without actual table creation logic.
                // TODO: Implement actual table creation logic (schema management, etc.)
                Ok(ExecutionResult::Success)
            }
            Command::SqlInsert { table_name: _, columns: _, values: _ } => {
                // For now, this is a no-op to allow INSERT INTO statements to parse
                // and be "executed" successfully without actual insertion logic.
                // TODO: Implement actual SQL insertion logic.
                Ok(ExecutionResult::Success)
            }
        }
    }
}
