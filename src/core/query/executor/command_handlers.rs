// File: src/core/query/executor/command_handlers.rs
// This file now primarily serves as a dispatcher for command execution
// to more specialized handler modules.

use crate::core::common::error::DbError;
use crate::core::query::commands::Command;
use super::{ExecutionResult, QueryExecutor};
use crate::core::storage::engine::traits::KeyValueStore; // Needed for QueryExecutor<S> bound

// The main execute_command method, dispatching to handlers in submodules.
impl<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> QueryExecutor<S> {
    pub fn execute_command(&mut self, command: Command) -> Result<ExecutionResult, DbError> {
        match command {
            Command::Insert { key, value } => self.handle_insert(key, value),
            Command::Get { key } => self.handle_get(key),
            Command::Delete { key } => self.handle_delete(key),
            Command::FindByIndex { index_name, value } => self.handle_find_by_index(index_name, value),

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
        }
    }
}
