// src/core/query/executor/processors.rs

use crate::core::common::OxidbError;
use crate::core::query::commands::Command; // Crucial: ensures Command is in scope
use crate::core::query::executor::{ExecutionResult, QueryExecutor};
use crate::core::storage::engine::traits::KeyValueStore;

/// The `CommandProcessor` trait defines the interface for processing a specific command.
pub trait CommandProcessor<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> {
    /// Processes the command using the provided QueryExecutor.
    fn process(&self, executor: &mut QueryExecutor<S>) -> Result<ExecutionResult, OxidbError>;
}

// Implementation of CommandProcessor for the Command enum itself
impl<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> CommandProcessor<S> for Command {
    fn process(&self, executor: &mut QueryExecutor<S>) -> Result<ExecutionResult, OxidbError> {
        match self {
            Command::Insert { key, value } => executor.handle_insert(key.clone(), value.clone()),
            Command::Get { key } => executor.handle_get(key.clone()),
            Command::Delete { key } => executor.handle_delete(key.clone()),
            Command::FindByIndex { index_name, value } => {
                executor.handle_find_by_index(index_name.clone(), value.clone())
            }
            Command::BeginTransaction => executor.handle_begin_transaction(),
            Command::CommitTransaction => executor.handle_commit_transaction(),
            Command::RollbackTransaction => executor.handle_rollback_transaction(),
            Command::Vacuum => executor.handle_vacuum(),
            Command::Select { columns, source, condition } => {
                executor.handle_select(columns.clone(), source.clone(), condition.clone())
            }
            Command::Update { source, assignments, condition } => {
                executor.handle_update(source.clone(), assignments.clone(), condition.clone())
            }
            Command::CreateTable { table_name, columns } => {
                // Forwarding to a dedicated handler, assuming it exists or will be created
                // For now, to match existing logic, this will be a call to a method on executor
                // which might currently be a no-op or call a more specific handler.
                // Based on command_handlers.rs, this is currently a direct Ok(ExecutionResult::Success)
                 Ok(ExecutionResult::Success) // Placeholder, matching original
            }
            Command::SqlInsert { table_name, columns, values } => {
                // Similar to CreateTable, forwarding or placeholder
                // Based on command_handlers.rs, this is currently a direct Ok(ExecutionResult::Success)
                Ok(ExecutionResult::Success) // Placeholder, matching original
            }
        }
    }
}
