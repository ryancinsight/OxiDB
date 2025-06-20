// File: src/core/query/executor/command_handlers.rs
use super::{ExecutionResult, QueryExecutor};
use crate::core::common::OxidbError;
use crate::core::query::commands::Command;
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::query::executor::processors::CommandProcessor;
use crate::core::common::types::TransactionId; // Required for TransactionId(0)

impl<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> QueryExecutor<S> {
    pub fn execute_command(&mut self, command: Command) -> Result<ExecutionResult, OxidbError> {
        let mut requires_auto_commit = false;
        let is_transaction_management_command = matches!(command, Command::BeginTransaction | Command::CommitTransaction | Command::RollbackTransaction);

        if !is_transaction_management_command && self.transaction_manager.current_active_transaction_id().is_none() {
            // If no active transaction and not a TxMgmt command, start Tx0 for auto-commit.
            // This directly calls TransactionManager's method to set active_transaction to Tx0.
            self.transaction_manager.begin_transaction_with_id(TransactionId(0))?;
            requires_auto_commit = true;
        }

        // Delegate processing to the command itself (via CommandProcessor trait)
        // This will call the specific handle_... method in QueryExecutor or logic in processors.rs
        let result = command.process(self);

        if requires_auto_commit {
            // If a temporary transaction (Tx0) was started for auto-commit.
            if result.is_ok() {
                // Call QueryExecutor's handle_commit_transaction.
                // It will commit the current active transaction (which is Tx0).
                // handle_commit_transaction logs physical commit, releases locks, and calls TM::commit_transaction.
                self.handle_commit_transaction()?;
            } else {
                // Call QueryExecutor's handle_rollback_transaction.
                self.handle_rollback_transaction()?;
            }
        }
        result
    }
}
