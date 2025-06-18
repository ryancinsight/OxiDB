// File: src/core/query/executor/command_handlers.rs
// This file now primarily serves as a dispatcher for command execution
// to more specialized handler modules.

use super::{ExecutionResult, QueryExecutor};
use crate::core::common::OxidbError;
use crate::core::query::commands::Command;
use crate::core::storage::engine::traits::KeyValueStore;
// Import the CommandProcessor trait
use crate::core::query::executor::processors::CommandProcessor;

// The main execute_command method, now simplified.
impl<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> QueryExecutor<S> {
    pub fn execute_command(&mut self, command: Command) -> Result<ExecutionResult, OxidbError> {
        // Delegate to the process method now available on Command via the CommandProcessor trait
        command.process(self)
    }
}
