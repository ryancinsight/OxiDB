// src/core/query/commands.rs

use crate::core::types::DataType;

/// Represents a key for operations.
pub type Key = Vec<u8>;
/// Represents a value for operations.
pub type Value = Vec<u8>;

/// Enum defining the different types of commands the database can execute.
/// These are internal representations, not directly parsed from strings yet.
#[derive(Debug, PartialEq, Clone)] // For testing and inspection
pub enum Command {
    Insert { key: Key, value: DataType },
    Get { key: Key },
    Delete { key: Key },
    // Transaction control commands
    BeginTransaction,
    CommitTransaction,
    RollbackTransaction,
    FindByIndex { index_name: String, value: Value }, // Find by secondary index
    Vacuum, // Added Vacuum command
    // Potentially others later, like:
    // Update { key: Key, value: Value },
    // Scan { prefix: Option<Key> },
}

// Example of how these might be constructed (not strictly part of this file,
// but for conceptual clarity - API layer will do this)
// pub fn create_insert_command(key: Key, value: Value) -> Command {
//     Command::Insert { key, value }
// }
//
// pub fn create_get_command(key: Key) -> Command {
//     Command::Get { key }
// }
