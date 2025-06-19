// src/core/wal/mod.rs

pub mod log_manager;
pub mod log_record;
pub mod writer;

pub use log_manager::LogManager;
// Updated to remove LogSequenceNumber and TransactionId as they are now sourced from common::types directly or through log_record's own imports.
// PageType, ActiveTransactionInfo, DirtyPageInfo are still defined in log_record.
pub use log_record::{ActiveTransactionInfo, DirtyPageInfo, LogRecord, PageType};
pub use writer::WalWriter;

// Potentially other modules related to WAL in the future:
// pub mod wal_manager;
// pub mod recovery_manager;
