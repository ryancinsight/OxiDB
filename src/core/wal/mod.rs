// src/core/wal/mod.rs

pub mod log_record;
pub mod writer;

pub use log_record::{LogRecord, TransactionId, PageType, ActiveTransactionInfo, DirtyPageInfo, LogSequenceNumber}; // Added other useful types from log_record.rs
pub use writer::WalWriter;

// Potentially other modules related to WAL in the future:
// pub mod wal_manager;
// pub mod recovery_manager;
