//! Analysis Phase Implementation
//!
//! This module implements the Analysis phase of the ARIES recovery algorithm.
//! The Analysis phase scans the Write-Ahead Log (WAL) to determine:
//!
//! 1. Which transactions were active at the time of the crash
//! 2. Which pages were dirty and may need to be redone
//! 3. The appropriate starting point for the Redo phase
//!
//! The Analysis phase builds two key data structures:
//! - Transaction Table: tracks the state of all transactions
//! - Dirty Page Table: tracks pages that may need redo operations

use crate::core::common::types::{Lsn, TransactionId};
use crate::core::recovery::tables::{DirtyPageTable, TransactionTable};
use crate::core::recovery::types::{RecoveryError, TransactionInfo};
use crate::core::wal::log_record::{ActiveTransactionInfo, DirtyPageInfo, LogRecord};
use crate::core::wal::reader::{WalReader, WalReaderError};
use std::collections::HashMap;

/// Result of the Analysis phase containing the built tables and recovery information.
#[derive(Debug, Clone)]
pub struct AnalysisResult {
    /// Transaction table built during analysis
    pub transaction_table: TransactionTable,
    /// Dirty page table built during analysis
    pub dirty_page_table: DirtyPageTable,
    /// LSN to start the Redo phase from
    pub redo_lsn: Option<Lsn>,
    /// LSN of the last checkpoint found
    pub last_checkpoint_lsn: Option<Lsn>,
    /// Total number of log records processed
    pub records_processed: usize,
}

impl AnalysisResult {
    /// Creates a new empty AnalysisResult.
    pub fn new() -> Self {
        Self {
            transaction_table: TransactionTable::new(),
            dirty_page_table: DirtyPageTable::new(),
            redo_lsn: None,
            last_checkpoint_lsn: None,
            records_processed: 0,
        }
    }

    /// Returns the number of active transactions that need to be undone.
    pub fn active_transaction_count(&self) -> usize {
        self.transaction_table.active_transactions().count()
    }

    /// Returns the number of dirty pages that may need redo.
    pub fn dirty_page_count(&self) -> usize {
        self.dirty_page_table.len()
    }

    /// Returns true if recovery is needed (there are active transactions or dirty pages).
    pub fn recovery_needed(&self) -> bool {
        self.active_transaction_count() > 0 || self.dirty_page_count() > 0
    }
}

impl Default for AnalysisResult {
    fn default() -> Self {
        Self::new()
    }
}

/// The Analysis phase implementation for ARIES recovery.
pub struct AnalysisPhase<'a> {
    /// WAL reader for scanning log records
    wal_reader: &'a mut WalReader,
    /// Current analysis result being built
    result: AnalysisResult,
}

impl<'a> AnalysisPhase<'a> {
    /// Creates a new Analysis phase with the given WAL reader.
    pub fn new(wal_reader: &'a mut WalReader) -> Self {
        Self {
            wal_reader,
            result: AnalysisResult::new(),
        }
    }

    /// Performs the Analysis phase of recovery.
    ///
    /// This method scans the WAL from the last checkpoint (if any) to the end,
    /// building the transaction table and dirty page table.
    pub async fn analyze(&mut self) -> Result<AnalysisResult, RecoveryError> {
        // Step 1: Find the last checkpoint
        self.find_last_checkpoint().await?;

        // Step 2: Initialize tables from checkpoint (if found)
        self.initialize_from_checkpoint().await?;

        // Step 3: Scan forward from checkpoint to end of log
        self.scan_forward_from_checkpoint().await?;

        // Step 4: Determine redo starting LSN
        self.determine_redo_lsn();

        Ok(self.result.clone())
    }

    /// Finds the last checkpoint in the WAL.
    async fn find_last_checkpoint(&mut self) -> Result<(), RecoveryError> {
        match self.wal_reader.find_last_checkpoint() {
            Ok(Some((_, checkpoint_end))) => {
                // Extract LSN from the checkpoint end record
                let checkpoint_lsn = match checkpoint_end {
                    LogRecord::CheckpointEnd { lsn, .. } => lsn,
                    _ => return Err(RecoveryError::WalError("Invalid checkpoint end record".to_string())),
                };
                self.result.last_checkpoint_lsn = Some(checkpoint_lsn);
            }
            Ok(None) => {
                // No checkpoint found, will scan from beginning
                self.result.last_checkpoint_lsn = None;
            }
            Err(e) => {
                return Err(RecoveryError::WalError(format!(
                    "Error finding last checkpoint: {}",
                    e
                )));
            }
        }
        Ok(())
    }

    /// Initializes the transaction and dirty page tables from the last checkpoint.
    async fn initialize_from_checkpoint(&mut self) -> Result<(), RecoveryError> {
        if let Some(checkpoint_lsn) = self.result.last_checkpoint_lsn {
            // Find the CheckpointEnd record that contains the table data
            let records = self
                .wal_reader
                .read_all_records()
                .map_err(|e| RecoveryError::WalError(format!("Failed to read records: {}", e)))?;

            for record in records {
                if let LogRecord::CheckpointEnd {
                    lsn,
                    active_transactions,
                    dirty_pages,
                } = record
                {
                    if lsn == checkpoint_lsn {
                        self.initialize_transaction_table_from_checkpoint(&active_transactions);
                        self.initialize_dirty_page_table_from_checkpoint(&dirty_pages);
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Initializes the transaction table from checkpoint data.
    fn initialize_transaction_table_from_checkpoint(
        &mut self,
        active_transactions: &[ActiveTransactionInfo],
    ) {
        for tx_info in active_transactions {
            let transaction_info = TransactionInfo::new_active(tx_info.tx_id, tx_info.last_lsn);
            self.result.transaction_table.insert(transaction_info);
        }
    }

    /// Initializes the dirty page table from checkpoint data.
    fn initialize_dirty_page_table_from_checkpoint(&mut self, dirty_pages: &[DirtyPageInfo]) {
        for page_info in dirty_pages {
            self.result
                .dirty_page_table
                .insert(page_info.page_id, page_info.recovery_lsn);
        }
    }

    /// Scans forward from the checkpoint (or beginning) to the end of the log.
    async fn scan_forward_from_checkpoint(&mut self) -> Result<(), RecoveryError> {
        let start_lsn = self.result.last_checkpoint_lsn.unwrap_or(0);

        let records = self
            .wal_reader
            .read_all_records()
            .map_err(|e| RecoveryError::WalError(format!("Failed to read records: {}", e)))?;

        for record in records {
            // Only process records after the checkpoint
            if self.get_record_lsn(&record) >= start_lsn {
                self.process_log_record(&record)?;
                self.result.records_processed += 1;
            }
        }

        Ok(())
    }

    /// Processes a single log record during the forward scan.
    fn process_log_record(&mut self, record: &LogRecord) -> Result<(), RecoveryError> {
        match record {
            LogRecord::BeginTransaction { lsn, tx_id } => {
                let tx_info = TransactionInfo::new_active(*tx_id, *lsn);
                self.result.transaction_table.insert(tx_info);
            }
            LogRecord::CommitTransaction { tx_id, .. } => {
                self.result.transaction_table.commit_transaction(tx_id);
            }
            LogRecord::AbortTransaction { tx_id, .. } => {
                self.result.transaction_table.abort_transaction(tx_id);
            }
            LogRecord::InsertRecord {
                lsn,
                tx_id,
                page_id,
                ..
            }
            | LogRecord::DeleteRecord {
                lsn,
                tx_id,
                page_id,
                ..
            }
            | LogRecord::UpdateRecord {
                lsn,
                tx_id,
                page_id,
                ..
            }
            | LogRecord::NewPage {
                lsn,
                tx_id,
                page_id,
                ..
            } => {
                // Update transaction table
                self.result.transaction_table.update_transaction(*tx_id, *lsn);
                
                // Add page to dirty page table if not already present
                if !self.result.dirty_page_table.contains(page_id) {
                    self.result.dirty_page_table.insert(*page_id, *lsn);
                }
            }
            LogRecord::CompensationLogRecord {
                lsn,
                tx_id,
                page_id,
                ..
            } => {
                // Update transaction table
                self.result.transaction_table.update_transaction(*tx_id, *lsn);
                
                // CLRs also dirty pages
                if !self.result.dirty_page_table.contains(page_id) {
                    self.result.dirty_page_table.insert(*page_id, *lsn);
                }
            }
            LogRecord::CheckpointBegin { .. } | LogRecord::CheckpointEnd { .. } => {
                // Checkpoint records don't affect transaction or dirty page state
            }
        }
        Ok(())
    }

    /// Determines the LSN to start the Redo phase from.
    fn determine_redo_lsn(&mut self) {
        // The redo LSN is the minimum recovery LSN from the dirty page table
        self.result.redo_lsn = self.result.dirty_page_table.min_recovery_lsn();
    }

    /// Extracts the LSN from a log record.
    fn get_record_lsn(&self, record: &LogRecord) -> Lsn {
        match record {
            LogRecord::BeginTransaction { lsn, .. }
            | LogRecord::CommitTransaction { lsn, .. }
            | LogRecord::AbortTransaction { lsn, .. }
            | LogRecord::InsertRecord { lsn, .. }
            | LogRecord::DeleteRecord { lsn, .. }
            | LogRecord::UpdateRecord { lsn, .. }
            | LogRecord::NewPage { lsn, .. }
            | LogRecord::CompensationLogRecord { lsn, .. }
            | LogRecord::CheckpointBegin { lsn, .. }
            | LogRecord::CheckpointEnd { lsn, .. } => *lsn,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::common::types::ids::{PageId, SlotId};
    use crate::core::common::types::TransactionId;
    use crate::core::wal::log_record::{ActiveTransactionInfo, DirtyPageInfo, LogRecord};
    use crate::core::wal::reader::{WalReader, WalReaderConfig};
    use crate::core::wal::writer::{WalWriter, WalWriterConfig};
    use tempfile::NamedTempFile;
    use tokio;

    async fn create_test_wal_with_records(records: Vec<LogRecord>) -> NamedTempFile {
        let temp_file = NamedTempFile::new().unwrap();
        let config = WalWriterConfig::default();
        let mut writer = WalWriter::new(temp_file.path().to_path_buf(), config);

        for record in records {
            writer.add_record(record).unwrap();
        }
        writer.flush().unwrap();

        temp_file
    }

    #[tokio::test]
    async fn test_analysis_empty_wal() {
        let temp_file = create_test_wal_with_records(vec![]).await;
        let config = WalReaderConfig::default();
        let mut wal_reader = WalReader::new(temp_file.path(), config);
        
        let mut analysis = AnalysisPhase::new(&mut wal_reader);
        let result = analysis.analyze().await.unwrap();
        
        assert_eq!(result.transaction_table.len(), 0);
        assert_eq!(result.dirty_page_table.len(), 0);
        assert_eq!(result.redo_lsn, None);
        assert_eq!(result.last_checkpoint_lsn, None);
        assert!(!result.recovery_needed());
    }

    #[tokio::test]
    async fn test_analysis_simple_transaction() {
        let tx_id = TransactionId(1);
        let page_id = PageId(100);
        let slot_id = SlotId(1);
        
        let records = vec![
            LogRecord::BeginTransaction { lsn: 1, tx_id },
            LogRecord::InsertRecord {
                lsn: 2,
                tx_id,
                page_id,
                slot_id,
                record_data: vec![1, 2, 3],
                prev_lsn: 1,
            },
            LogRecord::CommitTransaction {
                lsn: 3,
                tx_id,
                prev_lsn: 2,
            },
        ];
        
        let temp_file = create_test_wal_with_records(records).await;
        let config = WalReaderConfig::default();
        let mut wal_reader = WalReader::new(temp_file.path(), config);
        
        let mut analysis = AnalysisPhase::new(&mut wal_reader);
        let result = analysis.analyze().await.unwrap();
        
        assert_eq!(result.transaction_table.len(), 1);
        assert_eq!(result.dirty_page_table.len(), 1);
        assert_eq!(result.redo_lsn, Some(2)); // LSN of the insert record
        assert_eq!(result.records_processed, 3);
        
        // Transaction should be committed
        let tx_info = result.transaction_table.get(&tx_id).unwrap();
        assert_eq!(tx_info.state, crate::core::recovery::types::TransactionState::Committed);
        assert!(!tx_info.needs_undo());
        
        // Page should be in dirty page table
        let page_info = result.dirty_page_table.get(&page_id).unwrap();
        assert_eq!(page_info.recovery_lsn, 2);
    }

    #[tokio::test]
    async fn test_analysis_active_transaction() {
        let tx_id = TransactionId(1);
        let page_id = PageId(100);
        let slot_id = SlotId(1);
        
        let records = vec![
            LogRecord::BeginTransaction { lsn: 1, tx_id },
            LogRecord::InsertRecord {
                lsn: 2,
                tx_id,
                page_id,
                slot_id,
                record_data: vec![1, 2, 3],
                prev_lsn: 1,
            },
            // No commit record - transaction is still active
        ];
        
        let temp_file = create_test_wal_with_records(records).await;
        let config = WalReaderConfig::default();
        let mut wal_reader = WalReader::new(temp_file.path(), config);
        
        let mut analysis = AnalysisPhase::new(&mut wal_reader);
        let result = analysis.analyze().await.unwrap();
        
        assert_eq!(result.active_transaction_count(), 1);
        assert!(result.recovery_needed());
        
        // Transaction should be active and need undo
        let tx_info = result.transaction_table.get(&tx_id).unwrap();
        assert_eq!(tx_info.state, crate::core::recovery::types::TransactionState::Active);
        assert!(tx_info.needs_undo());
    }

    #[tokio::test]
    async fn test_analysis_with_checkpoint() {
        let tx_id = TransactionId(1);
        let page_id = PageId(100);
        
        let active_transactions = vec![ActiveTransactionInfo {
            tx_id,
            last_lsn: 5,
        }];
        
        let dirty_pages = vec![DirtyPageInfo {
            page_id,
            recovery_lsn: 3,
        }];
        
        let records = vec![
            LogRecord::CheckpointBegin { lsn: 10 },
            LogRecord::CheckpointEnd {
                lsn: 11,
                active_transactions,
                dirty_pages,
            },
            LogRecord::UpdateRecord {
                lsn: 12,
                tx_id,
                page_id,
                slot_id: SlotId(1),
                old_record_data: vec![1, 2],
                new_record_data: vec![3, 4],
                prev_lsn: 5,
            },
        ];
        
        let temp_file = create_test_wal_with_records(records).await;
        let config = WalReaderConfig::default();
        let mut wal_reader = WalReader::new(temp_file.path(), config);
        
        let mut analysis = AnalysisPhase::new(&mut wal_reader);
        let result = analysis.analyze().await.unwrap();
        
        assert_eq!(result.last_checkpoint_lsn, Some(11));
        assert_eq!(result.transaction_table.len(), 1);
        assert_eq!(result.dirty_page_table.len(), 1);
        assert_eq!(result.redo_lsn, Some(3)); // From checkpoint dirty page info
        
        // Transaction should be updated with new LSN
        let tx_info = result.transaction_table.get(&tx_id).unwrap();
        assert_eq!(tx_info.last_lsn, 12); // Updated by the UpdateRecord
    }
}