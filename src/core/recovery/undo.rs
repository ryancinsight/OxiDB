//! ARIES Undo Phase Implementation
//!
//! The Undo phase is the third and final phase of the ARIES recovery algorithm.
//! It undoes the effects of transactions that were active at the time of the crash,
//! ensuring that only committed transactions remain in the database.
//!
//! Key responsibilities:
//! - Process active transactions identified during the Analysis phase
//! - Traverse the log backwards using the prevLSN chain
//! - Generate Compensation Log Records (CLRs) for each undo operation
//! - Continue until all active transactions are fully undone
//! - Handle nested transactions and savepoints if supported

use crate::core::common::types::{Lsn, PageId, TransactionId};
use crate::core::recovery::tables::TransactionTable;
use crate::core::recovery::types::{RecoveryError, RecoveryState, TransactionInfo};
use crate::core::storage::engine::page::{Page, PageType};
use crate::core::wal::log_record::LogRecord;
use crate::core::wal::reader::WalReader;
use crate::core::wal::writer::{WalWriter, WalWriterConfig};
use log::{debug, info};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Statistics collected during the Undo phase.
#[derive(Debug, Clone)]
pub struct UndoStatistics {
    /// Number of transactions that needed to be undone
    pub transactions_undone: usize,
    /// Number of log records processed during undo
    pub records_processed: usize,
    /// Number of CLRs (Compensation Log Records) generated
    pub clrs_generated: usize,
    /// Current state of the undo phase
    pub state: RecoveryState,
}

impl UndoStatistics {
    /// Creates new empty undo statistics.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            transactions_undone: 0,
            records_processed: 0,
            clrs_generated: 0,
            state: RecoveryState::NotStarted,
        }
    }
}

impl Default for UndoStatistics {
    fn default() -> Self {
        Self::new()
    }
}

/// The Undo phase implementation for ARIES recovery.
pub struct UndoPhase {
    /// Transaction table from the Analysis phase
    transaction_table: TransactionTable,
    /// Cache of pages loaded during undo
    page_cache: HashMap<PageId, Arc<Mutex<Page>>>,
    /// Current state of the undo phase
    state: RecoveryState,
    /// Statistics collected during undo
    statistics: UndoStatistics,
    /// WAL writer for generating CLRs
    wal_writer: Option<WalWriter>,
}

impl UndoPhase {
    /// Creates a new Undo phase with the given transaction table.
    #[must_use]
    pub fn new(transaction_table: TransactionTable) -> Self {
        Self {
            transaction_table,
            page_cache: HashMap::new(),
            state: RecoveryState::NotStarted,
            statistics: UndoStatistics::new(),
            wal_writer: None,
        }
    }

    /// Performs the Undo phase of recovery.
    ///
    /// This method processes all active transactions identified during the Analysis phase,
    /// undoing their effects by traversing the log backwards and generating CLRs.
    pub fn undo<P: AsRef<Path>>(&mut self, wal_path: P) -> Result<(), RecoveryError> {
        self.state = RecoveryState::Undo;
        self.statistics.state = RecoveryState::Undo;

        info!("Starting undo phase");

        // Initialize WAL writer for CLRs
        self.initialize_wal_writer(&wal_path)?;

        // Get all active transactions that need to be undone
        let active_transactions: Vec<TransactionInfo> = self
            .transaction_table
            .active_transactions()
            .map(|(_, tx_info)| tx_info.clone())
            .collect();

        if active_transactions.is_empty() {
            info!("No active transactions found, skipping undo phase");
            self.state = RecoveryState::Completed;
            self.statistics.state = RecoveryState::Completed;
            return Ok(());
        }

        info!("Found {} active transactions to undo", active_transactions.len());

        // Create WAL reader for traversing log backwards
        let reader = WalReader::with_defaults(wal_path.as_ref());

        // Process each active transaction
        for tx_info in active_transactions {
            self.undo_transaction(&reader, &tx_info)?;
            self.statistics.transactions_undone += 1;
        }

        // Flush any remaining CLRs
        if let Some(ref mut writer) = self.wal_writer {
            writer
                .flush()
                .map_err(|e| RecoveryError::UndoError(format!("Failed to flush WAL: {e}")))?;
        }

        self.state = RecoveryState::Completed;
        self.statistics.state = RecoveryState::Completed;

        info!("Undo phase completed successfully. Undone {} transactions, processed {} records, generated {} CLRs",
              self.statistics.transactions_undone,
              self.statistics.records_processed,
              self.statistics.clrs_generated);

        Ok(())
    }

    /// Undoes a single transaction by traversing its log records backwards.
    fn undo_transaction(
        &mut self,
        reader: &WalReader,
        tx_info: &TransactionInfo,
    ) -> Result<(), RecoveryError> {
        debug!("Undoing transaction {} starting from LSN {}", tx_info.tx_id.0, tx_info.last_lsn);

        let mut current_lsn = Some(tx_info.last_lsn);
        let mut undo_next_lsn: Option<Lsn> = None;

        // Read all records to build a lookup map
        let all_records = reader
            .read_all_records()
            .map_err(|e| RecoveryError::UndoError(format!("Failed to read WAL records: {e}")))?;

        let mut record_map: HashMap<Lsn, LogRecord> = HashMap::new();
        for record in all_records {
            let lsn = self.extract_lsn(&record);
            record_map.insert(lsn, record);
        }

        // Traverse backwards through the transaction's log records
        while let Some(lsn) = current_lsn {
            if let Some(record) = record_map.get(&lsn) {
                // Only process records for this transaction
                if self.record_belongs_to_transaction(record, tx_info.tx_id) {
                    if let LogRecord::CompensationLogRecord { next_undo_lsn: unl, .. } = record {
                        // For CLRs, skip to the undo_next_lsn
                        current_lsn = *unl;
                        continue;
                    } else {
                        // Process the undo operation
                        debug!("Processing undo for record at LSN {}: {:?}", lsn, record);
                        let prev_lsn = self.undo_log_record(record, undo_next_lsn)?;
                        undo_next_lsn = Some(lsn);
                        current_lsn = prev_lsn;
                        // Only count records that actually need undo operations (not BeginTransaction)
                        match record {
                            LogRecord::InsertRecord { .. }
                            | LogRecord::DeleteRecord { .. }
                            | LogRecord::UpdateRecord { .. } => {
                                self.statistics.records_processed += 1;
                            }
                            _ => {} // Don't count BeginTransaction and other non-undoable records
                        }
                        debug!("CLRs generated so far: {}", self.statistics.clrs_generated);
                    }
                } else {
                    // This record doesn't belong to our transaction, get prev_lsn
                    current_lsn = self.extract_prev_lsn(record);
                }
            } else {
                // Record not found, stop traversal
                break;
            }
        }

        // Write an abort record for the transaction
        self.write_abort_record(tx_info.tx_id)?;

        debug!("Completed undoing transaction {}", tx_info.tx_id.0);
        Ok(())
    }

    /// Undoes a specific log record and generates a CLR.
    fn undo_log_record(
        &mut self,
        record: &LogRecord,
        undo_next_lsn: Option<Lsn>,
    ) -> Result<Option<Lsn>, RecoveryError> {
        debug!("undo_log_record called for: {:?}", record);
        match record {
            LogRecord::InsertRecord { lsn, tx_id, page_id, slot_id, prev_lsn, .. } => {
                // Undo insert by deleting the record
                self.undo_insert(*lsn, *tx_id, *page_id, *slot_id, undo_next_lsn)?;
                Ok(Some(*prev_lsn))
            }
            LogRecord::DeleteRecord { lsn, tx_id, page_id, slot_id, old_record_data, prev_lsn } => {
                // Undo delete by reinserting the record
                self.undo_delete(
                    *lsn,
                    *tx_id,
                    *page_id,
                    *slot_id,
                    old_record_data.clone(),
                    undo_next_lsn,
                )?;
                Ok(Some(*prev_lsn))
            }
            LogRecord::UpdateRecord {
                lsn,
                tx_id,
                page_id,
                slot_id,
                old_record_data,
                prev_lsn,
                ..
            } => {
                // Undo update by restoring the old record data
                self.undo_update(
                    *lsn,
                    *tx_id,
                    *page_id,
                    *slot_id,
                    old_record_data.clone(),
                    undo_next_lsn,
                )?;
                Ok(Some(*prev_lsn))
            }
            LogRecord::NewPage { prev_lsn, .. } => {
                // For new page records, we typically don't need to undo anything
                // as the page allocation will be handled by the storage manager
                Ok(Some(*prev_lsn))
            }
            LogRecord::BeginTransaction { .. } => {
                // Reached the beginning of the transaction
                Ok(None) // BeginTransaction doesn't have prev_lsn, so return None
            }
            _ => {
                // Other record types don't need undo
                Ok(self.extract_prev_lsn(record))
            }
        }
    }

    /// Undoes an insert operation by deleting the inserted record.
    fn undo_insert(
        &mut self,
        original_lsn: Lsn,
        tx_id: TransactionId,
        page_id: PageId,
        slot_id: crate::core::common::types::ids::SlotId,
        undo_next_lsn: Option<Lsn>,
    ) -> Result<(), RecoveryError> {
        debug!("Undoing insert: LSN {}, page {}, slot {}", original_lsn, page_id.0, slot_id.0);

        // Load the page (in a real implementation, this would interact with the buffer pool)
        let page = self.load_page(page_id)?;

        // Remove the record from the page
        {
            let _page_guard = page.lock().unwrap();
            // In a real implementation, this would call page.delete_record(slot_id)
            // For now, we'll simulate the operation
            debug!("Simulating deletion of record at slot {} on page {}", slot_id.0, page_id.0);
        }

        // Generate a CLR for the undo operation
        self.write_clr_for_insert_undo(tx_id, page_id, slot_id, original_lsn, undo_next_lsn)?;

        Ok(())
    }

    /// Undoes a delete operation by reinserting the deleted record.
    fn undo_delete(
        &mut self,
        original_lsn: Lsn,
        tx_id: TransactionId,
        page_id: PageId,
        slot_id: crate::core::common::types::ids::SlotId,
        record_data: Vec<u8>,
        undo_next_lsn: Option<Lsn>,
    ) -> Result<(), RecoveryError> {
        debug!("Undoing delete: LSN {}, page {}, slot {}", original_lsn, page_id.0, slot_id.0);

        // Load the page
        let page = self.load_page(page_id)?;

        // Reinsert the record
        {
            let _page_guard = page.lock().unwrap();
            // In a real implementation, this would call page.insert_record(slot_id, &record_data)
            debug!("Simulating reinsertion of record at slot {} on page {}", slot_id.0, page_id.0);
        }

        // Generate a CLR for the undo operation
        self.write_clr_for_delete_undo(
            tx_id,
            page_id,
            slot_id,
            record_data,
            original_lsn,
            undo_next_lsn,
        )?;

        Ok(())
    }

    /// Undoes an update operation by restoring the old record data.
    fn undo_update(
        &mut self,
        original_lsn: Lsn,
        tx_id: TransactionId,
        page_id: PageId,
        slot_id: crate::core::common::types::ids::SlotId,
        old_record_data: Vec<u8>,
        undo_next_lsn: Option<Lsn>,
    ) -> Result<(), RecoveryError> {
        debug!("Undoing update: LSN {}, page {}, slot {}", original_lsn, page_id.0, slot_id.0);

        // Load the page
        let page = self.load_page(page_id)?;

        // Restore the old record data
        {
            let _page_guard = page.lock().unwrap();
            // In a real implementation, this would call page.update_record(slot_id, &old_record_data)
            debug!("Simulating restoration of record at slot {} on page {}", slot_id.0, page_id.0);
        }

        // Generate a CLR for the undo operation
        self.write_clr_for_update_undo(
            tx_id,
            page_id,
            slot_id,
            old_record_data,
            original_lsn,
            undo_next_lsn,
        )?;

        Ok(())
    }

    /// Writes a CLR for an insert undo operation.
    fn write_clr_for_insert_undo(
        &mut self,
        tx_id: TransactionId,
        page_id: PageId,
        slot_id: crate::core::common::types::ids::SlotId,
        original_lsn: Lsn,
        undo_next_lsn: Option<Lsn>,
    ) -> Result<(), RecoveryError> {
        if let Some(ref mut writer) = self.wal_writer {
            let clr = LogRecord::CompensationLogRecord {
                lsn: 0, // Will be assigned by the writer
                tx_id,
                page_id,
                slot_id: Some(slot_id),
                undone_lsn: original_lsn,
                data_for_redo_of_undo: vec![], // No additional data needed for delete
                prev_lsn: 0,                   // Will be set by the writer
                next_undo_lsn: undo_next_lsn,
            };

            writer
                .add_record(&clr)
                .map_err(|e| RecoveryError::UndoError(format!("Failed to write CLR: {e}")))?;

            self.statistics.clrs_generated += 1;
            debug!("Generated CLR for insert undo on page {}, slot {}", page_id.0, slot_id.0);
        }
        Ok(())
    }

    /// Writes a CLR for a delete undo operation.
    fn write_clr_for_delete_undo(
        &mut self,
        tx_id: TransactionId,
        page_id: PageId,
        slot_id: crate::core::common::types::ids::SlotId,
        record_data: Vec<u8>,
        original_lsn: Lsn,
        undo_next_lsn: Option<Lsn>,
    ) -> Result<(), RecoveryError> {
        if let Some(ref mut writer) = self.wal_writer {
            let clr = LogRecord::CompensationLogRecord {
                lsn: 0, // Will be assigned by the writer
                tx_id,
                page_id,
                slot_id: Some(slot_id),
                undone_lsn: original_lsn,
                data_for_redo_of_undo: record_data,
                prev_lsn: 0, // Will be set by the writer
                next_undo_lsn: undo_next_lsn,
            };

            writer
                .add_record(&clr)
                .map_err(|e| RecoveryError::UndoError(format!("Failed to write CLR: {e}")))?;

            self.statistics.clrs_generated += 1;
            debug!("Generated CLR for delete undo on page {}, slot {}", page_id.0, slot_id.0);
        }
        Ok(())
    }

    /// Writes a CLR for an update undo operation.
    fn write_clr_for_update_undo(
        &mut self,
        tx_id: TransactionId,
        page_id: PageId,
        slot_id: crate::core::common::types::ids::SlotId,
        old_record_data: Vec<u8>,
        original_lsn: Lsn,
        undo_next_lsn: Option<Lsn>,
    ) -> Result<(), RecoveryError> {
        if let Some(ref mut writer) = self.wal_writer {
            let clr = LogRecord::CompensationLogRecord {
                lsn: 0, // Will be assigned by the writer
                tx_id,
                page_id,
                slot_id: Some(slot_id),
                undone_lsn: original_lsn,
                data_for_redo_of_undo: old_record_data,
                prev_lsn: 0, // Will be set by the writer
                next_undo_lsn: undo_next_lsn,
            };

            writer
                .add_record(&clr)
                .map_err(|e| RecoveryError::UndoError(format!("Failed to write CLR: {e}")))?;

            self.statistics.clrs_generated += 1;
            debug!("Generated CLR for update undo on page {}, slot {}", page_id.0, slot_id.0);
        }
        Ok(())
    }

    /// Writes an abort record for a transaction.
    fn write_abort_record(&mut self, tx_id: TransactionId) -> Result<(), RecoveryError> {
        if let Some(ref mut writer) = self.wal_writer {
            let abort_record = LogRecord::AbortTransaction {
                lsn: 0, // Will be assigned by the writer
                tx_id,
                prev_lsn: 0, // This will be the last record for the transaction
            };

            writer.add_record(&abort_record).map_err(|e| {
                RecoveryError::UndoError(format!("Failed to write abort record: {e}"))
            })?;

            debug!("Generated abort record for transaction {}", tx_id.0);
        }
        Ok(())
    }

    /// Initializes the WAL writer for generating CLRs.
    fn initialize_wal_writer<P: AsRef<Path>>(&mut self, wal_path: P) -> Result<(), RecoveryError> {
        let config = WalWriterConfig::default();
        let writer = WalWriter::new(wal_path.as_ref().to_path_buf(), config);
        self.wal_writer = Some(writer);
        Ok(())
    }

    /// Loads a page into the cache or returns the cached version.
    fn load_page(&mut self, page_id: PageId) -> Result<Arc<Mutex<Page>>, RecoveryError> {
        if let Some(page) = self.page_cache.get(&page_id) {
            Ok(page.clone())
        } else {
            // In a real implementation, this would load from the buffer pool
            // For now, create a mock page
            let page = Arc::new(Mutex::new(Page::new(page_id, PageType::Data)));
            self.page_cache.insert(page_id, page.clone());
            Ok(page)
        }
    }

    /// Checks if a log record belongs to the specified transaction.
    fn record_belongs_to_transaction(&self, record: &LogRecord, tx_id: TransactionId) -> bool {
        match record {
            LogRecord::BeginTransaction { tx_id: record_tx_id, .. }
            | LogRecord::CommitTransaction { tx_id: record_tx_id, .. }
            | LogRecord::AbortTransaction { tx_id: record_tx_id, .. }
            | LogRecord::InsertRecord { tx_id: record_tx_id, .. }
            | LogRecord::DeleteRecord { tx_id: record_tx_id, .. }
            | LogRecord::UpdateRecord { tx_id: record_tx_id, .. }
            | LogRecord::NewPage { tx_id: record_tx_id, .. }
            | LogRecord::CompensationLogRecord { tx_id: record_tx_id, .. } => {
                *record_tx_id == tx_id
            }
            LogRecord::CheckpointBegin { .. } | LogRecord::CheckpointEnd { .. } => false,
        }
    }

    /// Extracts the LSN from a log record.
    const fn extract_lsn(&self, record: &LogRecord) -> Lsn {
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

    /// Extracts the previous LSN from a log record.
    const fn extract_prev_lsn(&self, record: &LogRecord) -> Option<Lsn> {
        match record {
            LogRecord::CommitTransaction { prev_lsn, .. }
            | LogRecord::AbortTransaction { prev_lsn, .. }
            | LogRecord::InsertRecord { prev_lsn, .. }
            | LogRecord::DeleteRecord { prev_lsn, .. }
            | LogRecord::UpdateRecord { prev_lsn, .. }
            | LogRecord::NewPage { prev_lsn, .. } => Some(*prev_lsn),
            LogRecord::BeginTransaction { .. }
            | LogRecord::CompensationLogRecord { .. }
            | LogRecord::CheckpointBegin { .. }
            | LogRecord::CheckpointEnd { .. } => None,
        }
    }

    /// Returns the current state of the undo phase.
    #[must_use]
    pub const fn get_state(&self) -> &RecoveryState {
        &self.state
    }

    /// Returns the statistics collected during the undo phase.
    #[must_use]
    pub const fn get_statistics(&self) -> &UndoStatistics {
        &self.statistics
    }

    /// Returns the number of pages currently cached.
    #[must_use]
    pub fn cache_size(&self) -> usize {
        self.page_cache.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::common::types::ids::{PageId, SlotId};
    use crate::core::common::types::TransactionId;
    use crate::core::recovery::types::TransactionInfo;
    use crate::core::wal::log_record::LogRecord;
    use crate::core::wal::writer::{WalWriter, WalWriterConfig};
    use tempfile::NamedTempFile;

    fn create_test_wal_with_records(records: Vec<LogRecord>) -> NamedTempFile {
        let temp_file = NamedTempFile::new().unwrap();
        let config = WalWriterConfig::default();
        let mut writer = WalWriter::new(temp_file.path().to_path_buf(), config);

        for record in records {
            writer.add_record(&record).unwrap();
        }
        writer.flush().unwrap();

        temp_file
    }

    #[test]
    fn test_undo_phase_creation() {
        let mut transaction_table = TransactionTable::new();
        let tx_info = TransactionInfo::new_active(TransactionId(1), 100);
        transaction_table.insert(tx_info);

        let undo_phase = UndoPhase::new(transaction_table);

        assert_eq!(undo_phase.get_state(), &RecoveryState::NotStarted);
        assert_eq!(undo_phase.cache_size(), 0);
        assert_eq!(undo_phase.get_statistics().transactions_undone, 0);
    }

    #[test]
    fn test_undo_phase_no_active_transactions() {
        let transaction_table = TransactionTable::new(); // Empty table
        let mut undo_phase = UndoPhase::new(transaction_table);

        let temp_file = create_test_wal_with_records(vec![]);
        let result = undo_phase.undo(temp_file.path());

        assert!(result.is_ok());
        assert_eq!(undo_phase.get_state(), &RecoveryState::Completed);
        assert_eq!(undo_phase.get_statistics().transactions_undone, 0);
    }

    #[test]
    fn test_undo_phase_with_active_transaction() {
        let tx_id = TransactionId(1);
        let page_id = PageId(100);
        let slot_id = SlotId(1);

        // Create a transaction table with one active transaction
        let mut transaction_table = TransactionTable::new();
        let tx_info = TransactionInfo::new_active(tx_id, 3); // Last LSN is 3
        transaction_table.insert(tx_info);

        // Create WAL records for the transaction
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
            LogRecord::UpdateRecord {
                lsn: 3,
                tx_id,
                page_id,
                slot_id,
                old_record_data: vec![1, 2, 3],
                new_record_data: vec![4, 5, 6],
                prev_lsn: 2,
            },
        ];

        let temp_file = create_test_wal_with_records(records);
        let mut undo_phase = UndoPhase::new(transaction_table);

        let result = undo_phase.undo(temp_file.path());

        // Test assertions

        assert!(result.is_ok());
        assert_eq!(undo_phase.get_state(), &RecoveryState::Completed);
        assert_eq!(undo_phase.get_statistics().transactions_undone, 1);
        assert_eq!(undo_phase.get_statistics().records_processed, 2); // Insert and Update
        assert_eq!(undo_phase.get_statistics().clrs_generated, 2); // CLR for each operation
    }

    #[test]
    fn test_record_belongs_to_transaction() {
        let transaction_table = TransactionTable::new();
        let undo_phase = UndoPhase::new(transaction_table);

        let tx_id = TransactionId(1);
        let other_tx_id = TransactionId(2);

        let record = LogRecord::InsertRecord {
            lsn: 1,
            tx_id,
            page_id: PageId(100),
            slot_id: SlotId(1),
            record_data: vec![1, 2, 3],
            prev_lsn: 0,
        };

        assert!(undo_phase.record_belongs_to_transaction(&record, tx_id));
        assert!(!undo_phase.record_belongs_to_transaction(&record, other_tx_id));
    }

    #[test]
    fn test_extract_lsn() {
        let transaction_table = TransactionTable::new();
        let undo_phase = UndoPhase::new(transaction_table);

        let record = LogRecord::InsertRecord {
            lsn: 42,
            tx_id: TransactionId(1),
            page_id: PageId(100),
            slot_id: SlotId(1),
            record_data: vec![1, 2, 3],
            prev_lsn: 0,
        };

        assert_eq!(undo_phase.extract_lsn(&record), 42);
    }

    #[test]
    fn test_extract_prev_lsn() {
        let transaction_table = TransactionTable::new();
        let undo_phase = UndoPhase::new(transaction_table);

        let record_with_prev = LogRecord::InsertRecord {
            lsn: 42,
            tx_id: TransactionId(1),
            page_id: PageId(100),
            slot_id: SlotId(1),
            record_data: vec![1, 2, 3],
            prev_lsn: 41,
        };

        let record_without_prev = LogRecord::BeginTransaction { lsn: 1, tx_id: TransactionId(1) };

        assert_eq!(undo_phase.extract_prev_lsn(&record_with_prev), Some(41));
        assert_eq!(undo_phase.extract_prev_lsn(&record_without_prev), None);
    }

    #[test]
    fn test_undo_statistics() {
        let stats = UndoStatistics::new();

        assert_eq!(stats.transactions_undone, 0);
        assert_eq!(stats.records_processed, 0);
        assert_eq!(stats.clrs_generated, 0);
        assert_eq!(stats.state, RecoveryState::NotStarted);
    }
}
