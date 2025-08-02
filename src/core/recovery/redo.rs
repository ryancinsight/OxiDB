//! ARIES Redo Phase Implementation
//!
//! The Redo phase is the second phase of the ARIES recovery algorithm.
//! It repeats history by redoing all operations from the redo LSN forward,
//! ensuring that all committed changes are properly applied to the database.
//!
//! Key responsibilities:
//! - Start from the redo LSN determined by the Analysis phase
//! - Redo all operations for pages that were dirty at the time of crash
//! - Apply changes only if the page LSN is less than the log record LSN
//! - Update page LSNs after successful redo operations

use crate::core::common::types::{Lsn, PageId};
use crate::core::recovery::tables::DirtyPageTable;
use crate::core::recovery::types::{RecoveryError, RecoveryState};
use crate::core::storage::engine::page::{Page, PageType};
use crate::core::wal::log_record::LogRecord;
use crate::core::wal::reader::WalReader;
use log;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// The Redo phase of ARIES recovery.
///
/// This phase repeats history by redoing all operations from the redo LSN forward.
/// It ensures that all committed changes are properly applied to the database pages.
pub struct RedoPhase {
    /// The dirty page table from the Analysis phase
    dirty_page_table: DirtyPageTable,
    /// Cache of pages loaded during redo
    page_cache: HashMap<PageId, Arc<Mutex<Page>>>,
    /// The LSN to start redoing from
    redo_lsn: Option<Lsn>,
    /// Current state of the redo phase
    state: RecoveryState,
}

impl RedoPhase {
    /// Creates a new `RedoPhase` with the given dirty page table.
    #[must_use]
    pub fn new(dirty_page_table: DirtyPageTable) -> Self {
        let redo_lsn = dirty_page_table.min_recovery_lsn();

        Self {
            dirty_page_table,
            page_cache: HashMap::new(),
            redo_lsn,
            state: RecoveryState::NotStarted,
        }
    }

    /// Performs the complete redo phase.
    ///
    /// # Arguments
    /// * `wal_path` - Path to the WAL file
    ///
    /// # Returns
    /// * `Ok(())` if redo completed successfully
    /// * `Err(RecoveryError)` if an error occurred during redo
    pub fn redo<P: AsRef<Path>>(&mut self, wal_path: P) -> Result<(), RecoveryError> {
        self.state = RecoveryState::Redo;

        // If there's no redo LSN, no redo is needed
        let redo_lsn = if let Some(lsn) = self.redo_lsn {
            lsn
        } else {
            log::// info!("No redo LSN found, skipping redo phase");
            return Ok(());
        };

        log::// info!("Starting redo phase from LSN {}", redo_lsn);

        // Create WAL reader from path
        let reader = WalReader::with_defaults(wal_path.as_ref());
        let iterator = reader
            .iter_records()
            .map_err(|e| RecoveryError::WalError(format!("Failed to create WAL iterator: {e}")))?;

        // Read all records and process those from redo LSN forward
        for result in iterator {
            let log_record = result
                .map_err(|e| RecoveryError::WalError(format!("Failed to read WAL record: {e}")))?;
            let record_lsn = self.get_record_lsn(&log_record);
            if record_lsn >= redo_lsn {
                self.process_log_record(&log_record)?;
            }
        }

        log::// info!("Redo phase completed successfully");
        Ok(())
    }

    /// Processes a single log record during the redo phase.
    ///
    /// # Arguments
    /// * `log_record` - The log record to process
    ///
    /// # Returns
    /// * `Ok(())` if the record was processed successfully
    /// * `Err(RecoveryError)` if an error occurred
    fn process_log_record(&mut self, log_record: &LogRecord) -> Result<(), RecoveryError> {
        match log_record {
            LogRecord::UpdateRecord {
                lsn, page_id, old_record_data: _, new_record_data, ..
            } => self.redo_update(*lsn, *page_id, new_record_data)?,
            LogRecord::InsertRecord { lsn, page_id, record_data, .. } => {
                self.redo_insert(*lsn, *page_id, record_data)?;
            }
            LogRecord::DeleteRecord { lsn, page_id, .. } => self.redo_delete(*lsn, *page_id)?,
            LogRecord::BeginTransaction { .. }
            | LogRecord::CommitTransaction { .. }
            | LogRecord::AbortTransaction { .. }
            | LogRecord::CheckpointBegin { .. }
            | LogRecord::CheckpointEnd { .. }
            | LogRecord::NewPage { .. }
            | LogRecord::CompensationLogRecord { .. } => {
                // These record types don't require redo operations
            }
        }
        Ok(())
    }

    /// Redoes an update operation.
    ///
    /// # Arguments
    /// * `lsn` - LSN of the log record
    /// * `page_id` - ID of the page to update
    /// * `after_image` - The data to apply to the page
    fn redo_update(
        &mut self,
        lsn: Lsn,
        page_id: PageId,
        after_image: &[u8],
    ) -> Result<(), RecoveryError> {
        // Only redo if the page was dirty at crash time
        if !self.dirty_page_table.contains(&page_id) {
            return Ok(());
        }

        let page = self.get_or_load_page(page_id)?;
        let mut page_guard = page.lock().unwrap();

        // Only redo if page LSN < log record LSN
        if page_guard.get_lsn() < lsn {
            page_guard.apply_update(after_image).map_err(|e| {
                RecoveryError::RedoError(format!(
                    "Failed to apply update to page {}: {}",
                    page_id.0, e
                ))
            })?;

            page_guard.set_lsn(lsn);
            log::// debug!("Redid update on page {} with LSN {}", page_id.0, lsn);
        }

        Ok(())
    }

    /// Redoes an insert operation.
    ///
    /// # Arguments
    /// * `lsn` - LSN of the log record
    /// * `page_id` - ID of the page to insert into
    /// * `data` - The data to insert
    fn redo_insert(&mut self, lsn: Lsn, page_id: PageId, data: &[u8]) -> Result<(), RecoveryError> {
        // Only redo if the page was dirty at crash time
        if !self.dirty_page_table.contains(&page_id) {
            return Ok(());
        }

        let page = self.get_or_load_page(page_id)?;
        let mut page_guard = page.lock().unwrap();

        // Only redo if page LSN < log record LSN
        if page_guard.get_lsn() < lsn {
            page_guard.apply_insert(data).map_err(|e| {
                RecoveryError::RedoError(format!(
                    "Failed to apply insert to page {}: {}",
                    page_id.0, e
                ))
            })?;

            page_guard.set_lsn(lsn);
            log::// debug!("Redid insert on page {} with LSN {}", page_id.0, lsn);
        }

        Ok(())
    }

    /// Redoes a delete operation.
    ///
    /// # Arguments
    /// * `lsn` - LSN of the log record
    /// * `page_id` - ID of the page to delete from
    fn redo_delete(&mut self, lsn: Lsn, page_id: PageId) -> Result<(), RecoveryError> {
        // Only redo if the page was dirty at crash time
        if !self.dirty_page_table.contains(&page_id) {
            return Ok(());
        }

        let page = self.get_or_load_page(page_id)?;
        let mut page_guard = page.lock().unwrap();

        // Only redo if page LSN < log record LSN
        if page_guard.get_lsn() < lsn {
            page_guard.apply_delete().map_err(|e| {
                RecoveryError::RedoError(format!(
                    "Failed to apply delete to page {}: {}",
                    page_id.0, e
                ))
            })?;

            page_guard.set_lsn(lsn);
            log::// debug!("Redid delete on page {} with LSN {}", page_id.0, lsn);
        }

        Ok(())
    }

    /// Gets a page from the cache or loads it from storage.
    ///
    /// # Arguments
    /// * `page_id` - ID of the page to get or load
    ///
    /// # Returns
    /// * `Ok(Arc<Mutex<Page>>)` - The page wrapped in Arc<Mutex>
    /// * `Err(RecoveryError)` - If the page could not be loaded
    fn get_or_load_page(&mut self, page_id: PageId) -> Result<Arc<Mutex<Page>>, RecoveryError> {
        if let Some(page) = self.page_cache.get(&page_id) {
            return Ok(Arc::clone(page));
        }

        // In a real implementation, this would load the page from storage
        // For now, we'll create a mock page
        let page = Arc::new(Mutex::new(Page::new(page_id, PageType::Data)));
        self.page_cache.insert(page_id, Arc::clone(&page));

        Ok(page)
    }

    /// Returns the redo LSN determined for this phase.
    #[must_use]
    pub const fn get_redo_lsn(&self) -> Option<Lsn> {
        self.redo_lsn
    }

    /// Returns the current state of the redo phase.
    #[must_use]
    pub const fn get_state(&self) -> &RecoveryState {
        &self.state
    }

    /// Returns the number of pages in the cache.
    #[must_use]
    pub fn cache_size(&self) -> usize {
        self.page_cache.len()
    }

    /// Clears the page cache.
    pub fn clear_cache(&mut self) {
        self.page_cache.clear();
    }

    /// Returns statistics about the redo phase.
    #[must_use]
    pub fn get_statistics(&self) -> RedoStatistics {
        RedoStatistics {
            redo_lsn: self.redo_lsn,
            dirty_pages_count: self.dirty_page_table.len(),
            cached_pages_count: self.page_cache.len(),
            state: self.state.clone(),
        }
    }

    /// Extracts the LSN from a log record.
    const fn get_record_lsn(&self, record: &LogRecord) -> Lsn {
        self.extract_lsn(record)
    }

    /// Extract LSN from a log record
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
}

/// Statistics about the redo phase execution.
#[derive(Debug, Clone)]
pub struct RedoStatistics {
    /// The LSN from which redo started
    pub redo_lsn: Option<Lsn>,
    /// Number of dirty pages at the start of redo
    pub dirty_pages_count: usize,
    /// Number of pages currently cached
    pub cached_pages_count: usize,
    /// Current state of the redo phase
    pub state: RecoveryState,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::common::types::{PageId, TransactionId};
    use tempfile::NamedTempFile;

    fn create_test_wal_with_records() -> NamedTempFile {
        // For now, just create an empty temp file
        // In a real implementation, this would write actual WAL records
        NamedTempFile::new().unwrap()
    }

    #[test]
    fn test_redo_phase_creation() {
        let mut dirty_page_table = DirtyPageTable::new();
        dirty_page_table.insert(PageId(100), 50);
        dirty_page_table.insert(PageId(200), 75);

        let redo_phase = RedoPhase::new(dirty_page_table);

        assert_eq!(redo_phase.get_redo_lsn(), Some(50));
        assert_eq!(redo_phase.get_state(), &RecoveryState::NotStarted);
        assert_eq!(redo_phase.cache_size(), 0);
    }

    #[test]
    fn test_redo_phase_empty_dirty_page_table() {
        let dirty_page_table = DirtyPageTable::new();
        let redo_phase = RedoPhase::new(dirty_page_table);

        assert_eq!(redo_phase.get_redo_lsn(), None);
    }

    #[test]
    fn test_redo_phase_with_wal() {
        let temp_file = create_test_wal_with_records();

        let mut dirty_page_table = DirtyPageTable::new();
        dirty_page_table.insert(PageId(100), 1); // Start redo from LSN 1

        let mut redo_phase = RedoPhase::new(dirty_page_table);

        // Perform redo
        let result = redo_phase.redo(temp_file.path());
        assert!(result.is_ok());

        // Since the test WAL file is empty, no pages should be loaded into cache
        // In a real scenario with actual WAL records, pages would be loaded
        assert_eq!(redo_phase.cache_size(), 0);
    }

    #[test]
    fn test_redo_phase_no_redo_needed() {
        let temp_file = create_test_wal_with_records();

        // Empty dirty page table means no redo needed
        let dirty_page_table = DirtyPageTable::new();
        let mut redo_phase = RedoPhase::new(dirty_page_table);

        let result = redo_phase.redo(temp_file.path());
        assert!(result.is_ok());

        // No pages should be cached since no redo was needed
        assert_eq!(redo_phase.cache_size(), 0);
    }

    #[test]
    fn test_redo_statistics() {
        let mut dirty_page_table = DirtyPageTable::new();
        dirty_page_table.insert(PageId(100), 50);
        dirty_page_table.insert(PageId(200), 75);

        let redo_phase = RedoPhase::new(dirty_page_table);
        let stats = redo_phase.get_statistics();

        assert_eq!(stats.redo_lsn, Some(50));
        assert_eq!(stats.dirty_pages_count, 2);
        assert_eq!(stats.cached_pages_count, 0);
        assert_eq!(stats.state, RecoveryState::NotStarted);
    }

    #[test]
    fn test_cache_operations() {
        let dirty_page_table = DirtyPageTable::new();
        let mut redo_phase = RedoPhase::new(dirty_page_table);

        // Load a page into cache
        let page_id = PageId(123);
        let result = redo_phase.get_or_load_page(page_id);
        assert!(result.is_ok());
        assert_eq!(redo_phase.cache_size(), 1);

        // Load the same page again (should come from cache)
        let result2 = redo_phase.get_or_load_page(page_id);
        assert!(result2.is_ok());
        assert_eq!(redo_phase.cache_size(), 1); // Still 1, not 2

        // Clear cache
        redo_phase.clear_cache();
        assert_eq!(redo_phase.cache_size(), 0);
    }

    #[test]
    fn test_process_different_log_record_types() {
        let mut dirty_page_table = DirtyPageTable::new();
        let page_id = PageId(100);
        dirty_page_table.insert(page_id, 1);

        let mut redo_phase = RedoPhase::new(dirty_page_table);
        let tx_id = TransactionId(1);

        // Test Begin record (should not cause errors)
        let begin_record = LogRecord::BeginTransaction { lsn: 1, tx_id };
        assert!(redo_phase.process_log_record(&begin_record).is_ok());

        // Test Commit record (should not cause errors)
        let commit_record = LogRecord::CommitTransaction { lsn: 2, tx_id, prev_lsn: 1 };
        assert!(redo_phase.process_log_record(&commit_record).is_ok());
    }
}
