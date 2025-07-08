//! ARIES Recovery Implementation
//!
//! This module implements the ARIES (Algorithm for Recovery and Isolation Exploiting Semantics)
//! recovery algorithm, which is the industry standard for database recovery.
//!
//! ARIES consists of three phases:
//! 1. **Analysis Phase**: Determines which transactions were active at the time of crash
//!    and which pages might have been dirty.
//! 2. **Redo Phase**: Repeats history by redoing all operations, ensuring that all
//!    committed changes are applied.
//! 3. **Undo Phase**: Undoes the effects of transactions that were active at crash time.
//!
//! ## Module Structure
//!
//! - `analysis`: Implementation of the Analysis phase
//! - `redo`: Implementation of the Redo phase
//! - `tables`: Transaction and dirty page tables used during recovery
//! - `types`: Common types and enums used throughout recovery

pub mod analysis;
pub mod redo;
pub mod tables;
pub mod types;
pub mod undo;

pub use analysis::{AnalysisPhase, AnalysisResult};
pub use redo::RedoPhase;
pub use tables::{DirtyPageTable, TransactionTable};
pub use types::{RecoveryError, RecoveryState, TransactionState};
pub use undo::UndoPhase;

use crate::core::wal::reader::WalReader;
use std::path::Path;

/// The main recovery manager that orchestrates the ARIES recovery process.
///
/// This struct coordinates the three phases of recovery and maintains the overall
/// recovery state.
pub struct RecoveryManager {
    wal_reader: WalReader,
    wal_file_path: std::path::PathBuf,
}

impl RecoveryManager {
    /// Creates a new recovery manager with the specified WAL reader.
    pub fn new<P: AsRef<Path>>(wal_reader: WalReader, wal_file_path: P) -> Self {
        Self { wal_reader, wal_file_path: wal_file_path.as_ref().to_path_buf() }
    }

    /// Creates a new recovery manager from a WAL file path.
    pub fn from_wal_file<P: AsRef<Path>>(wal_file_path: P) -> Result<Self, RecoveryError> {
        let wal_reader = WalReader::with_defaults(&wal_file_path);
        Ok(Self::new(wal_reader, wal_file_path))
    }

    /// Performs the complete ARIES recovery process.
    ///
    /// This method executes all three phases of recovery:
    /// 1. Analysis phase to build transaction and dirty page tables
    /// 2. Redo phase to restore the database state
    /// 3. Undo phase to roll back uncommitted transactions
    pub async fn recover(&mut self) -> Result<(), RecoveryError> {
        // Phase 1: Analysis
        let analysis_result = self.run_analysis_phase().await?;

        // Phase 2: Redo
        self.run_redo_phase(&analysis_result)?;

        // Phase 3: Undo
        self.run_undo_phase(&analysis_result)?;

        Ok(())
    }

    /// Runs the Analysis phase of recovery.
    async fn run_analysis_phase(&mut self) -> Result<AnalysisResult, RecoveryError> {
        let mut analysis_phase = AnalysisPhase::new(&mut self.wal_reader);
        analysis_phase.analyze().await
    }

    /// Runs the Redo phase of recovery.
    fn run_redo_phase(&mut self, analysis_result: &AnalysisResult) -> Result<(), RecoveryError> {
        let mut redo_phase = RedoPhase::new(analysis_result.dirty_page_table.clone());
        redo_phase.redo(&self.wal_file_path)
    }

    /// Runs the Undo phase of recovery.
    fn run_undo_phase(&mut self, analysis_result: &AnalysisResult) -> Result<(), RecoveryError> {
        let mut undo_phase = UndoPhase::new(analysis_result.transaction_table.clone());
        undo_phase.undo(&self.wal_file_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::wal::WalReader;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_recovery_manager_creation() {
        let temp_file = NamedTempFile::new().unwrap();
        let wal_reader = WalReader::with_defaults(temp_file.path());

        let recovery_manager = RecoveryManager::new(wal_reader, temp_file.path());
        assert!(recovery_manager.wal_reader.get_statistics().unwrap().total_records == 0);
    }

    #[tokio::test]
    async fn test_recovery_manager_from_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let recovery_manager = RecoveryManager::from_wal_file(temp_file.path());
        assert!(recovery_manager.is_ok());
    }
}
