// src/core/storage/engine/implementations/in_memory.rs
use crate::core::common::OxidbError;
use crate::core::common::types::Lsn; // Added Lsn import
use crate::core::storage::engine::traits::{KeyValueStore, VersionedValue};
use crate::core::storage::engine::wal::WalEntry;
use crate::core::transaction::Transaction;
use std::collections::{HashMap, HashSet}; // Added HashSet

#[derive(Debug, Default)] // Added Default
pub struct InMemoryKvStore {
    pub(super) data: HashMap<Vec<u8>, Vec<VersionedValue<Vec<u8>>>>, // Made pub(super) for GC tests
}

impl InMemoryKvStore {
    pub fn new() -> Self {
        InMemoryKvStore { data: HashMap::new() }
    }
}

impl KeyValueStore<Vec<u8>, Vec<u8>> for InMemoryKvStore {
    fn put(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        transaction: &Transaction,
        _lsn: Lsn, // Added _lsn, unused in this implementation
    ) -> Result<(), OxidbError> {
        let versions = self.data.entry(key).or_default();
        // Mark the latest existing visible version (if any) as expired by this transaction.
        for version in versions.iter_mut().rev() {
            if version.expired_tx_id.is_none() {
                version.expired_tx_id = Some(transaction.id.0); // Use .0
                break; // Only expire the most recent version
            }
        }

        let new_version =
            VersionedValue { value, created_tx_id: transaction.id.0, expired_tx_id: None }; // Use .0
        versions.push(new_version);
        Ok(())
    }

    fn get(
        &self,
        key: &Vec<u8>,
        snapshot_id: u64,
        committed_ids: &HashSet<u64>,
    ) -> Result<Option<Vec<u8>>, OxidbError> {
        if let Some(versions) = self.data.get(key) {
            for version in versions.iter().rev() {
                // Is the version itself visible?
                // Case 1: Reading within a transaction (snapshot_id != 0)
                //         - Version created by the current transaction.
                // Case 2: Reading committed state (snapshot_id == 0 or snapshot_id != 0)
                //         - Version created by a committed transaction.
                // Case 3: Special handling for "auto-committed" data (created_tx_id == 0)
                //         when read by a "no active transaction" snapshot (snapshot_id == 0).
                let created_by_current_tx = snapshot_id != 0 && version.created_tx_id == snapshot_id;
                let is_committed_creator = committed_ids.contains(&version.created_tx_id);
                let is_autocommit_data_visible_to_autocommit_snapshot = version.created_tx_id == 0 && snapshot_id == 0;

                if created_by_current_tx || is_committed_creator || is_autocommit_data_visible_to_autocommit_snapshot {
                    // If visible, check if it's also visibly expired
                    if let Some(expired_tx_id) = version.expired_tx_id {
                        let expired_by_current_tx = snapshot_id != 0 && expired_tx_id == snapshot_id;
                        let is_committed_expirer = committed_ids.contains(&expired_tx_id);
                        let is_autocommit_expiry_visible_to_autocommit_snapshot = expired_tx_id == 0 && snapshot_id == 0;

                        if !(expired_by_current_tx || is_committed_expirer || is_autocommit_expiry_visible_to_autocommit_snapshot) {
                            // Not visibly expired, so this version is the one
                            return Ok(Some(version.value.clone()));
                        }
                        // If visibly expired, continue to older version
                    } else {
                        // No expiration, so this version is the one
                        return Ok(Some(version.value.clone()));
                    }
                }
            }
        }
        Ok(None)
    }

    fn delete(&mut self, key: &Vec<u8>, transaction: &Transaction, _lsn: Lsn) -> Result<bool, OxidbError> { // Added _lsn
        if let Some(versions) = self.data.get_mut(key) {
            for version in versions.iter_mut().rev() {
                // Compare TransactionId directly if Transaction.id is TransactionId struct
                // If Transaction.id is u64, then this is fine.
                // Assuming Transaction.id is u64 based on VersionedValue fields.
                // If Transaction.id became TransactionId, then version.created_tx_id should be compared with transaction.id.0
                // For now, assuming transaction.id is u64 for comparison with VersionedValue fields.
                // This part might need adjustment if Transaction.id type changed to TransactionId struct.
                // Based on Transaction struct, id is TransactionId. So comparison should be with transaction.id.0.
                if version.created_tx_id <= transaction.id.0 // Use .0 if transaction.id is TransactionId struct
                    && (version.expired_tx_id.is_none()
                        || version.expired_tx_id.unwrap() > transaction.id.0) // Use .0
                {
                    if version.expired_tx_id.is_none() {
                        version.expired_tx_id = Some(transaction.id.0); // Use .0
                        return Ok(true);
                    } else {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(false)
    }

    fn contains_key(
        &self,
        key: &Vec<u8>,
        snapshot_id: u64,
        committed_ids: &HashSet<u64>,
    ) -> Result<bool, OxidbError> { // Changed
        self.get(key, snapshot_id, committed_ids).map(|opt| opt.is_some())
    }

    fn log_wal_entry(&mut self, _entry: &WalEntry) -> Result<(), OxidbError> { // Changed
        Ok(())
    }

    fn gc(&mut self, low_water_mark: u64, committed_ids: &HashSet<u64>) -> Result<(), OxidbError> { // Changed
        self.data.retain(|_key, versions| {
            versions.retain_mut(|v| {
                let created_by_committed = committed_ids.contains(&v.created_tx_id);
                if !created_by_committed && v.created_tx_id < low_water_mark {
                    return false;
                }
                if created_by_committed && v.expired_tx_id.is_some() {
                    let etid = v.expired_tx_id.unwrap();
                    if committed_ids.contains(&etid) && etid < low_water_mark {
                        return false;
                    }
                }
                true
            });
            !versions.is_empty()
        });
        Ok(())
    }

    fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, OxidbError> { // Changed
        let mut results = Vec::new();
        for (key, version_vec) in self.data.iter() {
            if let Some(_latest_version) = version_vec.last() {
                let mut best_candidate: Option<&VersionedValue<Vec<u8>>> = None;
                for version in version_vec.iter().rev() {
                    if version.expired_tx_id.is_none() {
                        best_candidate = Some(version);
                        break;
                    }
                }
                if best_candidate.is_none() && !version_vec.is_empty() {
                    // No action if all expired for now
                }
                if let Some(visible_version) = best_candidate {
                    results.push((key.clone(), visible_version.value.clone()));
                }
            }
        }
        Ok(results)
    }
}
