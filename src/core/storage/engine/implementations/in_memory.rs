// src/core/storage/engine/implementations/in_memory.rs
use crate::core::common::types::Lsn; // Added Lsn import
use crate::core::common::OxidbError;
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
                let created_by_current_tx =
                    snapshot_id != 0 && version.created_tx_id == snapshot_id;
                let is_committed_creator = committed_ids.contains(&version.created_tx_id);
                let is_autocommit_data_visible_to_autocommit_snapshot =
                    version.created_tx_id == 0 && snapshot_id == 0;

                if created_by_current_tx
                    || is_committed_creator
                    || is_autocommit_data_visible_to_autocommit_snapshot
                {
                    // If visible, check if it's also visibly expired
                    if let Some(expired_tx_id) = version.expired_tx_id {
                        let expired_by_current_tx =
                            snapshot_id != 0 && expired_tx_id == snapshot_id;
                        let is_committed_expirer = committed_ids.contains(&expired_tx_id);
                        let is_autocommit_expiry_visible_to_autocommit_snapshot =
                            expired_tx_id == 0 && snapshot_id == 0;

                        if !(expired_by_current_tx
                            || is_committed_expirer
                            || is_autocommit_expiry_visible_to_autocommit_snapshot)
                        {
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

    fn delete(
        &mut self,
        key: &Vec<u8>,
        transaction: &Transaction,
        _lsn: Lsn, // _lsn is unused in InMemoryKvStore but required by trait
        committed_ids: &HashSet<u64>, // Added committed_ids
    ) -> Result<bool, OxidbError> {
        if let Some(versions) = self.data.get_mut(key) {
            for version in versions.iter_mut().rev() {
                // Determine if this version is currently visible (similar to snapshot_id=0 GET logic)
                // For InMemoryKvStore, snapshot_id for delete operation is effectively 0 (read committed)
                let creator_is_committed =
                    committed_ids.contains(&version.created_tx_id) || version.created_tx_id == 0;
                let mut is_visible = false;
                if creator_is_committed {
                    if let Some(expired_tx_id_val) = version.expired_tx_id {
                        let expirer_is_committed =
                            committed_ids.contains(&expired_tx_id_val) || expired_tx_id_val == 0;
                        if !expirer_is_committed {
                            is_visible = true; // Creator committed, expirer not committed
                        }
                    } else {
                        is_visible = true; // Creator committed, not expired
                    }
                }

                // If the version is visible, this is the one to mark as expired by the current transaction.
                // Also, handle the case where the current transaction itself created the version (e.g. rollback of an insert).
                let is_own_uncommitted_write = version.created_tx_id == transaction.id.0
                    && !committed_ids.contains(&transaction.id.0);

                if is_visible || (is_own_uncommitted_write && version.expired_tx_id.is_none()) {
                    // If it's visible and not yet expired by this transaction or another committed one, mark it.
                    // Or if it's an uncommitted write by the same transaction that is now being deleted (e.g. rollback).
                    if version.expired_tx_id.is_none()
                        || !committed_ids.contains(&version.expired_tx_id.unwrap_or(0))
                        || version.expired_tx_id.unwrap_or(0) == transaction.id.0
                    {
                        version.expired_tx_id = Some(transaction.id.0);
                        return Ok(true); // Successfully marked a version as deleted
                    }
                }
            }
        }
        Ok(false) // Key not found or no visible version to delete
    }

    fn contains_key(
        &self,
        key: &Vec<u8>,
        snapshot_id: u64,
        committed_ids: &HashSet<u64>,
    ) -> Result<bool, OxidbError> {
        // Changed
        self.get(key, snapshot_id, committed_ids).map(|opt| opt.is_some())
    }

    fn log_wal_entry(&mut self, _entry: &WalEntry) -> Result<(), OxidbError> {
        // Changed
        Ok(())
    }

    fn gc(&mut self, low_water_mark: u64, committed_ids: &HashSet<u64>) -> Result<(), OxidbError> {
        // Changed
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

    fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, OxidbError> {
        // Changed
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

    fn get_schema(
        &self,
        schema_key: &Vec<u8>,
        snapshot_id: u64,
        committed_ids: &HashSet<u64>,
    ) -> Result<Option<crate::core::types::schema::Schema>, OxidbError> {
        match self.get(schema_key, snapshot_id, committed_ids)? {
            Some(bytes_ref) => {
                // Schema is assumed to be serialized directly (e.g., using serde_json)
                // not wrapped in DataType::Schema variant.
                match serde_json::from_slice(bytes_ref) {
                    Ok(schema) => Ok(Some(schema)),
                    Err(e) => Err(OxidbError::Deserialization(format!(
                        "Failed to deserialize Schema for key {:?}: {}",
                        String::from_utf8_lossy(schema_key),
                        e
                    ))),
                }
            }
            None => Ok(None),
        }
    }
}
