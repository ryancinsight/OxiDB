use std::collections::HashMap;
use std::path::{Path, PathBuf};
// Required by QueryExecutor for the store it holds.

use crate::core::common::OxidbError;
use crate::core::common::types::Lsn; // Added Lsn
use crate::core::storage::engine::traits::{KeyValueStore, VersionedValue};
use crate::core::storage::engine::wal::WalWriter;
use crate::core::transaction::Transaction;
use std::collections::HashSet;

use super::persistence; // For load_data_from_disk, save_data_to_disk (will be pub(super))
use super::recovery; // For replay_wal_into_cache (will be pub(super))

#[derive(Debug)]
pub struct SimpleFileKvStore {
    pub(super) file_path: PathBuf, // Made pub(super) for access from persistence/recovery
    pub(super) cache: HashMap<Vec<u8>, Vec<VersionedValue<Vec<u8>>>>, // pub(super)
    pub(super) wal_writer: WalWriter, // pub(super)
}

impl SimpleFileKvStore {
    /// Creates a new `SimpleFileKvStore` instance.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, OxidbError> { // Changed
        let path_buf = path.as_ref().to_path_buf();

        let mut wal_file_path = path_buf.clone();
        let original_extension = wal_file_path.extension().map(|s| s.to_os_string());
        if let Some(ext) = original_extension {
            let mut new_ext = ext;
            new_ext.push(".wal");
            wal_file_path.set_extension(new_ext);
        } else {
            wal_file_path.set_extension("wal");
        }

        // WalWriter::new itself isn't fallible in the original code.
        // If it needs to create files or directories that can fail, it should return Result.
        // For now, assuming it works as originally designed.
        // Pass the main db path_buf to WalWriter, let WalWriter derive its path.
        let wal_writer = WalWriter::new(&path_buf);
        let mut cache = HashMap::new();

        // load_data_from_disk and replay_wal_into_cache still need the explicitly derived wal_file_path
        // because they operate before the store's wal_writer might have created its file.
        persistence::load_data_from_disk(&path_buf, &wal_file_path, &mut cache)?;
        recovery::replay_wal_into_cache(&mut cache, &wal_file_path)?;

        Ok(Self { file_path: path_buf, cache, wal_writer })
    }

    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    /// Persists the current state of the cache to disk.
    /// This is equivalent to the old `save_to_disk` method.
    pub fn persist(&self) -> Result<(), OxidbError> { // Changed
        persistence::save_data_to_disk(&self.file_path, &self.cache)
    }

    #[cfg(test)]
    pub(crate) fn get_cache_for_test(&self) -> &HashMap<Vec<u8>, Vec<VersionedValue<Vec<u8>>>> {
        &self.cache
    }

    #[cfg(test)]
    pub(crate) fn get_cache_entry_for_test(
        &self,
        key: &Vec<u8>,
    ) -> Option<&Vec<VersionedValue<Vec<u8>>>> {
        self.cache.get(key)
    }
}

impl KeyValueStore<Vec<u8>, Vec<u8>> for SimpleFileKvStore {
    fn put(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        transaction: &Transaction,
        lsn: Lsn, // Added lsn parameter
    ) -> Result<(), OxidbError> {
        let wal_entry = crate::core::storage::engine::wal::WalEntry::Put {
            lsn,
            transaction_id: transaction.id.0, // Ensure .0 is used for u64 WalEntry field
            key: key.clone(),
            value: value.clone(),
        };
        self.wal_writer.log_entry(&wal_entry)?; // Reverted to log_entry

        let versions = self.cache.entry(key).or_default();
        for version in versions.iter_mut().rev() {
            if version.expired_tx_id.is_none() {
                version.expired_tx_id = Some(transaction.id.0); // Use .0 for u64 VersionedValue field
                break;
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
        if let Some(versions) = self.cache.get(key) {
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

    fn delete(&mut self, key: &Vec<u8>, transaction: &Transaction, lsn: Lsn) -> Result<bool, OxidbError> {
        let wal_entry = crate::core::storage::engine::wal::WalEntry::Delete {
            lsn,
            transaction_id: transaction.id.0, // Use .0 for u64 field
            key: key.clone(),
        };
        self.wal_writer.log_entry(&wal_entry)?; // Reverted to log_entry

        if let Some(versions) = self.cache.get_mut(key) {
            for version in versions.iter_mut().rev() {
                if version.created_tx_id <= transaction.id.0 // Use .0 for comparison
                    && (version.expired_tx_id.is_none()
                        || version.expired_tx_id.unwrap() > transaction.id.0) // Use .0 for comparison
                {
                    if version.expired_tx_id.is_none() {
                        version.expired_tx_id = Some(transaction.id.0); // Use .0 for assignment
                        return Ok(true);
                    } else {
                        return Ok(false); // Already expired
                    }
                }
            }
        }
        Ok(false)
    }

    fn contains_key(
        &self,
        _key: &Vec<u8>,
        _snapshot_id: u64,
        _committed_ids: &HashSet<u64>,
    ) -> Result<bool, OxidbError> { // Changed
        Ok(false) // Placeholder as in original
    }

    fn log_wal_entry(&mut self, entry: &super::super::super::wal::WalEntry) -> Result<(), OxidbError> { // Changed
        // Adjusted path
        self.wal_writer.log_entry(entry) // Reverted to log_entry, no separate flush needed as log_entry syncs
    }

    fn gc(&mut self, _low_water_mark: u64, _committed_ids: &HashSet<u64>) -> Result<(), OxidbError> { // Changed
        Ok(()) // Placeholder as in original
    }

    fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, OxidbError> // Changed
    where
        Vec<u8>: Clone,
        Vec<u8>: Clone,
    {
        unimplemented!("Scan operation is not yet implemented for SimpleFileKvStore");
    }
}

impl Drop for SimpleFileKvStore {
    fn drop(&mut self) {
        // The save_data_to_disk function (now in persistence module) should handle WAL clearing.
        if let Err(e) = persistence::save_data_to_disk(&self.file_path, &self.cache) {
            eprintln!("Error saving data to disk during drop: {}", e);
        }
    }
}
