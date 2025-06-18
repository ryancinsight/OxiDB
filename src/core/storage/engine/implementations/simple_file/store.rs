use std::collections::HashMap;
use std::path::{Path, PathBuf};
// Required by QueryExecutor for the store it holds.

use crate::core::common::types::Lsn; // Added Lsn
use crate::core::common::OxidbError;
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
    pub fn new(path: impl AsRef<Path>) -> Result<Self, OxidbError> {
        // Changed
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
    pub fn persist(&self) -> Result<(), OxidbError> {
        // Changed
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
        if key == b"tx_delete_rollback_key".as_slice() || key == b"idx_del_key_tx_rollback".as_slice() {
            println!("[store.put] Called for key: {:?}, value_bytes_len: {}, tx_id: {}", String::from_utf8_lossy(&key), value.len(), transaction.id.0);
            println!("[store.put] Cache BEFORE for key {:?}: {:?}", String::from_utf8_lossy(&key), self.cache.get(&key));
        }

        let wal_entry = crate::core::storage::engine::wal::WalEntry::Put {
            lsn,
            transaction_id: transaction.id.0, // Ensure .0 is used for u64 WalEntry field
            key: key.clone(),
            value: value.clone(),
        };
        self.wal_writer.log_entry(&wal_entry)?; // Reverted to log_entry

        let versions = self.cache.entry(key.clone()).or_default(); // ensure key is cloned for cache entry
        for version in versions.iter_mut().rev() {
            if version.expired_tx_id.is_none() {
                version.expired_tx_id = Some(transaction.id.0); // Use .0 for u64 VersionedValue field
                break;
            }
        }
        let new_version =
            VersionedValue { value, created_tx_id: transaction.id.0, expired_tx_id: None }; // Use .0
        versions.push(new_version);

        if key == b"tx_delete_rollback_key".as_slice() || key == b"idx_del_key_tx_rollback".as_slice() {
            // 'key' was moved into cache.entry(key) if it wasn't already there.
            // For logging, we need to use a key that's still available or re-clone if necessary
            // However, self.cache.get() will use the original key if it's still valid.
            // The original 'key' variable is fine to use with String::from_utf8_lossy if borrowed.
            println!("[store.put] Cache AFTER for key {:?}: {:?}", String::from_utf8_lossy(&key), self.cache.get(&key));
        }
        Ok(())
    }

    fn get(
        &self,
        key: &Vec<u8>,
        snapshot_id: u64,
        committed_ids: &HashSet<u64>,
    ) -> Result<Option<Vec<u8>>, OxidbError> {
        if snapshot_id == 0 {
            // Non-transactional read: reflects the state after full recovery.
            // The cache, after load_from_disk and replay_wal_into_cache, should
            // contain the correct, visible versions. We just need the latest non-expired one.
            if let Some(versions) = self.cache.get(key) {
                if key == b"tx_delete_rollback_key".as_slice() || key == b"idx_del_key_tx_rollback".as_slice() {
                    println!("[store.get snapshot_id=0] Key: {:?}, Versions: {:?}", String::from_utf8_lossy(key), versions);
                }
                for version in versions.iter().rev() {
                    if version.expired_tx_id.is_none() {
                        if key == b"tx_delete_rollback_key".as_slice() || key == b"idx_del_key_tx_rollback".as_slice() {
                            println!("[store.get snapshot_id=0] Key: {:?}, Found visible version: {:?}", String::from_utf8_lossy(key), version);
                        }
                        return Ok(Some(version.value.clone()));
                    }
                }
            }
            if key == b"tx_delete_rollback_key".as_slice() || key == b"idx_del_key_tx_rollback".as_slice() {
                 println!("[store.get snapshot_id=0] Key: {:?}, No visible version found or key not in cache.", String::from_utf8_lossy(key));
            }
            return Ok(None);
        } else {
            // Transactional read (snapshot_id != 0) - Restoring original detailed MVCC logic
            if let Some(versions) = self.cache.get(key) {
                for version in versions.iter().rev() {
                    // Determine if the version's creator is visible in the current snapshot
                    let creator_is_visible =
                        (version.created_tx_id == snapshot_id) || // Created by the current transaction (snapshot_id is non-zero here)
                        committed_ids.contains(&version.created_tx_id) || // Created by a committed transaction visible in this snapshot
                        (version.created_tx_id == 0); // Baseline data (tx_id 0), always a candidate for visibility for active transactions

                    if creator_is_visible {
                        // If the creator is visible, check if the version is expired in the current snapshot
                        if let Some(expired_tx_id_val) = version.expired_tx_id {
                            let expirer_is_visible =
                                (expired_tx_id_val == snapshot_id) || // Expired by the current transaction
                                committed_ids.contains(&expired_tx_id_val) || // Expired by a committed transaction visible in this snapshot
                                (expired_tx_id_val == 0); // Baseline expiry (tx_id 0), makes it expired for all active transactions

                            if !expirer_is_visible {
                                // Expiration is NOT visible, so this version IS visible
                                return Ok(Some(version.value.clone()));
                            }
                            // If expiration IS visible, this version is not the one; continue to older version.
                        } else {
                            // No expiration_tx_id means the version is visible
                            return Ok(Some(version.value.clone()));
                        }
                    }
                }
            }
            Ok(None)
        }
    }

    fn delete(
        &mut self,
        key: &Vec<u8>,
        transaction: &Transaction,
        lsn: Lsn,
    ) -> Result<bool, OxidbError> {
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
                        || version.expired_tx_id.unwrap() > transaction.id.0)
                // Use .0 for comparison
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
    ) -> Result<bool, OxidbError> {
        // Changed
        Ok(false) // Placeholder as in original
    }

    fn log_wal_entry(
        &mut self,
        entry: &super::super::super::wal::WalEntry,
    ) -> Result<(), OxidbError> {
        // Changed
        // Adjusted path
        self.wal_writer.log_entry(entry) // Reverted to log_entry, no separate flush needed as log_entry syncs
    }

    fn gc(
        &mut self,
        _low_water_mark: u64,
        _committed_ids: &HashSet<u64>,
    ) -> Result<(), OxidbError> {
        // Changed
        Ok(()) // Placeholder as in original
    }

    fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, OxidbError>
    // Changed
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
