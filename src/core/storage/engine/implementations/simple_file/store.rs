use std::collections::HashMap;
use std::path::{Path, PathBuf};
 // Required by QueryExecutor for the store it holds.

use crate::core::common::error::DbError;
use crate::core::storage::engine::traits::{KeyValueStore, VersionedValue};
use crate::core::storage::engine::wal::WalWriter; // WalWriter will be part of SimpleFileKvStore struct
use crate::core::transaction::Transaction;
use std::collections::HashSet; // Required for KeyValueStore trait methods

use super::persistence; // For load_data_from_disk, save_data_to_disk (will be pub(super))
use super::recovery;   // For replay_wal_into_cache (will be pub(super))

#[derive(Debug)]
pub struct SimpleFileKvStore {
    pub(super) file_path: PathBuf, // Made pub(super) for access from persistence/recovery
    pub(super) cache: HashMap<Vec<u8>, Vec<VersionedValue<Vec<u8>>>>, // pub(super)
    pub(super) wal_writer: WalWriter, // pub(super)
}

impl SimpleFileKvStore {
    /// Creates a new `SimpleFileKvStore` instance.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, DbError> {
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
        let wal_writer = WalWriter::new(&path_buf);
        let mut cache = HashMap::new();

        persistence::load_data_from_disk(&path_buf, &wal_file_path, &mut cache)?;
        recovery::replay_wal_into_cache(&mut cache, &wal_file_path)?;

        Ok(Self {
            file_path: path_buf,
            cache,
            wal_writer,
        })
    }

    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    /// Persists the current state of the cache to disk.
    /// This is equivalent to the old `save_to_disk` method.
    pub fn persist(&self) -> Result<(), DbError> {
        persistence::save_data_to_disk(&self.file_path, &self.cache)
    }

    #[cfg(test)]
    pub(crate) fn get_cache_for_test(&self) -> &HashMap<Vec<u8>, Vec<VersionedValue<Vec<u8>>>> {
        &self.cache
    }

    #[cfg(test)]
    pub(crate) fn get_cache_entry_for_test(&self, key: &Vec<u8>) -> Option<&Vec<VersionedValue<Vec<u8>>>> {
        self.cache.get(key)
    }

}

impl KeyValueStore<Vec<u8>, Vec<u8>> for SimpleFileKvStore {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>, transaction: &Transaction) -> Result<(), DbError> {
        let wal_entry = super::super::super::wal::WalEntry::Put { // Adjusted path to WalEntry enum
            transaction_id: transaction.id,
            key: key.clone(),
            value: value.clone(),
        };
        self.wal_writer.log_entry(&wal_entry)?;

        let versions = self.cache.entry(key).or_default();
        for version in versions.iter_mut().rev() {
            if version.expired_tx_id.is_none() {
                version.expired_tx_id = Some(transaction.id);
                break;
            }
        }
        let new_version = VersionedValue {
            value,
            created_tx_id: transaction.id,
            expired_tx_id: None,
        };
        versions.push(new_version);
        Ok(())
    }

    fn get(&self, key: &Vec<u8>, snapshot_id: u64, committed_ids: &HashSet<u64>) -> Result<Option<Vec<u8>>, DbError> {
        if let Some(versions) = self.cache.get(key) {
            for version in versions.iter().rev() {
                let is_own_uncommitted_version = version.created_tx_id == snapshot_id;
                let is_committed_version = version.created_tx_id <= snapshot_id && committed_ids.contains(&version.created_tx_id);

                if is_own_uncommitted_version || is_committed_version {
                    match version.expired_tx_id {
                        None => return Ok(Some(version.value.clone())),
                        Some(expired_id) => {
                            let is_own_uncommitted_delete = expired_id == snapshot_id;
                            let is_committed_delete = expired_id <= snapshot_id && committed_ids.contains(&expired_id);
                            if !(is_own_uncommitted_delete || is_committed_delete) {
                                return Ok(Some(version.value.clone()));
                            }
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    fn delete(&mut self, key: &Vec<u8>, transaction: &Transaction) -> Result<bool, DbError> {
        let wal_entry = super::super::super::wal::WalEntry::Delete { // Adjusted path
            transaction_id: transaction.id,
            key: key.clone(),
        };
        self.wal_writer.log_entry(&wal_entry)?;

        if let Some(versions) = self.cache.get_mut(key) {
            for version in versions.iter_mut().rev() {
                if version.created_tx_id <= transaction.id &&
                   (version.expired_tx_id.is_none() || version.expired_tx_id.unwrap() > transaction.id) {
                    if version.expired_tx_id.is_none() {
                        version.expired_tx_id = Some(transaction.id);
                        return Ok(true);
                    } else {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(false)
    }

    fn contains_key(&self, _key: &Vec<u8>, _snapshot_id: u64, _committed_ids: &HashSet<u64>) -> Result<bool, DbError> {
        Ok(false) // Placeholder as in original
    }

    fn log_wal_entry(&mut self, entry: &super::super::super::wal::WalEntry) -> Result<(), DbError> { // Adjusted path
        self.wal_writer.log_entry(entry)
    }

    fn gc(&mut self, _low_water_mark: u64, _committed_ids: &HashSet<u64>) -> Result<(), DbError> {
        Ok(()) // Placeholder as in original
    }

    fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DbError>
        where Vec<u8>: Clone, Vec<u8>: Clone {
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
