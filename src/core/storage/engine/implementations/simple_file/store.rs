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
        // wal_file_path is derived above and used for recovery and loading.
        // The SimpleFileKvStore's own WalWriter will also derive an identical path from path_buf.
        let wal_writer = WalWriter::new(&path_buf); // This WalWriter is from core::storage::engine::wal
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
        eprintln!(
            "[SimpleFileKvStore::put] Method entered for key: {:?}",
            String::from_utf8_lossy(&key)
        ); // ADDED THIS LINE
        if key == b"tx_delete_rollback_key".as_slice()
            || key == b"idx_del_key_tx_rollback".as_slice()
        {
            println!(
                "[store.put] Called for key: {:?}, value_bytes_len: {}, tx_id: {}",
                String::from_utf8_lossy(&key),
                value.len(),
                transaction.id.0
            );
            println!(
                "[store.put] Cache BEFORE for key {:?}: {:?}",
                String::from_utf8_lossy(&key),
                self.cache.get(&key)
            );
        }

        let wal_entry = crate::core::storage::engine::wal::WalEntry::Put {
            lsn,
            transaction_id: transaction.id.0, // Ensure .0 is used for u64 WalEntry field
            key: key.clone(),
            value: value.clone(),
        };
        self.wal_writer.log_entry(&wal_entry)?; // Reverted to log_entry

        let versions = self.cache.entry(key.clone()).or_default(); // ensure key is cloned for cache entry
                                                                   // Correctly iterate and update existing versions if necessary, then add new one.
                                                                   // This simplified logic just adds; proper MVCC put would mark previous version of this tx expired.
                                                                   // For the purpose of this store, let's assume QueryExecutor handles versioning logic before calling put,
                                                                   // or that put is for new keys / overwriting is fine for some interpretation.
                                                                   // However, typical MVCC store.put would handle version chain updates.
                                                                   // The provided code snippet for put seems to be an attempt at this.
                                                                   // Let's refine it based on typical MVCC: mark latest version by this tx as expired, then add new.
                                                                   // Or, if it's a new value for a key, ensure old values by *other* txs are handled by QueryExecutor's get logic.
                                                                   // The current test failure is about WAL count, so focusing on removing any auto-commit WAL logic from here.

        // Preserving the existing MVCC cache logic from the file, assuming it's intended:
        for version in versions.iter_mut().rev() {
            if version.created_tx_id == transaction.id.0 && version.expired_tx_id.is_none() {
                // Found unexpired version from same tx
                version.expired_tx_id = Some(transaction.id.0); // Expire it
                break;
            }
        }
        // Add the new version
        let new_version =
            VersionedValue { value, created_tx_id: transaction.id.0, expired_tx_id: None };
        versions.push(new_version);

        // Removed the TransactionCommit logging for auto-commit from here.
        // That should be handled by QueryExecutor/TransactionManager.

        if key == b"tx_delete_rollback_key".as_slice()
            || key == b"idx_del_key_tx_rollback".as_slice()
        {
            println!(
                "[store.put] Cache AFTER for key {:?}: {:?}",
                String::from_utf8_lossy(&key),
                self.cache.get(&key)
            );
        }
        Ok(())
    }

    fn get(
        &self,
        key: &Vec<u8>,
        snapshot_id: u64,
        committed_ids: &HashSet<u64>,
    ) -> Result<Option<Vec<u8>>, OxidbError> {
        eprintln!(
            "[SFKvStore::get] Attempting to get key: '{}', snapshot_id: {}",
            String::from_utf8_lossy(key),
            snapshot_id
        );

        if snapshot_id == 0 {
            // Non-transactional read (snapshot_id 0): Read latest committed and not expired by a committed transaction.
            if let Some(versions) = self.cache.get(key) {
                eprintln!(
                    "[SFKvStore::get] Key: '{}', snapshot_id: 0. Cache versions: {:?}",
                    String::from_utf8_lossy(key),
                    versions
                );
                for version in versions.iter().rev() {
                    // Check if creator is committed (or primordial tx 0)
                    let creator_is_committed = committed_ids.contains(&version.created_tx_id)
                        || version.created_tx_id == 0;

                    if creator_is_committed {
                        if let Some(expired_tx_id_val) = version.expired_tx_id {
                            // If expired, the expirer must NOT be committed for this version to be visible
                            let expirer_is_committed = committed_ids.contains(&expired_tx_id_val)
                                || expired_tx_id_val == 0;
                            if !expirer_is_committed {
                                eprintln!("[SFKvStore::get] Key: '{}', snapshot_id: 0. Chosen version (creator committed, expirer not committed): {{ value_len: {}, created_tx: {}, expired_tx: {:?} }}", String::from_utf8_lossy(key), version.value.len(), version.created_tx_id, version.expired_tx_id);
                                return Ok(Some(version.value.clone()));
                            }
                        } else {
                            // Not expired, and creator is committed
                            eprintln!("[SFKvStore::get] Key: '{}', snapshot_id: 0. Chosen version (creator committed, not expired): {{ value_len: {}, created_tx: {}, expired_tx: None }}", String::from_utf8_lossy(key), version.value.len(), version.created_tx_id);
                            return Ok(Some(version.value.clone()));
                        }
                    }
                }
                eprintln!("[SFKvStore::get] Key: '{}', snapshot_id: 0. No committed and visible version found in cache.", String::from_utf8_lossy(key));
                Ok(None::<Vec<u8>>)
            } else {
                eprintln!(
                    "[SFKvStore::get] Key: '{}', snapshot_id: 0. Key not found in cache.",
                    String::from_utf8_lossy(key)
                );
                Ok(None::<Vec<u8>>)
            }
        } else {
            // Transactional read (snapshot_id != 0)
            if let Some(versions) = self.cache.get(key) {
                eprintln!(
                    "[SFKvStore::get] Key: '{}', snapshot_id: {}. Cache versions: {:?}",
                    String::from_utf8_lossy(key),
                    snapshot_id,
                    versions
                );
                for version in versions.iter().rev() {
                    let creator_is_visible = (version.created_tx_id == snapshot_id) || // Own transaction's write
                        committed_ids.contains(&version.created_tx_id) || // Other committed transaction
                        (version.created_tx_id == 0); // Primordial data

                    if creator_is_visible {
                        if let Some(expired_tx_id_val) = version.expired_tx_id {
                            let expirer_is_visible = (expired_tx_id_val == snapshot_id) || // Expired by own transaction
                                committed_ids.contains(&expired_tx_id_val) || // Expired by other committed transaction
                                (expired_tx_id_val == 0); // Expired by primordial (unlikely for user data)

                            if !expirer_is_visible {
                                eprintln!("[SFKvStore::get] Key: '{}', snapshot_id: {}. Chosen version (transactional): {{ value_len: {}, created_tx: {}, expired_tx: {:?} }}", String::from_utf8_lossy(key), snapshot_id, version.value.len(), version.created_tx_id, version.expired_tx_id);
                                return Ok(Some(version.value.clone()));
                            }
                        } else {
                            // Not expired, and creator is visible
                            eprintln!("[SFKvStore::get] Key: '{}', snapshot_id: {}. Chosen version (transactional, not expired): {{ value_len: {}, created_tx: {}, expired_tx: None }}", String::from_utf8_lossy(key), snapshot_id, version.value.len(), version.created_tx_id);
                            return Ok(Some(version.value.clone()));
                        }
                    }
                }
                eprintln!("[SFKvStore::get] Key: '{}', snapshot_id: {}. No visible version found for this transaction.", String::from_utf8_lossy(key), snapshot_id);
                Ok(None::<Vec<u8>>)
            } else {
                eprintln!(
                    "[SFKvStore::get] Key: '{}', snapshot_id: {}. Key not found in cache.",
                    String::from_utf8_lossy(key),
                    snapshot_id
                );
                Ok(None::<Vec<u8>>)
            }
        }
        // Note: The final eprintln! that was here is removed as each branch now returns directly.
        // If it was intended to log every exit path, it would need to be part of each return Ok(...).
    }

    fn delete(
        &mut self,
        key: &Vec<u8>,
        transaction: &Transaction,
        lsn: Lsn,
        committed_ids: &HashSet<u64>, // Added committed_ids
    ) -> Result<bool, OxidbError> {
        eprintln!("[SFKvStore::delete] Attempting to delete key: '{}', by transaction_id: {}, with committed_ids: {:?}", String::from_utf8_lossy(key), transaction.id.0, committed_ids);

        let wal_entry = crate::core::storage::engine::wal::WalEntry::Delete {
            lsn,
            transaction_id: transaction.id.0,
            key: key.clone(),
        };
        self.wal_writer.log_entry(&wal_entry)?;

        let mut deleted_a_version = false;
        if let Some(versions) = self.cache.get_mut(key) {
            eprintln!(
                "[SFKvStore::delete] Key: '{}', tx_id: {}. Cache versions BEFORE delete op: {:?}",
                String::from_utf8_lossy(key),
                transaction.id.0,
                versions
            );
            for version in versions.iter_mut().rev() {
                let creator_is_committed_or_own_tx = (version.created_tx_id == transaction.id.0)
                    || committed_ids.contains(&version.created_tx_id)
                    || version.created_tx_id == 0;

                if creator_is_committed_or_own_tx && version.expired_tx_id.is_none() {
                    // This version is visible to the current transaction and not yet expired.
                    // Or it's an uncommitted write by the current transaction.
                    eprintln!("[SFKvStore::delete] Key: '{}', tx_id: {}. Found version to mark expired: {{ value_len: {}, created_tx: {}, current_expired_tx: None }}. Marking expired with tx_id: {}",
                        String::from_utf8_lossy(key), transaction.id.0, version.value.len(), version.created_tx_id, transaction.id.0);
                    version.expired_tx_id = Some(transaction.id.0);
                    deleted_a_version = true;
                    break;
                }
            }
            if deleted_a_version {
                eprintln!("[SFKvStore::delete] Key: '{}', tx_id: {}. Cache versions AFTER delete op: {:?}", String::from_utf8_lossy(key), transaction.id.0, versions);
            } else {
                eprintln!("[SFKvStore::delete] Key: '{}', tx_id: {}. No effectively live version found to mark expired by this transaction.", String::from_utf8_lossy(key), transaction.id.0);
            }
        } else {
            eprintln!(
                "[SFKvStore::delete] Key: '{}', tx_id: {}. Key not found in cache.",
                String::from_utf8_lossy(key),
                transaction.id.0
            );
        }

        // Removed the TransactionCommit logging for auto-commit from here.

        eprintln!("[SFKvStore::delete] Key: '{}', tx_id: {}. Delete operation outcome (deleted_a_version): {}", String::from_utf8_lossy(key), transaction.id.0, deleted_a_version);
        Ok(deleted_a_version)
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
    where
        Vec<u8>: Clone, // K: Clone
        Vec<u8>: Clone, // V: Clone
    {
        let mut results = Vec::new();
        for (key, versions) in self.cache.iter() {
            // Find the latest, non-expired version for this key.
            // This mimics the logic in `get` for snapshot_id = 0 (non-transactional read).
            for version in versions.iter().rev() {
                if version.expired_tx_id.is_none() {
                    // This is the latest live version of the key.
                    results.push((key.clone(), version.value.clone()));
                    break; // Move to the next key
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
            Some(bytes) => {
                // Schema is assumed to be serialized directly (e.g., using serde_json)
                // not wrapped in DataType::Schema variant.
                match serde_json::from_slice(&bytes) {
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

impl Drop for SimpleFileKvStore {
    fn drop(&mut self) {
        // The save_data_to_disk function (now in persistence module) should handle WAL clearing.
        if let Err(e) = persistence::save_data_to_disk(&self.file_path, &self.cache) {
            eprintln!("Error saving data to disk during drop: {}", e);
        }
    }
}
