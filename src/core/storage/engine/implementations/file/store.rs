use std::collections::HashMap;
use std::path::{Path, PathBuf};
// Required by QueryExecutor for the store it holds.

use crate::core::common::types::Lsn; // Added Lsn
use crate::core::common::OxidbError;
use crate::core::storage::engine::traits::{KeyValueStore, VersionedValue};
use crate::core::storage::engine::wal::WalWriter;
use crate::core::transaction::Transaction;
use std::collections::HashSet;

use super::persistence; // For load_data_from_disk, save_data_to_disk
use super::recovery; // For replay_wal_into_cache

#[derive(Debug)]
pub struct FileKvStore {
	pub(super) file_path: PathBuf,
	pub(super) cache: HashMap<Vec<u8>, Vec<VersionedValue<Vec<u8>>>>,
	pub(super) wal_writer: WalWriter,
}

impl FileKvStore {
	/// Creates a new `FileKvStore` instance.
	pub fn new(path: impl AsRef<Path>) -> Result<Self, OxidbError> {
		let path_buf = path.as_ref().to_path_buf();

		let mut wal_file_path = path_buf.clone();
		let original_extension = wal_file_path.extension().map(std::ffi::OsStr::to_os_string);
		if let Some(ext) = original_extension {
			let mut new_ext = ext;
			new_ext.push(".wal");
			wal_file_path.set_extension(new_ext);
		} else {
			wal_file_path.set_extension("wal");
		}

		let wal_writer = WalWriter::new(&path_buf);
		let mut cache = HashMap::new();

		persistence::load_data_from_disk(&path_buf, &wal_file_path, &mut cache)?;
		recovery::replay_wal_into_cache(&mut cache, &wal_file_path)?;

		Ok(Self { file_path: path_buf, cache, wal_writer })
	}

	#[must_use]
	pub fn file_path(&self) -> &Path {
		&self.file_path
	}

	/// Persists the current state of the cache to disk.
	pub fn persist(&self) -> Result<(), OxidbError> {
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

impl KeyValueStore<Vec<u8>, Vec<u8>> for FileKvStore {
	fn put(
		&mut self,
		key: Vec<u8>,
		value: Vec<u8>,
		transaction: &Transaction,
		lsn: Lsn,
	) -> Result<(), OxidbError> {
		if cfg!(debug_assertions) {
			eprintln!(
				"[FileKvStore::put] Method entered for key: {:?}",
				String::from_utf8_lossy(&key)
			);
		}

		let wal_entry = crate::core::storage::engine::wal::WalEntry::Put {
			lsn,
			transaction_id: transaction.id.0,
			key: key.clone(),
			value: value.clone(),
		};
		self.wal_writer.log_entry(&wal_entry)?;

		let versions = self.cache.entry(key.clone()).or_default();
		for version in versions.iter_mut().rev() {
			if version.created_tx_id == transaction.id.0 && version.expired_tx_id.is_none() {
				version.expired_tx_id = Some(transaction.id.0);
				break;
			}
		}
		let new_version = VersionedValue { value, created_tx_id: transaction.id.0, expired_tx_id: None };
		versions.push(new_version);
		Ok(())
	}

	fn get(
		&self,
		key: &Vec<u8>,
		snapshot_id: u64,
		committed_ids: &HashSet<u64>,
	) -> Result<Option<Vec<u8>>, OxidbError> {
		if cfg!(debug_assertions) {
			eprintln!(
				"[FileKvStore::get] Attempting to get key: '{}', snapshot_id: {}",
				String::from_utf8_lossy(key),
				snapshot_id
			);
		}

		if snapshot_id == 0 {
			if let Some(versions) = self.cache.get(key) {
				for version in versions.iter().rev() {
					let creator_is_committed = committed_ids.contains(&version.created_tx_id) || version.created_tx_id == 0;
					if creator_is_committed {
						if let Some(expired_tx_id_val) = version.expired_tx_id {
							let expirer_is_committed = committed_ids.contains(&expired_tx_id_val) || expired_tx_id_val == 0;
							if !expirer_is_committed {
								return Ok(Some(version.value.clone()));
							}
						} else {
							return Ok(Some(version.value.clone()));
						}
					}
				}
				Ok(None::<Vec<u8>>)
			} else {
				Ok(None::<Vec<u8>>)
			}
		} else {
			if let Some(versions) = self.cache.get(key) {
				for version in versions.iter().rev() {
					let creator_is_visible = (version.created_tx_id == snapshot_id)
						|| committed_ids.contains(&version.created_tx_id)
						|| (version.created_tx_id == 0);
					if creator_is_visible {
						if let Some(expired_tx_id_val) = version.expired_tx_id {
							let expirer_is_visible = (expired_tx_id_val == snapshot_id)
								|| committed_ids.contains(&expired_tx_id_val)
								|| (expired_tx_id_val == 0);
							if !expirer_is_visible {
								return Ok(Some(version.value.clone()));
							}
						} else {
							return Ok(Some(version.value.clone()));
						}
					}
				}
				Ok(None::<Vec<u8>>)
			} else {
				Ok(None::<Vec<u8>>)
			}
		}
	}

	fn delete(
		&mut self,
		key: &Vec<u8>,
		transaction: &Transaction,
		lsn: Lsn,
		committed_ids: &HashSet<u64>,
	) -> Result<bool, OxidbError> {
		if cfg!(debug_assertions) {
			eprintln!(
				"[FileKvStore::delete] Attempting to delete key: '{}'",
				String::from_utf8_lossy(key)
			);
		}

		let wal_entry = crate::core::storage::engine::wal::WalEntry::Delete { lsn, transaction_id: transaction.id.0, key: key.clone() };
		self.wal_writer.log_entry(&wal_entry)?;

		let mut deleted_a_version = false;
		if let Some(versions) = self.cache.get_mut(key) {
			for version in versions.iter_mut().rev() {
				let creator_is_committed_or_own_tx = (version.created_tx_id == transaction.id.0)
					|| committed_ids.contains(&version.created_tx_id)
					|| version.created_tx_id == 0;
				if creator_is_committed_or_own_tx && version.expired_tx_id.is_none() {
					version.expired_tx_id = Some(transaction.id.0);
					deleted_a_version = true;
					break;
				}
			}
		}
		Ok(deleted_a_version)
	}

	fn contains_key(
		&self,
		key: &Vec<u8>,
		snapshot_id: u64,
		committed_ids: &HashSet<u64>,
	) -> Result<bool, OxidbError> {
		self.get(key, snapshot_id, committed_ids).map(|opt| opt.is_some())
	}

	fn log_wal_entry(
		&mut self,
		entry: &super::super::super::wal::WalEntry,
	) -> Result<(), OxidbError> {
		self.wal_writer.log_entry(entry)
	}

	fn gc(
		&mut self,
		low_water_mark: u64,
		committed_ids: &HashSet<u64>,
	) -> Result<(), OxidbError> {
		let mut keys_to_remove: Vec<Vec<u8>> = Vec::new();
		for (key, versions) in self.cache.iter_mut() {
			versions.retain(|v| match v.expired_tx_id {
				Some(expirer_tx) => {
					let creator_committed = committed_ids.contains(&v.created_tx_id) || v.created_tx_id == 0;
					let expirer_committed = committed_ids.contains(&expirer_tx) || expirer_tx == 0;
					!(creator_committed && expirer_committed && expirer_tx <= low_water_mark)
				}
				None => true,
			});
			if versions.is_empty() {
				keys_to_remove.push(key.clone());
			}
		}
		for key in keys_to_remove {
			self.cache.remove(&key);
		}
		Ok(())
	}

	fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, OxidbError>
	where
		Vec<u8>: Clone,
		Vec<u8>: Clone,
	{
		let mut results = Vec::new();
		for (key, versions) in &self.cache {
			for version in versions.iter().rev() {
				if version.expired_tx_id.is_none() {
					results.push((key.clone(), version.value.clone()));
					break;
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
			Some(bytes_ref) => match serde_json::from_slice(&bytes_ref) {
				Ok(schema) => Ok(Some(schema)),
				Err(e) => Err(OxidbError::Deserialization(format!(
					"Failed to deserialize Schema for key {:?}: {}",
					String::from_utf8_lossy(schema_key),
					e
				))),
			},
			None => Ok(None),
		}
	}
}

impl Drop for FileKvStore {
	fn drop(&mut self) {
		if let Err(e) = persistence::save_data_to_disk(&self.file_path, &self.cache) {
			eprintln!("Error saving data to disk during drop: {e}");
		}
	}
}