use crate::core::common::traits::{DataDeserializer, DataSerializer};
use crate::core::common::OxidbError;
use crate::core::storage::engine::traits::VersionedValue;
use std::collections::HashMap;
use std::fs::{rename, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, ErrorKind, Write};
use std::path::{Path, PathBuf};

fn derive_wal_path(db_path: &Path) -> PathBuf {
	let mut wal_path = db_path.to_path_buf();
	let original_extension = wal_path.extension().map(std::ffi::OsStr::to_os_string);
	if let Some(ext) = original_extension {
		let mut new_ext = ext;
		new_ext.push(".wal");
		wal_path.set_extension(new_ext);
	} else {
		wal_path.set_extension("wal");
	}
	wal_path
}

pub(super) fn load_data_from_disk(
	file_path: &Path,
	_wal_path: &Path,
	cache: &mut HashMap<Vec<u8>, Vec<VersionedValue<Vec<u8>>>>,
) -> Result<(), OxidbError> {
	let temp_file_path = file_path.with_extension("tmp");

	if temp_file_path.exists() {
		match read_data_into_cache_internal(cache, &temp_file_path) {
			Ok(()) => {
				if let Err(e) = rename(&temp_file_path, file_path) {
					return Err(OxidbError::Storage(format!(
						"Successfully loaded from temporary file {} but failed to rename it to {}: {}",
						temp_file_path.display(),
						file_path.display(),
						e
					)));
				}
				return Ok(());
			}
			Err(load_err) => {
				eprintln!(
					"Failed to load from temporary file {}: {}. Attempting to delete it.",
					temp_file_path.display(),
					load_err
				);
				if let Err(remove_err) = std::fs::remove_file(&temp_file_path) {
					return Err(OxidbError::Storage(format!(
						"Corrupted temporary file {} could not be loaded ({}) or deleted ({}). Manual intervention may be required.",
						temp_file_path.display(),
						load_err,
						remove_err
					)));
				}
			}
		}
	}
	read_data_into_cache_internal(cache, file_path)
}

fn read_data_into_cache_internal(
	cache: &mut HashMap<Vec<u8>, Vec<VersionedValue<Vec<u8>>>>,
	file_to_load: &Path,
) -> Result<(), OxidbError> {
	cache.clear();
	let file = match File::open(file_to_load) {
		Ok(f) => f,
		Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
		Err(e) => return Err(OxidbError::Io(e)),
	};

	let mut reader = BufReader::new(file);
	loop {
		let buffer = reader.fill_buf().map_err(OxidbError::Io)?;
		if buffer.is_empty() {
			break;
		}

		let key = <Vec<u8> as DataDeserializer<Vec<u8>>>::deserialize(&mut reader).map_err(|e| {
			OxidbError::Storage(format!(
				"Failed to deserialize key from {}: {}",
				file_to_load.display(),
				e
			))
		})?;

		let buffer_val_check = reader.fill_buf().map_err(OxidbError::Io)?;
		if buffer_val_check.is_empty() {
			return Err(OxidbError::Storage(format!(
				"Unexpected EOF after reading key {:?} from {}",
				String::from_utf8_lossy(&key),
				file_to_load.display()
			)));
		}

		let value_bytes = <Vec<u8> as DataDeserializer<Vec<u8>>>::deserialize(&mut reader).map_err(|e| {
			OxidbError::Storage(format!(
				"Failed to deserialize value for key {:?} from {}: {}",
				String::from_utf8_lossy(&key),
				file_to_load.display(),
				e
			))
		})?;

		let versioned_value = VersionedValue { value: value_bytes, created_tx_id: 0, expired_tx_id: None };
		cache.insert(key, vec![versioned_value]);
	}
	Ok(())
}

pub(super) fn save_data_to_disk(
	file_path: &Path,
	cache: &HashMap<Vec<u8>, Vec<VersionedValue<Vec<u8>>>>,
) -> Result<(), OxidbError> {
	let temp_file_path = file_path.with_extension("tmp");

	struct TempFileGuard<'a>(&'a PathBuf);
	impl<'a> Drop for TempFileGuard<'a> {
		fn drop(&mut self) {
			let _ = std::fs::remove_file(self.0);
		}
	}
	let _temp_file_guard = TempFileGuard(&temp_file_path);

	let temp_file = OpenOptions::new().write(true).create(true).truncate(true).open(&temp_file_path).map_err(OxidbError::Io)?;
	let mut writer = BufWriter::new(temp_file);

	for (key, versions) in cache {
		let mut value_to_persist: Option<&Vec<u8>> = None;
		for version in versions.iter().rev() {
			if version.expired_tx_id.is_none() {
				value_to_persist = Some(&version.value);
				break;
			}
		}
		if let Some(value_bytes) = value_to_persist {
			<Vec<u8> as DataSerializer<Vec<u8>>>::serialize(key, &mut writer)
				.map_err(|e| OxidbError::Storage(format!("Failed to serialize key: {e}")))?;
			<Vec<u8> as DataSerializer<Vec<u8>>>::serialize(value_bytes, &mut writer)
				.map_err(|e| OxidbError::Storage(format!("Failed to serialize value: {e}")))?;
		}
	}

	writer.flush().map_err(OxidbError::Io)?;
	writer.get_ref().sync_all().map_err(OxidbError::Io)?;

	rename(&temp_file_path, file_path).map_err(|e| {
		let _ = std::fs::remove_file(&temp_file_path);
		OxidbError::Io(e)
	})?;

	let wal_file_path = derive_wal_path(file_path);
	eprintln!("[save_data_to_disk] Attempting to delete WAL file: {:?}", &wal_file_path);
	if wal_file_path.exists() {
		if let Err(e) = std::fs::remove_file(&wal_file_path) {
			eprintln!(
				"[save_data_to_disk] Error: Failed to delete WAL file {}: {}. Main data save was successful.",
				wal_file_path.display(),
				e
			);
		} else {
			eprintln!("[save_data_to_disk] Successfully deleted WAL file: {:?}", &wal_file_path);
		}
	}
	Ok(())
}