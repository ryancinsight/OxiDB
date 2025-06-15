use crate::core::common::OxidbError; // Changed
use crate::core::common::traits::{DataDeserializer, DataSerializer};
use crate::core::storage::engine::traits::VersionedValue;
use std::collections::HashMap;
use std::fs::{rename, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, ErrorKind, Write};
use std::path::{Path, PathBuf}; // Added PathBuf for derive_wal_path

// Helper function to derive WAL path from DB path, needed for save_data_to_disk
// This was a local helper in the original tests, but useful here too.
// Made it internal to this module.
fn derive_wal_path(db_path: &Path) -> PathBuf {
    let mut wal_path = db_path.to_path_buf();
    let original_extension = wal_path.extension().map(|s| s.to_os_string());
    if let Some(ext) = original_extension {
        let mut new_ext = ext;
        new_ext.push(".wal");
        wal_path.set_extension(new_ext);
    } else {
        wal_path.set_extension("wal");
    }
    wal_path
}

/// Loads data from the main data file or a temporary recovery file into the cache.
/// This function does NOT handle WAL replay.
pub(super) fn load_data_from_disk(
    file_path: &Path,
    _wal_path: &Path, // Kept for signature compatibility if needed, but not used directly here
    cache: &mut HashMap<Vec<u8>, Vec<VersionedValue<Vec<u8>>>>,
) -> Result<(), OxidbError> { // Changed
    let temp_file_path = file_path.with_extension("tmp");

    if temp_file_path.exists() {
        match read_data_into_cache_internal(cache, &temp_file_path) {
            Ok(()) => {
                if let Err(e) = rename(&temp_file_path, file_path) {
                    return Err(OxidbError::Storage(format!( // Changed
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
                    return Err(OxidbError::Storage(format!( // Changed
                        "Corrupted temporary file {} could not be loaded ({}) or deleted ({}). Manual intervention may be required.",
                        temp_file_path.display(),
                        load_err,
                        remove_err
                    )));
                }
            }
        }
    }
    // If temp file didn't exist, or loading from it failed and it was deleted, load main file.
    read_data_into_cache_internal(cache, file_path)
}

/// Reads key-value pairs from the specified file path into the cache.
/// Assumes data is version 0 (base version).
fn read_data_into_cache_internal(
    cache: &mut HashMap<Vec<u8>, Vec<VersionedValue<Vec<u8>>>>,
    file_to_load: &Path,
) -> Result<(), OxidbError> { // Changed
    cache.clear();
    let file = match File::open(file_to_load) {
        Ok(f) => f,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(OxidbError::Io(e)), // Changed
    };

    let mut reader = BufReader::new(file);
    loop {
        // Check for EOF before trying to deserialize key length.
        // fill_buf returns an empty slice at EOF.
        let buffer = reader.fill_buf().map_err(OxidbError::Io)?; // Changed
        if buffer.is_empty() {
            break; // Clean EOF
        }

        let key =
            <Vec<u8> as DataDeserializer<Vec<u8>>>::deserialize(&mut reader).map_err(|e| {
                OxidbError::Storage(format!( // Changed
                    "Failed to deserialize key from {}: {}",
                    file_to_load.display(),
                    e
                ))
            })?;

        // Need to check for EOF again before deserializing value, in case file ends after a valid key.
        let buffer_val_check = reader.fill_buf().map_err(OxidbError::Io)?; // Changed
        if buffer_val_check.is_empty() {
            return Err(OxidbError::Storage(format!( // Changed
                "Unexpected EOF after reading key {:?} from {}",
                String::from_utf8_lossy(&key),
                file_to_load.display()
            )));
        }

        let value_bytes = <Vec<u8> as DataDeserializer<Vec<u8>>>::deserialize(&mut reader)
            .map_err(|e| {
                OxidbError::Storage(format!( // Changed
                    "Failed to deserialize value for key {:?} from {}: {}",
                    String::from_utf8_lossy(&key),
                    file_to_load.display(),
                    e
                ))
            })?;

        let versioned_value = VersionedValue {
            value: value_bytes,
            created_tx_id: 0, // Base version from .db file
            expired_tx_id: None,
        };
        cache.insert(key, vec![versioned_value]);
    }
    Ok(())
}

/// Saves the current in-memory cache to disk and clears the WAL.
pub(super) fn save_data_to_disk(
    file_path: &Path,
    cache: &HashMap<Vec<u8>, Vec<VersionedValue<Vec<u8>>>>,
) -> Result<(), OxidbError> { // Changed
    let temp_file_path = file_path.with_extension("tmp");

    struct TempFileGuard<'a>(&'a PathBuf);
    impl<'a> Drop for TempFileGuard<'a> {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(self.0);
        }
    }
    let _temp_file_guard = TempFileGuard(&temp_file_path);

    let temp_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp_file_path)
        .map_err(OxidbError::Io)?; // Changed

    let mut writer = BufWriter::new(temp_file);

    for (key, versions) in cache {
        // Find the latest version that should be persisted.
        // This logic assumes we only persist the latest non-expired version created by tx_id 0 (auto-commit)
        // or a version from a committed transaction that isn't expired by another committed transaction.
        // For simplicity, the original save_to_disk selected versions with created_tx_id == 0 and no expired_tx_id.
        // We'll replicate that simple logic for now. More complex logic would require passing committed_ids.
        let value_to_write_opt = versions
            .iter()
            .filter(|v| v.created_tx_id == 0 && v.expired_tx_id.is_none()) // Persist base/committed, non-expired versions
            .next_back() // Get the latest if multiple tx0 versions (should not happen with current logic)
            .map(|v| v.value.clone());

        if let Some(value_to_write) = value_to_write_opt {
            <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(key, &mut writer)
                .map_err(|e| OxidbError::Storage(format!("Failed to serialize key: {}", e)))?; // Changed
            <Vec<u8> as DataSerializer<Vec<u8>>>::serialize(&value_to_write, &mut writer)
                .map_err(|e| OxidbError::Storage(format!("Failed to serialize value: {}", e)))?; // Changed
        }
    }

    writer.flush().map_err(OxidbError::Io)?; // Changed
    writer.get_ref().sync_all().map_err(OxidbError::Io)?; // Changed

    rename(&temp_file_path, file_path).map_err(|e| {
        let _ = std::fs::remove_file(&temp_file_path);
        OxidbError::Io(e) // Changed
    })?;

    // Delete WAL file after successful save to disk
    let wal_file_path = derive_wal_path(file_path);
    eprintln!("[save_data_to_disk] Attempting to delete WAL file: {:?}", &wal_file_path);
    if wal_file_path.exists() {
        eprintln!("[save_data_to_disk] WAL file {:?} exists, proceeding with deletion.", &wal_file_path);
        if let Err(e) = std::fs::remove_file(&wal_file_path) {
            eprintln!(
                "[save_data_to_disk] Error: Failed to delete WAL file {}: {}. Main data save was successful.",
                wal_file_path.display(),
                e
            );
            // Not returning an error here as main data is safe in current design,
            // but for tests this might hide issues.
        } else {
            eprintln!("[save_data_to_disk] Successfully deleted WAL file: {:?}", &wal_file_path);
        }
    } else {
        eprintln!("[save_data_to_disk] WAL file {:?} did not exist, no deletion needed.", &wal_file_path);
    }
    Ok(())
}
