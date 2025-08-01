use crate::core::common::traits::DataDeserializer;
use crate::core::common::OxidbError; // Changed
use crate::core::storage::engine::traits::VersionedValue;
use crate::core::storage::engine::wal::WalEntry;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, ErrorKind}; // Added BufRead
use std::path::Path;

/// Replays Write-Ahead Log entries into the cache.
pub(super) fn replay_wal_into_cache(
    cache: &mut HashMap<Vec<u8>, Vec<VersionedValue<Vec<u8>>>>,
    wal_file_path: &Path,
) -> Result<(), OxidbError> {
    // Changed
    if !wal_file_path.exists() {
        return Ok(()); // No WAL file, nothing to replay.
    }

    let wal_file = match File::open(wal_file_path) {
        Ok(f) => f,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(OxidbError::Io(e)),
    };
    let mut reader = BufReader::new(wal_file);

    let mut transaction_operations: HashMap<u64, Vec<WalEntry>> = HashMap::new();
    let mut committed_transactions: HashSet<u64> = HashSet::new();
    let mut rolled_back_transactions: HashSet<u64> = HashSet::new();

    // First Pass: Populate data structures
    loop {
        match <WalEntry as DataDeserializer<WalEntry>>::deserialize(&mut reader) {
            Ok(entry) => match &entry {
                WalEntry::Put { lsn: _, transaction_id, .. }
                | WalEntry::Delete { lsn: _, transaction_id, .. } => {
                    // Added lsn: _
                    transaction_operations.entry(*transaction_id).or_default().push(entry);
                }
                WalEntry::TransactionCommit { lsn: _, transaction_id } => {
                    // Added lsn: _
                    committed_transactions.insert(*transaction_id);
                }
                WalEntry::TransactionRollback { lsn: _, transaction_id } => {
                    // Added lsn: _
                    rolled_back_transactions.insert(*transaction_id);
                }
            },
            Err(OxidbError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(OxidbError::Deserialization(msg)) => {
                // Changed
                eprintln!("WAL corruption detected (Deserialization error): {msg}. Replay stopped. Data up to this point is recovered.");
                break;
            }
            Err(e) => {
                // This e is now OxidbError
                eprintln!("Error during WAL replay: {e}. Replay stopped. Data up to this point is recovered.");
                break;
            }
        }
    }

    // Second Pass: Apply committed operations
    println!("[replay_wal] Committed transactions for replay: {committed_transactions:?}");
    println!("[replay_wal] Rolled back transactions for replay: {rolled_back_transactions:?}");

    let mut tx_ids: Vec<u64> = transaction_operations.keys().copied().collect();
    tx_ids.sort_unstable();
    println!("[replay_wal] Processing tx_ids in order: {tx_ids:?}");

    for tx_id in tx_ids {
        // If tx_id is 0 (auto-commit/non-transactional in store WAL) or explicitly committed,
        // and not rolled back, then apply.
        if (tx_id == 0 || committed_transactions.contains(&tx_id))
            && !rolled_back_transactions.contains(&tx_id)
        {
            println!("[replay_wal] Applying operations for tx_id: {tx_id} (implicit or committed)");
            if let Some(operations) = transaction_operations.get(&tx_id) {
                for entry in operations {
                    println!("[replay_wal] Applying entry: {entry:?}");
                    match entry {
                        WalEntry::Put { lsn: _, key, value, transaction_id } => {
                            let versions = cache.entry(key.clone()).or_default();
                            for version in versions.iter_mut().rev() {
                                if version.expired_tx_id.is_none()
                                    && (version.created_tx_id == *transaction_id
                                        || version.created_tx_id == 0
                                        || committed_transactions.contains(&version.created_tx_id))
                                {
                                    version.expired_tx_id = Some(*transaction_id);
                                    break;
                                }
                            }
                            let new_version = VersionedValue {
                                value: value.clone(),
                                created_tx_id: *transaction_id,
                                expired_tx_id: None,
                            };
                            versions.push(new_version);
                            if key == b"key_a_wal_restart".as_slice() {
                                println!(
                                    "[replay_wal] Cache for key_a after Put({tx_id}): {versions:?}"
                                );
                            }
                        }
                        WalEntry::Delete { lsn: _, key, transaction_id } => {
                            if let Some(versions) = cache.get_mut(key) {
                                for version in versions.iter_mut().rev() {
                                    if version.expired_tx_id.is_none()
                                        && (version.created_tx_id == *transaction_id
                                            || version.created_tx_id == 0
                                            || committed_transactions
                                                .contains(&version.created_tx_id))
                                    {
                                        version.expired_tx_id = Some(*transaction_id);
                                        break;
                                    }
                                }
                            }
                            if key == b"key_a_wal_restart".as_slice() {
                                println!(
                                    "[replay_wal] Cache for key_a after Delete({}): {:?}",
                                    tx_id,
                                    cache.get(key)
                                );
                            }
                        }
                        _ => {} // TransactionCommit/Rollback entries handled by sets
                    }
                }
            }
        } else {
            println!(
                "[replay_wal] Skipping operations for tx_id: {tx_id} (not committed or was rolled back)"
            );
        }
    }
    Ok(())
}
