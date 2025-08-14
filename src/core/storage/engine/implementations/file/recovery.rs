use crate::core::common::traits::DataDeserializer;
use crate::core::common::OxidbError;
use crate::core::storage::engine::traits::VersionedValue;
use crate::core::storage::engine::wal::WalEntry;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, ErrorKind};
use std::path::Path;

/// Replays Write-Ahead Log entries into the cache.
pub(super) fn replay_wal_into_cache(
	cache: &mut HashMap<Vec<u8>, Vec<VersionedValue<Vec<u8>>>>,
	wal_file_path: &Path,
) -> Result<(), OxidbError> {
	if !wal_file_path.exists() {
		return Ok(());
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

	// First pass: collect operations per transaction and commit/rollback markers
	loop {
		match <WalEntry as DataDeserializer<WalEntry>>::deserialize(&mut reader) {
			Ok(entry) => match &entry {
				WalEntry::Put { lsn: _, transaction_id, .. }
				| WalEntry::Delete { lsn: _, transaction_id, .. } => {
					transaction_operations.entry(*transaction_id).or_default().push(entry);
				}
				WalEntry::TransactionCommit { lsn: _, transaction_id } => {
					committed_transactions.insert(*transaction_id);
				}
				WalEntry::TransactionRollback { lsn: _, transaction_id } => {
					rolled_back_transactions.insert(*transaction_id);
				}
			},
			Err(OxidbError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
			Err(_) => break,
		}
	}

	// Second pass: apply operations for committed (or tx_id=0) and not rolled back
	let mut tx_ids: Vec<u64> = transaction_operations.keys().copied().collect();
	tx_ids.sort_unstable();
	for tx_id in tx_ids {
		if (tx_id == 0 || committed_transactions.contains(&tx_id))
			&& !rolled_back_transactions.contains(&tx_id)
		{
			if let Some(operations) = transaction_operations.get(&tx_id) {
				for entry in operations {
					match entry {
						WalEntry::Put { key, value, transaction_id, .. } => {
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
							let new_version = VersionedValue { value: value.clone(), created_tx_id: *transaction_id, expired_tx_id: None };
							versions.push(new_version);
						}
						WalEntry::Delete { key, transaction_id, .. } => {
							if let Some(versions) = cache.get_mut(key) {
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
							}
						}
						_ => {}
					}
				}
			}
		}
	}
	Ok(())
}