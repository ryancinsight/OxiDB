use serde::{Serialize, Deserialize};
use crate::core::common::types::ids::{PageId, SlotId};

// Define TransactionId and LogSequenceNumber if they don't exist in common::types
// For now, assuming they might be added there or are simple enough to be here.
pub type TransactionId = u32;
pub type LogSequenceNumber = u64;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Copy)]
pub enum PageType {
    TablePage,
    BTreeInternal,
    BTreeLeaf,
    // Potentially others like IndexHeaderPage, etc.
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ActiveTransactionInfo {
    pub tx_id: TransactionId,
    pub last_lsn: LogSequenceNumber,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DirtyPageInfo {
    pub page_id: PageId,
    pub recovery_lsn: LogSequenceNumber,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum LogRecord {
    BeginTransaction {
        tx_id: TransactionId,
    },
    CommitTransaction {
        tx_id: TransactionId,
        prev_lsn: LogSequenceNumber,
    },
    AbortTransaction {
        tx_id: TransactionId,
        prev_lsn: LogSequenceNumber,
    },
    InsertRecord {
        tx_id: TransactionId,
        page_id: PageId,
        slot_id: SlotId,
        record_data: Vec<u8>,
        prev_lsn: LogSequenceNumber,
    },
    DeleteRecord {
        tx_id: TransactionId,
        page_id: PageId,
        slot_id: SlotId,
        old_record_data: Vec<u8>,
        prev_lsn: LogSequenceNumber,
    },
    UpdateRecord {
        tx_id: TransactionId,
        page_id: PageId,
        slot_id: SlotId,
        old_record_data: Vec<u8>,
        new_record_data: Vec<u8>,
        prev_lsn: LogSequenceNumber,
    },
    NewPage {
        tx_id: TransactionId,
        page_id: PageId,
        page_type: PageType,
        prev_lsn: LogSequenceNumber,
    },
    CompensationLogRecord { // CLR
        tx_id: TransactionId,
        page_id: PageId,
        slot_id: Option<SlotId>, // Some operations might be page-level
        undone_lsn: LogSequenceNumber, // LSN of the log record that was undone
        // Data needed to redo the undo operation.
        // For an undone Insert, this would be the key/identifier to delete.
        // For an undone Delete, this would be the data that was deleted (to re-insert).
        // For an undone Update, this would be the *old* data before the update that was undone.
        data_for_redo_of_undo: Vec<u8>,
        prev_lsn: LogSequenceNumber, // Previous LSN for this transaction
        next_undo_lsn: Option<LogSequenceNumber>, // For traversing undo chain for this transaction
    },
    CheckpointBegin {
        // checkpoint_lsn: LogSequenceNumber, // The LSN at which this checkpoint process starts
    },
    CheckpointEnd {
        active_transactions: Vec<ActiveTransactionInfo>,
        dirty_pages: Vec<DirtyPageInfo>,
        // checkpoint_start_lsn: LogSequenceNumber, // Reference to the CheckpointBegin LSN
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode;
    use crate::core::common::types::ids::{PageId, SlotId};

    #[test]
    fn test_serialize_deserialize_begin_transaction() {
        let original_record = LogRecord::BeginTransaction { tx_id: 123 };
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_commit_transaction() {
        let original_record = LogRecord::CommitTransaction { tx_id: 123, prev_lsn: 455 };
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_abort_transaction() {
        let original_record = LogRecord::AbortTransaction { tx_id: 123, prev_lsn: 456 };
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_insert_record() {
        let original_record = LogRecord::InsertRecord {
            tx_id: 1,
            page_id: PageId(2),
            slot_id: SlotId(3),
            record_data: vec![10, 20, 30],
            prev_lsn: 100,
        };
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_delete_record() {
        let original_record = LogRecord::DeleteRecord {
            tx_id: 1,
            page_id: PageId(2),
            slot_id: SlotId(3),
            old_record_data: vec![40, 50, 60],
            prev_lsn: 101,
        };
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_update_record() {
        let original_record = LogRecord::UpdateRecord {
            tx_id: 1,
            page_id: PageId(2),
            slot_id: SlotId(3),
            old_record_data: vec![70, 80],
            new_record_data: vec![90, 100],
            prev_lsn: 102,
        };
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_new_page() {
        let original_record = LogRecord::NewPage {
            tx_id: 1,
            page_id: PageId(5),
            page_type: PageType::TablePage,
            prev_lsn: 103,
        };
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_compensation_log_record() {
        let original_record = LogRecord::CompensationLogRecord {
            tx_id: 1,
            page_id: PageId(6),
            slot_id: Some(SlotId(7)),
            undone_lsn: 200,
            data_for_redo_of_undo: vec![1, 2, 3],
            prev_lsn: 104,
            next_undo_lsn: Some(99),
        };
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_compensation_log_record_no_slot() {
        let original_record = LogRecord::CompensationLogRecord {
            tx_id: 1,
            page_id: PageId(6),
            slot_id: None,
            undone_lsn: 201,
            data_for_redo_of_undo: vec![4, 5, 6],
            prev_lsn: 105,
            next_undo_lsn: None,
        };
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_checkpoint_begin() {
        let original_record = LogRecord::CheckpointBegin {};
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_checkpoint_end() {
        let original_record = LogRecord::CheckpointEnd {
            active_transactions: vec![
                ActiveTransactionInfo { tx_id: 1, last_lsn: 10 },
                ActiveTransactionInfo { tx_id: 2, last_lsn: 20 },
            ],
            dirty_pages: vec![
                DirtyPageInfo { page_id: PageId(100), recovery_lsn: 5 },
                DirtyPageInfo { page_id: PageId(101), recovery_lsn: 15 },
            ],
        };
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }
}
