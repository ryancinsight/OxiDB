use crate::core::common::types::ids::{PageId, SlotId};
use crate::core::common::types::{Lsn, TransactionId};
use serde::{Deserialize, Serialize};

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
    pub last_lsn: Lsn,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DirtyPageInfo {
    pub page_id: PageId,
    pub recovery_lsn: Lsn,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum LogRecord {
    BeginTransaction {
        lsn: Lsn,
        tx_id: TransactionId,
    },
    CommitTransaction {
        lsn: Lsn,
        tx_id: TransactionId,
        prev_lsn: Lsn,
    },
    AbortTransaction {
        lsn: Lsn,
        tx_id: TransactionId,
        prev_lsn: Lsn,
    },
    InsertRecord {
        lsn: Lsn,
        tx_id: TransactionId,
        page_id: PageId,
        slot_id: SlotId,
        record_data: Vec<u8>,
        prev_lsn: Lsn,
    },
    DeleteRecord {
        lsn: Lsn,
        tx_id: TransactionId,
        page_id: PageId,
        slot_id: SlotId,
        old_record_data: Vec<u8>,
        prev_lsn: Lsn,
    },
    UpdateRecord {
        lsn: Lsn,
        tx_id: TransactionId,
        page_id: PageId,
        slot_id: SlotId,
        old_record_data: Vec<u8>,
        new_record_data: Vec<u8>,
        prev_lsn: Lsn,
    },
    NewPage {
        lsn: Lsn,
        tx_id: TransactionId,
        page_id: PageId,
        page_type: PageType,
        prev_lsn: Lsn,
    },
    CompensationLogRecord {
        // CLR
        lsn: Lsn,
        tx_id: TransactionId,
        page_id: PageId,
        slot_id: Option<SlotId>, // Some operations might be page-level
        undone_lsn: Lsn,         // LSN of the log record that was undone
        data_for_redo_of_undo: Vec<u8>,
        prev_lsn: Lsn,              // Previous LSN for this transaction
        next_undo_lsn: Option<Lsn>, // For traversing undo chain for this transaction
    },
    CheckpointBegin {
        lsn: Lsn,
        // checkpoint_lsn: Lsn, // The LSN at which this checkpoint process starts
    },
    CheckpointEnd {
        lsn: Lsn,
        active_transactions: Vec<ActiveTransactionInfo>,
        dirty_pages: Vec<DirtyPageInfo>,
        // checkpoint_start_lsn: Lsn, // Reference to the CheckpointBegin LSN
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::common::types::ids::{PageId, SlotId};
    use bincode;

    // Note: Existing tests will fail due to the added 'lsn' field.
    // These tests need to be updated to include the 'lsn' field in their assertions.
    // This subtask focuses on struct changes, test updates will be a separate step if not included here.
    // For now, I will update the tests to reflect the new structure.

    #[test]
    fn test_serialize_deserialize_begin_transaction() {
        let original_record = LogRecord::BeginTransaction { lsn: 0, tx_id: TransactionId(123) }; // Use TransactionId()
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_commit_transaction() {
        let original_record =
            LogRecord::CommitTransaction { lsn: 1, tx_id: TransactionId(123), prev_lsn: 0 }; // Use TransactionId()
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_abort_transaction() {
        let original_record =
            LogRecord::AbortTransaction { lsn: 2, tx_id: TransactionId(123), prev_lsn: 1 }; // Use TransactionId()
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_insert_record() {
        let original_record = LogRecord::InsertRecord {
            lsn: 3,
            tx_id: TransactionId(1), // Use TransactionId()
            page_id: PageId(2),
            slot_id: SlotId(3),
            record_data: vec![10, 20, 30],
            prev_lsn: 2,
        };
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_delete_record() {
        let original_record = LogRecord::DeleteRecord {
            lsn: 4,
            tx_id: TransactionId(1), // Use TransactionId()
            page_id: PageId(2),
            slot_id: SlotId(3),
            old_record_data: vec![40, 50, 60],
            prev_lsn: 3,
        };
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_update_record() {
        let original_record = LogRecord::UpdateRecord {
            lsn: 5,
            tx_id: TransactionId(1), // Use TransactionId()
            page_id: PageId(2),
            slot_id: SlotId(3),
            old_record_data: vec![70, 80],
            new_record_data: vec![90, 100],
            prev_lsn: 4,
        };
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_new_page() {
        let original_record = LogRecord::NewPage {
            lsn: 6,
            tx_id: TransactionId(1), // Use TransactionId()
            page_id: PageId(5),
            page_type: PageType::TablePage,
            prev_lsn: 5,
        };
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_compensation_log_record() {
        let original_record = LogRecord::CompensationLogRecord {
            lsn: 7,
            tx_id: TransactionId(1), // Use TransactionId()
            page_id: PageId(6),
            slot_id: Some(SlotId(7)),
            undone_lsn: 200, // This LSN refers to another record's LSN.
            data_for_redo_of_undo: vec![1, 2, 3],
            prev_lsn: 6,
            next_undo_lsn: Some(99), // This LSN also refers to another record's LSN.
        };
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_compensation_log_record_no_slot() {
        let original_record = LogRecord::CompensationLogRecord {
            lsn: 8,
            tx_id: TransactionId(1), // Use TransactionId()
            page_id: PageId(6),
            slot_id: None,
            undone_lsn: 201,
            data_for_redo_of_undo: vec![4, 5, 6],
            prev_lsn: 7,
            next_undo_lsn: None,
        };
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_checkpoint_begin() {
        let original_record = LogRecord::CheckpointBegin { lsn: 9 };
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_checkpoint_end() {
        let original_record = LogRecord::CheckpointEnd {
            lsn: 10,
            active_transactions: vec![
                ActiveTransactionInfo { tx_id: TransactionId(1), last_lsn: 8 }, // Use TransactionId()
                ActiveTransactionInfo { tx_id: TransactionId(2), last_lsn: 7 }, // Use TransactionId()
            ],
            dirty_pages: vec![
                DirtyPageInfo { page_id: PageId(100), recovery_lsn: 5 }, // recovery_lsn is an Lsn
                DirtyPageInfo { page_id: PageId(101), recovery_lsn: 6 },
            ],
        };
        let serialized = bincode::serialize(&original_record).unwrap();
        let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
        assert_eq!(original_record, deserialized);
    }
}
