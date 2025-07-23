use crate::core::common::error::OxidbError;
use crate::core::common::types::ids::SlotId;
use crate::core::storage::engine::page::{PAGE_HEADER_SIZE, PAGE_SIZE};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;

// Constants for TablePage layout within Page.data buffer
// The Page.data buffer is PAGE_SIZE - PAGE_HEADER_SIZE bytes long.
// pub(crate) const PAGE_DATA_SIZE_FOR_TESTING: usize = PAGE_SIZE - PAGE_HEADER_SIZE; // Removed, use PAGE_DATA_AREA_SIZE

// Offset for the number of records (u16)
pub(crate) const NUM_RECORDS_METADATA_OFFSET: usize = 0;
pub(crate) const NUM_RECORDS_METADATA_SIZE: usize = 2;

// Offset for the free space pointer (u16).
// This pointer indicates the offset where the next record *data* can be written.
// Record data grows from left to right (low addresses to high addresses).
pub(crate) const FREE_SPACE_POINTER_METADATA_OFFSET: usize =
    NUM_RECORDS_METADATA_OFFSET + NUM_RECORDS_METADATA_SIZE;
pub(crate) const FREE_SPACE_POINTER_METADATA_SIZE: usize = 2;

// Offset where the actual slot array begins.
pub(crate) const SLOTS_ARRAY_DATA_OFFSET: usize =
    FREE_SPACE_POINTER_METADATA_OFFSET + FREE_SPACE_POINTER_METADATA_SIZE;

/// Represents a slot in the `TablePage`.
/// A slot stores the location and size of a record's data.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Slot {
    /// Offset of the record data from the start of the Page's data area (Page.data).
    pub offset: u16,
    /// Length of the record data. If 0, this slot is considered empty/unoccupied.
    pub length: u16,
}

impl Slot {
    /// Size of a Slot when serialized on disk.
    pub const SERIALIZED_SIZE: usize = 2 + 2; // offset (u16) + length (u16)

    fn serialize(&self, buffer: &mut [u8]) -> Result<(), OxidbError> {
        if buffer.len() < Self::SERIALIZED_SIZE {
            return Err(OxidbError::Serialization("Buffer too small for Slot".to_string()));
        }
        let mut cursor = Cursor::new(buffer);
        cursor.write_u16::<LittleEndian>(self.offset)?;
        cursor.write_u16::<LittleEndian>(self.length)?;
        Ok(())
    }

    fn deserialize(buffer: &[u8]) -> Result<Self, OxidbError> {
        if buffer.len() < Self::SERIALIZED_SIZE {
            return Err(OxidbError::Deserialization("Buffer too small for Slot".to_string()));
        }
        let mut cursor = Cursor::new(buffer);
        let offset = cursor.read_u16::<LittleEndian>()?;
        let length = cursor.read_u16::<LittleEndian>()?;
        Ok(Self { offset, length })
    }
}

/// `TablePage` provides methods to manage records within a single page's data buffer.
/// It interprets and modifies a raw byte slice (`page_data`) according to a specific page layout.
///
/// Page Data Layout:
/// [ Number of Records (u16) ]
/// [ Free Space Pointer (u16) ] -> Points to the start of the available free space for record data.
/// [ Slot Array (Slot * `num_records_capacity`) ] -> Array of Slots.
/// [ Record Data Area (...) ] -> Actual record bytes, grows towards higher addresses.
/// [ Free Space Area (...) ]
///
/// Note: `num_records_capacity` is not explicitly stored; it's derived from available page space.
/// Slots are occupied from index 0 up to `get_num_records() - 1`.
/// Record data is generally stored contiguously but deletions can cause fragmentation.
/// This initial implementation assumes record data is appended, and deletions mark slots empty.
/// Compaction is not handled in this version.
pub struct TablePage;

impl TablePage {
    // This constant is specific to TablePage's data area, not the overall Page.
    // It's used by tests, so pub(crate) or pub.
    #[allow(dead_code)] // Used in tests, but clippy doesn't see it
    pub(crate) const PAGE_DATA_AREA_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE; // Uncommented for tests

    /// Gets the number of records currently stored in the page.
    /// This corresponds to the number of occupied/valid slots.
    pub(crate) fn get_num_records(page_data: &[u8]) -> Result<u16, OxidbError> {
        if page_data.len() < NUM_RECORDS_METADATA_OFFSET + NUM_RECORDS_METADATA_SIZE {
            return Err(OxidbError::Internal(
                "Page data too small for num_records metadata".into(),
            ));
        }
        let mut cursor = Cursor::new(&page_data[NUM_RECORDS_METADATA_OFFSET..]);
        Ok(cursor.read_u16::<LittleEndian>()?)
    }

    /// Sets the number of records in the page.
    pub(crate) fn set_num_records(page_data: &mut [u8], count: u16) -> Result<(), OxidbError> {
        if page_data.len() < NUM_RECORDS_METADATA_OFFSET + NUM_RECORDS_METADATA_SIZE {
            return Err(OxidbError::Internal(
                "Page data too small for num_records metadata".into(),
            ));
        }
        let mut cursor = Cursor::new(&mut page_data[NUM_RECORDS_METADATA_OFFSET..]);
        cursor.write_u16::<LittleEndian>(count)?;
        Ok(())
    }

    /// Gets the free space pointer.
    /// This pointer indicates the offset from the start of `page_data` where the next record's
    /// *data* can begin to be written.
    pub(crate) fn get_free_space_pointer(page_data: &[u8]) -> Result<u16, OxidbError> {
        if page_data.len() < FREE_SPACE_POINTER_METADATA_OFFSET + FREE_SPACE_POINTER_METADATA_SIZE {
            return Err(OxidbError::Internal(
                "Page data too small for free_space_pointer metadata".into(),
            ));
        }
        let mut cursor = Cursor::new(&page_data[FREE_SPACE_POINTER_METADATA_OFFSET..]);
        Ok(cursor.read_u16::<LittleEndian>()?)
    }

    /// Sets the free space pointer.
    pub(crate) fn set_free_space_pointer(
        page_data: &mut [u8],
        pointer: u16,
    ) -> Result<(), OxidbError> {
        if page_data.len() < FREE_SPACE_POINTER_METADATA_OFFSET + FREE_SPACE_POINTER_METADATA_SIZE {
            return Err(OxidbError::Internal(
                "Page data too small for free_space_pointer metadata".into(),
            ));
        }
        let mut cursor = Cursor::new(&mut page_data[FREE_SPACE_POINTER_METADATA_OFFSET..]);
        cursor.write_u16::<LittleEndian>(pointer)?;
        Ok(())
    }

    // Removed get_max_slot_capacity as it's no longer used with the new init/insert logic.

    /// Retrieves a slot's metadata. Returns `None` if `SlotId` is out of bounds of current `num_records`.
    /// Note: This doesn't mean the slot is occupied, check Slot.length.
    pub(crate) fn get_slot_info(
        page_data: &[u8],
        slot_id: SlotId,
    ) -> Result<Option<Slot>, OxidbError> {
        let num_records = Self::get_num_records(page_data)?;
        if slot_id.0 >= num_records {
            // Requesting a slot_id beyond the current number of active/initialized slots
            return Ok(None);
        }

        let slot_offset_in_page_data =
            SLOTS_ARRAY_DATA_OFFSET + (slot_id.0 as usize * Slot::SERIALIZED_SIZE);
        // Check if reading this slot's metadata would go out of bounds of the page_data slice itself.
        if slot_offset_in_page_data + Slot::SERIALIZED_SIZE > page_data.len() {
            return Err(OxidbError::Internal(format!(
                "SlotId {} metadata read out of bounds for page_data len {}",
                slot_id.0,
                page_data.len()
            )));
        }

        let slot_data =
            &page_data[slot_offset_in_page_data..slot_offset_in_page_data + Slot::SERIALIZED_SIZE];
        Ok(Some(Slot::deserialize(slot_data)?))
    }

    /// Writes a slot's metadata to the specified `SlotId`.
    /// Assumes `slot_id` is valid and within current `num_records` or is the next available slot.
    pub(crate) fn set_slot_info(
        page_data: &mut [u8],
        slot_id: SlotId,
        slot: Slot,
    ) -> Result<(), OxidbError> {
        let slot_offset_in_page_data =
            SLOTS_ARRAY_DATA_OFFSET + (slot_id.0 as usize * Slot::SERIALIZED_SIZE);

        // Check if writing this slot's metadata would go out of bounds.
        if slot_offset_in_page_data + Slot::SERIALIZED_SIZE > page_data.len() {
            return Err(OxidbError::Internal(format!(
                "Cannot set SlotId {} metadata as it's out of bounds for page_data len {}",
                slot_id.0,
                page_data.len()
            )));
        }

        let slot_buffer = &mut page_data
            [slot_offset_in_page_data..slot_offset_in_page_data + Slot::SERIALIZED_SIZE];
        slot.serialize(slot_buffer)?;
        Ok(())
    }

    /// Initializes a new `TablePage` structure on a raw `page_data` buffer.
    /// Sets `num_records` to 0 and `free_space_pointer` to the start of where data can be written.
    pub fn init(page_data: &mut [u8]) -> Result<(), OxidbError> {
        if page_data.len() < SLOTS_ARRAY_DATA_OFFSET {
            // Check if page can even hold the basic header
            return Err(OxidbError::Storage(
                "Page data too small to initialize as TablePage".to_string(),
            ));
        }
        Self::set_num_records(page_data, 0)?;
        // free_space_pointer now indicates the beginning of where record data can be written.
        // Initially, this is right after the fixed header part (num_records, free_space_pointer itself).
        // The slot array will start being populated from SLOTS_ARRAY_DATA_OFFSET.
        // Record data will be written starting from the current value of free_space_pointer.
        Self::set_free_space_pointer(page_data, SLOTS_ARRAY_DATA_OFFSET as u16)?;
        Ok(())
    }

    pub fn insert_record(page_data: &mut [u8], data: &[u8]) -> Result<SlotId, OxidbError> {
        if data.is_empty() {
            return Err(OxidbError::InvalidInput {
                message: "Record data cannot be empty".to_string(),
            });
        }
        let data_len = data.len() as u16;
        if data_len == 0 {
            // Should be caught by is_empty, but as u16 check.
            return Err(OxidbError::InvalidInput {
                message: "Record data length cannot be zero".to_string(),
            });
        }
        if data.len() > u16::MAX as usize {
            // data_len already u16, this check is more about original data.len()
            return Err(OxidbError::InvalidInput {
                message: "Record data too large for u16 length".to_string(),
            });
        }

        let num_records = Self::get_num_records(page_data)?;
        // current_data_append_ptr is the end of the last written record's data,
        // or SLOTS_ARRAY_DATA_OFFSET if no records yet.
        let current_data_append_ptr = Self::get_free_space_pointer(page_data)?;

        // Find an empty slot or determine if a new one is needed
        let mut target_slot_id_obj = None;
        for i in 0..num_records {
            let slot_id = SlotId(i);
            if let Some(slot_info) = Self::get_slot_info(page_data, slot_id)? {
                if slot_info.length == 0 {
                    // Found an empty (deleted) slot
                    target_slot_id_obj = Some(slot_id);
                    break;
                }
            }
        }

        let final_slot_id;
        let is_new_slot;
        if let Some(slot_id) = target_slot_id_obj {
            final_slot_id = slot_id;
            is_new_slot = false;
        } else {
            final_slot_id = SlotId(num_records); // Index for the new slot
            is_new_slot = true;
        }

        // Determine the end of the slot array IF this operation completes.
        // If it's a new slot, the array effectively grows.
        let end_of_slot_array_if_op_completes = if is_new_slot {
            SLOTS_ARRAY_DATA_OFFSET + ((num_records + 1) as usize * Slot::SERIALIZED_SIZE)
        } else {
            SLOTS_ARRAY_DATA_OFFSET + (num_records as usize * Slot::SERIALIZED_SIZE)
            // Current end
        };

        // Data must be written after the slot array.
        // Also, data is appended to where previous data ended (current_data_append_ptr).
        // So, the actual write offset for data is the max of these two.
        let record_data_write_offset =
            (end_of_slot_array_if_op_completes as u16).max(current_data_append_ptr);

        let record_data_write_end =
            record_data_write_offset.checked_add(data_len).ok_or_else(|| {
                OxidbError::Storage("Record data offset calculation overflow".to_string())
            })?;

        // Final space check: does the end of data exceed page capacity?
        if record_data_write_end as usize > page_data.len() {
            return Err(OxidbError::Storage(
                "Page full: no space for record data (after considering slot array)".to_string(),
            ));
        }

        // The overlap check `end_of_slot_array_after_this_op > record_data_write_offset`
        // is now implicitly handled because `record_data_write_offset` is guaranteed to be
        // at or after `end_of_slot_array_if_op_completes` (if it's a new slot)
        // or after the current slot array end (if reusing an old slot, where current_data_append_ptr might be even further).

        // All checks passed, proceed with writes:
        // 1. Write record data
        page_data[record_data_write_offset as usize..record_data_write_end as usize]
            .copy_from_slice(data);

        // 2. Write/Update slot information for `final_slot_id`
        let slot_info = Slot { offset: record_data_write_offset, length: data_len };
        Self::set_slot_info(page_data, final_slot_id, slot_info)?;

        // 3. Update free space pointer to be after the newly written data.
        Self::set_free_space_pointer(page_data, record_data_write_end)?;

        // 4. Update number of records if it's a brand new slot
        if is_new_slot {
            Self::set_num_records(page_data, num_records + 1)?;
        }
        // If reusing a slot, num_records (high water mark of slots) doesn't change. Slot.length > 0 marks it occupied.

        Ok(final_slot_id)
    }

    pub fn get_record(page_data: &[u8], slot_id: SlotId) -> Result<Option<Vec<u8>>, OxidbError> {
        match Self::get_slot_info(page_data, slot_id)? {
            Some(slot) if slot.length > 0 => {
                // Slot exists and is occupied
                let data_end = slot.offset as usize + slot.length as usize;
                if data_end > page_data.len() {
                    return Err(OxidbError::Internal(format!(
                        "Record data for SlotId {} (offset: {}, length: {}) exceeds page_data bounds (len: {})",
                        slot_id.0, slot.offset, slot.length, page_data.len()
                    )));
                }
                Ok(Some(page_data[slot.offset as usize..data_end].to_vec()))
            }
            Some(_) => Ok(None), // Slot exists but is empty (length == 0)
            None => Ok(None),    // SlotId is out of bounds of current num_records
        }
    }

    pub fn delete_record(page_data: &mut [u8], slot_id: SlotId) -> Result<(), OxidbError> {
        let num_records = Self::get_num_records(page_data)?;
        if slot_id.0 >= num_records {
            return Err(OxidbError::NotFound(format!("SlotId {} out of bounds", slot_id.0)));
        }

        let mut slot_info = match Self::get_slot_info(page_data, slot_id)? {
            Some(s) if s.length > 0 => s,
            _ => {
                return Err(OxidbError::NotFound(format!(
                    "Record at SlotId {} not found or already deleted",
                    slot_id.0
                )))
            }
        };

        // Mark slot as empty by setting its length to 0.
        // The actual data on disk is not wiped or moved in this simple version.
        slot_info.length = 0;
        // The offset might be kept or zeroed out, let's keep it for potential future compaction logic,
        // but it's effectively meaningless for a zero-length record.
        Self::set_slot_info(page_data, slot_id, slot_info)?;

        // Note: Free space pointer is not updated here, nor is num_records decremented.
        // Decrementing num_records could be complex if it's not the last slot.
        // For simplicity, num_records reflects the "high water mark" of slots used.
        // True compaction would change this.
        // TODO: Consider if num_records should be managed differently, e.g. count of occupied slots.
        // For now, get_num_records is more like "max_slot_id_used + 1".

        Ok(())
    }

    pub fn update_record(
        page_data: &mut [u8],
        slot_id: SlotId,
        new_data: &[u8],
    ) -> Result<(), OxidbError> {
        if new_data.is_empty() {
            return Err(OxidbError::InvalidInput {
                message: "New record data cannot be empty".to_string(),
            });
        }
        if new_data.len() > u16::MAX as usize {
            return Err(OxidbError::InvalidInput {
                message: "New record data too large for u16 length".to_string(),
            });
        }

        let num_records = Self::get_num_records(page_data)?;
        if slot_id.0 >= num_records {
            return Err(OxidbError::NotFound(format!("SlotId {} out of bounds", slot_id.0)));
        }

        let current_slot_info = match Self::get_slot_info(page_data, slot_id)? {
            Some(s) if s.length > 0 => s,
            _ => {
                return Err(OxidbError::NotFound(format!(
                    "Record at SlotId {} not found or has been deleted",
                    slot_id.0
                )))
            }
        };

        let new_data_len = new_data.len() as u16;

        if new_data_len <= current_slot_info.length {
            // New data is smaller or same size, update in place
            page_data[current_slot_info.offset as usize
                ..(current_slot_info.offset + new_data_len) as usize]
                .copy_from_slice(new_data);
            // If new data is smaller, the remaining part of the old record is now "dead space" within that slot's allocation.
            // Update slot length.
            let updated_slot_info = Slot { offset: current_slot_info.offset, length: new_data_len };
            Self::set_slot_info(page_data, slot_id, updated_slot_info)?;
            // Free space pointer does not change as we are using existing allocated space.
        } else {
            // New data is larger. Current simple model: return error.
            // A more complex version might try to "deallocate" the old record (like delete)
            // and then "insert" the new record if space allows.
            return Err(OxidbError::Storage(
                "Update failed: new data is larger than old data and in-place update is not supported for larger data.".to_string()
            ));
            // TODO: Advanced update: try to use free space if new_data > old_data.
            // This would involve:
            // 1. Checking if free_space_pointer + (new_data_len - current_slot_info.length) is within page_data.len()
            //    This is not quite right if data is not at the end of free_space_pointer.
            //    This requires actual free space management / compaction, which is not implemented.
            // For now, strictly no growing updates.
        }
        Ok(())
    }
}

// Test module is removed from here and moved to src/core/storage/engine/heap/tests/table_page_tests.rs
