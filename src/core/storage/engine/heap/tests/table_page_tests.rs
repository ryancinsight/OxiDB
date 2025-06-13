// src/core/storage/engine/heap/tests/table_page_tests.rs

#![cfg(test)]

use crate::core::common::error::OxidbError;
use crate::core::common::types::ids::SlotId;
use crate::core::storage::engine::heap::table_page::{
    TablePage, Slot, SLOTS_ARRAY_DATA_OFFSET, // Make consts from TablePage pub(super) or pub if needed by tests directly
                                              // For now, assuming they might be needed for complex assertions or setup.
                                              // If tests only use public TablePage API, these are not needed.
};


// Helper function to create test page data, adjusted for new location
fn create_test_page_data() -> Vec<u8> {
    // Accessing PAGE_DATA_SIZE from TablePage. It needs to be pub or pub(crate).
    // For now, let's assume it will be made accessible or we redefine it here for tests.
    // Let's try to make it accessible first. If not, define it here.
    // If TablePage::PAGE_DATA_SIZE is not pub, this will fail.
    // It was `super::TablePage::PAGE_DATA_SIZE` when tests were a child mod.
    // Now it's `TablePage::PAGE_DATA_SIZE` if TablePage is imported.
    // The const PAGE_DATA_SIZE is defined within `impl TablePage`, so it's not directly accessible as TablePage::PAGE_DATA_SIZE.
    // It should be a `pub const` at the module level or on the struct itself if used externally.
    // Now using TablePage::PAGE_DATA_AREA_SIZE.

    let mut data = vec![0u8; TablePage::PAGE_DATA_AREA_SIZE];
    TablePage::init(&mut data).expect("Failed to init test page data");
    data
}

#[test]
fn test_init_table_page() {
    let mut page_data = vec![0u8; TablePage::PAGE_DATA_AREA_SIZE];
    TablePage::init(&mut page_data).unwrap();

    // Accessing private methods like get_num_records directly is not possible.
    // Tests should ideally only use the public API of TablePage.
    // For unit testing internal state, we might need to expose helpers or test effects.
    // Assuming these get_* methods will be made pub(super) or pub(crate) for testing.
    // If not, these assertions need to be rethought.
    // For now, let's assume they are accessible for the sake of moving code.
    assert_eq!(TablePage::get_num_records(&page_data).unwrap(), 0);
    assert_eq!(TablePage::get_free_space_pointer(&page_data).unwrap(), SLOTS_ARRAY_DATA_OFFSET as u16);
}

#[test]
fn test_insert_and_get_record() {
    let mut page_data = create_test_page_data();
    let record_data1 = b"hello world";
    let record_data2 = b"another record";

    let slot_id1 = TablePage::insert_record(&mut page_data, record_data1).expect("Insert 1 failed");
    assert_eq!(slot_id1, SlotId(0));
    assert_eq!(TablePage::get_num_records(&page_data).unwrap(), 1);

    let retrieved1 = TablePage::get_record(&page_data, slot_id1).unwrap().unwrap();
    assert_eq!(retrieved1, record_data1);

    let expected_data_offset1 = (SLOTS_ARRAY_DATA_OFFSET + Slot::SERIALIZED_SIZE) as u16;
    let s_info1 = TablePage::get_slot_info(&page_data, slot_id1).unwrap().unwrap();
    assert_eq!(s_info1.offset, expected_data_offset1);
    assert_eq!(s_info1.length, record_data1.len() as u16);

    let expected_fsp1 = expected_data_offset1 + record_data1.len() as u16;
    assert_eq!(TablePage::get_free_space_pointer(&page_data).unwrap(), expected_fsp1);

    let slot_id2 = TablePage::insert_record(&mut page_data, record_data2).expect("Insert 2 failed");
    assert_eq!(slot_id2, SlotId(1));
    assert_eq!(TablePage::get_num_records(&page_data).unwrap(), 2);

    let retrieved2 = TablePage::get_record(&page_data, slot_id2).unwrap().unwrap();
    assert_eq!(retrieved2, record_data2);

    let expected_slot_array_end_for_2_slots = (SLOTS_ARRAY_DATA_OFFSET + 2 * Slot::SERIALIZED_SIZE) as u16;
    let expected_data_offset2 = expected_slot_array_end_for_2_slots.max(expected_fsp1);

    let s_info2 = TablePage::get_slot_info(&page_data, slot_id2).unwrap().unwrap();
    assert_eq!(s_info2.offset, expected_data_offset2);
    assert_eq!(s_info2.length, record_data2.len() as u16);

    let expected_fsp2 = expected_data_offset2 + record_data2.len() as u16;
    assert_eq!(TablePage::get_free_space_pointer(&page_data).unwrap(), expected_fsp2);
}

#[test]
fn test_page_full_on_insert_data() {
    let mut page_data = create_test_page_data();

    let space_for_first_slot_metadata = Slot::SERIALIZED_SIZE;
    let data_start_offset = SLOTS_ARRAY_DATA_OFFSET + space_for_first_slot_metadata;
    let available_data_space = TablePage::PAGE_DATA_AREA_SIZE - data_start_offset;

    let large_data = vec![0u8; available_data_space + 1];

    let result = TablePage::insert_record(&mut page_data, &large_data);
    assert!(matches!(result, Err(OxidbError::Storage(msg)) if msg.contains("no space for record data (after considering slot array)")));
}

#[test]
fn test_page_full_on_insert_slot_metadata() {
    let mut page_data = create_test_page_data();
    let small_data = [0u8; 1];
    let mut successful_inserts = 0;

    for i in 0..(TablePage::PAGE_DATA_AREA_SIZE) { // Use the const from TablePage
        match TablePage::insert_record(&mut page_data, &small_data) {
            Ok(_) => {
                successful_inserts += 1;
            }
            Err(e) => {
                assert!(matches!(e, OxidbError::Storage(ref msg) if msg.contains("Page full")),
                    "Test insert {}: Expected 'Page full' error, got {:?}", i, e);
                return;
            }
        }
    }
    panic!("Page did not fill up after {} successful inserts. Loop limit was {}.",
           successful_inserts, TablePage::PAGE_DATA_AREA_SIZE); // Use the const here too
}

#[test]
fn test_delete_record() {
    let mut page_data = create_test_page_data();
    let record_data1 = b"record1";
    let slot_id1 = TablePage::insert_record(&mut page_data, record_data1).unwrap();

    let record_data2 = b"record2_long";
    let slot_id2 = TablePage::insert_record(&mut page_data, record_data2).unwrap();

    assert_eq!(TablePage::get_num_records(&page_data).unwrap(), 2);
    assert!(TablePage::get_record(&page_data, slot_id1).unwrap().is_some());

    TablePage::delete_record(&mut page_data, slot_id1).unwrap();

    let slot_info1_after_delete = TablePage::get_slot_info(&page_data, slot_id1).unwrap().unwrap();
    assert_eq!(slot_info1_after_delete.length, 0);
    assert_eq!(TablePage::get_num_records(&page_data).unwrap(), 2);
    assert!(TablePage::get_record(&page_data, slot_id1).unwrap().is_none());

    assert!(TablePage::get_record(&page_data, slot_id2).unwrap().is_some());
    let s_info2 = TablePage::get_slot_info(&page_data, slot_id2).unwrap().unwrap();
    assert_eq!(s_info2.length, record_data2.len() as u16);

    let res_del_again = TablePage::delete_record(&mut page_data, slot_id1);
    assert!(matches!(res_del_again, Err(OxidbError::NotFound { .. })));

    let res_del_non_exist = TablePage::delete_record(&mut page_data, SlotId(99));
    assert!(matches!(res_del_non_exist, Err(OxidbError::NotFound { .. })));
}

#[test]
fn test_insert_reuses_deleted_slot() {
    let mut page_data = create_test_page_data();
    let record_data1 = b"data_one";
    let record_data2 = b"data_two";
    let record_data3 = b"data_three_new";

    let slot_id1 = TablePage::insert_record(&mut page_data, record_data1).unwrap();
    TablePage::insert_record(&mut page_data, record_data2).unwrap();

    let original_fsp = TablePage::get_free_space_pointer(&page_data).unwrap();

    TablePage::delete_record(&mut page_data, slot_id1).unwrap();
    assert_eq!(TablePage::get_slot_info(&page_data, slot_id1).unwrap().unwrap().length, 0);

    let slot_id3 = TablePage::insert_record(&mut page_data, record_data3).unwrap();
    assert_eq!(slot_id3, slot_id1);

    assert_eq!(TablePage::get_num_records(&page_data).unwrap(), 2);

    let retrieved3 = TablePage::get_record(&page_data, slot_id3).unwrap().unwrap();
    assert_eq!(retrieved3, record_data3);

    let s_info3 = TablePage::get_slot_info(&page_data, slot_id3).unwrap().unwrap();
    assert_eq!(s_info3.offset, original_fsp);
    assert_eq!(s_info3.length, record_data3.len() as u16);

    assert_eq!(TablePage::get_free_space_pointer(&page_data).unwrap(), original_fsp + record_data3.len() as u16);
}

#[test]
fn test_update_record_same_size() {
    let mut page_data = create_test_page_data();
    let old_data = b"original";
    let new_data = b"updated!";

    let slot_id = TablePage::insert_record(&mut page_data, old_data).unwrap();
    let original_slot_info = TablePage::get_slot_info(&page_data, slot_id).unwrap().unwrap();

    TablePage::update_record(&mut page_data, slot_id, new_data).unwrap();

    let retrieved = TablePage::get_record(&page_data, slot_id).unwrap().unwrap();
    assert_eq!(retrieved, new_data);

    let updated_slot_info = TablePage::get_slot_info(&page_data, slot_id).unwrap().unwrap();
    assert_eq!(updated_slot_info.offset, original_slot_info.offset);
    assert_eq!(updated_slot_info.length, new_data.len() as u16);
}

#[test]
fn test_update_record_smaller_size() {
    let mut page_data = create_test_page_data();
    let old_data = b"long_original_data";
    let new_data = b"short";

    let slot_id = TablePage::insert_record(&mut page_data, old_data).unwrap();
    let original_slot_info = TablePage::get_slot_info(&page_data, slot_id).unwrap().unwrap();

    TablePage::update_record(&mut page_data, slot_id, new_data).unwrap();

    let retrieved = TablePage::get_record(&page_data, slot_id).unwrap().unwrap();
    assert_eq!(retrieved, new_data);

    let updated_slot_info = TablePage::get_slot_info(&page_data, slot_id).unwrap().unwrap();
    assert_eq!(updated_slot_info.offset, original_slot_info.offset);
    assert_eq!(updated_slot_info.length, new_data.len() as u16);
}

#[test]
fn test_update_record_larger_size_error() {
    let mut page_data = create_test_page_data();
    let old_data = b"short";
    let new_data = b"much_longer_updated_data";

    let slot_id = TablePage::insert_record(&mut page_data, old_data).unwrap();
    let result = TablePage::update_record(&mut page_data, slot_id, new_data);

    assert!(matches!(result, Err(OxidbError::Storage(msg)) if msg.contains("new data is larger")));

    let retrieved = TablePage::get_record(&page_data, slot_id).unwrap().unwrap();
    assert_eq!(retrieved, old_data);
}

#[test]
fn test_get_invalid_slot() {
    let page_data = create_test_page_data();
    let result = TablePage::get_record(&page_data, SlotId(0));
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    let result_info = TablePage::get_slot_info(&page_data, SlotId(0));
     assert!(result_info.is_ok());
    assert!(result_info.unwrap().is_none());
}

#[test]
fn test_insert_empty_record_error() {
    let mut page_data = create_test_page_data();
    let empty_data = b"";
    let result = TablePage::insert_record(&mut page_data, empty_data);
    assert!(matches!(result, Err(OxidbError::InvalidInput { .. })));
}

// Private helper methods in TablePage like get_num_records, set_num_records,
// get_free_space_pointer, set_free_space_pointer, get_slot_info, set_slot_info
// are not directly testable unless made pub(crate) or pub.
// Their correctness is indirectly tested via the public API (insert, get, delete, update).
// If more granular tests for these helpers are needed, their visibility would need to change,
// or they'd be tested as part of a larger unit that uses them.
// For now, assuming public API tests are sufficient.

// Note on SLOTS_ARRAY_DATA_OFFSET and Slot::SERIALIZED_SIZE:
// These are constants from table_page.rs.
// If they were private, tests needing them for assertions would require them to be pub or pub(crate).
// Currently, they are module-level consts or associated consts, so they are accessible if table_page module is imported.
// `use crate::core::storage::engine::heap::table_page::{TablePage, Slot, SlotId, SLOTS_ARRAY_DATA_OFFSET};`
// This makes SLOTS_ARRAY_DATA_OFFSET available. Slot::SERIALIZED_SIZE is an associated const, also available.
// TablePage::PAGE_DATA_SIZE needs to be a pub const on TablePage struct or pub const in its module.
// Redefined TEST_PAGE_DATA_SIZE locally for now.
