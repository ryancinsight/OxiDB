use crate::core::common::types::PageId; // Use existing PageId
use serde::{Serialize, Deserialize};

// Define a standard page size.
pub const PAGE_SIZE: usize = 4096;

// Placeholder for different page types that might be used later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PageType {
    Data,       // General data page
    BTreeLeaf,  // B-Tree leaf node
    BTreeInternal, // B-Tree internal node
    Overflow,   // Overflow page for large records/values
    Metadata,   // Database metadata page
}

impl Default for PageType {
    fn default() -> Self {
        PageType::Data
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageHeader {
    pub page_id: PageId,
    pub page_type: PageType,
    pub free_space_offset: u16, // Offset to the start of free space
    pub slot_count: u16,        // Number of slots/records
    // pub lsn: u64,            // Log Sequence Number, for WAL later
    // pub checksum: u32,       // For page integrity, later
}

impl PageHeader {
    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        PageHeader {
            page_id,
            page_type,
            // Initially, free space starts after the header.
            // This will need to be updated if the header size is not fixed
            // or if using a more complex free space management.
            free_space_offset: std::mem::size_of::<PageHeader>() as u16,
            slot_count: 0,
            // lsn: 0,
            // checksum: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page {
    pub header: PageHeader,
    // The actual data content of the page.
    // Using Vec<u8> for easier serialization, as large arrays aren't directly supported by serde by default.
    // The actual fixed-size nature of a page will be managed by the buffer pool.
    pub data: Vec<u8>,
}

impl Page {
    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        let header = PageHeader::new(page_id, page_type);
        let data_size = PAGE_SIZE - std::mem::size_of::<PageHeader>();
        Page {
            header,
            data: vec![0; data_size],
        }
    }

    pub fn get_page_id(&self) -> PageId {
        self.header.page_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::common::types::PageId;

    #[test]
    fn test_new_page() {
        let page_id = PageId(1);
        let page = Page::new(page_id, PageType::Data);

        assert_eq!(page.header.page_id, page_id);
        assert_eq!(page.header.page_type, PageType::Data);
        assert_eq!(page.header.slot_count, 0);
        assert_eq!(page.header.free_space_offset, std::mem::size_of::<PageHeader>() as u16);
        assert_eq!(page.data.len(), PAGE_SIZE - std::mem::size_of::<PageHeader>());
    }

    #[test]
    fn test_page_header_serialization() {
        let header = PageHeader::new(PageId(1), PageType::BTreeLeaf);
        let serialized = serde_json::to_string(&header).unwrap();
        // Example: {"page_id":1,"page_type":"BTreeLeaf","free_space_offset":...,"slot_count":0}
        // The exact free_space_offset depends on the size of PageHeader itself after serialization,
        // or its fixed compile-time size if not dynamically calculated for this field.
        // For this test, let's just ensure it serializes and deserializes.
        let deserialized: PageHeader = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, header);
    }

    #[test]
    fn test_page_serialization() {
        let page = Page::new(PageId(2), PageType::Data);
        // Modify some data to make the test more robust
        // let mut page_mut = page.clone(); // Need to operate on a mutable copy if data is to be changed
        // page_mut.data[0] = 1;
        // page_mut.data[1] = 2;
        // let serialized = serde_json::to_string(&page_mut).unwrap();

        let serialized = serde_json::to_string(&page).unwrap();
        let deserialized: Page = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.header, page.header);
        // Comparing Vec<u8> directly works as Vec<T> implements PartialEq<Vec<U>> if T: PartialEq<U>.
        assert_eq!(deserialized.data, page.data);
    }
}
