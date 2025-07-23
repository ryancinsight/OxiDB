use crate::core::common::error::OxidbError;
use crate::core::common::types::Lsn; // Corrected Lsn import path
use crate::core::common::types::PageId;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::convert::TryFrom;
use std::io::Cursor;

// Define a standard page size.
pub const PAGE_SIZE: usize = 4096;
// Define the size of the PageHeader when serialized
// PageId (u64: 8) + PageType (u8: 1) + Lsn (u64: 8) + flags (u8: 1) = 18 bytes
pub const PAGE_HEADER_SIZE: usize = 18;

// Placeholder for different page types that might be used later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum PageType {
    Meta = 0,
    Data = 1,
    Index = 2,
    // BTreeLeaf, // Keeping original values for now, but task asks for Meta, Data, Index, Unknown
    // BTreeInternal,
    // Overflow,
    // Metadata, // This is Meta now
    #[default]
    Unknown = 255, // For invalid/uninitialized page types
}

impl TryFrom<u8> for PageType {
    type Error = OxidbError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Meta),
            1 => Ok(Self::Data),
            2 => Ok(Self::Index),
            255 => Ok(Self::Unknown),
            _ => Err(OxidbError::Deserialization(format!("Invalid PageType value: {value}"))),
        }
    }
}

impl From<PageType> for u8 {
    fn from(page_type: PageType) -> Self {
        page_type as Self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageHeader {
    pub page_id: PageId,
    pub page_type: PageType,
    pub lsn: Lsn,  // Log Sequence Number
    pub flags: u8, // e.g., is_dirty, is_pinned
}

impl PageHeader {
    #[must_use]
    pub const fn new(page_id: PageId, page_type: PageType) -> Self {
        Self {
            page_id,
            page_type,
            lsn: 0, // Lsn is u64, default to 0
            flags: 0,
        }
    }

    pub fn serialize(&self, buffer: &mut [u8]) -> Result<(), OxidbError> {
        if buffer.len() < PAGE_HEADER_SIZE {
            return Err(OxidbError::Serialization("Buffer too small for PageHeader".to_string()));
        }

        let mut cursor = Cursor::new(buffer);
        cursor.write_u64::<LittleEndian>(self.page_id.0)?;
        cursor.write_u8(self.page_type as u8)?;
        cursor.write_u64::<LittleEndian>(self.lsn)?; // Lsn is u64
        cursor.write_u8(self.flags)?;

        Ok(())
    }

    pub fn deserialize(buffer: &[u8]) -> Result<Self, OxidbError> {
        if buffer.len() < PAGE_HEADER_SIZE {
            return Err(OxidbError::Deserialization("Buffer too small for PageHeader".to_string()));
        }

        let mut cursor = Cursor::new(buffer);
        let page_id = PageId(cursor.read_u64::<LittleEndian>()?);
        let page_type_u8 = cursor.read_u8()?;
        let page_type = PageType::try_from(page_type_u8)?;
        let lsn = cursor.read_u64::<LittleEndian>()?; // Lsn is u64
        let flags = cursor.read_u8()?;

        Ok(Self { page_id, page_type, lsn, flags })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page {
    pub header: PageHeader,
    pub data: Vec<u8>,
}

impl Page {
    #[must_use]
    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        let header = PageHeader::new(page_id, page_type);
        // Data initialized to zeros, size of PAGE_SIZE - PAGE_HEADER_SIZE
        let data_size = PAGE_SIZE - PAGE_HEADER_SIZE;
        Self { header, data: vec![0; data_size] }
    }

    #[must_use]
    pub const fn get_page_id(&self) -> PageId {
        self.header.page_id
    }

    pub fn serialize(&self) -> Result<Vec<u8>, OxidbError> {
        let mut buffer = vec![0u8; PAGE_SIZE];

        // Serialize header into the beginning of the buffer
        self.header.serialize(&mut buffer[0..PAGE_HEADER_SIZE])?;

        // Copy page data into the buffer after the header
        let data_start_offset = PAGE_HEADER_SIZE;
        let data_end_offset = data_start_offset.saturating_add(self.data.len());

        if data_end_offset > PAGE_SIZE {
            // This case should ideally not happen if page.data is sized correctly upon creation/modification
            return Err(OxidbError::Serialization(
                "Page data exceeds available page size".to_string(),
            ));
        }
        buffer[data_start_offset..data_end_offset].copy_from_slice(&self.data);

        // The rest of the buffer (if any, up to PAGE_SIZE) remains as padding (e.g. zeros from vec init)
        // This is important if self.data.len() < PAGE_SIZE - PAGE_HEADER_SIZE

        Ok(buffer)
    }

    pub fn deserialize(buffer: &[u8]) -> Result<Self, OxidbError> {
        if buffer.len() != PAGE_SIZE {
            return Err(OxidbError::Deserialization(format!(
                "Buffer size {} does not match configured PAGE_SIZE {}",
                buffer.len(),
                PAGE_SIZE
            )));
        }

        // Deserialize header from the beginning of the buffer
        let header = PageHeader::deserialize(&buffer[0..PAGE_HEADER_SIZE])?;

        // Copy the remaining part of the buffer into the data field
        // The data field should contain data up to PAGE_SIZE - PAGE_HEADER_SIZE
        let data_size = PAGE_SIZE - PAGE_HEADER_SIZE;
        let mut data = vec![0u8; data_size];
        data.copy_from_slice(&buffer[PAGE_HEADER_SIZE..PAGE_SIZE]);

        Ok(Self { header, data })
    }

    /// Get the LSN (Log Sequence Number) of this page
    #[must_use]
    pub const fn get_lsn(&self) -> Lsn {
        self.header.lsn
    }

    /// Set the LSN (Log Sequence Number) of this page
    pub fn set_lsn(&mut self, lsn: Lsn) {
        self.header.lsn = lsn;
    }

    /// Apply an update operation to the page
    /// This is a simplified implementation for recovery purposes
    pub fn apply_update(&mut self, after_image: &[u8]) -> Result<(), OxidbError> {
        if after_image.len() > self.data.len() {
            return Err(OxidbError::InvalidInput {
                message: "Update data exceeds page capacity".to_string(),
            });
        }

        // For simplicity, we'll replace the beginning of the page data with the after_image
        // In a real implementation, this would be more sophisticated based on the specific
        // storage format and the nature of the update
        self.data[..after_image.len()].copy_from_slice(after_image);

        Ok(())
    }

    /// Apply an insert operation to the page
    /// This is a simplified implementation for recovery purposes
    pub fn apply_insert(&mut self, data: &[u8]) -> Result<(), OxidbError> {
        if data.len() > self.data.len() {
            return Err(OxidbError::InvalidInput {
                message: "Insert data exceeds page capacity".to_string(),
            });
        }

        // For simplicity, we'll append the data to the page
        // In a real implementation, this would involve proper slot management
        // and free space tracking
        let mut insert_offset = 0;

        // Find the first available space (simplified approach)
        while insert_offset + data.len() <= self.data.len() {
            if self.data[insert_offset..insert_offset + data.len()].iter().all(|&b| b == 0) {
                self.data[insert_offset..insert_offset + data.len()].copy_from_slice(data);
                return Ok(());
            }
            insert_offset += 1;
        }

        Err(OxidbError::InvalidInput { message: "No space available for insert".to_string() })
    }

    /// Apply a delete operation to the page
    /// This is a simplified implementation for recovery purposes
    pub fn apply_delete(&mut self) -> Result<(), OxidbError> {
        // For simplicity, we'll zero out the first non-zero data
        // In a real implementation, this would involve proper slot management
        // and record identification
        for byte in &mut self.data {
            if *byte != 0 {
                *byte = 0;
                return Ok(());
            }
        }

        // If no non-zero data found, the delete is a no-op
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // use crate::core::common::types::PageId; // Already imported via super::*

    #[test]
    fn test_new_page() {
        let page_id = PageId(1);
        let page = Page::new(page_id, PageType::Data);

        assert_eq!(page.header.page_id, page_id);
        assert_eq!(page.header.page_type, PageType::Data);
        assert_eq!(page.header.lsn, 0); // Lsn is u64
        assert_eq!(page.header.flags, 0);
        assert_eq!(page.data.len(), PAGE_SIZE - PAGE_HEADER_SIZE);
    }

    #[test]
    fn test_page_header_serialization_deserialization() {
        let page_types = [PageType::Meta, PageType::Data, PageType::Index, PageType::Unknown];

        for &page_type in page_types.iter() {
            let header = PageHeader {
                page_id: PageId(123),
                page_type,
                lsn: 456, // Lsn is u64
                flags: 0b10101010,
            };

            let mut buffer = vec![0u8; PAGE_HEADER_SIZE];
            header.serialize(&mut buffer).expect("Serialization failed in test");

            let deserialized_header =
                PageHeader::deserialize(&buffer).expect("Deserialization failed in test");
            assert_eq!(header, deserialized_header, "Mismatch for PageType::{:?}", page_type);
        }
    }

    #[test]
    fn test_page_header_serialize_buffer_too_small() {
        let header = PageHeader::new(PageId(1), PageType::Data);
        let mut buffer = vec![0u8; PAGE_HEADER_SIZE - 1];
        let result = header.serialize(&mut buffer);
        assert!(matches!(result, Err(OxidbError::Serialization(_))));
    }

    #[test]
    fn test_page_header_deserialize_buffer_too_small() {
        let buffer = vec![0u8; PAGE_HEADER_SIZE - 1];
        let result = PageHeader::deserialize(&buffer);
        assert!(matches!(result, Err(OxidbError::Deserialization(_))));
    }

    #[test]
    fn test_page_header_deserialize_invalid_page_type() {
        let mut buffer = vec![0u8; PAGE_HEADER_SIZE];
        // Manually construct a header buffer with an invalid page type byte
        let page_id = PageId(123);
        let invalid_page_type_byte = 99u8; // Assuming 99 is not a valid PageType u8 value
        let lsn: Lsn = 456; // Lsn is u64
        let flags = 0b10101010;

        let mut cursor = Cursor::new(buffer.as_mut_slice());
        cursor.write_u64::<LittleEndian>(page_id.0).expect("Failed to write page_id to cursor");
        cursor
            .write_u8(invalid_page_type_byte)
            .expect("Failed to write invalid_page_type_byte to cursor");
        cursor.write_u64::<LittleEndian>(lsn).expect("Failed to write lsn to cursor"); // Lsn is u64
        cursor.write_u8(flags).expect("Failed to write flags to cursor");

        let result = PageHeader::deserialize(&buffer);
        assert!(matches!(result, Err(OxidbError::Deserialization(_))));
        if let Err(OxidbError::Deserialization(msg)) = result {
            assert!(msg.contains("Invalid PageType value"));
        } else {
            panic!("Expected Deserialization error for invalid page type");
        }
    }

    #[test]
    fn test_page_serialization_deserialization() {
        let page_types_to_test = [PageType::Meta, PageType::Data, PageType::Index];

        for &page_type in page_types_to_test.iter() {
            // Test with initially zeroed data (as done by Page::new)
            let page_id_zeroed = PageId(456);
            let page_zeroed = Page::new(page_id_zeroed, page_type);

            let serialized_page_zeroed =
                page_zeroed.serialize().expect("Serialization of zeroed page failed");
            assert_eq!(serialized_page_zeroed.len(), PAGE_SIZE);

            let deserialized_page_zeroed = Page::deserialize(&serialized_page_zeroed)
                .expect("Deserialization of zeroed page failed");
            assert_eq!(
                page_zeroed.header, deserialized_page_zeroed.header,
                "Header mismatch for zeroed PageType::{:?}",
                page_type
            );
            assert_eq!(
                page_zeroed.data, deserialized_page_zeroed.data,
                "Data mismatch for zeroed PageType::{:?}",
                page_type
            );
            assert_eq!(deserialized_page_zeroed.data.len(), PAGE_SIZE - PAGE_HEADER_SIZE);

            // Test with fully populated data
            let page_id_populated = PageId(789);
            let mut page_populated = Page::new(page_id_populated, page_type);
            for i in 0..page_populated.data.len() {
                page_populated.data[i] = (i % 256) as u8;
            }
            // Modify header fields to be non-default for better testing
            page_populated.header.lsn = 101112; // Lsn is u64
            page_populated.header.flags = 0xAA;

            let serialized_page_populated =
                page_populated.serialize().expect("Serialization of populated page failed");
            assert_eq!(serialized_page_populated.len(), PAGE_SIZE);

            let deserialized_page_populated = Page::deserialize(&serialized_page_populated)
                .expect("Deserialization of populated page failed");
            assert_eq!(
                page_populated.header, deserialized_page_populated.header,
                "Header mismatch for populated PageType::{:?}",
                page_type
            );
            assert_eq!(
                page_populated.data, deserialized_page_populated.data,
                "Data mismatch for populated PageType::{:?}",
                page_type
            );
            assert_eq!(deserialized_page_populated.data.len(), PAGE_SIZE - PAGE_HEADER_SIZE);
        }
    }

    #[test]
    fn test_page_deserialize_buffer_too_small() {
        let buffer = vec![0u8; PAGE_SIZE - 1];
        let result = Page::deserialize(&buffer);
        assert!(matches!(result, Err(OxidbError::Deserialization(_))));
        if let Err(OxidbError::Deserialization(msg)) = result {
            assert!(msg.contains("does not match configured PAGE_SIZE"));
        } else {
            panic!("Expected Deserialization error for small buffer");
        }
    }

    #[test]
    fn test_page_deserialize_buffer_too_large() {
        let buffer = vec![0u8; PAGE_SIZE + 1];
        let result = Page::deserialize(&buffer);
        assert!(matches!(result, Err(OxidbError::Deserialization(_))));
        if let Err(OxidbError::Deserialization(msg)) = result {
            assert!(msg.contains("does not match configured PAGE_SIZE"));
        } else {
            panic!("Expected Deserialization error for large buffer");
        }
    }

    // #[test]
    // fn test_page_header_serialization() {
    //     let header = PageHeader::new(PageId(1), PageType::BTreeLeaf);
    //     let serialized = serde_json::to_string(&header).unwrap();
    //     // Example: {"page_id":1,"page_type":"BTreeLeaf","free_space_offset":...,"slot_count":0}
    //     // The exact free_space_offset depends on the size of PageHeader itself after serialization,
    //     // or its fixed compile-time size if not dynamically calculated for this field.
    //     // For this test, let's just ensure it serializes and deserializes.
    //     let deserialized: PageHeader = serde_json::from_str(&serialized).unwrap();
    //     assert_eq!(deserialized, header);
    // }

    // #[test]
    // fn test_page_serialization() {
    //     let page = Page::new(PageId(2), PageType::Data);
    //     // Modify some data to make the test more robust
    //     // let mut page_mut = page.clone(); // Need to operate on a mutable copy if data is to be changed
    //     // page_mut.data[0] = 1;
    //     // page_mut.data[1] = 2;
    //     // let serialized = serde_json::to_string(&page_mut).unwrap();

    //     let serialized = serde_json::to_string(&page).unwrap();
    //     let deserialized: Page = serde_json::from_str(&serialized).unwrap();
    //     assert_eq!(deserialized.header, page.header);
    //     // Comparing Vec<u8> directly works as Vec<T> implements PartialEq<Vec<U>> if T: PartialEq<U>.
    //     assert_eq!(deserialized.data, page.data);
    // }
}
