use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write, ErrorKind, Error as IoError};
use std::path::PathBuf;
use crate::core::common::error::OxidbError;
use crate::core::common::types::PageId; // Assuming PageId is u64 from common::types::ids
use crate::core::storage::engine::page::PAGE_SIZE;

pub struct DiskManager {
    db_file: File,
    db_path: PathBuf,
    next_page_id: PageId, // To keep track of the next page to allocate
}

impl DiskManager {
    pub fn open(db_path: PathBuf) -> Result<Self, OxidbError> {
        let is_new_db = !db_path.exists();

        let db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&db_path)
            .map_err(|e| OxidbError::Io(IoError::new(ErrorKind::Other, format!("Failed to open database file '{}': {}", db_path.display(), e))))?;

        let next_page_id = if is_new_db {
            PageId(0)
        } else {
            let metadata = db_file.metadata()
                .map_err(|e| OxidbError::Io(IoError::new(ErrorKind::Other, format!("Failed to read metadata for database file '{}': {}", db_path.display(), e))))?;
            PageId((metadata.len() / PAGE_SIZE as u64) as u64) // PageId is u64
        };

        Ok(Self {
            db_file,
            db_path,
            next_page_id,
        })
    }

    pub fn write_page(&mut self, page_id: PageId, page_data: &[u8]) -> Result<(), OxidbError> {
        if page_data.len() != PAGE_SIZE {
            return Err(OxidbError::Io(IoError::new(ErrorKind::InvalidInput, format!(
                "Page data length mismatch: expected {}, got {}",
                PAGE_SIZE,
                page_data.len()
            ))));
        }

        let offset = page_id.0 * PAGE_SIZE as u64;
        self.db_file.seek(SeekFrom::Start(offset))
            .map_err(|e| OxidbError::Io(IoError::new(ErrorKind::Other, format!("Failed to seek to page {} offset {}: {}", page_id.0, offset, e))))?;

        self.db_file.write_all(page_data)
            .map_err(|e| OxidbError::Io(IoError::new(ErrorKind::Other, format!("Failed to write page {}: {}", page_id.0, e))))?;

        Ok(())
    }

    pub fn read_page(&mut self, page_id: PageId, page_data_buf: &mut [u8]) -> Result<(), OxidbError> {
        if page_data_buf.len() != PAGE_SIZE {
            return Err(OxidbError::Io(IoError::new(ErrorKind::InvalidInput, format!(
                "Page data buffer length mismatch: expected {}, got {}",
                PAGE_SIZE,
                page_data_buf.len()
            ))));
        }

        if page_id.0 >= self.next_page_id.0 {
            return Err(OxidbError::Io(IoError::new(ErrorKind::NotFound, format!(
                "Page ID {} out of bounds (next_page_id is {})",
                page_id.0, self.next_page_id.0
            ))));
        }

        let offset = page_id.0 * PAGE_SIZE as u64;
        self.db_file.seek(SeekFrom::Start(offset))
            .map_err(|e| OxidbError::Io(IoError::new(ErrorKind::Other, format!("Failed to seek to page {} offset {}: {}", page_id.0, offset, e))))?;

        match self.db_file.read_exact(page_data_buf) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                Err(OxidbError::Io(IoError::new(ErrorKind::UnexpectedEof, format!(
                    "Unexpected EOF when reading page {}: not enough bytes", page_id.0
                ))))
            }
            Err(e) => Err(OxidbError::Io(IoError::new(ErrorKind::Other, format!("Failed to read page {}: {}", page_id.0, e)))),
        }
    }

    pub fn allocate_page(&mut self) -> Result<PageId, OxidbError> {
        let new_page_id = self.next_page_id;
        self.next_page_id = PageId(self.next_page_id.0 + 1);

        // Create a zeroed buffer for the new page
        let zeroed_page_data = vec![0u8; PAGE_SIZE];

        // Write the zeroed page to disk to extend the file and initialize the page
        // This also implicitly updates the file length if it was shorter.
        self.write_page(new_page_id, &zeroed_page_data)?;
        // Note: If write_page itself updates some internal state of DiskManager
        // that might conflict with next_page_id increment, that needs care.
        // Here, write_page doesn't interact with next_page_id, so it's fine.

        Ok(new_page_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use crate::core::storage::engine::page::PAGE_SIZE; // Already imported at module level but good for clarity

    fn create_temp_db_file() -> NamedTempFile {
        NamedTempFile::new().expect("Failed to create temp file")
    }

    #[test]
    fn test_open_new_db() {
        let temp_file = create_temp_db_file();
        let db_path = temp_file.path().to_path_buf();
        // Ensure file is deleted before DiskManager tries to manage it exclusively
        drop(temp_file);

        let dm = DiskManager::open(db_path.clone()).unwrap();
        assert_eq!(dm.next_page_id, PageId(0));
        // Verify file exists after open
        assert!(db_path.exists());
        std::fs::remove_file(db_path).unwrap(); // Clean up
    }

    #[test]
    fn test_open_existing_db() {
        let temp_file = create_temp_db_file();
        let db_path = temp_file.path().to_path_buf();

        // Pre-populate the file to simulate existing pages
        {
            let mut file = OpenOptions::new().write(true).open(&db_path).unwrap();
            file.set_len(PAGE_SIZE as u64 * 3).unwrap(); // Simulate 3 pages
        } // drop file to release lock before DiskManager::open

        let dm = DiskManager::open(db_path.clone()).unwrap();
        assert_eq!(dm.next_page_id, PageId(3));
        std::fs::remove_file(db_path).unwrap(); // Clean up
    }

    #[test]
    fn test_allocate_and_write_read_page() {
        let temp_file = create_temp_db_file();
        let db_path = temp_file.path().to_path_buf();
        drop(temp_file);

        let mut dm = DiskManager::open(db_path.clone()).unwrap();

        // Allocate first page
        let page_id0 = dm.allocate_page().unwrap();
        assert_eq!(page_id0, PageId(0));
        assert_eq!(dm.next_page_id, PageId(1));

        // Write to first page
        let mut write_data0 = vec![0u8; PAGE_SIZE];
        for i in 0..PAGE_SIZE {
            write_data0[i] = (i % 256) as u8;
        }
        dm.write_page(page_id0, &write_data0).unwrap();

        // Read back first page
        let mut read_buf0 = vec![0u8; PAGE_SIZE];
        dm.read_page(page_id0, &mut read_buf0).unwrap();
        assert_eq!(write_data0, read_buf0);

        // Allocate second page
        let page_id1 = dm.allocate_page().unwrap();
        assert_eq!(page_id1, PageId(1));
        assert_eq!(dm.next_page_id, PageId(2));

        // Write to second page
        let mut write_data1 = vec![0u8; PAGE_SIZE];
        for i in 0..PAGE_SIZE {
            write_data1[i] = ((PAGE_SIZE - 1 - i) % 256) as u8;
        }
        dm.write_page(page_id1, &write_data1).unwrap();

        // Read back second page
        let mut read_buf1 = vec![0u8; PAGE_SIZE];
        dm.read_page(page_id1, &mut read_buf1).unwrap();
        assert_eq!(write_data1, read_buf1);

        // Verify file size
        let metadata = dm.db_file.metadata().unwrap();
        assert_eq!(metadata.len(), PAGE_SIZE as u64 * 2);

        std::fs::remove_file(db_path).unwrap(); // Clean up
    }

    #[test]
    fn test_read_out_of_bounds_page() {
        let temp_file = create_temp_db_file();
        let db_path = temp_file.path().to_path_buf();
        drop(temp_file);

        let mut dm = DiskManager::open(db_path.clone()).unwrap();
        let mut read_buf = vec![0u8; PAGE_SIZE];

        // Try to read page 0 before allocation
        let result = dm.read_page(PageId(0), &mut read_buf);
        assert!(matches!(result, Err(OxidbError::Io(_))));
        if let Err(OxidbError::Io(err)) = result {
            assert!(err.to_string().contains("out of bounds"));
        } else {
            panic!("Expected IO error for out of bounds read");
        }

        // Allocate a page
        dm.allocate_page().unwrap(); // PageId(0) allocated

        // Try to read PageId(1) which is not yet allocated but next_page_id is 1
        let result_next = dm.read_page(PageId(1), &mut read_buf);
         assert!(matches!(result_next, Err(OxidbError::Io(_))));
         if let Err(OxidbError::Io(err)) = result_next {
            assert!(err.to_string().contains("out of bounds"));
        } else {
            panic!("Expected IO error for out of bounds read of next_page_id");
        }
        std::fs::remove_file(db_path).unwrap(); // Clean up
    }

    #[test]
    fn test_write_page_invalid_data_length() {
        let temp_file = create_temp_db_file();
        let db_path = temp_file.path().to_path_buf();
        drop(temp_file);

        let mut dm = DiskManager::open(db_path.clone()).unwrap();
        let page_id = dm.allocate_page().unwrap();

        let short_data = vec![0u8; PAGE_SIZE - 1];
        let result = dm.write_page(page_id, &short_data);
        assert!(matches!(result, Err(OxidbError::Io(_))));
        if let Err(OxidbError::Io(err)) = result {
            assert!(err.to_string().contains("Page data length mismatch"));
        } else {
            panic!("Expected IO error for short data write");
        }
        std::fs::remove_file(db_path).unwrap(); // Clean up
    }

    #[test]
    fn test_read_page_invalid_buffer_length() {
        let temp_file = create_temp_db_file();
        let db_path = temp_file.path().to_path_buf();
        drop(temp_file);

        let mut dm = DiskManager::open(db_path.clone()).unwrap();
        dm.allocate_page().unwrap(); // Allocate PageId(0)

        let mut short_buf = vec![0u8; PAGE_SIZE - 1];
        let result = dm.read_page(PageId(0), &mut short_buf);
        assert!(matches!(result, Err(OxidbError::Io(_))));
        if let Err(OxidbError::Io(err)) = result {
            assert!(err.to_string().contains("Page data buffer length mismatch"));
        } else {
            panic!("Expected IO error for short data read buffer");
        }
        std::fs::remove_file(db_path).unwrap(); // Clean up
    }
}
