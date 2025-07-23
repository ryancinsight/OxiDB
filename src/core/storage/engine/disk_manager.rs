use crate::core::common::types::PageId; // Assuming PageId is u64 from common::types::ids
use crate::core::common::OxidbError;
use crate::core::storage::engine::page::PAGE_SIZE;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

/// Manages disk operations for database pages, including reading, writing, and allocating pages.
pub struct DiskManager {
    /// The file handle to the database file.
    db_file: File,
    // db_path: PathBuf, // Removed unused field
    /// The ID of the next page to be allocated.
    next_page_id: PageId, // To keep track of the next page to allocate
}

impl DiskManager {
    /// Opens a database file and creates a new DiskManager instance
    ///
    /// If the database file doesn't exist, it will be created. The next page ID
    /// is calculated based on the existing file size.
    ///
    /// # Errors
    /// Returns `OxidbError` if:
    /// - The database file cannot be opened or created
    /// - File metadata cannot be read
    /// - File I/O operations fail
    pub fn open(db_path: PathBuf) -> Result<Self, OxidbError> {
        let is_new_db = !db_path.exists();

        let db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(is_new_db) // Truncate if we are creating it new
            .open(&db_path)
            .map_err(|e| {
                OxidbError::io_error(format!(
                    "Failed to open database file '{}': {}",
                    db_path.display(),
                    e
                ))
            })?;

        let next_page_id = if is_new_db || db_file.metadata()?.len() == 0 {
            // Also treat empty existing file as new for page counting
            PageId(0)
        } else {
            let metadata = db_file.metadata().map_err(|e| {
                OxidbError::io_error(format!(
                    "Failed to read metadata for database file '{}': {}",
                    db_path.display(),
                    e
                ))
            })?;
            // Calculate next_page_id based on file size. Each page is PAGE_SIZE bytes.
            // If metadata.len() is 0, this implies 0 pages.
            // If metadata.len() is PAGE_SIZE, this implies 1 page (page 0), so next_page_id should be 1.
            // If metadata.len() is N * PAGE_SIZE, this implies N pages (0 to N-1), so next_page_id should be N.
            PageId(metadata.len() / (PAGE_SIZE as u64))
        };

        Ok(Self {
            db_file,
            // db_path, // Removed unused field
            next_page_id,
        })
    }

    /// Writes a page to disk at the specified page ID
    ///
    /// # Errors
    /// Returns `OxidbError` if:
    /// - Page data length doesn't match PAGE_SIZE
    /// - File seek operation fails
    /// - File write operation fails
    /// - File sync operation fails
    pub fn write_page(&mut self, page_id: PageId, page_data: &[u8]) -> Result<(), OxidbError> {
        if page_data.len() != PAGE_SIZE {
            return Err(OxidbError::io_error(format!(
                "Page data length mismatch: expected {}, got {}",
                PAGE_SIZE,
                page_data.len()
            )));
        }

        let offset = page_id.0.saturating_mul(PAGE_SIZE as u64);
        self.db_file.seek(SeekFrom::Start(offset)).map_err(|e| {
            OxidbError::io_error(format!(
                "Failed to seek to page {} offset {}: {}",
                page_id.0, offset, e
            ))
        })?;

        self.db_file.write_all(page_data).map_err(|e| {
            OxidbError::io_error(format!("Failed to write page {}: {}", page_id.0, e))
        })?;

        Ok(())
    }

    pub fn read_page(
        &mut self,
        page_id: PageId,
        page_data_buf: &mut [u8],
    ) -> Result<(), OxidbError> {
        if page_data_buf.len() != PAGE_SIZE {
            return Err(OxidbError::io_error(format!(
                "Page data buffer length mismatch: expected {}, got {}",
                PAGE_SIZE,
                page_data_buf.len()
            )));
        }

        if page_id.0 >= self.next_page_id.0 {
            return Err(OxidbError::io_error(format!(
                "Page ID {} out of bounds (next_page_id is {})",
                page_id.0, self.next_page_id.0
            )));
        }

        let offset = page_id.0.saturating_mul(PAGE_SIZE as u64);
        self.db_file.seek(SeekFrom::Start(offset)).map_err(|e| {
            OxidbError::io_error(format!(
                "Failed to seek to page {} offset {}: {}",
                page_id.0, offset, e
            ))
        })?;

        match self.db_file.read_exact(page_data_buf) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => Err(OxidbError::io_error(format!(
                "Unexpected EOF when reading page {}: not enough bytes",
                page_id.0
            ))),
            Err(e) => {
                Err(OxidbError::io_error(format!("Failed to read page {}: {}", page_id.0, e)))
            }
        }
    }

    pub fn allocate_page(&mut self) -> Result<PageId, OxidbError> {
        let new_page_id = self.next_page_id;
        self.next_page_id = PageId(self.next_page_id.0.saturating_add(1));

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
    use crate::core::storage::engine::page::PAGE_SIZE;
    use tempfile::NamedTempFile; // Already imported at module level but good for clarity

    fn create_temp_db_file() -> NamedTempFile {
        NamedTempFile::new().expect("Failed to create temp file")
    }

    #[test]
    fn test_open_new_db() {
        let temp_file = create_temp_db_file();
        let db_path = temp_file.path().to_path_buf();
        // Ensure file is deleted before DiskManager tries to manage it exclusively
        drop(temp_file);

        let dm = DiskManager::open(db_path.clone()).expect("Failed to open new DiskManager");
        assert_eq!(dm.next_page_id, PageId(0));
        // Verify file exists after open
        assert!(db_path.exists());
        std::fs::remove_file(db_path).expect("Failed to clean up test_open_new_db file");
        // Clean up
    }

    #[test]
    fn test_open_existing_db() {
        let temp_file = create_temp_db_file();
        let db_path = temp_file.path().to_path_buf();

        // Pre-populate the file to simulate existing pages
        {
            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(&db_path)
                .expect("Failed to open for pre-population");
            file.set_len((PAGE_SIZE * 3) as u64).expect("Failed to set file length");
            // Simulate 3 pages
        } // drop file to release lock before DiskManager::open

        let dm = DiskManager::open(db_path.clone()).expect("Failed to open existing DiskManager");
        assert_eq!(dm.next_page_id, PageId(3));
        std::fs::remove_file(db_path).expect("Failed to clean up test_open_existing_db file");
        // Clean up
    }

    #[test]
    fn test_allocate_and_write_read_page() {
        let temp_file = create_temp_db_file();
        let db_path = temp_file.path().to_path_buf();
        drop(temp_file);

        let mut dm = DiskManager::open(db_path.clone())
            .expect("Failed to open DiskManager for read/write test");

        // Allocate first page
        let page_id0 = dm.allocate_page().expect("Failed to allocate page_id0");
        assert_eq!(page_id0, PageId(0));
        assert_eq!(dm.next_page_id, PageId(1));

        // Write to first page
        let mut write_data0 = vec![0u8; PAGE_SIZE];
        for (i, byte) in write_data0.iter_mut().enumerate() {
            *byte = u8::try_from(i % 256).expect("Modulo 256 should fit in u8");
        }
        dm.write_page(page_id0, &write_data0).expect("Failed to write page_id0");

        // Read back first page
        let mut read_buf0 = vec![0u8; PAGE_SIZE];
        dm.read_page(page_id0, &mut read_buf0).expect("Failed to read page_id0");
        assert_eq!(write_data0, read_buf0);

        // Allocate second page
        let page_id1 = dm.allocate_page().expect("Failed to allocate page_id1");
        assert_eq!(page_id1, PageId(1));
        assert_eq!(dm.next_page_id, PageId(2));

        // Write to second page
        let mut write_data1 = vec![0u8; PAGE_SIZE];
        for (i, byte) in write_data1.iter_mut().enumerate() {
            *byte = u8::try_from((PAGE_SIZE - 1 - i) % 256).expect("Modulo 256 should fit in u8");
        }
        dm.write_page(page_id1, &write_data1).expect("Failed to write page_id1");

        // Read back second page
        let mut read_buf1 = vec![0u8; PAGE_SIZE];
        dm.read_page(page_id1, &mut read_buf1).expect("Failed to read page_id1");
        assert_eq!(write_data1, read_buf1);

        // Verify file size
        let metadata = dm.db_file.metadata().expect("Failed to get metadata");
        assert_eq!(metadata.len(), (PAGE_SIZE.saturating_mul(2)) as u64);

        std::fs::remove_file(db_path)
            .expect("Failed to clean up test_allocate_and_write_read_page file");
        // Clean up
    }

    #[test]
    fn test_read_out_of_bounds_page() {
        let temp_file = create_temp_db_file();
        let db_path = temp_file.path().to_path_buf();
        drop(temp_file);

        let mut dm = DiskManager::open(db_path.clone())
            .expect("Failed to open DiskManager for out-of-bounds test");
        let mut read_buf = vec![0u8; PAGE_SIZE];

        // Try to read page 0 before allocation
        let result = dm.read_page(PageId(0), &mut read_buf);
        assert!(
            matches!(result, Err(OxidbError::Io(_))),
            "Expected IO error for reading unallocated page"
        );
        if let Err(OxidbError::Io(err)) = result {
            assert!(
                err.to_string().contains("out of bounds"),
                "Error message should indicate out of bounds"
            );
        } else {
            panic!("Expected IO error for out of bounds read, got {:?}", result);
        }

        // Allocate a page
        dm.allocate_page().expect("Failed to allocate page for out-of-bounds test"); // PageId(0) allocated

        // Try to read PageId(1) which is not yet allocated but next_page_id is 1
        let result_next = dm.read_page(PageId(1), &mut read_buf);
        assert!(
            matches!(result_next, Err(OxidbError::Io(_))),
            "Expected IO error for reading page at next_page_id"
        );
        if let Err(OxidbError::Io(err)) = result_next {
            assert!(
                err.to_string().contains("out of bounds"),
                "Error message for next_page_id read should indicate out of bounds"
            );
        } else {
            panic!(
                "Expected IO error for out of bounds read of next_page_id, got {:?}",
                result_next
            );
        }
        std::fs::remove_file(db_path)
            .expect("Failed to clean up test_read_out_of_bounds_page file"); // Clean up
    }

    #[test]
    fn test_write_page_invalid_data_length() {
        let temp_file = create_temp_db_file();
        let db_path = temp_file.path().to_path_buf();
        drop(temp_file);

        let mut dm = DiskManager::open(db_path.clone())
            .expect("Failed to open DiskManager for invalid data length test");
        let page_id =
            dm.allocate_page().expect("Failed to allocate page for invalid data length test");

        let short_data = vec![0u8; PAGE_SIZE - 1];
        let result = dm.write_page(page_id, &short_data);
        assert!(
            matches!(result, Err(OxidbError::Io(_))),
            "Expected IO error for writing short data"
        );
        if let Err(OxidbError::Io(err)) = result {
            assert!(
                err.to_string().contains("Page data length mismatch"),
                "Error message should indicate length mismatch"
            );
        } else {
            panic!("Expected IO error for short data write, got {:?}", result);
        }
        std::fs::remove_file(db_path)
            .expect("Failed to clean up test_write_page_invalid_data_length file");
        // Clean up
    }

    #[test]
    fn test_read_page_invalid_buffer_length() {
        let temp_file = create_temp_db_file();
        let db_path = temp_file.path().to_path_buf();
        drop(temp_file);

        let mut dm = DiskManager::open(db_path.clone())
            .expect("Failed to open DiskManager for invalid buffer length test");
        dm.allocate_page().expect("Failed to allocate page for invalid buffer length test"); // Allocate PageId(0)

        let mut short_buf = vec![0u8; PAGE_SIZE - 1];
        let result = dm.read_page(PageId(0), &mut short_buf);
        assert!(
            matches!(result, Err(OxidbError::Io(_))),
            "Expected IO error for reading into short buffer"
        );
        if let Err(OxidbError::Io(err)) = result {
            assert!(
                err.to_string().contains("Page data buffer length mismatch"),
                "Error message should indicate buffer length mismatch"
            );
        } else {
            panic!("Expected IO error for short data read buffer, got {:?}", result);
        }
        std::fs::remove_file(db_path)
            .expect("Failed to clean up test_read_page_invalid_buffer_length file");
        // Clean up
    }
}
