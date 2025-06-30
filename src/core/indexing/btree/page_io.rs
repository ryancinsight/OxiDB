use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use crate::core::indexing::btree::error::OxidbError;
use crate::core::indexing::btree::node::{BPlusTreeNode, PageId, SerializationError};

// Constants previously in tree.rs
/// The size of a page in bytes.
pub const PAGE_SIZE: u64 = 4096;
/// Sentinel Page ID to signify the end of the free list or no page.
pub const SENTINEL_PAGE_ID: PageId = u64::MAX; // u64::MAX is already used by tree.SENTINEL_PAGE_ID
/// The size of the metadata stored at the beginning of the B+Tree file.
/// order (u32) + root_page_id (u64) + next_available_page_id (u64) + free_list_head_page_id (u64)
pub const METADATA_SIZE: u64 = 4 + 8 + 8 + 8;

#[derive(Debug)]
pub struct PageManager {
    file_handle: Mutex<File>,
    // These fields will store the authoritative state of the metadata.
    // BPlusTreeIndex will query PageManager for these or update them via PageManager methods.
    order: usize, // Keep order here as it's part of metadata
    root_page_id: PageId,
    next_available_page_id: PageId,
    free_list_head_page_id: PageId,
}

impl PageManager {
    pub fn new(
        path: &PathBuf,
        order: usize,
        create_new_if_not_exists: bool,
    ) -> Result<Self, OxidbError> {
        let file_exists = path.exists();
        let mut file_obj = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create_new_if_not_exists) // Create if specified
            .truncate(create_new_if_not_exists && !file_exists) // Truncate if newly created
            .open(&path)?;

        if file_exists && file_obj.metadata()?.len() >= METADATA_SIZE {
            file_obj.seek(SeekFrom::Start(0))?;
            let mut u32_buf = [0u8; 4];
            let mut u64_buf = [0u8; 8];

            file_obj.read_exact(&mut u32_buf)?;
            let loaded_order = u32::from_be_bytes(u32_buf) as usize;

            file_obj.read_exact(&mut u64_buf)?;
            let root_page_id = u64::from_be_bytes(u64_buf);

            file_obj.read_exact(&mut u64_buf)?;
            let next_available_page_id = u64::from_be_bytes(u64_buf);

            file_obj.read_exact(&mut u64_buf)?;
            let free_list_head_page_id = u64::from_be_bytes(u64_buf);

            // If order from file doesn't match provided order, it could be an issue.
            // For now, let's assume if loading, the file's order is authoritative.
            // If the file's order is 0 (e.g. uninitialized but existing file), use provided.
            let effective_order = if loaded_order == 0 { order } else { loaded_order };
            if order != 0 && effective_order != order {
                // This case should be handled carefully. For now, log or return error.
                // Let's prefer the loaded order if the file was valid.
                eprintln!(
                    "Warning: Order mismatch. Provided: {}, Loaded: {}. Using loaded order.",
                    order, effective_order
                );
            }

            Ok(Self {
                file_handle: Mutex::new(file_obj),
                order: effective_order,
                root_page_id,
                next_available_page_id,
                free_list_head_page_id,
            })
        } else if create_new_if_not_exists {
            // File didn't exist or was too small, and we are allowed to create/truncate.
            if order < 3 {
                // Min order check
                return Err(OxidbError::TreeLogicError(format!(
                    "Order {} is too small. Minimum order is 3.",
                    order
                )));
            }
            let initial_root_page_id = 0; // Root always starts at page 0
            let initial_next_available_page_id = 1; // Page 0 is root, so next is 1
            let initial_free_list_head_page_id = SENTINEL_PAGE_ID; // No free pages initially

            let mut pm = Self {
                file_handle: Mutex::new(file_obj),
                order,
                root_page_id: initial_root_page_id,
                next_available_page_id: initial_next_available_page_id,
                free_list_head_page_id: initial_free_list_head_page_id,
            };
            pm.write_metadata_internal()?; // Write initial metadata
            Ok(pm)
        } else {
            // File does not exist, and create_new_if_not_exists is false
            Err(OxidbError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("B+Tree file not found at {:?} and not allowed to create.", path),
            )))
        }
    }

    // Internal write_metadata, expects file lock to be already acquired or not needed (e.g. during new)
    fn write_metadata_internal(&mut self) -> Result<(), OxidbError> {
        let mut file = self.file_handle.lock().map_err(|e| {
            OxidbError::BorrowError(format!("Mutex lock error for write_metadata_internal: {}", e))
        })?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&(self.order as u32).to_be_bytes())?; // Assuming order fits u32
        file.write_all(&self.root_page_id.to_be_bytes())?;
        file.write_all(&self.next_available_page_id.to_be_bytes())?;
        file.write_all(&self.free_list_head_page_id.to_be_bytes())?;
        file.flush()?; // Ensure metadata is written
        Ok(())
    }

    // Public method to persist metadata, callable by BPlusTreeIndex
    pub fn write_metadata(&mut self) -> Result<(), OxidbError> {
        self.write_metadata_internal()
    }

    // Getter methods for metadata needed by BPlusTreeIndex
    pub fn get_order(&self) -> usize {
        self.order
    }

    pub fn get_root_page_id(&self) -> PageId {
        self.root_page_id
    }

    pub fn set_root_page_id(&mut self, new_root_page_id: PageId) -> Result<(), OxidbError> {
        self.root_page_id = new_root_page_id;
        // self.write_metadata_internal() // Metadata should be written explicitly by BTreeIndex after all changes
        Ok(())
    }

    // allocate_new_page_id, deallocate_page_id, read_node, write_node will be added next
    pub fn allocate_new_page_id(&mut self) -> Result<PageId, OxidbError> {
        let new_page_id;
        if self.free_list_head_page_id != SENTINEL_PAGE_ID {
            new_page_id = self.free_list_head_page_id;
            // Read the first 8 bytes of this page to get the next free page ID
            let mut file = self.file_handle.lock().map_err(|e| {
                OxidbError::BorrowError(format!(
                    "Mutex lock error for allocate (read free list): {}",
                    e
                ))
            })?;
            let offset = PAGE_SIZE.saturating_add(new_page_id.saturating_mul(PAGE_SIZE)); // Metadata is on page 0, actual data pages start after PAGE_SIZE offset
            file.seek(SeekFrom::Start(offset))?;
            let mut next_free_buf = [0u8; 8];
            file.read_exact(&mut next_free_buf)?;
            self.free_list_head_page_id = PageId::from_be_bytes(next_free_buf);
        } else {
            new_page_id = self.next_available_page_id;
            self.next_available_page_id = self.next_available_page_id.saturating_add(1);
        }
        // self.write_metadata_internal()?; // Caller (BPlusTreeIndex) should decide when to write metadata
        Ok(new_page_id)
    }

    pub fn deallocate_page_id(&mut self, page_id_to_free: PageId) -> Result<(), OxidbError> {
        if page_id_to_free == SENTINEL_PAGE_ID {
            // Should match the constant defined in this file
            return Err(OxidbError::TreeLogicError(
                "Cannot deallocate sentinel page ID".to_string(),
            ));
        }

        let mut file = self.file_handle.lock().map_err(|e| {
            OxidbError::BorrowError(format!("Mutex lock error for deallocate: {}", e))
        })?;
        let offset = PAGE_SIZE.saturating_add(page_id_to_free.saturating_mul(PAGE_SIZE));
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&self.free_list_head_page_id.to_be_bytes())?;

        self.free_list_head_page_id = page_id_to_free;
        // self.write_metadata_internal()?; // Caller (BPlusTreeIndex) should decide when to write metadata
        Ok(())
    }

    pub fn read_node(&self, page_id: PageId) -> Result<BPlusTreeNode, OxidbError> {
        let mut file = self.file_handle.lock().map_err(|e| {
            OxidbError::BorrowError(format!("Mutex lock error for read_node: {}", e))
        })?;
        let offset = PAGE_SIZE.saturating_add(page_id.saturating_mul(PAGE_SIZE));
        file.seek(SeekFrom::Start(offset))?;

        let page_size_usize = usize::try_from(PAGE_SIZE).map_err(|_| {
            OxidbError::Serialization(SerializationError::InvalidFormat(
                "PAGE_SIZE too large for usize".to_string(),
            ))
        })?;
        let mut page_buffer = vec![0u8; page_size_usize];

        match file.read_exact(&mut page_buffer) {
            Ok(_) => BPlusTreeNode::from_bytes(&page_buffer).map_err(OxidbError::from),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                Err(OxidbError::NodeNotFound(page_id))
            }
            Err(e) => Err(OxidbError::Io(e)),
        }
    }

    pub fn write_node(&mut self, node: &BPlusTreeNode) -> Result<(), OxidbError> {
        let mut file = self.file_handle.lock().map_err(|e| {
            OxidbError::BorrowError(format!("Mutex lock error for write_node: {}", e))
        })?;
        let page_id = node.get_page_id();
        let offset = PAGE_SIZE.saturating_add(page_id.saturating_mul(PAGE_SIZE));

        let mut node_bytes = node.to_bytes()?;
        let page_size_usize = usize::try_from(PAGE_SIZE).map_err(|_| {
            OxidbError::Serialization(SerializationError::InvalidFormat(
                "PAGE_SIZE too large for usize".to_string(),
            ))
        })?;

        if node_bytes.len() > page_size_usize {
            return Err(OxidbError::PageFull(format!(
                "Serialized node size {} exceeds PAGE_SIZE {}",
                node_bytes.len(),
                PAGE_SIZE
            )));
        }
        node_bytes.resize(page_size_usize, 0); // Pad with zeros to fill the page

        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&node_bytes)?;
        file.flush()?; // Ensure node is written to disk
        Ok(())
    }

    pub fn sync_all_files(&self) -> Result<(), OxidbError> {
        let file = self.file_handle.lock().map_err(|e| {
            OxidbError::BorrowError(format!("Mutex lock error for sync_all_files: {}", e))
        })?;
        file.sync_all().map_err(OxidbError::from)
    }
}
