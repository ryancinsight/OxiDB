use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use crate::core::indexing::blink_tree::error::BlinkTreeError;
use crate::core::indexing::blink_tree::node::{BlinkTreeNode, PageId};

/// Page size for Blink tree (4KB, same as B+ tree)
pub const PAGE_SIZE: u64 = 4096;

/// Sentinel value for invalid page IDs  
pub const SENTINEL_PAGE_ID: PageId = u64::MAX;

/// Size of metadata section at the beginning of file
/// Format: order (4 bytes) + `root_page_id` (8 bytes) + `next_available_page_id` (8 bytes) + `free_list_head_page_id` (8 bytes)
pub const METADATA_SIZE: u64 = 4 + 8 + 8 + 8;

/// Page manager for Blink tree with concurrent access support
#[derive(Debug)]
pub struct BlinkPageManager {
    file_handle: Mutex<File>,
    // These fields will store the authoritative state of the metadata.
    // BlinkTreeIndex will query BlinkPageManager for these or update them via BlinkPageManager methods.
    order: usize, // Keep order here as it's part of metadata
    root_page_id: PageId,
    next_available_page_id: PageId,
    free_list_head_page_id: PageId,
}

impl BlinkPageManager {
    /// Create a new page manager for the Blink tree
    pub fn new(
        path: &PathBuf,
        order: usize,
        create_new_if_not_exists: bool,
    ) -> Result<Self, BlinkTreeError> {
        let file = if path.exists() {
            // File exists, open for read/write
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .map_err(|e| {
                    BlinkTreeError::Io(std::io::Error::new(
                        e.kind(),
                        format!(
                            "Failed to open existing blink tree file {:?}. Underlying error: {} (kind: {:?})",
                            path, e, e.kind()
                        ),
                    ))
                })?
        } else if create_new_if_not_exists {
            // File doesn't exist, create it
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .map_err(|e| {
                    BlinkTreeError::Io(std::io::Error::new(
                        e.kind(),
                        format!(
                            "Failed to create new blink tree file {:?}. Underlying error: {} (kind: {:?})",
                            path, e, e.kind()
                        ),
                    ))
                })?
        } else {
            return Err(BlinkTreeError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Blink tree file not found: {path:?}"),
            )));
        };

        let mut manager = Self {
            file_handle: Mutex::new(file),
            order,
            root_page_id: SENTINEL_PAGE_ID,
            next_available_page_id: 1, // Start from page 1 (page 0 is metadata)
            free_list_head_page_id: SENTINEL_PAGE_ID,
        };

        if path.exists() && path.metadata()?.len() > 0 {
            // Load existing metadata
            manager.load_metadata()?;
        } else {
            // Initialize new file with metadata
            manager.write_metadata_internal()?;
        }

        Ok(manager)
    }

    /// Load metadata from file
    fn load_metadata(&mut self) -> Result<(), BlinkTreeError> {
        let mut file_guard = self.file_handle.lock().unwrap();
        file_guard.seek(SeekFrom::Start(0))?;

        let mut buffer = [0u8; METADATA_SIZE as usize];
        file_guard.read_exact(&mut buffer)?;

        let order = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
        let root_page_id = u64::from_le_bytes([
            buffer[4], buffer[5], buffer[6], buffer[7], buffer[8], buffer[9], buffer[10],
            buffer[11],
        ]);
        let next_available_page_id = u64::from_le_bytes([
            buffer[12], buffer[13], buffer[14], buffer[15], buffer[16], buffer[17], buffer[18],
            buffer[19],
        ]);
        let free_list_head_page_id = u64::from_le_bytes([
            buffer[20], buffer[21], buffer[22], buffer[23], buffer[24], buffer[25], buffer[26],
            buffer[27],
        ]);

        self.order = order;
        self.root_page_id = root_page_id;
        self.next_available_page_id = next_available_page_id;
        self.free_list_head_page_id = free_list_head_page_id;

        Ok(())
    }

    /// Write metadata to file
    fn write_metadata_internal(&mut self) -> Result<(), BlinkTreeError> {
        let mut file_guard = self.file_handle.lock().unwrap();
        file_guard.seek(SeekFrom::Start(0))?;

        let mut buffer = [0u8; METADATA_SIZE as usize];

        // Write order
        let order_bytes = (self.order as u32).to_le_bytes();
        buffer[0..4].copy_from_slice(&order_bytes);

        // Write root_page_id
        let root_bytes = self.root_page_id.to_le_bytes();
        buffer[4..12].copy_from_slice(&root_bytes);

        // Write next_available_page_id
        let next_bytes = self.next_available_page_id.to_le_bytes();
        buffer[12..20].copy_from_slice(&next_bytes);

        // Write free_list_head_page_id
        let free_bytes = self.free_list_head_page_id.to_le_bytes();
        buffer[20..28].copy_from_slice(&free_bytes);

        file_guard.write_all(&buffer)?;
        file_guard.sync_all()?;
        Ok(())
    }

    /// Write metadata to file (public interface)
    pub fn write_metadata(&mut self) -> Result<(), BlinkTreeError> {
        self.write_metadata_internal()
    }

    /// Get the order of the Blink tree
    #[allow(dead_code)]
    pub const fn get_order(&self) -> usize {
        self.order
    }

    /// Get the root page ID
    pub const fn get_root_page_id(&self) -> PageId {
        self.root_page_id
    }

    /// Set the root page ID
    pub fn set_root_page_id(&mut self, new_root_page_id: PageId) -> Result<(), BlinkTreeError> {
        self.root_page_id = new_root_page_id;
        self.write_metadata()
    }

    /// Allocate a new page ID
    pub fn allocate_new_page_id(&mut self) -> Result<PageId, BlinkTreeError> {
        // TODO: Implement free list management for better page recycling
        // For now, just use next_available_page_id

        if self.free_list_head_page_id != SENTINEL_PAGE_ID {
            // Reuse a page from free list (simplified implementation)
            let reused_page_id = self.free_list_head_page_id;
            // In a full implementation, we would read the next free page from the freed page
            // For now, just reset the free list head
            self.free_list_head_page_id = SENTINEL_PAGE_ID;
            self.write_metadata()?;
            Ok(reused_page_id)
        } else {
            // Allocate new page
            let new_page_id = self.next_available_page_id;
            self.next_available_page_id += 1;
            self.write_metadata()?;
            Ok(new_page_id)
        }
    }

    /// Deallocate a page ID (add to free list)
    pub fn deallocate_page_id(&mut self, page_id_to_free: PageId) -> Result<(), BlinkTreeError> {
        // Simplified free list implementation
        // In a full implementation, we would write the current free_list_head to the freed page
        // and then update free_list_head to point to the freed page

        if self.free_list_head_page_id == SENTINEL_PAGE_ID {
            self.free_list_head_page_id = page_id_to_free;
        }
        // For now, we don't chain freed pages together

        self.write_metadata()?;
        Ok(())
    }

    /// Read a node from the specified page
    pub fn read_node(&self, page_id: PageId) -> Result<BlinkTreeNode, BlinkTreeError> {
        let mut file_guard = self.file_handle.lock().unwrap();
        let offset = METADATA_SIZE + (page_id * PAGE_SIZE);
        file_guard.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![0u8; PAGE_SIZE as usize];
        file_guard.read_exact(&mut buffer)?;

        // Find the actual data (skip any padding)
        // First 4 bytes should contain the actual data length
        let data_length = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;

        if data_length == 0 || data_length > (PAGE_SIZE as usize - 4) {
            return Err(BlinkTreeError::Generic(format!(
                "Invalid data length {data_length} for page {page_id}"
            )));
        }

        let node_data = &buffer[4..4 + data_length];
        BlinkTreeNode::from_bytes(node_data).map_err(BlinkTreeError::Serialization)
    }

    /// Write a node to its page
    pub fn write_node(&mut self, node: &BlinkTreeNode) -> Result<(), BlinkTreeError> {
        let page_id = node.get_page_id();
        let serialized_data = node.to_bytes().map_err(BlinkTreeError::Serialization)?;

        if serialized_data.len() > (PAGE_SIZE as usize - 4) {
            return Err(BlinkTreeError::PageFull(format!(
                "Serialized node data ({} bytes) exceeds page capacity ({} bytes)",
                serialized_data.len(),
                PAGE_SIZE - 4
            )));
        }

        let mut file_guard = self.file_handle.lock().unwrap();
        let offset = METADATA_SIZE + (page_id * PAGE_SIZE);
        file_guard.seek(SeekFrom::Start(offset))?;

        // Write data length first
        let data_length = serialized_data.len() as u32;
        file_guard.write_all(&data_length.to_le_bytes())?;

        // Write the actual data
        file_guard.write_all(&serialized_data)?;

        // Pad the rest of the page with zeros if necessary
        let remaining_space = PAGE_SIZE as usize - 4 - serialized_data.len();
        if remaining_space > 0 {
            let padding = vec![0u8; remaining_space];
            file_guard.write_all(&padding)?;
        }

        file_guard.sync_all()?;
        Ok(())
    }

    /// Sync all files to disk
    #[allow(dead_code)]
    pub fn sync_all_files(&self) -> Result<(), BlinkTreeError> {
        let file_guard = self.file_handle.lock().unwrap();
        file_guard.sync_all().map_err(BlinkTreeError::Io)
    }
}
