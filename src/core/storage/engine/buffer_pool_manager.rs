use super::disk_manager::DiskManager;
use super::page::PAGE_SIZE; // PageId removed from here
use crate::core::common::error::OxidbError;
use crate::core::common::types::PageId as CommonPageId; // Using alias to avoid conflict if super::page::PageId is different
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
// Removed use std::usize; as it's implicitly available

#[derive(Debug)]
pub struct Frame {
    page_id: Option<CommonPageId>, // Option<PageId> to represent an empty frame
    data: Arc<RwLock<[u8; PAGE_SIZE]>>, // Page data buffer
    pin_count: u32,
    is_dirty: bool,
}

impl Frame {
    fn new() -> Self {
        Frame {
            page_id: None,
            data: Arc::new(RwLock::new([0u8; PAGE_SIZE])),
            pin_count: 0,
            is_dirty: false,
        }
    }

    // Helper to reset frame state
    #[allow(dead_code)] // Will be used internally by BPM
    fn reset(&mut self) {
        self.page_id = None;
        // self.data is reused, content will be overwritten by new page read
        self.pin_count = 0;
        self.is_dirty = false;
    }
}

pub struct BufferPoolManager {
    frames: Vec<Mutex<Frame>>,                // Frames in the buffer pool
    page_table: HashMap<CommonPageId, usize>, // page_id to frame_index
    free_list: VecDeque<usize>,               // List of frame indices that are free
    replacer_queue: VecDeque<usize>, // Frame indices considered for replacement (FIFO for unpinned frames)
    disk_manager: Arc<Mutex<DiskManager>>,
    // pool_size: usize, // Removed unused field
}

impl BufferPoolManager {
    pub fn new(pool_size: usize, disk_manager: Arc<Mutex<DiskManager>>) -> Self {
        let mut frames = Vec::with_capacity(pool_size);
        let mut free_list = VecDeque::with_capacity(pool_size);
        for i in 0..pool_size {
            frames.push(Mutex::new(Frame::new()));
            free_list.push_back(i);
        }

        BufferPoolManager {
            frames,
            page_table: HashMap::new(),
            free_list,
            replacer_queue: VecDeque::new(),
            disk_manager,
            // pool_size, // Removed unused field
        }
    }

    fn find_victim_frame_index(&mut self) -> Option<usize> {
        // 1. Try free_list first
        if let Some(frame_idx) = self.free_list.pop_front() {
            return Some(frame_idx);
        }

        // 2. Try replacer_queue (FIFO for unpinned frames)
        // Iterate a number of times equal to current queue length to check each frame once
        for _ in 0..self.replacer_queue.len() {
            if let Some(frame_idx) = self.replacer_queue.pop_front() {
                let frame_guard = self.frames[frame_idx]
                    .lock()
                    .expect("Buffer pool frame lock poisoned for victim selection");
                if frame_guard.pin_count == 0 {
                    // Found a victim
                    // Drop the guard before returning, though it would drop anyway
                    drop(frame_guard);
                    return Some(frame_idx);
                }
                // It's pinned, add it back to the end of the queue
                self.replacer_queue.push_back(frame_idx);
            } else {
                // Should not happen if loop condition is based on replacer_queue.len()
                // but as a safeguard, if queue is empty, break.
                break;
            }
        }

        // No suitable frame found in replacer_queue
        None
    }

    pub fn fetch_page(
        &mut self,
        page_id: CommonPageId,
    ) -> Result<Arc<RwLock<[u8; PAGE_SIZE]>>, OxidbError> {
        // 1. Check if page is already in buffer pool
        if let Some(&frame_idx) = self.page_table.get(&page_id) {
            let mut frame = self.frames[frame_idx].lock().unwrap();
            frame.pin_count += 1;

            // Remove from replacer_queue if it was there, as it's now pinned.
            // This is a bit inefficient (linear scan), but for typical pool sizes and queue lengths, it might be acceptable.
            // A more efficient way would be to have a way to directly remove or mark elements in replacer_queue,
            // or use a different data structure for replacer_queue that supports efficient removal.
            // For FIFO, VecDeque::retain is an option.
            let original_len = self.replacer_queue.len();
            self.replacer_queue.retain(|&idx| idx != frame_idx);
            // If an element was removed, it means it was in the queue.
            // If not, it was either not in the queue or already processed (e.g. if it was pinned multiple times).
            // This check isn't strictly necessary for correctness of retain, but good for understanding.
            assert!(self.replacer_queue.len() <= original_len);

            return Ok(Arc::clone(&frame.data));
        }

        // 2. Page not in buffer pool (cache miss) - find a victim frame
        let victim_frame_idx = self.find_victim_frame_index().ok_or_else(|| {
            OxidbError::BufferPool("No free or unpinned frames available".to_string())
        })?;

        let mut victim_frame = self.frames[victim_frame_idx].lock().unwrap();

        // 3. If victim frame is dirty and occupied, write its content to disk
        if victim_frame.is_dirty {
            if let Some(old_page_id) = victim_frame.page_id {
                {
                    // Scope for data_guard
                    let data_guard = victim_frame.data.read().unwrap(); // Read lock for page data
                                                                        // It's important to unlock disk_manager after use.
                    self.disk_manager.lock().unwrap().write_page(old_page_id, &*data_guard)?;
                } // data_guard is dropped here
                victim_frame.is_dirty = false;
            } else {
                // This case (dirty but no page_id) should ideally not happen with correct logic.
                // If it does, it might indicate an issue. For now, we can ignore or log.
            }
        }

        // 4. If the victim frame was previously occupied, remove its old page_id from page_table
        if let Some(old_page_id) = victim_frame.page_id {
            self.page_table.remove(&old_page_id);
        }

        // 5. Update victim frame metadata for the new page
        victim_frame.page_id = Some(page_id);
        victim_frame.pin_count = 1;
        victim_frame.is_dirty = false; // New page is not dirty yet

        // 6. Read page data from disk into the frame's data buffer
        // Need a write lock on the frame's data to modify it.
        {
            // Scope for data_guard to release lock quickly
            let mut data_guard = victim_frame.data.write().unwrap();
            self.disk_manager.lock().unwrap().read_page(page_id, &mut *data_guard)?;
        }

        // 7. Add new page_id to page_table and return the data Arc
        self.page_table.insert(page_id, victim_frame_idx);

        // Frame is pinned (pin_count = 1), so it should not be added to replacer_queue yet.
        // It will be added when unpinned.

        Ok(Arc::clone(&victim_frame.data))
    }

    pub fn unpin_page(&mut self, page_id: CommonPageId, is_dirty: bool) -> Result<(), OxidbError> {
        let frame_idx = self.page_table.get(&page_id).ok_or_else(|| {
            OxidbError::BufferPool(format!(
                "Page {} not found in buffer pool page_table",
                page_id.0
            ))
        })?;

        let mut frame = self.frames[*frame_idx].lock().unwrap();

        if frame.pin_count == 0 {
            return Err(OxidbError::BufferPool(format!(
                "Page {} pin count is already zero",
                page_id.0
            )));
        }

        frame.pin_count -= 1;
        if is_dirty {
            frame.is_dirty = true;
        }

        if frame.pin_count == 0 {
            // Check if frame_idx is already in replacer_queue to avoid duplicates, though
            // correct pin_count management should prevent this for a single frame.
            // However, if a frame is unpinned, re-pinned, and then unpinned again before
            // the replacer processes it, it might be added twice if not checked.
            // For strict FIFO, it should be added. If it's already there, it means something
            // else (like a previous unpin) already made it a candidate.
            // A simple FIFO queue typically just adds it. If it's processed while pinned,
            // find_victim_frame_index handles that.
            if !self.replacer_queue.contains(frame_idx) {
                // Added a check to prevent duplicates, might be overly cautious or not needed depending on strict FIFO interpretation
                self.replacer_queue.push_back(*frame_idx);
            }
        }
        Ok(())
    }

    pub fn flush_page(&mut self, page_id: CommonPageId) -> Result<(), OxidbError> {
        if let Some(&frame_idx) = self.page_table.get(&page_id) {
            let mut frame = self.frames[frame_idx].lock().unwrap();

            if frame.is_dirty {
                // Ensure page_id in frame matches, though it should if it's in page_table
                if frame.page_id == Some(page_id) {
                    {
                        // Scope for data_guard
                        let data_guard = frame.data.read().unwrap();
                        self.disk_manager.lock().unwrap().write_page(page_id, &*data_guard)?;
                    } // data_guard is dropped here
                    frame.is_dirty = false;
                } else {
                    // This would be an inconsistent state
                    return Err(OxidbError::BufferPool(format!(
                        "Page ID mismatch in frame: expected {:?}, found {:?} for frame {}",
                        page_id, frame.page_id, frame_idx
                    )));
                }
            }
        }
        // If page_id not in page_table, it's considered not in the pool or already evicted.
        // The task specifies to return Ok(()) in this case.
        Ok(())
    }

    pub fn new_page(&mut self) -> Result<(CommonPageId, Arc<RwLock<[u8; PAGE_SIZE]>>), OxidbError> {
        let new_page_id = self.disk_manager.lock().unwrap().allocate_page()?;

        // Fetching the page will:
        // - Find a victim frame (possibly flushing it if dirty).
        // - Read the (zeroed) data of new_page_id from disk into the frame.
        // - Pin the frame (pin_count = 1).
        // - Add it to the page_table.
        let page_data_arc = self.fetch_page(new_page_id)?;

        // The page is initially not dirty when fetched (even if it's new and zeroed).
        // If the caller modifies it, they should unpin it with is_dirty = true.
        // fetch_page sets is_dirty = false for the newly fetched page.

        Ok((new_page_id, page_data_arc))
    }

    pub fn flush_all_pages(&mut self) -> Result<(), OxidbError> {
        // Collect all page IDs currently in the buffer pool.
        // Cloning is important here to avoid issues with borrowing self.page_table
        // while calling self.flush_page (which also borrows self).
        let page_ids: Vec<CommonPageId> = self.page_table.keys().cloned().collect();

        for page_id in page_ids {
            self.flush_page(page_id)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::storage::engine::disk_manager::DiskManager;
    use tempfile::NamedTempFile;
    // PathBuf removed from here

    // Helper to create a BPM with a temporary DiskManager
    fn setup_bpm(pool_size: usize) -> (BufferPoolManager, Arc<Mutex<DiskManager>>, NamedTempFile) {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file for BPM test");
        let db_path = temp_file.path().to_path_buf();

        // It's important that the temp_file is not dropped until the test is done,
        // but DiskManager::open needs the path. So, we might need to manage its lifecycle carefully
        // or let DiskManager create/own the file for testing purposes if that's simpler.
        // For now, assuming DiskManager::open works with the path from NamedTempFile.
        // We will drop the temp_file handle at the end of the setup, DiskManager will own the File.

        let disk_manager = Arc::new(Mutex::new(DiskManager::open(db_path).unwrap()));
        let bpm = BufferPoolManager::new(pool_size, Arc::clone(&disk_manager));
        (bpm, disk_manager, temp_file)
    }

    #[test]
    fn test_bpm_new() {
        const POOL_SIZE: usize = 10;
        let (bpm, _disk_manager, _temp_file) = setup_bpm(POOL_SIZE);
        assert_eq!(bpm.frames.len(), POOL_SIZE);
        assert_eq!(bpm.free_list.len(), POOL_SIZE);
        assert_eq!(bpm.page_table.len(), 0);
        assert_eq!(bpm.replacer_queue.len(), 0);
    }

    #[test]
    fn test_new_page_and_fetch() {
        const POOL_SIZE: usize = 3;
        let (mut bpm, _disk_manager, _temp_file) = setup_bpm(POOL_SIZE);

        // 1. Allocate a new page
        let (page_id0, page0_arc) = bpm.new_page().unwrap();
        assert_eq!(page_id0.0, 0);
        {
            let page0_data = page0_arc.read().unwrap();
            assert!(page0_data.iter().all(|&x| x == 0), "New page data should be zeroed");
        }
        // Frame for page_id0 should be pinned (pin_count=1)
        let frame0_idx_after_new = *bpm.page_table.get(&page_id0).unwrap();
        assert_eq!(bpm.frames[frame0_idx_after_new].lock().unwrap().pin_count, 1);
        assert_eq!(bpm.free_list.len(), POOL_SIZE - 1); // One frame used

        // 2. Fetch the same page
        let page0_fetched_arc = bpm.fetch_page(page_id0).unwrap();
        let frame0_idx_after_fetch = *bpm.page_table.get(&page_id0).unwrap();
        assert_eq!(bpm.frames[frame0_idx_after_fetch].lock().unwrap().pin_count, 2); // Pinned again
        assert!(Arc::ptr_eq(&page0_arc, &page0_fetched_arc)); // Should be the same Arc

        // 3. Unpin once
        bpm.unpin_page(page_id0, false).unwrap();
        let frame0_idx_after_unpin1 = *bpm.page_table.get(&page_id0).unwrap();
        assert_eq!(bpm.frames[frame0_idx_after_unpin1].lock().unwrap().pin_count, 1);
        assert_eq!(bpm.replacer_queue.len(), 0); // Still pinned, not in replacer

        // 4. Unpin again, making it available for replacement
        bpm.unpin_page(page_id0, true).unwrap(); // Mark dirty
        let frame0_idx_after_unpin2 = *bpm.page_table.get(&page_id0).unwrap();
        assert_eq!(bpm.frames[frame0_idx_after_unpin2].lock().unwrap().pin_count, 0);
        assert!(bpm.frames[frame0_idx_after_unpin2].lock().unwrap().is_dirty);
        assert_eq!(bpm.replacer_queue.len(), 1);
        assert_eq!(bpm.replacer_queue.front().unwrap(), &frame0_idx_after_unpin2);
    }

    #[test]
    fn test_page_replacement_fifo() {
        const POOL_SIZE: usize = 2;
        let (mut bpm, disk_manager_arc, _temp_file) = setup_bpm(POOL_SIZE);

        // Allocate page0
        let (page_id0, _page0_arc) = bpm.new_page().unwrap(); // pin_count = 1
        bpm.unpin_page(page_id0, false).unwrap(); // pin_count = 0, page_id0 in replacer

        // Allocate page1
        let (page_id1, _page1_arc) = bpm.new_page().unwrap(); // pin_count = 1
        bpm.unpin_page(page_id1, true).unwrap(); // pin_count = 0, page_id1 in replacer (dirty)

        // Pool is full (page0, page1), both unpinned. page_id0 is FIFO victim.
        // Replacer: [frame_for_page0, frame_for_page1]

        // Allocate page2, this should evict page0
        let (page_id2, page2_arc) = bpm.new_page().unwrap(); // pin_count = 1 for page2

        assert!(bpm.page_table.contains_key(&page_id1));
        assert!(bpm.page_table.contains_key(&page_id2));
        assert!(!bpm.page_table.contains_key(&page_id0), "Page0 should have been evicted");
        assert_eq!(bpm.free_list.len(), 0);
        assert_eq!(bpm.replacer_queue.len(), 1); // Only frame for page1 should be in replacer (page2 is pinned)

        // Verify page0 (not dirty) was not written to disk (implicitly, as it wasn't marked dirty)
        // Verify page1 (dirty) would be written if it was chosen as victim.
        // Here, page0 was victim. Let's check page2 data.
        {
            let page2_data = page2_arc.read().unwrap();
            assert!(page2_data.iter().all(|&x| x == 0), "New page data should be zeroed");
        }

        // Now, let page_id1 be the victim. page_id1 was dirty.
        // Unpin page_id2
        bpm.unpin_page(page_id2, false).unwrap(); // Replacer: [frame_for_page1, frame_for_page2]

        // Allocate page3, this should evict page1 (which was dirty)
        let (_page_id3, _page3_arc) = bpm.new_page().unwrap();
        assert!(!bpm.page_table.contains_key(&page_id1), "Page1 should have been evicted");

        // Verify page1 was flushed to disk
        let mut dm = disk_manager_arc.lock().unwrap();
        let mut page1_data_from_disk = [0u8; PAGE_SIZE];
        dm.read_page(page_id1, &mut page1_data_from_disk).unwrap();
        // The original page1 was created by new_page, so it was zeroed.
        // It was then unpinned with is_dirty = true.
        // So, its content on disk should be what it was when it became dirty.
        // If we had modified it, we'd check for that pattern. Since it was just new_page -> unpin(dirty),
        // its data in the frame was [0; PAGE_SIZE]. So disk should have [0; PAGE_SIZE].
        assert!(page1_data_from_disk.iter().all(|&x| x == 0));
    }

    #[test]
    fn test_flush_page() {
        const POOL_SIZE: usize = 1;
        let (mut bpm, disk_manager_arc, _temp_file) = setup_bpm(POOL_SIZE);

        // New page, pin_count = 1
        let (page_id, page_arc) = bpm.new_page().unwrap();

        // Modify page data
        {
            let mut page_data = page_arc.write().unwrap();
            page_data[0] = 100;
        }

        // Unpin and mark dirty
        bpm.unpin_page(page_id, true).unwrap();
        let frame_idx = *bpm.page_table.get(&page_id).unwrap();
        assert!(bpm.frames[frame_idx].lock().unwrap().is_dirty);

        // Flush page
        bpm.flush_page(page_id).unwrap();
        assert!(
            !bpm.frames[frame_idx].lock().unwrap().is_dirty,
            "Page should not be dirty after flush"
        );

        // Verify data on disk
        let mut dm = disk_manager_arc.lock().unwrap();
        let mut page_data_from_disk = [0u8; PAGE_SIZE];
        dm.read_page(page_id, &mut page_data_from_disk).unwrap();
        assert_eq!(page_data_from_disk[0], 100);
    }

    #[test]
    fn test_flush_all_pages() {
        const POOL_SIZE: usize = 2;
        let (mut bpm, _disk_manager_arc, _temp_file) = setup_bpm(POOL_SIZE);

        let (p0, p0_arc) = bpm.new_page().unwrap();
        {
            p0_arc.write().unwrap()[0] = 1;
        }
        bpm.unpin_page(p0, true).unwrap(); // dirty

        let (p1, p1_arc) = bpm.new_page().unwrap();
        {
            p1_arc.write().unwrap()[0] = 2;
        }
        bpm.unpin_page(p1, true).unwrap(); // dirty

        bpm.flush_all_pages().unwrap();

        let f0_idx = *bpm.page_table.get(&p0).unwrap();
        let f1_idx = *bpm.page_table.get(&p1).unwrap();
        assert!(!bpm.frames[f0_idx].lock().unwrap().is_dirty);
        assert!(!bpm.frames[f1_idx].lock().unwrap().is_dirty);
    }

    #[test]
    fn test_pin_count_limits_replacement() {
        const POOL_SIZE: usize = 1;
        let (mut bpm, _disk_manager_arc, _temp_file) = setup_bpm(POOL_SIZE);

        let (_page_id0, _page0_arc) = bpm.new_page().unwrap(); // Pinned (count=1)
                                                               // Pool is full, page0 is pinned.

        let result = bpm.new_page(); // Should fail as no frame can be victimized
        assert!(matches!(result, Err(OxidbError::BufferPool(_))));
        if let Err(OxidbError::BufferPool(msg)) = result {
            assert!(msg.contains("No free or unpinned frames available"));
        } else {
            panic!("Expected BufferPool error");
        }
    }

    #[test]
    fn test_unpin_non_existent_page() {
        let (mut bpm, _disk_manager, _temp_file) = setup_bpm(1);
        let result = bpm.unpin_page(CommonPageId(0), false);
        assert!(matches!(result, Err(OxidbError::BufferPool(_))));
    }

    #[test]
    fn test_unpin_already_zero_pin_count() {
        let (mut bpm, _disk_manager, _temp_file) = setup_bpm(1);
        let (page_id, _) = bpm.new_page().unwrap();
        bpm.unpin_page(page_id, false).unwrap(); // pin_count is now 0
        let result = bpm.unpin_page(page_id, false); // unpin again
        assert!(matches!(result, Err(OxidbError::BufferPool(_))));
        if let Err(OxidbError::BufferPool(msg)) = result {
            assert!(msg.contains("pin count is already zero"));
        } else {
            panic!("Expected BufferPool error");
        }
    }
}
