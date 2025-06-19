use crate::core::common::OxidbError; // Changed

/// Represents the type of pointer to the data's location.
/// For SimpleFileKvStore, this could be a byte offset in the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)] // Derive what's needed
pub struct DataPointer(pub u64); // Example: u64 for file offset

/// Trait for an index structure that maps keys to data pointers.
pub trait Index<K, P> {
    /// Inserts or updates an entry in the index.
    fn insert_entry(&mut self, key: K, ptr: P) -> Result<(), OxidbError>; // Changed

    /// Finds the pointer associated with a key.
    fn find_entry(&self, key: &K) -> Result<Option<P>, OxidbError>; // Changed

    /// Deletes an entry from the index.
    fn delete_entry(&mut self, key: &K) -> Result<Option<P>, OxidbError>; // Changed

    // (Optional) Returns all entries, e.g., for rebuilding or iteration.
    // fn get_all_entries(&self) -> Result<Vec<(K, P)>, OxidbError>; // Changed
}
