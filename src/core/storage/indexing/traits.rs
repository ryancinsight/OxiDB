use crate::core::common::error::DbError;

/// Represents the type of pointer to the data's location.
/// For SimpleFileKvStore, this could be a byte offset in the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)] // Derive what's needed
pub struct DataPointer(pub u64); // Example: u64 for file offset

/// Trait for an index structure that maps keys to data pointers.
pub trait Index<K, P> {
    /// Inserts or updates an entry in the index.
    fn insert_entry(&mut self, key: K, ptr: P) -> Result<(), DbError>;

    /// Finds the pointer associated with a key.
    fn find_entry(&self, key: &K) -> Result<Option<P>, DbError>;

    /// Deletes an entry from the index.
    fn delete_entry(&mut self, key: &K) -> Result<Option<P>, DbError>; // Returns old pointer if existed

    // (Optional) Returns all entries, e.g., for rebuilding or iteration.
    // fn get_all_entries(&self) -> Result<Vec<(K, P)>, DbError>;
}
