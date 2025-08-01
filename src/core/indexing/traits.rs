use crate::core::common::OxidbError; // Changed
use crate::core::query::commands::Key as PrimaryKey; // Alias for clarity
use crate::core::query::commands::Value; // Using Value for flexibility in indexed values
use std::fmt::Debug; // Import Debug

/// Trait for secondary indexes.
///
/// A secondary index maps values of a specific column (or a set of columns)
/// to the primary keys of the rows containing those values.
pub trait Index: Debug {
    // Add Debug as a supertrait
    /// Returns the name of the index.
    fn name(&self) -> &str;

    /// Inserts a new entry into the index.
    ///
    /// # Arguments
    ///
    /// * `value` - The value being indexed (e.g., content of an indexed column).
    /// * `primary_key` - The primary key of the row containing this value.
    ///
    /// # Errors
    ///
    /// Returns `OxidbError` if the insertion fails (e.g., due to I/O issues when persisting).
    fn insert(&mut self, value: &Value, primary_key: &PrimaryKey) -> Result<(), OxidbError>; // Changed

    /// Deletes an entry from the index.
    ///
    /// Note: Depending on the index implementation (e.g., if it allows multiple primary keys
    /// per indexed value), this might remove a specific value-primary_key pair or
    /// all entries for a given value if the `primary_key` is not specific enough.
    /// For a simple hash index mapping a value to a list of PKs, it would remove the PK from the list.
    ///
    /// # Arguments
    ///
    /// * `value` - The value of the index entry to delete.
    /// * `primary_key` - The primary key associated with the value (optional, depends on index type).
    ///
    /// # Errors
    ///
    /// Returns `OxidbError` if the deletion fails.
    fn delete(&mut self, value: &Value, primary_key: Option<&PrimaryKey>)
        -> Result<(), OxidbError>; // Changed

    /// Finds primary keys associated with a given indexed value.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to search for in the index.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `Vec<PrimaryKey>` of matching primary keys if found,
    /// or `None` if the value is not in the index.
    /// Returns `OxidbError` if the lookup fails.
    fn find(&self, value: &Value) -> Result<Option<Vec<PrimaryKey>>, OxidbError>; // Changed

    /// Saves the index data to persistent storage.
    /// The specific storage mechanism (e.g., file path) should be managed by the
    /// implementing struct.
    fn save(&self) -> Result<(), OxidbError>; // Changed

    /// Loads the index data from persistent storage.
    fn load(&mut self) -> Result<(), OxidbError>; // Changed

    /// Updates an index entry.
    /// This typically involves removing the old index entry (if the indexed value changed)
    /// and inserting the new one.
    ///
    /// # Arguments
    ///
    /// * `old_value_for_index` - The old value that was indexed.
    /// * `new_value_for_index` - The new value to be indexed.
    /// * `primary_key` - The primary key of the row being updated.
    ///
    /// # Errors
    ///
    /// Returns `OxidbError` if the update fails.
    fn update(
        &mut self,
        old_value_for_index: &Value,
        new_value_for_index: &Value,
        primary_key: &PrimaryKey,
    ) -> Result<(), OxidbError>; // Changed
}
