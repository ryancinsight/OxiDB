pub mod in_memory; // Keep in_memory declaration
pub mod file_storage; // Add file_storage module

pub use in_memory::InMemoryKvStore;
pub use file_storage::SimpleFileKvStore; // Expose SimpleFileKvStore

#[cfg(test)]
mod tests;
