pub mod in_memory; // Keep in_memory declaration
pub mod simple_file; // Add simple_file module

pub use in_memory::InMemoryKvStore;
pub use simple_file::SimpleFileKvStore; // Expose SimpleFileKvStore

#[cfg(test)]
mod tests;
