pub mod in_memory; // Keep in_memory declaration
pub mod file; // Renamed from simple_file

pub use in_memory::InMemoryKvStore;
pub use file::FileKvStore; // Expose FileKvStore

#[cfg(test)]
mod tests;
