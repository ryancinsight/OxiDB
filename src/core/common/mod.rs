pub mod bincode_compat; // Pure Rust binary serialization
pub mod byteorder; // Pure Rust byte order handling
pub mod cow_utils; // Performance optimizations using Copy-on-Write
pub mod crc32; // Pure Rust CRC32 implementation
pub mod error; // Consolidated error handling
pub mod hex; // Pure Rust hex encoding/decoding
pub mod io_utils; // IO utilities following DRY principle
pub mod lock_utils; // Lock error handling utilities
pub mod result_utils; // New result utilities module
pub mod serialization;
pub mod traits;
pub mod types;

pub use error::OxidbError;
pub use result_utils::{retry_with_backoff, IntoOxidbError, ResultExt};

#[cfg(test)]
mod tests {
    mod error_tests;
}

#[cfg(test)]
pub use result_utils::TestResultExt;
