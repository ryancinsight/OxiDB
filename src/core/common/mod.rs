pub mod cow_utils; // Performance optimizations using Copy-on-Write
pub mod error; // Consolidated error handling
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
