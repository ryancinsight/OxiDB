pub mod error;
pub use error::OxidbError;
pub mod serialization;
pub mod traits;
pub mod types;

#[cfg(test)]
mod tests {
    mod error_tests;
}
