// src/lib.rs
pub mod core;
pub mod api;

// Optional: Re-export key types/traits for easier use by library consumers later
// pub use crate::core::common::error::DbError;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
