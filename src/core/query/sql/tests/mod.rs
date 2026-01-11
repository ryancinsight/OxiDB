// This file declares test modules for SQL components.

// Common test utilities
mod common;

// Organized test modules by SQL statement type
mod create_tests;
mod error_tests;
mod parse_tests;
mod select_tests;
mod translate_tests;
mod update_tests;
mod column_comparison;

// Legacy large test file (to be removed after migration)
pub mod parser_tests;
