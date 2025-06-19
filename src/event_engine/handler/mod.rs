// src/event_engine/handler/mod.rs

// This is the "EventHandler" submodule, a critical part of the "EventEngine" wing.
// It's responsible for the direct handling and processing of individual events.

// According to the Cathedral Engineering manifesto, this module will be further
// decomposed into its functional aspects:
// - "types.rs": Event definitions, result types (The "Skeleton" - data structures)
// - "core.rs": Main event processing logic (The "Mind" - decision making)
// - "processors.rs": Specific event processing implementations (The "Soul" - individual capabilities)
// - "tests.rs": Unit tests (The "Immune System" - integrity checks)

pub mod types;
pub mod core;
pub mod processors;

// Re-export key components for easier use by the parent `event_engine` module
// or other parts of the system that might interact with event handling.
pub use types::{Event, EventResult}; // Event and EventResult will be defined in types.rs
pub use core::process_event;       // process_event will be defined in core.rs
pub use processors::Processor; // Processor trait will be defined in processors.rs - uncomment when defined

#[cfg(test)]
mod tests;
