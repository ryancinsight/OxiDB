// src/event_engine/handler/core.rs

// This file represents the "Mind" of the event handler.
// It contains the central decision-making logic for processing events.
// This version is refactored to use the Processor trait for "Flat Logic".

use super::types::{Event, EventResult};
use super::processors::{
    Processor, // The trait
    UserCreatedProcessor,
    OrderPlacedProcessor,
    NotificationSentProcessor,
    DataUpdatedProcessor,
}; // Concrete processor types

/// Selects and returns the appropriate event processor for a given event.
///
/// This function uses dynamic dispatch (`Box<dyn Processor>`) to return
/// a trait object, allowing for flexible processor selection.
fn get_processor_for_event(event: &Event) -> Box<dyn Processor> {
    match event {
        Event::UserCreated { .. } => Box::new(UserCreatedProcessor),
        Event::OrderPlaced { .. } => Box::new(OrderPlacedProcessor),
        Event::NotificationSent { .. } => Box::new(NotificationSentProcessor),
        Event::DataUpdated { .. } => Box::new(DataUpdatedProcessor),
        // Note: If new events are added to the Event enum,
        // this match statement MUST be updated to include them,
        // otherwise, it will result in a compile-time error due to
        // non-exhaustive matching, which is a good safety feature.
    }
}

/// Processes an incoming event using a refactored, flattened logic.
///
/// This function now delegates the actual processing logic to specialized
/// `Processor` implementations, adhering to the "Flat Logic" principle.
pub fn process_event(event: &Event) -> EventResult {
    println!("Processing event via dynamic dispatch: {:?}", event); // Updated logging

    // 1. Get the appropriate processor for the event.
    let processor = get_processor_for_event(event);

    // 2. Call the processor's process method.
    // The specific logic for the event is now encapsulated within the processor.
    processor.process(event)
}
