// src/event_engine/handler/tests.rs

// This file serves as the "Immune System" for the event handler,
// ensuring its components function correctly and maintain integrity.

// Removed outer #[cfg(test)] and mod tests { ... } wrapper
use super::core::process_event;
use super::processors::{
    DataUpdatedProcessor,
    NotificationSentProcessor,
    OrderPlacedProcessor,
    Processor, // The trait
    UserCreatedProcessor,
};
use super::types::Event; // Event types // The refactored process_event function

// Helper to create a UserCreated event
fn user_created_event_sample() -> Event {
    Event::UserCreated {
        user_id: "user123".to_string(),
        user_email: "test@example.com".to_string(),
    }
}

// Helper to create an OrderPlaced event
fn order_placed_event_sample() -> Event {
    Event::OrderPlaced { order_id: "order456".to_string(), amount: 750 }
}

// Helper to create an OrderPlaced event with large amount
fn order_placed_event_large_amount_sample() -> Event {
    Event::OrderPlaced { order_id: "order789-special".to_string(), amount: 1500 }
}

// Helper to create a NotificationSent event
fn notification_sent_event_sample() -> Event {
    Event::NotificationSent {
        notification_id: "notify001".to_string(),
        recipient: "recipient@example.com".to_string(),
        message_type: "PasswordReset".to_string(),
    }
}

// Helper to create a DataUpdated event
fn data_updated_event_sample() -> Event {
    Event::DataUpdated {
        resource_id: "resX".to_string(),
        old_value: "old_data".to_string(),
        new_value: "new_data".to_string(),
        changed_by: "admin".to_string(),
    }
}

// --- Tests for Individual Processors ---

#[test]
fn test_user_created_processor() {
    let processor = UserCreatedProcessor;
    let event = user_created_event_sample();
    let result = processor.process(&event);
    assert!(result.is_ok(), "UserCreatedProcessor failed: {:?}", result.err());
    // Add more assertions here if the processors had more complex outputs
    // For now, we rely on the println! statements for behavior verification,
    // which is not ideal for automated tests but matches current impl.
}

#[test]
fn test_order_placed_processor() {
    let processor = OrderPlacedProcessor;
    let event = order_placed_event_sample();
    let result = processor.process(&event);
    assert!(result.is_ok(), "OrderPlacedProcessor failed: {:?}", result.err());
}

#[test]
fn test_notification_sent_processor() {
    let processor = NotificationSentProcessor;
    let event = notification_sent_event_sample();
    let result = processor.process(&event);
    assert!(result.is_ok(), "NotificationSentProcessor failed: {:?}", result.err());
}

#[test]
fn test_data_updated_processor() {
    let processor = DataUpdatedProcessor;
    let event = data_updated_event_sample();
    let result = processor.process(&event);
    assert!(result.is_ok(), "DataUpdatedProcessor failed: {:?}", result.err());
}

#[test]
fn test_processor_mismatched_event() {
    let processor = UserCreatedProcessor;
    let event = order_placed_event_sample(); // Mismatched event
    let result = processor.process(&event);
    assert!(result.is_err(), "UserCreatedProcessor should fail on mismatched event");
    if let Err(e) = result {
        assert!(e.to_string().contains("Mismatched event type for UserCreatedProcessor"));
    }
}

// --- Tests for the refactored process_event (Dispatch Logic) ---

#[test]
fn test_process_event_dispatches_to_user_created() {
    let event = user_created_event_sample();
    let result = process_event(&event); // Uses the refactored core::process_event
    assert!(result.is_ok(), "process_event failed for UserCreated: {:?}", result.err());
    // This implicitly tests if the correct processor was called.
    // More advanced tests could use mocks or check side effects if they were more complex.
}

#[test]
fn test_process_event_dispatches_to_order_placed() {
    let event = order_placed_event_sample();
    let result = process_event(&event);
    assert!(result.is_ok(), "process_event failed for OrderPlaced: {:?}", result.err());
}

#[test]
fn test_process_event_dispatches_to_order_placed_large_amount() {
    let event = order_placed_event_large_amount_sample();
    let result = process_event(&event);
    assert!(
        result.is_ok(),
        "process_event failed for OrderPlaced (large amount): {:?}",
        result.err()
    );
}

#[test]
fn test_process_event_dispatches_to_notification_sent() {
    let event = notification_sent_event_sample();
    let result = process_event(&event);
    assert!(result.is_ok(), "process_event failed for NotificationSent: {:?}", result.err());
}

#[test]
fn test_process_event_dispatches_to_data_updated() {
    let event = data_updated_event_sample();
    let result = process_event(&event);
    assert!(result.is_ok(), "process_event failed for DataUpdated: {:?}", result.err());
}
// Removed outer }
