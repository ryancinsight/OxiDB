// src/event_engine/handler/processors.rs

// This file defines the "Soul" of the event handler:
// the individual capabilities and specific logic units for handling each event type.
// It starts with the definition of the `Processor` trait.

use super::types::{Event, EventResult}; // Adjusted path assuming types.rs is in the same directory (handler)

/// The `Processor` trait defines a common interface for all event processors.
/// Each concrete event type will have an associated struct that implements this trait.
pub trait Processor {
    /// Processes a given event.
    ///
    /// # Arguments
    /// * `event`: A reference to the `Event` to be processed.
    ///
    /// # Returns
    /// * `EventResult`: Ok(()) if processing is successful, or an error.
    fn process(&self, event: &Event) -> EventResult;
}

// Placeholder for concrete processor implementations.
// These will be added in a subsequent step.
// For example:
//
// pub struct UserCreatedProcessor;
// impl Processor for UserCreatedProcessor {
//     fn process(&self, event: &Event) -> EventResult {
//         if let Event::UserCreated { user_id, user_email } = event {
//             println!("UserCreatedProcessor: Handling event for user_id: {}, email: {}", user_id, user_email);
//             // Specific logic for UserCreated event
//             Ok(())
//         } else {
//             // This processor should only be called with UserCreated events.
//             // Handling this mismatch is part of the dispatch logic design.
//             // For now, returning an error or panicking might be options.
//             // Or, ensure dispatcher only calls the correct processor.
//             Err(anyhow::anyhow!("Mismatched event type for UserCreatedProcessor"))
//         }
//     }
// }
// Concrete Processor Implementations

// --- UserCreated Event Processor ---
pub struct UserCreatedProcessor;

impl Processor for UserCreatedProcessor {
    fn process(&self, event: &Event) -> EventResult {
        if let Event::UserCreated { user_id, user_email } = event {
            println!("UserCreatedProcessor: Handling UserCreated event");
            // Logic extracted from the original match statement in core.rs
            println!("User created: ID={user_id}, Email={user_email}");
            if user_email.contains("@example.com") {
                println!("Sending welcome email to example.com user: {user_email}");
            } else {
                println!("Sending standard welcome email to: {user_email}");
            }
            println!("Provisioning initial resources for user: {user_id}");
            println!("Notifying analytics service about new user: {user_id}");
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Mismatched event type for UserCreatedProcessor. Expected UserCreated."
            ))
        }
    }
}

// --- OrderPlaced Event Processor ---
pub struct OrderPlacedProcessor;

impl Processor for OrderPlacedProcessor {
    fn process(&self, event: &Event) -> EventResult {
        if let Event::OrderPlaced { order_id, amount } = event {
            println!("OrderPlacedProcessor: Handling OrderPlaced event");
            // Logic extracted from the original match statement in core.rs
            println!("Order placed: ID={order_id}, Amount={amount}");
            if *amount > 1000 {
                println!("Order {order_id} requires additional verification (amount > 1000)");
                if order_id.starts_with("ORD-SPECIAL-") {
                    println!("Special order {order_id} - bypassing some checks.");
                } else {
                    println!("Standard order {order_id} - performing full checks.");
                }
            } else {
                println!("Order {order_id} amount is within standard limits.");
            }
            println!("Updating inventory for order: {order_id}");
            println!("Sending order confirmation for: {order_id}");
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Mismatched event type for OrderPlacedProcessor. Expected OrderPlaced."
            ))
        }
    }
}

// --- NotificationSent Event Processor ---
pub struct NotificationSentProcessor;

impl Processor for NotificationSentProcessor {
    fn process(&self, event: &Event) -> EventResult {
        if let Event::NotificationSent { notification_id, recipient, message_type } = event {
            println!("NotificationSentProcessor: Handling NotificationSent event");
            // Logic extracted from the original match statement in core.rs
            println!(
                "Notification sent: ID={notification_id}, Recipient={recipient}, Type={message_type}"
            );
            match message_type.as_str() {
                "PasswordReset" => {
                    println!("Logging password reset notification: {notification_id}");
                }
                "TwoFactorAuth" => {
                    println!("Logging 2FA notification: {notification_id}");
                }
                _ => {
                    println!("Logging generic notification: {notification_id}");
                }
            }
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Mismatched event type for NotificationSentProcessor. Expected NotificationSent."
            ))
        }
    }
}

// --- DataUpdated Event Processor ---
pub struct DataUpdatedProcessor;

impl Processor for DataUpdatedProcessor {
    fn process(&self, event: &Event) -> EventResult {
        if let Event::DataUpdated { resource_id, old_value, new_value, changed_by } = event {
            println!("DataUpdatedProcessor: Handling DataUpdated event");
            // Logic extracted from the original match statement in core.rs
            println!("Data updated: ResourceID={resource_id}, ChangedBy={changed_by}");
            println!("Old value: '{old_value}', New value: '{new_value}'");
            if old_value.len() > new_value.len() && new_value.is_empty() {
                println!(
                    "Warning: Value for {resource_id} was potentially cleared by {changed_by}"
                );
            }
            println!("Creating audit log for resource {resource_id}: changed by {changed_by}");
            println!("Notifying subscribers about update to resource {resource_id}");
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Mismatched event type for DataUpdatedProcessor. Expected DataUpdated."
            ))
        }
    }
}
