#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)] // Add derives as needed
pub struct PageId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransactionId(pub u64);

// Potentially define a generic Value type or enum here later
// pub enum Value {
//     Integer(i64),
//     String(String),
//     Boolean(bool),
//     // ...
// }
