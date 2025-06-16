use std::fmt;
use std::ops::AddAssign;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct PageId(pub u64);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct TransactionId(pub u64);

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AddAssign<u64> for TransactionId {
    #[allow(clippy::arithmetic_side_effects)]
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
    }
}

// Removed Lsn struct from here, as it's defined as `pub type Lsn = u64;` in types/mod.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SlotId(pub u16);
