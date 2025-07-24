use super::data_type::DataType;
use std::cmp::Ordering; // Required for Ordering::Equal

/// Represents a value that can be stored in the database.
/// 
/// This enum encompasses all the data types that the database can handle,
/// including integers, floating-point numbers, text, booleans, binary data,
/// vectors for similarity search, and null values.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Value {
    /// A 64-bit signed integer value
    Integer(i64),
    /// A 64-bit floating-point value
    Float(f64),
    /// A UTF-8 text string
    Text(String),
    /// A boolean value (true or false)
    Boolean(bool),
    /// Binary large object (arbitrary byte data)
    Blob(Vec<u8>),
    /// A vector of 32-bit floating-point numbers for similarity search
    Vector(Vec<f32>),
    /// Represents a null/missing value
    Null,
}

impl Value {
    /// Returns the data type of this value.
    /// 
    /// # Examples
    /// 
    /// ```
    /// use oxidb::Value;
    /// use oxidb::core::common::types::DataType;
    /// 
    /// let val = Value::Integer(42);
    /// assert_eq!(val.get_type(), DataType::Integer);
    /// ```
    #[must_use]
    pub const fn get_type(&self) -> DataType {
        match self {
            Self::Integer(_) => DataType::Integer,
            Self::Float(_) => DataType::Float,
            Self::Text(_) => DataType::Text,
            Self::Boolean(_) => DataType::Boolean,
            Self::Blob(_) => DataType::Blob,
            Self::Vector(_) => DataType::Vector(None), // Or determine dimension if stored
            Self::Null => DataType::Null,
        }
    }
}

impl PartialOrd for Value {
    /// Compares two values for ordering.
    /// 
    /// Returns `None` if the values are not comparable (different types except
    /// for integer/float conversions, or when comparing vectors).
    /// 
    /// # Precision Considerations
    /// 
    /// When comparing integers with floats, integers are converted to floats
    /// using safe conversion methods that preserve as much precision as possible.
    /// However, very large integers (beyond f64's mantissa precision) may lose
    /// precision in the comparison.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => a.partial_cmp(b),
            (Self::Float(a), Self::Float(b)) => a.partial_cmp(b),
            // Use safe conversion that checks for precision loss
            (Self::Integer(a), Self::Float(b)) => {
                // Check if the integer can be exactly represented as f64
                if *a == (*a as f64) as i64 {
                    (*a as f64).partial_cmp(b)
                } else {
                    // For very large integers, use a more careful comparison
                    let a_float = *a as f64;
                    if a_float.is_infinite() {
                        if *a > 0 { Some(Ordering::Greater) } else { Some(Ordering::Less) }
                    } else {
                        a_float.partial_cmp(b)
                    }
                }
            }
            (Self::Float(a), Self::Integer(b)) => {
                // Check if the integer can be exactly represented as f64
                if *b == (*b as f64) as i64 {
                    a.partial_cmp(&(*b as f64))
                } else {
                    // For very large integers, use a more careful comparison
                    let b_float = *b as f64;
                    if b_float.is_infinite() {
                        if *b > 0 { Some(Ordering::Less) } else { Some(Ordering::Greater) }
                    } else {
                        a.partial_cmp(&b_float)
                    }
                }
            }
            (Self::Text(a), Self::Text(b)) => a.partial_cmp(b),
            (Self::Boolean(a), Self::Boolean(b)) => a.partial_cmp(b),
            (Self::Blob(a), Self::Blob(b)) => a.partial_cmp(b),
            // Vector comparison is not straightforward (e.g., lexicographical, magnitude)
            // For now, vectors are not comparable for ordering purposes
            (Self::Vector(_), Self::Vector(_)) => None,
            (Self::Null, Self::Null) => Some(Ordering::Equal),
            // All other combinations are non-compatible
            _ => None,
        }
    }
}
