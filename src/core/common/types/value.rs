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
    /// Implements partial comparison for Value types.
    /// 
    /// When comparing integers with floats, this implementation uses robust
    /// precision-aware comparison that correctly handles large integers that
    /// cannot be precisely represented as f64. This prevents incorrect query
    /// results that could occur with naive casting approaches.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => a.partial_cmp(b),
            (Self::Float(a), Self::Float(b)) => a.partial_cmp(b),
            // Robust comparison for Integer vs Float
            (Self::Integer(a), Self::Float(b)) => {
                if b.is_nan() {
                    return None;
                }
                // If `a` can be precisely represented as an `f64`, perform a direct float comparison.
                if *a == (*a as f64) as i64 {
                    return (*a as f64).partial_cmp(b);
                }

                // `a` is a large integer that loses precision. Compare against `b`'s parts.
                let b_trunc = b.trunc();
                if b_trunc > i64::MAX as f64 { return Some(Ordering::Less); }
                if b_trunc < i64::MIN as f64 { return Some(Ordering::Greater); }

                match a.cmp(&(b_trunc as i64)) {
                    Ordering::Equal => b.fract().partial_cmp(&0.0).map(std::cmp::Ordering::reverse),
                    other => Some(other),
                }
            }
            // Robust comparison for Float vs Integer
            (Self::Float(a), Self::Integer(b)) => {
                if a.is_nan() {
                    return None;
                }
                // If `b` can be precisely represented as an `f64`, perform a direct float comparison.
                if *b == (*b as f64) as i64 {
                    return a.partial_cmp(&(*b as f64));
                }

                // `b` is a large integer that loses precision. Compare `a` against it.
                let a_trunc = a.trunc();
                if a_trunc > i64::MAX as f64 { return Some(Ordering::Greater); }
                if a_trunc < i64::MIN as f64 { return Some(Ordering::Less); }

                match (a_trunc as i64).cmp(b) {
                    Ordering::Equal => a.fract().partial_cmp(&0.0),
                    other => Some(other),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn test_integer_float_comparison_precision() {
        // Test case where precision is maintained
        let small_int = Value::Integer(42);
        let float_val = Value::Float(42.5);
        assert_eq!(small_int.partial_cmp(&float_val), Some(Ordering::Less));

        // Test the critical edge case: 2^53 + 1
        // This integer cannot be precisely represented as f64
        let large_int = Value::Integer((1i64 << 53) + 1); // 9_007_199_254_740_993
        let exact_float = Value::Float((1i64 << 53) as f64); // 9_007_199_254_740_992.0
        
        // The integer should be greater than the float, not equal
        assert_eq!(large_int.partial_cmp(&exact_float), Some(Ordering::Greater));
        
        // Test the reverse comparison
        assert_eq!(exact_float.partial_cmp(&large_int), Some(Ordering::Less));

        // Test with fractional part
        let float_with_fract = Value::Float(9_007_199_254_740_992.5);
        assert_eq!(large_int.partial_cmp(&float_with_fract), Some(Ordering::Greater));
        assert_eq!(float_with_fract.partial_cmp(&large_int), Some(Ordering::Less));

        // Test edge case where float fractional part makes it larger
        let float_larger = Value::Float(9_007_199_254_740_993.1);
        assert_eq!(large_int.partial_cmp(&float_larger), Some(Ordering::Less));
        assert_eq!(float_larger.partial_cmp(&large_int), Some(Ordering::Greater));
    }

    #[test]
    fn test_large_negative_integer_comparison() {
        // Test large negative integers
        let large_neg_int = Value::Integer(-((1i64 << 53) + 1));
        let neg_float = Value::Float(-((1i64 << 53) as f64));
        
        assert_eq!(large_neg_int.partial_cmp(&neg_float), Some(Ordering::Less));
        assert_eq!(neg_float.partial_cmp(&large_neg_int), Some(Ordering::Greater));
    }

    #[test]
    fn test_float_bounds_comparison() {
        // Test floats that exceed i64 range
        let max_int = Value::Integer(i64::MAX);
        let large_float = Value::Float(i64::MAX as f64 + 1e20);
        
        assert_eq!(max_int.partial_cmp(&large_float), Some(Ordering::Less));
        assert_eq!(large_float.partial_cmp(&max_int), Some(Ordering::Greater));

        let min_int = Value::Integer(i64::MIN);
        let small_float = Value::Float(i64::MIN as f64 - 1e20);
        
        assert_eq!(min_int.partial_cmp(&small_float), Some(Ordering::Greater));
        assert_eq!(small_float.partial_cmp(&min_int), Some(Ordering::Less));
    }

    #[test]
    fn test_nan_comparison() {
        let int_val = Value::Integer(42);
        let nan_val = Value::Float(f64::NAN);
        
        assert_eq!(int_val.partial_cmp(&nan_val), None);
        assert_eq!(nan_val.partial_cmp(&int_val), None);
    }

    #[test]
    fn test_precise_integer_comparison() {
        // Test integers that can be precisely represented as f64
        let precise_int = Value::Integer(1024);
        let precise_float = Value::Float(1024.0);
        
        assert_eq!(precise_int.partial_cmp(&precise_float), Some(Ordering::Equal));
        assert_eq!(precise_float.partial_cmp(&precise_int), Some(Ordering::Equal));
        
        let precise_float_larger = Value::Float(1024.1);
        assert_eq!(precise_int.partial_cmp(&precise_float_larger), Some(Ordering::Less));
        assert_eq!(precise_float_larger.partial_cmp(&precise_int), Some(Ordering::Greater));
    }
}
