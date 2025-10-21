//! Zero-copy value view for efficient value access without copying
//!
//! Provides view types for accessing Value contents efficiently,
//! enabling zero-copy operations on database values.

use crate::core::common::types::Value;
use std::borrow::Cow;

/// Zero-copy string view that can be either borrowed or owned
pub type StringView<'a> = Cow<'a, str>;

/// Zero-copy bytes view that can be either borrowed or owned
pub type BytesView<'a> = Cow<'a, [u8]>;

/// Value view that provides zero-copy access to Value contents
///
/// Provides efficient access to value data without copying for scalar types
/// and with minimal copying for complex types using Cow.
#[derive(Debug, Clone)]
pub enum ValueView<'a> {
    Integer(i64),
    Float(f64),
    Text(StringView<'a>),
    Boolean(bool),
    Blob(BytesView<'a>),
    Vector(&'a [f32]),
    Null,
}

impl<'a> ValueView<'a> {
    /// Create a value view from a Value reference
    ///
    /// # Arguments
    /// * `value` - Reference to the value to create a view for
    ///
    /// # Returns
    /// ValueView that provides zero-copy access to the value contents
    ///
    /// # Examples
    /// ```
    /// use oxidb::core::zero_cost::views::ValueView;
    /// use oxidb::core::common::types::Value;
    ///
    /// let value = Value::Integer(42);
    /// let view = ValueView::from_value(&value);
    /// assert_eq!(view.as_integer(), Some(42));
    /// ```
    pub fn from_value(value: &'a Value) -> Self {
        match value {
            Value::Integer(i) => ValueView::Integer(*i),
            Value::Float(f) => ValueView::Float(*f),
            Value::Text(s) => ValueView::Text(Cow::Borrowed(s)),
            Value::Boolean(b) => ValueView::Boolean(*b),
            Value::Blob(b) => ValueView::Blob(Cow::Borrowed(b)),
            Value::Vector(v) => ValueView::Vector(v),
            Value::Null => ValueView::Null,
        }
    }

    /// Check if the value is null
    ///
    /// # Returns
    /// `true` if the value is null, `false` otherwise
    #[inline]
    pub const fn is_null(&self) -> bool {
        matches!(self, ValueView::Null)
    }

    /// Try to get as integer
    ///
    /// # Returns
    /// * `Some(i64)` - If the value is an integer
    /// * `None` - If the value is not an integer
    #[inline]
    pub const fn as_integer(&self) -> Option<i64> {
        match self {
            ValueView::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Try to get as float
    ///
    /// # Returns
    /// * `Some(f64)` - If the value is a float
    /// * `None` - If the value is not a float
    #[inline]
    pub const fn as_float(&self) -> Option<f64> {
        match self {
            ValueView::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Try to get as string
    ///
    /// # Returns
    /// * `Some(&str)` - If the value is text
    /// * `None` - If the value is not text
    #[inline]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            ValueView::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as boolean
    ///
    /// # Returns
    /// * `Some(bool)` - If the value is a boolean
    /// * `None` - If the value is not a boolean
    #[inline]
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            ValueView::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to get as bytes
    ///
    /// # Returns
    /// * `Some(&[u8])` - If the value is a blob
    /// * `None` - If the value is not a blob
    #[inline]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            ValueView::Blob(b) => Some(b),
            _ => None,
        }
    }

    /// Try to get as vector
    ///
    /// # Returns
    /// * `Some(&[f32])` - If the value is a vector
    /// * `None` - If the value is not a vector
    #[inline]
    pub const fn as_vector(&self) -> Option<&[f32]> {
        match self {
            ValueView::Vector(v) => Some(v),
            _ => None,
        }
    }

    /// Get the type name of the value
    ///
    /// # Returns
    /// String representation of the value type
    pub fn type_name(&self) -> &'static str {
        match self {
            ValueView::Integer(_) => "Integer",
            ValueView::Float(_) => "Float",
            ValueView::Text(_) => "Text",
            ValueView::Boolean(_) => "Boolean",
            ValueView::Blob(_) => "Blob",
            ValueView::Vector(_) => "Vector",
            ValueView::Null => "Null",
        }
    }

    /// Check if the value is numeric (integer or float)
    ///
    /// # Returns
    /// `true` if the value is numeric, `false` otherwise
    #[inline]
    pub const fn is_numeric(&self) -> bool {
        matches!(self, ValueView::Integer(_) | ValueView::Float(_))
    }

    /// Check if the value is textual
    ///
    /// # Returns
    /// `true` if the value is text, `false` otherwise
    #[inline]
    pub const fn is_text(&self) -> bool {
        matches!(self, ValueView::Text(_))
    }
}

impl<'a> PartialEq for ValueView<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ValueView::Integer(a), ValueView::Integer(b)) => a == b,
            (ValueView::Float(a), ValueView::Float(b)) => a == b,
            (ValueView::Text(a), ValueView::Text(b)) => a == b,
            (ValueView::Boolean(a), ValueView::Boolean(b)) => a == b,
            (ValueView::Blob(a), ValueView::Blob(b)) => a == b,
            (ValueView::Vector(a), ValueView::Vector(b)) => a == b,
            (ValueView::Null, ValueView::Null) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::common::types::Value;

    #[test]
    fn test_value_view_from_integer() {
        let value = Value::Integer(42);
        let view = ValueView::from_value(&value);

        assert_eq!(view.as_integer(), Some(42));
        assert!(!view.is_null());
        assert!(view.is_numeric());
        assert_eq!(view.type_name(), "Integer");
    }

    #[test]
    fn test_value_view_from_text() {
        let value = Value::Text("hello".to_string());
        let view = ValueView::from_value(&value);

        assert_eq!(view.as_str(), Some("hello"));
        assert!(!view.is_null());
        assert!(view.is_text());
        assert_eq!(view.type_name(), "Text");
    }

    #[test]
    fn test_value_view_from_float() {
        let value = Value::Float(3.14);
        let view = ValueView::from_value(&value);

        assert_eq!(view.as_float(), Some(3.14));
        assert!(!view.is_null());
        assert!(view.is_numeric());
        assert_eq!(view.type_name(), "Float");
    }

    #[test]
    fn test_value_view_from_boolean() {
        let value = Value::Boolean(true);
        let view = ValueView::from_value(&value);

        assert_eq!(view.as_bool(), Some(true));
        assert!(!view.is_null());
        assert!(!view.is_numeric());
        assert_eq!(view.type_name(), "Boolean");
    }

    #[test]
    fn test_value_view_from_blob() {
        let value = Value::Blob(vec![1, 2, 3, 4]);
        let view = ValueView::from_value(&value);

        assert_eq!(view.as_bytes(), Some(&[1, 2, 3, 4][..]));
        assert!(!view.is_null());
        assert_eq!(view.type_name(), "Blob");
    }

    #[test]
    fn test_value_view_from_vector() {
        let value = Value::Vector(vec![1.0, 2.0, 3.0]);
        let view = ValueView::from_value(&value);

        assert_eq!(view.as_vector(), Some(&[1.0, 2.0, 3.0][..]));
        assert!(!view.is_null());
        assert_eq!(view.type_name(), "Vector");
    }

    #[test]
    fn test_value_view_from_null() {
        let value = Value::Null;
        let view = ValueView::from_value(&value);

        assert!(view.is_null());
        assert_eq!(view.as_integer(), None);
        assert_eq!(view.as_str(), None);
        assert_eq!(view.type_name(), "Null");
    }

    #[test]
    fn test_value_view_type_checking() {
        let int_value = Value::Integer(42);
        let int_view = ValueView::from_value(&int_value);

        // Should only match integer accessor
        assert!(int_view.as_integer().is_some());
        assert!(int_view.as_float().is_none());
        assert!(int_view.as_str().is_none());
        assert!(int_view.as_bool().is_none());
        assert!(int_view.as_bytes().is_none());
        assert!(int_view.as_vector().is_none());
    }

    #[test]
    fn test_value_view_equality() {
        let value1 = Value::Integer(42);
        let value2 = Value::Integer(42);
        let value3 = Value::Integer(43);

        let view1 = ValueView::from_value(&value1);
        let view2 = ValueView::from_value(&value2);
        let view3 = ValueView::from_value(&value3);

        assert_eq!(view1, view2);
        assert_ne!(view1, view3);
    }

    #[test]
    fn test_string_view_cow() {
        let borrowed_str = "hello";
        let string_view: StringView = Cow::Borrowed(borrowed_str);
        assert_eq!(&*string_view, "hello");

        let owned_string = "world".to_string();
        let string_view: StringView = Cow::Owned(owned_string);
        assert_eq!(&*string_view, "world");
    }

    #[test]
    fn test_bytes_view_cow() {
        let borrowed_bytes = &[1, 2, 3, 4][..];
        let bytes_view: BytesView = Cow::Borrowed(borrowed_bytes);
        assert_eq!(&*bytes_view, &[1, 2, 3, 4]);

        let owned_bytes = vec![5, 6, 7, 8];
        let bytes_view: BytesView = Cow::Owned(owned_bytes);
        assert_eq!(&*bytes_view, &[5, 6, 7, 8]);
    }
}
