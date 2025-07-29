// src/core/zero_cost/borrowed.rs
//! Borrowed data abstractions that minimize allocations and enable zero-copy operations

use std::borrow::Cow;
use std::marker::PhantomData;
use std::ops::Deref;

/// Zero-copy borrowed slice with compile-time guarantees
#[derive(Debug)]
pub struct BorrowedSlice<'a, T> {
    data: &'a [T],
    _phantom: PhantomData<&'a T>,
}

impl<'a, T> BorrowedSlice<'a, T> {
    /// Create a new borrowed slice
    #[inline]
    pub const fn new(data: &'a [T]) -> Self {
        Self {
            data,
            _phantom: PhantomData,
        }
    }
    
    /// Get the length
    #[inline]
    pub const fn len(&self) -> usize {
        self.data.len()
    }
    
    /// Check if empty
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    
    /// Get element by index
    #[inline]
    pub fn get(&self, index: usize) -> Option<&'a T> {
        self.data.get(index)
    }
    
    /// Create a sub-slice
    #[inline]
    pub fn slice(&self, start: usize, end: usize) -> BorrowedSlice<'a, T> {
        BorrowedSlice::new(&self.data[start..end])
    }
    
    /// Iterate over elements
    #[inline]
    pub fn iter(&self) -> std::slice::Iter<'a, T> {
        self.data.iter()
    }
    
    /// Split at index
    #[inline]
    pub fn split_at(&self, mid: usize) -> (BorrowedSlice<'a, T>, BorrowedSlice<'a, T>) {
        let (left, right) = self.data.split_at(mid);
        (BorrowedSlice::new(left), BorrowedSlice::new(right))
    }
    
    /// Get first element
    #[inline]
    pub fn first(&self) -> Option<&'a T> {
        self.data.first()
    }
    
    /// Get last element
    #[inline]
    pub fn last(&self) -> Option<&'a T> {
        self.data.last()
    }
    
    /// Create windows of specified size
    pub fn windows(&self, size: usize) -> impl Iterator<Item = BorrowedSlice<'a, T>> {
        self.data.windows(size).map(BorrowedSlice::new)
    }
    
    /// Create chunks of specified size
    pub fn chunks(&self, chunk_size: usize) -> impl Iterator<Item = BorrowedSlice<'a, T>> {
        self.data.chunks(chunk_size).map(BorrowedSlice::new)
    }
}

impl<'a, T> Deref for BorrowedSlice<'a, T> {
    type Target = [T];
    
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.data
    }
}

impl<'a, T> AsRef<[T]> for BorrowedSlice<'a, T> {
    #[inline]
    fn as_ref(&self) -> &[T] {
        self.data
    }
}

impl<'a, T> IntoIterator for BorrowedSlice<'a, T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    
    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.data.iter()
    }
}

/// Zero-copy borrowed string with optional interning
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BorrowedStr<'a> {
    /// Borrowed from existing string
    Borrowed(&'a str),
    /// Statically allocated (interned)
    Static(&'static str),
}

impl<'a> BorrowedStr<'a> {
    /// Create from borrowed string
    #[inline]
    pub const fn borrowed(s: &'a str) -> Self {
        Self::Borrowed(s)
    }
    
    /// Create from static string
    #[inline]
    pub const fn static_str(s: &'static str) -> Self {
        Self::Static(s)
    }
    
    /// Get string slice
    #[inline]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Borrowed(s) => s,
            Self::Static(s) => s,
        }
    }
    
    /// Get length
    #[inline]
    pub fn len(&self) -> usize {
        self.as_str().len()
    }
    
    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.as_str().is_empty()
    }
    
    /// Check if static
    #[inline]
    pub const fn is_static(&self) -> bool {
        matches!(self, Self::Static(_))
    }
    
    /// Convert to owned string
    #[inline]
    pub fn to_owned(&self) -> String {
        self.as_str().to_owned()
    }
    
    /// Create a Cow from this borrowed string
    #[inline]
    pub fn to_cow(&self) -> Cow<'a, str> {
        match self {
            Self::Borrowed(s) => Cow::Borrowed(s),
            Self::Static(s) => Cow::Borrowed(s),
        }
    }
}

impl<'a> AsRef<str> for BorrowedStr<'a> {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl<'a> std::fmt::Display for BorrowedStr<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl<'a> From<&'a str> for BorrowedStr<'a> {
    #[inline]
    fn from(s: &'a str) -> Self {
        Self::Borrowed(s)
    }
}

/// Zero-copy borrowed bytes
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BorrowedBytes<'a> {
    data: &'a [u8],
}

impl<'a> BorrowedBytes<'a> {
    /// Create from borrowed bytes
    #[inline]
    pub const fn new(data: &'a [u8]) -> Self {
        Self { data }
    }
    
    /// Get byte slice
    #[inline]
    pub const fn as_bytes(&self) -> &'a [u8] {
        self.data
    }
    
    /// Get length
    #[inline]
    pub const fn len(&self) -> usize {
        self.data.len()
    }
    
    /// Check if empty
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    
    /// Get byte by index
    #[inline]
    pub fn get(&self, index: usize) -> Option<u8> {
        self.data.get(index).copied()
    }
    
    /// Create a sub-slice
    #[inline]
    pub fn slice(&self, start: usize, end: usize) -> BorrowedBytes<'a> {
        BorrowedBytes::new(&self.data[start..end])
    }
    
    /// Split at index
    #[inline]
    pub fn split_at(&self, mid: usize) -> (BorrowedBytes<'a>, BorrowedBytes<'a>) {
        let (left, right) = self.data.split_at(mid);
        (BorrowedBytes::new(left), BorrowedBytes::new(right))
    }
    
    /// Convert to owned bytes
    #[inline]
    pub fn to_vec(&self) -> Vec<u8> {
        self.data.to_vec()
    }
    
    /// Create a Cow from this borrowed bytes
    #[inline]
    pub fn to_cow(&self) -> Cow<'a, [u8]> {
        Cow::Borrowed(self.data)
    }
    
    /// Try to convert to UTF-8 string
    pub fn to_str(&self) -> Result<&'a str, std::str::Utf8Error> {
        std::str::from_utf8(self.data)
    }
}

impl<'a> AsRef<[u8]> for BorrowedBytes<'a> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.data
    }
}

impl<'a> Deref for BorrowedBytes<'a> {
    type Target = [u8];
    
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.data
    }
}

impl<'a> From<&'a [u8]> for BorrowedBytes<'a> {
    #[inline]
    fn from(data: &'a [u8]) -> Self {
        Self::new(data)
    }
}

impl<'a> From<&'a str> for BorrowedBytes<'a> {
    #[inline]
    fn from(s: &'a str) -> Self {
        Self::new(s.as_bytes())
    }
}

/// Zero-copy key-value pair for database operations
#[derive(Debug, Clone)]
pub struct BorrowedKeyValue<'a, K, V> {
    key: &'a K,
    value: &'a V,
}

impl<'a, K, V> BorrowedKeyValue<'a, K, V> {
    /// Create a new borrowed key-value pair
    #[inline]
    pub const fn new(key: &'a K, value: &'a V) -> Self {
        Self { key, value }
    }
    
    /// Get the key
    #[inline]
    pub const fn key(&self) -> &'a K {
        self.key
    }
    
    /// Get the value
    #[inline]
    pub const fn value(&self) -> &'a V {
        self.value
    }
    
    /// Destructure into key and value
    #[inline]
    pub const fn into_parts(self) -> (&'a K, &'a V) {
        (self.key, self.value)
    }
}

/// Zero-copy iterator over borrowed key-value pairs
#[derive(Debug)]
pub struct BorrowedKeyValueIter<'a, K, V> {
    keys: std::slice::Iter<'a, K>,
    values: std::slice::Iter<'a, V>,
}

impl<'a, K, V> BorrowedKeyValueIter<'a, K, V> {
    /// Create a new borrowed key-value iterator
    #[inline]
    pub fn new(keys: &'a [K], values: &'a [V]) -> Self {
        Self {
            keys: keys.iter(),
            values: values.iter(),
        }
    }
}

impl<'a, K, V> Iterator for BorrowedKeyValueIter<'a, K, V> {
    type Item = BorrowedKeyValue<'a, K, V>;
    
    fn next(&mut self) -> Option<Self::Item> {
        match (self.keys.next(), self.values.next()) {
            (Some(key), Some(value)) => Some(BorrowedKeyValue::new(key, value)),
            _ => None,
        }
    }
    
    fn size_hint(&self) -> (usize, Option<usize>) {
        let keys_hint = self.keys.size_hint();
        let values_hint = self.values.size_hint();
        let min = keys_hint.0.min(values_hint.0);
        let max = match (keys_hint.1, values_hint.1) {
            (Some(k), Some(v)) => Some(k.min(v)),
            _ => None,
        };
        (min, max)
    }
}

/// Zero-copy borrowed map view
#[derive(Debug)]
pub struct BorrowedMap<'a, K, V> {
    keys: &'a [K],
    values: &'a [V],
}

impl<'a, K, V> BorrowedMap<'a, K, V> {
    /// Create a new borrowed map view
    #[inline]
    pub fn new(keys: &'a [K], values: &'a [V]) -> Option<Self> {
        if keys.len() == values.len() {
            Some(Self { keys, values })
        } else {
            None
        }
    }
    
    /// Get the number of key-value pairs
    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }
    
    /// Check if the map is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
    
    /// Get value by index
    #[inline]
    pub fn get_by_index(&self, index: usize) -> Option<BorrowedKeyValue<'a, K, V>> {
        match (self.keys.get(index), self.values.get(index)) {
            (Some(key), Some(value)) => Some(BorrowedKeyValue::new(key, value)),
            _ => None,
        }
    }
    
    /// Find value by key (linear search)
    pub fn get<Q>(&self, key: &Q) -> Option<&'a V>
    where
        K: PartialEq<Q>,
    {
        self.keys
            .iter()
            .position(|k| k == key)
            .and_then(|index| self.values.get(index))
    }
    
    /// Iterate over key-value pairs
    #[inline]
    pub fn iter(&self) -> BorrowedKeyValueIter<'a, K, V> {
        BorrowedKeyValueIter::new(self.keys, self.values)
    }
    
    /// Get keys slice
    #[inline]
    pub const fn keys(&self) -> &'a [K] {
        self.keys
    }
    
    /// Get values slice
    #[inline]
    pub const fn values(&self) -> &'a [V] {
        self.values
    }
}

impl<'a, K, V> IntoIterator for BorrowedMap<'a, K, V> {
    type Item = BorrowedKeyValue<'a, K, V>;
    type IntoIter = BorrowedKeyValueIter<'a, K, V>;
    
    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Zero-copy borrowed option that avoids Option allocation overhead
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BorrowedOption<'a, T> {
    Some(&'a T),
    None,
}

impl<'a, T> BorrowedOption<'a, T> {
    /// Create a Some variant
    #[inline]
    pub const fn some(value: &'a T) -> Self {
        Self::Some(value)
    }
    
    /// Create a None variant
    #[inline]
    pub const fn none() -> Self {
        Self::None
    }
    
    /// Check if Some
    #[inline]
    pub const fn is_some(&self) -> bool {
        matches!(self, Self::Some(_))
    }
    
    /// Check if None
    #[inline]
    pub const fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }
    
    /// Unwrap the value (panics if None)
    #[inline]
    pub const fn unwrap(self) -> &'a T {
        match self {
            Self::Some(value) => value,
            Self::None => panic!("called `BorrowedOption::unwrap()` on a `None` value"),
        }
    }
    
    /// Get the value or a default
    #[inline]
    pub const fn unwrap_or(self, default: &'a T) -> &'a T {
        match self {
            Self::Some(value) => value,
            Self::None => default,
        }
    }
    
    /// Map the contained value
    #[inline]
    pub fn map<U, F>(self, f: F) -> BorrowedOption<'a, U>
    where
        F: FnOnce(&'a T) -> &'a U,
    {
        match self {
            Self::Some(value) => BorrowedOption::Some(f(value)),
            Self::None => BorrowedOption::None,
        }
    }
    
    /// Convert to standard Option
    #[inline]
    pub const fn to_option(self) -> Option<&'a T> {
        match self {
            Self::Some(value) => Some(value),
            Self::None => None,
        }
    }
}

impl<'a, T> From<Option<&'a T>> for BorrowedOption<'a, T> {
    #[inline]
    fn from(opt: Option<&'a T>) -> Self {
        match opt {
            Some(value) => Self::Some(value),
            None => Self::None,
        }
    }
}

impl<'a, T> From<BorrowedOption<'a, T>> for Option<&'a T> {
    #[inline]
    fn from(borrowed_opt: BorrowedOption<'a, T>) -> Self {
        borrowed_opt.to_option()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_borrowed_slice() {
        let data = vec![1, 2, 3, 4, 5];
        let slice = BorrowedSlice::new(&data);
        
        assert_eq!(slice.len(), 5);
        assert!(!slice.is_empty());
        assert_eq!(slice.get(2), Some(&3));
        assert_eq!(slice.first(), Some(&1));
        assert_eq!(slice.last(), Some(&5));
        
        let (left, right) = slice.split_at(3);
        assert_eq!(left.len(), 3);
        assert_eq!(right.len(), 2);
        
        let sub_slice = slice.slice(1, 4);
        assert_eq!(sub_slice.len(), 3);
        assert_eq!(sub_slice.get(0), Some(&2));
    }
    
    #[test]
    fn test_borrowed_str() {
        let borrowed = BorrowedStr::borrowed("hello");
        let static_str = BorrowedStr::static_str("world");
        
        assert_eq!(borrowed.as_str(), "hello");
        assert_eq!(static_str.as_str(), "world");
        assert_eq!(borrowed.len(), 5);
        assert!(!borrowed.is_empty());
        assert!(!borrowed.is_static());
        assert!(static_str.is_static());
        
        let cow = borrowed.to_cow();
        assert!(matches!(cow, Cow::Borrowed(_)));
    }
    
    #[test]
    fn test_borrowed_bytes() {
        let data = b"hello world";
        let bytes = BorrowedBytes::new(data);
        
        assert_eq!(bytes.len(), 11);
        assert!(!bytes.is_empty());
        assert_eq!(bytes.get(0), Some(b'h'));
        
        let (left, right) = bytes.split_at(5);
        assert_eq!(left.as_bytes(), b"hello");
        assert_eq!(right.as_bytes(), b" world");
        
        let sub_bytes = bytes.slice(6, 11);
        assert_eq!(sub_bytes.as_bytes(), b"world");
        
        assert_eq!(bytes.to_str().unwrap(), "hello world");
    }
    
    #[test]
    fn test_borrowed_key_value() {
        let key = "name";
        let value = "Alice";
        let kv = BorrowedKeyValue::new(&key, &value);
        
        assert_eq!(kv.key(), &"name");
        assert_eq!(kv.value(), &"Alice");
        
        let (k, v) = kv.into_parts();
        assert_eq!(k, &"name");
        assert_eq!(v, &"Alice");
    }
    
    #[test]
    fn test_borrowed_map() {
        let keys = vec!["a", "b", "c"];
        let values = vec![1, 2, 3];
        
        let map = BorrowedMap::new(&keys, &values).unwrap();
        
        assert_eq!(map.len(), 3);
        assert!(!map.is_empty());
        assert_eq!(map.get(&"b"), Some(&2));
        assert_eq!(map.get(&"d"), None);
        
        let kv = map.get_by_index(1).unwrap();
        assert_eq!(kv.key(), &"b");
        assert_eq!(kv.value(), &2);
        
        let collected: Vec<_> = map.iter().collect();
        assert_eq!(collected.len(), 3);
    }
    
    #[test]
    fn test_borrowed_option() {
        let value = 42;
        let some_opt = BorrowedOption::some(&value);
        let none_opt = BorrowedOption::<i32>::none();
        
        assert!(some_opt.is_some());
        assert!(!some_opt.is_none());
        assert!(!none_opt.is_some());
        assert!(none_opt.is_none());
        
        assert_eq!(some_opt.unwrap(), &42);
        assert_eq!(some_opt.unwrap_or(&0), &42);
        assert_eq!(none_opt.unwrap_or(&0), &0);
        
        let mapped = some_opt.map(|x| x);
        assert!(mapped.is_some());
        
        let std_option: Option<&i32> = some_opt.into();
        assert_eq!(std_option, Some(&42));
    }
    
    #[test]
    fn test_borrowed_slice_windows_and_chunks() {
        let data = vec![1, 2, 3, 4, 5, 6];
        let slice = BorrowedSlice::new(&data);
        
        let windows: Vec<_> = slice.windows(3).collect();
        assert_eq!(windows.len(), 4);
        assert_eq!(windows[0].as_ref(), &[1, 2, 3]);
        assert_eq!(windows[1].as_ref(), &[2, 3, 4]);
        
        let chunks: Vec<_> = slice.chunks(2).collect();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].as_ref(), &[1, 2]);
        assert_eq!(chunks[1].as_ref(), &[3, 4]);
        assert_eq!(chunks[2].as_ref(), &[5, 6]);
    }
}