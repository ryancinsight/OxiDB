//! Pure Rust hex encoding and decoding
//! 
//! This module provides hex encoding/decoding functionality that replaces
//! the external hex dependency, following the YAGNI principle.

use crate::core::common::OxidbError;

/// Encode bytes to lowercase hexadecimal string
pub fn encode<T: AsRef<[u8]>>(data: T) -> String {
    encode_to_slice_inner(data.as_ref(), HexCase::Lower)
}

/// Encode bytes to uppercase hexadecimal string
pub fn encode_upper<T: AsRef<[u8]>>(data: T) -> String {
    encode_to_slice_inner(data.as_ref(), HexCase::Upper)
}

/// Decode hexadecimal string to bytes
pub fn decode<T: AsRef<[u8]>>(data: T) -> Result<Vec<u8>, OxidbError> {
    decode_to_slice_inner(data.as_ref())
}

/// Internal hex case enum
#[derive(Clone, Copy)]
enum HexCase {
    Lower,
    Upper,
}

/// Hex characters lookup table
const HEX_CHARS_LOWER: &[u8; 16] = b"0123456789abcdef";
const HEX_CHARS_UPPER: &[u8; 16] = b"0123456789ABCDEF";

/// Internal encoding implementation
fn encode_to_slice_inner(data: &[u8], case: HexCase) -> String {
    let hex_chars = match case {
        HexCase::Lower => HEX_CHARS_LOWER,
        HexCase::Upper => HEX_CHARS_UPPER,
    };
    
    let mut result = String::with_capacity(data.len() * 2);
    
    for &byte in data {
        let high = (byte >> 4) as usize;
        let low = (byte & 0x0F) as usize;
        result.push(hex_chars[high] as char);
        result.push(hex_chars[low] as char);
    }
    
    result
}

/// Convert hex character to nibble value
#[inline]
fn hex_char_to_nibble(c: u8) -> Result<u8, OxidbError> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(OxidbError::Deserialization(format!("Invalid hex character: {}", c as char))),
    }
}

/// Internal decoding implementation
fn decode_to_slice_inner(data: &[u8]) -> Result<Vec<u8>, OxidbError> {
    if data.len() % 2 != 0 {
        return Err(OxidbError::Deserialization("Odd number of hex characters".to_string()));
    }
    
    let mut result = Vec::with_capacity(data.len() / 2);
    
    for chunk in data.chunks_exact(2) {
        let high = hex_char_to_nibble(chunk[0])?;
        let low = hex_char_to_nibble(chunk[1])?;
        result.push((high << 4) | low);
    }
    
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_encode_empty() {
        assert_eq!(encode(b""), "");
    }
    
    #[test]
    fn test_encode_single_byte() {
        assert_eq!(encode(b"\x00"), "00");
        assert_eq!(encode(b"\xFF"), "ff");
        assert_eq!(encode(b"\x42"), "42");
    }
    
    #[test]
    fn test_encode_multiple_bytes() {
        assert_eq!(encode(b"Hello"), "48656c6c6f");
        assert_eq!(encode(b"\x01\x23\x45\x67\x89\xAB\xCD\xEF"), "0123456789abcdef");
    }
    
    #[test]
    fn test_encode_upper() {
        assert_eq!(encode_upper(b"\x01\x23\x45\x67\x89\xAB\xCD\xEF"), "0123456789ABCDEF");
    }
    
    #[test]
    fn test_decode_empty() {
        assert_eq!(decode("").unwrap(), b"");
    }
    
    #[test]
    fn test_decode_single_byte() {
        assert_eq!(decode("00").unwrap(), b"\x00");
        assert_eq!(decode("ff").unwrap(), b"\xFF");
        assert_eq!(decode("FF").unwrap(), b"\xFF");
        assert_eq!(decode("42").unwrap(), b"\x42");
    }
    
    #[test]
    fn test_decode_multiple_bytes() {
        assert_eq!(decode("48656c6c6f").unwrap(), b"Hello");
        assert_eq!(decode("0123456789abcdef").unwrap(), b"\x01\x23\x45\x67\x89\xAB\xCD\xEF");
        assert_eq!(decode("0123456789ABCDEF").unwrap(), b"\x01\x23\x45\x67\x89\xAB\xCD\xEF");
    }
    
    #[test]
    fn test_decode_odd_length() {
        assert!(decode("1").is_err());
        assert!(decode("123").is_err());
    }
    
    #[test]
    fn test_decode_invalid_char() {
        assert!(decode("0g").is_err());
        assert!(decode("zz").is_err());
        assert!(decode("@@").is_err());
    }
    
    #[test]
    fn test_roundtrip() {
        let data = b"The quick brown fox jumps over the lazy dog";
        let encoded = encode(data);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(data, &decoded[..]);
    }
}