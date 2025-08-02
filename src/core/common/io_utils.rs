//! IO utility functions following DRY principle
//! 
//! This module provides common IO operations and error handling utilities
//! to reduce code duplication across the codebase.

use crate::core::common::OxidbError;
use std::io;

/// Extension trait for IO Result types to simplify error conversion
pub trait IoResultExt<T> {
    /// Convert IO errors to OxidbError::Io
    fn oxidb_io(self) -> Result<T, OxidbError>;
}

impl<T> IoResultExt<T> for io::Result<T> {
    fn oxidb_io(self) -> Result<T, OxidbError> {
        self.map_err(OxidbError::Io)
    }
}

/// Helper trait for Write operations with OxidbError
pub trait WriteExt: io::Write {
    /// Write all bytes and convert errors to OxidbError
    fn write_all_oxidb(&mut self, buf: &[u8]) -> Result<(), OxidbError> {
        self.write_all(buf).oxidb_io()
    }
    
    /// Flush and convert errors to OxidbError
    fn flush_oxidb(&mut self) -> Result<(), OxidbError> {
        self.flush().oxidb_io()
    }
}

/// Helper trait for Read operations with OxidbError
pub trait ReadExt: io::Read {
    /// Read exact bytes and convert errors to OxidbError
    fn read_exact_oxidb(&mut self, buf: &mut [u8]) -> Result<(), OxidbError> {
        self.read_exact(buf).oxidb_io()
    }
}

// Implement for all types that implement the base traits
impl<W: io::Write + ?Sized> WriteExt for W {}
impl<R: io::Read + ?Sized> ReadExt for R {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    
    #[test]
    fn test_io_result_ext() {
        let result: io::Result<()> = Ok(());
        assert!(result.oxidb_io().is_ok());
        
        let error: io::Result<()> = Err(io::Error::new(io::ErrorKind::NotFound, "test"));
        assert!(matches!(error.oxidb_io(), Err(OxidbError::Io(_))));
    }
    
    #[test]
    fn test_write_ext() {
        let mut buffer = Vec::new();
        assert!(buffer.write_all_oxidb(b"test").is_ok());
        assert!(buffer.flush_oxidb().is_ok());
        assert_eq!(buffer, b"test");
    }
    
    #[test]
    fn test_read_ext() {
        let mut cursor = Cursor::new(b"test");
        let mut buf = [0u8; 4];
        assert!(cursor.read_exact_oxidb(&mut buf).is_ok());
        assert_eq!(&buf, b"test");
    }
}