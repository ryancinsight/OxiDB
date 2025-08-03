//! Pure Rust byte order handling using only core and std::io
//! 
//! This module provides byte order conversion functionality that replaces
//! the external byteorder dependency, following the YAGNI principle
//! by implementing only what we need for page serialization.

use core::convert::TryInto;
use std::io;

/// Trait for reading bytes in little-endian order
pub trait ReadBytesExt: io::Read {
    /// Read a u8 value
    /// 
    /// # Errors
    /// 
    /// Returns an error if the underlying read operation fails
    fn read_u8(&mut self) -> io::Result<u8> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }
    
    /// Read a u16 value in little-endian order
    /// 
    /// # Errors
    /// 
    /// Returns an error if the underlying read operation fails
    fn read_u16<T: ByteOrder>(&mut self) -> io::Result<u16> {
        let mut buf = [0u8; 2];
        self.read_exact(&mut buf)?;
        Ok(T::read_u16(&buf))
    }
    
    /// Read a u32 value in little-endian order
    /// 
    /// # Errors
    /// 
    /// Returns an error if the underlying read operation fails
    fn read_u32<T: ByteOrder>(&mut self) -> io::Result<u32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(T::read_u32(&buf))
    }
    
    /// Read a u64 value in little-endian order
    /// 
    /// # Errors
    /// 
    /// Returns an error if the underlying read operation fails
    fn read_u64<T: ByteOrder>(&mut self) -> io::Result<u64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(T::read_u64(&buf))
    }
    
    // Note: read_usize removed to avoid platform-dependent serialization
    // Always use fixed-size integers (u32/u64) for portable on-disk formats
}

/// Trait for writing bytes in little-endian order
pub trait WriteBytesExt: io::Write {
    /// Write a u8 value
    fn write_u8(&mut self, n: u8) -> io::Result<()> {
        self.write_all(&[n])
    }
    
    /// Write a u16 value in little-endian order
    fn write_u16<T: ByteOrder>(&mut self, n: u16) -> io::Result<()> {
        let mut buf = [0u8; 2];
        T::write_u16(&mut buf, n);
        self.write_all(&buf)
    }
    
    /// Write a u32 value in little-endian order
    fn write_u32<T: ByteOrder>(&mut self, n: u32) -> io::Result<()> {
        let mut buf = [0u8; 4];
        T::write_u32(&mut buf, n);
        self.write_all(&buf)
    }
    
    /// Write a u64 value in little-endian order
    fn write_u64<T: ByteOrder>(&mut self, n: u64) -> io::Result<()> {
        let mut buf = [0u8; 8];
        T::write_u64(&mut buf, n);
        self.write_all(&buf)
    }
    
    // Note: write_usize removed to avoid platform-dependent serialization
    // Always use fixed-size integers (u32/u64) for portable on-disk formats
}

/// Implement ReadBytesExt for all types that implement Read
impl<R: io::Read + ?Sized> ReadBytesExt for R {}

/// Implement WriteBytesExt for all types that implement Write
impl<W: io::Write + ?Sized> WriteBytesExt for W {}

/// Trait defining byte order operations
pub trait ByteOrder {
    /// Read u16 from buffer
    fn read_u16(buf: &[u8]) -> u16;
    /// Read u32 from buffer
    fn read_u32(buf: &[u8]) -> u32;
    /// Read u64 from buffer
    fn read_u64(buf: &[u8]) -> u64;
    /// Write u16 to buffer
    fn write_u16(buf: &mut [u8], n: u16);
    /// Write u32 to buffer
    fn write_u32(buf: &mut [u8], n: u32);
    /// Write u64 to buffer
    fn write_u64(buf: &mut [u8], n: u64);
}

/// Little-endian byte order
pub struct LittleEndian;

impl ByteOrder for LittleEndian {
    #[inline]
    fn read_u16(buf: &[u8]) -> u16 {
        u16::from_le_bytes(buf[..2].try_into().unwrap())
    }
    
    #[inline]
    fn read_u32(buf: &[u8]) -> u32 {
        u32::from_le_bytes(buf[..4].try_into().unwrap())
    }
    
    #[inline]
    fn read_u64(buf: &[u8]) -> u64 {
        u64::from_le_bytes(buf[..8].try_into().unwrap())
    }
    
    #[inline]
    fn write_u16(buf: &mut [u8], n: u16) {
        buf[..2].copy_from_slice(&n.to_le_bytes());
    }
    
    #[inline]
    fn write_u32(buf: &mut [u8], n: u32) {
        buf[..4].copy_from_slice(&n.to_le_bytes());
    }
    
    #[inline]
    fn write_u64(buf: &mut [u8], n: u64) {
        buf[..8].copy_from_slice(&n.to_le_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_little_endian_u16() {
        let mut buf = [0u8; 2];
        LittleEndian::write_u16(&mut buf, 0x1234);
        assert_eq!(buf, [0x34, 0x12]);
        assert_eq!(LittleEndian::read_u16(&buf), 0x1234);
    }
    
    #[test]
    fn test_little_endian_u32() {
        let mut buf = [0u8; 4];
        LittleEndian::write_u32(&mut buf, 0x1234_5678);
        assert_eq!(buf, [0x78, 0x56, 0x34, 0x12]);
        assert_eq!(LittleEndian::read_u32(&buf), 0x1234_5678);
    }
    
    #[test]
    fn test_little_endian_u64() {
        let mut buf = [0u8; 8];
        LittleEndian::write_u64(&mut buf, 0x1234_5678_9ABC_DEF0);
        assert_eq!(buf, [0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12]);
        assert_eq!(LittleEndian::read_u64(&buf), 0x1234_5678_9ABC_DEF0);
    }
}