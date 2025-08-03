//! Pure Rust CRC32 implementation using only core and alloc
//! 
//! This module provides a CRC32 checksum implementation that replaces
//! the external crc32fast dependency, following the YAGNI principle
//! by implementing only what we need for WAL checksums.

/// CRC32 polynomial used for checksum calculation (IEEE 802.3)
const CRC32_POLYNOMIAL: u32 = 0xEDB8_8320;

/// Precomputed CRC32 lookup table for performance
const CRC32_TABLE: [u32; 256] = generate_crc32_table();

/// Generate the CRC32 lookup table at compile time
const fn generate_crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        
        while j < 8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ CRC32_POLYNOMIAL;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        
        table[i] = crc;
        i += 1;
    }
    
    table
}

/// CRC32 hasher that maintains state for incremental hashing
#[derive(Debug, Clone)]
pub struct Hasher {
    state: u32,
}

impl Default for Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher {
    /// Create a new CRC32 hasher with initial state
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: 0xFFFF_FFFF,
        }
    }
    
    /// Update the hash with a byte slice
    pub fn update(&mut self, data: &[u8]) {
        for &byte in data {
            let table_idx = ((self.state ^ u32::from(byte)) & 0xFF) as usize;
            self.state = (self.state >> 8) ^ CRC32_TABLE[table_idx];
        }
    }
    
    /// Finalize the hash and return the checksum
    #[must_use]
    pub const fn finalize(&self) -> u32 {
        !self.state
    }
    
    /// Convenience method to hash data and return checksum in one call
    pub fn hash(data: &[u8]) -> u32 {
        let mut hasher = Self::new();
        hasher.update(data);
        hasher.finalize()
    }
}

/// Calculate CRC32 checksum for a byte slice
#[must_use]
pub fn checksum(data: &[u8]) -> u32 {
    Hasher::hash(data)
}

/// Verify data integrity by comparing with expected checksum
#[must_use]
pub fn verify(data: &[u8], expected: u32) -> bool {
    checksum(data) == expected
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;
    
    #[test]
    fn test_empty_input() {
        assert_eq!(checksum(&[]), 0);
    }
    
    #[test]
    fn test_known_values() {
        // Test vectors from IEEE 802.3 standard
        assert_eq!(checksum(b"123456789"), 0xCBF4_3926);
        assert_eq!(checksum(b"The quick brown fox jumps over the lazy dog"), 0x414F_A339);
    }
    
    #[test]
    fn test_incremental_hashing() {
        let data = b"Hello, World!";
        
        // Hash all at once
        let full_hash = checksum(data);
        
        // Hash incrementally
        let mut hasher = Hasher::new();
        hasher.update(b"Hello");
        hasher.update(b", ");
        hasher.update(b"World!");
        let incremental_hash = hasher.finalize();
        
        assert_eq!(full_hash, incremental_hash);
    }
    
    #[test]
    fn test_verify() {
        let data = b"Test data";
        let checksum = checksum(data);
        
        assert!(verify(data, checksum));
        assert!(!verify(data, checksum + 1));
    }
}