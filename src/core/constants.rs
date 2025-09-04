/// Database configuration constants following domain-driven design principles
/// 
/// This module centralizes all configurable constants to avoid magic numbers
/// throughout the codebase and enable easy tuning for different deployment scenarios.

// === Storage Layer Constants ===

/// Default page size for storage engine (4KB for disk alignment)
pub const PAGE_SIZE: usize = 4096;

/// Default buffer pool size in pages (64MB with 4KB pages)
pub const DEFAULT_BUFFER_POOL_SIZE: usize = 16384;

/// Maximum buffer pool size to prevent memory exhaustion
pub const MAX_BUFFER_POOL_SIZE: usize = 1024 * 1024; // 4GB with 4KB pages

/// Default WAL buffer size for batching writes
pub const DEFAULT_WAL_BUFFER_SIZE: usize = 100;

/// WAL flush interval in milliseconds for durability vs performance balance
pub const DEFAULT_WAL_FLUSH_INTERVAL_MS: u64 = 1000;

/// WAL reader buffer size for efficient sequential reads
pub const DEFAULT_WAL_READER_BUFFER_SIZE: usize = 8192;

// === Query Processing Constants ===

/// Maximum query execution time in milliseconds before timeout
pub const DEFAULT_QUERY_TIMEOUT_MS: u64 = 30_000;

/// Slow query logging threshold in milliseconds
pub const DEFAULT_SLOW_QUERY_THRESHOLD_MS: u64 = 100;

/// Maximum recursion depth for query optimization to prevent stack overflow
pub const MAX_OPTIMIZATION_RECURSION_DEPTH: usize = 100;

/// Default batch size for bulk operations
pub const DEFAULT_BATCH_SIZE: usize = 1000;

// === Indexing Constants ===

/// B+ tree default node size (cache-line friendly)
pub const BTREE_DEFAULT_NODE_SIZE: usize = 256;

/// Hash index default bucket count (power of 2 for efficient modulo)
pub const HASH_INDEX_DEFAULT_BUCKETS: usize = 1024;

/// HNSW maximum connections per node for vector similarity search
pub const HNSW_DEFAULT_MAX_CONNECTIONS: usize = 16;

/// HNSW construction parameter for layer selection
pub const HNSW_DEFAULT_ML: f64 = 1.0 / 2.0_f64.ln();

// === Vector Operations Constants ===

/// Maximum vector dimension for similarity search
pub const MAX_VECTOR_DIMENSION: usize = 4096;

/// Default vector similarity search result count
pub const DEFAULT_SIMILARITY_SEARCH_K: usize = 10;

/// Vector serialization size limit (1GB) to prevent memory exhaustion
pub const MAX_VECTOR_SIZE: usize = 1_000_000_000;

/// Maximum vector elements for bincode compatibility
pub const MAX_VECTOR_ELEMENTS: u64 = 256 * 1024 * 1024; // 256M elements

// === Transaction Management Constants ===

/// Default transaction timeout in milliseconds
pub const DEFAULT_TRANSACTION_TIMEOUT_MS: u64 = 60_000;

/// Maximum number of concurrent transactions
pub const MAX_CONCURRENT_TRANSACTIONS: usize = 10_000;

/// Lock timeout in milliseconds to prevent indefinite blocking
pub const DEFAULT_LOCK_TIMEOUT_MS: u64 = 5_000;

/// Deadlock detection interval in milliseconds
pub const DEADLOCK_DETECTION_INTERVAL_MS: u64 = 1_000;

// === Connection Management Constants ===

/// Default connection pool size
pub const DEFAULT_CONNECTION_POOL_SIZE: usize = 10;

/// Maximum connection pool size
pub const MAX_CONNECTION_POOL_SIZE: usize = 1000;

/// Connection idle timeout in milliseconds
pub const DEFAULT_CONNECTION_IDLE_TIMEOUT_MS: u64 = 300_000; // 5 minutes

/// Connection validation query timeout
pub const CONNECTION_VALIDATION_TIMEOUT_MS: u64 = 1_000;

// === Performance Monitoring Constants ===

/// Metrics collection interval in milliseconds
pub const METRICS_COLLECTION_INTERVAL_MS: u64 = 1_000;

/// Performance history retention in seconds (24 hours)
pub const PERFORMANCE_HISTORY_RETENTION_SECONDS: u64 = 86_400;

/// Memory usage alert threshold as percentage of total memory
pub const MEMORY_ALERT_THRESHOLD_PERCENT: f64 = 80.0;

/// CPU usage alert threshold as percentage
pub const CPU_ALERT_THRESHOLD_PERCENT: f64 = 85.0;

// === Cryptographic Constants ===

/// CRC32 polynomial for data integrity checks
pub const CRC32_POLYNOMIAL: u32 = 0xEDB8_8320;

/// Default hash seed for deterministic hashing
pub const DEFAULT_HASH_SEED: u64 = 0x51_7C_C1_B7_27_22_0A_95;

// === Serialization Constants ===

/// Hexadecimal characters for encoding (lowercase)
pub const HEX_CHARS_LOWER: &[u8; 16] = b"0123456789abcdef";

/// Hexadecimal characters for encoding (uppercase)  
pub const HEX_CHARS_UPPER: &[u8; 16] = b"0123456789ABCDEF";

/// Maximum serialized object size to prevent memory exhaustion
pub const MAX_SERIALIZED_OBJECT_SIZE: usize = 100 * 1024 * 1024; // 100MB

// === Testing Constants ===

/// Default test timeout in milliseconds
pub const TEST_TIMEOUT_MS: u64 = 10_000;

/// Test database file prefix to avoid conflicts
pub const TEST_DB_PREFIX: &str = "test_oxidb_";

/// Maximum test data size for memory-bounded testing
pub const MAX_TEST_DATA_SIZE: usize = 10 * 1024 * 1024; // 10MB

// === Network Constants (for distributed features) ===

/// Default network buffer size for efficient I/O
pub const NETWORK_BUFFER_SIZE: usize = 64 * 1024; // 64KB

/// Network connection timeout in milliseconds
pub const NETWORK_CONNECT_TIMEOUT_MS: u64 = 5_000;

/// Network read timeout in milliseconds
pub const NETWORK_READ_TIMEOUT_MS: u64 = 30_000;

/// Maximum message size for network protocols
pub const MAX_NETWORK_MESSAGE_SIZE: usize = 16 * 1024 * 1024; // 16MB

/// Heartbeat interval for distributed nodes
pub const HEARTBEAT_INTERVAL_MS: u64 = 1_000;

// === Configuration Validation ===

/// Validate that critical constants are within reasonable bounds
pub const fn validate_constants() {
    // Ensure page size is power of 2 and reasonable
    assert!(PAGE_SIZE.is_power_of_two());
    assert!(PAGE_SIZE >= 1024 && PAGE_SIZE <= 65536);
    
    // Ensure buffer pool sizes are reasonable
    assert!(DEFAULT_BUFFER_POOL_SIZE <= MAX_BUFFER_POOL_SIZE);
    
    // Ensure timeouts are reasonable
    assert!(DEFAULT_QUERY_TIMEOUT_MS >= 1000); // At least 1 second
    assert!(DEFAULT_TRANSACTION_TIMEOUT_MS >= 1000);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants_validation() {
        validate_constants();
    }

    #[test] 
    fn test_page_size_alignment() {
        assert_eq!(PAGE_SIZE % 1024, 0); // Should be KB-aligned
        assert!(PAGE_SIZE.is_power_of_two()); // Should be power of 2
    }

    #[test]
    fn test_buffer_pool_constraints() {
        assert!(DEFAULT_BUFFER_POOL_SIZE > 0);
        assert!(DEFAULT_BUFFER_POOL_SIZE <= MAX_BUFFER_POOL_SIZE);
        
        // Buffer pool should be able to hold at least 10 pages
        assert!(DEFAULT_BUFFER_POOL_SIZE >= 10);
    }

    #[test]
    fn test_timeout_reasonableness() {
        // Timeouts should be at least 1 second
        assert!(DEFAULT_QUERY_TIMEOUT_MS >= 1000);
        assert!(DEFAULT_TRANSACTION_TIMEOUT_MS >= 1000);
        assert!(DEFAULT_LOCK_TIMEOUT_MS >= 1000);
        
        // Connection timeouts should be reasonable
        assert!(DEFAULT_CONNECTION_IDLE_TIMEOUT_MS >= 60_000); // At least 1 minute
    }

    #[test]
    fn test_vector_constraints() {
        assert!(MAX_VECTOR_DIMENSION > 0);
        assert!(MAX_VECTOR_DIMENSION <= 10_000); // Reasonable upper bound
        assert!(DEFAULT_SIMILARITY_SEARCH_K > 0);
        assert!(DEFAULT_SIMILARITY_SEARCH_K <= 1000); // Reasonable upper bound
    }
}