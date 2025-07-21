//! Connection Pool Implementation
//! 
//! This module provides a thread-safe connection pool for database connections,
//! following SOLID principles and implementing proper resource management.

use crate::core::common::{OxidbError, ResultExt};
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};
use std::thread;
use uuid::Uuid;

/// Trait for database connections that can be pooled
/// Follows SOLID's Interface Segregation Principle
pub trait PoolableConnection: Send + Sync {
    /// Check if the connection is still valid
    fn is_valid(&self) -> bool;
    
    /// Reset the connection to a clean state
    fn reset(&mut self) -> Result<(), OxidbError>;
    
    /// Get the connection's unique identifier
    fn connection_id(&self) -> Uuid;
    
    /// Get the time when this connection was last used
    fn last_used(&self) -> Instant;
    
    /// Mark the connection as used
    fn mark_used(&mut self);
}

/// Configuration for the connection pool
/// Follows CUPID's Composable principle
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Minimum number of connections to maintain
    pub min_connections: usize,
    /// Maximum number of connections allowed
    pub max_connections: usize,
    /// Maximum time a connection can be idle before being closed
    pub max_idle_time: Duration,
    /// Maximum time to wait for a connection from the pool
    pub connection_timeout: Duration,
    /// How often to check for idle connections to clean up
    pub cleanup_interval: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 10,
            max_idle_time: Duration::from_secs(300), // 5 minutes
            connection_timeout: Duration::from_secs(30),
            cleanup_interval: Duration::from_secs(60),
        }
    }
}

/// A pooled connection wrapper
pub struct PooledConnection<T: PoolableConnection> {
    /// The actual connection
    connection: Option<T>,
    /// Reference to the pool for returning the connection
    pool: Weak<Mutex<PoolInner<T>>>,
}

impl<T: PoolableConnection> PooledConnection<T> {
    /// Create a new pooled connection
    fn new(connection: T, pool: Weak<Mutex<PoolInner<T>>>) -> Self {
        Self {
            connection: Some(connection),
            pool,
        }
    }
    
    /// Get a reference to the underlying connection
    pub fn as_ref(&self) -> Option<&T> {
        self.connection.as_ref()
    }
    
    /// Get a mutable reference to the underlying connection
    pub fn as_mut(&mut self) -> Option<&mut T> {
        self.connection.as_mut()
    }
}

impl<T: PoolableConnection> Drop for PooledConnection<T> {
    fn drop(&mut self) {
        if let (Some(connection), Some(pool)) = (self.connection.take(), self.pool.upgrade()) {
            if let Ok(mut inner) = pool.lock() {
                inner.return_connection(connection);
            }
        }
    }
}

/// Internal pool state
struct PoolInner<T: PoolableConnection> {
    /// Available connections
    available: VecDeque<T>,
    /// Number of connections currently in use
    in_use: usize,
    /// Pool configuration
    config: PoolConfig,
    /// Condition variable for waiting threads
    condvar: Condvar,
}

impl<T: PoolableConnection> PoolInner<T> {
    fn new(config: PoolConfig) -> Self {
        Self {
            available: VecDeque::new(),
            in_use: 0,
            config,
            condvar: Condvar::new(),
        }
    }
    
    fn total_connections(&self) -> usize {
        self.available.len() + self.in_use
    }
    
    fn return_connection(&mut self, mut connection: T) {
        // Reset the connection state
        if connection.reset().is_ok() && connection.is_valid() {
            connection.mark_used();
            self.available.push_back(connection);
        }
        
        // Debug assertion to catch logic errors in development
        debug_assert!(self.in_use > 0, "in_use should always be > 0 when returning a connection");
        self.in_use -= 1;
        
        // Notify waiting threads
        self.condvar.notify_one();
    }
    
    fn cleanup_idle_connections(&mut self) {
        let now = Instant::now();
        self.available.retain(|conn| {
            let idle_time = now.duration_since(conn.last_used());
            idle_time < self.config.max_idle_time && conn.is_valid()
        });
        
        // Ensure we maintain minimum connections
        while self.available.len() < self.config.min_connections 
            && self.total_connections() < self.config.max_connections {
            // This would require a factory function to create new connections
            // For now, we'll just break to avoid infinite loop
            break;
        }
    }
}

/// A thread-safe connection pool
/// Follows SOLID's Single Responsibility Principle
pub struct ConnectionPool<T: PoolableConnection> {
    /// Internal pool state
    inner: Arc<Mutex<PoolInner<T>>>,
    /// Handle to the cleanup thread
    _cleanup_handle: thread::JoinHandle<()>,
}

impl<T: PoolableConnection + 'static> ConnectionPool<T> {
    /// Create a new connection pool with the given configuration
    pub fn new(config: PoolConfig) -> Self {
        let inner = Arc::new(Mutex::new(PoolInner::new(config.clone())));
        let cleanup_inner = Arc::downgrade(&inner);
        
        // Start cleanup thread
        let cleanup_handle = thread::spawn(move || {
            loop {
                thread::sleep(config.cleanup_interval);
                
                if let Some(pool) = cleanup_inner.upgrade() {
                    if let Ok(mut inner) = pool.lock() {
                        inner.cleanup_idle_connections();
                    }
                } else {
                    // Pool has been dropped, exit cleanup thread
                    break;
                }
            }
        });
        
        Self {
            inner,
            _cleanup_handle: cleanup_handle,
        }
    }
    
    /// Get a connection from the pool
    pub fn get_connection(&self) -> Result<PooledConnection<T>, OxidbError> {
        let timeout = {
            let inner = self.inner.lock()
                .with_static_context("Failed to acquire pool lock")?;
            inner.config.connection_timeout
        };
        
        let start_time = Instant::now();
        
        loop {
            let mut inner = self.inner.lock()
                .with_static_context("Failed to acquire pool lock")?;
            
            // Try to get an available connection
            while let Some(mut connection) = inner.available.pop_front() {
                if connection.is_valid() {
                    connection.mark_used();
                    inner.in_use += 1;
                    let pool_ref = Arc::downgrade(&self.inner);
                    return Ok(PooledConnection::new(connection, pool_ref));
                }
                // Connection is invalid, discard it
            }
            
            // Check if we can create a new connection
            if inner.total_connections() < inner.config.max_connections {
                // Would need a factory function to create new connections
                return Err(OxidbError::Other(
                    "Connection factory not implemented".to_string()
                ));
            }
            
            // Check timeout
            if start_time.elapsed() >= timeout {
                return Err(OxidbError::Other(
                    "Timeout waiting for connection".to_string()
                ));
            }
            
            // Wait for a connection to become available
            let wait_timeout = timeout - start_time.elapsed();
            let (new_inner, timeout_result) = inner.condvar.wait_timeout(inner, wait_timeout)
                .with_static_context("Failed to wait for connection")?;
            
            inner = new_inner;
            
            if timeout_result.timed_out() {
                return Err(OxidbError::Other(
                    "Timeout waiting for connection".to_string()
                ));
            }
        }
    }
    
    /// Get pool statistics
    pub fn stats(&self) -> Result<PoolStats, OxidbError> {
        let inner = self.inner.lock()
            .with_static_context("Failed to acquire pool lock")?;
        
        Ok(PoolStats {
            available: inner.available.len(),
            in_use: inner.in_use,
            total: inner.total_connections(),
            max_connections: inner.config.max_connections,
        })
    }
    
    /// Add a connection to the pool
    pub fn add_connection(&self, connection: T) -> Result<(), OxidbError> {
        let mut inner = self.inner.lock()
            .with_static_context("Failed to acquire pool lock")?;
        
        if inner.total_connections() < inner.config.max_connections {
            inner.available.push_back(connection);
            inner.condvar.notify_one();
            Ok(())
        } else {
            Err(OxidbError::Other(
                "Pool is at maximum capacity".to_string()
            ))
        }
    }
}

/// Pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    /// Number of available connections
    pub available: usize,
    /// Number of connections currently in use
    pub in_use: usize,
    /// Total number of connections
    pub total: usize,
    /// Maximum number of connections allowed
    pub max_connections: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    // Mock connection for testing
    struct MockConnection {
        id: Uuid,
        last_used: Instant,
        valid: AtomicBool,
        reset_count: AtomicUsize,
    }

    impl MockConnection {
        fn new() -> Self {
            Self {
                id: Uuid::new_v4(),
                last_used: Instant::now(),
                valid: AtomicBool::new(true),
                reset_count: AtomicUsize::new(0),
            }
        }
        
        fn invalidate(&self) {
            self.valid.store(false, Ordering::SeqCst);
        }
    }

    impl PoolableConnection for MockConnection {
        fn is_valid(&self) -> bool {
            self.valid.load(Ordering::SeqCst)
        }
        
        fn reset(&mut self) -> Result<(), OxidbError> {
            self.reset_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        
        fn connection_id(&self) -> Uuid {
            self.id
        }
        
        fn last_used(&self) -> Instant {
            self.last_used
        }
        
        fn mark_used(&mut self) {
            self.last_used = Instant::now();
        }
    }

    #[test]
    fn test_pool_creation() {
        let config = PoolConfig::default();
        let pool: ConnectionPool<MockConnection> = ConnectionPool::new(config);
        let stats = pool.stats().unwrap();
        
        assert_eq!(stats.available, 0);
        assert_eq!(stats.in_use, 0);
        assert_eq!(stats.total, 0);
    }

    #[test]
    fn test_add_connection() {
        let config = PoolConfig::default();
        let pool = ConnectionPool::new(config);
        let connection = MockConnection::new();
        
        pool.add_connection(connection).unwrap();
        let stats = pool.stats().unwrap();
        
        assert_eq!(stats.available, 1);
        assert_eq!(stats.total, 1);
    }

    #[test]
    fn test_connection_return() {
        let config = PoolConfig::default();
        let pool = ConnectionPool::new(config);
        let connection = MockConnection::new();
        let reset_count_before = connection.reset_count.load(Ordering::SeqCst);
        
        pool.add_connection(connection).unwrap();
        
        {
            let _pooled_conn = pool.get_connection().unwrap();
            let stats = pool.stats().unwrap();
            assert_eq!(stats.in_use, 1);
            assert_eq!(stats.available, 0);
        } // Connection should be returned here
        
        let stats = pool.stats().unwrap();
        assert_eq!(stats.in_use, 0);
        assert_eq!(stats.available, 1);
    }
}