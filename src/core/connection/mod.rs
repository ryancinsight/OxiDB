// src/core/connection/mod.rs
//! Connection management module implementing SOLID, CUPID, GRASP, DRY, YAGNI, and ACID principles
//!
//! This module provides connection pooling and management capabilities for the database.
//! It ensures efficient resource utilization and follows established design patterns.

// pub mod pool; // TODO: Implement connection pool in future

use crate::core::common::OxidbError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Represents a unique connection identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(u64);

impl ConnectionId {
    fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// Connection state tracking
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionState {
    Active,
    Idle,
    Closed,
    Error(String),
}

/// Database connection metadata
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub id: ConnectionId,
    pub created_at: Instant,
    pub last_used: Instant,
    pub state: ConnectionState,
    pub transaction_count: u64,
    pub query_count: u64,
}

impl ConnectionInfo {
    #[allow(dead_code)]
    fn new(id: ConnectionId) -> Self {
        let now = Instant::now();
        Self {
            id,
            created_at: now,
            last_used: now,
            state: ConnectionState::Idle,
            transaction_count: 0,
            query_count: 0,
        }
    }

    #[allow(dead_code)]
    fn mark_used(&mut self) {
        self.last_used = Instant::now();
        self.query_count += 1;
    }

    #[allow(dead_code)]
    fn start_transaction(&mut self) {
        self.transaction_count += 1;
        self.state = ConnectionState::Active;
    }

    #[allow(dead_code)]
    fn end_transaction(&mut self) {
        self.state = ConnectionState::Idle;
    }

    #[allow(dead_code)]
    fn is_expired(&self, max_idle_time: Duration) -> bool {
        self.last_used.elapsed() > max_idle_time
    }
}

/// Connection factory trait for dependency injection (Dependency Inversion Principle)
pub trait ConnectionFactory: Send + Sync {
    type Connection: DatabaseConnection;

    fn create_connection(&self) -> Result<Self::Connection, OxidbError>;
    fn validate_connection(&self, connection: &Self::Connection) -> bool;
}

/// Database connection trait (Interface Segregation Principle)
pub trait DatabaseConnection: Send + Sync {
    fn id(&self) -> ConnectionId;
    fn info(&self) -> &ConnectionInfo;
    fn info_mut(&mut self) -> &mut ConnectionInfo;
    fn is_active(&self) -> bool;
    fn close(&mut self) -> Result<(), OxidbError>;
    fn reset(&mut self) -> Result<(), OxidbError>;
}

/// Statistics for connection pool monitoring
#[derive(Debug, Clone, Default)]
pub struct PoolStatistics {
    pub total_connections: usize,
    pub active_connections: usize,
    pub idle_connections: usize,
    pub total_requests: u64,
    pub successful_acquisitions: u64,
    pub failed_acquisitions: u64,
    pub timeouts: u64,
    pub connections_created: u64,
    pub connections_destroyed: u64,
}

/// Connection pool configuration
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub min_connections: usize,
    pub max_connections: usize,
    pub acquire_timeout: Duration,
    pub max_idle_time: Duration,
    pub validation_interval: Duration,
    pub enable_metrics: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 5,
            max_connections: 50,
            acquire_timeout: Duration::from_secs(30),
            max_idle_time: Duration::from_secs(300), // 5 minutes
            validation_interval: Duration::from_secs(60),
            enable_metrics: true,
        }
    }
}

impl PoolConfig {
    /// Creates a new PoolConfig builder
    pub fn builder() -> PoolConfigBuilder {
        PoolConfigBuilder::new()
    }

    /// Validates the configuration
    pub fn validate(&self) -> Result<(), OxidbError> {
        if self.min_connections > self.max_connections {
            return Err(OxidbError::Configuration(
                "min_connections cannot be greater than max_connections".to_string(),
            ));
        }

        if self.max_connections == 0 {
            return Err(OxidbError::Configuration(
                "max_connections must be greater than 0".to_string(),
            ));
        }

        if self.acquire_timeout.is_zero() {
            return Err(OxidbError::Configuration(
                "acquire_timeout must be greater than 0".to_string(),
            ));
        }

        Ok(())
    }

    /// Creates a configuration for high-throughput scenarios
    pub fn for_high_throughput() -> Self {
        Self {
            min_connections: 20,
            max_connections: 200,
            acquire_timeout: Duration::from_secs(10),
            max_idle_time: Duration::from_secs(120),
            validation_interval: Duration::from_secs(30),
            enable_metrics: true,
        }
    }

    /// Creates a configuration for low-resource scenarios
    pub fn for_low_resource() -> Self {
        Self {
            min_connections: 2,
            max_connections: 10,
            acquire_timeout: Duration::from_secs(60),
            max_idle_time: Duration::from_secs(600),
            validation_interval: Duration::from_secs(120),
            enable_metrics: false,
        }
    }
}

/// Builder for PoolConfig (Builder Pattern)
#[derive(Debug)]
pub struct PoolConfigBuilder {
    min_connections: Option<usize>,
    max_connections: Option<usize>,
    acquire_timeout: Option<Duration>,
    max_idle_time: Option<Duration>,
    validation_interval: Option<Duration>,
    enable_metrics: Option<bool>,
}

impl PoolConfigBuilder {
    fn new() -> Self {
        Self {
            min_connections: None,
            max_connections: None,
            acquire_timeout: None,
            max_idle_time: None,
            validation_interval: None,
            enable_metrics: None,
        }
    }

    pub fn min_connections(mut self, min: usize) -> Self {
        self.min_connections = Some(min);
        self
    }

    pub fn max_connections(mut self, max: usize) -> Self {
        self.max_connections = Some(max);
        self
    }

    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = Some(timeout);
        self
    }

    pub fn max_idle_time(mut self, idle_time: Duration) -> Self {
        self.max_idle_time = Some(idle_time);
        self
    }

    pub fn validation_interval(mut self, interval: Duration) -> Self {
        self.validation_interval = Some(interval);
        self
    }

    pub fn enable_metrics(mut self, enable: bool) -> Self {
        self.enable_metrics = Some(enable);
        self
    }

    pub fn build(self) -> Result<PoolConfig, OxidbError> {
        let config = PoolConfig {
            min_connections: self.min_connections.unwrap_or(5),
            max_connections: self.max_connections.unwrap_or(50),
            acquire_timeout: self.acquire_timeout.unwrap_or_else(|| Duration::from_secs(30)),
            max_idle_time: self.max_idle_time.unwrap_or_else(|| Duration::from_secs(300)),
            validation_interval: self
                .validation_interval
                .unwrap_or_else(|| Duration::from_secs(60)),
            enable_metrics: self.enable_metrics.unwrap_or(true),
        };

        config.validate()?;
        Ok(config)
    }
}

/// Connection ID generator (Single Responsibility Principle)
#[derive(Debug)]
pub struct ConnectionIdGenerator {
    counter: AtomicU64,
}

impl ConnectionIdGenerator {
    pub fn new() -> Self {
        Self { counter: AtomicU64::new(1) }
    }

    pub fn next_id(&self) -> ConnectionId {
        let id = self.counter.fetch_add(1, Ordering::SeqCst);
        ConnectionId::new(id)
    }
}

impl Default for ConnectionIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_id_generation() {
        let generator = ConnectionIdGenerator::new();
        let id1 = generator.next_id();
        let id2 = generator.next_id();

        assert_ne!(id1, id2);
        assert_eq!(id1.as_u64(), 1);
        assert_eq!(id2.as_u64(), 2);
    }

    #[test]
    fn test_connection_info_lifecycle() {
        let id = ConnectionId::new(1);
        let mut info = ConnectionInfo::new(id);

        assert_eq!(info.id, id);
        assert_eq!(info.state, ConnectionState::Idle);
        assert_eq!(info.transaction_count, 0);
        assert_eq!(info.query_count, 0);

        info.mark_used();
        assert_eq!(info.query_count, 1);

        info.start_transaction();
        assert_eq!(info.state, ConnectionState::Active);
        assert_eq!(info.transaction_count, 1);

        info.end_transaction();
        assert_eq!(info.state, ConnectionState::Idle);
    }

    #[test]
    fn test_pool_config_validation() {
        // Valid configuration
        let config = PoolConfig::builder().min_connections(5).max_connections(10).build();
        assert!(config.is_ok());

        // Invalid: min > max
        let config = PoolConfig::builder().min_connections(10).max_connections(5).build();
        assert!(config.is_err());

        // Invalid: max = 0
        let config = PoolConfig::builder().max_connections(0).build();
        assert!(config.is_err());
    }

    #[test]
    fn test_specialized_pool_configs() {
        let high_throughput = PoolConfig::for_high_throughput();
        assert_eq!(high_throughput.min_connections, 20);
        assert_eq!(high_throughput.max_connections, 200);

        let low_resource = PoolConfig::for_low_resource();
        assert_eq!(low_resource.min_connections, 2);
        assert_eq!(low_resource.max_connections, 10);
    }

    #[test]
    fn test_connection_expiration() {
        let id = ConnectionId::new(1);
        let info = ConnectionInfo::new(id);

        // Not expired immediately
        assert!(!info.is_expired(Duration::from_secs(1)));

        // Simulate time passage
        std::thread::sleep(Duration::from_millis(10));
        assert!(info.is_expired(Duration::from_millis(5)));
    }
}
