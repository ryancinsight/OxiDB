// src/core/config.rs

use crate::core::common::OxidbError; // Changed
use serde::{Deserialize, Serialize};
use std::fs; // For reading file
use std::path::Path;
use std::path::PathBuf; // Import PathBuf for Default impl // For load_from_file argument

/// Configuration for the database.
///
/// This struct follows the Single Responsibility Principle by focusing only on configuration management.
/// It implements the Builder pattern for improved usability and follows YAGNI by only including
/// necessary configuration options.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Config {
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default = "default_database_file", alias = "database_file_path")] // Support both names
    pub database_file: PathBuf, // Added to store the specific database file path
    #[serde(default = "default_index_dir", alias = "index_base_path")] // Support both names
    pub index_dir: PathBuf,
    #[serde(default = "default_max_cache_size")]
    pub max_cache_size: usize,
    #[serde(default = "default_wal_enabled")]
    pub wal_enabled: bool,
    #[serde(default = "default_auto_checkpoint_interval")]
    pub auto_checkpoint_interval: u64,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_query_timeout_ms")]
    pub query_timeout_ms: u64,
    #[serde(default = "default_enable_vector_search")]
    pub enable_vector_search: bool,
    #[serde(default = "default_vector_dimension")]
    pub vector_dimension: usize,
    #[serde(default = "default_similarity_threshold")]
    pub similarity_threshold: f32,
}

/// Default functions for Config fields (following DRY principle)
fn default_data_dir() -> PathBuf {
    PathBuf::from("data")
}

/// Default function for `database_file` field
fn default_database_file() -> PathBuf {
    PathBuf::from("oxidb.db")
}

fn default_index_dir() -> PathBuf {
    PathBuf::from("oxidb_indexes")
}

const fn default_max_cache_size() -> usize {
    1024 * 1024 // 1MB
}

const fn default_wal_enabled() -> bool {
    true
}

const fn default_auto_checkpoint_interval() -> u64 {
    1000
}

const fn default_max_connections() -> u32 {
    100
}

const fn default_query_timeout_ms() -> u64 {
    30000 // 30 seconds
}

const fn default_enable_vector_search() -> bool {
    false
}

const fn default_vector_dimension() -> usize {
    128
}

const fn default_similarity_threshold() -> f32 {
    0.7
}

/// Builder for Config struct implementing the Builder pattern.
///
/// This follows SOLID principles:
/// - Single Responsibility: Only responsible for building Config instances
/// - Open/Closed: Can be extended with new configuration options without modification
/// - Interface Segregation: Provides focused methods for each configuration aspect
#[derive(Debug, Clone)]
pub struct Builder {
    /// Base directory for database files
    data_dir: Option<PathBuf>,
    /// Path to the main database file
    database_file: Option<PathBuf>, // Added to store the specific database file path
    /// Directory for index storage
    index_dir: Option<PathBuf>,
    /// Maximum cache size in bytes
    max_cache_size: Option<usize>,
    /// Whether WAL (Write-Ahead Logging) is enabled
    wal_enabled: Option<bool>,
    /// Automatic checkpoint interval in seconds
    auto_checkpoint_interval: Option<u64>,
    /// Maximum number of concurrent connections
    max_connections: Option<u32>,
    /// Query timeout in milliseconds
    query_timeout_ms: Option<u64>,
    /// Whether vector search capabilities are enabled
    enable_vector_search: Option<bool>,
    /// Dimension size for vector operations
    vector_dimension: Option<usize>,
    similarity_threshold: Option<f32>,
}

impl Builder {
    /// Creates a new `Builder` with default values
    #[must_use]
    pub const fn new() -> Self {
        Self {
            data_dir: None,
            database_file: None,
            index_dir: None,
            max_cache_size: None,
            wal_enabled: None,
            auto_checkpoint_interval: None,
            max_connections: None,
            query_timeout_ms: None,
            enable_vector_search: None,
            vector_dimension: None,
            similarity_threshold: None,
        }
    }

    /// Sets the data directory
    pub fn data_dir<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.data_dir = Some(path.into());
        self
    }

    /// Sets the database file path
    pub fn database_file<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.database_file = Some(path.into());
        self
    }

    /// Sets the index directory
    pub fn index_dir<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.index_dir = Some(path.into());
        self
    }

    /// Sets the maximum cache size
    #[must_use]
    pub const fn max_cache_size(mut self, size: usize) -> Self {
        self.max_cache_size = Some(size);
        self
    }

    /// Enables or disables WAL
    #[must_use]
    pub const fn wal_enabled(mut self, enabled: bool) -> Self {
        self.wal_enabled = Some(enabled);
        self
    }

    /// Sets the auto checkpoint interval
    #[must_use]
    pub const fn auto_checkpoint_interval(mut self, interval: u64) -> Self {
        self.auto_checkpoint_interval = Some(interval);
        self
    }

    /// Sets the maximum number of connections
    #[must_use]
    pub const fn max_connections(mut self, connections: u32) -> Self {
        self.max_connections = Some(connections);
        self
    }

    /// Sets the query timeout in milliseconds
    #[must_use]
    pub const fn query_timeout_ms(mut self, timeout: u64) -> Self {
        self.query_timeout_ms = Some(timeout);
        self
    }

    /// Enables or disables vector search
    #[must_use]
    pub const fn enable_vector_search(mut self, enabled: bool) -> Self {
        self.enable_vector_search = Some(enabled);
        self
    }

    /// Sets the vector dimension
    #[must_use]
    pub const fn vector_dimension(mut self, dimension: usize) -> Self {
        self.vector_dimension = Some(dimension);
        self
    }

    /// Sets the similarity threshold
    #[must_use]
    pub const fn similarity_threshold(mut self, threshold: f32) -> Self {
        self.similarity_threshold = Some(threshold);
        self
    }

    /// Builds the Config instance with validation
    pub fn build(self) -> Result<Config, OxidbError> {
        let config = Config {
            data_dir: self.data_dir.unwrap_or_else(|| PathBuf::from("data")),
            database_file: self.database_file.unwrap_or_else(|| PathBuf::from("oxidb.db")),
            index_dir: self.index_dir.unwrap_or_else(|| PathBuf::from("oxidb_indexes")),
            max_cache_size: self.max_cache_size.unwrap_or(1024 * 1024), // 1MB default
            wal_enabled: self.wal_enabled.unwrap_or(true),
            auto_checkpoint_interval: self.auto_checkpoint_interval.unwrap_or(1000),
            max_connections: self.max_connections.unwrap_or(100),
            query_timeout_ms: self.query_timeout_ms.unwrap_or(30000), // 30 seconds
            enable_vector_search: self.enable_vector_search.unwrap_or(false),
            vector_dimension: self.vector_dimension.unwrap_or(128),
            similarity_threshold: self.similarity_threshold.unwrap_or(0.7),
        };

        // Validation following DRY principle
        config.validate()?;
        Ok(config)
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            database_file: PathBuf::from("oxidb.db"), // Default database file path
            index_dir: PathBuf::from("oxidb_indexes"),
            max_cache_size: 1024 * 1024, // 1MB
            wal_enabled: true,
            auto_checkpoint_interval: 1000,
            max_connections: 100,
            query_timeout_ms: 30000, // 30 seconds
            enable_vector_search: false,
            vector_dimension: 128,
            similarity_threshold: 0.7,
        }
    }
}

impl Config {
    /// Creates a new `Builder` for fluent configuration
#[must_use]
pub const fn builder() -> Builder {
    Builder::new()
    }

    /// Validates the configuration
    ///
    /// This method follows the Single Responsibility Principle by focusing only on validation
    pub fn validate(&self) -> Result<(), OxidbError> {
        if self.max_cache_size == 0 {
            return Err(OxidbError::Configuration(
                "max_cache_size must be greater than 0".to_string(),
            ));
        }

        if self.max_connections == 0 {
            return Err(OxidbError::Configuration(
                "max_connections must be greater than 0".to_string(),
            ));
        }

        if self.query_timeout_ms == 0 {
            return Err(OxidbError::Configuration(
                "query_timeout_ms must be greater than 0".to_string(),
            ));
        }

        if self.enable_vector_search {
            if self.vector_dimension == 0 {
                return Err(OxidbError::Configuration(
                    "vector_dimension must be greater than 0 when vector search is enabled"
                        .to_string(),
                ));
            }

            if !(0.0..=1.0).contains(&self.similarity_threshold) {
                return Err(OxidbError::Configuration(
                    "similarity_threshold must be between 0.0 and 1.0".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Loads configuration from a TOML file.
    ///
    /// # Errors
    ///
    /// Returns `OxidbError::Configuration` if the file cannot be read or if parsing fails.
    pub fn load_from_file(path: &Path) -> Result<Self, OxidbError> {
        match fs::read_to_string(path) {
            Ok(contents) => {
                let config: Self = toml::from_str(&contents).map_err(|e| {
                    OxidbError::Configuration(format!(
                        // Changed
                        "Failed to parse config file '{}': {}",
                        path.display(),
                        e
                    ))
                })?;

                // Validate the loaded configuration
                config.validate()?;
                Ok(config)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(OxidbError::Io(e)),
        }
    }

    /// Loads configuration from an optional TOML file path.
    ///
    /// If `optional_path` is `Some(path)`, it attempts to load from that file.
    /// If `optional_path` is `None`, it returns the default configuration.
    /// If the file doesn't exist, it also returns the default configuration.
    ///
    /// # Errors
    ///
    /// Returns `OxidbError::Configuration` if the file exists but cannot be parsed.
    pub fn load_or_default(optional_path: Option<&Path>) -> Result<Self, OxidbError> {
        match optional_path {
            Some(path) => Self::load_from_file(path),
            None => Ok(Self::default()),
        }
    }

    /// Helper to get a `PathBuf` for the data directory.
    #[must_use]
    pub const fn data_dir_path(&self) -> &PathBuf {
        &self.data_dir
    }

    /// Helper to get a `PathBuf` for the index directory.
    #[must_use]
    pub const fn index_dir_path(&self) -> &PathBuf {
        &self.index_dir
    }

    /// Legacy compatibility method for `database_path`
    #[must_use]
    pub fn database_path(&self) -> PathBuf {
        self.database_file.clone()
    }

    /// Legacy compatibility method for `wal_path`
    #[must_use]
    pub fn wal_path(&self) -> PathBuf {
        self.data_dir.join("oxidb.wal")
    }

    /// Legacy compatibility method for `index_path`
    #[must_use]
    pub fn index_path(&self) -> PathBuf {
        self.index_dir.clone()
    }

    /// Creates a configuration optimized for vector operations
    pub fn for_vector_operations(dimension: usize, threshold: f32) -> Result<Self, OxidbError> {
        Self::builder()
            .enable_vector_search(true)
            .vector_dimension(dimension)
            .similarity_threshold(threshold)
            .max_cache_size(2 * 1024 * 1024) // 2MB for vector operations
            .build()
    }

    /// Creates a configuration optimized for high-performance operations
    pub fn for_high_performance() -> Result<Self, OxidbError> {
        Self::builder()
            .max_cache_size(10 * 1024 * 1024) // 10MB
            .max_connections(500)
            .auto_checkpoint_interval(5000)
            .wal_enabled(true)
            .build()
    }

    /// Creates a configuration for testing with minimal resources
    pub fn for_testing() -> Result<Self, OxidbError> {
        Self::builder()
            .max_cache_size(64 * 1024) // 64KB
            .max_connections(10)
            .query_timeout_ms(5000) // 5 seconds
            .auto_checkpoint_interval(100)
            .build()
    }
}

// Add this to src/core/mod.rs
// pub mod config;

// Add this to src/core/common/error.rs
// #[error("Configuration error: {0}")]
// Configuration(String),

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.data_dir, PathBuf::from("data"));
        assert_eq!(config.index_dir, PathBuf::from("oxidb_indexes"));
        assert_eq!(config.max_cache_size, 1024 * 1024);
        assert!(config.wal_enabled);
        assert_eq!(config.auto_checkpoint_interval, 1000);
        assert_eq!(config.max_connections, 100);
        assert_eq!(config.query_timeout_ms, 30000);
        assert!(!config.enable_vector_search);
        assert_eq!(config.vector_dimension, 128);
        assert_eq!(config.similarity_threshold, 0.7);
        assert_eq!(config.database_file, PathBuf::from("oxidb.db"));
    }

    #[test]
    fn test_config_builder() {
        let config = Config::builder()
            .data_dir("/custom/data")
            .index_dir("/custom/indexes")
            .max_cache_size(2048)
            .wal_enabled(false)
            .enable_vector_search(true)
            .vector_dimension(256)
            .similarity_threshold(0.8)
            .build()
            .unwrap();

        assert_eq!(config.data_dir, PathBuf::from("/custom/data"));
        assert_eq!(config.index_dir, PathBuf::from("/custom/indexes"));
        assert_eq!(config.max_cache_size, 2048);
        assert!(!config.wal_enabled);
        assert!(config.enable_vector_search);
        assert_eq!(config.vector_dimension, 256);
        assert_eq!(config.similarity_threshold, 0.8);
        assert_eq!(config.database_file, PathBuf::from("oxidb.db"));
    }

    #[test]
    fn test_config_validation() {
        // Test invalid cache size
        let result = Config::builder().max_cache_size(0).build();
        assert!(result.is_err());

        // Test invalid similarity threshold
        let result = Config::builder().enable_vector_search(true).similarity_threshold(1.5).build();
        assert!(result.is_err());

        // Test valid configuration
        let result = Config::builder()
            .enable_vector_search(true)
            .vector_dimension(128)
            .similarity_threshold(0.7)
            .build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_specialized_configs() {
        // Test vector operations config
        let config = Config::for_vector_operations(256, 0.8).unwrap();
        assert!(config.enable_vector_search);
        assert_eq!(config.vector_dimension, 256);
        assert_eq!(config.similarity_threshold, 0.8);
        assert_eq!(config.database_file, PathBuf::from("oxidb.db"));

        // Test high performance config
        let config = Config::for_high_performance().unwrap();
        assert_eq!(config.max_cache_size, 10 * 1024 * 1024);
        assert_eq!(config.max_connections, 500);
        assert_eq!(config.database_file, PathBuf::from("oxidb.db"));

        // Test testing config
        let config = Config::for_testing().unwrap();
        assert_eq!(config.max_cache_size, 64 * 1024);
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.database_file, PathBuf::from("oxidb.db"));
    }

    #[test]
    fn test_load_from_existing_file() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let config_content = r#"
            data_dir = "/tmp/test_data"
            index_dir = "/tmp/test_indexes"
            max_cache_size = 2048
            wal_enabled = false
            auto_checkpoint_interval = 500
            max_connections = 50
            query_timeout_ms = 15000
            enable_vector_search = true
            vector_dimension = 256
            similarity_threshold = 0.8
        "#;
        writeln!(temp_file, "{}", config_content).unwrap();

        let config = Config::load_from_file(temp_file.path()).unwrap();
        assert_eq!(config.data_dir, PathBuf::from("/tmp/test_data"));
        assert_eq!(config.index_dir, PathBuf::from("/tmp/test_indexes"));
        assert_eq!(config.max_cache_size, 2048);
        assert!(!config.wal_enabled);
        assert_eq!(config.auto_checkpoint_interval, 500);
        assert_eq!(config.max_connections, 50);
        assert_eq!(config.query_timeout_ms, 15000);
        assert!(config.enable_vector_search);
        assert_eq!(config.vector_dimension, 256);
        assert_eq!(config.similarity_threshold, 0.8);
        assert_eq!(config.database_file, PathBuf::from("oxidb.db"));
    }

    #[test]
    fn test_load_from_file_uses_defaults_for_missing_fields() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let config_content = r#"
            data_dir = "/tmp/test_data"
            max_cache_size = 2048
        "#;
        writeln!(temp_file, "{}", config_content).unwrap();

        let config = Config::load_from_file(temp_file.path()).unwrap();
        assert_eq!(config.data_dir, PathBuf::from("/tmp/test_data"));
        assert_eq!(config.max_cache_size, 2048);
        // Check that defaults are used for missing fields
        assert_eq!(config.index_dir, PathBuf::from("oxidb_indexes"));
        assert!(config.wal_enabled);
        assert_eq!(config.database_file, PathBuf::from("oxidb.db"));
    }

    #[test]
    fn test_load_from_non_existent_file_returns_default() {
        let non_existent_path = Path::new("/this/file/does/not/exist.toml");
        let config = Config::load_from_file(non_existent_path).unwrap();
        assert_eq!(config, Config::default());
    }

    #[test]
    fn test_load_from_malformed_file_returns_error() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let malformed_content = "this is not valid toml content";
        writeln!(temp_file, "{}", malformed_content).unwrap();

        let result = Config::load_from_file(temp_file.path());
        assert!(result.is_err());
        if let Err(OxidbError::Configuration(msg)) = result {
            // Changed
            assert!(msg.contains("Failed to parse config file"));
        } else {
            panic!("Expected OxidbError::Configuration, got {:?}", result); // Changed
        }
    }

    #[test]
    fn test_load_or_default_with_none() {
        let config = Config::load_or_default(None).unwrap();
        assert_eq!(config, Config::default());
    }

    #[test]
    fn test_load_or_default_with_path() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let config_content = r#"
            data_dir = "/custom/data"
            max_cache_size = 4096
        "#;
        writeln!(temp_file, "{}", config_content).unwrap();

        let config = Config::load_or_default(Some(temp_file.path())).unwrap();
        assert_eq!(config.data_dir, PathBuf::from("/custom/data"));
        assert_eq!(config.max_cache_size, 4096);
        assert_eq!(config.database_file, PathBuf::from("oxidb.db"));
    }

    #[test]
    fn test_load_or_default_with_non_existent_path() {
        let non_existent_path = Path::new("/this/file/does/not/exist.toml");
        let config = Config::load_or_default(Some(non_existent_path)).unwrap();
        assert_eq!(config, Config::default());
    }

    #[test]
    fn test_path_buf_helpers() {
        let config = Config::default();
        assert_eq!(config.data_dir_path(), &PathBuf::from("data"));
        assert_eq!(config.index_dir_path(), &PathBuf::from("oxidb_indexes"));
    }
}
