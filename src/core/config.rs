// src/core/config.rs

use crate::core::common::OxidbError; // Changed
use serde::Deserialize;
use std::fs; // For reading file
use std::path::Path;
use std::path::PathBuf; // Import PathBuf for Default impl // For load_from_file argument

/// Represents the configuration for Oxidb.
///
/// This struct encapsulates various settings that can be tuned for the database.
/// It supports loading from a TOML file (e.g., `Oxidb.toml`) and provides
/// sensible default values.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)] // Optional: Be strict about unknown fields in TOML
pub struct Config {
    /// The path to the main database file.
    /// Default: "oxidb.db"
    #[serde(default = "default_database_file_path")]
    pub database_file_path: String,

    /// The base directory path for storing index files.
    /// Default: "oxidb_indexes/"
    #[serde(default = "default_index_base_path")]
    pub index_base_path: String,

    /// The path to the Write-Ahead Log (WAL) file.
    /// Default: "oxidb.wal"
    #[serde(default = "default_wal_file_path")]
    pub wal_file_path: String,

    // --- Future Configuration Options (with defaults) ---
    /// Enables or disables the Write-Ahead Log (WAL).
    /// Currently, WAL is always used if this feature is compiled. This is a placeholder.
    /// Default: true
    #[serde(default = "default_wal_enabled")]
    pub wal_enabled: bool,

    /// Approximate maximum size of the in-memory cache in megabytes (MB).
    /// This is a placeholder for future cache management enhancements.
    /// Default: 64
    #[serde(default = "default_cache_size_mb")]
    pub cache_size_mb: usize,

    /// Default transaction isolation level.
    /// This is a placeholder for future support of different isolation levels.
    /// Current behavior is typically Serializable or close to it.
    /// Default: "Serializable"
    #[serde(default = "default_isolation_level")]
    pub default_isolation_level: String,
}

// Default value functions for serde
fn default_database_file_path() -> String {
    "oxidb.db".to_string()
} // Added
fn default_index_base_path() -> String {
    "oxidb_indexes/".to_string()
} // Added
fn default_wal_file_path() -> String {
    "oxidb.wal".to_string()
}
fn default_wal_enabled() -> bool {
    true
}
fn default_cache_size_mb() -> usize {
    64
}
fn default_isolation_level() -> String {
    "Serializable".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Config {
            database_file_path: default_database_file_path(),
            index_base_path: default_index_base_path(),
            wal_file_path: default_wal_file_path(),
            wal_enabled: default_wal_enabled(),
            cache_size_mb: default_cache_size_mb(),
            default_isolation_level: default_isolation_level(),
        }
    }
}

impl Config {
    /// Loads configuration from a TOML file.
    ///
    /// If the specified file does not exist, default configuration values are returned.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the TOML configuration file.
    ///
    /// # Errors
    ///
    /// Returns `OxidbError::Configuration` if the file cannot be read or if parsing fails.
    pub fn load_from_file(path: &Path) -> Result<Self, OxidbError> {
        match fs::read_to_string(path) {
            Ok(contents) => toml::from_str(&contents).map_err(|e| {
                OxidbError::Configuration(format!( // Changed
                    "Failed to parse config file '{}': {}",
                    path.display(),
                    e
                ))
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
            Err(e) => Err(OxidbError::Io(e)), // Changed to Io variant
        }
    }

    /// Loads configuration from an optional TOML file path.
    ///
    /// If `optional_path` is `Some(path)`, it attempts to load from that file.
    /// If the file doesn't exist at `path`, or if `optional_path` is `None`,
    /// it returns the default configuration.
    ///
    /// # Arguments
    ///
    /// * `optional_path` - An `Option<&Path>` to the configuration file.
    ///
    /// # Errors
    ///
    /// Returns `OxidbError::Configuration` if a file path is provided but the file
    /// cannot be read or parsed.
    pub fn load_or_default(optional_path: Option<&Path>) -> Result<Self, OxidbError> {
        match optional_path {
            Some(path) => Self::load_from_file(path),
            None => Ok(Config::default()),
        }
    }

    // Helper to get database_file_path as PathBuf
    pub fn database_path(&self) -> PathBuf {
        PathBuf::from(&self.database_file_path)
    }

    // Helper to get index_base_path as PathBuf
    pub fn index_path(&self) -> PathBuf {
        PathBuf::from(&self.index_base_path)
    }

    // Helper to get wal_file_path as PathBuf
    pub fn wal_path(&self) -> PathBuf {
        PathBuf::from(&self.wal_file_path)
    }
}

// Add this to src/core/mod.rs
// pub mod config;

// Add this to src/core/common/error.rs
// #[error("Configuration error: {0}")]
// ConfigError(String),

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.database_file_path, "oxidb.db");
        assert_eq!(config.index_base_path, "oxidb_indexes/");
        assert_eq!(config.wal_file_path, "oxidb.wal");
        assert!(config.wal_enabled);
        assert_eq!(config.cache_size_mb, 64);
        assert_eq!(config.default_isolation_level, "Serializable");
    }

    #[test]
    fn test_load_from_existing_file() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let config_content = r#"
            database_file_path = "my_custom.db"
            index_base_path = "my_custom_indexes/"
            wal_file_path = "my_custom.wal"
            wal_enabled = false
            cache_size_mb = 128
            default_isolation_level = "ReadCommitted"
        "#;
        writeln!(temp_file, "{}", config_content).unwrap();

        let config = Config::load_from_file(temp_file.path()).unwrap();

        assert_eq!(config.database_file_path, "my_custom.db");
        assert_eq!(config.index_base_path, "my_custom_indexes/");
        assert_eq!(config.wal_file_path, "my_custom.wal");
        assert!(!config.wal_enabled);
        assert_eq!(config.cache_size_mb, 128);
        assert_eq!(config.default_isolation_level, "ReadCommitted");
    }

    #[test]
    fn test_load_from_file_uses_defaults_for_missing_fields() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let config_content = r#"
            database_file_path = "partial.db"
            # index_base_path is missing
        "#;
        writeln!(temp_file, "{}", config_content).unwrap();

        let config = Config::load_from_file(temp_file.path()).unwrap();

        assert_eq!(config.database_file_path, "partial.db");
        assert_eq!(config.index_base_path, "oxidb_indexes/"); // Should be default
        assert_eq!(config.wal_file_path, "oxidb.wal"); // Should be default
        assert!(config.wal_enabled); // Default
        assert_eq!(config.cache_size_mb, 64); // Default
        assert_eq!(config.default_isolation_level, "Serializable"); // Default
    }

    #[test]
    fn test_load_from_non_existent_file_returns_default() {
        let non_existent_path = Path::new("non_existent_config.toml");
        let config = Config::load_from_file(non_existent_path).unwrap();
        assert_eq!(config.database_file_path, Config::default().database_file_path);
        assert_eq!(config.index_base_path, Config::default().index_base_path);
        assert_eq!(config.wal_file_path, Config::default().wal_file_path);
    }

    #[test]
    fn test_load_from_malformed_file_returns_error() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let malformed_content = "this is not valid toml content";
        writeln!(temp_file, "{}", malformed_content).unwrap();

        let result = Config::load_from_file(temp_file.path());
        assert!(result.is_err());
        if let Err(OxidbError::Configuration(msg)) = result { // Changed
            assert!(msg.contains("Failed to parse config file"));
        } else {
            panic!("Expected OxidbError::Configuration, got {:?}", result); // Changed
        }
    }

    #[test]
    fn test_load_or_default_with_path() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let config_content = r#"database_file_path = "custom_via_load_or_default.db""#;
        writeln!(temp_file, "{}", config_content).unwrap();

        let config = Config::load_or_default(Some(temp_file.path())).unwrap();
        assert_eq!(config.database_file_path, "custom_via_load_or_default.db");
        assert_eq!(config.wal_file_path, "oxidb.wal"); // Default as not specified in file
    }

    #[test]
    fn test_load_or_default_with_none() {
        let config = Config::load_or_default(None).unwrap();
        assert_eq!(config.database_file_path, Config::default().database_file_path);
        assert_eq!(config.wal_file_path, Config::default().wal_file_path);
    }

    #[test]
    fn test_load_or_default_with_non_existent_path() {
        let non_existent_path = Path::new("another_non_existent.toml");
        let config = Config::load_or_default(Some(non_existent_path)).unwrap();
        assert_eq!(config.database_file_path, Config::default().database_file_path);
        assert_eq!(config.wal_file_path, Config::default().wal_file_path);
    }

    #[test]
    fn test_path_buf_helpers() {
        let config = Config {
            database_file_path: "test.db".to_string(),
            index_base_path: "test_indexes/".to_string(),
            wal_file_path: "test.wal".to_string(),
            ..Default::default()
        };
        assert_eq!(config.database_path(), PathBuf::from("test.db"));
        assert_eq!(config.index_path(), PathBuf::from("test_indexes/"));
        assert_eq!(config.wal_path(), PathBuf::from("test.wal"));
    }
}
