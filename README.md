# oxidb: A Pure Rust Database

## Overview

oxidb is a learning project to implement a pure Rust database emphasizing safety, Rust-specific features (generics, traits), a deep vertical file tree structure, and minimal dependencies. This project aims to explore database design principles and Rust's capabilities in building performant and safe systems.

## Goals

*   **Safety:** Leverage Rust's ownership and type system to ensure data integrity and prevent common database vulnerabilities.
*   **Rust Features:** Extensively use generics, traits, and other Rust idioms to create flexible and maintainable code.
*   **Deep Vertical Hierarchy:** Implement a well-organized file structure with clear separation of concerns (at least 5 levels deep).
*   **Minimal Dependencies:** Rely on the Rust standard library as much as possible, only introducing external crates when absolutely necessary.
*   **Learning & Exploration:** Serve as a practical project for understanding database internals and advanced Rust programming.

## Configuration

Oxidb can be configured via an `Oxidb.toml` file placed in the root of your project. If this file is not present, or if specific settings are omitted within the file, Oxidb will use sensible default values.

The primary configurable options currently include:

*   `database_file_path`: Specifies the path to the main database file (e.g., `"my_data.db"`).
*   `index_base_path`: Defines the base directory where index files will be stored (e.g., `"my_indexes/"`).

Additionally, the following options are placeholders for future enhancements and will use their default values:

*   `wal_enabled`: (Default: `true`) Controls the Write-Ahead Log.
*   `cache_size_mb`: (Default: `64`) Approximate maximum size of the in-memory cache in MB.
*   `default_isolation_level`: (Default: `"Serializable"`) Default transaction isolation level.

Please refer to the sample `Oxidb.toml` file included in the repository for a detailed example.

### Instantiating Oxidb

You can create an Oxidb instance in several ways:

*   `Oxidb::new(db_path)`: The simplest method if you only need to specify the database file path. Other settings will use defaults. The `index_base_path` will default to `"oxidb_indexes/"` created in the same directory as the `db_path`'s parent if possible, or in the current working directory.
*   `Oxidb::new_from_config_file(config_file_path)`: Loads configuration from the specified TOML file.
*   `Oxidb::new_with_config(config_struct)`: Allows for programmatic configuration by passing a `Config` struct.

## Contribution Guidelines

(To be defined)
