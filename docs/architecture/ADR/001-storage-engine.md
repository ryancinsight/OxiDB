# ADR-001: Core Storage Engine Design

*   **Status:** Proposed
*   **Date:** $(date +'%Y-%m-%d')
*   **Deciders:** AI Architect
*   **Context:** The Oxidb cathedral requires a persistent storage mechanism to store and retrieve data. This is a foundational component for all database operations. The `progress_ledger.md` indicates the Storage Engine is "Under Construction," but no formal ADR exists to guide its development.
*   **Decision:** We will design and implement a Storage Engine based on a Key-Value abstraction. The initial implementation will be a simple, single-file based Key-Value store to facilitate rapid prototyping and establish core interfaces. Future ADRs may propose more sophisticated storage mechanisms (e.g., LSM Trees, B-Trees, page-based storage).

## Requirements

1.  **Key-Value Interface:** The engine must provide a basic get/put/delete interface for byte array keys and values.
2.  **Durability (Initial Scope):** Data should be persisted to disk. The initial implementation will focus on basic file writes. More robust durability (e.g., WAL, fsync control) will be addressed in subsequent ADRs and implementations.
3.  **Testability:** The design must allow for comprehensive unit testing of its components.
4.  **Extensibility:** The core traits should be designed to accommodate different storage backend implementations in the future.

## Proposed Core Traits (to be located in `src/core/storage/engine/traits.rs`)

```rust
/// Represents a generic error type for storage operations.
pub trait StorageError: std::error::Error + Send + Sync + 'static {}

/// Defines the core Key-Value store operations.
pub trait KvStore {
    type Error: StorageError;

    /// Retrieves a value associated with a key.
    /// Returns `Ok(Some(value))` if the key exists.
    /// Returns `Ok(None)` if the key does not exist.
    /// Returns `Err(Self::Error)` for storage errors.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Inserts or updates a key-value pair.
    /// Returns `Ok(())` on success.
    /// Returns `Err(Self::Error)` for storage errors.
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error>;

    /// Deletes a key-value pair.
    /// Returns `Ok(())` on success, even if the key didn't exist.
    /// Returns `Err(Self::Error)` for storage errors.
    fn delete(&mut self, key: &[u8]) -> Result<(), Self::Error>;

    // Future considerations: iterators, atomic operations, etc.
}

/// Represents the main storage engine, potentially managing multiple KV stores or tables.
/// For the initial simple file KV store, this might be synonymous with the KvStore itself.
pub trait StorageEngine {
    type KvStore: KvStore;
    type Error: StorageError; // Or associated type from KvStore's error

    // Method to get or create/open a specific KV store instance (e.g., by name or path)
    // For a single-file KV store, this might just return the main store.
    fn open_kv_store(&self, path_or_name: &str) -> Result<Self::KvStore, Self::Error>;
}
```

## Rationale for Initial Simple File KV Store

*   **Simplicity:** Allows focusing on core trait design and overall database structure without premature optimization or complex storage internals.
*   **Speed of Development:** Enables quicker implementation of higher-level components that depend on storage.
*   **Foundation:** Provides a concrete implementation against which the core storage traits can be refined.

## Consequences

*   The initial storage engine will have performance limitations.
*   Advanced features like transactions, concurrency control, and rich indexing will require more sophisticated storage backends, to be defined in future ADRs.
*   The `simple_file_kv_store.rs` will be the first concrete implementation of the `KvStore` trait.

## Future Considerations

*   Definition of `StorageError` types using `thiserror`.
*   Integration with a Write-Ahead Log (WAL) for better durability.
*   Development of a page-based storage system and buffer pool.
*   Introduction of indexing mechanisms.
