# Software Design Document: oxidb

## 1. Introduction

This document provides detailed design specifications for the components and modules within oxidb. It elaborates on the architectural concepts outlined in the SAD.md. This is a living document and will be updated as the design evolves.

## 2. Core Philosophies

*   **Safety First:** Design choices will prioritize data integrity and system robustness, leveraging Rust's safety features.
*   **Explicit is Better than Implicit:** Clear interfaces and explicit error handling.
*   **Minimalism:** Start with the simplest effective solution and iterate. Avoid premature optimization or over-engineering.
*   **Testability:** Design components to be easily unit-testable.

## 3. Detailed Design - Storage Engine (`src/core/storage`)

The Storage Engine is responsible for the physical persistence and retrieval of data.

### 3.1. Core Traits

To ensure flexibility and modularity, the storage engine will be built around a set of core traits.

#### 3.1.1. `DataSerializer<T>` and `DataDeserializer<T>`

*   **Purpose:** Handle the conversion of in-memory data types to and from byte representations suitable for disk storage.
*   **Generic Parameter:** `T` represents the data type to be serialized/deserialized.
*   **Draft Trait Definition (to be placed in `src/core/common/traits.rs` or similar):**
    ```rust
    // In a new file, e.g., src/core/common/traits.rs
    // pub mod traits { // Or directly in common module

    use std::io::{Read, Write};
    use crate::core::common::error::DbError; // Assuming DbError will be defined

    /// Trait for serializing data of type T into a byte stream.
    pub trait DataSerializer<T> {
        fn serialize<W: Write>(value: &T, writer: &mut W) -> Result<(), DbError>;
    }

    /// Trait for deserializing data of type T from a byte stream.
    pub trait DataDeserializer<T> {
        fn deserialize<R: Read>(reader: &mut R) -> Result<T, DbError>;
    }

    // }
    ```
*   **Considerations:**
    *   Error handling will use a custom `DbError` type.
    *   Initial implementations might use simple schemes like `bincode` or custom logic for basic types. The choice of serialization format will be critical for performance and flexibility. We will aim for minimal external dependencies here.

#### 3.1.2. `KeyValueStore<K, V>`

*   **Purpose:** Define the fundamental operations for a key-value storage layer.
*   **Generic Parameters:** `K` for key type, `V` for value type. Both `K` and `V` would typically be byte arrays (`Vec<u8>` or `&[u8]`) or types that can be serialized to them.
*   **Draft Trait Definition (to be placed in `src/core/storage/engine/traits.rs` or similar):**
    ```rust
    // In a new file, e.g., src/core/storage/engine/traits.rs
    // pub mod traits { // Or directly in engine module

    use crate::core::common::error::DbError; // Assuming DbError

    /// Trait for basic key-value store operations.
    pub trait KeyValueStore<K, V> {
        /// Inserts a key-value pair into the store.
        /// If the key already exists, its value is updated.
        fn put(&mut self, key: K, value: V) -> Result<(), DbError>;

        /// Retrieves the value associated with a key.
        /// Returns `Ok(Some(value))` if the key exists, `Ok(None)` otherwise.
        fn get(&self, key: &K) -> Result<Option<V>, DbError>;

        /// Deletes a key-value pair from the store.
        /// Returns `Ok(true)` if the key was found and deleted, `Ok(false)` otherwise.
        fn delete(&mut self, key: &K) -> Result<bool, DbError>;

        /// Checks if a key exists in the store.
        fn contains_key(&self, key: &K) -> Result<bool, DbError>;

        // Other potential methods:
        // fn scan(&self, key_prefix: &K) -> Result<Vec<(K, V)>, DbError>;
        // fn clear(&mut self) -> Result<(), DbError>;
    }

    // }
    ```
*   **Considerations:**
    *   Keys should ideally be byte-comparable for ordered iteration if B-tree like structures are used.
    *   Values can be arbitrary byte arrays.
    *   Error handling is crucial.

#### 3.1.3. `Index<K, PointerType>` (Placeholder - More detailed design later)

*   **Purpose:** Define operations for an index structure that maps keys to pointers/offsets within the data store.
*   **Generic Parameters:** `K` for key type, `PointerType` for the type representing the location of the data (e.g., file offset, page ID).
*   **Draft Trait Definition (conceptual, to be refined):**
    ```rust
    // Conceptual - details to be fleshed out in Phase 2
    // pub trait Index<K, PointerType> {
    //     fn insert_entry(&mut self, key: K, ptr: PointerType) -> Result<(), DbError>;
    //     fn find_entry(&self, key: &K) -> Result<Option<PointerType>, DbError>;
    //     fn delete_entry(&mut self, key: &K) -> Result<bool, DbError>;
    // }
    ```

### 3.3. SimpleFileKvStore Implementation (`src/core/storage/engine/simple_file_kv_store.rs`)

This provides a basic, persistent key-value store implementation using a single file for `oxidb`.

*   **Structure:**
    *   `file_path: PathBuf`: Stores the path to the data file.
    *   `cache: HashMap<Vec<u8>, Vec<u8>>`: An in-memory cache of the entire database content. All operations (`put`, `get`, `delete`, `contains_key`) primarily interact with this cache.

*   **Initialization (`new`)**:
    *   When a `SimpleFileKvStore` is created with a file path:
        *   It calls an internal `load_from_disk()` method.
        *   `load_from_disk()` attempts to open the file. If the file is not found, it's treated as a new store (empty cache), which is not an error.
        *   If the file exists, its entire content is read using a `BufReader`. The method then deserializes key-value pairs sequentially and populates the in-memory `cache`.
    *   This means the entire dataset is loaded into memory upon startup if the file exists and contains data.

*   **Write-Ahead Log (WAL) Mechanism**:
    *   **Purpose**: To enhance data durability for `put`/`delete` operations and improve write performance by avoiding full file rewrites on each modification. The `save_to_disk` method still periodically persists the complete state to the main data file.
    *   **Log Entry Format (`WalEntry`)**:
        *   The `WalEntry` enum (`src/core/storage/engine/wal.rs`) defines the structure of log entries:
            *   `Put { key: Vec<u8>, value: Vec<u8> }`: Represents a key-value insertion or update.
            *   `Delete { key: Vec<u8> }`: Represents a key deletion.
        *   On-disk serialization of each `WalEntry`:
            1.  **Operation Type (1 byte)**: `0x01` for `Put`, `0x02` for `Delete`.
            2.  **Serialized Key**: The key (`Vec<u8>`) is serialized using the standard length-prefixing format (length as `u64` + bytes).
            3.  **Serialized Value (for Put only)**: The value (`Vec<u8>`) is serialized using the standard length-prefixing format. This part is omitted for `Delete` entries.
            4.  **CRC32 Checksum (4 bytes)**: A CRC32 checksum of all preceding bytes in the entry (operation type + key + optional value) is appended to ensure integrity.
    *   **`put` and `delete` Operations with WAL**:
        *   When `put` or `delete` is called, the operation is no longer immediately written to the main data file by rewriting the entire dataset.
        *   Instead, a corresponding `WalEntry` (`Put` or `Delete`) is created.
        *   This `WalEntry` is serialized and appended to a dedicated WAL file (e.g., `[db_filename].wal`, where `[db_filename]` is the name of the main data file).
        *   The WAL file write is flushed and synced to disk to ensure the log entry is durable.
        *   After the WAL entry is successfully persisted, the in-memory `cache` is updated to reflect the change.
        *   The `save_to_disk()` method is no longer called directly by `put` or `delete`.
    *   **WAL Replay during `load_from_disk`**:
        *   After the initial step of loading data from the main data file (or performing recovery from a `.tmp` file if one exists), the `load_from_disk` method proceeds to check for a WAL file.
        *   If a `.wal` file associated with the database file exists, it signifies that there might be operations that haven't been persisted to the main data file yet.
        *   Entries are read from the WAL file sequentially. For each entry:
            *   The CRC32 checksum is verified.
            *   The entry is deserialized.
            *   The operation (`Put` or `Delete`) is replayed into the in-memory `cache`, bringing it up-to-date.
        *   If corruption is detected (e.g., checksum mismatch, invalid entry format, deserialization error), the WAL replay process stops at that point. Data recovered from valid entries up to the point of corruption is kept. This prevents a corrupted WAL entry from halting the entire database load.
    *   **WAL Truncation/Reset**:
        *   The WAL file is not allowed to grow indefinitely.
        *   After a successful `save_to_disk` operation (which writes the complete, current state of the in-memory `cache` to the main data file), the corresponding `.wal` file is deleted.
        *   This is safe because all operations recorded in the WAL up to that point are now reflected in the main data file. Deleting the WAL prevents replay of already persisted operations and reclaims disk space.

*   **Persistence (`save_to_disk`)**:
    *   To ensure data integrity, especially in the event of a crash during a write operation, `save_to_disk` employs an atomic write strategy.
    *   **Temporary File:** When saving, all data from the in-memory `cache` is first written to a new temporary file (e.g., `[original_filename].tmp`) in the same directory as the main database file. This uses a `BufWriter` for efficiency.
    *   **Flush and Sync:** After all data is written to the temporary file, its `BufWriter` is flushed, and the underlying file is explicitly synced to disk (e.g., via `File::sync_all()`) to ensure all data is persisted.
    *   **Atomic Rename:** If the write, flush, and sync operations to the temporary file are successful, the temporary file is then atomically renamed to the name of the main database file. This rename operation overwrites the original main database file, effectively committing the changes in a single, atomic step.
    *   **Error Handling and Cleanup:** An RAII guard (`TempFileGuard`) is used to ensure that if any error occurs during the creation of, writing to, flushing, or syncing of the temporary file, the temporary file is automatically deleted. This prevents orphaned temporary files and ensures that the original database file remains untouched if the process is interrupted before the atomic rename.
    *   This atomic write mechanism significantly mitigates data corruption by ensuring that the main database file always reflects a consistent state (either the old data or the completely new data).

*   **Initialization (`new`) / Loading (`load_from_disk`)**:
    *   When a `SimpleFileKvStore` is created, its `load_from_disk()` method is called to populate the in-memory `cache`. This method now includes recovery logic:
        *   **Temporary File Check:** Before attempting to load from the main database file, the store checks for the existence of a corresponding temporary file (e.g., `[filename].tmp`).
        *   **Recovery from Temporary File:** If a temporary file exists, it's considered potentially more up-to-date (due to an interrupted previous save). The store attempts to load data from this temporary file.
            *   If loading from the temporary file is successful, the temporary file is then atomically renamed to the main database file. This completes the interrupted save operation. If this rename fails, an error is reported, and the temporary file is left for potential manual recovery.
            *   If the temporary file is found but is corrupted (e.g., deserialization fails), it is deleted to prevent issues on subsequent loads. The store then proceeds to attempt loading from the main database file.
        *   **Loading from Main File:** If no temporary file is found, or if loading from a temporary file failed and it was cleaned up, the store attempts to load from the main database file as before. If the main file is not found, it's treated as a new, empty store.
    *   This recovery logic makes the store more resilient to crashes that might occur during `save_to_disk`.

*   **Serialization Format (in the file):**
    *   Each key-value pair is stored sequentially. Keys and values are `Vec<u8>`.
    *   The key is serialized first: its length as a `u64` (big-endian, 8 bytes), followed by the key bytes. This uses `Vec<u8>::serialize()` which in turn uses `u64::serialize()` for the length.
    *   The value is serialized immediately after its corresponding key, using the same length-prefixing format.
    *   `load_from_disk` reads these pairs one by one from the main data file. It first checks if the reader is at EOF using `fill_buf().is_empty()`. If not, it attempts to deserialize a key. If successful, it must find a value. Any premature EOF or deserialization error during this process is treated as a `DbError::StorageError`. After this, WAL replay occurs as described above.

*   **Error Handling:**
    *   I/O errors during file operations are wrapped in `DbError::IoError`.
    *   Failures during the load loop (e.g., malformed data, unexpected EOF) are typically wrapped into `DbError::StorageError`.
    *   **Deserialization Safety:** To enhance robustness against corrupted data files (which might specify excessively large lengths for keys or values), the `Vec<u8>::deserialize` and `String::deserialize` methods in `src/core/common/serialization.rs` now include a check against a `MAX_ALLOWED_ITEM_LENGTH`. If a deserialized length prefix exceeds this sanity limit (e.g., 256 MiB), a `DbError::StorageError` is returned, preventing potential excessive memory allocation and crashes. This is particularly relevant during `load_from_disk`.

*   **Limitations for this initial version:**
    *   **Scalability:** Loads the entire database into memory. Not suitable for datasets larger than available RAM.
    *   **Write Performance:** While individual `put`/`delete` operations are now faster due to WAL, `save_to_disk` still rewrites the entire dataset. The frequency of `save_to_disk` calls will influence overall write throughput characteristics.
    *   **Concurrency:** Not thread-safe for concurrent access.
    *   This implementation serves as a basic starting point for `oxidb`.

### 3.4. Indexing Strategies (`src/core/storage/indexing`)

To optimize data retrieval, especially for storage engines that don't load the entire dataset into memory, indexing is crucial. An index maps keys (or parts of keys) to the physical location of the data.

#### 3.4.1. `Index<K, P>` Trait and `DataPointer`

A generic `Index` trait is defined in `src/core/storage/indexing/traits.rs` to abstract index operations:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DataPointer(pub u64); // Example: u64 for file offset

pub trait Index<K, P> {
    fn insert_entry(&mut self, key: K, ptr: P) -> Result<(), DbError>;
    fn find_entry(&self, key: &K) -> Result<Option<P>, DbError>;
    fn delete_entry(&mut self, key: &K) -> Result<Option<P>, DbError>;
}
```

*   `K` is the key type.
*   `P` is the pointer type, like `DataPointer` which could represent a byte offset in a file.
*   This trait provides basic operations to manage index entries.

#### 3.4.2. Conceptual Role and `SimpleFileKvStore`

*   **Performance:** Indexes allow for faster lookups by avoiding full scans of data. Instead of reading and deserializing all data until a key is found, an index can quickly point to the data's location.

*   **`SimpleFileKvStore` Context:**
    *   The current `SimpleFileKvStore` loads all key-value pairs into an in-memory `HashMap<Vec<u8>, Vec<u8>>`. This `HashMap` itself acts as a very fast index where values are directly available in memory.
    *   Therefore, a separate index mapping keys to file offsets (e.g., `HashMap<Vec<u8>, DataPointer>`) would not improve `get()` performance for *this specific implementation*, as the value is already cached in RAM.

*   **Future Storage Engines:**
    *   The `Index` trait and `DataPointer` are defined primarily for future, more advanced storage engines. For example:
        *   An **append-only log store**: Here, data is always appended to a file. An in-memory index (like a `HashMap<Key, DataPointer>`) would be built during startup by scanning the log. This index would map keys to their byte offsets in the log file. `get()` operations would use the index to find the offset, then seek and read the value from disk. `put()` would append to the log and update the index. `delete()` would append a tombstone record and update the index.
        *   **Page-based stores (e.g., B-Trees):** More complex index structures like B-Trees would directly manage data layout in pages on disk. These would implement the `Index` trait (or a more specialized version) to navigate the tree structure and find data.

*   **Simplest Index for Non-Caching `SimpleFileKvStore` Variant:**
    *   If `SimpleFileKvStore` were modified to *not* cache values in memory (i.e., only keys or no cache at all for values), then an in-memory `HashMap<Vec<u8>, DataPointer>` would be a viable simple indexing strategy.
    *   This index would be populated during `load_from_disk` by recording the byte offset of each value as it's encountered in the data file. `get()` would then use this index to seek directly to the value's position in the file and deserialize only that value.
    *   However, with the current full value caching in `SimpleFileKvStore`, this file-offset index remains a conceptual design for alternative or future engines.

### 3.2. Initial Data Safety and Rust Feature Strategy

*   **Error Handling:** A unified `DbError` enum will be defined in `src/core/common/error.rs`. This enum will cover I/O errors, serialization errors, not found errors, constraint violations, etc. The `thiserror` crate might be considered if it significantly simplifies error definition without adding much overhead.
*   **Generics:** As shown in the traits, generics will be used to make components adaptable to different data types (e.g., `KeyValueStore<K, V>`).
*   **Newtypes:** For identifiers (e.g., `PageId`, `TransactionId`), the newtype pattern (`struct PageId(u64);`) will be used to prevent accidental misuse of raw integer types. These will be defined in `src/core/common/types.rs`.
*   **Ownership/Borrowing:** Standard Rust practices will be followed. Functions will take ownership or borrow parameters as appropriate to minimize cloning and ensure memory safety. Lifetimes will be used where necessary, especially in API design.
*   **Traits for Behavior:** Core logic will be abstracted behind traits, allowing for different implementations (e.g., an in-memory `KeyValueStore` for testing, a persistent one for production).

## 4. Detailed Design - Common Utilities (`src/core/common`)

### 4.1. Error Handling (`src/core/common/error.rs`)
*   Define `DbError` enum.
    ```rust
    // In a new file src/core/common/error.rs
    // pub mod error { // Or directly in common module

    // Consider using the 'thiserror' crate if it simplifies things.
    // For now, a manual definition:
    #[derive(Debug)] // Add more derive macros as needed (e.g., PartialEq for testing)
    pub enum DbError {
        IoError(std::io::Error),
        SerializationError(String), // Or a more specific error type from a serialization crate
        DeserializationError(String), // Or a more specific error type
        NotFoundError(String),
        InvalidQuery(String),
        TransactionError(String),
        StorageError(String),
        InternalError(String), // For unexpected issues
        // Add more variants as needed
    }

    // Implement std::fmt::Display for DbError
    impl std::fmt::Display for DbError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                DbError::IoError(e) => write!(f, "IO Error: {}", e),
                DbError::SerializationError(s) => write!(f, "Serialization Error: {}", s),
                DbError::DeserializationError(s) => write!(f, "Deserialization Error: {}", s),
                DbError::NotFoundError(s) => write!(f, "Not Found: {}", s),
                DbError::InvalidQuery(s) => write!(f, "Invalid Query: {}", s),
                DbError::TransactionError(s) => write!(f, "Transaction Error: {}", s),
                DbError::StorageError(s) => write!(f, "Storage Error: {}", s),
                DbError::InternalError(s) => write!(f, "Internal Error: {}", s),
            }
        }
    }

    // Implement std::error::Error for DbError
    impl std::error::Error for DbError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                DbError::IoError(e) => Some(e),
                _ => None,
            }
        }
    }

    // Optional: Implement From<std::io::Error> for DbError
    impl From<std::io::Error> for DbError {
        fn from(err: std::io::Error) -> Self {
            DbError::IoError(err)
        }
    }
    // }
    ```

### 4.2. Core Types (`src/core/common/types.rs`)
*   Define newtypes for IDs.
    ```rust
    // In src/core/common/types.rs
    // pub mod types { // Or directly in common module

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)] // Add derives as needed
    pub struct PageId(pub u64);

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct TransactionId(pub u64);

    // Potentially define a generic Value type or enum here later
    // pub enum Value {
    //     Integer(i64),
    //     String(String),
    //     Boolean(bool),
    //     // ...
    // }
    // }
    ```

### 4.3. Serialization Implementations (`src/core/common/serialization.rs`)

Concrete implementations of the `DataSerializer` and `DataDeserializer` traits are provided in `src/core/common/serialization.rs` for common types.

*   **`u64`**:
    *   Serialized as 8 bytes in big-endian format using `u64::to_be_bytes()`.
    *   Deserialized from 8 bytes using `u64::from_be_bytes()`.
    *   This ensures consistent byte ordering across different platforms.

*   **`String`**:
    *   The string's length (number of bytes in its UTF-8 representation) is first serialized as a `u64` (which itself is serialized as 8 big-endian bytes).
    *   The actual UTF-8 bytes of the string are then written to the stream.
    *   Deserialization reads the length, then reads that many bytes, and finally attempts to convert them back to a `String` using `String::from_utf8()`. Errors during UTF-8 conversion are mapped to `DbError::DeserializationError`.

*   **`Vec<u8>`**:
    *   Similar to `String`, the vector's length (number of bytes) is first serialized as a `u64` (8 big-endian bytes).
    *   The raw bytes of the vector are then written to the stream.
    *   Deserialization reads the length, then reads that many bytes directly into a new `Vec<u8>`.
*   **Safety Enhancement**: Both `String` and `Vec<u8>` deserialization routines incorporate a check against `MAX_ALLOWED_ITEM_LENGTH` (e.g., 256 MiB). If a serialized length prefix indicates an item larger than this threshold, deserialization is aborted, and a `DbError::StorageError` is returned. This prevents attempts to allocate excessive memory due to corrupted length information in the data file, significantly improving robustness against malformed data.

## 5. Detailed Design - Query Processor (`src/core/query`)

The Query Processor is responsible for translating user intentions (whether from direct API calls or a query language in the future) into executable operations.

### 5.1. Internal Command Representation (`src/core/query/commands.rs`)

The first step in processing queries is to have a clear, internal representation of the operations the database can perform. This is defined in `src/core/query/commands.rs`.

*   **`Key` and `Value` Type Aliases:**
    *   `pub type Key = Vec<u8>;`
    *   `pub type Value = Vec<u8>;`
    *   These aliases provide semantic meaning to byte vectors used as keys and values.

*   **`Command` Enum:**
    *   This enum defines the set of possible operations:
        ```rust
        #[derive(Debug, PartialEq)]
        pub enum Command {
            Insert { key: Key, value: Value },
            Get { key: Key },
            Delete { key: Key },
        }
        ```
    *   **`Insert { key: Key, value: Value }`**: Represents an operation to store a new key-value pair or update an existing one.
    *   **`Get { key: Key }`**: Represents an operation to retrieve the value associated with a given key.
    *   **`Delete { key: Key }`**: Represents an operation to remove a key-value pair.
    *   This enum is fundamental for the query executor, which will pattern match on these commands to perform the corresponding actions against the storage layer.
    *   It's derived with `Debug` and `PartialEq` for ease of testing and inspection.

## 6. Detailed Design - API Layer (`src/api`)

The API Layer provides the primary interface for users to interact with the database. Initially, this will be a programmatic Rust API.

### 6.1. Basic API Design (`src/api/mod.rs`)

The functions in `src/api/mod.rs` are responsible for constructing `Command` objects that can then be passed to a (future) query execution engine.

*   **Conceptual Functions:**
    *   `prepare_insert(key: Key, value: Value) -> Command`: Creates a `Command::Insert`.
    *   `prepare_get(key: Key) -> Command`: Creates a `Command::Get`.
    *   `prepare_delete(key: Key) -> Command`: Creates a `Command::Delete`.
*   **Purpose:** These functions serve as the entry point for database operations. In a more complete system, they would likely be methods on a database handle or connection object (e.g., `db.insert(key, value)`), which would then interact with the query processor and transaction manager. For this conceptual stage, they directly return the `Command` enum.
*   **Future Evolution:** As the database matures, these API functions will:
    *   Take a reference to a database instance/context (e.g., `&mut Oxidb`).
    *   Invoke a query executor with the constructed `Command`.
    *   Handle results and errors returned by the executor, translating them for the user.
    *   The `DbError` type will be used for error propagation.
