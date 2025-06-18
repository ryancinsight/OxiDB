# Software Architecture Document: oxidb

## 1. Introduction

This document describes the overall architecture of oxidb, a pure Rust database. It outlines the major components, their responsibilities, and how they interact. The architecture prioritizes modularity, safety, and leverages Rust's strengths.

## 2. Architectural Goals

*   **Modularity:** Components should be well-defined with clear interfaces, allowing for independent development and testing.
*   **Safety:** Utilize Rust's type system and ownership model to ensure memory safety and data integrity.
*   **Extensibility:** Design components to be extensible for future features.
*   **Clarity:** The architecture should be easy to understand and maintain.
*   **Minimalism:** Avoid unnecessary complexity and dependencies.

## 3. High-Level Architecture Overview

oxidb will follow a layered architecture consisting of the following primary components:

*   **API Layer:** Provides the external interface for interacting with the database (e.g., Rust functions, potentially a CLI or network interface in the future).
*   **Query Processor:** Responsible for parsing, validating, optimizing (future), and executing queries.
*   **Transaction Manager:** Handles concurrency control and ensures ACID properties (or a subset thereof).
*   **Storage Engine:** Manages the physical storage and retrieval of data on disk. Includes sub-components for page management, indexing, and data serialization.
*   **Common Utilities:** Core data structures, error types, and helper functions used across multiple components.

## 4. Component Descriptions

### 4.1. API Layer (`src/api`)

*   **Responsibilities:**
    *   Expose public functions for database operations (connect, execute_query, etc.).
    *   Handle request validation from the client.
    *   Format results for the client.
*   **Key Interactions:**
    *   Receives requests and forwards them to the Query Processor.
    *   May interact with the Transaction Manager to begin/commit/rollback transactions.

### 4.2. Query Processor (`src/core/query`)

*   **Sub-components:**
    *   **Parser (`src/core/query/parser`):** Translates query strings (or API calls) into an internal representation (Abstract Syntax Tree - AST).
    *   **Validator (Future):** Checks the semantic correctness of queries.
    *   **Optimizer (Future):** Improves query execution plans.
    *   **Executor:** Takes the (optimized) query plan and interacts with the Storage Engine and Transaction Manager to fulfill the query.
*   **Responsibilities:**
    *   Provide an intermediate representation for queries.
    *   Execute the query logic.
*   **Key Interactions:**
    *   Receives query requests from the API Layer.
    *   Interacts with the Storage Engine to fetch or modify data.
    *   Coordinates with the Transaction Manager for transactional operations.

### 4.3. Transaction Manager (`src/core/transaction`)

*   **Responsibilities:**
    *   Ensure atomicity, consistency, isolation, and durability (ACID properties). Initially, focus will be on atomicity for single operations.
    *   Manage concurrent access to data (e.g., using locking mechanisms).
    *   Handle transaction begin, commit, and rollback.
*   **Key Interactions:**
    *   Works closely with the Query Executor and Storage Engine during data manipulation.

### 4.4. Storage Engine (`src/core/storage`)

*   **Sub-components:**
    *   **Engine (`src/core/storage/engine`):** Core logic for managing data on disk.
    *   **Write-Ahead Log (`src/core/wal/`):** Ensures durability and atomicity through logging changes before they are applied to persistent storage.
    *   **Page Manager (Future):** Handles allocation and deallocation of pages on disk.
    *   **Buffer Manager (Future):** Caches frequently accessed data in memory.
    *   **Indexer (Future):** Manages index structures (e.g., B-Trees) for efficient data lookup.
    *   **Serializer/Deserializer:** Converts in-memory data structures to byte streams for storage and vice-versa.
*   **Responsibilities:**
    *   Persist data to disk reliably.
    *   Retrieve data from disk efficiently.
    *   Manage data layout and organization.
    *   Implement mechanisms for data integrity at the storage level.
*   **Key Interactions:**
    *   Provides data access primitives to the Query Executor and Transaction Manager.

### 4.5. Common Utilities (`src/core/common`)

*   **Sub-components:**
    *   **Types (`src/core/common/types`):** Defines fundamental data types used throughout the database (e.g., for representing values, schema, errors).
    *   **Error Handling:** Centralized error types and handling mechanisms.
    *   **Configuration (Future):** Manages database configuration settings.
*   **Responsibilities:**
    *   Provide shared, reusable components and definitions.

### 4.6. Event Engine (`src/event_engine`)

*   **Purpose:** Provides a decoupled mechanism for handling asynchronous events and managing event-driven logic within the system. It allows different parts of the application to react to occurrences without being tightly coupled to the source of the event.
*   **Sub-components:**
    *   **Handler (`src/event_engine/handler`):** The core of the event engine, responsible for processing individual events.
        *   `types.rs`: Defines event structures (`Event` enum) and result types (`EventResult`).
        *   `core.rs`: Contains the central event dispatch logic (`process_event` function). This logic is designed to be "flat," delegating processing to specialized processors.
        *   `processors.rs`: Defines the `Processor` trait and provides concrete implementations for each event type, encapsulating the specific handling logic.
*   **Responsibilities:**
    *   Define a clear set of system events.
    *   Dispatch events to appropriate handlers/processors.
    *   Execute event-specific logic in a modular and extensible way.
*   **Key Interactions:**
    *   Receives events from various components within the system (e.g., API layer after a user action, storage engine after a data change, etc. - specific integrations to be defined).
    *   May interact with any other component as part of an event's processing logic (e.g., sending notifications, updating data, logging).
*   **Architectural Principles Applied:**
    *   **Law of Sacred Spaces:** The `event_engine` is a distinct top-level module.
    *   **Hierarchical Decomposition:** The `handler` submodule further organizes event processing logic.
    *   **Duality of Depth and Flatness:** The `process_event` function in `handler/core.rs` uses a flat dispatch mechanism (delegating to processors) to avoid deep conditional nesting.
    *   **Law of Internal Composition:** The `handler` module's files (`types.rs`, `core.rs`, `processors.rs`) represent its Skeleton, Mind, and Soul, respectively.

## 5. Data Flow

*   A typical read query: `API Layer -> Query Parser -> Query Executor -> Storage Engine -> Query Executor -> API Layer`
*   A typical write query: `API Layer -> Query Parser -> Query Executor -> Transaction Manager -> Storage Engine -> Transaction Manager -> Query Executor -> API Layer`

## 6. Modularity, Separation of Concerns, and Directory Structure

The architecture emphasizes strong modularity and clear separation of concerns, which is directly reflected in the project's directory structure. We adhere to a **Deep Vertical File Tree** philosophy, particularly within the `src/core` module.

**Philosophy:**

*   **Clear Ownership:** Each fine-grained component or sub-feature resides in its own dedicated directory. This makes ownership واضح (clear) and responsibilities distinct.
*   **Reduced Cognitive Load:** When working on a specific deeply nested component (e.g., a particular storage engine mechanism or a query parsing rule), the relevant files are co-located, minimizing the need to jump across wide, flat directory structures.
*   **Improved Navigability:** While the tree can become deep, it provides a logical path to components. For instance, a B-Tree implementation within the storage engine would naturally reside in a path like `src/core/storage/engine/b_tree/`.
*   **Scalability:** As the system grows, new sub-modules or deeper specializations can be added without cluttering existing directories, maintaining organizational clarity.

**Implementation in `oxidb`:**

The `src/` directory is organized as follows:

*   **`src/api/`**: Contains all code related to the database's external Application Programming Interface.
*   **`src/core/`**: Houses the core database logic. This is where the deep vertical structure is most prominent:
    *   **`src/core/common/`**: For truly cross-cutting concerns like custom error types (`error.rs`), serialization utilities (`serialization.rs`), shared traits (`traits.rs`), and fundamental data type definitions (`types/`). The `types/` subdirectory further organizes type-related definitions.
    *   **`src/core/execution/`**: Manages the execution of query plans.
        *   `operators/`: Contains distinct subdirectories for different types of execution operators (e.g., `scans/`, `joins/`, `filters/`), allowing each operator's logic to be self-contained.
    *   **`src/core/indexing/`**: Dedicated to data indexing mechanisms. Specific index types (e.g., `btree/`, `hash/`) have their own subdirectories. This consolidates all indexing logic previously scattered (e.g., removing `src/core/storage/indexing/`).
    *   **`src/core/optimizer/`**: Concerned with query optimization.
        *   `rules/`: Subdirectory for individual optimization rules or rule sets.
    *   **`src/core/query/`**: Handles the initial stages of query processing.
        *   `parser/`: Contains all logic for parsing SQL strings into an Abstract Syntax Tree (AST), including lexer and AST definitions.
        *   `binder/`: For semantic analysis and binding identifiers.
        *   `planner/`: For converting ASTs into logical and physical query plans.
        *   `statements/`: For specific SQL statement handlers or structures.
    *   **`src/core/storage/`**: Manages the persistence and retrieval of data.
        *   `engine/`: The core storage engine, with subdirectories for its fundamental components like `page_manager/`, `buffer_pool/`, `heap/` (for table heap management), and potentially specific storage structures like `b_tree/` if tightly coupled with the engine's page management.
    *   **`src/core/transaction/`**: Manages transaction lifecycle and concurrency control.
    *   **`src/core/wal/`**: Manages the Write-Ahead Log for durability and recovery.
*   **`src/lib.rs`**: The root of the Rust library.

Each module (directory) is intended to have a well-defined public API, primarily through its `mod.rs` file, which exports the necessary items for interaction with other modules. This hierarchical and granular approach aims to make the codebase more understandable, maintainable, and easier to extend by isolating concerns at each level of the system.

## 7. Safety and Rust Features

*   **Ownership and Borrowing:** Will be strictly enforced to prevent data races and dangling pointers.
*   **Generics and Traits:** Will be used extensively to allow for flexible data types and behaviors (e.g., different storage backends or index types in the future) while maintaining type safety.
*   **Error Handling:** Comprehensive use of `Result<T, E>` for all fallible operations. Custom error types will provide context.
*   **Newtype Pattern:** Will be used where appropriate to create distinct types for IDs, keys, etc., enhancing type safety.
