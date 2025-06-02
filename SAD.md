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

## 5. Data Flow

*   A typical read query: `API Layer -> Query Parser -> Query Executor -> Storage Engine -> Query Executor -> API Layer`
*   A typical write query: `API Layer -> Query Parser -> Query Executor -> Transaction Manager -> Storage Engine -> Transaction Manager -> Query Executor -> API Layer`

## 6. Modularity and Separation of Concerns

The file structure (e.g., `src/core/storage/engine`) reflects the modular design. Each module will have a well-defined public API (`mod.rs` or `lib.rs` within the module's directory) to interact with other modules. This deep vertical hierarchy aims to clearly separate concerns at each level of the system.

## 7. Safety and Rust Features

*   **Ownership and Borrowing:** Will be strictly enforced to prevent data races and dangling pointers.
*   **Generics and Traits:** Will be used extensively to allow for flexible data types and behaviors (e.g., different storage backends or index types in the future) while maintaining type safety.
*   **Error Handling:** Comprehensive use of `Result<T, E>` for all fallible operations. Custom error types will provide context.
*   **Newtype Pattern:** Will be used where appropriate to create distinct types for IDs, keys, etc., enhancing type safety.
