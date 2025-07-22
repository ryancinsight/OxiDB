# Product Requirements Document: oxidb

## 1. Introduction

This document outlines the product requirements for oxidb, a pure Rust-based database system. The project has evolved from a learning prototype to a sophisticated database implementation featuring ACID compliance, advanced indexing, vector operations for RAG, and comprehensive SQL support.

## 2. Current Achievement Status

*   **✅ Functional database prototype** - Complete with 683 passing tests
*   **✅ Data safety and integrity** - ACID compliance with WAL and recovery
*   **✅ Efficient storage and retrieval** - Multiple indexing strategies (B+ Tree, Blink Tree, Hash, HNSW)
*   **✅ Clear and documented codebase** - Comprehensive architecture documentation

## 3. Target Audience

*   **Production developers** seeking a lightweight, embedded Rust database
*   **Database researchers** exploring advanced indexing and vector search
*   **Rust ecosystem contributors** requiring ACID-compliant embedded storage
*   **AI/ML developers** needing vector database capabilities for RAG applications

## 4. High-Level Features

*   **Data Storage:** Persistent storage of data.
*   **CRUD Operations:** Support for Create, Read, Update, and Delete operations, exposed via a programmatic Rust API.
*   **Data Types:** Initial support for basic data types (e.g., integers, strings, booleans).
*   **Querying:** Initial version will support direct key-based operations (get, insert, delete) via the programmatic Rust API. Specifics of more advanced querying to be defined later.
*   **Transactions:** Basic transactional support (atomicity for single operations initially).
*   **Safety:** Strong emphasis on compile-time and run-time safety.
*   **Configuration:** Minimal configuration, sensible defaults.
*   **Vector Support:** Store and query vector embeddings for RAG.

## 5. Non-Functional Requirements

*   **Performance:** While not the primary initial focus, the design should allow for future performance optimizations.
*   **Reliability:** Data should be durable and consistent.
*   **Maintainability:** Code should be well-structured, commented, and easy to understand.
*   **Minimal Dependencies:** External libraries should be used sparingly.

## 6. Future Considerations (Out of Scope for Initial Version)

*   Advanced indexing (beyond basic vector indexing)
*   Complex query language (SQL-like integration with vector search)
*   Concurrency control for multi-user access
*   Network interface
*   Replication / Distributed operations
*   Advanced RAG capabilities (e.g., sophisticated chunking, re-ranking, knowledge graph integration)
