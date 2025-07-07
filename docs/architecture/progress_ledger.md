# Progress Ledger: The Cathedral's Construction

This ledger tracks the status of major features and components of the Oxidb cathedral.

## Core Architectural Pillars

| Feature             | Status        | Required Components (Illustrative) | Notes                                      |
|---------------------|---------------|------------------------------------|--------------------------------------------|
| **API Layer**       | Under Construction | `api/mod.rs`, `api/types.rs`, `api/errors.rs`, `api/traits.rs`, `api/api_impl.rs` | External interface for database interaction. |
| **Query Processor** | Partially Implemented | `core/query/parser/`, `core/query/executor/`, `core/query/optimizer/` | Parsing, validation, optimization, execution. |
| **Transaction Mgr.**| Partially Implemented | `core/transaction/manager.rs`      | ACID properties, concurrency.              |
| **Storage Engine**  | Partially Implemented | `core/storage/engine/`, `core/storage/wal/`, `core/storage/indexing/` | Physical data storage and retrieval.       |
| **Common Utilities**| Substantially Implemented | `core/common/error.rs`, `core/common/types.rs`, `core/common/serialization.rs` | Shared types, errors, utils.             |
| **Event Engine**    | Initial Implementation | `event_engine/handler/`            | For asynchronous event processing.         |

## Detailed Component Status

### API Layer (`src/api`)
*   Status: Under Construction
*   Checklist:
    *   [x] `mod.rs` (Public API definition, updated)
    *   [x] `types.rs` (API specific data types)
    *   [x] `errors.rs` (API specific error types, boilerplate)
    *   [x] `traits.rs` (Initial API traits (OxidbApi) defined.)
    *   [x] `api_impl.rs` (API implementation logic)
    *   [ ] ADR for API design choices (specific to API behavior, structure governed by ADR-000)

### Query Processor (`src/core/query`)
*   Status: Partially Implemented
*   Sub-Modules:
    *   **Parser** (`src/core/query/parser`)
        *   Status: Fully Implemented
        *   Checklist:
            *   [x] `mod.rs`
            *   [x] `sql/tokenizer.rs` (Note: Tokenizer is within `src/core/query/sql/`)
            *   [x] `sql/ast.rs` (Note: AST definitions are within `src/core/query/sql/`)
            *   [x] `sql/parser/ (core.rs, statement.rs, expression.rs)` (Note: Parser modules are within `src/core/query/sql/`)
            *   [x] Comprehensive test coverage for all major SQL statements
            *   [ ] ADR for query language/parser design
        *   **Implementation Notes**:
            *   Full recursive descent parser implementation
            *   Supports DDL: CREATE TABLE (with constraints), DROP TABLE
            *   Supports DML: SELECT (with WHERE, JOIN, ORDER BY, LIMIT), INSERT, UPDATE, DELETE
            *   Comprehensive error handling with position tracking
            *   Extensive test suite in `tests/parser_tests.rs`
    *   **Binder** (`src/core/query/binder`)
        *   Status: Partially Implemented
        *   Checklist:
            *   [x] `mod.rs` (Created with module definition for binder.rs)
            *   [x] `binder.rs` (Initial implementation with Binder struct, placeholder BoundStatement/BindError, and bind_statement method. Currently returns NotImplemented.)
    *   **Planner** (`src/core/query/planner`)
        *   Status: Implemented (Distributed)
        *   Note: Logical plan generation (AST to QueryPlanNode) is handled by `build_initial_plan` in `src/core/optimizer/optimizer.rs`. Physical plan translation (QueryPlanNode to ExecutionOperator tree) is handled by `src/core/query/executor/planner.rs`.
    *   **Optimizer** (`src/core/optimizer`)
        *   Status: Partially Implemented
        *   Checklist:
            *   [x] `mod.rs` (Defines plan nodes and core structures)
            *   [x] `optimizer.rs` (Initial plan building and basic optimization passes implemented)
            *   [~] `rules/` (Directory exists, constant_folding_rule.rs implemented)
            *   [ ] ADR for optimization strategies
    *   **Executor** (`src/core/query/executor/`)
        *   Status: Partially Implemented
        *   Checklist:
            *   [x] `mod.rs`
            *   [x] `operator logic` (Dedicated operator implementations exist in `src/core/execution/operators/`)
            *   [x] Basic DELETE statement execution and WAL logging
            *   [ ] ADR for execution model

### Transaction Manager (`src/core/transaction`)
*   Status: Partially Implemented
*   Checklist:
    *   [x] `mod.rs`
    *   [x] `transaction.rs` (Transaction struct and lifecycle implemented)
    *   [x] `manager.rs` (Transaction management implemented with WAL logging)
    *   [x] `lock_manager.rs` (Locking mechanisms implemented)
    *   [x] `errors.rs` (TransactionError enum defined for specific transaction issues)
    *   [ ] ADR for concurrency control strategy

### Storage Engine (`src/core/storage`)
*   Status: Partially Implemented
*   Sub-Modules:
    *   **Engine** (`src/core/storage/engine`)
        *   Status: Partially Implemented
        *   Checklist:
            *   [x] `mod.rs` (Exists and defines module structure)
            *   [x] `traits/mod.rs` (Core storage traits defined in `traits/mod.rs`)
            *   [x] `simple_file/store.rs` (SimpleFileKvStore implemented in `implementations/simple_file/store.rs`)
            *   [?] `page_manager.rs` (No specific high-level page_manager.rs; `disk_manager.rs` handles low-level page I/O, `page.rs` defines page structure)
            *   [x] `buffer_pool_manager.rs` (BufferPoolManager implemented in `buffer_pool_manager.rs`, `buffer_pool/` dir is placeholder)
            *   [x] ADR for storage formats and strategies (ADR-001)
    *   **Write-Ahead Log (WAL)** (`src/core/wal`)
        *   Status: Substantially Implemented
        *   Checklist:
            *   [x] `mod.rs` (Exists and defines module structure)
            *   [x] `log_manager.rs` (LogManager for LSN allocation implemented)
            *   [x] `log_record.rs` (LogRecord enum with various record types defined)
            *   [x] `writer.rs` (WalWriter for buffering and writing log records implemented)
            *   [x] `reader.rs` (WalReader for parsing and reading WAL files implemented with comprehensive functionality)
            *   [x] Verified LSN integrity for physical WAL entries (including INSERT, UPDATE, DELETE)
            *   [ ] ADR for WAL implementation
    *   **Indexing** (`src/core/indexing`)
        *   Status: Substantially Implemented (B+-Tree, Blink Tree Completed; R-Tree Foundation)
        *   Checklist:
            *   [x] `mod.rs` (Exists and defines module structure)
            *   [x] `traits.rs` (Index trait defined)
            *   [x] `hash/` (HashIndex implementation refactored into submodule structure)
            *   [x] `manager.rs` (IndexManager for managing multiple indexes implemented)
            *   [x] `btree/` (B+-Tree FULLY IMPLEMENTED using fixed-size pages, supporting insert, find, delete with rebalancing)
            *   [x] `blink_tree/` (Blink Tree FULLY IMPLEMENTED with concurrent access and lock-free reads)
            *   [~] `rtree/` (R-Tree geometric foundation implemented, core algorithms in progress)
            *   [x] ADR for indexing strategies (See ADR-002 for B+-Tree)
        *   **B+-Tree Implementation Notes**:
            *   Complete implementation in `src/core/indexing/btree/`
            *   Fixed-size page design (PAGE_SIZE = 4096 bytes)
            *   Full support for insert, delete, search, and range scan operations
            *   Advanced rebalancing logic including borrowing from siblings and node merging
            *   Comprehensive test suite with edge case coverage in `btree/tree/tests.rs`
            *   Proper serialization/deserialization for persistent storage
            *   Integration with IndexManager via the Index trait
        *   **Blink Tree Implementation Notes**:
            *   Complete concurrent implementation in `src/core/indexing/blink_tree/`
            *   Lock-free concurrent reads during splits using right-link pointers
            *   High keys for safe concurrent traversal and split detection
            *   Minimal locking strategy optimized for write operations
            *   Range scan operations showcasing concurrent traversal capabilities
            *   21 comprehensive tests covering all operations and concurrent safety
            *   Production-ready for high-concurrency OLTP workloads
        *   **Hash Index Refactoring Notes**:
            *   Successfully moved from `hash_index.rs` to proper `src/core/indexing/hash/` submodule
            *   Clean module structure with proper exports and re-exports
            *   All tests continue to pass with no regressions (570 total tests passing)
        *   **R-Tree Foundation Notes**:
            *   Geometric types implemented: Point, Rectangle, MBR (Minimum Bounding Rectangle)
            *   Comprehensive spatial operations: area, perimeter, intersection, union, distance
            *   BoundingBox trait for spatial objects integration
            *   Error handling and module structure established
            *   Core R-Tree algorithms (insert, search, delete) in development

### Common Utilities (`src/core/common`)
*   Status: Substantially Implemented
*   Checklist:
    *   [x] `mod.rs` (Exists and defines module structure)
    *   [x] `types/` (Core data types like Value, PageId, TransactionId, Schema, Row, LSN alias defined in `types/` subdirectory with multiple files. Note potential `DataType` definition variance.)
    *   [x] `error.rs` (Centralized `OxidbError` enum implemented)
    *   [x] `serialization.rs` (Serialization helpers for `DataType` and `DataSerializer`/`DataDeserializer` traits for `Vec<u8>` implemented)
    *   [x] `traits.rs` (Commonly used traits like `DataSerializer`/`DataDeserializer` defined)

This ledger will be updated as work progresses on each component. "Required Components" are illustrative and will be refined in specific ADRs for each feature.

### Event Engine (`src/event_engine`)
*   Status: Initial Implementation
*   Checklist:
    *   [x] `mod.rs` (Module definition)
    *   [x] `README.md` (Sectional Blueprint for Event Engine)
    *   [x] `handler/mod.rs` (Handler submodule definition)
    *   [x] `handler/README.md` (Sectional Blueprint for Handler)
    *   [x] `handler/types.rs` (`Event` enum, `EventResult` type)
    *   [x] `handler/core.rs` (`process_event` function with flat logic)
    *   [x] `handler/processors.rs` (`Processor` trait and implementations)
    *   [x] `handler/tests.rs` (Unit tests for event handling)
    *   [ ] ADR for Event Engine design and `Processor` pattern (Recommended)

## Recent Updates - 2024-07-28

*   **SQL DELETE Implemented**:
    *   Added support for the SQL `DELETE` command, including parsing, planning, optimization (basic), and execution via a new `DeleteOperator`.
    *   Ensured correct physical WAL entries (`WalEntry::Delete` and `WalEntry::TransactionCommit` for auto-commits) are logged with proper LSNs.
*   **Test Suite Enhanced**:
    *   The previously ignored test `core::storage::engine::implementations::tests::simple_file_tests::test_physical_wal_lsn_integration` has been fixed, unignored, and is now passing. This test verifies LSN generation and physical WAL logging for INSERT, UPDATE, and DELETE operations.
    *   All `cargo test --all-features` now pass (previously 409, now 410 with the unignored test).
*   **Minor test adjustment**: Changed `BEGIN TRANSACTION` to `BEGIN` in `test_physical_wal_lsn_integration` to align with parser expectations for that specific test context.

## Recent Updates - 2025-06-19

*   **Test Suite:** All `cargo test` pass.
    *   Resolved an issue in `api::tests::db_tests::test_execute_query_str_update_ok`. The `FilterOperator` was enhanced to correctly resolve named columns (e.g., 'name') from `DataType::Map` during `WHERE` clause evaluation in `UPDATE` statements. This involved adjustments in `TableScanOperator` to emit key and row data separately, and in the `UPDATE` optimizer to ensure the key is projected through the internal selection plan.
*   **Code Cleanup:**
    *   Removed an outdated `TODO` comment from `src/core/query/executor/update_execution.rs` concerning `ExecutionResult::Updated` count, as this functionality was already implemented.
    *   Removed unnecessary `#[allow(dead_code)]` annotations for `Tuple` and `ExecutionOperator` in `src/core/execution/mod.rs` as these are actively used.
    *   Reviewed remaining `TODOs` (primarily in `src/core/optimizer/mod.rs`) and an unused `Row` struct; these were deemed acceptable to leave for future development.

## Recent Updates - 2025-06-20

*   **Test Suite**: All `cargo test --all-features` continue to pass (410 tests).
*   **Code Cleanup & TODOs**:
    *   Resolved several compiler warnings related to unused imports and unreachable code.
    *   Removed an unused local `struct Row` definition and its associated TODOs from `src/core/execution/mod.rs`.
    *   Removed an obsolete TODO comment from `src/core/query/sql/parser/statement.rs` as the described functionality (`ast::DeleteStatement`) was already in use.
    *   Updated a TODO comment in `src/core/query/executor/planner.rs` to clarify that dynamic primary key determination for DELETE operations is currently blocked by schema limitations (`ColumnDef` lacking an `is_primary_key` marker).
    *   Previously noted TODOs in `src/core/optimizer/mod.rs` (related to `#[allow(dead_code)]`) and `src/core/storage/engine/heap/table_page.rs` (design considerations for record management and advanced updates) remain deferred for future work.

## Recent Updates - 2025-01-XX (Indexing Implementations)

*   **Blink Tree Implementation Completed**:
    *   Full concurrent B+ tree variant implemented in `src/core/indexing/blink_tree/`
    *   Lock-free concurrent reads during splits using right-link pointers and high keys
    *   Minimal locking strategy for write operations with deferred maintenance
    *   Comprehensive implementation includes insert, delete, search, range scan, and tree verification
    *   21 new tests covering all operations, concurrent safety, and edge cases
    *   Complete integration with Index trait for seamless use in IndexManager
    *   Production-ready for high-concurrency OLTP workloads
*   **Hash Index Refactoring**:
    *   Successfully refactored hash index from single file to proper submodule structure
    *   Moved from `hash_index.rs` to `src/core/indexing/hash/` with proper module exports
    *   Updated IndexManager imports with no functionality changes
    *   All 549 existing tests continue to pass with no regressions
*   **R-Tree Foundation Implementation**:
    *   Comprehensive geometric foundation implemented in `src/core/indexing/rtree/geometry.rs`
    *   Point and Rectangle types with full spatial operations (area, intersection, union, distance)
    *   Minimum Bounding Rectangle (MBR) alias with spatial-specific methods
    *   BoundingBox trait for generic spatial object handling
    *   Module structure and error types established for future R-Tree algorithms
*   **Test Suite Status**:
    *   All 570 tests passing including 21 new Blink tree tests
    *   No regressions in existing functionality
    *   Comprehensive coverage for all indexing implementations
