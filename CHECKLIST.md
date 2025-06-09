# Checklist for Creating a Pure Rust SQLite Alternative

This checklist outlines the tasks required to create a pure Rust, minimal dependency SQLite alternative, emphasizing elite programming practices and a deep vertical file tree.

## Phase 1: Project Setup and Initial Review

1.  **Review and Resolve Existing Issues:**
    *   [x] Identify any existing build errors.
        *   [x] Subtask: Run `cargo build` and document all errors.
        *   [x] Subtask: Analyze the root cause of each build error. (No build errors found)
    *   [x] Resolve all identified build errors.
        *   [x] Subtask: Implement fixes for each build error. (No build errors to fix)
        *   [x] Subtask: Verify fixes by running `cargo build` successfully. (Build was already successful)
    *   [x] Identify any existing test failures.
        *   [x] Subtask: Run `cargo test` and document all failing tests.
        *   [x] Subtask: Analyze the root cause of each test failure. (No test failures found)
    *   [x] Resolve all identified test failures.
        *   [x] Subtask: Implement fixes for each failing test. (No test failures to fix)
        *   [x] Subtask: Verify fixes by running `cargo test` successfully. (Tests were already successful)
    *   [x] **Validation:** Ensure `cargo build` and `cargo test` pass without errors.

2.  **[x] Establish Project Structure and Conventions:**
    *   [x] Define a deep vertical file tree structure.
        *   [x] Subtask: Design the directory layout (e.g., `src/core/storage/engine/b_tree/`).
        *   [x] Subtask: Document the rationale for the chosen structure.
    *   [x] Setup version control (Git) with appropriate `.gitignore`.
        *   [x] Subtask: Initialize Git repository if not already present. (Already initialized)
        *   [x] Subtask: Create a comprehensive `.gitignore` file for Rust projects. (Already present and seems comprehensive)
    *   [x] Configure linting tools (e.g., Clippy) and code formatter (e.g., `rustfmt`).
        *   [x] Subtask: Add Clippy and `rustfmt` to project configuration. (rustfmt is configured via rustfmt.toml; Clippy is available by default with Rust)
        *   [x] Subtask: Define project-specific linting rules. (Activated core clippy lints like unwrap_used, expect_used, etc., via src/lib.rs attributes)
    *   [x] Set up a Continuous Integration (CI) pipeline (e.g., GitHub Actions). (Workflow exists in .github/workflows/rust.yml)
        *   [x] Subtask: Create a basic CI workflow that runs `cargo build` and `cargo test` on every push. (Workflow includes build, test, fmt, and clippy checks)
    *   [x] **Validation:** CI pipeline passes; code formatting and linting tools are functional.

3.  **[x] Define Core Data Structures and Types:**
    *   [x] Define basic data types (e.g., `Value`, `DataType`, `Row`, `Schema`).
        *   [x] Subtask: Implement serializable and comparable data types.
        *   [x] Subtask: Write unit tests for data type conversions and comparisons. (Existing tests cover serialization, get_type, basic comparisons; advanced conversions not yet implemented)
    *   [x] Define error handling mechanisms (e.g., custom `Error` enums).
        *   [x] Subtask: Implement a comprehensive error type for the database.
        *   [x] Subtask: Write unit tests for error propagation and handling. (Comprehensive OxidbError enum exists and is used/matched in some tests; further specific error path tests can be added iteratively)
    *   [x] **Validation:** All core data types are implemented and thoroughly tested. (Added serialization tests for ID types and comprehensive PartialOrd tests for Value type; fixed PartialOrd bug for Value.)

## Phase 2: Storage Engine

4.  **Implement the Page Management System:**
    *   [ ] Design page layout and structure (e.g., header, data area).
        *   [x] Subtask: Define structs for page headers and page data. (Created src/core/storage/engine/page.rs with Page, PageHeader, PageType structs and PAGE_SIZE constant. Page.data uses Vec<u8> for now due to serde large array limitations.)
    *   [x] Implement page serialization and deserialization.
        *   [x] Subtask: Write functions to read/write pages from/to disk. (Covered by Page ser/de and DiskManager)
        *   [x] Subtask: Write unit tests for page serialization/deserialization.
    *   [x] Implement a buffer pool manager.
        *   [x] Subtask: Design the buffer pool with a page replacement policy (e.g., LRU). (FIFO implemented)
        *   [x] Subtask: Implement methods for fetching, pinning, and unpinning pages.
        *   [x] Subtask: Write unit tests for buffer pool operations.
    *   [x] **Validation:** Pages can be reliably read from and written to disk via DiskManager; page serialization/deserialization is functional and tested. Buffer pool manages pages with a FIFO policy, and its core operations (fetch, unpin, new, flush) are implemented and tested.

5.  **Implement the Table Heap/File Manager:**
    *   [ ] Design how tables and records are stored on pages.
        *   [ ] Subtask: Define structures for table metadata and record layout.
    *   [ ] Implement record-level operations (insert, delete, update, get).
        *   [ ] Subtask: Write functions for managing records within pages.
        *   [ ] Subtask: Implement handling for variable-length records and overflow pages if necessary.
        *   [ ] Subtask: Write unit tests for all record operations.
    *   [ ] **Validation:** Records can be inserted, deleted, updated, and retrieved correctly.

6.  **Implement a Write-Ahead Log (WAL) for Durability:**
    *   [ ] Design WAL record format.
    *   [ ] Implement WAL writer and manager.
        *   [ ] Subtask: Write functions to append log records to the WAL.
        *   [ ] Subtask: Implement log flushing mechanisms.
    *   [ ] Implement recovery mechanisms using WAL (e.g., ARIES).
        *   [ ] Subtask: Design the analysis, redo, and undo phases of recovery.
        *   [ ] Subtask: Write unit tests for recovery scenarios (e.g., crash before commit, crash during commit).
    *   [ ] **Validation:** Database can recover to a consistent state after a crash.

## Phase 3: Query Processing

7.  **Implement SQL Parser:**
    *   [ ] Define supported SQL grammar (subset of SQLite).
        *   [ ] Subtask: Specify supported DDL (CREATE TABLE, DROP TABLE) and DML (SELECT, INSERT, UPDATE, DELETE) statements.
    *   [ ] Choose or implement a parser library (e.g., `sqlparser-rs` or custom).
        *   [ ] Subtask: If using a library, integrate it into the project.
        *   [ ] Subtask: If custom, implement lexer and parser.
    *   [ ] Convert SQL strings into an Abstract Syntax Tree (AST).
        *   [ ] Subtask: Define AST node types.
        *   [ ] Subtask: Write unit tests for parsing various SQL queries.
    *   [ ] **Validation:** SQL queries are correctly parsed into ASTs.

8.  **Implement Query Planner and Optimizer:**
    *   [ ] Convert AST to a logical query plan.
        *   [ ] Subtask: Define logical plan operators (e.g., Scan, Filter, Join, Project).
        *   [ ] Subtask: Write unit tests for AST to logical plan conversion.
    *   [ ] Implement basic query optimization rules (e.g., predicate pushdown, constant folding).
        *   [ ] Subtask: Implement transformation rules for the logical plan.
        *   [ ] Subtask: Write unit tests to verify optimization rule correctness.
    *   [ ] Convert logical query plan to a physical query plan.
        *   [ ] Subtask: Define physical plan operators (e.g., TableScan, IndexScan, HashJoin, NestedLoopJoin).
        *   [ ] Subtask: Write unit tests for logical to physical plan conversion.
    *   [ ] **Validation:** Optimized physical query plans are generated.

9.  **Implement Query Execution Engine (Volcano/Iterator Model):**
    *   [ ] Implement executor for each physical plan operator.
        *   [ ] Subtask: Each operator should implement a `next()` method returning tuples.
        *   [ ] Subtask: Write unit tests for each individual operator.
    *   [ ] Implement query execution context.
        *   [ ] Subtask: Manage transaction context and other execution-time state.
    *   [ ] **Validation:** Queries are executed correctly, and results match expectations. Test with various SELECT, INSERT, UPDATE, DELETE statements.

## Phase 4: Concurrency and Indexing

10. **Implement Transaction Management:**
    *   [ ] Implement transaction begin, commit, and abort.
        *   [ ] Subtask: Define transaction states and transitions.
    *   [ ] Implement concurrency control mechanisms (e.g., Two-Phase Locking - 2PL, MVCC).
        *   [ ] Subtask: Design lock manager or versioning system.
        *   [ ] Subtask: Write unit tests for concurrent transaction scenarios (e.g., deadlocks, serializability).
    *   [ ] Implement isolation levels (e.g., Read Committed, Serializable).
        *   [ ] Subtask: Ensure transaction operations respect the chosen isolation levels.
        *   [ ] Subtask: Write unit tests to verify isolation level guarantees.
    *   [ ] **Validation:** Transactions are ACID compliant.

11. **Implement Indexing Structures:**
    *   [ ] Implement B+ Tree index.
        *   [ ] Subtask: Design B+ Tree node structure and operations (insert, delete, search).
        *   [ ] Subtask: Implement serialization/deserialization for B+ Tree nodes.
        *   [ ] Subtask: Write extensive unit tests for B+ Tree operations, including edge cases and concurrent access if applicable.
    *   [ ] (Optional) Implement other index types (e.g., Hash Index, GiST).
        *   [ ] Subtask: Design and implement the chosen index structure.
        *   [ ] Subtask: Write unit tests for the new index type.
    *   [ ] Integrate indexing with the query executor (IndexScan operator).
        *   [ ] Subtask: Modify the query optimizer to consider using indexes.
        *   [ ] Subtask: Implement the IndexScan physical operator.
        *   [ ] Subtask: Write integration tests for queries using indexes.
    *   [ ] **Validation:** Indexes speed up query performance; data retrieval via indexes is correct.

## Phase 5: API and Finalization

12. **Define and Implement a Client API:**
    *   [ ] Design a minimal, ergonomic Rust API for database operations.
        *   [ ] Subtask: Define functions for connecting, executing queries, and retrieving results.
    *   [ ] (Optional) Implement a C API for broader compatibility.
    *   [ ] (Optional) Implement a network protocol for remote access (e.g., based on PostgreSQL wire protocol or custom).
    *   [ ] Write API usage examples and documentation.
    *   [ ] **Validation:** API is easy to use and allows for all core database functionalities.

13. **Documentation and Benchmarking:**
    *   [ ] Write comprehensive internal and external documentation.
        *   [ ] Subtask: Document code modules, functions, and complex logic.
        *   [ ] Subtask: Create user guides and tutorials.
    *   [ ] Develop a benchmarking suite.
        *   [ ] Subtask: Create benchmarks for common operations (e.g., inserts, selects, updates, joins).
        *   [ ] Subtask: Compare performance against other embedded databases (e.g., SQLite, RocksDB).
    *   [ ] **Validation:** Documentation is clear and complete; performance benchmarks are established.

14. **Dependency Minimization and Code Polish:**
    *   [ ] Review all external dependencies.
        *   [ ] Subtask: Identify and remove unnecessary dependencies.
        *   [ ] Subtask: Consider replacing large dependencies with smaller, more focused ones or custom implementations if feasible and aligned with "elite practices".
    *   [ ] Perform a final code review for adherence to "elite programming practices".
        *   [ ] Subtask: Check for code clarity, efficiency, error handling, and idiomatic Rust.
        *   [ ] Subtask: Ensure all `unsafe` blocks are justified and minimized.
    *   [ ] Ensure all tests pass, and code coverage is high.
    *   [ ] **Validation:** Dependencies are minimal; code quality is high; test coverage is satisfactory.

This checklist provides a high-level overview. Each task will require further breakdown and detailed design.
