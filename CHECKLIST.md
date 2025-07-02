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
    *   [x] Design how tables and records are stored on pages.
        *   [x] Subtask: Define structures for table metadata and record layout. (Implemented SlotId, Slot, RecordIdentifier, TablePageHeader, TablePage with tests for layout, serialization, and basic calculations. Location: `src/core/storage/engine/heap/table_page.rs` and `src/core/common/types/ids.rs` for SlotId)
    *   [x] Implement record-level operations (insert, delete, update, get).
        *   [x] Subtask: Implement `insert_record` to add data to the page, managing slot metadata and free space.
        *   [x] Subtask: Implement `get_record` to retrieve data from a specified slot.
        *   [x] Subtask: Implement `delete_record` to mark a slot as empty (no compaction initially).
        *   [x] Subtask: Implement `update_record` to modify data in a slot (erroring if new data is larger and doesn't fit).
        *   [x] Subtask: Initial implementation in `src/core/storage/engine/heap/table_page.rs`.
        *   [x] Subtask: Write comprehensive unit tests for all record operations, including edge cases and variable-length records.
            *   [x] Subtask: Created test suite in `src/core/storage/engine/heap/tests/table_page_tests.rs`.
            *   [x] Subtask: Tests cover `insert_record` (basic, full page, slot reuse), `get_record` (basic, non-existent), `delete_record` (basic, already deleted), `update_record` (same/smaller size, larger size error), and empty record insertion.
            *   [x] Subtask: All project tests (306) pass.
    *   [x] **Validation:** Records can be inserted, deleted, updated, and retrieved correctly.

6.  **Implement a Write-Ahead Log (WAL) for Durability:**
    *   [x] Design WAL record format.
        *   [x] Subtask: Define different types of log records (e.g., BEGIN, COMMIT, ABORT, INSERT, UPDATE, DELETE).
        *   [x] Subtask: Specify the content of each log record type (e.g., transaction ID, page ID, offset, old data, new data).
    *   [ ] Implement WAL writer and manager.
        *   [ ] Subtask: Write functions to append log records to the WAL.
            *   [x] Sub-subtask: Implement serialization for each WAL record type. (Completed as part of record format definition)
            *   [x] Sub-subtask: Implement buffering for log records before writing to disk. (Implemented `WalWriter` with an in-memory `Vec<LogRecord>` buffer and an `add_record` method.)
        *   [x] Subtask: Implement log flushing mechanisms.
            *   [x] Sub-subtask: Implemented a `flush` method in `WalWriter` that serializes records, writes them to a file with length prefixes, and uses `sync_all()` for durability.
            *   [x] Sub-subtask: Implement `fsync` or similar calls to ensure durability. (Achieved using `File::sync_all()` in `WalWriter::flush`)
            *   [x] Sub-subtask: Design policies for when to flush (e.g., on commit, periodically).
                *   [x] Sub-sub-subtask: Implemented "flush on commit" policy in `WalWriter` (flushes when `LogRecord::CommitTransaction` is added).
                *   [x] Sub-sub-subtask: Integrated `WalWriter` with `TransactionManager` to log and flush a `LogRecord::CommitTransaction` upon successful transaction commit.
                *   [ ] Sub-sub-subtask: Design and implement periodic flush policy (if deemed necessary).
                *   [ ] Sub-sub-subtask: Design and implement other flush policies (e.g., buffer full).
    *   [x] Implement Log Sequence Number (LSN) generation and management. <!-- COMPLETED -->
        *   [x] Subtask: Design a `LogManager` component responsible for LSN generation.
            *   [x] Sub-subtask: Located in `src/core/wal/log_manager.rs`.
            *   [x] Sub-subtask: Manages an in-memory atomic counter for LSNs, starting from 0.
        *   [x] Subtask: `LogManager` to assign a unique, increasing LSN to each log record.
            *   [x] Sub-subtask: Modified `LogRecord` (logical WAL) struct to include an `lsn` field.
            *   [x] Sub-subtask: Modified `WalEntry` (physical WAL) struct to include an `lsn` field.
        *   [x] Subtask: `LogManager` to update `Transaction::prev_lsn` with the LSN of the latest record for that transaction.
            *   [x] Sub-subtask: The `Transaction` struct's `prev_lsn` field stores this LSN.
        *   [x] Subtask: Operations generating log records (logical `LogRecord` for Begin/Commit/Abort via `TransactionManager`, and physical `WalEntry` for DML via `QueryExecutor` -> `Store`) now use `LogManager` to obtain LSNs.
        *   [x] Subtask: `LogManager` provides LSNs; `WalWriter` (logical) and `engine::wal::WalWriter` (physical) handle writing.
        *   [x] Subtask: Write unit tests for LSN generation and `LogManager` functionality. (Includes basic LogManager tests, WalEntry (de)serialization with LSN, TransactionManager LSN awareness, prev_lsn updates, and physical WAL LSN integration tests).
    *   [ ] Implement recovery mechanisms using WAL (e.g., ARIES).
        *   [ ] Subtask: Design the analysis, redo, and undo phases of recovery.
            *   [ ] Sub-subtask: Analysis phase: Scan WAL to identify dirty pages and active transactions at the time of crash.
            *   [ ] Sub-subtask: Redo phase: Replay log records for committed transactions to ensure their changes are applied.
            *   [ ] Sub-subtask: Undo phase: Revert changes made by uncommitted (aborted) transactions.
        *   [ ] Subtask: Write unit tests for recovery scenarios (e.g., crash before commit, crash during commit).
            *   [ ] Sub-subtask: Test case: Recovery with no uncommitted transactions.
            *   [ ] Sub-subtask: Test case: Recovery with uncommitted transactions that need undo.
            *   [ ] Sub-subtask: Test case: Recovery with committed transactions that need redo.
            *   [ ] Sub-subtask: Test case: Recovery after multiple checkpoints.
    *   [ ] **Validation:** Database can recover to a consistent state after a crash.

## Phase 3: Query Processing

7.  **Implement SQL Parser:**
    *   [ ] Define supported SQL grammar (subset of SQLite).
        *   [ ] Subtask: Specify supported DDL (CREATE TABLE, DROP TABLE) and DML (SELECT, INSERT, UPDATE, DELETE) statements.
        *   [ ] Subtask: Document specific clauses and options for each supported DDL/DML statement (e.g., for SELECT: WHERE, LIMIT, ORDER BY; for CREATE TABLE: column types, constraints like NOT NULL, PRIMARY KEY).
    *   [ ] Choose or implement a parser library (e.g., `sqlparser-rs` or custom).
        *   [ ] Subtask: Evaluate pros and cons of `sqlparser-rs` vs. a custom recursive descent parser.
        *   [ ] Subtask: If using a library, integrate it into the project.
        *   [ ] Subtask: If custom, implement lexer and parser.
    *   [ ] Convert SQL strings into an Abstract Syntax Tree (AST).
        *   [ ] Subtask: Define AST node types.
            *   [ ] Sub-subtask: Define Rust enums/structs for statements (e.g., `Statement::CreateTable`, `Statement::Select`).
            *   [ ] Sub-subtask: Define Rust enums/structs for expressions, table names, column names, values, etc.
        *   [ ] Subtask: Write unit tests for parsing various SQL queries.
            *   [ ] Sub-subtask: Test CREATE TABLE with different column definitions and constraints.
            *   [ ] Sub-subtask: Test INSERT with various value combinations.
            *   [ ] Sub-subtask: Test SELECT with different clauses (WHERE, JOIN, GROUP BY, ORDER BY, LIMIT).
            *   [ ] Sub-subtask: Test UPDATE with WHERE clauses.
            *   [ ] Sub-subtask: Test DELETE with WHERE clauses.
            *   [ ] Sub-subtask: Test parsing invalid SQL syntax to ensure proper error reporting.
    *   [ ] **Validation:** SQL queries are correctly parsed into ASTs.

8.  **Implement Query Planner and Optimizer:**
    *   [ ] Convert AST to a logical query plan.
        *   [ ] Subtask: Define logical plan operators (e.g., Scan, Filter, Join, Project).
            *   [ ] Sub-subtask: Implement Rust structs/enums for each logical operator, storing relevant information (e.g., Filter operator stores the filter predicate).
        *   [ ] Subtask: Write unit tests for AST to logical plan conversion.
            *   [ ] Sub-subtask: Test conversion for simple SELECT queries.
            *   [ ] Sub-subtask: Test conversion for queries with WHERE clauses (AST predicate to Filter operator).
            *   [ ] Sub-subtask: Test conversion for queries with JOIN clauses.
    *   [ ] Implement basic query optimization rules (e.g., predicate pushdown, constant folding).
        *   [ ] Subtask: Implement transformation rules for the logical plan.
            *   [ ] Sub-subtask: Implement a rule for pushing Filter operators closer to Scan operators.
            *   [ ] Sub-subtask: Implement a rule for evaluating constant expressions (e.g., `1+2` becomes `3`).
        *   [ ] Subtask: Write unit tests to verify optimization rule correctness.
            *   [ ] Sub-subtask: Test predicate pushdown: ensure filter is applied correctly after pushdown.
            *   [ ] Sub-subtask: Test constant folding: ensure expressions are correctly simplified.
    *   [ ] Convert logical query plan to a physical query plan.
        *   [ ] Subtask: Define physical plan operators (e.g., TableScan, IndexScan, HashJoin, NestedLoopJoin).
            *   [ ] Sub-subtask: Implement Rust structs/enums for physical operators, detailing how they will be executed.
        *   [ ] Subtask: Write unit tests for logical to physical plan conversion.
            *   [ ] Sub-subtask: Test conversion of LogicalScan to PhysicalTableScan.
            *   [ ] Sub-subtask: Test conversion of LogicalFilter to PhysicalFilter.
            *   [ ] Sub-subtask: Test selection of appropriate join algorithms (e.g., HashJoin vs. NestedLoopJoin based on heuristics or statistics if available).
    *   [ ] **Validation:** Optimized physical query plans are generated.

9.  **Implement Query Execution Engine (Volcano/Iterator Model):**
    *   [ ] Implement executor for each physical plan operator.
        *   [ ] Subtask: Each operator should implement a `next()` method returning tuples.
            *   [ ] Sub-subtask: Implement `TableScanExecutor` to read rows from a table.
            *   [ ] Sub-subtask: Implement `FilterExecutor` to apply predicates.
            *   [ ] Sub-subtask: Implement `ProjectionExecutor` to select specific columns.
            *   [ ] Sub-subtask: Implement `LimitExecutor` to restrict the number of output rows.
            *   [ ] Sub-subtask: Implement `InsertExecutor` to insert rows into a table.
            *   [ ] Sub-subtask: Implement `UpdateExecutor` to modify existing rows.
            *   [ ] Sub-subtask: Implement `DeleteExecutor` to remove rows.
        *   [ ] Subtask: Write unit tests for each individual operator.
            *   [ ] Sub-subtask: Test `TableScanExecutor` by reading all rows from a known table.
            *   [ ] Sub-subtask: Test `FilterExecutor` with various predicates.
            *   [ ] Sub-subtask: Test `ProjectionExecutor` with different column selections.
    *   [ ] Implement query execution context.
        *   [ ] Subtask: Manage transaction context and other execution-time state.
            *   [ ] Sub-subtask: Design struct for `ExecutionContext` holding transaction ID, buffer pool manager instance, catalog access, etc.
    *   [ ] **Validation:** Queries are executed correctly, and results match expectations. Test with various SELECT, INSERT, UPDATE, DELETE statements.

## Phase 4: Concurrency and Indexing

10. **Implement Transaction Management:**
    *   [ ] Implement transaction begin, commit, and abort.
        *   [ ] Subtask: Define transaction states and transitions. (e.g., ACTIVE, COMMITTED, ABORTED).
        *   [ ] Subtask: Implement `BEGIN TRANSACTION`, `COMMIT`, `ROLLBACK` commands.
    *   [ ] Implement concurrency control mechanisms (e.g., Two-Phase Locking - 2PL, MVCC).
        *   [ ] Subtask: Design lock manager or versioning system.
            *   [ ] Sub-subtask: If 2PL: Design lock table, lock modes (shared, exclusive), deadlock detection/prevention.
            *   [ ] Sub-subtask: If MVCC: Design version storage, read/write protocols for different transaction timestamps/IDs.
        *   [ ] Subtask: Write unit tests for concurrent transaction scenarios (e.g., deadlocks, serializability).
            *   [ ] Sub-subtask: Test concurrent reads of the same data.
            *   [ ] Sub-subtask: Test concurrent writes to different data.
            *   [ ] Sub-subtask: Test concurrent write to the same data (should block or error depending on isolation).
            *   [ ] Sub-subtask: Test for deadlock detection and resolution (e.g., one transaction aborted).
    *   [ ] Implement isolation levels (e.g., Read Committed, Serializable).
        *   [ ] Subtask: Ensure transaction operations respect the chosen isolation levels.
            *   [ ] Sub-subtask: Modify read/write operations to acquire locks or read appropriate versions according to the current transaction's isolation level.
        *   [ ] Subtask: Write unit tests to verify isolation level guarantees.
            *   [ ] Sub-subtask: Test for dirty reads (should not occur in Read Committed).
            *   [ ] Sub-subtask: Test for non-repeatable reads (may occur in Read Committed, should not in Serializable).
            *   [ ] Sub-subtask: Test for phantom reads (may occur in Read Committed, should not in Serializable).
    *   [ ] **Validation:** Transactions are ACID compliant.

11. **Implement Indexing Structures:**
    *   [ ] Implement B+ Tree index.
        *   [ ] Subtask: Design B+ Tree node structure and operations (insert, delete, search).
            *   [ ] Sub-subtask: Define internal node and leaf node structures.
            *   [ ] Sub-subtask: Implement algorithms for key insertion, deletion, and point/range searches.
            *   [ ] Sub-subtask: Implement node splitting and merging logic.
        *   [ ] Subtask: Implement serialization/deserialization for B+ Tree nodes. (To store them on pages).
        *   [ ] Subtask: Write extensive unit tests for B+ Tree operations, including edge cases and concurrent access if applicable.
            *   [ ] Sub-subtask: Test insert into empty tree.
            *   [ ] Sub-subtask: Test insert causing leaf node split.
            *   [ ] Sub-subtask: Test insert causing internal node split.
            *   [ ] Sub-subtask: Test delete causing leaf node merge.
            *   [ ] Sub-subtask: Test delete causing internal node merge.
            *   [ ] Sub-subtask: Test search for existing and non-existing keys.
            *   [ ] Sub-subtask: Test range scans.
    *   [ ] (Optional) Implement other index types (e.g., Hash Index, GiST).
        *   [ ] Subtask: Design and implement the chosen index structure.
        *   [ ] Subtask: Write unit tests for the new index type.
    *   [ ] Integrate indexing with the query executor (IndexScan operator).
        *   [ ] Subtask: Modify the query optimizer to consider using indexes.
            *   [ ] Sub-subtask: Add logic to identify query predicates that can be satisfied by an index.
            *   [ ] Sub-subtask: Estimate costs for table scan vs. index scan.
        *   [ ] Subtask: Implement the IndexScan physical operator.
            *   [ ] Sub-subtask: Use B+ Tree search/scan methods to retrieve row identifiers.
            *   [ ] Sub-subtask: Fetch actual rows using the retrieved row identifiers.
        *   [ ] Subtask: Write integration tests for queries using indexes.
            *   [ ] Sub-subtask: Test SELECT queries with WHERE clauses on indexed columns.
            *   [ ] Sub-subtask: Compare performance with and without indexes (manually or via EXPLAIN).
    *   [ ] **Validation:** Indexes speed up query performance; data retrieval via indexes is correct.

## Phase 4.5: Advanced Features (includes RAG)

11a. **Implement Vector Data Type and Storage:**
    *   [x] Define a new `Vector` data type in `src/core/common/types/value.rs` and `src/core/common/types/data_type.rs`.
        *   [x] Subtask: The `Vector` type should store a list of floating-point numbers.
        *   [x] Subtask: Implement serialization and deserialization for the `Vector` type (via Serde).
        *   [x] Subtask: Write unit tests for `Vector` type operations (covered by `Value` and `DataType` tests).
    *   [x] Adapt storage engine to handle `Vector` data (physical storage in DB tables - VERIFIED VIA SERIALIZATION).
        *   [x] Subtask: Ensure `TablePage` and record operations can store and retrieve `Vector` types (verified by testing storage of serialized `Row` containing `Value::Vector`).
        *   [x] Subtask: Write tests for storing and retrieving records with `Vector` data (test `test_insert_and_get_row_with_vector` in `table_page_tests.rs` added).
    *   [x] **Validation:** `Vector` data type is correctly implemented at the type system level. Physical storage and retrieval of serialized vectors in DB pages is now VERIFIED.

11b. **Implement Vector Similarity Search (Core Logic):**
    *   [x] Design and implement functions for basic vector similarity calculations (e.g., cosine similarity, dot product) in a new module `src/core/vector/similarity.rs`.
        *   [x] Subtask: Implement cosine similarity function.
        *   [x] Subtask: Implement dot product function.
        *   [x] Subtask: Write unit tests for similarity functions.
    *   [x] **Validation:** Vector similarity functions are correct.

11c. **Implement RAG Framework Core:**
    *   [x] Create a new module `src/core/rag/mod.rs`.
    *   [x] Design core RAG pipeline components (e.g., `Document`, `Embedding` structs, `EmbeddingModel` trait, `Retriever` trait).
        *   [x] Subtask: Define traits for embedders (to allow different embedding models).
        *   [x] Subtask: Implement a basic retriever that uses vector similarity search (`InMemoryRetriever`).
    *   [x] Write unit tests for core RAG components.
    *   [x] **Validation:** Core RAG components are functional and tested.

## Phase 5: API and Finalization

12. **Define and Implement a Client API:**
    *   [ ] Design a minimal, ergonomic Rust API for database operations.
        *   [ ] Subtask: Define functions for connecting, executing queries, and retrieving results.
            *   [ ] Sub-subtask: `Connection::open(path)`
            *   [ ] Sub-subtask: `Connection::execute(sql_string)` -> `Result<QueryResult>` or `Result<RowsAffected>`
            *   [ ] Sub-subtask: `QueryResult::new(schema, rows_iterator)`
            *   [ ] Sub-subtask: `Row::get_value(column_index_or_name)`
        *   [ ] Subtask: Implement error handling within the API.
    *   [ ] (Optional) Implement a C API for broader compatibility.
    *   [ ] (Optional) Implement a network protocol for remote access (e.g., based on PostgreSQL wire protocol or custom).
    *   [ ] Write API usage examples and documentation.
        *   [ ] Subtask: Create example programs demonstrating connecting, creating tables, inserting data, and querying data.
    *   [ ] **Validation:** API is easy to use and allows for all core database functionalities.

13. **Documentation and Benchmarking:**
    *   [ ] Write comprehensive internal and external documentation.
        *   [ ] Subtask: Document code modules, functions, and complex logic. (Using `rustdoc` comments).
        *   [ ] Subtask: Create user guides and tutorials. (Markdown files in a `/docs` directory or similar).
    *   [ ] Develop a benchmarking suite.
        *   [ ] Subtask: Create benchmarks for common operations (e.g., inserts, selects, updates, joins). (Using `criterion.rs` or similar).
        *   [ ] Subtask: Compare performance against other embedded databases (e.g., SQLite, RocksDB).
            *   [ ] Sub-subtask: Define standard benchmark scenarios (e.g., TPC-C like, if applicable at small scale, or custom workloads).
    *   [ ] **Validation:** Documentation is clear and complete; performance benchmarks are established.

14. **Dependency Minimization and Code Polish:**
    *   [ ] Review all external dependencies.
        *   [ ] Subtask: Identify and remove unnecessary dependencies.
        *   [ ] Subtask: Consider replacing large dependencies with smaller, more focused ones or custom implementations if feasible and aligned with "elite practices".
            *   [ ] Sub-subtask: For each dependency: document its purpose, evaluate if it's essential, explore alternatives.
    *   [ ] Perform a final code review for adherence to "elite programming practices".
        *   [ ] Subtask: Check for code clarity, efficiency, error handling, and idiomatic Rust.
        *   [ ] Subtask: Ensure all `unsafe` blocks are justified and minimized. (Add `SAFETY:` comments explaining why `unsafe` is necessary and correct).
    *   [ ] Ensure all tests pass, and code coverage is high.
        *   [ ] Subtask: Set up code coverage tooling (e.g., `tarpaulin` or `grcov`).
        *   [ ] Subtask: Identify and write tests for uncovered code paths.
    *   [ ] **Validation:** Dependencies are minimal; code quality is high; test coverage is satisfactory.

This checklist provides a high-level overview. Each task will require further breakdown and detailed design.
