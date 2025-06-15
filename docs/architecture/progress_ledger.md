# Progress Ledger: The Cathedral's Construction

This ledger tracks the status of major features and components of the Oxidb cathedral.

## Core Architectural Pillars

| Feature             | Status        | Required Components (Illustrative) | Notes                                      |
|---------------------|---------------|------------------------------------|--------------------------------------------|
| **API Layer**       | In Design     | `api/mod.rs`, `api/db.rs`          | External interface for database interaction. |
| **Query Processor** | In Design     | `core/query/parser/`, `core/query/executor/`, `core/query/optimizer/` | Parsing, validation, optimization, execution. |
| **Transaction Mgr.**| In Design     | `core/transaction/manager.rs`      | ACID properties, concurrency.              |
| **Storage Engine**  | Under Construction | `core/storage/engine/`, `core/storage/wal/`, `core/storage/indexing/` | Physical data storage and retrieval.       |
| **Common Utilities**| Under Construction | `core/common/error.rs`, `core/common/types.rs`, `core/common/serialization.rs` | Shared types, errors, utils.             |

## Detailed Component Status

### API Layer (`src/api`)
*   Status: In Design
*   Checklist:
    *   [ ] `mod.rs` (Public API definition)
    *   [ ] `db.rs` (Database interaction logic)
    *   [ ] `types.rs` (API specific data types, if any)
    *   [ ] `errors.rs` (API specific error types, if any)
    *   [ ] ADR for API design choices

### Query Processor (`src/core/query`)
*   Status: In Design
*   Sub-Modules:
    *   **Parser** (`src/core/query/parser`)
        *   Status: Partially Implemented
        *   Checklist:
            *   [ ] `mod.rs`
            *   [ ] `lexer.rs`
            *   [ ] `ast.rs`
            *   [ ] `parser.rs`
            *   [ ] ADR for query language/parser design
    *   **Binder** (`src/core/query/binder`)
        *   Status: Not Started
        *   Checklist:
            *   [ ] `mod.rs`
            *   [ ] `binder.rs`
    *   **Planner** (`src/core/query/planner`)
        *   Status: Not Started
        *   Checklist:
            *   [ ] `mod.rs`
            *   [ ] `logical_planner.rs`
            *   [ ] `physical_planner.rs`
    *   **Optimizer** (`src/core/optimizer`)
        *   Status: Not Started
        *   Checklist:
            *   [ ] `mod.rs`
            *   [ ] `optimizer.rs`
            *   [ ] `rules/`
            *   [ ] ADR for optimization strategies
    *   **Executor** (`src/core/execution`)
        *   Status: Partially Implemented
        *   Checklist:
            *   [ ] `mod.rs`
            *   [ ] `operators/` (various operator implementations)
            *   [ ] ADR for execution model

### Transaction Manager (`src/core/transaction`)
*   Status: Partially Implemented
*   Checklist:
    *   [ ] `mod.rs`
    *   [ ] `transaction.rs` (Transaction struct and lifecycle)
    *   [ ] `manager.rs` (Transaction management, concurrency control)
    *   [ ] `lock_manager.rs` (Locking mechanisms)
    *   [ ] `errors.rs`
    *   [ ] ADR for concurrency control strategy

### Storage Engine (`src/core/storage`)
*   Status: Under Construction
*   Sub-Modules:
    *   **Engine** (`src/core/storage/engine`)
        *   Status: Partially Implemented (SimpleFileKvStore exists)
        *   Checklist:
            *   [ ] `mod.rs`
            *   [ ] `traits.rs` (Core storage traits)
            *   [ ] `simple_file_kv_store.rs` (or other specific KV store)
            *   [ ] `page_manager.rs` (Future)
            *   [ ] `buffer_pool.rs` (Future)
            *   [x] ADR for storage formats and strategies (ADR-001)
    *   **Write-Ahead Log (WAL)** (`src/core/wal`)
        *   Status: Partially Implemented
        *   Checklist:
            *   [ ] `mod.rs`
            *   [ ] `log_manager.rs`
            *   [ ] `log_record.rs`
            *   [ ] `writer.rs`
            *   [ ] ADR for WAL implementation
    *   **Indexing** (`src/core/indexing`)
        *   Status: Partially Implemented
        *   Checklist:
            *   [ ] `mod.rs`
            *   [ ] `traits.rs` (Indexing traits)
            *   [ ] `hash_index.rs` (Example index)
            *   [ ] `btree/` (Future B-Tree implementation)
            *   [ ] ADR for indexing strategies

### Common Utilities (`src/core/common`)
*   Status: Under Construction
*   Checklist:
    *   [ ] `mod.rs`
    *   [ ] `types.rs` (Core data types, newtypes)
    *   [ ] `error.rs` (Centralized DbError enum)
    *   [ ] `serialization.rs` (Serialization/Deserialization traits and impls)
    *   [ ] `traits.rs` (Commonly used traits)

This ledger will be updated as work progresses on each component. "Required Components" are illustrative and will be refined in specific ADRs for each feature.
