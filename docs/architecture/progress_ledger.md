# Progress Ledger: The Cathedral's Construction

This ledger tracks the status of major features and components of the Oxidb cathedral.

## Core Architectural Pillars

| Feature             | Status        | Required Components (Illustrative) | Notes                                      |
|---------------------|---------------|------------------------------------|--------------------------------------------|
| **API Layer**       | Under Construction | `api/mod.rs`, `api/types.rs`, `api/errors.rs`, `api/traits.rs`, `api/api_impl.rs` | External interface for database interaction. |
| **Query Processor** | Under Construction | `core/query/parser/`, `core/query/executor/`, `core/query/optimizer/` | Parsing, validation, optimization, execution. |
| **Transaction Mgr.**| Partially Implemented | `core/transaction/manager.rs`      | ACID properties, concurrency.              |
| **Storage Engine**  | Under Construction | `core/storage/engine/`, `core/storage/wal/`, `core/storage/indexing/` | Physical data storage and retrieval.       |
| **Common Utilities**| Under Construction | `core/common/error.rs`, `core/common/types.rs`, `core/common/serialization.rs` | Shared types, errors, utils.             |

## Detailed Component Status

### API Layer (`src/api`)
*   Status: Under Construction
*   Checklist:
    *   [x] `mod.rs` (Public API definition, updated)
    *   [x] `types.rs` (API specific data types)
    *   [x] `errors.rs` (API specific error types, boilerplate)
    *   [ ] `traits.rs` (API specific traits, to be defined)
    *   [x] `api_impl.rs` (API implementation logic)
    *   [ ] ADR for API design choices (specific to API behavior, structure governed by ADR-000)

### Query Processor (`src/core/query`)
*   Status: Under Construction
*   Sub-Modules:
    *   **Parser** (`src/core/query/parser`)
        *   Status: Partially Implemented
        *   Checklist:
            *   [x] `mod.rs`
            *   [x] `sql/tokenizer.rs` (Note: Tokenizer is within `src/core/query/sql/`)
            *   [x] `sql/ast.rs` (Note: AST definitions are within `src/core/query/sql/`)
            *   [x] `sql/parser/ (core.rs, statement.rs, etc.)` (Note: Parser modules are within `src/core/query/sql/`)
            *   [ ] ADR for query language/parser design
    *   **Binder** (`src/core/query/binder`)
        *   Status: Not Started
        *   Checklist:
            *   [ ] `mod.rs` (exists as .gitkeep)
            *   [ ] `binder.rs` (not present)
    *   **Planner** (`src/core/query/planner`)
        *   Status: Not Started
        *   Checklist:
            *   [ ] `mod.rs` (exists as .gitkeep)
            *   [ ] `logical_planner.rs` (not present)
            *   [ ] `physical_planner.rs` (not present)
        *   Note: Initial planning logic present in `src/core/query/executor/planner.rs` and `optimizer.rs`.
    *   **Optimizer** (`src/core/optimizer`)
        *   Status: Partially Implemented
        *   Checklist:
            *   [x] `mod.rs` (Defines plan nodes and core structures)
            *   [x] `optimizer.rs` (Initial plan building and basic optimization passes implemented)
            *   [ ] `rules/` (Directory exists, no rules implemented)
            *   [ ] ADR for optimization strategies
    *   **Executor** (`src/core/query/executor/`)
        *   Status: Partially Implemented
        *   Checklist:
            *   [x] `mod.rs`
            *   [?] `operator logic` (Operator logic integrated within executor and plan nodes, no separate operators/ dir found)
            *   [ ] ADR for execution model

### Transaction Manager (`src/core/transaction`)
*   Status: Partially Implemented
*   Checklist:
    *   [x] `mod.rs`
    *   [x] `transaction.rs` (Transaction struct and lifecycle implemented)
    *   [x] `manager.rs` (Transaction management implemented with WAL logging)
    *   [x] `lock_manager.rs` (Locking mechanisms implemented)
    *   [ ] `errors.rs` (File missing, transaction-specific errors to be defined or confirmed if covered by global error handling)
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
