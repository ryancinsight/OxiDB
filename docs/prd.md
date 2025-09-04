# Product Requirements Document - OxiDB High-Performance Database Engine

## Project Vision
Develop a production-ready, high-performance in-memory database engine with advanced indexing, vector similarity search, and SQL compatibility, targeting enterprise applications requiring sub-millisecond query latency and ACID compliance.

## Current Development State
**Phase:** Mid-development - Core database functionality implemented with comprehensive testing
**Status:** 736 tests passing, ACID compliance achieved, multiple indexing strategies operational

## Architecture Requirements

### **Core Database Engine** (`src/core/`)
- **Storage Layer** (`src/core/storage/`): ACID-compliant storage with WAL and MVCC
- **Query Processor** (`src/core/query/`): SQL parser with optimization framework  
- **Indexing Engine** (`src/core/indexing/`): B+Tree, Hash, HNSW for vector similarity
- **Transaction Manager** (`src/core/transaction/`): Multi-version concurrency control
- **Recovery System** (`src/core/recovery/`): WAL-based crash recovery

### **Performance Targets**
- **Query Latency:** <1ms for indexed lookups
- **Throughput:** >10,000 transactions/second on commodity hardware
- **Memory Efficiency:** <100MB for 1M record operations
- **Concurrency:** Support 1,000 concurrent connections
- **Test Coverage:** >95% with property-based testing

### **Advanced Features**
- **Vector Operations:** Native support for RAG applications with SIMD optimization
- **GPU Acceleration:** WGPU-based compute shaders for parallel operations
- **Zero-Copy Operations:** Slice-based data access and Cow for efficient memory usage
- **Generic Programming:** `<T: DatabaseValue>` traits for type safety
- **Async Operations:** Tokio-based async I/O with zero-copy buffer mappings

## Technical Specifications

### **Data Structures** (`src/core/common/`)
```rust
// Design-by-contract traits with documented invariants
pub trait DatabaseValue: Clone + PartialEq + Send + Sync {
    /// Invariant: serialized size must be deterministic
    fn serialize(&self) -> Result<Vec<u8>, SerializationError>;
    /// Invariant: deserialization must be inverse of serialization  
    fn deserialize(data: &[u8]) -> Result<Self, SerializationError>;
}
```

### **Storage Engine** (`src/core/storage/`)
```rust
// Zero-copy buffer management
pub struct BufferPool<T: DatabaseValue> {
    // Use memory-mapped files for large datasets
    pages: Vec<MemoryMappedPage<T>>,
    // LRU replacement with lock-free operations
    replacement_policy: LockFreeLRU,
}
```

### **Query Optimization** (`src/core/optimizer/`)
```rust
// SOLID: Single responsibility for each optimization rule
pub trait OptimizationRule<T: QueryNode> {
    /// Invariant: transformation must preserve query semantics
    fn apply(&self, query: T) -> Result<T, OptimizationError>;
    /// Invariant: cost must decrease or remain same
    fn cost_improvement(&self, before: &T, after: &T) -> f64;
}
```

### **Vector Operations** (`src/core/vector/`)
```rust
// SIMD-optimized vector operations for RAG
pub fn similarity_search<T: Float>(
    query: &[T],
    database: &VectorIndex<T>,
    k: usize,
) -> Result<Vec<SimilarityResult<T>>, VectorError> {
    // Use std::arch for platform-specific SIMD
    #[cfg(target_arch = "x86_64")]
    similarity_search_avx2(query, database, k)
    
    #[cfg(not(target_arch = "x86_64"))]
    similarity_search_generic(query, database, k)
}
```

## Module Organization Standards

### **File Size Constraints**
- **Maximum:** 300 lines per module for readability
- **Enforcement:** Automated checks via `xtask/src/main.rs`
- **Violations:** Automatic module splitting when threshold exceeded

### **Naming Conventions**
- **Neutral Naming:** `BTreeIndex` vs `BTreeIndexRefactored`
- **Generic Types:** `DatabaseValue` vs `T` for public APIs  
- **Constants:** `const BUFFER_SIZE: usize = 4096;` in `src/core/constants.rs`

### **Module Structure** (Following Rust Book Ch. 7)
```
src/core/
├── storage/
│   ├── mod.rs           # Traits with invariants
│   ├── buffer_pool.rs   # Memory management <300 lines
│   ├── disk_manager.rs  # Disk I/O operations <300 lines
│   └── page.rs          # Page structure <300 lines
├── indexing/
│   ├── mod.rs           # Index trait definitions
│   ├── btree.rs         # B+ tree implementation <300 lines
│   ├── hash.rs          # Hash index <300 lines
│   └── vector.rs        # Vector similarity index <300 lines
└── query/
    ├── mod.rs           # Query processing traits
    ├── parser.rs        # SQL parsing <300 lines
    ├── optimizer.rs     # Query optimization <300 lines
    └── executor.rs      # Query execution <300 lines
```

## Performance Engineering

### **Benchmarking Infrastructure** (`benches/`)
```rust
// Continuous micro-benchmarks with criterion
fn benchmark_index_lookup(c: &mut Criterion) {
    c.bench_function("btree_lookup", |b| {
        b.iter(|| {
            // Target: <1ms for indexed lookup
            let result = index.find(black_box(&key));
            assert!(result.is_ok());
        });
    });
}
```

### **Memory Optimization**
- **Iterator Pipelines:** For LLVM auto-vectorization
- **Zero-Copy:** Slice-based access patterns
- **SIMD:** `std::arch` with `cfg(target_arch)` fallbacks
- **GPU Compute:** WGPU integration for parallel operations

### **Async Design**
```rust
// Async database operations with zero-copy
pub async fn execute_query<T: DatabaseValue>(
    query: QueryPlan<T>,
) -> Result<QueryResult<T>, DatabaseError> {
    // Use async shaders for GPU operations
    let gpu_result = gpu_executor.execute_async(query).await?;
    Ok(QueryResult::from_gpu(gpu_result))
}
```

## Quality Assurance Framework

### **Testing Strategy** (`tests/`)
- **Unit Tests:** Each module <300 lines tested independently
- **Integration Tests:** Cross-module operation testing
- **Property-Based:** Using `proptest` for algorithmic correctness
- **Formal Verification:** `creusot` or `kani` for critical paths

### **Continuous Integration** (`.github/workflows/ci.yml`)
```yaml
# Automated quality gates
- name: Code Quality Check
  run: |
    cargo clippy -- -D warnings  # Zero warnings policy
    cargo fmt --check            # Consistent formatting
    cargo nextest run            # Fast test execution
    cargo miri test              # Memory safety validation
    cargo deny check             # Dependency security audit
```

### **Metrics Tracking** (`docs/checklist.md`)
- **Test Coverage:** >95% (currently 736 tests)
- **Clippy Warnings:** 0 (currently achieved)  
- **Module Size:** <300 lines (automated enforcement)
- **Memory Usage:** <100MB per 1M operations
- **Query Latency:** <1ms for indexed operations
- **Cyclomatic Complexity:** <10 per function

## Advanced Engineering Requirements

### **Design Patterns**
- **SOLID:** Single responsibility, dependency inversion
- **CUPID:** Composable, Unix-philosophy inspired design
- **GRASP:** Low coupling, high cohesion
- **DRY:** Eliminate code duplication
- **POLA:** Principle of least astonishment

### **Error Handling**
```rust
// Comprehensive error documentation
impl DatabaseEngine {
    /// Execute a query against the database
    /// 
    /// # Errors
    /// 
    /// Returns `QueryError::ParseError` if SQL syntax is invalid
    /// Returns `QueryError::PermissionDenied` if table access forbidden
    /// Returns `QueryError::ConstraintViolation` if data constraints violated
    /// Returns `QueryError::Timeout` if operation exceeds deadline
    pub fn execute_query(&self, sql: &str) -> Result<QueryResult, QueryError> {
        // Implementation with comprehensive error handling
    }
}
```

### **Dependency Management**
- **Security:** `cargo-deny` for vulnerability scanning
- **Minimalism:** Prefer standard library implementations
- **Versioning:** SemVer compliance tracked in CHANGELOG.md

## Success Criteria

### **Phase Completion Targets**
1. **Performance:** All database operations meet latency targets
2. **Reliability:** 24/7 operation without data corruption
3. **Scalability:** Linear performance scaling with core count
4. **Safety:** Zero unsafe code in production paths
5. **Documentation:** Complete rustdoc with usage examples

### **Production Readiness Validation**
- **Load Testing:** 1000 concurrent connections sustained
- **Crash Recovery:** Full data recovery from any failure point
- **Memory Safety:** Validated by Miri with zero undefined behavior
- **Performance:** TPC-C benchmark competitive with established databases

**Target Release:** Production-ready 1.0 with enterprise support capabilities