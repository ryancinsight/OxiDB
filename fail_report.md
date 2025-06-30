# OxiDB Test Failure Report

**Date:** December 20, 2024  
**Baseline Commit:** dc1d2b936c67a7ba6b4cb011846815021b023529  
**Baseline Tag:** v0.8.0-pre-vector  
**Platform:** Windows 11 Business Insider Preview (Build 26200)  
**Rust Version:** rustc 1.89.0-nightly (4b27a04cc 2025-06-04)  
**Cargo Version:** cargo 1.89.0-nightly (64a124607 2025-05-30)  

## Executive Summary

Running `cargo test --all-features` reveals 3 critical test failures:
- 2 B-tree deletion operation failures in the indexing module
- 1 WAL file permission issue in the storage engine module

Total test results: **467 passed; 3 failed; 0 ignored**

## Test Environment

### Windows Environment
- **OS:** Microsoft Windows 11 Business Insider Preview
- **Build:** 10.0.26200 N/A Build 26200
- **Architecture:** x86_64-pc-windows-msvc
- **Temp Directory:** `C:\Users\RyanClanton\AppData\Local\Temp\`

### Development Matrix
Currently testing on Windows. Linux matrix testing pending.

## Failure Details

### 1. B-tree Failure: Internal Borrow from Right Sibling

**Test:** `core::indexing::btree::tree::tests::test_delete_internal_borrow_from_right_sibling`  
**File:** `src\core\indexing\btree\tree.rs:961`  
**Status:** FAILED

#### Error Details
```
assertion `left == right` failed: Root key incorrect
  left: [[48, 53]]
 right: [[49, 48]]
```

#### Stack Trace
```
thread 'core::indexing::btree::tree::tests::test_delete_internal_borrow_from_right_sibling' panicked at src\core\indexing\btree\tree.rs:961:17:
assertion `left == right` failed: Root key incorrect
  left: [[48, 53]]
 right: [[49, 48]]
stack backtrace:
   0:     0x7ff72233bf82 - std::backtrace_rs::backtrace::win64::trace
   1:     0x7ff72233bf82 - std::backtrace_rs::backtrace::trace_unsynchronized
   ...
  17:     0x7ff72217eb7b - core::panicking::assert_failed<ref$<slice2$<alloc::vec::Vec<u8,alloc::alloc::Global> > >,ref$<array$<alloc::vec::Vec<u8,alloc::alloc::Global>,1> > >
  18:     0x7ff7220fde41 - oxidb::core::indexing::btree::tree::tests::test_delete_internal_borrow_from_right_sibling
                               at C:\Users\RyanClanton\OxiDB\src\core\indexing\btree\tree.rs:961
```

#### Analysis
The test is failing because the root key after a B-tree internal node borrow operation does not match expected values. The assertion expects the root to contain key `[49, 48]` (which represents `k("10")`) but finds `[48, 53]` (which represents `k("05")`).

#### Debug Output
```
[DEBUG CONSTRUCT] Writing node PageID: 0, Keys: [[48, 53]]
[DEBUG CONSTRUCT] ... Children: [1, 4]
[DEBUG CONSTRUCT] Writing node PageID: 1, Keys: [[48, 48]]
[DEBUG CONSTRUCT] ... Children: [2, 3]
[DEBUG CONSTRUCT] Writing node PageID: 2, Keys: [[48, 48]]
[DEBUG CONSTRUCT] ... Value sets count: 1, NextLeaf: Some(3)
[DEBUG CONSTRUCT] Writing node PageID: 3, Keys: [[48, 50]]
[DEBUG CONSTRUCT] ... Value sets count: 1, NextLeaf: Some(5)
[DEBUG CONSTRUCT] Writing node PageID: 4, Keys: [[49, 48], [49, 50]]
[DEBUG CONSTRUCT] ... Children: [5, 6, 7]
[DEBUG CONSTRUCT] Writing node PageID: 5, Keys: [[49, 48]]
[DEBUG CONSTRUCT] ... Value sets count: 1, NextLeaf: Some(6)
[DEBUG CONSTRUCT] Writing node PageID: 6, Keys: [[49, 50]]
[DEBUG CONSTRUCT] ... Value sets count: 1, NextLeaf: Some(7)
[DEBUG CONSTRUCT] Writing node PageID: 7, Keys: [[49, 52]]
[DEBUG CONSTRUCT] ... Value sets count: 1, NextLeaf: None
```

### 2. B-tree Failure: Internal Merge with Left Sibling

**Test:** `core::indexing::btree::tree::tests::test_delete_internal_merge_with_left_sibling`  
**File:** `src\core\indexing\btree\tree.rs:1034`  
**Status:** FAILED

#### Error Details
```
assertion `left != right` failed: Root PID should change
  left: 0
 right: 0
```

#### Stack Trace
```
thread 'core::indexing::btree::tree::tests::test_delete_internal_merge_with_left_sibling' panicked at src\core\indexing\btree\tree.rs:1034:9:
assertion `left != right` failed: Root PID should change
  left: 0
 right: 0
stack backtrace:
   0:     0x7ff72233bf82 - std::backtrace_rs::backtrace::win64::trace
   ...
  18:     0x7ff722101189 - oxidb::core::indexing::btree::tree::tests::test_delete_internal_merge_with_left_sibling
                               at C:\Users\RyanClanton\OxiDB\src\core\indexing\btree\tree.rs:1034
```

#### Analysis
The test expects the root page ID to change after an internal node merge operation that should cascade to the root, but the root page ID remains 0. This suggests the B-tree delete operation is not properly handling the merge cascade when both internal nodes are at minimum capacity.

#### Debug Output
```
[DEBUG CONSTRUCT] Writing node PageID: 0, Keys: [[48, 50]]
[DEBUG CONSTRUCT] ... Children: [1, 2]
[DEBUG CONSTRUCT] Writing node PageID: 1, Keys: [[48, 48]]
[DEBUG CONSTRUCT] ... Children: [3, 4]
[DEBUG CONSTRUCT] Writing node PageID: 2, Keys: [[48, 52]]
[DEBUG CONSTRUCT] ... Children: [5, 6]
[DEBUG CONSTRUCT] Writing node PageID: 3, Keys: [[48, 48]]
[DEBUG CONSTRUCT] ... Value sets count: 1, NextLeaf: Some(4)
[DEBUG CONSTRUCT] Writing node PageID: 4, Keys: [[48, 49]]
[DEBUG CONSTRUCT] ... Value sets count: 1, NextLeaf: Some(5)
[DEBUG CONSTRUCT] Writing node PageID: 5, Keys: [[48, 52]]
[DEBUG CONSTRUCT] ... Value sets count: 1, NextLeaf: Some(6)
[DEBUG CONSTRUCT] Writing node PageID: 6, Keys: [[48, 53]]
[DEBUG CONSTRUCT] ... Value sets count: 1, NextLeaf: None
```

### 3. WAL Permission Failure

**Test:** `core::storage::engine::implementations::tests::simple_file_tests::test_delete_atomicity_wal_failure`  
**File:** `src\core\storage\engine\implementations\tests\simple_file_tests.rs:948`  
**Status:** FAILED

#### Error Details
```
called `Result::unwrap()` on an `Err` value: Io(Os { code: 5, kind: PermissionDenied, message: "Access is denied." })
```

#### Stack Trace
```
thread 'core::storage::engine::implementations::tests::simple_file_tests::test_delete_atomicity_wal_failure' panicked at src\core\storage\engine\implementations\tests\simple_file_tests.rs:948:54:
called `Result::unwrap()` on an `Err` value: Io(Os { code: 5, kind: PermissionDenied, message: "Access is denied." })
stack backtrace:
   0:     0x7ff72233bf82 - std::backtrace_rs::backtrace::win64::trace
   ...
  18:     0x7ff721ee22b3 - oxidb::core::storage::engine::implementations::tests::simple_file_tests::test_delete_atomicity_wal_failure
                               at C:\Users\RyanClanton\OxiDB\src\core\storage\engine\implementations\tests\simple_file_tests.rs:948
```

#### Analysis
This test intentionally creates a directory where the WAL file should be created, simulating a WAL write failure scenario. The test then attempts to create a new `SimpleFileKvStore` instance, which tries to initialize a WAL writer. On Windows, this results in an "Access is denied" error (OS error code 5) when trying to create/access the WAL file because a directory with the same name already exists.

The test expects this failure and should handle the `OxidbError::Io` variant, but the test is calling `.unwrap()` on the result causing a panic instead of proper error handling.

#### Debug Output
```
[engine::wal::WalWriter::new] Received db_file_path: "C:\\Users\\RyanClanton\\AppData\\Local\\Temp\\.tmpWlXLAx\\test_delete_atomicity.db"
[engine::wal::WalWriter::new] Derived wal_file_path: "C:\\Users\\RyanClanton\\AppData\\Local\\Temp\\.tmpWlXLAx\\test_delete_atomicity.db.wal"
[SimpleFileKvStore::put] Method entered for key: "atomic_del_key"
[engine::wal::WalWriter::log_entry] Method entered. Attempting to log to: "C:\\Users\\RyanClanton\\AppData\\Local\\Temp\\.tmpWlXLAx\\test_delete_atomicity.db.wal", entry: Put { lsn: 0, transaction_id: 0, key: [97, 116, 111, 109, 105, 99, 95, 100, 101, 108, 95, 107, 101, 121], value: [97, 116, 111, 109, 105, 99, 95, 100, 101, 108, 95, 118, 97, 108, 117, 101] }
[engine::wal::WalWriter::log_entry] Successfully opened/created file: "C:\\Users\\RyanClanton\\AppData\\Local\\Temp\\.tmpWlXLAx\\test_delete_atomicity.db.wal"
[save_data_to_disk] Attempting to delete WAL file: "C:\\Users\\RyanClanton\\AppData\\Local\\Temp\\.tmpWlXLAx\\test_delete_atomicity.db.wal"
[save_data_to_disk] WAL file "C:\\Users\\RyanClanton\\AppData\\Local\\Temp\\.tmpWlXLAx\\test_delete_atomicity.db.wal" exists, proceeding with deletion.
[save_data_to_disk] Successfully deleted WAL file: "C:\\Users\\RyanClanton\\AppData\\Local\\Temp\\.tmpWlXLAx\\test_delete_atomicity.db.wal"
[save_data_to_disk] Attempting to delete WAL file: "C:\\Users\\RyanClanton\\AppData\\Local\\Temp\\.tmpWlXLAx\\test_delete_atomicity.db.wal"
[save_data_to_disk] WAL file "C:\\Users\\RyanClanton\\AppData\\Local\\Temp\\.tmpWlXLAx\\test_delete_atomicity.db.wal" did not exist, no deletion needed.
[engine::wal::WalWriter::new] Received db_file_path: "C:\\Users\\RyanClanton\\AppData\\Local\\Temp\\.tmpWlXLAx\\test_delete_atomicity.db"
[engine::wal::WalWriter::new] Derived wal_file_path: "C:\\Users\\RyanClanton\\AppData\\Local\\Temp\\.tmpWlXLAx\\test_delete_atomicity.db.wal"
```

## Root Cause Analysis

### B-tree Issues
Both B-tree failures appear to be related to the internal node rebalancing logic during delete operations:

1. **Internal Borrow Failure:** The borrow operation from right sibling is not correctly updating the root key when redistributing keys between internal nodes.

2. **Internal Merge Failure:** The merge operation is not properly cascading changes to the root when both internal nodes are at minimum capacity.

These issues likely stem from the recent refactoring mentioned in commit `29d3360: "Refactor BPlusTreeIndex for modularity by introducing PageManager"`.

### WAL Permission Issue
The WAL permission failure is a Windows-specific issue where the test setup creates a directory with the same name as the expected WAL file, causing a permission denied error when trying to create the WAL file. This appears to be a test design issue rather than a core functionality problem.

## Impact Assessment

### Severity: HIGH
- **B-tree Issues:** Critical data structure corruption possible during delete operations
- **WAL Issue:** Test framework reliability on Windows platform

### Affected Components
- `core::indexing::btree::tree` - B-tree deletion operations
- `core::storage::engine::implementations::simple_file` - WAL file handling on Windows

## Recommendations

### Immediate Actions
1. **B-tree Fixes:**
   - Review the internal node rebalancing logic in delete operations
   - Verify key redistribution during borrow operations
   - Ensure proper root updates during merge cascades

2. **WAL Test Fix:**
   - Modify test to properly handle expected permission errors
   - Consider platform-specific test implementations

3. **Testing:**
   - Expand Linux testing to verify platform-specific behavior
   - Add regression tests for the fixed scenarios

### Next Steps
1. Analyze the PageManager refactoring impact on B-tree operations
2. Review test setup procedures for cross-platform compatibility
3. Consider adding debug instrumentation for B-tree operations

## Artifacts

- **Baseline Tag:** `v0.8.0-pre-vector` created at commit `dc1d2b936c67a7ba6b4cb011846815021b023529`
- **Full Test Output:** Available in CI logs
- **Stack Traces:** Captured with `RUST_BACKTRACE=full`

---

**Report Generated:** December 20, 2024  
**Environment:** Windows 11 + Rust nightly  
**Next Phase:** Fix implementation and expand testing matrix
