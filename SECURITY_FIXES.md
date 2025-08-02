# Security Fixes

## Removed Vulnerable Example: wasm_test.rs

**Date**: Current
**Severity**: High
**Type**: SQL Injection Vulnerability

### Description

The `examples/wasm_test.rs` file contained critical security vulnerabilities where SQL queries were constructed using `format!` string interpolation, making them susceptible to SQL injection attacks.

### Vulnerable Code Examples

```rust
// SQL Injection vulnerable code from the removed file:
let query = format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, data TEXT)", table_name);
let query = format!("INSERT INTO {} (id, data) VALUES ({}, '{}')", table_name, id, data);
```

### Resolution

- **Removed**: `examples/wasm_test.rs` - This example was obsolete and insecure
- **Removed**: Corresponding entry in `Cargo.toml`
- **Preserved**: The proper WASM implementation in `src/wasm.rs` which uses safe query methods
- **Preserved**: `wasm_test.html` which correctly uses the safe implementation from `src/wasm.rs`

### Safe Alternative

The correct WASM implementation is available in `src/wasm.rs` which:
- Uses proper parameterized queries through the OxiDB API
- Does not construct SQL strings through concatenation
- Is actively maintained and tested

### Impact

This change removes a significant security risk and prevents developers from accidentally using the vulnerable example code as a reference for their own implementations.