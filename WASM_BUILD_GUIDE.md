# OxiDB WASM Build Guide

This guide explains how to build and test OxiDB for WebAssembly (WASM).

## Prerequisites

1. Install Rust and the wasm32 target:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup target add wasm32-unknown-unknown
```

2. Install wasm-pack:
```bash
cargo install wasm-pack
```

3. Install wasm-bindgen-cli (optional, for manual builds):
```bash
cargo install wasm-bindgen-cli
```

## Building for WASM

### Using wasm-pack (Recommended)

```bash
wasm-pack build --target web --out-dir pkg
```

This will create a `pkg` directory with:
- `oxidb.js` - JavaScript bindings
- `oxidb_bg.wasm` - The WASM binary
- `oxidb.d.ts` - TypeScript definitions
- `package.json` - NPM package file

### Manual Build

```bash
cargo build --target wasm32-unknown-unknown --lib
```

## Key Changes for WASM Compatibility

1. **Conditional Dependencies**: The `Cargo.toml` has been updated to use different dependencies for WASM:
   - Tokio without networking features (no `net` feature)
   - UUID with JavaScript random source (`js` feature)
   - getrandom with JavaScript support

2. **WASM Module**: A new `src/wasm.rs` module provides WASM-specific bindings:
   - `WasmDatabase` struct wraps the OxiDB Connection
   - Methods return JSON strings for easy JavaScript consumption
   - Error handling converts Rust errors to JavaScript errors

3. **Library Type**: The crate is configured to build as both a regular library and a C-compatible dynamic library (`cdylib`) for WASM.

## Testing the WASM Build

1. Build the WASM module:
```bash
wasm-pack build --target web --out-dir pkg
```

2. Start the test server:
```bash
python3 serve_wasm.py
```

3. Open http://localhost:8000/wasm_test.html in your browser

The test page allows you to:
- Create tables
- Insert data
- Query data
- See results in a formatted output

## Example Usage in JavaScript

```javascript
import init, { WasmDatabase } from './pkg/oxidb.js';

// Initialize the WASM module
await init();

// Create a new database instance
const db = new WasmDatabase();

// Execute SQL commands
const result = await db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
console.log(JSON.parse(result));

// Insert data
await db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");

// Query data
const queryResult = await db.query("SELECT * FROM users");
const data = JSON.parse(queryResult);
console.log(data.rows);
```

## Limitations

- Only in-memory databases are supported in WASM (no file system access)
- Some features requiring OS-specific functionality may not be available
- Performance may be different compared to native builds

## Troubleshooting

### Build Errors

1. **getrandom error**: Make sure the `js` feature is enabled for getrandom in WASM builds
2. **mio error**: Ensure Tokio is configured without the `net` feature for WASM
3. **uuid error**: Enable the `js` feature for uuid in WASM builds

### Runtime Errors

1. **CORS errors**: Use the provided Python server script which sets proper headers
2. **Module not found**: Ensure the import path in your HTML/JS matches the actual file location
3. **Initialization failed**: Check browser console for detailed error messages