use wasm_bindgen::prelude::*;
use oxidb::db::{Database, QueryBuilder};
use oxidb::value::JsonValue;

#[wasm_bindgen]
pub struct WasmDatabase {
    db: Database,
}

#[wasm_bindgen]
impl WasmDatabase {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        // Initialize with in-memory database for WASM
        let db = Database::in_memory();
        WasmDatabase { db }
    }

    pub fn create_table(&mut self, table_name: &str) -> Result<(), JsValue> {
        let query = format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, data TEXT)", table_name);
        self.db.execute(&query)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(())
    }

    pub fn insert(&mut self, table_name: &str, id: i32, data: &str) -> Result<(), JsValue> {
        let query = format!("INSERT INTO {} (id, data) VALUES ({}, '{}')", table_name, id, data);
        self.db.execute(&query)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(())
    }

    pub fn query(&mut self, sql: &str) -> Result<String, JsValue> {
        let result = self.db.execute(sql)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        // Convert result to JSON string for easy consumption in JS
        let json_result = serde_json::to_string(&result)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        Ok(json_result)
    }
}

// When the wasm module is instantiated, this will be called
#[wasm_bindgen(start)]
pub fn main() {
    // Set panic hook for better error messages in wasm
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}