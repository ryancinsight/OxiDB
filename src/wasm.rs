#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
use crate::api::{Connection, QueryResult};

#[cfg(target_arch = "wasm32")]
use serde::{Serialize, Deserialize};

#[cfg(target_arch = "wasm32")]
#[derive(Serialize, Deserialize)]
struct JsonQueryResult {
    success: bool,
    message: String,
    rows_affected: Option<u64>,
    columns: Option<Vec<String>>,
    rows: Option<Vec<Vec<serde_json::Value>>>,
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub struct WasmDatabase {
    conn: Connection,
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
impl WasmDatabase {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<WasmDatabase, JsValue> {
        // Initialize with in-memory database for WASM
        let conn = Connection::open_in_memory()
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(WasmDatabase { conn })
    }

    pub fn execute(&mut self, sql: &str) -> Result<String, JsValue> {
        let result = self.conn.execute(sql)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        // Convert QueryResult to our JSON-friendly format
        let json_result = match result {
            QueryResult::Success => JsonQueryResult {
                success: true,
                message: "Success".to_string(),
                rows_affected: None,
                columns: None,
                rows: None,
            },
            QueryResult::RowsAffected(count) => JsonQueryResult {
                success: true,
                message: format!("{} rows affected", count),
                rows_affected: Some(count),
                columns: None,
                rows: None,
            },
            QueryResult::Data(data) => {
                let mut json_rows = Vec::new();
                for row in &data.rows {
                    let mut json_row = Vec::new();
                    for i in 0..data.columns.len() {
                        let value = row.get(i).map(|v| {
                            // Convert Value to serde_json::Value
                            match v {
                                crate::core::common::types::Value::Null => serde_json::Value::Null,
                                crate::core::common::types::Value::Boolean(b) => serde_json::Value::Bool(*b),
                                crate::core::common::types::Value::Integer(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
                                crate::core::common::types::Value::Float(f) => {
                                    serde_json::Number::from_f64(*f)
                                        .map(serde_json::Value::Number)
                                        .unwrap_or(serde_json::Value::Null)
                                },
                                crate::core::common::types::Value::Text(s) => serde_json::Value::String(s.clone()),
                                crate::core::common::types::Value::Blob(b) => serde_json::Value::String(hex::encode(b)),
                                crate::core::common::types::Value::Vector(v) => {
                                    // Convert vector to array of numbers
                                    let vec_values: Vec<serde_json::Value> = v.iter()
                                        .map(|f| serde_json::Number::from_f64(*f as f64)
                                            .map(serde_json::Value::Number)
                                            .unwrap_or(serde_json::Value::Null))
                                        .collect();
                                    serde_json::Value::Array(vec_values)
                                },
                            }
                        }).unwrap_or(serde_json::Value::Null);
                        json_row.push(value);
                    }
                    json_rows.push(json_row);
                }
                
                JsonQueryResult {
                    success: true,
                    message: format!("{} rows returned", json_rows.len()),
                    rows_affected: None,
                    columns: Some(data.columns),
                    rows: Some(json_rows),
                }
            }
        };
        
        // Convert to JSON string
        let json_string = serde_json::to_string(&json_result)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        Ok(json_string)
    }

    pub fn query(&mut self, sql: &str) -> Result<String, JsValue> {
        // For query, we'll use execute which handles SELECT statements
        self.execute(sql)
    }
}