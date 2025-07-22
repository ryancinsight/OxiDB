// src/api/types.rs
//! Defines the data structures and enumerations used within the API layer.
// Required for Oxidb::new_with_config if it were here, but methods are in api_impl
use crate::core::query::executor::QueryExecutor;
use crate::core::storage::engine::SimpleFileKvStore;
// std::path::PathBuf is not directly used in the struct fields, but methods in api_impl might return it.
// No, Oxidb struct itself does not need PathBuf, Config, etc. directly.
// QueryExecutor and SimpleFileKvStore are essential.

/// `Oxidb` is the primary structure providing the public API for the key-value store.
///
/// It encapsulates a `QueryExecutor` instance to manage database operations,
/// which in turn uses a `SimpleFileKvStore` for persistence.
#[derive(Debug)]
pub struct Oxidb {
    /// The query executor responsible for handling database operations.
    /// Visible within the `api` module (crate::api) to allow `api_impl.rs` to access it.
    pub(crate) executor: QueryExecutor<SimpleFileKvStore>,
}

/// Represents the result of a query execution.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    /// Successful operation that doesn't return data
    Success,
    /// Query that returns data (e.g., SELECT)
    Data(QueryResultData),
    /// Number of rows affected by INSERT, UPDATE, DELETE
    RowsAffected(u64),
}

/// Data returned by a SELECT query
#[derive(Debug, Clone, PartialEq)]
pub struct QueryResultData {
    /// Column names
    pub columns: Vec<String>,
    /// Row data
    pub rows: Vec<Row>,
}

/// A single row of query results
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    /// Values in the row, indexed by column position
    values: Vec<crate::core::common::types::Value>,
}

impl Row {
    /// Creates a new row with the given values
    pub fn new(values: Vec<crate::core::common::types::Value>) -> Self {
        Self { values }
    }
    
    /// Gets a value by column index
    pub fn get(&self, index: usize) -> Option<&crate::core::common::types::Value> {
        self.values.get(index)
    }
    
    /// Gets a value by column name (requires column metadata from QueryResultData)
    pub fn get_by_name(&self, columns: &[String], name: &str) -> Option<&crate::core::common::types::Value> {
        columns.iter()
            .position(|col| col == name)
            .and_then(|index| self.values.get(index))
    }
    
    /// Returns the number of columns in this row
    pub fn len(&self) -> usize {
        self.values.len()
    }
    
    /// Returns true if the row has no columns
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
    
    /// Returns an iterator over the values in this row
    pub fn iter(&self) -> std::slice::Iter<crate::core::common::types::Value> {
        self.values.iter()
    }
}

impl QueryResult {
    /// Creates a QueryResult from the executor's ExecutionResult
    pub fn from_execution_result(result: crate::core::query::executor::ExecutionResult) -> Self {
        use crate::core::query::executor::ExecutionResult;
        
        match result {
            ExecutionResult::Success => QueryResult::Success,
            ExecutionResult::Value(Some(data_type)) => {
                // Convert DataType to Value and create data result
                let value = Self::data_type_to_value(data_type);
                QueryResult::Data(QueryResultData {
                    columns: vec!["value".to_string()],
                    rows: vec![Row::new(vec![value])],
                })
            },
            ExecutionResult::Value(None) => {
                // No value found
                QueryResult::Data(QueryResultData {
                    columns: vec!["value".to_string()],
                    rows: vec![],
                })
            },
            ExecutionResult::Values(data_types) => {
                // Convert multiple DataTypes to Values
                let values: Vec<crate::core::common::types::Value> = data_types.into_iter()
                    .map(Self::data_type_to_value)
                    .collect();
                
                QueryResult::Data(QueryResultData {
                    columns: vec!["value".to_string()],
                    rows: vec![Row::new(values)],
                })
            },
            ExecutionResult::Deleted(true) => QueryResult::RowsAffected(1),
            ExecutionResult::Deleted(false) => QueryResult::RowsAffected(0),
            ExecutionResult::Updated { count } => QueryResult::RowsAffected(count as u64),
            ExecutionResult::RankedResults(ranked_results) => {
                if ranked_results.is_empty() {
                    // Empty result set
                    QueryResult::Data(QueryResultData {
                        columns: vec![],
                        rows: vec![],
                    })
                } else {
                    // Check if this is a similarity search (has non-zero distances) or regular SELECT (all distances are 0.0)
                    let is_similarity_search = ranked_results.iter().any(|(distance, _)| *distance != 0.0);
                    
                    if is_similarity_search {
                        // Convert similarity search results with distance column
                        let rows: Vec<Row> = ranked_results.into_iter()
                            .map(|(distance, data_types)| {
                                let mut values: Vec<crate::core::common::types::Value> = data_types.into_iter()
                                    .map(Self::data_type_to_value)
                                    .collect();
                                // Prepend the distance as the first column
                                values.insert(0, crate::core::common::types::Value::Float(distance as f64));
                                Row::new(values)
                            })
                            .collect();
                        
                        QueryResult::Data(QueryResultData {
                            columns: vec!["distance".to_string(), "data".to_string()],
                            rows,
                        })
                                        } else {
                        // Extract column names from the first row if available (before consuming the iterator)
                        let columns = if let Some(first_result) = ranked_results.first() {
                            if first_result.1.len() >= 2 {
                                if let crate::core::types::DataType::Map(map) = &first_result.1[1] {
                                    map.0.keys().map(|k| String::from_utf8_lossy(k).to_string()).collect()
                                } else {
                                    // Generate generic column names
                                    (0..first_result.1.len()).map(|i| format!("col_{}", i)).collect()
                                }
                            } else {
                                vec![]
                            }
                        } else {
                            vec![]
                        };
                        
                        // Convert regular SELECT results without distance column
                        let rows: Vec<Row> = ranked_results.into_iter()
                            .map(|(_distance, data_types)| {
                                // For each row, we need to extract the actual column data
                                // The TableScanOperator returns [key, row_data] where row_data is a Map
                                if data_types.len() >= 2 {
                                    // Skip the key (first element) and extract columns from the Map (second element)
                                    if let crate::core::types::DataType::Map(map) = &data_types[1] {
                                        let values: Vec<crate::core::common::types::Value> = map.0.values()
                                             .map(|dt| Self::data_type_to_value(dt.clone()))
                                             .collect();
                                        Row::new(values)
                                    } else {
                                        // Fallback: convert all data_types to values
                                        let values: Vec<crate::core::common::types::Value> = data_types.into_iter()
                                            .map(Self::data_type_to_value)
                                            .collect();
                                        Row::new(values)
                                    }
                                } else {
                                    // Fallback: convert all data_types to values
                                    let values: Vec<crate::core::common::types::Value> = data_types.into_iter()
                                        .map(Self::data_type_to_value)
                                        .collect();
                                    Row::new(values)
                                }
                            })
                            .collect();
                        
                        QueryResult::Data(QueryResultData {
                            columns,
                            rows,
                        })
                    }
                }
            },
        }
    }
    
    /// Helper method to convert DataType to Value
    fn data_type_to_value(data_type: crate::core::types::DataType) -> crate::core::common::types::Value {
        use crate::core::types::DataType;
        
        match data_type {
            DataType::Integer(i) => crate::core::common::types::Value::Integer(i),
            DataType::Float(f) => crate::core::common::types::Value::Float(f),
            DataType::String(s) => crate::core::common::types::Value::Text(s),
            DataType::Boolean(b) => crate::core::common::types::Value::Boolean(b),
            DataType::RawBytes(b) => crate::core::common::types::Value::Blob(b),
            DataType::Vector(v) => crate::core::common::types::Value::Vector(v.data),
            DataType::Null => crate::core::common::types::Value::Null,
            DataType::Map(map) => crate::core::common::types::Value::Text(serde_json::to_string(&map.0).unwrap_or_else(|_| "{}".to_string())), // Serialize map to JSON string
            DataType::JsonBlob(json) => crate::core::common::types::Value::Text(json.to_string()),
        }
    }
    
    /// Returns true if this result represents successful data retrieval
    pub fn has_data(&self) -> bool {
        matches!(self, QueryResult::Data(_))
    }
    
    /// Returns true if this result represents a successful modification
    pub fn has_rows_affected(&self) -> bool {
        matches!(self, QueryResult::RowsAffected(_))
    }
    
    /// Gets the number of rows affected, if applicable
    pub fn rows_affected(&self) -> Option<u64> {
        match self {
            QueryResult::RowsAffected(count) => Some(*count),
            _ => None,
        }
    }
    
    /// Gets the data, if this is a data result
    pub fn data(&self) -> Option<&QueryResultData> {
        match self {
            QueryResult::Data(data) => Some(data),
            _ => None,
        }
    }
}

impl QueryResultData {
    /// Creates new query result data
    pub fn new(columns: Vec<String>, rows: Vec<Row>) -> Self {
        Self { columns, rows }
    }
    
    /// Returns the number of columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
    
    /// Returns the number of rows
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
    
    /// Gets a row by its index
    pub fn get_row(&self, index: usize) -> Option<&Row> {
        self.rows.get(index)
    }
    
    /// Returns an iterator over the rows
    pub fn rows(&self) -> std::slice::Iter<Row> {
        self.rows.iter()
    }
    
    /// Returns the column names
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}

// Example (to be expanded later):
// pub struct ApiRequest { /* ... */ }
// pub enum ApiResponse { /* ... */ }
