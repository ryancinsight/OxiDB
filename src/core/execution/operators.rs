// Will contain execution operator implementations
use crate::core::common::error::DbError;
use crate::core::types::DataType; // Make sure DataType is accessible
use crate::core::execution::{ExecutionOperator, Tuple}; // Assuming Tuple is Vec<DataType>
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::indexing::manager::IndexManager;
use crate::core::common::serialization::deserialize_data_type; // For deserializing store values
use crate::core::query::commands::Key; // Or whatever your primary key type is
use std::sync::Arc;
use std::collections::HashSet; // Removed HashMap

// Imports for FilterOperator / ProjectOperator
use crate::core::optimizer::Expression; // Assuming these are in optimizer/mod.rs
use crate::core::optimizer::JoinPredicate; // Ensure this is imported
// SimplePredicate is part of Expression enum based on previous setup.
// QueryPlanNode is not directly used by these operators, but Expression is.

#[allow(dead_code)] // To be removed when used
pub struct TableScanOperator<S: KeyValueStore<Key, Vec<u8>>> {
    store: Arc<S>, // Use Arc for shared ownership if QueryExecutor holds the store in Arc
    table_name: String, // Or some identifier for the data to scan
    snapshot_id: u64,
    committed_ids: Arc<HashSet<u64>>, // Use Arc for shared ownership
    executed: bool, // To ensure iterator is consumed only once if necessary
}

#[allow(dead_code)] // To be removed when used
impl<S: KeyValueStore<Key, Vec<u8>>> TableScanOperator<S> {
    pub fn new(
        store: Arc<S>,
        table_name: String,
        snapshot_id: u64,
        committed_ids: Arc<HashSet<u64>>
    ) -> Self {
        TableScanOperator {
            store,
            table_name,
            snapshot_id,
            committed_ids,
            executed: false,
        }
    }
}

// Define the actual iterator struct to be returned by NestedLoopJoinOperator::execute
#[allow(dead_code)]
struct NestedLoopJoinIteratorInternal {
    left_input_iter: Box<dyn Iterator<Item = Result<Tuple, DbError>> + Send + Sync>,
    join_predicate: Option<JoinPredicate>, // Cloned from the operator
    current_left_tuple: Option<Tuple>,
    right_tuples_buffer: Arc<Vec<Tuple>>,
    current_right_buffer_iter: std::vec::IntoIter<Tuple>,
}

#[allow(dead_code)]
impl NestedLoopJoinIteratorInternal {
    // Helper to evaluate join predicate
    fn evaluate_join_predicate(&self, left_tuple: &Tuple, right_tuple: &Tuple) -> Result<bool, DbError> {
         if let Some(ref predicate) = self.join_predicate {
            let left_col_idx = predicate.left_column.parse::<usize>().map_err(|_| DbError::Internal(format!("Invalid left column index: {}", predicate.left_column)))?;
            let right_col_idx = predicate.right_column.parse::<usize>().map_err(|_| DbError::Internal(format!("Invalid right column index: {}", predicate.right_column)))?;

            if left_col_idx >= left_tuple.len() || right_col_idx >= right_tuple.len() {
                return Err(DbError::Internal("Join predicate column index out of bounds.".to_string()));
            }
            Ok(left_tuple[left_col_idx] == right_tuple[right_col_idx])
        } else {
            Ok(true) // Cartesian product if no predicate
        }
    }
}

impl Iterator for NestedLoopJoinIteratorInternal {
    type Item = Result<Tuple, DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Ensure we have a current left tuple
            if self.current_left_tuple.is_none() {
                match self.left_input_iter.next() {
                    Some(Ok(left_tuple)) => {
                        self.current_left_tuple = Some(left_tuple);
                        // Reset right iterator for the new left tuple by cloning the Arc's content
                        self.current_right_buffer_iter = Arc::clone(&self.right_tuples_buffer).as_ref().clone().into_iter();
                    }
                    Some(Err(e)) => return Some(Err(e)), // Error from left input
                    None => return None, // Left input exhausted
                }
            }

            // current_left_tuple is guaranteed to be Some here
            let left_tuple = self.current_left_tuple.as_ref().unwrap();

            // Iterate through the current view of the right tuples buffer
            while let Some(right_tuple) = self.current_right_buffer_iter.next() {
                match self.evaluate_join_predicate(left_tuple, &right_tuple) {
                    Ok(true) => {
                        // Predicate matches, combine tuples
                        let mut joined_tuple = left_tuple.clone();
                        joined_tuple.extend(right_tuple.clone()); // Simple concatenation
                        return Some(Ok(joined_tuple));
                    }
                    Ok(false) => continue, // Predicate false, try next right tuple
                    Err(e) => return Some(Err(e)), // Error evaluating predicate
                }
            }

            // If right buffer exhausted for current left tuple, reset current_left_tuple to fetch a new one in the next iteration
            self.current_left_tuple = None;
        }
    }
}

#[allow(dead_code)]
impl ExecutionOperator for NestedLoopJoinOperator {
    fn execute(&mut self) -> Result<Box<dyn Iterator<Item = Result<Tuple, DbError>> + Send + Sync>, DbError> {
        if self.right_tuples_buffer.is_none() {
            // First execution: buffer the right input
            let mut right_buffer_for_iter = Vec::new();
            let right_exec = self.right_input.execute()?; // Consumes right_input if not designed for re-execution, removed mut
            for item_res in right_exec {
                right_buffer_for_iter.push(item_res?);
            }
            self.right_tuples_buffer = Some(right_buffer_for_iter);
        }

        // If right_tuples_buffer is Some (either just populated or from previous partial execution),
        // we can create the iterator.
        // This design assumes that if execute is called again, it reuses the buffered right tuples.
        // And gets a new iterator for the left side.

        let left_iter = self.left_input.execute()?; // Gets a fresh iterator from left_input

        let right_buffer_cloned_for_iter = Arc::new(
            self.right_tuples_buffer.as_ref()
                .ok_or_else(|| DbError::Internal("Right buffer not loaded in NLJ".to_string()))?
                .clone()
        );

        let iter = NestedLoopJoinIteratorInternal {
            left_input_iter: left_iter,
            join_predicate: self.join_predicate.clone(),
            current_left_tuple: None,
            current_right_buffer_iter: Arc::clone(&right_buffer_cloned_for_iter).as_ref().clone().into_iter(), // Initial iterator
            right_tuples_buffer: right_buffer_cloned_for_iter,
        };

        Ok(Box::new(iter))
    }
}

impl<S: KeyValueStore<Key, Vec<u8>> + 'static> ExecutionOperator for TableScanOperator<S> {
    fn execute(&mut self) -> Result<Box<dyn Iterator<Item = Result<Tuple, DbError>> + Send + Sync>, DbError> {
        if self.executed {
            return Err(DbError::Internal("TableScanOperator cannot be executed more than once".to_string()));
        }
        self.executed = true;

        // TODO: For a real table scan, KeyValueStore::scan() needs to be able
        // to filter by table or use a key prefix convention.
        // For now, it scans ALL keys.
        let all_kvs = self.store.scan()?;

        // These fields are not used in the current simplified iterator logic,
        // but would be crucial for proper MVCC visibility checks if scan() returned all versions
        // or if we needed to call self.store.get() with snapshot info.
        // let _snapshot_id = self.snapshot_id;
        // let _committed_ids = Arc::clone(&self.committed_ids);
        // let _store_clone = Arc::clone(&self.store);

        let iterator = all_kvs.into_iter().filter_map(move |(_key, value_bytes)| {
            // Assuming scan() provides data that is already visible under some snapshot rule,
            // or that MVCC is not fully implemented at this operator level for scan().
            // Proper MVCC for scan would require KeyValueStore::scan to be snapshot-aware.
            match deserialize_data_type(&value_bytes) {
                Ok(data_type) => {
                    let tuple = match data_type {
                        DataType::Map(map_data) => {
                            // Convert map to a vector of its values. Order might be an issue.
                            // For now, just take values. A proper schema would define order.
                            map_data.values().cloned().collect::<Vec<DataType>>()
                        }
                        // If it's a JsonBlob that represents a map-like structure, handle similarly or as defined.
                        DataType::JsonBlob(json_value) => {
                            if json_value.is_object() {
                                // If you want to treat JSON objects like maps:
                                json_value.as_object().unwrap().values().map(|v| {
                                    // This is a simplification: converting serde_json::Value to DataType
                                    // might require a more robust conversion logic.
                                    // For now, let's assume it can be stringified or handled.
                                    // This part is highly dependent on how JsonBlob is meant to be used.
                                    // Simplest for now: push a string representation or a wrapped JsonBlob DataType.
                                    // This is likely not the final form for tuple conversion.
                                    DataType::String(v.to_string()) // Example: convert JSON values to string
                                }).collect::<Vec<DataType>>()
                            } else {
                                vec![DataType::JsonBlob(json_value)] // Treat as single column if not object
                            }
                        }
                        single_val => vec![single_val], // If not a map or handled JsonBlob, treat as a single-column tuple
                    };
                    Some(Ok(tuple))
                }
                Err(e) => Some(Err(DbError::SerializationError(format!("Failed to deserialize data during table scan: {}", e)))),
            }
        });

        Ok(Box::new(iterator))
    }
}

#[allow(dead_code)]
pub struct NestedLoopJoinOperator {
    left_input: Box<dyn ExecutionOperator + Send + Sync>,
    right_input: Box<dyn ExecutionOperator + Send + Sync>,
    join_predicate: Option<JoinPredicate>,
    // Internal state for iteration (used by the execute method to create the iterator):
    // These are effectively one-time use for creating the iterator state.
    // If execute could be called multiple times, these would need to be handled differently (e.g. reset)
    // or the operator itself would need to be consumed/cloned.
    // For now, execute will populate right_tuples_buffer once.
    right_tuples_buffer: Option<Vec<Tuple>>, // Populated once by execute
}

#[allow(dead_code)]
impl NestedLoopJoinOperator {
    pub fn new(
        left_input: Box<dyn ExecutionOperator + Send + Sync>,
        right_input: Box<dyn ExecutionOperator + Send + Sync>,
        join_predicate: Option<JoinPredicate>,
    ) -> Self {
        NestedLoopJoinOperator {
            left_input,
            right_input,
            join_predicate,
            right_tuples_buffer: None, // Initialized by execute
        }
    }

    // Helper to evaluate join predicate - this will be used by the iterator.
    // For now, keep it here as a static-like method if possible, or it needs to be on the iterator.
    // Let's make it available to the iterator by having it on the operator,
    // and the iterator will call it.
    // This function is effectively static as it doesn't depend on self's mutable state beyond the predicate.
    #[allow(dead_code)] // Potentially unused if iterator directly implements or copies logic
    fn evaluate_join_predicate_logic(left_tuple: &Tuple, right_tuple: &Tuple, join_predicate: &Option<JoinPredicate>) -> Result<bool, DbError> {
        if let Some(ref predicate) = join_predicate {
            let left_col_idx = predicate.left_column.parse::<usize>().map_err(|_| DbError::Internal(format!("Invalid left column index: {}", predicate.left_column)))?;
            let right_col_idx = predicate.right_column.parse::<usize>().map_err(|_| DbError::Internal(format!("Invalid right column index: {}", predicate.right_column)))?;

            if left_col_idx >= left_tuple.len() || right_col_idx >= right_tuple.len() {
                return Err(DbError::Internal("Join predicate column index out of bounds.".to_string()));
            }

            Ok(left_tuple[left_col_idx] == right_tuple[right_col_idx])
        } else {
            Ok(true) // Cartesian product
        }
    }
}

#[allow(dead_code)]
pub struct ProjectOperator {
    input: Box<dyn ExecutionOperator + Send + Sync>,
    // For simplicity, columns are specified by index.
    // Later, this could be Vec<String> and resolved using schema.
    column_indices: Vec<usize>,
}

#[allow(dead_code)]
impl ProjectOperator {
    pub fn new(input: Box<dyn ExecutionOperator + Send + Sync>, column_indices: Vec<usize>) -> Self {
        ProjectOperator { input, column_indices }
    }
}

#[allow(dead_code)]
impl ExecutionOperator for ProjectOperator {
    fn execute(&mut self) -> Result<Box<dyn Iterator<Item = Result<Tuple, DbError>> + Send + Sync>, DbError> {
        let input_iter = self.input.execute()?;
        let indices_clone = self.column_indices.clone(); // Clone for use in closure

        let iterator = input_iter.map(move |tuple_result| {
            tuple_result.and_then(|tuple| {
                let mut projected_tuple = Vec::with_capacity(indices_clone.len());
                for &index in &indices_clone {
                    if index < tuple.len() {
                        projected_tuple.push(tuple[index].clone()); // Clone data into new tuple
                    } else {
                        // Error: index out of bounds
                        return Err(DbError::Internal(format!(
                            "Projection index {} out of bounds for tuple length {}",
                            index, tuple.len()
                        )));
                    }
                }
                Ok(projected_tuple)
            })
        });

        Ok(Box::new(iterator))
    }
}

#[allow(dead_code)]
pub struct FilterOperator {
    input: Box<dyn ExecutionOperator + Send + Sync>,
    predicate: Expression, // From optimizer::mod.rs
                           // Potentially schema or column_names: Arc<HashMap<String, usize>> to map names to indices
}

#[allow(dead_code)]
impl FilterOperator {
    pub fn new(input: Box<dyn ExecutionOperator + Send + Sync>, predicate: Expression) -> Self {
        FilterOperator { input, predicate }
    }

    // Helper function to evaluate the predicate on a tuple.
    // This will be basic for now.
    // This method is not used due to closure constraints in execute, static_evaluate_predicate is used instead.
    #[allow(dead_code)]
    fn evaluate_predicate(&self, tuple: &Tuple) -> Result<bool, DbError> {
        match &self.predicate {
            Expression::Predicate(simple_predicate) => {
                let column_index = match simple_predicate.column.parse::<usize>() {
                    Ok(idx) => idx,
                    Err(_) => {
                        return Err(DbError::NotImplemented(format!(
                            "Column name resolution ('{}') not implemented in FilterOperator. Use numeric index.",
                            simple_predicate.column
                        )));
                    }
                };

                if column_index >= tuple.len() {
                    return Err(DbError::Internal(format!(
                        "Predicate column index {} out of bounds for tuple length {}",
                        column_index, tuple.len()
                    )));
                }

                let tuple_value = &tuple[column_index];
                let condition_value = &simple_predicate.value; // This is DataType

                match simple_predicate.operator.as_str() {
                    "=" => Ok(tuple_value == condition_value),
                    "!=" => Ok(tuple_value != condition_value),
                    ">" => match (tuple_value, condition_value) {
                        (DataType::Integer(a), DataType::Integer(b)) => Ok(a > b),
                        (DataType::Float(a), DataType::Float(b)) => Ok(a > b),
                        (DataType::String(a), DataType::String(b)) => Ok(a > b),
                        _ => Err(DbError::TypeError(format!("Cannot compare {:?} and {:?} with '>'", tuple_value, condition_value)))
                    },
                    "<" => match (tuple_value, condition_value) {
                        (DataType::Integer(a), DataType::Integer(b)) => Ok(a < b),
                        (DataType::Float(a), DataType::Float(b)) => Ok(a < b),
                        (DataType::String(a), DataType::String(b)) => Ok(a < b),
                        _ => Err(DbError::TypeError(format!("Cannot compare {:?} and {:?} with '<'", tuple_value, condition_value)))
                    },
                    _ => Err(DbError::NotImplemented(format!(
                        "Operator '{}' not implemented in FilterOperator.",
                        simple_predicate.operator
                    ))),
                }
            }
            _ => Err(DbError::NotImplemented("Complex expressions not yet supported in FilterOperator".to_string())),
        }
    }

    // Static version of evaluate_predicate for use in the closure
    fn static_evaluate_predicate(tuple: &Tuple, predicate: &Expression) -> Result<bool, DbError> {
        match predicate {
            Expression::Predicate(simple_predicate) => {
                let column_index = match simple_predicate.column.parse::<usize>() {
                    Ok(idx) => idx,
                    Err(_) => {
                        return Err(DbError::NotImplemented(format!(
                            "Column name resolution ('{}') not implemented. Use numeric index.",
                            simple_predicate.column
                        )));
                    }
                };

                if column_index >= tuple.len() {
                    return Err(DbError::Internal(format!(
                        "Predicate column index {} out of bounds.", column_index
                    )));
                }

                let tuple_value = &tuple[column_index];
                let condition_value = &simple_predicate.value;

                match simple_predicate.operator.as_str() {
                    "=" => Ok(tuple_value == condition_value),
                    "!=" => Ok(tuple_value != condition_value),
                    ">" => match (tuple_value, condition_value) {
                        (DataType::Integer(a), DataType::Integer(b)) => Ok(a > b),
                        (DataType::Float(a), DataType::Float(b)) => Ok(a > b),
                        (DataType::String(a), DataType::String(b)) => Ok(a > b),
                        _ => Err(DbError::TypeError("Type mismatch for '>' operator".into()))
                    },
                    "<" => match (tuple_value, condition_value) {
                        (DataType::Integer(a), DataType::Integer(b)) => Ok(a < b),
                        (DataType::Float(a), DataType::Float(b)) => Ok(a < b),
                        (DataType::String(a), DataType::String(b)) => Ok(a < b),
                        _ => Err(DbError::TypeError("Type mismatch for '<' operator".into()))
                    },
                    _ => Err(DbError::NotImplemented(format!("Operator '{}' not implemented.", simple_predicate.operator))),
                }
            }
            _ => Err(DbError::NotImplemented("Complex expressions not supported.".to_string())),
        }
    }
}

#[allow(dead_code)]
impl ExecutionOperator for FilterOperator {
    fn execute(&mut self) -> Result<Box<dyn Iterator<Item = Result<Tuple, DbError>> + Send + Sync>, DbError> {
        let input_iter = self.input.execute()?;
        let predicate_clone = self.predicate.clone();

        let iterator = input_iter.filter_map(move |tuple_result| {
            match tuple_result {
                Ok(tuple) => {
                    match FilterOperator::static_evaluate_predicate(&tuple, &predicate_clone) {
                        Ok(true) => Some(Ok(tuple)),
                        Ok(false) => None, // Filtered out
                        Err(e) => Some(Err(e)), // Error during predicate evaluation
                    }
                }
                Err(e) => Some(Err(e)), // Pass through errors from input operator
            }
        });

        Ok(Box::new(iterator))
    }
}

#[allow(dead_code)] // To be removed when used
pub struct IndexScanOperator<S: KeyValueStore<Key, Vec<u8>>> {
    store: Arc<S>,
    index_manager: Arc<IndexManager>,
    index_name: String,
    scan_value: Vec<u8>, // Value to lookup in the index (already serialized for HashIndex)
    snapshot_id: u64,
    committed_ids: Arc<HashSet<u64>>,
    executed: bool,
}

#[allow(dead_code)] // To be removed when used
impl<S: KeyValueStore<Key, Vec<u8>>> IndexScanOperator<S> {
    pub fn new(
        store: Arc<S>,
        index_manager: Arc<IndexManager>,
        index_name: String,
        scan_value: Vec<u8>, // This should be the serialized form of the DataType used for index lookup
        snapshot_id: u64,
        committed_ids: Arc<HashSet<u64>>
    ) -> Self {
        IndexScanOperator {
            store,
            index_manager,
            index_name,
            scan_value,
            snapshot_id,
            committed_ids,
            executed: false,
        }
    }
}

impl<S: KeyValueStore<Key, Vec<u8>> + 'static> ExecutionOperator for IndexScanOperator<S> {
    fn execute(&mut self) -> Result<Box<dyn Iterator<Item = Result<Tuple, DbError>> + Send + Sync>, DbError> {
        if self.executed {
            return Err(DbError::Internal("IndexScanOperator cannot be executed more than once".to_string()));
        }
        self.executed = true;

        let primary_keys = match self.index_manager.find_by_index(&self.index_name, &self.scan_value)? {
            Some(pks) => pks,
            None => Vec::new(), // No keys found for this index value
        };

        if primary_keys.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        let store_clone = Arc::clone(&self.store);
        let snapshot_id = self.snapshot_id;
        let committed_ids_clone = Arc::clone(&self.committed_ids);

        // Map primary keys to actual row data from the store
        let iterator = primary_keys.into_iter().filter_map(move |pk| {
            match store_clone.get(&pk, snapshot_id, &committed_ids_clone) {
                Ok(Some(value_bytes)) => {
                    // Value exists and is visible under current transaction snapshot
                    match deserialize_data_type(&value_bytes) {
                        Ok(data_type) => {
                            // Convert DataType to Tuple, similar to TableScanOperator
                            let tuple = match data_type {
                                DataType::Map(map_data) => {
                                    map_data.values().cloned().collect::<Vec<DataType>>()
                                }
                                DataType::JsonBlob(json_value) => { // Consistent handling with TableScan
                                    if json_value.is_object() {
                                        json_value.as_object().unwrap().values().map(|v| {
                                            DataType::String(v.to_string())
                                        }).collect::<Vec<DataType>>()
                                    } else {
                                        vec![DataType::JsonBlob(json_value)]
                                    }
                                }
                                single_val => vec![single_val],
                            };
                            Some(Ok(tuple))
                        }
                        Err(e) => Some(Err(DbError::SerializationError(format!("Failed to deserialize data during index scan for PK {:?}: {}", pk, e)))),
                    }
                }
                Ok(None) => None, // Value not found or not visible for this PK
                Err(e) => Some(Err(e)), // Store error for this PK
            }
        });

        Ok(Box::new(iterator))
    }
}
