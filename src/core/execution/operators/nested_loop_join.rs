use crate::core::common::error::DbError;
use crate::core::execution::{ExecutionOperator, Tuple};
use crate::core::optimizer::JoinPredicate;
use std::sync::Arc;
// Removed KeyValueStore, IndexManager, deserialize_data_type, Key, HashSet from this file's direct imports
// as NestedLoopJoinOperator itself doesn't directly use them. Its inputs might.

// Define the actual iterator struct to be returned by NestedLoopJoinOperator::execute
struct NestedLoopJoinIteratorInternal {
    left_input_iter: Box<dyn Iterator<Item = Result<Tuple, DbError>> + Send + Sync>,
    join_predicate: Option<JoinPredicate>,
    current_left_tuple: Option<Tuple>,
    right_tuples_buffer: Arc<Vec<Tuple>>,
    current_right_buffer_iter: std::vec::IntoIter<Tuple>,
}

impl NestedLoopJoinIteratorInternal {
    fn evaluate_join_predicate(&self, left_tuple: &Tuple, right_tuple: &Tuple) -> Result<bool, DbError> {
         if let Some(ref predicate) = self.join_predicate {
            let left_col_idx = predicate.left_column.parse::<usize>().map_err(|_| DbError::Internal(format!("Invalid left column index: {}", predicate.left_column)))?;
            let right_col_idx = predicate.right_column.parse::<usize>().map_err(|_| DbError::Internal(format!("Invalid right column index: {}", predicate.right_column)))?;

            if left_col_idx >= left_tuple.len() || right_col_idx >= right_tuple.len() {
                return Err(DbError::Internal("Join predicate column index out of bounds.".to_string()));
            }
            Ok(left_tuple[left_col_idx] == right_tuple[right_col_idx])
        } else {
            Ok(true)
        }
    }
}

impl Iterator for NestedLoopJoinIteratorInternal {
    type Item = Result<Tuple, DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_left_tuple.is_none() {
                match self.left_input_iter.next() {
                    Some(Ok(left_tuple)) => {
                        self.current_left_tuple = Some(left_tuple);
                        self.current_right_buffer_iter = Arc::clone(&self.right_tuples_buffer).as_ref().clone().into_iter();
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    None => return None,
                }
            }

            let left_tuple = self.current_left_tuple.as_ref().unwrap();

            while let Some(right_tuple) = self.current_right_buffer_iter.next() {
                match self.evaluate_join_predicate(left_tuple, &right_tuple) {
                    Ok(true) => {
                        let mut joined_tuple = left_tuple.clone();
                        joined_tuple.extend(right_tuple.clone());
                        return Some(Ok(joined_tuple));
                    }
                    Ok(false) => continue,
                    Err(e) => return Some(Err(e)),
                }
            }
            self.current_left_tuple = None;
        }
    }
}

pub struct NestedLoopJoinOperator {
    left_input: Box<dyn ExecutionOperator + Send + Sync>,
    right_input: Box<dyn ExecutionOperator + Send + Sync>,
    join_predicate: Option<JoinPredicate>,
    right_tuples_buffer: Option<Vec<Tuple>>,
}

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
            right_tuples_buffer: None,
        }
    }
}

impl ExecutionOperator for NestedLoopJoinOperator {
    fn execute(&mut self) -> Result<Box<dyn Iterator<Item = Result<Tuple, DbError>> + Send + Sync>, DbError> {
        if self.right_tuples_buffer.is_none() {
            let mut right_buffer_for_iter = Vec::new();
            let right_exec = self.right_input.execute()?;
            for item_res in right_exec {
                right_buffer_for_iter.push(item_res?);
            }
            self.right_tuples_buffer = Some(right_buffer_for_iter);
        }

        let left_iter = self.left_input.execute()?;

        let right_buffer_cloned_for_iter = Arc::new(
            self.right_tuples_buffer.as_ref()
                .ok_or_else(|| DbError::Internal("Right buffer not loaded in NLJ".to_string()))?
                .clone()
        );

        let iter = NestedLoopJoinIteratorInternal {
            left_input_iter: left_iter,
            join_predicate: self.join_predicate.clone(),
            current_left_tuple: None,
            current_right_buffer_iter: Arc::clone(&right_buffer_cloned_for_iter).as_ref().clone().into_iter(),
            right_tuples_buffer: right_buffer_cloned_for_iter,
        };

        Ok(Box::new(iter))
    }
}
