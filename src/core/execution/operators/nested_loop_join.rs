use crate::core::common::OxidbError;
use crate::core::execution::{ExecutionOperator, Tuple};
use crate::core::optimizer::JoinPredicate;
use std::sync::Arc;
// Removed KeyValueStore, IndexManager, deserialize_data_type, Key, HashSet from this file's direct imports
// as NestedLoopJoinOperator itself doesn't directly use them. Its inputs might.

// Define the actual iterator struct to be returned by NestedLoopJoinOperator::execute
/// Internal iterator for `NestedLoopJoinOperator`.
///
/// This struct manages the state of the nested loop join iteration,
/// including the current left tuple, the buffer of right tuples, and the join predicate.
struct NestedLoopJoinIteratorInternal {
    /// Iterator for the left input of the join.
    left_input_iter: Box<dyn Iterator<Item = Result<Tuple, OxidbError>> + Send + Sync>, // Changed
    /// The join predicate, if any.
    join_predicate: Option<JoinPredicate>,
    /// The current tuple from the left input.
    current_left_tuple: Option<Tuple>,
    /// A buffer holding all tuples from the right input.
    right_tuples_buffer: Arc<Vec<Tuple>>,
    /// An iterator over the `right_tuples_buffer`.
    current_right_buffer_iter: std::vec::IntoIter<Tuple>,
}

impl NestedLoopJoinIteratorInternal {
    /// Evaluates the join predicate for a given pair of left and right tuples.
    ///
    /// # Arguments
    /// * `left_tuple` - The tuple from the left input.
    /// * `right_tuple` - The tuple from the right input.
    ///
    /// # Returns
    /// * `Ok(true)` if the join predicate evaluates to true or if there is no predicate.
    /// * `Ok(false)` if the join predicate evaluates to false.
    /// * `Err(OxidbError)` if an error occurs during evaluation (e.g., invalid column index).
    fn evaluate_join_predicate(
        &self,
        left_tuple: &Tuple,
        right_tuple: &Tuple,
    ) -> Result<bool, OxidbError> {
        // Changed
        if let Some(ref predicate) = self.join_predicate {
            let left_col_idx = predicate.left_column.parse::<usize>().map_err(|_| {
                OxidbError::Internal(format!(
                    "Invalid left column index: {}",
                    predicate.left_column
                )) // Changed
            })?;
            let right_col_idx = predicate.right_column.parse::<usize>().map_err(|_| {
                OxidbError::Internal(format!(
                    "Invalid right column index: {}",
                    predicate.right_column
                )) // Changed
            })?;

            if left_col_idx >= left_tuple.len() || right_col_idx >= right_tuple.len() {
                return Err(OxidbError::Internal(
                    // Changed
                    "Join predicate column index out of bounds.".to_string(),
                ));
            }
            Ok(left_tuple[left_col_idx] == right_tuple[right_col_idx])
        } else {
            Ok(true)
        }
    }
}

impl Iterator for NestedLoopJoinIteratorInternal {
    type Item = Result<Tuple, OxidbError>; // Changed

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_left_tuple.is_none() {
                match self.left_input_iter.next() {
                    Some(Ok(left_tuple)) => {
                        self.current_left_tuple = Some(left_tuple);
                        self.current_right_buffer_iter =
                            Arc::clone(&self.right_tuples_buffer).as_ref().clone().into_iter();
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    None => return None,
                }
            }

            #[allow(clippy::unwrap_used)] // Logic ensures current_left_tuple is Some here
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
    /// The left input operator for the join.
    left_input: Box<dyn ExecutionOperator + Send + Sync>,
    /// The right input operator for the join.
    right_input: Box<dyn ExecutionOperator + Send + Sync>,
    /// The join predicate, if any.
    join_predicate: Option<JoinPredicate>,
    /// A buffer for tuples from the right input, populated on first execution.
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
    fn execute(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Result<Tuple, OxidbError>> + Send + Sync>, OxidbError> {
        // Changed
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
            self.right_tuples_buffer
                .as_ref()
                .ok_or_else(|| OxidbError::Internal("Right buffer not loaded in NLJ".to_string()))? // Changed
                .clone(),
        );

        let iter = NestedLoopJoinIteratorInternal {
            left_input_iter: left_iter,
            join_predicate: self.join_predicate.clone(),
            current_left_tuple: None,
            current_right_buffer_iter: Arc::clone(&right_buffer_cloned_for_iter)
                .as_ref()
                .clone()
                .into_iter(),
            right_tuples_buffer: right_buffer_cloned_for_iter,
        };

        Ok(Box::new(iter))
    }
}
