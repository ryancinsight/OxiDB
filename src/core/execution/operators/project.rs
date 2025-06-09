use crate::core::common::OxidbError;
use crate::core::execution::{ExecutionOperator, Tuple};

pub struct ProjectOperator {
    input: Box<dyn ExecutionOperator + Send + Sync>,
    column_indices: Vec<usize>,
}

impl ProjectOperator {
    pub fn new(
        input: Box<dyn ExecutionOperator + Send + Sync>,
        column_indices: Vec<usize>,
    ) -> Self {
        ProjectOperator { input, column_indices }
    }
}

impl ExecutionOperator for ProjectOperator {
    fn execute(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Result<Tuple, OxidbError>> + Send + Sync>, OxidbError> { // Changed
        let input_iter = self.input.execute()?;

        if self.column_indices.is_empty() {
            // Special case: empty column_indices means project all (pass through)
            // This is a simplification for SELECT * where indices are not predetermined.
            Ok(Box::new(input_iter))
        } else {
            let indices_clone = self.column_indices.clone();
            let iterator = input_iter.map(move |tuple_result| {
                tuple_result.and_then(|tuple| {
                    let mut projected_tuple = Vec::with_capacity(indices_clone.len());
                    for &index in &indices_clone {
                        if index < tuple.len() {
                            projected_tuple.push(tuple[index].clone());
                        } else {
                            return Err(OxidbError::Internal(format!( // Changed
                                "Projection index {} out of bounds for tuple length {}",
                                index,
                                tuple.len()
                            )));
                        }
                    }
                    Ok(projected_tuple)
                })
            });
            Ok(Box::new(iterator))
        }
    }
}
