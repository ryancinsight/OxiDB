use crate::core::common::OxidbError;
use crate::core::execution::{ExecutionOperator, Tuple};
use crate::core::optimizer::Expression;
use crate::core::types::DataType;

pub struct FilterOperator {
    /// The input operator that provides tuples.
    input: Box<dyn ExecutionOperator + Send + Sync>,
    /// The expression used to filter tuples.
    predicate: Expression,
}

impl FilterOperator {
    pub fn new(input: Box<dyn ExecutionOperator + Send + Sync>, predicate: Expression) -> Self {
        FilterOperator { input, predicate }
    }

    // Static version of evaluate_predicate for use in the closure
    /// Evaluates a predicate against a tuple.
    ///
    /// This is a static method used internally by the `FilterOperator`.
    ///
    /// # Arguments
    /// * `tuple` - The tuple to evaluate the predicate against.
    /// * `predicate` - The expression representing the predicate.
    ///
    /// # Returns
    /// * `Ok(true)` if the predicate evaluates to true for the tuple.
    /// * `Ok(false)` if the predicate evaluates to false for the tuple.
    /// * `Err(OxidbError)` if an error occurs during evaluation (e.g., type mismatch, unimplemented operator).
    fn static_evaluate_predicate(
        tuple: &Tuple,
        predicate: &Expression,
    ) -> Result<bool, OxidbError> {
        match predicate {
            Expression::CompareOp { left, op, right } => {
                // Assumption: left is Column, right is Literal for now, to match old logic.
                // A full expression evaluation system would be needed for arbitrary expressions.
                let left_val = match &**left {
                    Expression::Column(col_name) => {
                        let column_index = match col_name.parse::<usize>() {
                            Ok(idx) => idx,
                            Err(_) => {
                                return Err(OxidbError::NotImplemented {
                                    feature: format!(
                                        "Column name resolution ('{}') not implemented. Use numeric index.",
                                        col_name
                                    ),
                                });
                            }
                        };
                        if column_index >= tuple.len() {
                            return Err(OxidbError::Internal(format!(
                                "Predicate column index {} out of bounds.",
                                column_index
                            )));
                        }
                        &tuple[column_index]
                    }
                    Expression::Literal(val) => val, // Allow literal on left side
                    _ => return Err(OxidbError::NotImplemented {
                        feature: "Complex expressions in left side of CompareOp not supported yet".to_string(),
                    }),
                };

                let right_val = match &**right {
                    Expression::Literal(val) => val,
                    Expression::Column(col_name) => { // Allow column on right side
                        let column_index = match col_name.parse::<usize>() {
                            Ok(idx) => idx,
                            Err(_) => {
                                return Err(OxidbError::NotImplemented {
                                    feature: format!(
                                        "Column name resolution ('{}') not implemented. Use numeric index.",
                                        col_name
                                    ),
                                });
                            }
                        };
                        if column_index >= tuple.len() {
                            return Err(OxidbError::Internal(format!(
                                "Predicate column index {} out of bounds.",
                                column_index
                            )));
                        }
                        &tuple[column_index]
                    }
                    _ => return Err(OxidbError::NotImplemented {
                        feature: "Complex expressions in right side of CompareOp not supported yet".to_string(),
                    }),
                };

                match op.as_str() {
                    "=" => Ok(left_val == right_val),
                    "!=" => Ok(left_val != right_val),
                    ">" => match (left_val, right_val) {
                        (DataType::Integer(a), DataType::Integer(b)) => Ok(a > b),
                        (DataType::Float(a), DataType::Float(b)) => Ok(a > b),
                        (DataType::String(a), DataType::String(b)) => Ok(a > b),
                        _ => Err(OxidbError::Type("Type mismatch for '>' operator".into())),
                    },
                    "<" => match (left_val, right_val) {
                        (DataType::Integer(a), DataType::Integer(b)) => Ok(a < b),
                        (DataType::Float(a), DataType::Float(b)) => Ok(a < b),
                        (DataType::String(a), DataType::String(b)) => Ok(a < b),
                        _ => Err(OxidbError::Type("Type mismatch for '<' operator".into())),
                    },
                    // TODO: Add other operators like ">=", "<=", "AND", "OR" etc.
                    // For "AND", "OR", the structure of CompareOp might not be appropriate,
                    // and a more general Expression::BinaryOp might be used.
                    _ => Err(OxidbError::NotImplemented {
                        feature: format!("Operator '{}' not implemented.", op),
                    }),
                }
            }
            // If other Expression variants (Literal, Column, BinaryOp) can be predicates:
            Expression::Literal(DataType::Boolean(b)) => Ok(*b), // e.g. WHERE true
            _ => Err(OxidbError::NotImplemented {
                feature: "This type of expression is not supported as a predicate yet".to_string(),
            }),
        }
    }
}

impl ExecutionOperator for FilterOperator {
    fn execute(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Result<Tuple, OxidbError>> + Send + Sync>, OxidbError> {
        // Changed DbError to OxidbError
        let input_iter = self.input.execute()?;
        let predicate_clone = self.predicate.clone();

        let iterator = input_iter.filter_map(move |tuple_result| match tuple_result {
            Ok(tuple) => {
                match FilterOperator::static_evaluate_predicate(&tuple, &predicate_clone) {
                    Ok(true) => Some(Ok(tuple)),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                }
            }
            Err(e) => Some(Err(e)),
        });

        Ok(Box::new(iterator))
    }
}
