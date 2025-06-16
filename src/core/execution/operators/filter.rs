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
        // Changed DbError to OxidbError
        match predicate {
            Expression::Predicate(simple_predicate) => {
                let column_index =
                    match simple_predicate.column.parse::<usize>() {
                        Ok(idx) => idx,
                        Err(_) => {
                            return Err(OxidbError::NotImplemented{feature: format!( // Changed
                            "Column name resolution ('{}') not implemented. Use numeric index.",
                            simple_predicate.column
                        )});
                        }
                    };

                if column_index >= tuple.len() {
                    return Err(OxidbError::Internal(format!(
                        // Changed
                        "Predicate column index {} out of bounds.",
                        column_index
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
                        _ => Err(OxidbError::Type("Type mismatch for '>' operator".into())), // Changed
                    },
                    "<" => match (tuple_value, condition_value) {
                        (DataType::Integer(a), DataType::Integer(b)) => Ok(a < b),
                        (DataType::Float(a), DataType::Float(b)) => Ok(a < b),
                        (DataType::String(a), DataType::String(b)) => Ok(a < b),
                        _ => Err(OxidbError::Type("Type mismatch for '<' operator".into())), // Changed
                    },
                    _ => Err(OxidbError::NotImplemented {
                        feature: format!(
                            // Changed
                            "Operator '{}' not implemented.",
                            simple_predicate.operator
                        ),
                    }),
                }
            } // Since Expression only has one variant (Predicate), this match is exhaustive.
              // If other Expression variants are added, this match will need to be updated.
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
