use crate::core::common::error::DbError;
use crate::core::types::DataType;
use crate::core::execution::{ExecutionOperator, Tuple};
use crate::core::optimizer::Expression;

pub struct FilterOperator {
    input: Box<dyn ExecutionOperator + Send + Sync>,
    predicate: Expression,
}

impl FilterOperator {
    pub fn new(input: Box<dyn ExecutionOperator + Send + Sync>, predicate: Expression) -> Self {
        FilterOperator { input, predicate }
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
            // Since Expression only has one variant (Predicate), this match is exhaustive.
            // If other Expression variants are added, this match will need to be updated.
        }
    }
}

impl ExecutionOperator for FilterOperator {
    fn execute(&mut self) -> Result<Box<dyn Iterator<Item = Result<Tuple, DbError>> + Send + Sync>, DbError> {
        let input_iter = self.input.execute()?;
        let predicate_clone = self.predicate.clone();

        let iterator = input_iter.filter_map(move |tuple_result| {
            match tuple_result {
                Ok(tuple) => {
                    match FilterOperator::static_evaluate_predicate(&tuple, &predicate_clone) {
                        Ok(true) => Some(Ok(tuple)),
                        Ok(false) => None,
                        Err(e) => Some(Err(e)),
                    }
                }
                Err(e) => Some(Err(e)),
            }
        });

        Ok(Box::new(iterator))
    }
}
