use crate::core::common::OxidbError;
use crate::core::execution::{ExecutionOperator, Tuple};
use crate::core::optimizer::Expression;
use crate::core::types::{DataType, schema::Schema};
use std::borrow::Cow;
use std::sync::Arc;

pub struct FilterOperator {
    /// The input operator that provides tuples.
    input: Box<dyn ExecutionOperator + Send + Sync>,
    /// The expression used to filter tuples.
    predicate: Expression,
    /// The schema of the input tuples, used to resolve column names to indices
    schema: Option<Arc<Schema>>,
}

impl FilterOperator {
    #[must_use]
    pub fn new(input: Box<dyn ExecutionOperator + Send + Sync>, predicate: Expression) -> Self {
        Self { input, predicate, schema: None }
    }
    
    #[must_use]
    pub fn with_schema(input: Box<dyn ExecutionOperator + Send + Sync>, predicate: Expression, schema: Arc<Schema>) -> Self {
        Self { input, predicate, schema: Some(schema) }
    }

    // Static version of evaluate_predicate for use in the closure
    /// Evaluates a predicate against a tuple.
    ///
    /// This is a static method used internally by the `FilterOperator`.
    ///
    /// # Arguments
    /// * `tuple` - The tuple to evaluate the predicate against.
    /// * `predicate` - The expression representing the predicate.
    /// * `schema` - Optional schema to resolve column names to indices.
    ///
    /// # Returns
    /// * `Ok(true)` if the predicate evaluates to true for the tuple.
    /// * `Ok(false)` if the predicate evaluates to false for the tuple.
    /// * `Err(OxidbError)` if an error occurs during evaluation (e.g., type mismatch, unimplemented operator).
    fn static_evaluate_predicate(
        tuple: &Tuple,
        predicate: &Expression,
        schema: Option<&Arc<Schema>>,
    ) -> Result<bool, OxidbError> {
        match predicate {
            Expression::CompareOp { left, op, right } => {
                let left_val_cow = Self::evaluate_expression_to_datatype(tuple, left, schema)?;
                let right_val_cow = Self::evaluate_expression_to_datatype(tuple, right, schema)?;

                // Dereference Cow to get &DataType for comparison
                let left_val = &*left_val_cow;
                let right_val = &*right_val_cow;

                match op.as_str() {
                    "=" => Ok(left_val == right_val),
                    "!=" => Ok(left_val != right_val),
                    ">" => match (left_val, right_val) {
                        (DataType::Integer(a), DataType::Integer(b)) => Ok(a > b),
                        (DataType::Float(a), DataType::Float(b)) => Ok(a > b),
                        (DataType::String(a), DataType::String(b)) => Ok(a > b),
                        // Handle mixed numeric types
                        (DataType::Float(a), DataType::Integer(b)) => Ok(a.0 > *b as f64),
                        (DataType::Integer(a), DataType::Float(b)) => Ok(*a as f64 > b.0),
                        _ => Err(OxidbError::Type("Type mismatch for '>' operator".into())),
                    },
                    "<" => match (left_val, right_val) {
                        (DataType::Integer(a), DataType::Integer(b)) => Ok(a < b),
                        (DataType::Float(a), DataType::Float(b)) => Ok(a < b),
                        (DataType::String(a), DataType::String(b)) => Ok(a < b),
                        // Handle mixed numeric types
                        (DataType::Float(a), DataType::Integer(b)) => Ok(a.0 < *b as f64),
                        (DataType::Integer(a), DataType::Float(b)) => Ok((*a as f64) < b.0),
                        _ => Err(OxidbError::Type("Type mismatch for '<' operator".into())),
                    },
                    ">=" => match (left_val, right_val) {
                        (DataType::Integer(a), DataType::Integer(b)) => Ok(a >= b),
                        (DataType::Float(a), DataType::Float(b)) => Ok(a >= b),
                        (DataType::String(a), DataType::String(b)) => Ok(a >= b),
                        // Handle mixed numeric types
                        (DataType::Float(a), DataType::Integer(b)) => Ok(a.0 >= *b as f64),
                        (DataType::Integer(a), DataType::Float(b)) => Ok(*a as f64 >= b.0),
                        _ => Err(OxidbError::Type("Type mismatch for '>=' operator".into())),
                    },
                    "<=" => match (left_val, right_val) {
                        (DataType::Integer(a), DataType::Integer(b)) => Ok(a <= b),
                        (DataType::Float(a), DataType::Float(b)) => Ok(a <= b),
                        (DataType::String(a), DataType::String(b)) => Ok(a <= b),
                        // Handle mixed numeric types
                        (DataType::Float(a), DataType::Integer(b)) => Ok(a.0 <= *b as f64),
                        (DataType::Integer(a), DataType::Float(b)) => Ok(*a as f64 <= b.0),
                        _ => Err(OxidbError::Type("Type mismatch for '<=' operator".into())),
                    },
                    _ => Err(OxidbError::NotImplemented {
                        feature: format!("Operator '{op}' not implemented in CompareOp."),
                    }),
                }
            }
            // If other Expression variants (Literal, Column, BinaryOp) can be predicates:
            Expression::Literal(DataType::Boolean(b)) => Ok(*b), // e.g. WHERE true
            Expression::BinaryOp { left, op, right } => {
                // Evaluate left and right sub-expressions recursively
                let left_result = Self::static_evaluate_predicate(tuple, left, schema)?;

                // Short-circuit for AND and OR
                match op.as_str() {
                    "AND" => {
                        if !left_result {
                            return Ok(false); // Short-circuit if left is false
                        }
                        Self::static_evaluate_predicate(tuple, right, schema)
                    }
                    "OR" => {
                        if left_result {
                            return Ok(true); // Short-circuit if left is true
                        }
                        Self::static_evaluate_predicate(tuple, right, schema)
                    }
                    _ => Err(OxidbError::NotImplemented {
                        feature: format!("Logical operator '{op}' not implemented in BinaryOp."),
                    }),
                }
            }
            _ => Err(OxidbError::NotImplemented {
                feature: "This type of expression is not supported as a predicate yet for direct evaluation in filter".to_string(),
            }),
        }
    }

    /// Helper function to evaluate an expression to a concrete `DataType`.
    /// Supports Literal and Column expressions.
    /// For Column expressions, it attempts to resolve column names against a `DataType::Map`
    /// assumed to be the first element of the tuple.
    fn evaluate_expression_to_datatype<'a>(
        // Lifetime 'a tied to tuple
        tuple: &'a Tuple,
        expr: &Expression,
        schema: Option<&Arc<Schema>>,
    ) -> Result<Cow<'a, DataType>, OxidbError> {
        match expr {
            Expression::Literal(val) => Ok(Cow::Owned(val.clone())), // Literals are cloned
            Expression::Column(col_name) => {
                // First, try to resolve column name using schema if available
                if let Some(schema) = schema {
                    if let Some(column_index) = schema.get_column_index(col_name) {
                        if column_index >= tuple.len() {
                            return Err(OxidbError::Internal(format!(
                                "Column index {} (for column '{}') out of bounds for tuple with len {}.",
                                column_index,
                                col_name,
                                tuple.len()
                            )));
                        }
                        return Ok(Cow::Borrowed(&tuple[column_index]));
                    }
                }
                
                // If schema is not available or column not found, try parsing as numeric index
                if let Ok(column_index) = col_name.parse::<usize>() {
                    if column_index >= tuple.len() {
                        return Err(OxidbError::Internal(format!(
                            "Column index {} out of bounds for tuple with len {}.",
                            column_index,
                            tuple.len()
                        )));
                    }
                    Ok(Cow::Borrowed(&tuple[column_index]))
                } else {
                    // If not a usize and no schema available, assume it's a named column for a map.
                    // This is specific to how UPDATE works: the SELECT sub-query for UPDATE
                    // should yield full DataType::Map rows if filtering by name is intended.
                    // We assume the map is the first (and likely only) element in the tuple.
                    // If not a usize, assume it's a named column for a map.
                    // The TableScanOperator now produces: vec![key_data_type, row_data_type]
                    // So, the actual row data (e.g., a map) is at tuple[1].
                    if tuple.len() < 2 {
                        return Err(OxidbError::Internal(format!(
                            "Tuple too short ({}) for named column lookup ('{}'). Expected at least 2 elements (key, map).",
                            tuple.len(), col_name
                        )));
                    }
                    match &tuple[1] { // Check tuple[1] for the map
                        DataType::Map(map_data) => {
                            let key_bytes = col_name.as_bytes().to_vec();

                            // Direct lookup with raw bytes - this should work since INSERT stores column names as raw bytes
                            if let Some(data_type_value) = map_data.0.get(&key_bytes) {
                                return Ok(Cow::Borrowed(data_type_value));
                            }

                            // Debug: Print available keys to help diagnose the issue
                            let available_keys: Vec<String> = map_data.0.keys()
                                .map(|k| String::from_utf8_lossy(k).to_string())
                                .collect();

                            Err(OxidbError::InvalidInput { message: format!(
                                "Column '{col_name}' not found in map at tuple[1]. Available keys: {available_keys:?}"
                            )})
                        }
                        _ => Err(OxidbError::Type(format!(
                            "Expected DataType::Map at tuple[1] for named column lookup ('{}'), but found {:?}.",
                            col_name, tuple[1]
                        ))),
                    }
                }
            }
            _ => Err(OxidbError::NotImplemented {
                feature:
                    "Expression type not supported for direct DataType evaluation in predicate."
                        .to_string(),
            }),
        }
    }
}

impl ExecutionOperator for FilterOperator {
    fn execute(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Result<Tuple, OxidbError>> + Send + Sync>, OxidbError> {
        // Changed
        let input_iter = self.input.execute()?;
        let predicate_clone = self.predicate.clone();
        let schema_clone = self.schema.clone();

        let iterator = input_iter.filter_map(move |tuple_result| match tuple_result {
            Ok(tuple) => match Self::static_evaluate_predicate(&tuple, &predicate_clone, schema_clone.as_ref()) {
                Ok(true) => Some(Ok(tuple)),
                Ok(false) => None,
                Err(e) => Some(Err(e)),
            },
            Err(e) => Some(Err(e)),
        });
        Ok(Box::new(iterator))
    }
}

#[cfg(test)]
mod tests;
