use crate::core::common::OxidbError;
use crate::core::execution::{ExecutionOperator, Tuple};
use crate::core::query::sql::ast::AggregateFunction;
use crate::core::types::DataType;
use std::collections::HashMap;

pub struct AggregateOperator {
    input: Box<dyn ExecutionOperator + Send + Sync>,
    aggregates: Vec<AggregateSpec>,
    group_by_indices: Vec<usize>,
}

#[derive(Clone)]
pub struct AggregateSpec {
    pub function: AggregateFunction,
    pub column_index: Option<usize>, // None for COUNT(*)
    pub alias: Option<String>,
}

impl AggregateOperator {
    pub fn new(
        input: Box<dyn ExecutionOperator + Send + Sync>,
        aggregates: Vec<AggregateSpec>,
        group_by_indices: Vec<usize>,
    ) -> Self {
        Self {
            input,
            aggregates,
            group_by_indices,
        }
    }
}

impl ExecutionOperator for AggregateOperator {
    fn execute(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Result<Tuple, OxidbError>> + Send + Sync>, OxidbError> {
        let input_iter = self.input.execute()?;
        let aggregates = self.aggregates.clone();
        let group_by_indices = self.group_by_indices.clone();
        
        // Collect all rows to compute aggregates
        let rows: Result<Vec<Tuple>, OxidbError> = input_iter.collect();
        let rows = rows?;
        
        if group_by_indices.is_empty() {
            // No GROUP BY - single result row
            let result = compute_aggregates_no_group(&rows, &aggregates)?;
            Ok(Box::new(std::iter::once(Ok(result))))
        } else {
            // GROUP BY - group rows and compute aggregates per group
            let grouped_results = compute_aggregates_with_group(&rows, &aggregates, &group_by_indices)?;
            Ok(Box::new(grouped_results.into_iter().map(Ok)))
        }
    }
}

fn compute_aggregates_no_group(
    rows: &[Tuple],
    aggregates: &[AggregateSpec],
) -> Result<Tuple, OxidbError> {
    let mut result = Vec::new();
    
    for agg in aggregates {
        let value = match agg.function {
            AggregateFunction::Count => {
                if agg.column_index.is_none() {
                    // COUNT(*)
                    DataType::Integer(rows.len() as i64)
                } else {
                    // COUNT(column) - count non-null values
                    let col_idx = agg.column_index.unwrap();
                    let count = rows.iter()
                        .filter(|row| {
                            row.get(col_idx)
                                .map(|v| !matches!(v, DataType::Null))
                                .unwrap_or(false)
                        })
                        .count();
                    DataType::Integer(count as i64)
                }
            }
            AggregateFunction::Sum => {
                if let Some(col_idx) = agg.column_index {
                    let sum = rows.iter()
                        .filter_map(|row| row.get(col_idx))
                        .try_fold(0.0f64, |acc, val| match val {
                            DataType::Integer(i) => Ok(acc + *i as f64),
                            DataType::Float(f) => Ok(acc + f.0),
                            DataType::Null => Ok(acc),
                            _ => Err(OxidbError::Type("SUM requires numeric values".to_string())),
                        })?;
                    DataType::Float(crate::core::types::OrderedFloat(sum))
                } else {
                    return Err(OxidbError::Execution("SUM requires a column".to_string()));
                }
            }
            AggregateFunction::Avg => {
                if let Some(col_idx) = agg.column_index {
                    let (sum, count) = rows.iter()
                        .filter_map(|row| row.get(col_idx))
                        .try_fold((0.0f64, 0usize), |(sum, count), val| match val {
                            DataType::Integer(i) => Ok((sum + *i as f64, count + 1)),
                            DataType::Float(f) => Ok((sum + f.0, count + 1)),
                            DataType::Null => Ok((sum, count)),
                            _ => Err(OxidbError::Type("AVG requires numeric values".to_string())),
                        })?;
                    if count > 0 {
                        DataType::Float(crate::core::types::OrderedFloat(sum / count as f64))
                    } else {
                        DataType::Null
                    }
                } else {
                    return Err(OxidbError::Execution("AVG requires a column".to_string()));
                }
            }
            AggregateFunction::Min => {
                if let Some(col_idx) = agg.column_index {
                    let mut min_value: Option<DataType> = None;
                    for row in rows {
                        if let Some(val) = row.get(col_idx) {
                            if !matches!(val, DataType::Null) {
                                match &min_value {
                                    None => min_value = Some(val.clone()),
                                    Some(current_min) => {
                                        if val < current_min {
                                            min_value = Some(val.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    min_value.unwrap_or(DataType::Null)
                } else {
                    return Err(OxidbError::Execution("MIN requires a column".to_string()));
                }
            }
            AggregateFunction::Max => {
                if let Some(col_idx) = agg.column_index {
                    let mut max_value: Option<DataType> = None;
                    for row in rows {
                        if let Some(val) = row.get(col_idx) {
                            if !matches!(val, DataType::Null) {
                                match &max_value {
                                    None => max_value = Some(val.clone()),
                                    Some(current_max) => {
                                        if val > current_max {
                                            max_value = Some(val.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    max_value.unwrap_or(DataType::Null)
                } else {
                    return Err(OxidbError::Execution("MAX requires a column".to_string()));
                }
            }
        };
        result.push(value);
    }
    
    Ok(result)
}

fn compute_aggregates_with_group(
    rows: &[Tuple],
    aggregates: &[AggregateSpec],
    group_by_indices: &[usize],
) -> Result<Vec<Tuple>, OxidbError> {
    // Group rows by the group_by columns
    let mut groups: HashMap<Vec<DataType>, Vec<&Tuple>> = HashMap::new();
    
    for row in rows {
        let key: Vec<DataType> = group_by_indices.iter()
            .filter_map(|&idx| row.get(idx).cloned())
            .collect();
        groups.entry(key).or_default().push(row);
    }
    
    // Compute aggregates for each group
    let mut results = Vec::new();
    for (group_key, group_rows) in groups {
        let mut result_row = group_key;
        
        // Convert group_rows from Vec<&Tuple> to Vec<Tuple> for compute_aggregates_no_group
        let group_rows_owned: Vec<Tuple> = group_rows.into_iter().cloned().collect();
        let agg_values = compute_aggregates_no_group(&group_rows_owned, aggregates)?;
        
        result_row.extend(agg_values);
        results.push(result_row);
    }
    
    Ok(results)
}