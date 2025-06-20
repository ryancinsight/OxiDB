// src/core/query/executor/processors.rs

use crate::core::common::OxidbError;
use crate::core::types::DataType;
use crate::core::query::commands::Command;
use uuid; // Added for Uuid::new_v4()
use crate::core::query::executor::{ExecutionResult, QueryExecutor};
use crate::core::storage::engine::traits::KeyValueStore;

/// The `CommandProcessor` trait defines the interface for processing a specific command.
pub trait CommandProcessor<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> {
    /// Processes the command using the provided QueryExecutor.
    fn process(&self, executor: &mut QueryExecutor<S>) -> Result<ExecutionResult, OxidbError>;
}

// Implementation of CommandProcessor for the Command enum itself
impl<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> CommandProcessor<S> for Command {
    fn process(&self, executor: &mut QueryExecutor<S>) -> Result<ExecutionResult, OxidbError> {
        match self {
            Command::Insert { key, value } => executor.handle_insert(key.clone(), value.clone()),
            Command::Get { key } => executor.handle_get(key.clone()),
            Command::Delete { key } => executor.handle_delete(key.clone()),
            Command::FindByIndex { index_name, value } => {
                executor.handle_find_by_index(index_name.clone(), value.clone())
            }
            Command::BeginTransaction => executor.handle_begin_transaction(),
            Command::CommitTransaction => executor.handle_commit_transaction(),
            Command::RollbackTransaction => executor.handle_rollback_transaction(),
            Command::Vacuum => executor.handle_vacuum(),
            Command::Select { columns, source, condition } => {
                executor.handle_select(columns.clone(), source.clone(), condition.clone())
            }
            Command::Update { source, assignments, condition } => {
                executor.handle_update(source.clone(), assignments.clone(), condition.clone())
            }
            Command::CreateTable { table_name, columns } => {
                // Call the actual DDL handler in QueryExecutor
                executor.handle_create_table(table_name.clone(), columns.clone())
            }
            Command::SqlInsert { table_name, columns: insert_columns_opt, values } => {
                let schema_arc = executor.get_table_schema(table_name)?
                    .ok_or_else(|| OxidbError::Execution(format!("Table '{}' not found.", table_name)))?;
                let schema = schema_arc.as_ref();

                let current_op_tx_id = executor.transaction_manager.current_active_transaction_id().unwrap_or(crate::core::common::types::TransactionId(0));
                let committed_ids_for_read: std::collections::HashSet<u64> = executor
                    .transaction_manager
                    .get_committed_tx_ids_snapshot()
                    .into_iter()
                    .map(|tx_id| tx_id.0)
                    .collect();

                for row_values_to_insert in values {
                    let mut row_map_data = std::collections::HashMap::new();
                    let mut pk_value_opt: Option<DataType> = None;
                    let mut pk_col_name_opt: Option<String> = None;

                    // Populate row_map_data based on provided columns or schema order
                    if let Some(insert_column_names) = insert_columns_opt {
                        if insert_column_names.len() != row_values_to_insert.len() {
                            return Err(OxidbError::Execution(
                                "Column count does not match value count for INSERT.".to_string()
                            ));
                        }
                        for (i, col_name) in insert_column_names.iter().enumerate() {
                            row_map_data.insert(col_name.as_bytes().to_vec(), row_values_to_insert[i].clone());
                        }
                    } else {
                        if schema.columns.len() != row_values_to_insert.len() {
                            return Err(OxidbError::Execution(
                                "Column count does not match value count for INSERT (schema order).".to_string()
                            ));
                        }
                        for (i, col_def) in schema.columns.iter().enumerate() {
                            row_map_data.insert(col_def.name.as_bytes().to_vec(), row_values_to_insert[i].clone());
                        }
                    }

                    // Constraint Checks
                    for col_def in &schema.columns {
                        let value_for_column = row_map_data.get(col_def.name.as_bytes())
                            .cloned()
                            .unwrap_or(DataType::Null); // Treat missing columns in map as Null for constraint checks

                        // NOT NULL Check
                        if !col_def.is_nullable && value_for_column == DataType::Null {
                            return Err(OxidbError::ConstraintViolation {
                                message: format!("NOT NULL constraint failed for column '{}' in table '{}'", col_def.name, table_name),
                            });
                        }

                        // UNIQUE / PRIMARY KEY Uniqueness Check
                        if col_def.is_unique { // is_primary_key implies is_unique (set during translation)
                            if value_for_column == DataType::Null && !col_def.is_primary_key {
                                // Standard SQL: Multiple NULLs allowed in UNIQUE column, but not for PK.
                                // PK nullability is already handled by is_nullable = false for PKs.
                            } else {
                                // For INSERT, current_row_pk_bytes is None as there's no "current row" yet.
                                executor.check_uniqueness(
                                    table_name,
                                    schema,
                                    col_def,
                                    &value_for_column,
                                    None, // No existing row's PK to exclude for INSERT
                                    current_op_tx_id.0,
                                    &committed_ids_for_read
                                )?;
                            }
                        }
                        if col_def.is_primary_key {
                            pk_value_opt = Some(value_for_column.clone());
                            pk_col_name_opt = Some(col_def.name.clone());
                        }
                    }

                    // Determine KV store key
                    // TODO: Handle composite PKs. For now, assume single PK or use UUID.
                    let kv_key = if let (Some(pk_val), Some(pk_name)) = (pk_value_opt, pk_col_name_opt) {
                        // Use a consistent format for PK-based keys
                        // This format needs to be used by SELECT/UPDATE/DELETE as well.
                        format!("{}_pk_{}_{:?}", table_name, pk_name, pk_val)
                            .replace("Integer(", "").replace("String(\"", "").replace("\")", "").replace(")", "") // Basic normalization
                            .into_bytes()
                    } else {
                        // Fallback to UUID if no PK or complex PK (not yet supported for keying)
                        format!("{}_{}", table_name, uuid::Uuid::new_v4().to_string()).into_bytes()
                    };

                    let row_data_type = DataType::Map(crate::core::types::JsonSafeMap(row_map_data));
                    executor.handle_insert(kv_key, row_data_type)?; // Call low-level KV insert
                }
                Ok(ExecutionResult::Success) // TODO: Return rows affected (values.len())
            }
            Command::SqlDelete { table_name, condition } => {
                executor.handle_sql_delete(table_name.clone(), condition.clone())
            }
        }
    }
}
