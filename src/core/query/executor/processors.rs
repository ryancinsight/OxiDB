// src/core/query/executor/processors.rs

use crate::core::common::OxidbError;
use crate::core::query::commands::Command;
use crate::core::query::executor::{ExecutionResult, QueryExecutor};
use crate::core::storage::engine::traits::KeyValueStore;
use crate::core::types::DataType;
use uuid; // Added for Uuid::new_v4()

/// The `CommandProcessor` trait defines the interface for processing a specific command.
pub trait CommandProcessor<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> {
    /// Processes the command using the provided `QueryExecutor`.
    fn process(&self, executor: &mut QueryExecutor<S>) -> Result<ExecutionResult, OxidbError>;
}

// Implementation of CommandProcessor for the Command enum itself
impl<S: KeyValueStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static> CommandProcessor<S> for Command {
    fn process(&self, executor: &mut QueryExecutor<S>) -> Result<ExecutionResult, OxidbError> {
        match self {
            Self::BeginTransaction => executor.handle_begin_transaction(),
            Self::CommitTransaction => executor.handle_commit_transaction(),
            Self::RollbackTransaction => executor.handle_rollback_transaction(),
            Self::Vacuum => executor.handle_vacuum(),
            Self::Select { columns, source, condition, order_by: _order_by, limit: _limit } => {
                // Updated pattern
                // TODO: Pass order_by and limit to handle_select
                executor.handle_select(columns.clone(), source.clone(), condition.clone())
            }
            Self::Update { source, assignments, condition } => {
                executor.handle_update(source.clone(), assignments.clone(), condition.clone())
            }
            Self::CreateTable { table_name, columns } => {
                // Call the actual DDL handler in QueryExecutor
                executor.handle_create_table(table_name.clone(), columns.clone())
            }
            Self::SqlInsert { table_name, columns: insert_columns_opt, values } => {
                let schema_arc = executor.get_table_schema(table_name)?.ok_or_else(|| {
                    OxidbError::Execution(format!("Table '{table_name}' not found."))
                })?;
                let schema = schema_arc.as_ref();

                let current_op_tx_id = executor
                    .transaction_manager
                    .current_active_transaction_id()
                    .unwrap_or(crate::core::common::types::TransactionId(0));

                for row_values_to_insert in values {
                    let mut row_map_data = std::collections::HashMap::new();
                    let mut pk_value_opt: Option<DataType> = None;
                    let mut pk_col_name_opt: Option<String> = None;

                    // Populate row_map_data based on provided columns or schema order
                    if let Some(insert_column_names) = insert_columns_opt {
                        if insert_column_names.len() != row_values_to_insert.len() {
                            return Err(OxidbError::Execution(
                                "Column count does not match value count for INSERT.".to_string(),
                            ));
                        }
                        for (col_name, value) in insert_column_names.iter().zip(row_values_to_insert.iter()) {
                            row_map_data.insert(
                                col_name.as_bytes().to_vec(),
                                value.clone(),
                            );
                        }
                    } else {
                        if schema.columns.len() != row_values_to_insert.len() {
                            return Err(OxidbError::Execution(
                                "Column count does not match value count for INSERT (schema order).".to_string()
                            ));
                        }
                        for (col_def, value) in schema.columns.iter().zip(row_values_to_insert.iter()) {
                            row_map_data.insert(
                                col_def.name.as_bytes().to_vec(),
                                value.clone(),
                            );
                        }
                    }

                    // Auto-increment processing: Generate values for auto-increment columns that are NULL or missing
                    for col_def in &schema.columns {
                        if col_def.is_auto_increment {
                            let current_value = row_map_data
                                .get(col_def.name.as_bytes())
                                .cloned()
                                .unwrap_or(DataType::Null);

                            // Only generate auto-increment value if no explicit value was provided (NULL or missing)
                            if current_value == DataType::Null {
                                let next_id = executor
                                    .get_next_auto_increment_value(table_name, &col_def.name);
                                let auto_value = match col_def.data_type {
                                    DataType::Integer(_) => DataType::Integer(next_id),
                                    _ => return Err(OxidbError::Execution(
                                        format!("AUTOINCREMENT is only supported for INTEGER columns, but column '{}' is {:?}",
                                               col_def.name, col_def.data_type)
                                    )),
                                };
                                row_map_data.insert(col_def.name.as_bytes().to_vec(), auto_value);
                            }
                        }
                    }

                    // Constraint Checks
                    for col_def in &schema.columns {
                        let value_for_column = row_map_data
                            .get(col_def.name.as_bytes())
                            .cloned()
                            .unwrap_or(DataType::Null); // Treat missing columns in map as Null for constraint checks

                        // NOT NULL Check
                        if !col_def.is_nullable && value_for_column == DataType::Null {
                            return Err(OxidbError::ConstraintViolation(format!(
                                "NOT NULL constraint failed for column '{}' in table '{}'",
                                col_def.name, table_name
                            )));
                        }

                        // UNIQUE / PRIMARY KEY Uniqueness Check
                        if col_def.is_unique {
                            // is_primary_key implies is_unique (set during translation)
                            if value_for_column == DataType::Null && !col_def.is_primary_key {
                                // Standard SQL: Multiple NULLs allowed in UNIQUE column, but not for PK.
                                // PK nullability is already handled by is_nullable = false for PKs.
                            } else {
                                // For INSERT, current_row_pk_bytes is None as there's no "current row" yet.
                                executor.check_uniqueness(
                                    table_name,
                                    col_def,
                                    &value_for_column,
                                    None, // No existing row's PK to exclude for INSERT
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
                    let kv_key = if let (Some(DataType::String(pk_str_val)), Some(ref pk_c_name)) =
                        (&pk_value_opt, &pk_col_name_opt)
                    {
                        if pk_c_name == "_kv_key" {
                            // Special convention: if PK column is named _kv_key and is String, use its value directly.
                            pk_str_val.as_bytes().to_vec()
                        } else {
                            // Standard PK-based key generation
                            format!(
                                "{}_pk_{}_{:?}",
                                table_name,
                                pk_c_name,
                                pk_value_opt.as_ref().ok_or_else(|| OxidbError::Internal(
                                    "PK value expected but not found for key generation"
                                        .to_string()
                                ))?
                            )
                            .replace("Integer(", "")
                            .replace("String(\"", "")
                            .replace("\")", "")
                            .replace(')', "")
                            .into_bytes()
                        }
                    } else if let (Some(pk_val), Some(pk_c_name)) =
                        (&pk_value_opt, &pk_col_name_opt)
                    {
                        // Standard PK-based key generation for non-string PKs or different PK col name
                        format!("{table_name}_pk_{pk_c_name}_{pk_val:?}")
                            .replace("Integer(", "")
                            .replace("String(\"", "")
                            .replace("\")", "")
                            .replace(')', "")
                            .into_bytes()
                    } else {
                        // Fallback to UUID if no PK or complex PK (not yet supported for keying)
                        format!("{}_{}", table_name, uuid::Uuid::new_v4()).into_bytes()
                    };

                    let row_data_type =
                        DataType::Map(crate::core::types::JsonSafeMap(row_map_data.clone())); // Clone row_map_data for handle_insert

                    // --- Start: Per-column index updates ---
                    for col_def in &schema.columns {
                        if col_def.is_primary_key || col_def.is_unique {
                            let value_for_column = row_map_data
                                .get(col_def.name.as_bytes())
                                .cloned()
                                .unwrap_or(DataType::Null);

                            if value_for_column == DataType::Null && !col_def.is_primary_key {
                                // Skip indexing NULLs for non-primary key unique columns
                                continue;
                            }

                            let index_name = format!("idx_{}_{}", table_name, col_def.name);
                            let serialized_column_value =
                                crate::core::common::serialization::serialize_data_type(
                                    &value_for_column,
                                )?;

                            // Insert into the specific column index
                            executor
                                .index_manager
                                .write()
                                .map_err(|e| OxidbError::LockTimeout(format!("Failed to acquire write lock on index manager for insert: {e}")))?
                                .insert_into_index(
                                    &index_name,
                                    &serialized_column_value,
                                    &kv_key,
                                )?;

                            // Add undo log for this index insertion
                            if current_op_tx_id.0 != 0 {
                                // Only if in an active transaction
                                if let Some(active_tx_mut) =
                                    executor.transaction_manager.get_active_transaction_mut()
                                {
                                    active_tx_mut.add_undo_operation(
                                        crate::core::transaction::UndoOperation::IndexRevertInsert { // Adjusted path
                                            index_name, // Moves index_name
                                            key: kv_key.clone(), // Primary key of the row
                                            value_for_index: serialized_column_value, // Serialized value of the indexed column
                                        },
                                    );
                                }
                            }
                        }
                    }
                    // --- End: Per-column index updates ---

                    // Use helper method for storage operation (DRY principle)
                    executor.store_row_data(kv_key.clone(), &row_data_type)?;
                }
                Ok(ExecutionResult::Updated { count: values.len() }) // Return rows affected
            }
            Self::SqlDelete { table_name, condition } => {
                executor.handle_sql_delete(table_name.clone(), condition.clone())
            }
            Self::SimilaritySearch {
                table_name: _table_name,
                vector_column_name: _vector_column_name,
                query_vector: _query_vector,
                top_k: _top_k,
            } => {
                // TODO: Implement actual call to an executor method for similarity search
                // For now, using a placeholder. This will likely involve calling a new method on `executor`.
                // executor.handle_similarity_search(table_name.clone(), vector_column_name.clone(), query_vector.clone(), *top_k)
                Err(OxidbError::NotImplemented {
                    feature: "SimilaritySearch command processing".to_string(),
                })
            }
            Self::DropTable { table_name: _table_name, if_exists: _if_exists } => {
                // TODO: Implement actual call to an executor method for drop table
                // executor.handle_drop_table(table_name.clone(), *if_exists)
                Err(OxidbError::NotImplemented {
                    feature: "DropTable command processing".to_string(),
                })
            }
            Self::ParameterizedSql { statement, parameters } => {
                // Handle parameterized SQL execution
                executor.execute_parameterized_statement(statement, parameters)
            }
            // Added legacy KV and index variants delegating to core handlers
            Self::Insert { key, value } => executor.handle_insert(key.clone(), value.clone()),
            Self::Get { key } => executor.handle_get(key.clone()),
            Self::Delete { key } => executor.handle_delete(key.clone()),
            Self::FindByIndex { index_name, value } => executor.handle_find_by_index(index_name.clone(), value.clone()),
        }
    }
}
