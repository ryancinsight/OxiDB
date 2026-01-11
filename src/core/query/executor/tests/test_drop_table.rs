#[cfg(test)]
mod tests {
    use crate::core::common::types::ColumnType;
    use crate::core::query::commands::Command;
    use crate::core::query::executor::*;
    use crate::core::storage::engine::SimpleFileKvStore;
    use crate::core::types::DataType;
    use tempfile::NamedTempFile;
    use crate::core::common::OxidbError;
    use crate::core::wal::writer::WalWriter;
    use std::sync::Arc;

    fn create_test_executor() -> (QueryExecutor<SimpleFileKvStore>, tempfile::TempDir, NamedTempFile) {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir for indexes");
        let index_path = temp_dir.path().to_path_buf();
        let temp_store_file = NamedTempFile::new().expect("Failed to create temp db file");
        let store_path = temp_store_file.path().to_path_buf();
        let temp_store = SimpleFileKvStore::new(&store_path).unwrap();
        let wal_config = crate::core::wal::writer::WalWriterConfig::default();
        let wal_writer = WalWriter::new(store_path.with_extension("tx_wal"), wal_config);
        let log_manager = Arc::new(crate::core::wal::log_manager::LogManager::new());
        let exec = QueryExecutor::new(temp_store, index_path, wal_writer, log_manager).unwrap();
        (exec, temp_dir, temp_store_file)
    }

    #[test]
    fn test_drop_table_successful() -> Result<(), OxidbError> {
        let (mut exec, _temp_dir, _store_file) = create_test_executor();

        // 1. Create Table
        let create = Command::CreateTable {
            table_name: "users".to_string(),
            columns: vec![
                crate::core::types::schema::ColumnDef {
                    name: "id".to_string(),
                    data_type: ColumnType::Integer,
                    is_nullable: false,
                    is_primary_key: true,
                    is_unique: true,
                    is_auto_increment: false,
                },
                crate::core::types::schema::ColumnDef {
                    name: "name".to_string(),
                    data_type: ColumnType::Text,
                    is_nullable: false,
                    is_primary_key: false,
                    is_unique: false,
                    is_auto_increment: false,
                },
            ],
        };
        exec.execute_command(create)?;

        // 2. Insert Data
        let insert = Command::SqlInsert {
            table_name: "users".to_string(),
            columns: Some(vec!["id".to_string(), "name".to_string()]),
            values: vec![
                vec![DataType::Integer(1), DataType::String("Alice".into())],
                vec![DataType::Integer(2), DataType::String("Bob".into())],
            ],
        };
        exec.execute_command(insert)?;

        // Verify data exists
        let select = Command::Select {
            columns: crate::core::query::commands::SelectColumnSpec::All,
            source: "users".to_string(),
            condition: None,
            order_by: None,
            limit: None,
        };
        let res = exec.execute_command(select)?;
        match res {
            ExecutionResult::Query { rows, .. } => assert_eq!(rows.len(), 2),
            ExecutionResult::RankedResults(results) => assert_eq!(results.len(), 2),
            _ => panic!("Expected Query or RankedResults, got {:?}", res),
        }

        // 3. Drop Table
        let drop = Command::DropTable {
            table_name: "users".to_string(),
            if_exists: false,
        };
        let result = exec.execute_command(drop)?;
        assert!(matches!(result, ExecutionResult::Success));

        // 4. Verify Table is Gone (Select should fail or return empty schema error)
        let select_after = Command::Select {
            columns: crate::core::query::commands::SelectColumnSpec::All,
            source: "users".to_string(),
            condition: None,
            order_by: None,
            limit: None,
        };
        // Expecting TableNotFound error implicitly or explicitly depending on Select implementation
        // Select calls get_table_schema inside.
        let result_select = exec.execute_command(select_after);
        assert!(result_select.is_err(), "Expected error after dropping table, got Ok({:?})", result_select.unwrap()); // TableNotFound

        // 5. Verify Index Gone (Optional, check files)
        // Accessing internal index manager is hard here, but we can try creating the table again
        // and see if index creation warns or fails?
        // Or trust that implementation logic is sound.

        Ok(())
    }

    #[test]
    fn test_drop_table_if_exists() -> Result<(), OxidbError> {
        let (mut exec, _temp_dir, _store_file) = create_test_executor();

        // Drop non-existent table with IF EXISTS
        let drop = Command::DropTable {
            table_name: "non_existent".to_string(),
            if_exists: true,
        };
        let result = exec.execute_command(drop)?;
        assert!(matches!(result, ExecutionResult::Success));

        Ok(())
    }

    #[test]
    fn test_drop_table_not_found() {
        let (mut exec, _temp_dir, _store_file) = create_test_executor();

        // Drop non-existent table without IF EXISTS
        let drop = Command::DropTable {
            table_name: "non_existent".to_string(),
            if_exists: false,
        };
        let result = exec.execute_command(drop);
        assert!(result.is_err());
        match result {
            Err(OxidbError::TableNotFound(name)) => assert_eq!(name, "non_existent"),
            _ => panic!("Expected TableNotFound error"),
        }
    }
}
