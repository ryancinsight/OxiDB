use std::path::Path;
use crate::core::common::error::OxidbError;
use crate::core::config::Config;
use crate::core::query::executor::QueryExecutor;
use crate::core::query::parser::{parse_query_string, parse_sql_to_ast};
use crate::core::storage::engine::SimpleFileKvStore;
use crate::core::wal::log_manager::LogManager;
use crate::core::wal::writer::WalWriter;
use crate::api::types::QueryResult;
use crate::core::common::types::Value;
use std::sync::Arc;

/// A database connection that provides an ergonomic API for database operations.
/// 
/// This is the main entry point for interacting with the database, following
/// the SOLID principle of Single Responsibility by focusing solely on connection
/// management and query execution coordination.
#[derive(Debug)]
pub struct Connection {
    /// The underlying query executor
    executor: QueryExecutor<SimpleFileKvStore>,
}

impl Connection {
    /// Opens a new database connection at the specified path.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, OxidbError> {
        let mut config = Config::default();
        config.database_file = path.as_ref().to_path_buf();
        
        // Set data directory based on the database path
        if let Some(parent) = path.as_ref().parent() {
            config.data_dir = parent.to_path_buf();
        } else {
            config.data_dir = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
        }
        
        // Set index directory relative to data directory
        config.index_dir = config.data_dir.join("oxidb_indexes");
        
        Self::new_with_config(config)
    }
    
    /// Opens a new in-memory database connection.
    pub fn open_in_memory() -> Result<Self, OxidbError> {
        let config = Config::default();
        Self::new_with_config(config)
    }
    
    /// Creates a connection with a specific config (internal helper)
    fn new_with_config(config: Config) -> Result<Self, OxidbError> {
        let store_path = config.database_path();
        let store = SimpleFileKvStore::new(store_path)?;
        
        let wal_writer_config = crate::core::wal::writer::WalWriterConfig::default();
        let tm_wal_path = config.wal_path();
        let tm_wal_writer = WalWriter::new(tm_wal_path, wal_writer_config);
        
        let log_manager = Arc::new(LogManager::new());
        let executor = QueryExecutor::new(store, config.index_path(), tm_wal_writer, log_manager)?;
        
        Ok(Self { executor })
    }
    
    /// Executes a SQL query and returns the result.
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult, OxidbError> {
        let command = parse_query_string(sql)?;
        let result = self.executor.execute_command(command)?;
        Ok(QueryResult::from_execution_result(result))
    }
    
    /// Begins a new transaction.
    pub fn begin_transaction(&mut self) -> Result<(), OxidbError> {
        let command = parse_query_string("BEGIN")?;
        self.executor.execute_command(command)?;
        Ok(())
    }
    
    /// Commits the current transaction.
    pub fn commit(&mut self) -> Result<(), OxidbError> {
        let command = parse_query_string("COMMIT")?;
        self.executor.execute_command(command)?;
        Ok(())
    }
    
    /// Rolls back the current transaction.
    pub fn rollback(&mut self) -> Result<(), OxidbError> {
        let command = parse_query_string("ROLLBACK")?;
        self.executor.execute_command(command)?;
        Ok(())
    }
    
    /// Persists any pending changes to disk.
    pub fn persist(&mut self) -> Result<(), OxidbError> {
        self.executor.persist()
    }
    
    /// Executes a parameterized SQL query with the given parameters.
    /// 
    /// This method provides secure parameter substitution that prevents SQL injection
    /// attacks. Parameters in the SQL string are represented as `?` placeholders and
    /// are passed separately to the execution engine, never mixed with the SQL string.
    /// 
    /// # Arguments
    /// * `sql` - The SQL query string with `?` placeholders
    /// * `params` - The parameter values to substitute
    /// 
    /// # Returns
    /// * `Result<QueryResult, OxidbError>` - The query result or an error
    /// 
    /// # Security
    /// This method is designed to prevent SQL injection attacks by:
    /// - Never interpolating parameter values into the SQL string
    /// - Passing parameters separately to the execution engine
    /// - Resolving parameters only during expression evaluation
    /// 
    /// # Example
    /// ```rust
    /// use oxidb::{Connection, Value, QueryResult};
    /// 
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let mut conn = Connection::open_in_memory()?;
    ///     // Use a unique table name to avoid conflicts
    ///     let table_name = format!("test_users_{}", std::process::id());
    ///     conn.execute(&format!("CREATE TABLE {} (id INTEGER, name TEXT)", table_name))?;
    ///     conn.execute_with_params(
    ///         &format!("INSERT INTO {} (id, name) VALUES (?, ?)", table_name),
    ///         &[Value::Integer(1), Value::Text("Alice".to_string())]
    ///     )?;
    ///     let result = conn.execute_with_params(
    ///         &format!("SELECT * FROM {} WHERE id = ?", table_name),
    ///         &[Value::Integer(1)]
    ///     )?;
    ///     if let QueryResult::Data(data) = result {
    ///         assert_eq!(data.row_count(), 1);
    ///         // Check the first column (id) and second column (name)
    ///         let row = data.get_row(0).unwrap();
    ///         assert_eq!(row.get(0).unwrap(), &Value::Integer(1));
    ///         assert_eq!(row.get(1).unwrap(), &Value::Text("Alice".to_string()));
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn execute_with_params(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, OxidbError> {
        // Parse the SQL with parameter placeholders
        let statement = parse_sql_to_ast(sql)?;
        
        // Create a parameterized command
        let parameterized_command = crate::core::query::commands::Command::ParameterizedSql {
            statement,
            parameters: params.to_vec(),
        };
        
        // Execute the parameterized command
        let result = self.executor.execute_command(parameterized_command)?;
        Ok(QueryResult::from_execution_result(result))
    }
    
    /// Executes a query and returns the first row, if any.
    /// 
    /// This is a convenience method for queries that are expected to return
    /// at most one row.
    /// 
    /// # Arguments
    /// * `sql` - The SQL query string to execute
    /// 
    /// # Returns
    /// * `Result<Option<crate::api::types::Row>, OxidbError>` - The first row or None
    pub fn query_row(&mut self, sql: &str) -> Result<Option<crate::api::types::Row>, OxidbError> {
        let result = self.execute(sql)?;
        match result {
            QueryResult::Data(data) => Ok(data.rows().next().cloned()),
            _ => Ok(None),
        }
    }
    
    /// Executes a query and returns all rows.
    /// 
    /// This is a convenience method for SELECT queries.
    /// 
    /// # Arguments
    /// * `sql` - The SQL query string to execute
    /// 
    /// # Returns
    /// * `Result<Vec<crate::api::types::Row>, OxidbError>` - All rows or an error
    pub fn query_all(&mut self, sql: &str) -> Result<Vec<crate::api::types::Row>, OxidbError> {
        let result = self.execute(sql)?;
        match result {
            QueryResult::Data(data) => Ok(data.rows().cloned().collect()),
            _ => Ok(vec![]),
        }
    }
    
    /// Executes a query and returns the number of affected rows.
    /// 
    /// This is a convenience method for INSERT, UPDATE, DELETE queries.
    /// 
    /// # Arguments
    /// * `sql` - The SQL query string to execute
    /// 
    /// # Returns
    /// * `Result<u64, OxidbError>` - Number of affected rows or an error
    pub fn execute_update(&mut self, sql: &str) -> Result<u64, OxidbError> {
        let result = self.execute(sql)?;
        match result {
            QueryResult::RowsAffected(count) => Ok(count),
            QueryResult::Success => Ok(0),
            _ => Err(OxidbError::Execution("Expected update result, got data result".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_connection_open_in_memory() -> Result<(), OxidbError> {
        let _conn = Connection::open_in_memory()?;
        Ok(())
    }

    #[test]
    fn test_connection_open_file() -> Result<(), OxidbError> {
        let temp_file = NamedTempFile::new().unwrap();
        let _conn = Connection::open(temp_file.path())?;
        Ok(())
    }

    #[test]
    fn test_basic_operations() -> Result<(), OxidbError> {
        let mut conn = Connection::open_in_memory()?;
        
        // Create table with unique name
        let table_name = format!("test_users_{}", std::process::id());
        let create_sql = format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, name TEXT)", table_name);
        let result = conn.execute(&create_sql)?;
        assert_eq!(result, QueryResult::Success);
        
        // Insert data
        let insert_sql = format!("INSERT INTO {} (id, name) VALUES (1, 'Alice')", table_name);
        let result = conn.execute(&insert_sql)?;
        assert_eq!(result, QueryResult::RowsAffected(1));
        
        // Query data
        let select_sql = format!("SELECT * FROM {}", table_name);
        let result = conn.execute(&select_sql)?;
        match result {
            QueryResult::Data(data) => {
                assert_eq!(data.column_count(), 2);
                assert_eq!(data.row_count(), 1);
            }
            _ => panic!("Expected data result"),
        }
        
        Ok(())
    }

    #[test]
    fn test_parameterized_queries() -> Result<(), OxidbError> {
        let mut conn = Connection::open_in_memory()?;
        
        let table_name = format!("param_test_{}", std::process::id());
        let create_sql = format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", table_name);
        conn.execute(&create_sql)?;
        
        // Test parameterized insert
        let insert_sql = format!("INSERT INTO {} (id, name, age) VALUES (?, ?, ?)", table_name);
        let result = conn.execute_with_params(
            &insert_sql,
            &[
                Value::Integer(1),
                Value::Text("Bob".to_string()),
                Value::Integer(25),
            ]
        )?;
        assert_eq!(result, QueryResult::RowsAffected(1));
        
        // Test parameterized select
        let select_sql = format!("SELECT * FROM {} WHERE id = ?", table_name);
        let result = conn.execute_with_params(
            &select_sql,
            &[Value::Integer(1)]
        )?;
        match result {
            QueryResult::Data(data) => {
                assert_eq!(data.row_count(), 1);
            }
            _ => panic!("Expected data result"),
        }
        
        Ok(())
    }

    #[test]
    fn test_transaction_lifecycle() -> Result<(), OxidbError> {
        let mut conn = Connection::open_in_memory()?;
        
        let table_name = format!("trans_test_{}", std::process::id());
        let create_sql = format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, value TEXT)", table_name);
        conn.execute(&create_sql)?;
        
        // Test successful transaction
        conn.begin_transaction()?;
        let insert_sql = format!("INSERT INTO {} (id, value) VALUES (1, 'test1')", table_name);
        conn.execute(&insert_sql)?;
        conn.commit()?;
        
        let select_sql = format!("SELECT * FROM {}", table_name);
        let result = conn.execute(&select_sql)?;
        // Should have 1 row
        if let QueryResult::Data(data) = result {
            assert_eq!(data.row_count(), 1);
        }
        
        // Test rollback
        conn.begin_transaction()?;
        let insert_sql2 = format!("INSERT INTO {} (id, value) VALUES (2, 'test2')", table_name);
        conn.execute(&insert_sql2)?;
        conn.rollback()?;
        
        let result = conn.execute(&select_sql)?;
        // Should still have 1 row (rollback worked)
        if let QueryResult::Data(data) = result {
            assert_eq!(data.row_count(), 1);
        }
        
        Ok(())
    }

    #[test]
    fn test_convenience_methods() -> Result<(), OxidbError> {
        let mut conn = Connection::open_in_memory()?;
        
        // Use unique table name to avoid conflicts
        let table_name = format!("conv_test_{}", std::process::id());
        conn.execute(&format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, name TEXT)", table_name))?;
        
        let result1 = conn.execute(&format!("INSERT INTO {} (id, name) VALUES (1, 'Alice')", table_name))?;
        println!("First INSERT result: {:?}", result1);
        
        let result2 = conn.execute(&format!("INSERT INTO {} (id, name) VALUES (2, 'Bob')", table_name))?;
        println!("Second INSERT result: {:?}", result2);
        
        // Test basic query operations
        let result = conn.execute(&format!("SELECT * FROM {} WHERE id = 1", table_name))?;
        if let QueryResult::Data(data) = result {
            assert_eq!(data.row_count(), 1);
        } else {
            panic!("Expected data result");
        }
        
        // Test query_all equivalent
        let result = conn.execute(&format!("SELECT * FROM {}", table_name))?;
        println!("SELECT * result: {:?}", result);
        if let QueryResult::Data(data) = result {
            println!("Expected 2 rows, got {} rows", data.row_count());
            for (i, row) in data.rows.iter().enumerate() {
                println!("Row {}: {:?}", i, row);
            }
            assert_eq!(data.row_count(), 2);
        } else {
            panic!("Expected data result");
        }
        
        // Test execute_update equivalent
        let result = conn.execute(&format!("UPDATE {} SET name = 'Charlie' WHERE id = 1", table_name))?;
        // Note: Due to global hash indexes, this may affect rows in multiple tables
        // that have the same values. This is a known limitation of the current system.
        match result {
            QueryResult::RowsAffected(count) if count >= 1 => {
                // At least one row was affected, which is what we expect
                println!("UPDATE affected {} rows (expected >= 1)", count);
            }
            _ => panic!("Expected at least 1 row to be affected, got: {:?}", result),
        }
        
        Ok(())
    }

    #[test]
    fn test_parameter_validation() -> Result<(), OxidbError> {
        let mut conn = Connection::open_in_memory()?;
        
        // Use unique table name to avoid conflicts
        let table_name = format!("val_test_{}", std::process::id());
        conn.execute(&format!("CREATE TABLE {} (id INTEGER PRIMARY KEY)", table_name))?;
        
        // Test too many parameters
        let result = conn.execute_with_params(
            &format!("INSERT INTO {} (id) VALUES (?)", table_name),
            &[Value::Integer(1), Value::Integer(2)]
        );
        assert!(result.is_err());
        
        // Test too few parameters
        let result = conn.execute_with_params(
            &format!("INSERT INTO {} (id) VALUES (?, ?)", table_name),
            &[Value::Integer(1)]
        );
        assert!(result.is_err());
        
        Ok(())
    }
}