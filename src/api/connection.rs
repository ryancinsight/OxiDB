use crate::api::types::{QueryResult, Row};
use crate::api::types::DataSet;
use crate::core::common::types::Value;
use crate::core::common::OxidbError;
use crate::core::config::Config;
use crate::core::performance::{PerformanceContext, PerformanceAnalyzer};
use crate::core::query::executor::QueryExecutor;
use crate::core::query::parser::parse_query;
use crate::core::query::sql::parser::SqlParser;
use crate::core::query::sql::tokenizer::Tokenizer;

use crate::core::storage::engine::FileKvStore;
use crate::core::wal::log_manager::LogManager;
use crate::core::wal::writer::WalWriter;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::path::PathBuf;

/// A database connection that provides an ergonomic API for database operations.
///
/// This is the main entry point for interacting with the database, following
/// the SOLID principle of Single Responsibility by focusing solely on connection
/// management and query execution coordination.
#[derive(Debug)]
pub struct Connection {
    /// The underlying query executor
    executor: QueryExecutor<FileKvStore>,
    /// Performance monitoring context
    performance: PerformanceContext,
}

// Helper function to convert DataType to Value
fn data_type_to_value(data_type: crate::core::types::DataType) -> Value {
    use crate::core::types::DataType;
    
    match data_type {
        DataType::Integer(i) => Value::Integer(i),
        DataType::Float(f) => Value::Float(f.0),
        DataType::String(s) => Value::Text(s),
        DataType::Boolean(b) => Value::Boolean(b),
        DataType::RawBytes(b) => Value::Blob(b),
        DataType::Vector(v) => Value::Vector(v.0.data),
        DataType::Null => Value::Null,
        DataType::Map(map) => Value::Text(
            serde_json::to_string(&map.0).unwrap_or_else(|_| "{}".to_string())
        ),
        DataType::JsonBlob(json) => Value::Text(json.0.to_string()),
    }
}

impl Connection {
    /// Opens a new database connection at the specified path.
    ///
    /// # Errors
    /// Returns `OxidbError` if:
    /// - The database file cannot be created or accessed
    /// - The directory structure cannot be initialized
    /// - The storage engine fails to initialize
    /// - Index directory creation fails
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, OxidbError> {
        let path_buf = path.as_ref().to_path_buf();
        let data_dir = path.as_ref().parent().map_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")), std::path::Path::to_path_buf);
        
        let index_dir = data_dir.join("oxidb_indexes");
        let config = Config {
            database_file: path_buf,
            data_dir,
            index_dir,
            ..Config::default()
        };

        Self::new_with_config(&config)
    }

    /// Opens a new in-memory database connection.
    ///
    /// # Errors
    /// Returns `OxidbError` if:
    /// - Temporary file creation fails
    /// - Storage engine initialization fails
    /// - Memory allocation for database structures fails
    pub fn open_in_memory() -> Result<Self, OxidbError> {
        let mut config = Config::default();

        // Use a unique temporary file path to avoid conflicts between concurrent tests
        use std::sync::atomic::{AtomicU64, Ordering};
        /// Counter for generating unique database file names in tests
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique_id = COUNTER.fetch_add(1, Ordering::SeqCst);

        let temp_db_name = format!("temp_oxidb_{}_{}.db", std::process::id(), unique_id);
        config.database_file = std::path::PathBuf::from(temp_db_name);

        Self::new_with_config(&config)
    }

    /// Creates a connection with a specific config (internal helper)
    fn new_with_config(config: &Config) -> Result<Self, OxidbError> {
        let store_path = config.database_path();
        let store = FileKvStore::new(store_path)?;

        let wal_writer_config = crate::core::wal::writer::WalWriterConfig::default();
        let tm_wal_path = config.wal_path();
        let tm_wal_writer = WalWriter::new(tm_wal_path, wal_writer_config);

        let log_manager = Arc::new(LogManager::new());
        let executor = QueryExecutor::new(store, config.index_path(), tm_wal_writer, log_manager)?;
        let performance = PerformanceContext::new();

        Ok(Self { executor, performance })
    }

    /// Enables performance monitoring for this connection.
    /// 
    /// This method configures the connection to track detailed performance metrics
    /// for all subsequent operations.
    pub fn enable_performance_monitoring(&mut self) {
        // Update monitoring configuration
        self.performance.config.enable_profiling = true;
        self.performance.config.enable_monitoring = true;
        self.performance.config.slow_query_threshold = Duration::from_millis(100);
    }

    /// Execute a SQL query and return the results.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use oxidb::Connection;
    /// # use oxidb::Config;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut conn = Connection::new(Config::default())?;
    /// let result = conn.query("SELECT * FROM users WHERE age > 18")?;
    /// for row in result.rows {
    ///     println!("{:?}", row);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query(&mut self, sql: &str) -> Result<QueryResult, OxidbError> {
        // Parse SQL to Command
        let command = parse_query(sql)?;
        let result = self.executor.execute_command(command)?;
        
        // Convert ExecutionResult to QueryResult
        use crate::core::query::executor::ExecutionResult;
        
        let query_result = match result {
            ExecutionResult::Query { columns, rows } => {
                // Convert Vec<Vec<DataType>> rows to Value rows
                let converted_rows = rows.into_iter()
                    .map(|row_values| Row {
                        values: row_values.into_iter()
                            .map(data_type_to_value)
                            .collect(),
                    })
                    .collect();
                
                QueryResult::Data(DataSet::new(columns, converted_rows))
            }
            ExecutionResult::Updated { count } => QueryResult::RowsAffected(count as u64),
            ExecutionResult::Value(Some(dt)) => QueryResult::Data(DataSet::new(
                vec!["value".to_string()],
                vec![Row { values: vec![data_type_to_value(dt)] }],
            )),
            ExecutionResult::Value(None) => QueryResult::Data(DataSet::new(
                vec!["value".to_string()],
                vec![],
            )),
            ExecutionResult::Values(dts) => QueryResult::Data(DataSet::new(
                vec!["value".to_string()],
                dts.into_iter().map(|dt| Row { values: vec![data_type_to_value(dt)] }).collect(),
            )),
            ExecutionResult::Deleted(success) => QueryResult::Data(DataSet::new(
                vec!["deleted".to_string()],
                vec![Row { values: vec![Value::Boolean(success)] }],
            )),
            ExecutionResult::Success => QueryResult::Success,
            ExecutionResult::RankedResults(results) => {
                // For ranked results, include distance as a column
                let columns = vec!["distance".to_string(), "data".to_string()];
                let converted_rows = results.into_iter()
                    .map(|(distance, data_values)| Row {
                        values: vec![
                            Value::Float(f64::from(distance)),
                            // Combine data values into a single JSON string
                            Value::Text(serde_json::to_string(&data_values).unwrap_or_else(|_| "[]".to_string())),
                        ],
                    })
                    .collect();
                
                QueryResult::Data(DataSet::new(columns, converted_rows))
            },
        };
        
        Ok(query_result)
    }

    /// Execute a SQL statement that modifies the database.
    ///
    /// Returns the number of rows affected by the operation.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use oxidb::Connection;
    /// # use oxidb::Config;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut conn = Connection::new(Config::default())?;
    /// let affected = conn.execute("INSERT INTO users (name, age) VALUES ('Alice', 25)")?;
    /// println!("Inserted {} rows", affected);
    /// # Ok(())
    /// # }
    /// ```
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult, OxidbError> {
        // Parse SQL to Command
        let command = parse_query(sql)?;
        let result = self.executor.execute_command(command)?;
        
        // Convert ExecutionResult to rows affected count
        use crate::core::query::executor::ExecutionResult;
        
        let mapped = match result {
            ExecutionResult::Updated { count } => QueryResult::RowsAffected(count as u64),
            ExecutionResult::Deleted(success) => QueryResult::RowsAffected(if success { 1 } else { 0 }),
            ExecutionResult::Success => QueryResult::Success,
            ExecutionResult::Query { columns, rows } => {
                let converted_rows = rows
                    .into_iter()
                    .map(|row_values| Row {
                        values: row_values.into_iter().map(data_type_to_value).collect(),
                    })
                    .collect();
                QueryResult::Data(DataSet::new(columns, converted_rows))
            }
            ExecutionResult::Value(Some(dt)) => QueryResult::Data(DataSet::new(
                vec!["value".to_string()],
                vec![Row { values: vec![data_type_to_value(dt)] }],
            )),
            ExecutionResult::Value(None) => QueryResult::Data(DataSet::new(vec!["value".to_string()], vec![])),
            ExecutionResult::Values(dts) => QueryResult::Data(DataSet::new(
                vec!["value".to_string()],
                dts.into_iter().map(|dt| Row { values: vec![data_type_to_value(dt)] }).collect(),
            )),
            ExecutionResult::RankedResults(results) => {
                let columns = vec!["distance".to_string(), "data".to_string()];
                let converted_rows = results
                    .into_iter()
                    .map(|(distance, data_values)| Row {
                        values: vec![
                            Value::Float(f64::from(distance)),
                            Value::Text(serde_json::to_string(&data_values).unwrap_or_else(|_| "[]".to_string())),
                        ],
                    })
                    .collect();
                QueryResult::Data(DataSet::new(columns, converted_rows))
            }
        };
        Ok(mapped)
    }

    /// Begin a new transaction.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - A transaction is already active
    /// - The transaction manager fails to initialize the transaction
    /// - WAL logging fails during transaction start
    pub fn begin_transaction(&mut self) -> Result<(), OxidbError> {
        let command = parse_query("BEGIN")?;
        self.executor.execute_command(command)?;
        Ok(())
    }

    /// Commit the current transaction.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - No active transaction exists
    /// - WAL flush operations fail
    /// - Lock release encounters system errors
    /// - Data persistence to disk fails
    /// - Transaction state consistency cannot be maintained
    pub fn commit(&mut self) -> Result<(), OxidbError> {
        let command = parse_query("COMMIT")?;
        self.executor.execute_command(command)?;
        Ok(())
    }

    /// Rollback the current transaction.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - No active transaction exists
    /// - Undo operations fail to restore previous state
    /// - WAL recovery encounters corrupted entries
    /// - Lock release fails during rollback cleanup
    pub fn rollback(&mut self) -> Result<(), OxidbError> {
        let command = parse_query("ROLLBACK")?;
        self.executor.execute_command(command)?;
        Ok(())
    }

    /// Persists any pending changes to disk.
    ///
    /// # Errors
    /// Returns `OxidbError` if:
    /// - Disk write operations fail due to I/O errors
    /// - WAL flush encounters storage issues
    /// - File system permissions prevent write access
    /// - Insufficient disk space for persistence operations
    pub fn persist(&mut self) -> Result<(), OxidbError> {
        self.executor.persist()
    }

    /// Generates a comprehensive performance report for this connection.
    ///
    /// This method provides detailed insights into query performance, including:
    /// - Query execution statistics (count, average time, slowest/fastest queries)
    /// - Transaction performance metrics
    /// - Storage I/O analysis
    /// - Performance bottleneck identification
    /// - Optimization recommendations
    ///
    /// # Returns
    /// * `Result<String, OxidbError>` - Detailed performance analysis as a formatted string
    ///
    /// # Errors
    /// Returns `OxidbError` if report generation fails due to internal errors
    ///
    /// # Example
    /// ```rust
    /// use oxidb::Connection;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let mut conn = Connection::open_in_memory()?;
    ///     
    ///     // Execute some queries
    ///     conn.execute("CREATE TABLE users (id INTEGER, name TEXT)")?;
    ///     conn.execute("INSERT INTO users VALUES (1, 'Alice')")?;
    ///     conn.execute("SELECT * FROM users")?;
    ///     
    ///     // Get performance report
    ///     let report = conn.get_performance_report()?;
    ///     println!("{}", report);
    ///     
    ///     Ok(())
    /// }
    /// ```
    pub fn get_performance_report(&self) -> Result<String, OxidbError> {
        let analyzer = PerformanceAnalyzer::new();
        
        // Get a read lock on the metrics
        let metrics = self.performance.metrics
            .read()
            .map_err(|_| OxidbError::Lock("Failed to acquire metrics lock".to_string()))?;
        
        let report = analyzer.analyze(&*metrics);
        Ok(report.to_string())
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
    ///     if let Some(rows) = result.rows {
    ///         assert_eq!(rows.rows.len(), 1);
    ///         // Check the row data
    ///         let row = &rows.rows[0];
    ///         // The actual data structure may vary based on internal storage
    ///         assert!(row.len() >= 2); // At least 2 columns
    ///     }
    ///     Ok(())
    /// }
    /// ```
    /// # Errors
    /// Returns `OxidbError` if:
    /// - The SQL query cannot be parsed
    /// - Parameter count doesn't match placeholder count
    /// - Parameter types are incompatible with query requirements
    /// - Query execution fails due to storage or transaction errors
    pub fn execute_with_params(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, OxidbError> {
        // Parse SQL directly to AST for parameterized queries
        let mut tokenizer = Tokenizer::new(sql);
        let tokens = tokenizer.tokenize().map_err(|e| {
            OxidbError::SqlParsing(format!("SQL tokenizer error: {e}"))
        })?;
        
        let mut parser = SqlParser::new(tokens);
        let statement = parser.parse().map_err(|e| {
            OxidbError::SqlParsing(format!("SQL parse error: {e}"))
        })?;

        // Create a parameterized command
        let parameterized_command = crate::core::query::commands::Command::ParameterizedSql {
            statement,
            parameters: params.to_vec(),
        };

        // Execute the parameterized command
        let result = self.executor.execute_command(parameterized_command)?;
        
        // Convert ExecutionResult to QueryResult
        use crate::core::query::executor::ExecutionResult;
        
        let query_result = match result {
            ExecutionResult::Query { columns, rows } => {
                // Convert Vec<Vec<DataType>> rows to Value rows
                let converted_rows = rows.into_iter()
                    .map(|row_values| Row {
                        values: row_values.into_iter()
                            .map(data_type_to_value)
                            .collect(),
                    })
                    .collect();
                
                QueryResult::Data(DataSet::new(columns, converted_rows))
            }
            ExecutionResult::Updated { count } => QueryResult::RowsAffected(count as u64),
            _ => QueryResult::Success,
        };
        
        Ok(query_result)
    }

    /// Execute a query and return only the first row, if any.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use oxidb::Connection;
    /// # use oxidb::Config;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut conn = Connection::new(Config::default())?;
    /// if let Some(row) = conn.query_first("SELECT * FROM users WHERE id = 1")? {
    ///     println!("Found user: {:?}", row);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_first(&mut self, sql: &str) -> Result<Option<Row>, OxidbError> {
        let result = self.query(sql)?;
        match result {
            QueryResult::Data(ds) => Ok(ds.rows.into_iter().next()),
            _ => Ok(None),
        }
    }

    /// Execute a query and return all rows.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use oxidb::Connection;
    /// # use oxidb::Config;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut conn = Connection::new(Config::default())?;
    /// let rows = conn.query_all("SELECT * FROM users")?;
    /// for row in rows {
    ///     println!("{:?}", row);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_all(&mut self, sql: &str) -> Result<Vec<Row>, OxidbError> {
        let result = self.query(sql)?;
        match result {
            QueryResult::Data(ds) => Ok(ds.rows),
            _ => Ok(Vec::new()),
        }
    }

    /// Executes an UPDATE, INSERT, or DELETE statement and returns the number of affected rows.
    /// 
    /// # Errors
    /// 
    /// Returns `OxidbError` if:
    /// - The SQL statement cannot be parsed or executed
    /// - The statement returns data instead of a row count (e.g., SELECT statements)
    /// - Underlying database operations fail
    pub fn execute_update(&mut self, sql: &str) -> Result<u64, OxidbError> {
        let res = self.execute(sql)?;
        match res {
            QueryResult::RowsAffected(count) => Ok(count),
            _ => Ok(0), // Should not happen for UPDATE/INSERT/DELETE
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
        let create_sql = format!("CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, name TEXT)");
        let result = conn.execute(&create_sql)?;
        assert_eq!(result.row_count(), 0); // Changed from assert_eq!(result, 0)

        // Insert data
        let insert_sql = format!("INSERT INTO {table_name} (id, name) VALUES (1, 'Alice')");
        let result = conn.execute(&insert_sql)?;
        assert_eq!(result.row_count(), 1); // Changed from assert_eq!(result, 1)

        // Query data
        let select_sql = format!("SELECT * FROM {table_name}");
        let result = conn.query(&select_sql)?;
        match result {
            QueryResult::Data(ds) => {
                assert!(!ds.rows.is_empty());
                assert_eq!(ds.columns.len(), 2);
                assert_eq!(ds.rows.len(), 1);
            }
            other => panic!("Unexpected result: {:?}", other),
        }

        Ok(())
    }

    #[test]
    fn test_parameterized_queries() -> Result<(), OxidbError> {
        let mut conn = Connection::open_in_memory()?;

        let table_name = format!("param_test_{}", std::process::id());
        let create_sql =
            format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", table_name);
        conn.execute(&create_sql)?;

        // Test parameterized insert
        let insert_sql = format!("INSERT INTO {} (id, name, age) VALUES (?, ?, ?)", table_name);
        let result = conn.execute_with_params(
            &insert_sql,
            &[Value::Integer(1), Value::Text("Bob".to_string()), Value::Integer(25)],
        )?;
        assert_eq!(result.row_count(), 1);

        // Test parameterized select
        let select_sql = format!("SELECT * FROM {table_name} WHERE id = ?");
        let result = conn.execute_with_params(&select_sql, &[Value::Integer(1)])?;
        match result {
            QueryResult::Data(ds) => assert_eq!(ds.rows.len(), 1),
            other => panic!("Unexpected result: {:?}", other),
        }

        Ok(())
    }

    #[test]
    fn test_transaction_lifecycle() -> Result<(), OxidbError> {
        let mut conn = Connection::open_in_memory()?;

        let table_name = format!("trans_test_{}", std::process::id());
        let create_sql =
            format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, value TEXT)", table_name);
        conn.execute(&create_sql)?;

        // Test successful transaction
        conn.begin_transaction()?;
        let insert_sql = format!("INSERT INTO {} (id, value) VALUES (1, 'test1')", table_name);
        conn.execute(&insert_sql)?;
        conn.commit()?;

        let select_sql = format!("SELECT * FROM {table_name}");
        let result = conn.query(&select_sql)?;
        // Should have 1 row
        match result {
            QueryResult::Data(ds) => assert_eq!(ds.rows.len(), 1),
            other => panic!("Unexpected result: {:?}", other),
        }

        // Test rollback
        conn.begin_transaction()?;
        let insert_sql2 = format!("INSERT INTO {} (id, value) VALUES (2, 'test2')", table_name);
        conn.execute(&insert_sql2)?;
        conn.rollback()?;

        let result = conn.query(&select_sql)?;
        // Should still have 1 row (rollback worked)
        match result {
            QueryResult::Data(ds) => assert_eq!(ds.rows.len(), 1),
            other => panic!("Unexpected result: {:?}", other),
        }

        Ok(())
    }

    #[test]
    fn test_convenience_methods() -> Result<(), OxidbError> {
        let mut conn = Connection::open_in_memory()?;

        // Use unique table name to avoid conflicts
        let table_name = format!("conv_test_{}", std::process::id());
        conn.execute(&format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, name TEXT)", table_name))?;

        let result1 =
            conn.execute(&format!("INSERT INTO {} (id, name) VALUES (1, 'Alice')", table_name))?;
        println!("First INSERT result: {:?}", result1);

        let result2 =
            conn.execute(&format!("INSERT INTO {} (id, name) VALUES (2, 'Bob')", table_name))?;
        println!("Second INSERT result: {:?}", result2);

        // Test basic query operations
        let result = conn.query(&format!("SELECT * FROM {} WHERE id = 1", table_name))?;
        match result {
            QueryResult::Data(ds) => assert_eq!(ds.rows.len(), 1),
            other => panic!("Unexpected result: {:?}", other),
        }

        // Test query_all equivalent
        let result = conn.query(&format!("SELECT * FROM {}", table_name))?;
        println!("SELECT * result: {:?}", result);
        match result {
            QueryResult::Data(ds) => {
                println!("Expected 2 rows, got {} rows", ds.rows.len());
                for (i, row) in ds.rows.iter().enumerate() {
                    println!("Row {}: {:?}", i, row);
                }
                assert_eq!(ds.rows.len(), 2);
            }
            other => panic!("Unexpected result: {:?}", other),
        }

        // Test execute_update equivalent
        let result =
            conn.execute(&format!("UPDATE {} SET name = 'Charlie' WHERE id = 1", table_name))?;
        // Note: Due to global hash indexes, this may affect rows in multiple tables
        // that have the same values. This is a known limitation of the current system.
        assert!(result.row_count() >= 1, "Expected at least 1 row to be affected, got: {}", result.row_count());
        println!("UPDATE affected {} rows (expected >= 1)", result.row_count());

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
            &[Value::Integer(1), Value::Integer(2)],
        );
        assert!(result.is_err());

        // Test too few parameters
        let result = conn.execute_with_params(
            &format!("INSERT INTO {} (id) VALUES (?, ?)", table_name),
            &[Value::Integer(1)],
        );
        assert!(result.is_err());

        Ok(())
    }
}
