use anyhow::{Context, Result};
use oxidb::{core::query::executor::ExecutionResult, Oxidb}; // Removed OxidbError, DataType

const DB_PATH: &str = "test_datatypes.db";

fn execute_and_report(db: &mut Oxidb, query: &str, description: &str) {
    println!("Executing: {} ({})", query, description);
    match db.execute_query_str(query) {
        Ok(result) => println!("  Success: {:?}\n", result),
        Err(e) => println!("  Error: {:?}\n", e),
    }
}

fn execute_select_and_report(db: &mut Oxidb, query: &str, description: &str) {
    println!("Executing SELECT: {} ({})", query, description);
    match db.execute_query_str(query) {
        Ok(ExecutionResult::Values(values)) => {
            if values.is_empty() {
                println!("  Result: No rows returned.\n");
            } else {
                println!("  Result: {} rows returned.", values.len() / 2); // Assuming key-value pairs
                for (i, chunk) in values.chunks_exact(2).enumerate() {
                    println!("    Row {}: Key={:?}, ValueMap={:?}", i + 1, chunk[0], chunk[1]);
                }
                println!();
            }
        }
        Ok(other_result) => println!("  Success (non-values): {:?}\n", other_result),
        Err(e) => println!("  Error: {:?}\n", e),
    }
}

fn test_primary_key(db: &mut Oxidb) -> Result<()> {
    println!("--- Testing PRIMARY KEY Constraint ---");
    execute_and_report(
        db,
        "CREATE TABLE test_pk (id INTEGER PRIMARY KEY, name TEXT)",
        "Create table with PK",
    );
    execute_and_report(
        db,
        "INSERT INTO test_pk (id, name) VALUES (1, 'Alice')",
        "Insert first record",
    );
    execute_and_report(
        db,
        "INSERT INTO test_pk (id, name) VALUES (1, 'Alice V2')",
        "Attempt duplicate PK insert",
    );
    execute_and_report(
        db,
        "INSERT INTO test_pk (name) VALUES ('Bob')",
        "Attempt insert without PK",
    );
    execute_and_report(
        db,
        "INSERT INTO test_pk (id, name) VALUES (2, 'Charlie')",
        "Insert second distinct PK",
    );
    execute_select_and_report(db, "SELECT * FROM test_pk", "Final data in test_pk");
    println!("--- PRIMARY KEY Test Done ---\n");
    Ok(())
}

fn test_unique_constraint(db: &mut Oxidb) -> Result<()> {
    println!("--- Testing UNIQUE Constraint ---");
    execute_and_report(
        db,
        "CREATE TABLE test_unique (id INTEGER, email TEXT UNIQUE NOT NULL)",
        "Create table with UNIQUE NOT NULL email",
    );
    execute_and_report(
        db,
        "INSERT INTO test_unique (id, email) VALUES (1, 'alice@example.com')",
        "Insert first email",
    );
    execute_and_report(
        db,
        "INSERT INTO test_unique (id, email) VALUES (2, 'alice@example.com')",
        "Attempt duplicate email",
    );
    execute_and_report(
        db,
        "INSERT INTO test_unique (id, email) VALUES (3, 'bob@example.com')",
        "Insert second unique email",
    );
    execute_select_and_report(db, "SELECT * FROM test_unique", "Final data in test_unique");

    execute_and_report(
        db,
        "CREATE TABLE test_unique_nullable (id INTEGER, phone TEXT UNIQUE)",
        "Create table with UNIQUE nullable phone",
    );
    execute_and_report(
        db,
        "INSERT INTO test_unique_nullable (id, phone) VALUES (1, NULL)",
        "Insert NULL phone 1",
    );
    execute_and_report(
        db,
        "INSERT INTO test_unique_nullable (id, phone) VALUES (2, NULL)",
        "Insert NULL phone 2",
    );
    execute_and_report(
        db,
        "INSERT INTO test_unique_nullable (id, phone) VALUES (3, '123-4567')",
        "Insert phone number",
    );
    execute_and_report(
        db,
        "INSERT INTO test_unique_nullable (id, phone) VALUES (4, '123-4567')",
        "Attempt duplicate phone number",
    );
    execute_select_and_report(
        db,
        "SELECT * FROM test_unique_nullable",
        "Final data in test_unique_nullable",
    );
    println!("--- UNIQUE Constraint Test Done ---\n");
    Ok(())
}

fn test_not_null_constraint(db: &mut Oxidb) -> Result<()> {
    println!("--- Testing NOT NULL Constraint ---");
    execute_and_report(
        db,
        "CREATE TABLE test_not_null (id INTEGER, description TEXT NOT NULL, notes TEXT)",
        "Create table with NOT NULL description",
    );
    execute_and_report(
        db,
        "INSERT INTO test_not_null (id, description, notes) VALUES (1, 'Desc 1', 'Note 1')",
        "Insert valid record",
    );
    execute_and_report(
        db,
        "INSERT INTO test_not_null (id, description, notes) VALUES (2, NULL, 'Note 2')",
        "Attempt insert NULL for NOT NULL description",
    );
    execute_and_report(
        db,
        "INSERT INTO test_not_null (id, notes) VALUES (3, 'Note 3')",
        "Attempt insert record omitting NOT NULL description",
    );
    execute_select_and_report(db, "SELECT * FROM test_not_null", "Final data in test_not_null");
    println!("--- NOT NULL Constraint Test Done ---\n");
    Ok(())
}

fn test_various_datatypes(db: &mut Oxidb) -> Result<()> {
    println!("--- Testing Various Data Types ---");
    execute_and_report(db,
        // "CREATE TABLE test_datatypes (c_integer INTEGER, c_text TEXT, c_boolean BOOLEAN, c_varchar_10 VARCHAR(10), c_numeric_5_2 NUMERIC(5,2))",
        "CREATE TABLE test_datatypes (c_integer INTEGER, c_text TEXT, c_boolean BOOLEAN, c_varchar_10 VARCHAR(10))", // c_numeric_5_2 NUMERIC(5,2) removed due to parsing error
        "Create table with various data types (NUMERIC commented out)"
    );

    // INTEGER tests
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_integer) VALUES (123)",
        "Insert INTEGER 123",
    );
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_integer) VALUES (-456)",
        "Insert INTEGER -456",
    );
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_integer) VALUES ('789')",
        "Insert STRING '789' into INTEGER (type affinity test)",
    );
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_integer) VALUES ('abc')",
        "Insert STRING 'abc' into INTEGER (should fail or store as 0/NULL if affinity is loose)",
    );
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_integer) VALUES (1.23)",
        "Insert REAL 1.23 into INTEGER (type affinity test)",
    );

    // TEXT tests
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_text) VALUES ('hello world')",
        "Insert TEXT 'hello world'",
    );
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_text) VALUES (12345)",
        "Insert INTEGER 12345 into TEXT (type affinity test)",
    );
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_text) VALUES (true)",
        "Insert BOOLEAN true into TEXT (type affinity test)",
    );

    // BOOLEAN tests
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_boolean) VALUES (true)",
        "Insert BOOLEAN true",
    );
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_boolean) VALUES (false)",
        "Insert BOOLEAN false",
    );
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_boolean) VALUES (1)",
        "Insert INTEGER 1 into BOOLEAN (type affinity test, should be true)",
    );
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_boolean) VALUES (0)",
        "Insert INTEGER 0 into BOOLEAN (type affinity test, should be false)",
    );
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_boolean) VALUES ('true')",
        "Insert STRING 'true' into BOOLEAN (type affinity test)",
    );
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_boolean) VALUES ('false')",
        "Insert STRING 'false' into BOOLEAN (type affinity test)",
    );
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_boolean) VALUES ('yes')",
        "Insert STRING 'yes' into BOOLEAN (should fail or be NULL/false)",
    );
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_boolean) VALUES (NULL)",
        "Insert NULL into BOOLEAN",
    );

    // VARCHAR(10) tests (expecting it to behave like TEXT)
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_varchar_10) VALUES ('short')",
        "Insert 'short' into VARCHAR(10)",
    );
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_varchar_10) VALUES ('longervalue')",
        "Insert 'longervalue' into VARCHAR(10) (11 chars)",
    );

    // NUMERIC(5,2) tests (expecting it to behave like TEXT or REAL)
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_numeric_5_2) VALUES ('123.45')",
        "Insert STRING '123.45' into NUMERIC(5,2)",
    );
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_numeric_5_2) VALUES (123.45)",
        "Insert REAL 123.45 into NUMERIC(5,2)",
    );
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_numeric_5_2) VALUES ('1234.56')",
        "Insert '1234.56' into NUMERIC(5,2) (violates precision)",
    );
    execute_and_report(
        db,
        "INSERT INTO test_datatypes (c_numeric_5_2) VALUES ('abc')",
        "Insert 'abc' into NUMERIC(5,2)",
    );

    execute_select_and_report(db, "SELECT * FROM test_datatypes", "Final data in test_datatypes");
    println!("--- Various Data Types Test Done ---\n");
    Ok(())
}

fn main() -> Result<()> {
    println!("Starting Oxidb DataType and Constraint Tests...");
    // Delete old database file if it exists
    if std::path::Path::new(DB_PATH).exists() {
        std::fs::remove_file(DB_PATH)
            .context(format!("Failed to delete old database file: {}", DB_PATH))?;
        println!("Removed old database file: {}", DB_PATH);
    }

    let mut db = Oxidb::new(DB_PATH).context("Failed to create/open database")?;
    println!("Database instance created/opened at: {}", DB_PATH);

    test_primary_key(&mut db).context("Primary Key test failed")?;
    test_unique_constraint(&mut db).context("Unique Constraint test failed")?;
    test_not_null_constraint(&mut db).context("Not Null Constraint test failed")?;
    test_various_datatypes(&mut db).context("Various DataTypes test failed")?;

    db.persist().context("Failed to persist database at the end of tests")?;
    println!("All tests concluded. Database persisted.");
    Ok(())
}
