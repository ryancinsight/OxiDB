# Oxidb Data Type and Constraint Tests

This example is designed to test how `oxidb` handles various SQL data types and constraints defined in `CREATE TABLE` statements.

## Purpose

The primary goal is to observe and document `oxidb`'s behavior when:
- Creating tables with specific data types (e.g., `INTEGER`, `TEXT`, `BOOLEAN`, and SQL-standard types like `VARCHAR(N)`, `NUMERIC(P,S)`).
- Attempting to enforce constraints like `PRIMARY KEY`, `UNIQUE`, and `NOT NULL`.
- Inserting data that complies with or violates these types and constraints.
- Retrieving data to see how types are stored or converted.

This is not a comprehensive SQL feature test suite but a targeted exploration based on common SQL features relevant to data definition.

## Prerequisites

- Rust and Cargo installed (https://www.rust-lang.org/tools/install)
- The `oxidb` crate should be present two levels above this example's directory (i.e., `../../oxidb`). The path dependency is configured in `Cargo.toml`.

## Building the Example

To build the application, navigate to the `examples/data_type_tests` directory and run:

```bash
cargo build
```

## Running the Example

You can run the application directly using `cargo run`:

```bash
cargo run
```
Or, from the root of the `oxidb` repository:
```bash
cargo run --manifest-path examples/data_type_tests/Cargo.toml
```

The application will:
1. Delete any existing `test_datatypes.db` file.
2. Create a new `test_datatypes.db` file.
3. Execute a series of tests, printing output for each operation (CREATE TABLE, INSERT, SELECT).
4. Report successes, errors, and retrieved data to standard output.

Observe the output to understand how `oxidb` handles each test case. The full output of a test run has been saved to `test_output.log`.

## Test Results and Observations (from run on [Date of Test])

**General Observations:**

*   **Data Visibility**: A critical and pervasive issue is that `SELECT * FROM ...` queries consistently returned "No rows returned" immediately after `INSERT` operations, even within the same database session (before `persist()` and reload). This means the success of `INSERT` operations (indicated by `Ok(Success)`) could not be verified by immediate data retrieval. The findings below regarding `INSERT` behavior are based on whether the `INSERT` command itself returned `Ok` or `Err`, not on subsequent `SELECT` data. This significantly impacts the ability to test data integrity and type storage accurately.
*   **Constraint Enforcement**: Based on the `INSERT` command results, constraints (`PRIMARY KEY` uniqueness, `UNIQUE`, `NOT NULL`) are **not currently enforced** by `oxidb`. Inserts that violate these constraints still return `Ok(Success)`.

**Detailed Test Case Results:**

1.  **Primary Key (`test_pk`)**
    *   **DDL**: `CREATE TABLE test_pk (id INTEGER PRIMARY KEY, name TEXT)`
    *   **Operations & Observations**:
        *   `CREATE TABLE`: Success.
        *   `INSERT INTO test_pk (id, name) VALUES (1, 'Alice')`: Success.
        *   `INSERT INTO test_pk (id, name) VALUES (1, 'Alice V2')` (duplicate PK): **Success**. (PRIMARY KEY uniqueness not enforced).
        *   `INSERT INTO test_pk (name) VALUES ('Bob')` (PK not specified, implies NULL or AUTOINCREMENT): **Success**. (Behavior of AUTOINCREMENT or NULL PK not verifiable due to SELECT issue).
        *   `INSERT INTO test_pk (id, name) VALUES (2, 'Charlie')`: Success.
        *   `SELECT * FROM test_pk`: No rows returned.

2.  **Unique Constraint (`test_unique`, `test_unique_nullable`)**
    *   **DDL 1**: `CREATE TABLE test_unique (id INTEGER, email TEXT UNIQUE NOT NULL)`
    *   **Operations & Observations (test_unique)**:
        *   `CREATE TABLE`: Success.
        *   `INSERT INTO test_unique (id, email) VALUES (1, 'alice@example.com')`: Success.
        *   `INSERT INTO test_unique (id, email) VALUES (2, 'alice@example.com')` (duplicate email): **Success**. (UNIQUE constraint not enforced).
        *   `SELECT * FROM test_unique`: No rows returned.
    *   **DDL 2**: `CREATE TABLE test_unique_nullable (id INTEGER, phone TEXT UNIQUE)`
    *   **Operations & Observations (test_unique_nullable)**:
        *   `CREATE TABLE`: Success.
        *   `INSERT INTO test_unique_nullable (id, phone) VALUES (1, NULL)`: Success.
        *   `INSERT INTO test_unique_nullable (id, phone) VALUES (2, NULL)` (multiple NULLs): **Success**. (Correct if UNIQUE allows multiple NULLs, but lack of other enforcement makes this inconclusive).
        *   `INSERT INTO test_unique_nullable (id, phone) VALUES (3, '123-4567')`: Success.
        *   `INSERT INTO test_unique_nullable (id, phone) VALUES (4, '123-4567')` (duplicate phone): **Success**. (UNIQUE constraint not enforced).
        *   `SELECT * FROM test_unique_nullable`: No rows returned.

3.  **Not Null Constraint (`test_not_null`)**
    *   **DDL**: `CREATE TABLE test_not_null (id INTEGER, description TEXT NOT NULL, notes TEXT)`
    *   **Operations & Observations**:
        *   `CREATE TABLE`: Success.
        *   `INSERT INTO test_not_null (id, description, notes) VALUES (1, 'Desc 1', 'Note 1')`: Success.
        *   `INSERT INTO test_not_null (id, description, notes) VALUES (2, NULL, 'Note 2')` (explicit NULL): **Success**. (NOT NULL constraint not enforced).
        *   `INSERT INTO test_not_null (id, notes) VALUES (3, 'Note 3')` (implicit NULL): **Success**. (NOT NULL constraint not enforced).
        *   `SELECT * FROM test_not_null`: No rows returned.

4.  **Various Data Types (`test_datatypes`)**
    *   **DDL Attempted**: `CREATE TABLE test_datatypes (c_integer INTEGER, c_text TEXT, c_boolean BOOLEAN, c_varchar_10 VARCHAR(10), c_numeric_5_2 NUMERIC(5,2))`
    *   **Observation on DDL**: `CREATE TABLE` failed with `Error: SqlParsing("Unsupported column type during CREATE TABLE translation: NUMERIC(5, 2)")`. The `NUMERIC(P,S)` type is not supported by the parser. (The test code was subsequently modified to comment out this column to allow other tests to proceed).
    *   **DDL Used (Modified)**: `CREATE TABLE test_datatypes (c_integer INTEGER, c_text TEXT, c_boolean BOOLEAN, c_varchar_10 VARCHAR(10))`
    *   **Operations & Observations (general)**: All `INSERT` operations reported `Success` except where noted. `SELECT *` returned no rows.
        *   **INTEGER**:
            *   `INSERT ... VALUES (123)`: Success.
            *   `INSERT ... VALUES (-456)`: `Error: SqlParsing("SQL tokenizer error: Invalid character '-' at position 47")`. Negative integers are not tokenized/parsed correctly.
            *   `INSERT ... VALUES ('789')` (string to INTEGER): Success. (Type affinity behavior unknown due to SELECT issue).
            *   `INSERT ... VALUES ('abc')` (non-numeric string to INTEGER): Success. (Behavior unknown).
            *   `INSERT ... VALUES (1.23)` (real to INTEGER): Success. (Behavior unknown).
        *   **TEXT**:
            *   `INSERT ... VALUES ('hello world')`: Success.
            *   `INSERT ... VALUES (12345)` (integer to TEXT): Success. (Behavior unknown).
            *   `INSERT ... VALUES (true)` (boolean to TEXT): Success. (Behavior unknown).
        *   **BOOLEAN**:
            *   `INSERT ... VALUES (true)`, `VALUES (false)`: Success.
            *   `INSERT ... VALUES (1)`, `VALUES (0)` (integer to BOOLEAN): Success. (Presumably stored as true/false).
            *   `INSERT ... VALUES ('true')`, `VALUES ('false')` (string to BOOLEAN): Success. (Behavior unknown).
            *   `INSERT ... VALUES ('yes')` (non-standard boolean string): Success. (Behavior unknown).
            *   `INSERT ... VALUES (NULL)`: Success.
        *   **VARCHAR(10)** (Parser treats as TEXT):
            *   `INSERT ... VALUES ('short')`: Success.
            *   `INSERT ... VALUES ('longervalue')` (11 chars, exceeding "length"): Success. (Length constraint notional, behaves like TEXT).

**Summary of `oxidb` Behavior:**

*   **Constraints (PRIMARY KEY, UNIQUE, NOT NULL) are not currently enforced.** `INSERT` statements that violate these constraints are accepted without error.
*   **Data Visibility is a major issue.** Data inserted is not visible to `SELECT` queries immediately, making it difficult to verify the state of the database after operations.
*   **Data Type Affinity/Conversion is largely untested due to the visibility issue.** While many cross-type inserts reported "Success", the actual stored values could not be inspected.
*   **SQL Parser has limitations:**
    *   `NUMERIC(P,S)` data type is not supported in `CREATE TABLE`.
    *   Negative integers in `INSERT` statements cause a tokenizer/parser error.
*   `VARCHAR(N)` is treated like `TEXT` with no length enforcement observed.

This example highlights several areas where `oxidb`'s SQL layer is still under development and does not yet conform to standard SQL database behaviors regarding constraint enforcement, data type handling, and immediate data visibility.
