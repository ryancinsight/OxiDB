use assert_cmd::prelude::*; // Add methods on commands
use predicates::prelude::*; // Used for writing assertions
use std::process::Command; // Run programs
use std::fs;
// use uuid::Uuid; // Removed unused import
use tempfile::NamedTempFile;

// Helper function to get the path to the binary
fn get_todo_app_cmd() -> Command {
    Command::cargo_bin("todo_app").unwrap()
}

// Helper function to create a unique temporary database file path name.
// The actual files (db_path and db_path.wal) will be created by oxidb.
// NamedTempFile is used here to get a unique path; the temp file itself is not used by oxidb.
fn get_unique_db_path_name() -> String {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file for DB name");
    let path = temp_file.path().to_str().unwrap().to_string();
    // temp_file goes out of scope here and is deleted, but we have its path string.
    path
}


#[test]
fn test_add_item() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = get_unique_db_path_name();
    let mut cmd = get_todo_app_cmd();

    cmd.env("OXIDB_PATH", &db_path);
    cmd.arg("add")
        .arg("Test item 1 from integration test");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Added item with ID: 1"));

    // Cleanup
    let _ = fs::remove_file(&db_path); // Use `let _ =` to ignore error if file doesn't exist
    let _ = fs::remove_file(format!("{}.wal", &db_path));

    Ok(())
}

#[test]
fn test_list_items_empty() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = get_unique_db_path_name();
    let mut cmd = get_todo_app_cmd();

    cmd.env("OXIDB_PATH", &db_path);
    cmd.arg("list");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("No todo items yet!"));

    // Cleanup
    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(format!("{}.wal", &db_path));

    Ok(())
}

#[test]
fn test_add_and_list_items() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = get_unique_db_path_name();
    let mut cmd_add = get_todo_app_cmd();

    cmd_add.env("OXIDB_PATH", &db_path);
    cmd_add.arg("add").arg("Buy groceries for test");
    cmd_add.assert().success().stdout(predicate::str::contains("Added item with ID: 1"));

    let mut cmd_add_2 = get_todo_app_cmd();
    cmd_add_2.env("OXIDB_PATH", &db_path);
    cmd_add_2.arg("add").arg("Call Alice for test");
    cmd_add_2.assert().success().stdout(predicate::str::contains("Added item with ID: 2"));

    let mut cmd_list = get_todo_app_cmd();
    cmd_list.env("OXIDB_PATH", &db_path);
    cmd_list.arg("list");

    cmd_list.assert()
        .success()
        .stdout(predicate::str::contains("Using database at:").and( // Check for DB path message
                predicate::str::contains("[ ] 1 - Buy groceries for test").and(
                predicate::str::contains("[ ] 2 - Call Alice for test"))));

    // Cleanup
    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(format!("{}.wal", &db_path));

    Ok(())
}

#[test]
fn test_add_list_mark_done_list_items() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = get_unique_db_path_name();

    // 1. Add item
    let mut cmd_add = get_todo_app_cmd();
    cmd_add.env("OXIDB_PATH", &db_path);
    cmd_add.arg("add").arg("Learn Rust testing");
    cmd_add.assert().success().stdout(predicate::str::contains("Added item with ID: 1"));

    // 2. List items (item should be not done)
    let mut cmd_list_1 = get_todo_app_cmd();
    cmd_list_1.env("OXIDB_PATH", &db_path);
    cmd_list_1.arg("list");
    cmd_list_1.assert()
        .success()
        .stdout(predicate::str::contains("[ ] 1 - Learn Rust testing"));

    // 3. Mark item as done
    let mut cmd_done = get_todo_app_cmd();
    cmd_done.env("OXIDB_PATH", &db_path);
    cmd_done.arg("done").arg("1");
    cmd_done.assert().success().stdout(predicate::str::contains("Marked item 1 as done"));

    // 4. List items again (item should be done)
    let mut cmd_list_2 = get_todo_app_cmd();
    cmd_list_2.env("OXIDB_PATH", &db_path);
    cmd_list_2.arg("list");
    cmd_list_2.assert()
        .success()
        .stdout(predicate::str::contains("[x] 1 - Learn Rust testing"));

    // Cleanup
    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(format!("{}.wal", &db_path));

    Ok(())
}

#[test]
fn test_mark_done_non_existent_item() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = get_unique_db_path_name();
    let mut cmd = get_todo_app_cmd();

    cmd.env("OXIDB_PATH", &db_path); // Set env var for this command too
    cmd.arg("done").arg("99"); // Assuming item 99 does not exist

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Item with ID 99 not found"));

    // Cleanup
    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(format!("{}.wal", &db_path));

    Ok(())
}
