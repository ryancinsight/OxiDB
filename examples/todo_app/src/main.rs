use clap::Parser;
use oxidb::{Oxidb, OxidbError};
use serde::{Deserialize, Serialize};
use std::error::Error;

// For parsing results from oxidb
use oxidb::core::query::executor::ExecutionResult;
// Use the DataType that holds values, from core::types
// use oxidb::core::types::DataType; // This was unused

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
enum Commands {
    Add { description: String },
    List {},
    Done { id: u64 },
    Delete { id: u64 },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TodoItem {
    id: u64,
    description: String,
    done: bool,
}

const TODO_TABLE: &str = "todos";
const DB_PATH: &str = "todo_app.db";

fn ensure_table_exists(db: &mut Oxidb) -> Result<(), Box<dyn Error>> {
    let query = format!(
        "CREATE TABLE {} (id INTEGER PRIMARY KEY AUTOINCREMENT, description TEXT, done BOOLEAN)",
        TODO_TABLE
    );
    // It's possible CREATE TABLE IF NOT EXISTS is not supported.
    // If so, this will error if table exists. We might need to catch specific error.
    match db.execute_query_str(&query) {
        Ok(_) => {
            // oxidb's CREATE TABLE seems to be idempotent or behave like IF NOT EXISTS,
            // as it doesn't error if the table already exists.
            println!("Table '{}' ensured (created if not existing).", TODO_TABLE);
            Ok(())
        }
        Err(e) => {
            // This arm would handle other errors during table creation,
            // if CREATE TABLE could fail for reasons other than "already exists".
            eprintln!("Error ensuring table '{}' exists: {:?}", TODO_TABLE, e);
            Err(Box::new(e))
        }
    }
}

fn delete_item(db: &mut Oxidb, id: u64) -> Result<(), Box<dyn Error>> {
    ensure_table_exists(db)?;
    let query = format!("DELETE FROM {} WHERE id = {}", TODO_TABLE, id);

    match db.execute_query_str(&query) {
        Ok(ExecutionResult::Updated { count }) => {
            // SQL DELETE returns Updated variant
            if count > 0 {
                println!("Successfully deleted item {}. ({} row(s) affected)", id, count);
            } else {
                println!("Item with ID {} not found.", id); // Or no rows matched the condition
            }
            Ok(())
        }
        Ok(other_result) => {
            eprintln!("Unexpected result from SQL DELETE: {:?}", other_result);
            Err(Box::new(OxidbError::Internal("Unexpected SQL DELETE result".into())))
        }
        Err(e) => {
            eprintln!("Error deleting item {}: {:?}", id, e);
            Err(Box::new(e))
        }
    }
}

fn add_item(db: &mut Oxidb, description: String) -> Result<(), Box<dyn Error>> {
    ensure_table_exists(db)?;

    // Escape backslashes first, then single quotes for SQL
    let escaped_description = description.replace("\\", "\\\\").replace("'", "\\'");

    let query = format!(
        "INSERT INTO {} (description, done) VALUES ('{}', false)",
        TODO_TABLE, escaped_description
    );

    match db.execute_query_str(&query) {
        Ok(ExecutionResult::Success) => {
            println!(
                "Added item with description: '{}'. (ID not displayed due to current limitations).",
                description
            );
            // LIMITATION: oxidb currently does not support standard SQL mechanisms for retrieving
            // the last inserted ID (e.g., `RETURNING id` or `LAST_INSERT_ID()`).
            // Thus, the ID of the newly added item cannot be immediately displayed.
            // A previous attempt using `SELECT MAX(id)` also failed due to parser/planner limitations.
            Ok(())
        }
        Ok(other_result) => {
            eprintln!("Unexpected result from INSERT: {:?}", other_result);
            Err(Box::new(OxidbError::Internal("Unexpected INSERT result".into())))
        }
        Err(e) => {
            eprintln!("Error adding item: {:?}", e);
            Err(Box::new(e))
        }
    }
}

fn list_items(db: &mut Oxidb) -> Result<(), Box<dyn Error>> {
    ensure_table_exists(db)?;
    // Changed to SELECT * due to planner expecting numeric indices for projection
    let query = format!("SELECT * FROM {}", TODO_TABLE);

    match db.execute_query_str(&query) {
        Ok(ExecutionResult::Values(values)) => {
            if values.is_empty() {
                println!("No todo items yet!");
                return Ok(());
            }

            let mut items = Vec::new();
            // Values are flat: [kv_key1, map1, kv_key2, map2, ...]
            // We expect 2 fields per item from the store's perspective (key, value_map).
            if values.len() % 2 != 0 {
                eprintln!(
                    "Error: Number of values ({}) is not a multiple of 2 (key, map per item).",
                    values.len()
                );
                return Err(Box::new(OxidbError::Internal(
                    "Invalid data structure from SELECT *".into(),
                )));
            }

            for chunk in values.chunks_exact(2) {
                // chunk[0] is the kv_key (e.g., Uuid based, or DataType::String/Blob)
                // chunk[1] should be the DataType::Map
                let item_map = match &chunk[1] {
                    oxidb::core::types::DataType::Map(map_data) => &map_data.0, // .0 to get the inner HashMap
                    _ => {
                        return Err(Box::new(OxidbError::Internal(
                            "Expected item data to be a Map".into(),
                        )));
                    }
                };

                // Extract fields from the map by their string keys (converted to Vec<u8>)
                let id_key = "id".as_bytes().to_vec();
                let description_key = "description".as_bytes().to_vec();
                let done_key = "done".as_bytes().to_vec();

                let id = match item_map.get(&id_key) {
                    Some(oxidb::core::types::DataType::Integer(i)) => *i as u64,
                    _ => {
                        return Err(Box::new(OxidbError::Internal(
                            "Map missing 'id' or not an integer".into(),
                        )));
                    }
                };
                let description = match item_map.get(&description_key) {
                    Some(oxidb::core::types::DataType::String(s)) => s.clone(),
                    _ => {
                        return Err(Box::new(OxidbError::Internal(
                            "Map missing 'description' or not a string".into(),
                        )));
                    }
                };
                let done = match item_map.get(&done_key) {
                    Some(oxidb::core::types::DataType::Boolean(b)) => *b,
                    _ => {
                        return Err(Box::new(OxidbError::Internal(
                            "Map missing 'done' or not a boolean".into(),
                        )));
                    }
                };
                items.push(TodoItem { id, description, done });
            }

            if items.is_empty() {
                // Should be caught by values.is_empty() earlier, but good for safety.
                println!("No todo items yet!");
            } else {
                println!("Todo items:");
                for item in items {
                    println!(
                        "[{}] {} - {}",
                        if item.done { "x" } else { " " },
                        item.id,
                        item.description
                    );
                }
            }
            Ok(())
        }
        Ok(other_result) => {
            eprintln!("Unexpected result type from list_items query: {:?}", other_result);
            Err(Box::new(OxidbError::Internal("Unexpected result type for list".into())))
        }
        Err(e) => {
            eprintln!("Error listing items: {:?}", e);
            Err(Box::new(e))
        }
    }
}

fn mark_done(db: &mut Oxidb, id: u64) -> Result<(), Box<dyn Error>> {
    ensure_table_exists(db)?;
    let query = format!("UPDATE {} SET done = true WHERE id = {}", TODO_TABLE, id);

    match db.execute_query_str(&query) {
        Ok(ExecutionResult::Updated { count }) => {
            if count > 0 {
                println!("Marked item {} as done.", id);
            } else {
                println!("Item with ID {} not found or already done.", id); // Update may not change if already true
            }
            Ok(())
        }
        Ok(other_result) => {
            eprintln!("Unexpected result from UPDATE: {:?}", other_result);
            Err(Box::new(OxidbError::Internal("Unexpected UPDATE result".into())))
        }
        Err(e) => {
            eprintln!("Error marking item as done: {:?}", e);
            Err(Box::new(e))
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let mut db = Oxidb::new(DB_PATH)?;
    println!("Using database at: {}", DB_PATH);

    match cli.command {
        Commands::Add { description } => {
            add_item(&mut db, description)?;
        }
        Commands::List {} => {
            list_items(&mut db)?;
        }
        Commands::Done { id } => {
            mark_done(&mut db, id)?;
        }
        Commands::Delete { id } => {
            delete_item(&mut db, id)?;
        }
    }

    db.persist().map_err(|e| Box::new(e) as Box<dyn Error>)?;
    Ok(())
}
