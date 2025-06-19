use oxidb::Oxidb; // Assuming Oxidb::new is not async
use serde::{Deserialize, Serialize};
use std::env;
use std::error::Error;
use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
enum Commands {
    /// Adds a new todo item
    Add {
        #[clap(value_parser)]
        description: String,
    },
    /// Lists all todo items
    List {},
    /// Marks a todo item as done
    Done {
        #[clap(value_parser)]
        id: u64,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct TodoItem {
    id: u64,
    description: String,
    done: bool,
}

const ALL_TODOS_KEY: &[u8] = b"all_todos"; // Use a byte slice for keys

// Helper to load all items from the DB
fn load_all_items(db: &mut Oxidb) -> Result<Vec<TodoItem>, Box<dyn Error>> {
    match db.get(ALL_TODOS_KEY.to_vec())? {
        Some(json_data) => {
            if json_data.is_empty() {
                Ok(Vec::new())
            } else {
                let items: Vec<TodoItem> = serde_json::from_str(&json_data)?;
                Ok(items)
            }
        }
        None => Ok(Vec::new()), // No items yet, return empty list
    }
}

// Helper to save all items to the DB
fn save_all_items(db: &mut Oxidb, items: &Vec<TodoItem>) -> Result<(), Box<dyn Error>> {
    let json_data = serde_json::to_string(items)?;
    db.insert(ALL_TODOS_KEY.to_vec(), json_data)?;
    db.persist()?; // Persist changes immediately for simplicity in example
    Ok(())
}

// No longer async, returns Result directly
fn add_item(db: &mut Oxidb, description: String) -> Result<u64, Box<dyn Error>> {
    let mut items = load_all_items(db)?;

    // Determine next ID
    let next_id = items.iter().map(|item| item.id).max().unwrap_or(0) + 1;

    let new_item = TodoItem {
        id: next_id,
        description,
        done: false,
    };
    items.push(new_item);
    save_all_items(db, &items)?;

    println!("Added item with ID: {}", next_id);
    Ok(next_id)
}

// No longer async
fn list_items(db: &mut Oxidb) -> Result<(), Box<dyn Error>> {
    let items = load_all_items(db)?;
    if items.is_empty() {
        println!("No todo items yet!");
    } else {
        println!("Todo items:");
        for item in items {
            println!("[{}] {} - {}", if item.done { "x" } else { " " }, item.id, item.description);
        }
    }
    Ok(())
}

// No longer async
fn mark_done(db: &mut Oxidb, id_to_mark: u64) -> Result<(), Box<dyn Error>> {
    let mut items = load_all_items(db)?;

    let mut found = false;
    for item in items.iter_mut() {
        if item.id == id_to_mark {
            item.done = true;
            found = true;
            break;
        }
    }

    if found {
        save_all_items(db, &items)?;
        println!("Marked item {} as done.", id_to_mark);
    } else {
        println!("Item with ID {} not found.", id_to_mark);
    }
    Ok(())
}

// Main function is no longer async, as Oxidb operations are sync
fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    let db_path = env::var("OXIDB_PATH").unwrap_or_else(|_| "todo_app.db".to_string());

    // Oxidb::new is not async
    let mut db = Oxidb::new(&db_path)?;
    println!("Using database at: {}", db_path);

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
    }

    Ok(())
}
