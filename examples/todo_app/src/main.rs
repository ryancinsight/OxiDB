use oxidb::Oxidb;
use serde::{Deserialize, Serialize};
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

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TodoItem {
    id: u64,
    description: String,
    done: bool,
}

const TODO_TABLE: &str = "todos";
const DB_PATH: &str = "todo_app.db";

async fn ensure_table_exists(db: &Oxidb) -> Result<(), Box<dyn Error>> {
    if !db.table_exists(TODO_TABLE).await? {
        db.create_table::<TodoItem>(TODO_TABLE, "id").await?;
        println!("Created table '{}'", TODO_TABLE);
    }
    Ok(())
}

async fn add_item(db: &Oxidb, description: String) -> Result<u64, Box<dyn Error>> {
    ensure_table_exists(db).await?;
    let items_table = db.table::<TodoItem>(TODO_TABLE).await?;
    let new_item = TodoItem {
        id: 0, // oxidb will generate an ID
        description,
        done: false,
    };
    let id = items_table.insert(new_item).await?;
    println!("Added item with ID: {}", id);
    Ok(id)
}

async fn list_items(db: &Oxidb) -> Result<(), Box<dyn Error>> {
    ensure_table_exists(db).await?;
    let items_table = db.table::<TodoItem>(TODO_TABLE).await?;
    let all_items = items_table.get_all().await?;
    if all_items.is_empty() {
        println!("No todo items yet!");
    } else {
        println!("Todo items:");
        for item in all_items {
            println!("[{}] {} - {}", if item.done { "x" } else { " " }, item.id, item.description);
        }
    }
    Ok(())
}

async fn mark_done(db: &Oxidb, id: u64) -> Result<(), Box<dyn Error>> {
    ensure_table_exists(db).await?;
    let items_table = db.table::<TodoItem>(TODO_TABLE).await?;
    if let Some(mut item) = items_table.get(id).await? {
        item.done = true;
        items_table.update(id, item).await?;
        println!("Marked item {} as done.", id);
    } else {
        println!("Item with ID {} not found.", id);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    // Open the database. It will be created if it doesn't exist.
    let db = Oxidb::new(DB_PATH).await?;
    println!("Using database at: {}", DB_PATH);


    match cli.command {
        Commands::Add { description } => {
            add_item(&db, description).await?;
        }
        Commands::List {} => {
            list_items(&db).await?;
        }
        Commands::Done { id } => {
            mark_done(&db, id).await?;
        }
    }

    Ok(())
}
