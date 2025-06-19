use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::Parser;
use oxidb::{core::query::executor::ExecutionResult, Oxidb, OxidbError};
use serde::{Deserialize, Serialize};
use std::path::Path;
// Removed: use uuid::Uuid;

// --- CLI Configuration ---
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
enum Commands {
    /// Initializes the database by creating necessary tables.
    InitDb {},
    /// Adds a new author.
    AddAuthor {
        #[clap(long)]
        name: String,
    },
    /// Lists all authors.
    ListAuthors {},
    /// Deletes an author and all their posts.
    DeleteAuthor {
        #[clap(long)]
        id: String, // Will be parsed to u64
    },
    /// Adds a new post.
    AddPost {
        #[clap(long)]
        author_id: String, // Will be parsed to u64
        #[clap(long)]
        title: String,
        #[clap(long)]
        content: Option<String>,
    },
    /// Lists all posts.
    ListPosts {},
    /// Lists posts by a specific author.
    ListPostsByAuthor {
        #[clap(long)]
        author_id: String, // Will be parsed to u64
    },
    /// Gets details of a specific post.
    GetPost {
        #[clap(long)]
        id: String, // Will be parsed to u64
    },
    /// Publishes a post by setting its publication date.
    PublishPost {
        #[clap(long)]
        id: String, // Will be parsed to u64
    },
    /// Deletes a post.
    DeletePost {
        #[clap(long)]
        id: String, // Will be parsed to u64
    },
}

// --- Data Structures ---
#[derive(Serialize, Deserialize, Debug, Clone)]
struct Author {
    id: u64, // Changed to u64
    name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Post {
    id: u64,        // Changed to u64
    author_id: u64, // Changed to u64
    title: String,
    content: String,
    published_date: Option<DateTime<Utc>>,
}

// Helper function to extract string value from DataType Map
fn get_string_from_map(
    item_map: &std::collections::HashMap<Vec<u8>, oxidb::core::types::DataType>,
    key: &str,
) -> Result<String> {
    item_map
        .get(key.as_bytes())
        .and_then(|data_type| match data_type {
            oxidb::core::types::DataType::String(s) => Some(s.clone()),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("Map missing string key '{}' or not a String", key))
}

// Helper function to extract u64 value from DataType Map
fn get_u64_from_map(
    item_map: &std::collections::HashMap<Vec<u8>, oxidb::core::types::DataType>,
    key: &str,
) -> Result<u64> {
    item_map
        .get(key.as_bytes())
        .and_then(|data_type| match data_type {
            oxidb::core::types::DataType::Integer(i) => Some(*i as u64),
            oxidb::core::types::DataType::String(s) => s.parse::<u64>().ok(),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("Map missing key '{}' or not a u64 compatible type", key))
}

fn get_optional_string_from_map(
    item_map: &std::collections::HashMap<Vec<u8>, oxidb::core::types::DataType>,
    key: &str,
) -> Result<Option<String>> {
    match item_map.get(key.as_bytes()) {
        Some(oxidb::core::types::DataType::String(s)) => Ok(Some(s.clone())),
        Some(oxidb::core::types::DataType::Null) => Ok(None), // Handle SQL NULL as Option::None
        None => Ok(None), // Key not present means None for optional fields
        some_other_type => {
            Err(anyhow::anyhow!("Map key '{}' is not a String or Null: {:?}", key, some_other_type))
        }
    }
}

// Helper to parse ExecutionResult::Values into Vec<Author>
fn parse_authors_from_result(values: Vec<oxidb::core::types::DataType>) -> Result<Vec<Author>> {
    let mut authors = Vec::new();
    if values.is_empty() {
        return Ok(authors);
    }
    if values.len() % 2 != 0 {
        return Err(anyhow::anyhow!("Invalid data structure for Authors: odd number of values"));
    }

    for chunk in values.chunks_exact(2) {
        // Assuming chunk[0] is the KV store key, chunk[1] is the map of columns.
        // For AUTOINCREMENT INTEGER PRIMARY KEY, oxidb *should* ideally include 'id' in the map.
        if let oxidb::core::types::DataType::Map(map_data) = &chunk[1] {
            let item_map = &map_data.0;
            authors.push(Author {
                id: get_u64_from_map(item_map, "id")?, // Expect 'id' in the map.
                name: get_string_from_map(item_map, "name")?,
            });
        } else {
            return Err(anyhow::anyhow!("Expected item data to be a Map for authors"));
        }
    }
    Ok(authors)
}

// Helper to parse ExecutionResult::Values into Vec<Post>
fn parse_posts_from_result(values: Vec<oxidb::core::types::DataType>) -> Result<Vec<Post>> {
    let mut posts = Vec::new();
    if values.is_empty() {
        return Ok(posts);
    }
    if values.len() % 2 != 0 {
        return Err(anyhow::anyhow!("Invalid data structure for Posts: odd number of values"));
    }

    for chunk in values.chunks_exact(2) {
        // Assuming chunk[0] is the KV store key, chunk[1] is the map of columns.
        if let oxidb::core::types::DataType::Map(map_data) = &chunk[1] {
            let item_map = &map_data.0;
            let published_date_str = get_optional_string_from_map(item_map, "published_date")?;
            let published_date = published_date_str
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                .map(|dt| dt.with_timezone(&Utc));

            posts.push(Post {
                id: get_u64_from_map(item_map, "id")?, // Expect 'id' in the map.
                author_id: get_u64_from_map(item_map, "author_id")?,
                title: get_string_from_map(item_map, "title")?,
                content: get_string_from_map(item_map, "content")?,
                published_date,
            });
        } else {
            return Err(anyhow::anyhow!("Expected item data to be a Map for posts"));
        }
    }
    Ok(posts)
}

// --- Constants ---
const AUTHORS_TABLE: &str = "authors";
const POSTS_TABLE: &str = "posts";
const DB_PATH: &str = "simple_blog.db";

// --- Database Initialization ---
fn ensure_tables_exist(db: &mut Oxidb) -> Result<(), OxidbError> {
    // Create Authors Table
    let create_authors_table_query = format!(
        "CREATE TABLE {} (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE)",
        AUTHORS_TABLE
    );
    match db.execute_query_str(&create_authors_table_query) {
        Ok(_) => {
            println!("Table '{}' ensured (created or already existed).", AUTHORS_TABLE);
        }
        Err(e) => {
            // Based on previous experience, oxidb's CREATE TABLE seems idempotent.
            // If an error occurs here, it's likely not "table already exists" but something else.
            eprintln!("Error creating/ensuring table '{}': {:?}", AUTHORS_TABLE, e);
            return Err(e.into()); // Convert OxidbError to anyhow::Error
        }
    }

    // Create Posts Table
    // NOTE: FOREIGN KEY constraint removed. IDs changed to INTEGER.
    let create_posts_table_query = format!(
        "CREATE TABLE {} (id INTEGER PRIMARY KEY AUTOINCREMENT, author_id INTEGER NOT NULL, title TEXT NOT NULL, content TEXT, published_date TEXT)",
        POSTS_TABLE
    );
    match db.execute_query_str(&create_posts_table_query) {
        Ok(_) => {
            println!("Table '{}' ensured (created or already existed).", POSTS_TABLE);
        }
        Err(e) => {
            eprintln!("Error creating/ensuring table '{}': {:?}", POSTS_TABLE, e);
            return Err(e.into()); // Convert OxidbError to anyhow::Error
        }
    }
    Ok(())
}

// --- Post Management Functions ---

fn add_post(db: &mut Oxidb, author_id: u64, title: &str, content: Option<&str>) -> Result<()> {
    ensure_tables_exist(db)?;

    // Check if author exists
    if get_author(db, author_id)?.is_none() {
        return Err(anyhow::anyhow!("Author with ID '{}' not found.", author_id));
    }

    let escaped_title = title.replace("'", "''");
    let escaped_content = content.unwrap_or("").replace("'", "''");

    // ID is AUTOINCREMENT, published_date is initially NULL
    let query = format!(
        "INSERT INTO {} (author_id, title, content, published_date) VALUES ({}, '{}', '{}', NULL)",
        POSTS_TABLE, author_id, escaped_title, escaped_content
    );

    match db.execute_query_str(&query)? {
        ExecutionResult::Success => {
            // LIMITATION: Cannot get last inserted post ID.
            println!("Added post '{}'. (ID generated by DB)", title);
        }
        other => {
            return Err(anyhow::anyhow!("Unexpected result from INSERT post: {:?}", other));
        }
    }
    Ok(())
}

fn list_posts(db: &mut Oxidb) -> Result<()> {
    ensure_tables_exist(db)?;
    // Fetch all authors first for easier name lookup
    let all_authors_query = format!("SELECT * FROM {}", AUTHORS_TABLE);
    let authors_map: std::collections::HashMap<u64, String> = // Changed Key type to u64
        match db.execute_query_str(&all_authors_query)? {
            ExecutionResult::Values(author_values) => {
                parse_authors_from_result(author_values)?
                    .into_iter()
                    .map(|a| (a.id, a.name)) // a.id is u64, a.name is String
                    .collect()
            }
            _ => std::collections::HashMap::new(),
        };

    let query = format!("SELECT * FROM {}", POSTS_TABLE);
    match db.execute_query_str(&query)? {
        ExecutionResult::Values(values) => {
            if values.is_empty() {
                println!("No posts found.");
                return Ok(());
            }
            match parse_posts_from_result(values) {
                Ok(posts) => {
                    println!("Posts:");
                    for post in posts {
                        let author_name = authors_map
                            .get(&post.author_id)
                            .cloned()
                            .unwrap_or_else(|| "Unknown Author".to_string());
                        println!(
                            "- ID: {}\n  Title: {}\n  Author: {} (ID: {})\n  Published: {}\n  Content: {:.100}{}\n---",
                            post.id,
                            post.title,
                            author_name,
                            post.author_id,
                            post.published_date.map_or("Not Published".to_string(), |d| d.to_rfc3339()),
                            post.content,
                            if post.content.len() > 100 { "..." } else { "" }
                        );
                    }
                }
                Err(e) => eprintln!("Error parsing posts: {}", e),
            }
        }
        other => eprintln!("Unexpected result from SELECT posts: {:?}", other),
    }
    Ok(())
}

fn list_posts_by_author(db: &mut Oxidb, author_id: u64) -> Result<()> {
    ensure_tables_exist(db)?;
    let author = match get_author(db, author_id)? {
        Some(a) => a,
        None => {
            println!("Author with ID '{}' not found.", author_id);
            return Ok(());
        }
    };
    println!("Posts by Author: {} (ID: {})", author.name, author.id);

    let query = format!("SELECT * FROM {} WHERE author_id = {}", POSTS_TABLE, author_id); // No escape for u64

    match db.execute_query_str(&query)? {
        ExecutionResult::Values(values) => {
            if values.is_empty() {
                println!("No posts found for this author.");
                return Ok(());
            }
            match parse_posts_from_result(values) {
                Ok(posts) => {
                    for post in posts {
                        println!(
                            "- ID: {}\n  Title: {}\n  Published: {}\n  Content: {:.100}{}\n---",
                            post.id,
                            post.title,
                            post.published_date
                                .map_or("Not Published".to_string(), |d| d.to_rfc3339()),
                            post.content,
                            if post.content.len() > 100 { "..." } else { "" }
                        );
                    }
                }
                Err(e) => eprintln!("Error parsing posts for author: {}", e),
            }
        }
        other => eprintln!("Unexpected result from SELECT posts by author: {:?}", other),
    }
    Ok(())
}

fn get_post_details(db: &mut Oxidb, post_id: u64) -> Result<()> {
    ensure_tables_exist(db)?;
    let query = format!("SELECT * FROM {} WHERE id = {}", POSTS_TABLE, post_id); // No escape for u64

    let post_opt = match db.execute_query_str(&query)? {
        ExecutionResult::Values(values) => {
            if values.is_empty() {
                println!("Post with ID '{}' not found.", post_id);
                return Ok(());
            }
            parse_posts_from_result(values)?.pop()
        }
        _ => {
            println!("Failed to retrieve post with ID '{}'.", post_id);
            return Ok(());
        }
    };

    if let Some(p) = post_opt {
        let author_name = match get_author(db, p.author_id)? {
            // p.author_id is u64
            Some(a) => a.name,
            None => "Unknown Author".to_string(),
        };
        println!("Post Details:");
        println!("ID: {}", p.id);
        println!("Title: {}", p.title);
        println!("Author: {} (ID: {})", author_name, p.author_id);
        println!(
            "Published: {}",
            p.published_date.map_or("Not Published".to_string(), |d| d.to_rfc3339())
        );
        println!("Content:\n{}", p.content);
    } else {
        println!("Post with ID '{}' not found after parsing.", post_id);
    }
    Ok(())
}

fn publish_post(db: &mut Oxidb, post_id: u64) -> Result<()> {
    ensure_tables_exist(db)?;
    let now_utc = Utc::now().to_rfc3339(); // ISO 8601 format

    let query = format!(
        "UPDATE {} SET published_date = '{}' WHERE id = {}", // No escape for u64 post_id
        POSTS_TABLE, now_utc, post_id
    );

    match db.execute_query_str(&query)? {
        ExecutionResult::Updated { count } => {
            if count > 0 {
                println!("Post {} published successfully at {}.", post_id, now_utc);
            } else {
                println!("Post with ID {} not found for publishing.", post_id);
            }
        }
        other => return Err(anyhow::anyhow!("Unexpected result from UPDATE post: {:?}", other)),
    }
    Ok(())
}

fn delete_post(db: &mut Oxidb, post_id: u64) -> Result<()> {
    ensure_tables_exist(db)?;
    let query = format!("DELETE FROM {} WHERE id = {}", POSTS_TABLE, post_id); // No escape for u64

    match db.execute_query_str(&query)? {
        ExecutionResult::Updated { count } => {
            if count > 0 {
                println!("Successfully deleted post {}.", post_id);
            } else {
                println!("Post with ID {} not found.", post_id);
            }
        }
        other => return Err(anyhow::anyhow!("Unexpected result from DELETE post: {:?}", other)),
    }
    Ok(())
}

// --- Author Management Functions ---
fn list_authors(db: &mut Oxidb) -> Result<()> {
    ensure_tables_exist(db)
        .map_err(|e| anyhow::anyhow!("DB setup error for list_authors: {}", e))?;
    let query_star = format!("SELECT * FROM {}", AUTHORS_TABLE);

    match db.execute_query_str(&query_star)? {
        ExecutionResult::Values(values) => {
            if values.is_empty() {
                println!("No authors found.");
                return Ok(());
            }
            match parse_authors_from_result(values) {
                Ok(authors) => {
                    println!("Authors:");
                    for author in authors {
                        println!("- ID: {}, Name: {}", author.id, author.name);
                    }
                }
                Err(e) => {
                    eprintln!("Error parsing authors: {}", e);
                }
            }
        }
        other_result => {
            eprintln!("Unexpected result from SELECT authors: {:?}", other_result);
        }
    }
    Ok(())
}

fn get_author(db: &mut Oxidb, author_id: u64) -> Result<Option<Author>> {
    // author_id is u64
    ensure_tables_exist(db)?;
    let query = format!("SELECT * FROM {} WHERE id = {}", AUTHORS_TABLE, author_id); // No need to escape u64

    match db.execute_query_str(&query)? {
        ExecutionResult::Values(values) => {
            if values.is_empty() {
                return Ok(None);
            }
            match parse_authors_from_result(values) {
                Ok(mut authors) if !authors.is_empty() => Ok(Some(authors.remove(0))),
                Ok(_) => Ok(None),
                Err(e) => Err(anyhow::anyhow!("Error parsing author data: {}", e)),
            }
        }
        _ => Ok(None),
    }
}

fn delete_author(db: &mut Oxidb, author_id: u64) -> Result<()> {
    // author_id is u64
    ensure_tables_exist(db)?;

    // 1. Delete posts by this author (manual cascade)
    let delete_posts_query = format!(
        "DELETE FROM {} WHERE author_id = {}", // No need to escape u64
        POSTS_TABLE, author_id
    );
    match db.execute_query_str(&delete_posts_query)? {
        ExecutionResult::Updated { count } => {
            println!("Deleted {} post(s) by author {}.", count, author_id);
        }
        other => {
            eprintln!(
                "Unexpected result when deleting posts for author {}: {:?}",
                author_id, other
            );
        }
    }

    // 2. Delete the author
    let delete_author_query = format!(
        "DELETE FROM {} WHERE id = {}", // No need to escape u64
        AUTHORS_TABLE, author_id
    );
    match db.execute_query_str(&delete_author_query)? {
        ExecutionResult::Updated { count } => {
            if count > 0 {
                println!("Successfully deleted author {}.", author_id);
            } else {
                println!("Author with ID {} not found.", author_id);
            }
        }
        other => {
            return Err(anyhow::anyhow!(
                "Unexpected result when deleting author {}: {:?}",
                author_id,
                other
            ));
        }
    }
    Ok(())
}

// --- Main Application Logic ---
fn main() -> Result<()> {
    let cli = Cli::parse();

    // Ensure the database directory exists if DB_PATH includes directories
    if let Some(parent_dir) = Path::new(DB_PATH).parent() {
        if !parent_dir.exists() {
            std::fs::create_dir_all(parent_dir)?;
        }
    }

    let mut db = Oxidb::new(DB_PATH)?;
    println!("Using database at: {}", DB_PATH);

    match cli.command {
        Commands::InitDb {} => {
            ensure_tables_exist(&mut db)?;
            println!("Database initialized successfully.");
        }
        Commands::AddAuthor { name } => {
            ensure_tables_exist(&mut db).map_err(|e| anyhow::anyhow!("DB setup error: {}", e))?;
            let escaped_name = name.replace("'", "''");
            let query = format!("INSERT INTO {} (name) VALUES ('{}')", AUTHORS_TABLE, escaped_name);

            match db.execute_query_str(&query)? {
                ExecutionResult::Success => {
                    println!("Added author: '{}'. (ID generated by DB)", name);
                    println!("--- Attempting to list authors immediately (same DB instance) ---");
                    list_authors(&mut db)?; // Call list_authors immediately
                }
                other_result => {
                    eprintln!("Unexpected result from INSERT author: {:?}", other_result);
                }
            }
        }
        Commands::ListAuthors {} => {
            list_authors(&mut db)?;
        }
        Commands::DeleteAuthor { id } => {
            let author_id_u64 = id
                .parse::<u64>()
                .map_err(|_| anyhow::anyhow!("Invalid author ID format: {}", id))?;
            delete_author(&mut db, author_id_u64)?;
        }
        Commands::AddPost { author_id, title, content } => {
            let author_id_u64 = author_id
                .parse::<u64>()
                .map_err(|_| anyhow::anyhow!("Invalid author ID format for post: {}", author_id))?;
            add_post(&mut db, author_id_u64, &title, content.as_deref())?;
        }
        Commands::ListPosts {} => {
            list_posts(&mut db)?;
        }
        Commands::ListPostsByAuthor { author_id } => {
            let author_id_u64 = author_id
                .parse::<u64>()
                .map_err(|_| anyhow::anyhow!("Invalid author ID format: {}", author_id))?;
            list_posts_by_author(&mut db, author_id_u64)?;
        }
        Commands::GetPost { id } => {
            let post_id_u64 =
                id.parse::<u64>().map_err(|_| anyhow::anyhow!("Invalid post ID format: {}", id))?;
            get_post_details(&mut db, post_id_u64)?;
        }
        Commands::PublishPost { id } => {
            let post_id_u64 =
                id.parse::<u64>().map_err(|_| anyhow::anyhow!("Invalid post ID format: {}", id))?;
            publish_post(&mut db, post_id_u64)?;
        }
        Commands::DeletePost { id } => {
            let post_id_u64 =
                id.parse::<u64>().map_err(|_| anyhow::anyhow!("Invalid post ID format: {}", id))?;
            delete_post(&mut db, post_id_u64)?;
        }
    }

    db.persist().map_err(|e| anyhow::anyhow!("Failed to persist database: {}", e))?;
    Ok(())
}
