use anyhow::Result;
use clap::Parser;
use oxidb::core::query::executor::ExecutionResult; // For handling query results
use oxidb::core::types::DataType; // For parsing query results
use oxidb::{Oxidb, OxidbError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap; // For parsing map data type
use std::path::Path;
// std::fs and std::io might be needed if reading file content from the filesystem
// For now, content is string argument.

// --- Constants ---
const USERS_TABLE: &str = "users";
const USER_FILES_TABLE: &str = "user_files";
const DB_PATH: &str = "user_auth_files.db";

// --- CLI Structures ---
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
enum Commands {
    /// Initializes the database and its tables.
    InitDb {},
    /// Registers a new user.
    Register {
        #[clap(long)]
        username: String,
        #[clap(long)]
        password: String,
    },
    /// Logs in a user.
    Login {
        #[clap(long)]
        username: String,
        #[clap(long)]
        password: String,
    },
    /// Adds a file for the logged-in user.
    AddFile {
        #[clap(long)] // User ID will be from session
        file_name: String,
        #[clap(long)] // Content as string for now
        content: String,
    },
    /// Lists files for the logged-in user.
    ListFiles {}, // User ID will be from session
    /// Gets a specific file for the logged-in user.
    GetFile {
        // User ID will be from session
        #[clap(long)]
        file_id: u64,
    },
}

// --- User Struct ---
#[derive(Serialize, Deserialize, Debug, Clone)]
struct User {
    id: u64,
    username: String,
}

// --- UserFile Struct ---
#[derive(Serialize, Deserialize, Debug, Clone)]
struct UserFile {
    id: u64,
    user_id: u64,
    file_name: String,
    content: Vec<u8>, // Storing content as bytes
}

// --- Database Initialization ---
fn ensure_tables_exist(db: &mut Oxidb) -> Result<(), OxidbError> {
    // Create Users Table
    let create_users_table_query = format!(
        "CREATE TABLE {} (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL)",
        USERS_TABLE
    );
    match db.execute_query_str(&create_users_table_query) {
        Ok(_) => {}
        Err(e) => {
            if !e.to_string().to_lowercase().contains("already exists")
                && !e.to_string().to_lowercase().contains("duplicate table name")
            {
                eprintln!("Error creating/ensuring table '{}': {:?}", USERS_TABLE, e);
                return Err(e.into());
            }
        }
    }

    // Create UserFiles Table - SQL schema uses BLOB type
    let create_user_files_table_query = format!(
        "CREATE TABLE {} (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, file_name TEXT NOT NULL, file_content BLOB NOT NULL)",
        USER_FILES_TABLE
    );
    match db.execute_query_str(&create_user_files_table_query) {
        Ok(_) => {}
        Err(e) => {
            if !e.to_string().to_lowercase().contains("already exists")
                && !e.to_string().to_lowercase().contains("duplicate table name")
            {
                eprintln!("Error creating/ensuring table '{}': {:?}", USER_FILES_TABLE, e);
                return Err(e.into());
            }
        }
    }
    Ok(())
}

// --- Helper functions for parsing query results ---
fn get_string_from_map(item_map: &HashMap<Vec<u8>, DataType>, key: &str) -> Result<String> {
    item_map
        .get(key.as_bytes())
        .and_then(|data_type| match data_type {
            DataType::String(s) => Some(s.clone()),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("Map missing string key '{}' or not a String", key))
}

fn get_u64_from_map(item_map: &HashMap<Vec<u8>, DataType>, key: &str) -> Result<u64> {
    item_map
        .get(key.as_bytes())
        .and_then(|data_type| match data_type {
            DataType::Integer(i) => Some(*i as u64),
            DataType::String(s) => s.parse::<u64>().ok(),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("Map missing key '{}' or not a u64 compatible type", key))
}

// Updated to expect DataType::RawBytes
fn get_raw_bytes_from_map(item_map: &HashMap<Vec<u8>, DataType>, key: &str) -> Result<Vec<u8>> {
    item_map
        .get(key.as_bytes())
        .and_then(|data_type| match data_type {
            DataType::RawBytes(b) => Some(b.clone()),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("Map missing key '{}' or not RawBytes", key))
}

fn parse_user_files_from_result(values: Vec<DataType>) -> Result<Vec<UserFile>> {
    let mut files = Vec::new();
    if values.is_empty() {
        return Ok(files);
    }
    if values.len() % 2 != 0 {
        return Err(anyhow::anyhow!(
            "Invalid data structure for UserFiles: odd number of values. Expected key-value pairs."
        ));
    }

    for chunk in values.chunks_exact(2) {
        if let DataType::Map(map_data) = &chunk[1] {
            let item_map = &map_data.0;
            files.push(UserFile {
                id: get_u64_from_map(item_map, "id")?,
                user_id: get_u64_from_map(item_map, "user_id")?,
                file_name: get_string_from_map(item_map, "file_name")?,
                content: get_raw_bytes_from_map(item_map, "file_content")?, // Updated call
            });
        } else {
            return Err(anyhow::anyhow!(
                "Expected item data to be a Map for user_files. Found: {:?}",
                chunk[1]
            ));
        }
    }
    Ok(files)
}

// --- Password Hashing ---
fn hash_password(password: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
}

// --- User Management Functions ---
fn register_user(db: &mut Oxidb, username: &str, password: &str) -> Result<()> {
    ensure_tables_exist(db)?;
    let hashed_password = hash_password(password);
    let escaped_username = username.replace("'", "''");
    let escaped_hashed_password = hashed_password.replace("'", "''");

    let query = format!(
        "INSERT INTO {} (username, password_hash) VALUES ('{}', '{}')",
        USERS_TABLE, escaped_username, escaped_hashed_password
    );

    match db.execute_query_str(&query)? {
        ExecutionResult::Success => {
            println!("User '{}' registered successfully.", username);
            Ok(())
        }
        other => Err(anyhow::anyhow!("Failed to register user '{}': {:?}", username, other)),
    }
}

fn login_user(db: &mut Oxidb, username: &str, password: &str) -> Result<Option<User>> {
    ensure_tables_exist(db)?;
    let escaped_username = username.replace("'", "''");
    let query = format!(
        "SELECT id, username, password_hash FROM {} WHERE username = '{}'",
        USERS_TABLE, escaped_username
    );

    match db.execute_query_str(&query)? {
        ExecutionResult::Values(values) => {
            if values.is_empty() {
                println!("Login failed: User '{}' not found.", username);
                return Ok(None);
            }
            if values.len() != 2 {
                return Err(anyhow::anyhow!(
                    "Login error: Unexpected data structure for user '{}'.",
                    username
                ));
            }
            if let DataType::Map(map_data) = &values[1] {
                let item_map = &map_data.0;
                let stored_hash = get_string_from_map(item_map, "password_hash")?;
                if hash_password(password) == stored_hash {
                    Ok(Some(User {
                        id: get_u64_from_map(item_map, "id")?,
                        username: get_string_from_map(item_map, "username")?,
                    }))
                } else {
                    println!("Login failed: Incorrect password for user '{}'.", username);
                    Ok(None)
                }
            } else {
                Err(anyhow::anyhow!(
                    "Login error: Expected user data to be a Map for user '{}'.",
                    username
                ))
            }
        }
        ExecutionResult::Success => {
            println!("Login failed: User '{}' not found (ExecutionResult::Success).", username);
            Ok(None)
        }
        other => Err(anyhow::anyhow!("Login failed for user '{}': {:?}", username, other)),
    }
}

// --- File Management Functions ---
fn add_file(db: &mut Oxidb, user_id: u64, file_name: &str, content: &str) -> Result<()> {
    ensure_tables_exist(db)?;
    let escaped_file_name = file_name.replace("'", "''");
    let content_bytes = content.as_bytes();
    let hex_content = hex::encode(content_bytes); // Content is stored as hex string for X'' literal

    // Using X'' for blob literal, assuming Oxidb SQL parser handles this for BLOB columns
    let query = format!(
        "INSERT INTO {} (user_id, file_name, file_content) VALUES ({}, '{}', X'{}')",
        USER_FILES_TABLE, user_id, escaped_file_name, hex_content
    );

    match db.execute_query_str(&query)? {
        ExecutionResult::Success => {
            println!("File '{}' added successfully for user ID {}.", file_name, user_id);
            Ok(())
        }
        other => Err(anyhow::anyhow!("Failed to add file '{}': {:?}", file_name, other)),
    }
}

fn list_files(db: &mut Oxidb, user_id: u64) -> Result<()> {
    ensure_tables_exist(db)?;
    let query = format!(
        "SELECT id, user_id, file_name, file_content FROM {} WHERE user_id = {}",
        USER_FILES_TABLE, user_id
    );

    match db.execute_query_str(&query)? {
        ExecutionResult::Values(values) => {
            if values.is_empty() {
                println!("No files found for user ID {}.", user_id);
                return Ok(());
            }
            match parse_user_files_from_result(values) {
                Ok(files) => {
                    println!("Files for user ID {}:", user_id);
                    for file in files {
                        let content_preview = String::from_utf8_lossy(&file.content);
                        println!(
                            "- ID: {}, Name: {}, Content Preview (lossy UTF-8): {:.50}{}",
                            file.id,
                            file.file_name,
                            content_preview.chars().take(50).collect::<String>(),
                            if content_preview.len() > 50 { "..." } else { "" }
                        );
                    }
                }
                Err(e) => eprintln!("Error parsing files for user ID {}: {}", user_id, e),
            }
        }
        ExecutionResult::Success => {
            println!("No files found for user ID {} (ExecutionResult::Success).", user_id);
        }
        other => {
            eprintln!("Unexpected result when listing files for user ID {}: {:?}", user_id, other)
        }
    }
    Ok(())
}

fn get_file(db: &mut Oxidb, user_id: u64, file_id: u64) -> Result<Option<UserFile>> {
    ensure_tables_exist(db)?;
    let query = format!(
        "SELECT id, user_id, file_name, file_content FROM {} WHERE id = {} AND user_id = {}",
        USER_FILES_TABLE, file_id, user_id
    );

    match db.execute_query_str(&query)? {
        ExecutionResult::Values(values) => {
            if values.is_empty() {
                return Ok(None); // File not found or not owned by user
            }
            match parse_user_files_from_result(values) {
                Ok(mut files) if !files.is_empty() => Ok(files.pop()), // Should be only one
                Ok(_) => Ok(None),                                     // Parsed but somehow empty
                Err(e) => {
                    Err(anyhow::anyhow!("Error parsing file data for file ID {}: {}", file_id, e))
                }
            }
        }
        ExecutionResult::Success => Ok(None), // No file found
        other => Err(anyhow::anyhow!("Failed to get file ID {}: {:?}", file_id, other)),
    }
}

// --- Main Function ---
fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut current_user_id: Option<u64> = None;

    if let Some(parent_dir) = Path::new(DB_PATH).parent() {
        if !parent_dir.exists() {
            std::fs::create_dir_all(parent_dir)?;
        }
    }
    let mut db = Oxidb::new(DB_PATH)?;

    // Attempt to load session or state if applicable (e.g. from a file)
    // For this example, session is ephemeral and starts empty.

    match cli.command {
        Commands::InitDb {} => {
            ensure_tables_exist(&mut db)?;
            println!("Database tables ensured in '{}'.", DB_PATH);
        }
        Commands::Register { username, password } => {
            register_user(&mut db, &username, &password)?;
        }
        Commands::Login { username, password } => {
            match login_user(&mut db, &username, &password)? {
                Some(user) => {
                    println!("Logged in: User ID: {}, Username: {}", user.id, user.username);
                    current_user_id = Some(user.id);
                    if let Some(id) = current_user_id {
                        println!("Session context now set for user ID: {}.", id);
                        // Artificially "read"
                    }
                    // Here you might save user.id to a temporary session file if you want persistence across commands
                    // For now, it's only for the lifetime of this single command execution.
                    // If another command is run, current_user_id will be None again unless Login is called.
                }
                None => { /* Message already printed by login_user */ }
            }
        }
        Commands::AddFile { file_name, content } => {
            // This command structure implies that login must happen in the same execution
            // or session_user_id must be loaded from a persistent source.
            // For this CLI example, we'll assume login must precede file ops in a single run,
            // or we enhance it to save/load session_user_id.
            // The current_user_id will be None unless Login was called in *this* execution.
            // This is a limitation of simple CLI state. A real app might use a token file.

            // Simulate loading session for non-login commands if we had persistence
            // if current_user_id.is_none() { current_user_id = load_session_id_from_disk_etc(); }

            if let Some(user_id) = current_user_id {
                // Correctly use the variable from the main scope
                add_file(&mut db, user_id, &file_name, &content)?;
            } else {
                println!("Error: You must be logged in to add a file. Please use the 'login' command first in this session.");
            }
        }
        Commands::ListFiles {} => {
            if let Some(user_id) = current_user_id {
                // Correctly use the variable
                list_files(&mut db, user_id)?;
            } else {
                println!("Error: You must be logged in to list files. Please use the 'login' command first in this session.");
            }
        }
        Commands::GetFile { file_id } => {
            if let Some(user_id) = current_user_id {
                // Correctly use the variable
                match get_file(&mut db, user_id, file_id)? {
                    Some(file) => {
                        let content_string = String::from_utf8_lossy(&file.content);
                        println!("--- File ID: {} ---", file.id);
                        println!("Name: {}", file.file_name);
                        println!("--- Content ---");
                        println!("{}", content_string);
                        println!("--- End of Content ---");
                    }
                    None => {
                        println!(
                            "File ID '{}' not found or you do not have permission to view it.",
                            file_id
                        );
                    }
                }
            } else {
                println!("Error: You must be logged in to get a file. Please use the 'login' command first in this session.");
            }
        }
    }

    // Persist changes at the end of any command that modifies the DB
    // Note: `ensure_tables_exist` might attempt to create tables, which is a modification.
    // `register_user`, `add_file` are explicit modifications.
    // `login_user`, `list_files`, `get_file` are read-only.
    // `db.persist()` should ideally be called only if actual writes happened.
    // However, for simplicity, calling it always is fine for this example,
    // as Oxidb might have mechanisms to avoid unnecessary writes.
    db.persist().map_err(|e| anyhow::anyhow!("Failed to persist database changes: {}", e))?;
    Ok(())
}
