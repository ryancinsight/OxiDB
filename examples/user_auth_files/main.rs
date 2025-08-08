use anyhow::Result;
use clap::Parser;
use oxidb::{Connection, OxidbError};
use serde::{Deserialize, Serialize};
// use sha2::{Digest, Sha256}; // Removed to minimize dependencies
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
fn ensure_tables_exist(db: &mut Connection) -> Result<(), OxidbError> {
    // Create Users Table
    let create_users_table_query = format!(
        "CREATE TABLE {} (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL)",
        USERS_TABLE
    );
    match db.execute(&create_users_table_query) {
        Ok(_) => {}
        Err(e) => {
            if !e.to_string().to_lowercase().contains("already exists")
                && !e.to_string().to_lowercase().contains("duplicate table name")
            {
                eprintln!("Error creating/ensuring table '{}': {:?}", USERS_TABLE, e);
                return Err(e);
            }
        }
    }

    // Create UserFiles Table - SQL schema uses BLOB type
    let create_user_files_table_query = format!(
        "CREATE TABLE {} (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, file_name TEXT NOT NULL, file_content BLOB NOT NULL)",
        USER_FILES_TABLE
    );
    match db.execute(&create_user_files_table_query) {
        Ok(_) => {}
        Err(e) => {
            if !e.to_string().to_lowercase().contains("already exists")
                && !e.to_string().to_lowercase().contains("duplicate table name")
            {
                eprintln!("Error creating/ensuring table '{}': {:?}", USER_FILES_TABLE, e);
                return Err(e);
            }
        }
    }
    Ok(())
}

// --- Password Hashing ---
fn hash_password(password: &str) -> String {
    // Simple hash function to replace SHA256 (for demo purposes only)
    // In production, use a proper crypto library
    let mut hash = 0u64;
    for byte in password.bytes() {
        hash = hash.wrapping_mul(31).wrapping_add(u64::from(byte));
    }
    oxidb::core::common::hex::encode(&hash.to_le_bytes())
}

// --- User Management Functions ---
fn register_user(db: &mut Connection, username: &str, password: &str) -> Result<()> {
    ensure_tables_exist(db)?;
    let hashed_password = hash_password(password);
    let escaped_username = username.replace("'", "''");
    let escaped_hashed_password = hashed_password.replace("'", "''");

    let query = format!(
        "INSERT INTO {} (username, password_hash) VALUES ('{}', '{}')",
        USERS_TABLE, escaped_username, escaped_hashed_password
    );

    let affected = db.execute(&query)?;
    if affected > 0 {
        println!("User '{}' registered successfully.", username);
        Ok(())
    } else {
        Err(anyhow::anyhow!("Failed to register user '{}'", username))
    }
}

fn login_user(db: &mut Connection, username: &str, password: &str) -> Result<Option<User>> {
    ensure_tables_exist(db)?;
    let escaped_username = username.replace("'", "''");
    let query = format!(
        "SELECT id, username, password_hash FROM {} WHERE username = '{}'",
        USERS_TABLE, escaped_username
    );

    let data = db.query(&query)?;
    if data.rows.is_empty() {
        println!("Login failed: User '{}' not found.", username);
        return Ok(None);
    }

    let row = &data.rows[0];
    if let (Some(id_val), Some(username_val), Some(password_hash_val)) = (row.get(0), row.get(1), row.get(2)) {
        use oxidb::Value;
        let id = match id_val {
            Value::Integer(i) => *i as u64,
            _ => return Err(anyhow::anyhow!("Invalid id type")),
        };
        let stored_username = match username_val {
            Value::Text(s) => s.clone(),
            _ => return Err(anyhow::anyhow!("Invalid username type")),
        };
        let stored_hash = match password_hash_val {
            Value::Text(s) => s.clone(),
            _ => return Err(anyhow::anyhow!("Invalid password_hash type")),
        };
        if hash_password(password) == stored_hash {
            Ok(Some(User { id, username: stored_username }))
        } else {
            println!("Login failed: Incorrect password for user '{}'.", username);
            Ok(None)
        }
    } else {
        Err(anyhow::anyhow!("Login error: Missing data for user '{}'", username))
    }
}

// --- File Management Functions ---
fn add_file(db: &mut Connection, user_id: u64, file_name: &str, content: &str) -> Result<()> {
    ensure_tables_exist(db)?;
    let escaped_file_name = file_name.replace("'", "''");
    let content_bytes = content.as_bytes();
    let hex_content = oxidb::core::common::hex::encode(content_bytes);

    let query = format!(
        "INSERT INTO {} (user_id, file_name, file_content) VALUES ({}, '{}', X'{}')",
        USER_FILES_TABLE, user_id, escaped_file_name, hex_content
    );

    let affected = db.execute(&query)?;
    if affected > 0 {
        println!("File '{}' added successfully for user ID {}.", file_name, user_id);
        Ok(())
    } else {
        Err(anyhow::anyhow!("Failed to add file '{}'", file_name))
    }
}

fn list_files(db: &mut Connection, user_id: u64) -> Result<()> {
    ensure_tables_exist(db)?;
    let query = format!(
        "SELECT id, user_id, file_name, file_content FROM {} WHERE user_id = {}",
        USER_FILES_TABLE, user_id
    );

    let data = db.query(&query)?;
    if data.rows.is_empty() {
        println!("No files found for user ID {}.", user_id);
        return Ok(());
    }
    println!("Files for user ID {}:", user_id);
    for row in &data.rows {
        if let (Some(id_val), Some(_user_id_val), Some(name_val), Some(content_val)) = (row.get(0), row.get(1), row.get(2), row.get(3)) {
            use oxidb::Value;
            let file_id = match id_val { Value::Integer(i) => *i, _ => continue };
            let file_name = match name_val { Value::Text(s) => s.clone(), _ => continue };
            let content = match content_val { Value::Blob(b) => b.clone(), _ => continue };
            let content_preview = String::from_utf8_lossy(&content);
            println!(
                "- ID: {}, Name: {}, Content Preview (lossy UTF-8): {:.50}{}",
                file_id,
                file_name,
                content_preview,
                if content_preview.len() > 50 { "..." } else { "" }
            );
        }
    }
    Ok(())
}

fn get_file_by_id(db: &mut Connection, user_id: u64, file_id: u64) -> Result<Option<UserFile>> {
    ensure_tables_exist(db)?;
    let query = format!(
        "SELECT id, user_id, file_name, file_content FROM {} WHERE id = {} AND user_id = {}",
        USER_FILES_TABLE, file_id, user_id
    );

    let data = db.query(&query)?;
    if data.rows.is_empty() {
        return Ok(None);
    }
    let row = &data.rows[0];
    use oxidb::Value;
    let id = match row.get(0) { Some(Value::Integer(i)) => *i as u64, _ => return Ok(None) };
    let uid = match row.get(1) { Some(Value::Integer(i)) => *i as u64, _ => return Ok(None) };
    let name = match row.get(2) { Some(Value::Text(s)) => s.clone(), _ => return Ok(None) };
    let content = match row.get(3) { Some(Value::Blob(b)) => b.clone(), _ => return Ok(None) };
    Ok(Some(UserFile { id, user_id: uid, file_name: name, content }))
}

fn delete_file(db: &mut Connection, user_id: u64, file_id: u64) -> Result<bool> {
    ensure_tables_exist(db)?;
    let query = format!(
        "DELETE FROM {} WHERE id = {} AND user_id = {}",
        USER_FILES_TABLE, file_id, user_id
    );
    let affected = db.execute(&query)?;
    Ok(affected > 0)
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
    let mut db = Connection::open(DB_PATH)?;

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
                match get_file_by_id(&mut db, user_id, file_id)? {
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
