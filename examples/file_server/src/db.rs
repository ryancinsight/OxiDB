use anyhow::Result;
use oxidb::Connection;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::Mutex;

pub type DbConnection = Arc<Mutex<Connection>>;

// Global database connection
static DB: OnceLock<DbConnection> = OnceLock::new();

pub async fn init_database(path: &str) -> Result<()> {
    let mut conn = Connection::open(path)?;
    
    // Check if tables already exist by trying to query them
    let tables_exist = conn.execute("SELECT * FROM users LIMIT 1").is_ok();
    
    if !tables_exist {
        // Create users table
        conn.execute(
            "CREATE TABLE users (
            id TEXT PRIMARY KEY,
            username TEXT UNIQUE,
            email TEXT UNIQUE,
            password_hash TEXT,
            created_at TEXT,
            updated_at TEXT
        )",
        )?;
        
        // Create files table
        conn.execute(
            "CREATE TABLE files (
            id TEXT PRIMARY KEY,
            user_id TEXT,
            filename TEXT,
            original_name TEXT,
            mime_type TEXT,
            size INTEGER,
            path TEXT,
            uploaded_at TEXT,
            is_public INTEGER
        )",
        )?;
        
        // Create file_shares table for sharing files with specific users
        conn.execute(
            "CREATE TABLE file_shares (
            id TEXT PRIMARY KEY,
            file_id TEXT,
            shared_with_user_id TEXT,
            shared_by_user_id TEXT,
            shared_at TEXT,
            permissions TEXT
        )",
        )?;
        
        // Create sessions table
        conn.execute(
            "CREATE TABLE sessions (
            id TEXT PRIMARY KEY,
            user_id TEXT,
            token TEXT UNIQUE,
            expires_at TEXT,
            created_at TEXT
        )",
        )?;
    }

    // Initialize the global OnceLock if not already set
    DB.get_or_init(|| Arc::new(Mutex::new(conn)));
    
    Ok(())
}

pub fn get_db() -> DbConnection {
    DB.get().expect("Database not initialized").clone()
}