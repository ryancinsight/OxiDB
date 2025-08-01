use anyhow::Result;
use oxidb::api::Connection;
use std::sync::Arc;
use tokio::sync::Mutex;

pub type DbConnection = Arc<Mutex<Connection>>;

// Global database connection
static mut DB: Option<DbConnection> = None;

pub async fn init_database(path: &str) -> Result<()> {
    let mut conn = Connection::open(path)?;
    
    // Create users table
    conn.execute(
        "CREATE TABLE users (
            id TEXT PRIMARY KEY,
            username TEXT UNIQUE,
            email TEXT UNIQUE,
            password_hash TEXT,
            created_at TEXT,
            updated_at TEXT
        )"
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
        )"
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
        )"
    )?;
    
    // Create sessions table
    conn.execute(
        "CREATE TABLE sessions (
            id TEXT PRIMARY KEY,
            user_id TEXT,
            token TEXT UNIQUE,
            expires_at TEXT,
            created_at TEXT
        )"
    )?;
    
    // Create indexes for better performance
    // Note: OxiDB may not support CREATE INDEX statements
    // conn.execute("CREATE INDEX idx_files_user_id ON files(user_id)")?;
    // conn.execute("CREATE INDEX idx_file_shares_file_id ON file_shares(file_id)")?;
    // conn.execute("CREATE INDEX idx_file_shares_shared_with ON file_shares(shared_with_user_id)")?;
    // conn.execute("CREATE INDEX idx_sessions_token ON sessions(token)")?;
    // conn.execute("CREATE INDEX idx_sessions_user_id ON sessions(user_id)")?;
    
    // Store connection globally
    unsafe {
        DB = Some(Arc::new(Mutex::new(conn)));
    }
    
    Ok(())
}

pub fn get_db() -> DbConnection {
    unsafe {
        DB.as_ref().expect("Database not initialized").clone()
    }
}