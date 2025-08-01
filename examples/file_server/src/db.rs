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
        "CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )"
    )?;
    
    // Create files table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS files (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            filename TEXT NOT NULL,
            original_name TEXT NOT NULL,
            mime_type TEXT,
            size INTEGER NOT NULL,
            path TEXT NOT NULL,
            uploaded_at TEXT NOT NULL,
            is_public INTEGER DEFAULT 0,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )"
    )?;
    
    // Create file_shares table for sharing files with specific users
    conn.execute(
        "CREATE TABLE IF NOT EXISTS file_shares (
            id TEXT PRIMARY KEY,
            file_id TEXT NOT NULL,
            shared_with_user_id TEXT NOT NULL,
            shared_by_user_id TEXT NOT NULL,
            shared_at TEXT NOT NULL,
            permissions TEXT DEFAULT 'read',
            FOREIGN KEY (file_id) REFERENCES files(id),
            FOREIGN KEY (shared_with_user_id) REFERENCES users(id),
            FOREIGN KEY (shared_by_user_id) REFERENCES users(id),
            UNIQUE(file_id, shared_with_user_id)
        )"
    )?;
    
    // Create sessions table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            token TEXT UNIQUE NOT NULL,
            expires_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )"
    )?;
    
    // Create indexes for better performance
    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_user_id ON files(user_id)")?;
    conn.execute("CREATE INDEX IF NOT EXISTS idx_file_shares_file_id ON file_shares(file_id)")?;
    conn.execute("CREATE INDEX IF NOT EXISTS idx_file_shares_shared_with ON file_shares(shared_with_user_id)")?;
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token)")?;
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id)")?;
    
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