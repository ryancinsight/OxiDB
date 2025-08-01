use crate::{auth, db, models::*};
use anyhow::anyhow;
use axum::{
    extract::{Multipart, Path, Query},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use oxidb::api::QueryResult;
use oxidb::core::common::types::Value;
use serde::Deserialize;
use std::path::PathBuf;
use uuid::Uuid;

// Re-export models locally to avoid cross-module type issues
pub use crate::models::{
    User as UserModel,
    RegisterRequest as RegisterRequestModel,
    LoginRequest as LoginRequestModel,
    LoginResponse as LoginResponseModel,
    File as FileModel,
    FileShare as FileShareModel,
    ShareFileRequest as ShareFileRequestModel,
    FileListResponse as FileListResponseModel,
};

// Local type aliases to work around Axum handler issues
type User = UserModel;
type RegisterRequest = RegisterRequestModel;
type LoginRequest = LoginRequestModel;
type LoginResponse = LoginResponseModel;
type File = FileModel;
type FileShare = FileShareModel;
type ShareFileRequest = ShareFileRequestModel;
type FileListResponse = FileListResponseModel;

// Re-export AuthUser
pub use crate::auth::AuthUser;

// Error handling
#[derive(Debug)]
pub enum AppError {
    BadRequest(String),
    NotFound(String),
    Forbidden(String),
    Internal(anyhow::Error),
}

impl From<anyhow::Error> for AppError {
    fn from(err: anyhow::Error) -> Self {
        AppError::Internal(err)
    }
}

impl From<oxidb::OxidbError> for AppError {
    fn from(err: oxidb::OxidbError) -> Self {
        AppError::Internal(anyhow::anyhow!("Database error: {:?}", err))
    }
}

impl From<std::io::Error> for AppError {
    fn from(err: std::io::Error) -> Self {
        AppError::Internal(anyhow::anyhow!("IO error: {:?}", err))
    }
}

impl From<axum::extract::multipart::MultipartError> for AppError {
    fn from(err: axum::extract::multipart::MultipartError) -> Self {
        AppError::BadRequest(err.to_string())
    }
}

impl From<chrono::ParseError> for AppError {
    fn from(err: chrono::ParseError) -> Self {
        AppError::BadRequest(format!("Invalid date format: {}", err))
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            AppError::Forbidden(msg) => (StatusCode::FORBIDDEN, msg),
            AppError::Internal(err) => {
                eprintln!("Internal error: {:?}", err);
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error".to_string())
            }
        };
        
        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

// Helper functions for OxiDB Value extraction
fn value_as_text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(s) => Some(s.as_str()),
        _ => None,
    }
}

fn value_as_integer(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(i) => Some(*i),
        _ => None,
    }
}

// API Routes
pub fn api_routes() -> Router {
    Router::new()
        // Auth routes
        .route("/auth/register", post(register))
        .route("/auth/login", post(login))
        .route("/auth/logout", post(logout))
        // File routes (protected)
        .route("/files", get(list_files).post(upload_file))
        .route("/files/:id", get(get_file))
        .route("/files/:id", axum::routing::delete(delete_file))
        .route("/files/:id/share", post(share_file))
        .route("/files/:id/unshare", post(unshare_file))
        .route("/files/:id/download", get(download_file))
        // User routes
        .route("/users/me", get(get_current_user))
}

// Auth handlers
async fn register(Json(req): Json<RegisterRequest>) -> Result<Json<User>, AppError> {
    let user = auth::register_user(req).await.map_err(|e| AppError::from(e))?;
    Ok(Json(user))
}

async fn login(Json(req): Json<LoginRequest>) -> Result<Json<LoginResponse>, AppError> {
    let response = auth::login_user(req).await.map_err(|e| AppError::from(e))?;
    Ok(Json(response))
}

async fn logout(auth_user: AuthUser) -> Result<StatusCode, AppError> {
    let db = db::get_db();
    let mut conn = db.lock().await;
    
    // Remove all sessions for this user
    let query = format!("DELETE FROM sessions WHERE user_id = '{}'", auth_user.user_id);
    conn.execute(&query)?;
    
    Ok(StatusCode::OK)
}

async fn get_current_user(auth_user: AuthUser) -> Result<Json<User>, AppError> {
    let db = db::get_db();
    let mut conn = db.lock().await;
    
    let query = format!("SELECT * FROM users WHERE id = '{}'", auth_user.user_id);
    let result = conn.execute(&query)?;
    
    if let QueryResult::Data(data) = result {
        if let Some(row) = data.rows.first() {
            let user = User {
                id: row.get(0).and_then(value_as_text).ok_or(anyhow!("Invalid data"))?.to_string(),
                username: row.get(1).and_then(value_as_text).ok_or(anyhow!("Invalid data"))?.to_string(),
                email: row.get(2).and_then(value_as_text).ok_or(anyhow!("Invalid data"))?.to_string(),
                password_hash: row.get(3).and_then(value_as_text).ok_or(anyhow!("Invalid data"))?.to_string(),
                created_at: DateTime::parse_from_rfc3339(row.get(4).and_then(value_as_text).ok_or(anyhow!("Invalid data"))?)?.with_timezone(&Utc),
                updated_at: DateTime::parse_from_rfc3339(row.get(5).and_then(value_as_text).ok_or(anyhow!("Invalid data"))?)?.with_timezone(&Utc),
            };
            return Ok(Json(user));
        }
    }
    
    Err(AppError::NotFound("User not found".to_string()))
}

// File handlers
async fn upload_file(
    mut multipart: Multipart,
    auth_user: AuthUser,
) -> Result<Json<File>, AppError> {
    while let Some(field) = multipart.next_field().await? {
        let name = field.name().unwrap_or("").to_string();
        if name != "file" {
            continue;
        }
        
        let filename = field.file_name().unwrap_or("unknown").to_string();
        let content_type = field.content_type().unwrap_or("application/octet-stream").to_string();
        let data = field.bytes().await?;
        let size = data.len() as i64;
        
        // Generate unique filename
        let file_id = Uuid::new_v4().to_string();
        let path = PathBuf::from(&filename);
        let extension = path
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("");
        let stored_filename = format!("{}.{}", file_id, extension);
        let file_path = format!("uploads/{}/{}", &auth_user.user_id, stored_filename);
        
        // Create user directory if it doesn't exist
        let user_dir = format!("uploads/{}", &auth_user.user_id);
        tokio::fs::create_dir_all(&user_dir).await?;
        
        // Save file to disk
        tokio::fs::write(&file_path, &data).await?;
        
        // Save file metadata to database
        let db = db::get_db();
        let mut conn = db.lock().await;
        
        let now = Utc::now().to_rfc3339();
        let query = format!(
            "INSERT INTO files (id, user_id, filename, original_name, mime_type, size, path, uploaded_at, is_public) 
             VALUES ('{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', 0)",
            file_id, auth_user.user_id, stored_filename, filename, content_type, size, file_path, now
        );
        
        conn.execute(&query)?;
        
        let file = File {
            id: file_id,
            user_id: auth_user.user_id.clone(),
            filename: stored_filename,
            original_name: filename,
            mime_type: content_type,
            size,
            path: file_path,
            uploaded_at: Utc::now(),
            is_public: false,
        };
        
        return Ok(Json(file));
    }
    
    Err(AppError::BadRequest("No file uploaded".to_string()))
}

#[derive(Deserialize)]
struct ListFilesQuery {
    include_shared: Option<bool>,
}

async fn list_files(
    Query(params): Query<ListFilesQuery>,
    auth_user: AuthUser,
) -> Result<Json<FileListResponse>, AppError> {
    let db = db::get_db();
    let mut conn = db.lock().await;
    
    // Get user's own files
    let query = format!("SELECT * FROM files WHERE user_id = '{}'", auth_user.user_id);
    let result = conn.execute(&query)?;
    
    let mut files = Vec::new();
    if let QueryResult::Data(data) = result {
        for row in data.rows {
            let file = File {
                id: row.get(0).and_then(value_as_text).unwrap_or("").to_string(),
                user_id: row.get(1).and_then(value_as_text).unwrap_or("").to_string(),
                filename: row.get(2).and_then(value_as_text).unwrap_or("").to_string(),
                original_name: row.get(3).and_then(value_as_text).unwrap_or("").to_string(),
                mime_type: row.get(4).and_then(value_as_text).unwrap_or("").to_string(),
                size: row.get(5).and_then(value_as_integer).unwrap_or(0),
                path: row.get(6).and_then(value_as_text).unwrap_or("").to_string(),
                uploaded_at: DateTime::parse_from_rfc3339(
                    row.get(7).and_then(value_as_text).unwrap_or("1970-01-01T00:00:00Z")
                ).unwrap_or_else(|_| Utc::now().fixed_offset()).with_timezone(&Utc),
                is_public: row.get(8).and_then(value_as_integer).unwrap_or(0) != 0,
            };
            files.push(file);
        }
    }
    
    // Get shared files if requested
    let mut shared_files = Vec::new();
    if params.include_shared.unwrap_or(false) {
        let query = format!(
            "SELECT f.*, fs.permissions FROM files f 
             JOIN file_shares fs ON f.id = fs.file_id 
             WHERE fs.shared_with_user_id = '{}'",
            auth_user.user_id
        );
        
        if let Ok(result) = conn.execute(&query) {
            if let QueryResult::Data(data) = result {
                for row in data.rows {
                    let file = File {
                        id: row.get(0).and_then(value_as_text).unwrap_or("").to_string(),
                        user_id: row.get(1).and_then(value_as_text).unwrap_or("").to_string(),
                        filename: row.get(2).and_then(value_as_text).unwrap_or("").to_string(),
                        original_name: row.get(3).and_then(value_as_text).unwrap_or("").to_string(),
                        mime_type: row.get(4).and_then(value_as_text).unwrap_or("").to_string(),
                        size: row.get(5).and_then(value_as_integer).unwrap_or(0),
                        path: row.get(6).and_then(value_as_text).unwrap_or("").to_string(),
                        uploaded_at: DateTime::parse_from_rfc3339(
                            row.get(7).and_then(value_as_text).unwrap_or("1970-01-01T00:00:00Z")
                        ).unwrap_or_else(|_| Utc::now().fixed_offset()).with_timezone(&Utc),
                        is_public: row.get(8).and_then(value_as_integer).unwrap_or(0) != 0,
                    };
                    shared_files.push(file);
                }
            }
        }
    }
    
    Ok(Json(FileListResponse {
        owned_files: files,
        shared_files,
    }))
}

async fn get_file(
    Path(file_id): Path<String>,
    auth_user: AuthUser,
) -> Result<Json<File>, AppError> {
    let db = db::get_db();
    let mut conn = db.lock().await;
    
    // Check if user owns the file or has access to it
    let query = format!(
        "SELECT * FROM files WHERE id = '{}' AND (user_id = '{}' OR is_public = 1)",
        file_id, auth_user.user_id
    );
    
    let result = conn.execute(&query)?;
    
    if let QueryResult::Data(data) = result {
        if let Some(row) = data.rows.first() {
            let file = File {
                id: row.get(0).and_then(value_as_text).unwrap_or("").to_string(),
                user_id: row.get(1).and_then(value_as_text).unwrap_or("").to_string(),
                filename: row.get(2).and_then(value_as_text).unwrap_or("").to_string(),
                original_name: row.get(3).and_then(value_as_text).unwrap_or("").to_string(),
                mime_type: row.get(4).and_then(value_as_text).unwrap_or("").to_string(),
                size: row.get(5).and_then(value_as_integer).unwrap_or(0),
                path: row.get(6).and_then(value_as_text).unwrap_or("").to_string(),
                uploaded_at: DateTime::parse_from_rfc3339(
                    row.get(7).and_then(value_as_text).unwrap_or("1970-01-01T00:00:00Z")
                ).unwrap_or_else(|_| Utc::now().fixed_offset()).with_timezone(&Utc),
                is_public: row.get(8).and_then(value_as_integer).unwrap_or(0) != 0,
            };
            return Ok(Json(file));
        }
    }
    
    // Check if file is shared with user
    let query = format!(
        "SELECT f.* FROM files f 
         JOIN file_shares fs ON f.id = fs.file_id 
         WHERE f.id = '{}' AND fs.shared_with_user_id = '{}'",
        file_id, auth_user.user_id
    );
    
    let result = conn.execute(&query)?;
    
    if let QueryResult::Data(data) = result {
        if let Some(row) = data.rows.first() {
            let file = File {
                id: row.get(0).and_then(value_as_text).unwrap_or("").to_string(),
                user_id: row.get(1).and_then(value_as_text).unwrap_or("").to_string(),
                filename: row.get(2).and_then(value_as_text).unwrap_or("").to_string(),
                original_name: row.get(3).and_then(value_as_text).unwrap_or("").to_string(),
                mime_type: row.get(4).and_then(value_as_text).unwrap_or("").to_string(),
                size: row.get(5).and_then(value_as_integer).unwrap_or(0),
                path: row.get(6).and_then(value_as_text).unwrap_or("").to_string(),
                uploaded_at: DateTime::parse_from_rfc3339(
                    row.get(7).and_then(value_as_text).unwrap_or("1970-01-01T00:00:00Z")
                ).unwrap_or_else(|_| Utc::now().fixed_offset()).with_timezone(&Utc),
                is_public: row.get(8).and_then(value_as_integer).unwrap_or(0) != 0,
            };
            return Ok(Json(file));
        }
    }
    
    Err(AppError::NotFound("File not found".to_string()))
}

async fn delete_file(
    Path(file_id): Path<String>,
    auth_user: AuthUser,
) -> Result<StatusCode, AppError> {
    let db = db::get_db();
    let mut conn = db.lock().await;
    
    // Check if user owns the file
    let query = format!(
        "SELECT path FROM files WHERE id = '{}' AND user_id = '{}'",
        file_id, auth_user.user_id
    );
    
    let result = conn.execute(&query)?;
    
    if let QueryResult::Data(data) = result {
        if let Some(row) = data.rows.first() {
            let file_path = row.get(0).and_then(value_as_text).ok_or(anyhow!("Invalid file path"))?;
            
            // Delete file from disk
            if let Err(e) = tokio::fs::remove_file(file_path).await {
                eprintln!("Failed to delete file from disk: {:?}", e);
            }
            
            // Delete file shares
            let query = format!("DELETE FROM file_shares WHERE file_id = '{}'", file_id);
            conn.execute(&query)?;
            
            // Delete file record
            let query = format!("DELETE FROM files WHERE id = '{}'", file_id);
            conn.execute(&query)?;
            
            Ok(StatusCode::OK)
        } else {
            Err(AppError::NotFound("File not found".to_string()))
        }
    } else {
        Err(AppError::NotFound("File not found".to_string()))
    }
}

async fn share_file(
    Path(file_id): Path<String>,
    Json(req): Json<ShareFileRequest>,
    auth_user: AuthUser,
) -> Result<StatusCode, AppError> {
    let db = db::get_db();
    let mut conn = db.lock().await;
    
    // Check if user owns the file
    let query = format!(
        "SELECT id FROM files WHERE id = '{}' AND user_id = '{}'",
        file_id, auth_user.user_id
    );
    
    let result = conn.execute(&query)?;
    
    if let QueryResult::Data(data) = result {
        if data.rows.is_empty() {
            return Err(AppError::Forbidden("You don't own this file".to_string()));
        }
    } else {
        return Err(AppError::NotFound("File not found".to_string()));
    }
    
    // Check if target user exists
    let query = format!("SELECT id FROM users WHERE username = '{}'", req.username);
    let result = conn.execute(&query)?;
    
    let target_user_id = if let QueryResult::Data(data) = result {
        if let Some(row) = data.rows.first() {
            row.get(0).and_then(value_as_text).ok_or(anyhow!("Invalid user id"))?.to_string()
        } else {
            return Err(AppError::NotFound("User not found".to_string()));
        }
    } else {
        return Err(AppError::NotFound("User not found".to_string()));
    };
    
    // Create file share
    let share_id = Uuid::new_v4().to_string();
    let now = Utc::now().to_rfc3339();
    let permissions = req.permissions.unwrap_or("read".to_string());
    
    let query = format!(
        "INSERT INTO file_shares (id, file_id, shared_with_user_id, shared_by_user_id, shared_at, permissions) 
         VALUES ('{}', '{}', '{}', '{}', '{}', '{}')",
        share_id, file_id, target_user_id, auth_user.user_id, now, permissions
    );
    
    conn.execute(&query)?;
    
    Ok(StatusCode::OK)
}

async fn unshare_file(
    Path(file_id): Path<String>,
    Json(req): Json<ShareFileRequest>,
    auth_user: AuthUser,
) -> Result<StatusCode, AppError> {
    let db = db::get_db();
    let mut conn = db.lock().await;
    
    // Check if user owns the file
    let query = format!(
        "SELECT id FROM files WHERE id = '{}' AND user_id = '{}'",
        file_id, auth_user.user_id
    );
    
    let result = conn.execute(&query)?;
    
    if let QueryResult::Data(data) = result {
        if data.rows.is_empty() {
            return Err(AppError::Forbidden("You don't own this file".to_string()));
        }
    } else {
        return Err(AppError::NotFound("File not found".to_string()));
    }
    
    // Get target user id
    let query = format!("SELECT id FROM users WHERE username = '{}'", req.username);
    let result = conn.execute(&query)?;
    
    let target_user_id = if let QueryResult::Data(data) = result {
        if let Some(row) = data.rows.first() {
            row.get(0).and_then(value_as_text).ok_or(anyhow!("Invalid user id"))?.to_string()
        } else {
            return Err(AppError::NotFound("User not found".to_string()));
        }
    } else {
        return Err(AppError::NotFound("User not found".to_string()));
    };
    
    // Delete file share
    let query = format!(
        "DELETE FROM file_shares WHERE file_id = '{}' AND shared_with_user_id = '{}'",
        file_id, target_user_id
    );
    
    conn.execute(&query)?;
    
    Ok(StatusCode::OK)
}

type FileDownloadResponse = ([(& 'static str, String); 2], Vec<u8>);

async fn download_file(
    Path(file_id): Path<String>,
    auth_user: AuthUser,
) -> Result<FileDownloadResponse, AppError> {
    let db = db::get_db();
    let mut conn = db.lock().await;
    
    // Check if user has access to the file
    let query = format!(
        "SELECT path, original_name, mime_type FROM files 
         WHERE id = '{}' AND (user_id = '{}' OR is_public = 1)",
        file_id, auth_user.user_id
    );
    
    let mut result = conn.execute(&query)?;
    
    // If not found, check if it's shared with the user
    if let QueryResult::Data(ref data) = result {
        if data.rows.is_empty() {
            let query = format!(
                "SELECT f.path, f.original_name, f.mime_type FROM files f 
                 JOIN file_shares fs ON f.id = fs.file_id 
                 WHERE f.id = '{}' AND fs.shared_with_user_id = '{}'",
                file_id, auth_user.user_id
            );
            result = conn.execute(&query)?;
        }
    }
    
    if let QueryResult::Data(data) = result {
        if let Some(row) = data.rows.first() {
            let file_path = row.get(0).and_then(value_as_text).ok_or(anyhow!("Invalid file path"))?.to_string();
            let original_name = row.get(1).and_then(value_as_text).ok_or(anyhow!("Invalid file name"))?.to_string();
            let mime_type = row.get(2).and_then(value_as_text).unwrap_or("application/octet-stream").to_string();
            
            // Read file
            let file_data = tokio::fs::read(&file_path).await?;
            
            // Return file with appropriate headers
            let content_disposition = format!("attachment; filename=\"{}\"", original_name);
            Ok((
                [
                    ("Content-Type", mime_type),
                    ("Content-Disposition", content_disposition),
                ],
                file_data,
            ))
        } else {
            Err(AppError::NotFound("File not found".to_string()))
        }
    } else {
        Err(AppError::NotFound("File not found".to_string()))
    }
}