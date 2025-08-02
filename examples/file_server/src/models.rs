use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct User {
    pub id: String,
    pub username: String,
    pub email: String,
    #[serde(skip_serializing)]
    pub password_hash: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterRequest {
    pub username: String,
    pub email: String,
    pub password: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LoginResponse {
    pub token: String,
    pub user: User,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct File {
    pub id: String,
    pub user_id: String,
    pub filename: String,
    pub original_name: String,
    pub mime_type: Option<String>,
    pub size: i64,
    pub path: String,
    pub uploaded_at: DateTime<Utc>,
    pub is_public: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileShare {
    pub id: String,
    pub file_id: String,
    pub shared_with_user_id: String,
    pub shared_by_user_id: String,
    pub shared_at: DateTime<Utc>,
    pub permissions: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ShareFileRequest {
    pub file_id: String,
    pub username: String,
    pub permissions: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileListResponse {
    pub owned_files: Vec<File>,
    pub shared_files: Vec<FileWithOwner>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileWithOwner {
    pub file: File,
    pub owner: String,
    pub permissions: String,
}

#[derive(Debug, Clone)]
pub struct Session {
    pub id: String,
    pub user_id: String,
    pub token: String,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String, // user_id
    pub exp: i64,    // expiration time
    pub iat: i64,    // issued at
}