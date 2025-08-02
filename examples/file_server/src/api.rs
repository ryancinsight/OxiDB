use axum::{
    extract::{Json, Path, Multipart},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;

// Local models to avoid cross-module issues
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub username: String,
    pub email: String,
    #[serde(skip_serializing)]
    pub password_hash: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub username: String,
    pub email: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize)]
pub struct LoginResponse {
    pub token: String,
    pub user: User,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct File {
    pub id: String,
    pub user_id: String,
    pub filename: String,
    pub original_name: String,
    pub mime_type: String,
    pub size: i64,
    pub path: String,
    pub uploaded_at: DateTime<Utc>,
    pub is_public: bool,
}

// Simple auth user for demo
#[derive(Debug, Clone)]
pub struct AuthUser {
    pub user_id: String,
}

// Implement a simple extractor for AuthUser
#[axum::async_trait]
impl<S> axum::extract::FromRequestParts<S> for AuthUser
where
    S: Send + Sync,
{
    type Rejection = StatusCode;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        // For demo purposes, just check for a header
        let auth_header = parts
            .headers
            .get("Authorization")
            .and_then(|h| h.to_str().ok())
            .ok_or(StatusCode::UNAUTHORIZED)?;

        // Simple token parsing (in real app, verify JWT)
        if auth_header.starts_with("Bearer ") {
            Ok(AuthUser {
                user_id: "demo-user-id".to_string(),
            })
        } else {
            Err(StatusCode::UNAUTHORIZED)
        }
    }
}

// Error type
#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    NotFound(String),
    Unauthorized,
    Internal(String),
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        ApiError::Internal(err.to_string())
    }
}

impl From<std::io::Error> for ApiError {
    fn from(err: std::io::Error) -> Self {
        ApiError::Internal(err.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            ApiError::Unauthorized => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };
        
        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

// API routes
pub fn create_routes() -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/auth/register", post(register))
        .route("/auth/login", post(login))
        .route("/files", get(list_files))
        // .route("/files", post(upload_file)) // Commented due to Handler trait issues
        .route("/files/:id", get(get_file))
        .route("/users/me", get(get_current_user))
}

// Handlers
async fn health() -> &'static str {
    "OK"
}

async fn register(Json(req): Json<RegisterRequest>) -> Result<Json<User>, ApiError> {
    // Demo implementation
    let user = User {
        id: Uuid::new_v4().to_string(),
        username: req.username,
        email: req.email,
        password_hash: bcrypt::hash(req.password, bcrypt::DEFAULT_COST)
            .map_err(|e| ApiError::Internal(e.to_string()))?,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    
    Ok(Json(user))
}

async fn login(Json(req): Json<LoginRequest>) -> Result<Json<LoginResponse>, ApiError> {
    // Demo implementation - in real app, verify password
    let user = User {
        id: Uuid::new_v4().to_string(),
        username: req.username.clone(),
        email: format!("{}@example.com", req.username),
        password_hash: String::new(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    
    let response = LoginResponse {
        token: format!("demo-token-for-{}", req.username),
        user,
    };
    
    Ok(Json(response))
}

async fn get_current_user(auth_user: AuthUser) -> Result<Json<User>, ApiError> {
    // Demo implementation
    let user = User {
        id: auth_user.user_id,
        username: "demo_user".to_string(),
        email: "demo@example.com".to_string(),
        password_hash: String::new(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    
    Ok(Json(user))
}

async fn list_files(auth_user: AuthUser) -> Result<Json<Vec<File>>, ApiError> {
    // Demo implementation
    let files = vec![
        File {
            id: Uuid::new_v4().to_string(),
            user_id: auth_user.user_id.clone(),
            filename: "example.txt".to_string(),
            original_name: "example.txt".to_string(),
            mime_type: "text/plain".to_string(),
            size: 1024,
            path: "uploads/example.txt".to_string(),
            uploaded_at: Utc::now(),
            is_public: false,
        },
    ];
    
    Ok(Json(files))
}

async fn upload_file(
    mut multipart: Multipart,
    auth_user: AuthUser,
) -> Result<Json<File>, ApiError> {
    while let Some(field) = multipart.next_field().await
        .map_err(|e| ApiError::BadRequest(e.to_string()))?
    {
        let name = field.name().unwrap_or("").to_string();
        if name != "file" {
            continue;
        }
        
        let filename = field.file_name().unwrap_or("unknown").to_string();
        let content_type = field.content_type().unwrap_or("application/octet-stream").to_string();
        let data = field.bytes().await
            .map_err(|e| ApiError::BadRequest(e.to_string()))?;
        
        // Demo: just return a mock file
        let file = File {
            id: Uuid::new_v4().to_string(),
            user_id: auth_user.user_id,
            filename: filename.clone(),
            original_name: filename,
            mime_type: content_type,
            size: data.len() as i64,
            path: format!("uploads/{}", Uuid::new_v4()),
            uploaded_at: Utc::now(),
            is_public: false,
        };
        
        return Ok(Json(file));
    }
    
    Err(ApiError::BadRequest("No file uploaded".to_string()))
}

async fn get_file(
    Path(file_id): Path<String>,
    auth_user: AuthUser,
) -> Result<Json<File>, ApiError> {
    // Demo implementation
    let file = File {
        id: file_id,
        user_id: auth_user.user_id,
        filename: "example.txt".to_string(),
        original_name: "example.txt".to_string(),
        mime_type: "text/plain".to_string(),
        size: 1024,
        path: "uploads/example.txt".to_string(),
        uploaded_at: Utc::now(),
        is_public: false,
    };
    
    Ok(Json(file))
}