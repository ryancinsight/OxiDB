use crate::{db, models::*};
use anyhow::{anyhow, Result};
use axum::{
    extract::FromRequestParts,
    http::{header, request::Parts, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use bcrypt::{hash, verify, DEFAULT_COST};
use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use oxidb::api::{QueryResult};
use oxidb::core::common::types::Value;
use uuid::Uuid;

const JWT_SECRET: &[u8] = b"your-secret-key-change-this-in-production";

// Helper function to extract text from Value
fn value_as_text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(s) => Some(s.as_str()),
        _ => None,
    }
}

pub async fn register_user(req: RegisterRequest) -> Result<User> {
    let db = db::get_db();
    let mut conn = db.lock().await;
    
    // Hash password
    let password_hash = hash(&req.password, DEFAULT_COST)?;
    
    // Create user
    let user_id = Uuid::new_v4().to_string();
    let now = Utc::now();
    
    let query = format!(
        "INSERT INTO users (id, username, email, password_hash, created_at, updated_at) 
         VALUES ('{}', '{}', '{}', '{}', '{}', '{}')",
        user_id, req.username, req.email, password_hash, now.to_rfc3339(), now.to_rfc3339()
    );
    
    match conn.execute(&query) {
        Ok(_) => {
            let user = User {
                id: user_id,
                username: req.username,
                email: req.email,
                password_hash,
                created_at: now,
                updated_at: now,
            };
            Ok(user)
        }
        Err(e) => {
            if e.to_string().contains("UNIQUE constraint failed") {
                Err(anyhow!("Username or email already exists"))
            } else {
                Err(e.into())
            }
        }
    }
}

pub async fn login_user(req: LoginRequest) -> Result<LoginResponse> {
    let db = db::get_db();
    let mut conn = db.lock().await;
    
    // Find user by username
    let query = format!(
        "SELECT id, username, email, password_hash, created_at, updated_at 
         FROM users WHERE username = '{}'",
        req.username
    );
    
    let result = conn.execute(&query)?;
    
    if let QueryResult::Data(data) = result {
        if let Some(row) = data.rows.first() {
            let user_id = row.get(0).and_then(value_as_text).ok_or(anyhow!("Invalid user data"))?;
            let username = row.get(1).and_then(value_as_text).ok_or(anyhow!("Invalid user data"))?;
            let email = row.get(2).and_then(value_as_text).ok_or(anyhow!("Invalid user data"))?;
            let password_hash = row.get(3).and_then(value_as_text).ok_or(anyhow!("Invalid user data"))?;
            let created_at = row.get(4).and_then(value_as_text).ok_or(anyhow!("Invalid user data"))?;
            let updated_at = row.get(5).and_then(value_as_text).ok_or(anyhow!("Invalid user data"))?;
            
            // Verify password
            if verify(&req.password, password_hash)? {
                // Create JWT token
                let token = create_jwt_token(user_id)?;
                
                // Store session
                let session_id = Uuid::new_v4().to_string();
                let now = Utc::now();
                let expires_at = now + Duration::days(7);
                
                let session_query = format!(
                    "INSERT INTO sessions (id, user_id, token, expires_at, created_at) 
                     VALUES ('{}', '{}', '{}', '{}', '{}')",
                    session_id, user_id, token, expires_at.to_rfc3339(), now.to_rfc3339()
                );
                
                conn.execute(&session_query)?;
                
                let user = User {
                    id: user_id.to_string(),
                    username: username.to_string(),
                    email: email.to_string(),
                    password_hash: password_hash.to_string(),
                    created_at: DateTime::parse_from_rfc3339(created_at)?.with_timezone(&Utc),
                    updated_at: DateTime::parse_from_rfc3339(updated_at)?.with_timezone(&Utc),
                };
                
                Ok(LoginResponse { token, user })
            } else {
                Err(anyhow!("Invalid credentials"))
            }
        } else {
            Err(anyhow!("User not found"))
        }
    } else {
        Err(anyhow!("User not found"))
    }
}

pub fn create_jwt_token(user_id: &str) -> Result<String> {
    let now = Utc::now();
    let exp = now + Duration::days(7);
    
    let claims = Claims {
        sub: user_id.to_string(),
        exp: exp.timestamp(),
        iat: now.timestamp(),
    };
    
    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(JWT_SECRET),
    )?;
    
    Ok(token)
}

pub fn verify_jwt_token(token: &str) -> Result<Claims> {
    let token_data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(JWT_SECRET),
        &Validation::default(),
    )?;
    
    Ok(token_data.claims)
}

// Axum extractor for authenticated user
pub struct AuthUser {
    pub user_id: String,
}

#[axum::async_trait]
impl<S> FromRequestParts<S> for AuthUser
where
    S: Send + Sync,
{
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Extract token from Authorization header
        let auth_header = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .ok_or(AuthError::MissingToken)?;
        
        let token = auth_header
            .strip_prefix("Bearer ")
            .ok_or(AuthError::InvalidToken)?;
        
        // Verify token
        let claims = verify_jwt_token(token).map_err(|_| AuthError::InvalidToken)?;
        
        // For now, we'll trust the JWT token without checking the database
        // This makes the extractor Send-safe
        // In production, you might want to check a cache or use a different approach
        
        Ok(AuthUser {
            user_id: claims.sub,
        })
    }
}

#[derive(Debug)]
pub enum AuthError {
    MissingToken,
    InvalidToken,
    SessionExpired,
    DatabaseError,
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AuthError::MissingToken => (StatusCode::UNAUTHORIZED, "Missing authentication token"),
            AuthError::InvalidToken => (StatusCode::UNAUTHORIZED, "Invalid authentication token"),
            AuthError::SessionExpired => (StatusCode::UNAUTHORIZED, "Session expired"),
            AuthError::DatabaseError => (StatusCode::INTERNAL_SERVER_ERROR, "Database error"),
        };
        
        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}