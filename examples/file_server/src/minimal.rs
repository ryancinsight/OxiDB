use axum::{
    routing::post,
    Router,
    Json,
    response::IntoResponse,
    http::StatusCode,
};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
struct AppError(String);

impl From<anyhow::Error> for AppError {
    fn from(err: anyhow::Error) -> Self {
        AppError(err.to_string())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0).into_response()
    }
}

#[derive(Serialize, Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Serialize, Deserialize)]
struct LoginResponse {
    token: String,
}

// This should work
async fn login(Json(req): Json<LoginRequest>) -> Result<Json<LoginResponse>, AppError> {
    Ok(Json(LoginResponse {
        token: format!("token_for_{}", req.username),
    }))
}

pub fn minimal_routes() -> Router {
    Router::new()
        .route("/login", post(login))
}