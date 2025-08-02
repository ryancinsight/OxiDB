use axum::{
    extract::Json,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct MyError(anyhow::Error);

impl From<anyhow::Error> for MyError {
    fn from(err: anyhow::Error) -> Self {
        MyError(err)
    }
}

impl IntoResponse for MyError {
    fn into_response(self) -> axum::response::Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

#[derive(Serialize, Deserialize)]
pub struct CreateUser {
    username: String,
}

#[derive(Serialize, Deserialize)]
pub struct User {
    id: u64,
    username: String,
}

async fn create_user(Json(payload): Json<CreateUser>) -> Result<Json<User>, MyError> {
    Ok(Json(User {
        id: 1337,
        username: payload.username,
    }))
}

async fn health() -> &'static str {
    "OK"
}

pub fn create_app() -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/users", post(create_user))
}