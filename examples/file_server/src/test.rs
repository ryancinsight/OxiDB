use axum::{
    routing::get,
    Router,
    Json,
    response::IntoResponse,
    http::StatusCode,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct TestPayload {
    message: String,
}

// Simple handler
async fn hello() -> &'static str {
    "Hello, World!"
}

// Handler with JSON
async fn json_handler() -> Json<TestPayload> {
    Json(TestPayload {
        message: "Hello from JSON".to_string(),
    })
}

// Handler with Result
async fn result_handler() -> Result<Json<TestPayload>, StatusCode> {
    Ok(Json(TestPayload {
        message: "Hello from Result".to_string(),
    }))
}

// Custom error type
#[derive(Debug)]
struct MyError;

impl IntoResponse for MyError {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::INTERNAL_SERVER_ERROR, "Something went wrong").into_response()
    }
}

// Handler with custom error
async fn custom_error_handler() -> Result<Json<TestPayload>, MyError> {
    Ok(Json(TestPayload {
        message: "Hello with custom error".to_string(),
    }))
}

pub fn test_routes() -> Router {
    Router::new()
        .route("/", get(hello))
        .route("/json", get(json_handler))
        .route("/result", get(result_handler))
        .route("/custom", get(custom_error_handler))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_handlers_compile() {
        let app = test_routes();
        // If this compiles, our handlers are valid
    }
}