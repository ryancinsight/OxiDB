mod auth;
mod db;
mod handlers;
mod models;
mod test;
mod minimal;
mod working;

use anyhow::Result;
use axum::Router;
use std::net::SocketAddr;
use tower_http::services::ServeDir;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Initialize database
    let db_path = "file_server.db";
    db::init_database(db_path).await?;

    // Create upload directory if it doesn't exist
    tokio::fs::create_dir_all("uploads").await?;

    // Build our application with routes
    let app = Router::new()
        // API routes - using working routes for now
        .nest("/api", working::create_app())
        // Static file serving for uploaded files (protected)
        .nest_service("/files", ServeDir::new("uploads"))
        // Serve static assets (HTML, CSS, JS)
        .nest_service("/", ServeDir::new("static"));

    // Run the server
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("File server running on http://{}", addr);
    
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}