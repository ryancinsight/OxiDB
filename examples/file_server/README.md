# OxiDB File Server Example

This example demonstrates a complete file serving website with user authentication and user-based storage using OxiDB as the backend database.

## Features

- **User Authentication**: Registration and login with JWT tokens
- **File Upload/Download**: Users can upload and download files
- **User-based Storage**: Each user has their own storage space
- **File Sharing**: Users can share files with other users
- **Access Control**: Files are private by default, with sharing permissions
- **Web Interface**: Clean, responsive web UI

## Architecture

### Backend (Rust + Axum)
- `main.rs`: Application entry point and server setup
- `db.rs`: Database initialization and connection management
- `models.rs`: Data structures for users, files, and sessions
- `auth.rs`: Authentication logic with JWT tokens
- `handlers.rs`: HTTP request handlers for all endpoints

### Frontend (HTML/CSS/JS)
- `static/index.html`: Main HTML page
- `static/style.css`: Styling for the UI
- `static/app.js`: JavaScript for interactivity and API calls

### Database Schema (OxiDB)
- **users**: User accounts with hashed passwords
- **files**: File metadata and storage paths
- **file_shares**: File sharing permissions
- **sessions**: Active user sessions

## API Endpoints

### Authentication
- `POST /api/auth/register` - Create new user account
- `POST /api/auth/login` - Login and receive JWT token
- `POST /api/auth/logout` - Logout and invalidate session

### Files
- `GET /api/files` - List user's files (with optional shared files)
- `POST /api/files` - Upload a new file
- `GET /api/files/:id` - Get file metadata
- `DELETE /api/files/:id` - Delete a file (owner only)
- `GET /api/files/:id/download` - Download file content
- `POST /api/files/:id/share` - Share file with another user
- `POST /api/files/:id/unshare` - Remove file share

### Users
- `GET /api/users/me` - Get current user info

## Running the Example

1. Build the project:
```bash
cd examples/file_server
cargo build
```

2. Run the server:
```bash
cargo run
```

3. Open your browser to http://localhost:3000

## Usage

1. **Register**: Create a new account with username, email, and password
2. **Login**: Sign in with your credentials
3. **Upload Files**: Use the upload form to add files
4. **Manage Files**: View, download, share, or delete your files
5. **Share Files**: Share files with other users by username

## Security Features

- Passwords are hashed with bcrypt
- JWT tokens for stateless authentication
- File access is restricted to owners and shared users
- SQL injection prevention through parameterized queries
- CORS protection via tower-http

## Technical Notes

### Known Limitations

1. **Handler Signatures**: Due to Axum's extractor ordering requirements, some handler functions may need parameter reordering. Custom extractors (like `AuthUser`) should typically come last.

2. **Send Trait**: The OxiDB Connection type may not be Send, which can cause issues with async handlers. This can be worked around by:
   - Using spawn_blocking for database operations
   - Simplifying extractors to avoid database calls
   - Using a connection pool wrapper

3. **Value Type**: OxiDB's Value enum doesn't have convenience methods like `as_text()`. Pattern matching is required:
```rust
match value {
    Value::Text(s) => Some(s.as_str()),
    _ => None,
}
```

### Potential Improvements

1. Add file preview functionality
2. Implement file versioning
3. Add user quotas and storage limits
4. Support folder organization
5. Add file search capabilities
6. Implement real-time notifications for shared files
7. Add support for public file links
8. Implement file encryption for sensitive data

## Dependencies

- **oxidb**: The database engine
- **axum**: Web framework
- **tokio**: Async runtime
- **bcrypt**: Password hashing
- **jsonwebtoken**: JWT authentication
- **uuid**: Unique identifiers
- **chrono**: Date/time handling
- **serde**: Serialization