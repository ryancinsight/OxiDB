# Simple Blog Example for Oxidb

This example demonstrates a basic command-line interface (CLI) application for managing a simple blog. It is built with Rust and uses `oxidb` for data storage.

## Purpose

The primary purpose of this example is to showcase multi-table data management and CRUD (Create, Read, Update, Delete) operations using `oxidb`. It involves creating tables for authors and posts, attempting to link them, and performing operations on them.

This example aims to illustrate:
- Schema creation for multiple related tables.
- Inserting data into tables.
- Basic CLI interactions for managing blog content.
- Querying and updating data.

## Prerequisites

- Rust and Cargo installed (https://www.rust-lang.org/tools/install)
- The `oxidb` crate should be present two levels above this example's directory (i.e., `../../oxidb`). The path dependency is configured in `Cargo.toml`.

## Building the Example

To build the application, navigate to the `examples/simple_blog` directory and run:

```bash
cargo build
```

## Running the Example

You can run the application directly using `cargo run -- <COMMAND> [OPTIONS]`. All arguments after `--` will be passed to the application. The database file `simple_blog.db` will be created in the current directory.

### 1. Initialize the Database
This step creates the `authors` and `posts` tables if they don't already exist.
```bash
cargo run -- init-db
```

### 2. Add Authors
```bash
cargo run -- add-author --name "Alice Wonderland"
cargo run -- add-author --name "Bob The Builder"
```

### 3. List Authors
```bash
cargo run -- list-authors
```
*(See "Known Limitations" regarding data visibility)*

### 4. Add Posts
(Assuming Alice's ID is 1 and Bob's ID is 2 from a fresh database. This is fragile due to ID visibility limitations.)
```bash
cargo run -- add-post --author-id "1" --title "Alice's First Post" --content "Hello from Wonderland!"
cargo run -- add-post --author-id "2" --title "Bob's Great Idea" --content "We can build it!"
cargo run -- add-post --author-id "1" --title "Alice's Second Adventure" --content "Down the rabbit hole again."
```

### 5. List All Posts
```bash
cargo run -- list-posts
```

### 6. List Posts by a Specific Author
```bash
cargo run -- list-posts-by-author --author-id "1"
```

### 7. Get Specific Post Details
```bash
cargo run -- get-post --id "1"
```

### 8. Publish a Post
```bash
cargo run -- publish-post --id "1"
```
Check `get-post` again to see the updated `published_date`.

### 9. Delete a Post
```bash
cargo run -- delete-post --id "2"
```

### 10. Delete an Author
(This will also attempt to delete their posts first.)
```bash
cargo run -- delete-author --id "2"
```

## Available Commands

-   `init-db`: Initializes the database by creating the `authors` and `posts` tables.
-   `add-author --name <NAME>`: Adds a new author.
-   `list-authors`: Lists all authors.
-   `delete-author --id <AUTHOR_ID>`: Deletes an author and all their posts.
-   `add-post --author-id <AUTHOR_ID> --title <TITLE> [--content <CONTENT>]`: Adds a new post.
-   `list-posts`: Lists all posts, including author names.
-   `list-posts-by-author --author-id <AUTHOR_ID>`: Lists posts for a specific author.
-   `get-post --id <POST_ID>`: Shows detailed information for a specific post, including author details.
-   `publish-post --id <POST_ID>`: Marks a post as published by setting its `published_date`.
-   `delete-post --id <POST_ID>`: Deletes a specific post.

## Known Limitations & Oxidb Observations

This example has revealed several limitations or specific behaviors of the current version of `oxidb`:

1.  **Last Inserted ID**: The application cannot display the ID of a newly added item (author or post) when using `INTEGER PRIMARY KEY AUTOINCREMENT`. `oxidb` does not currently support a standard SQL mechanism to retrieve the last inserted ID (e.g., `RETURNING id`, `LAST_INSERT_ID()`).
2.  **Data Visibility After `INSERT`**: Data inserted into tables (both `TEXT PRIMARY KEY` in earlier tests and `INTEGER PRIMARY KEY AUTOINCREMENT` currently) may not be immediately visible to subsequent `SELECT` queries, even within the same `Oxidb` instance and before any `persist()` call. This significantly impacts the ability to reliably fetch data just written, making testing and interactive use difficult. For example, `list-authors` or `get_author` may not find an author immediately after `add-author` reports success. Data *may* become visible after the database is persisted and reloaded in a new application run, but this is not consistent or ideal.
3.  **Column Projection in `SELECT`**: `oxidb` appears to most reliably support `SELECT *` for querying. While the SQL parser might accept `SELECT column_name1, column_name2 FROM ...`, this was found to cause errors in earlier `todo_app` testing, necessitating reliance on `SELECT *` and client-side parsing of the full key-value map data structure returned by `oxidb`.
4.  **`FOREIGN KEY` Constraints**: The `CREATE TABLE` statement for `posts` initially included a `FOREIGN KEY` constraint. This caused SQL parsing errors. The constraint was removed to allow table creation. This means foreign key relationships are not currently enforced by `oxidb` at the database level in this example, and cascading deletes (like deleting an author's posts when the author is deleted) must be handled manually by the application logic.
5.  **Error Messages for "Table already exists"**: The `CREATE TABLE` statements in `oxidb` appear to be idempotent (like `CREATE TABLE IF NOT EXISTS`), as they don't error if the table already exists. This simplifies table creation logic.

These observations are based on the behavior encountered while developing this example and the `todo_app`. The example code attempts to work around these or operate with these limitations in mind.
The data is stored in a local file named `simple_blog.db`.
