# Todo App Example

This example demonstrates a simple command-line interface (CLI) todo application built with Rust. It uses `oxidb` for data storage and `clap` for parsing CLI arguments.

## Purpose

The primary purpose of this example is to showcase a basic integration of the `oxidb` database with a common Rust CLI pattern. You can add, list, and mark todo items as done.

## Prerequisites

- Rust and Cargo installed (https://www.rust-lang.org/tools/install)
- The `oxidb` crate should be present in the parent directory (this example uses a path dependency `../../../oxidb`).

## Building the Example

To build the application, navigate to the root of the repository and run:

```bash
cargo build --manifest-path examples/todo_app/Cargo.toml
```

Alternatively, if you are already in the `examples/todo_app` directory, you can simply run:
```bash
cargo build
```
(This assumes you have a global `oxidb` crate or have adjusted the path in `Cargo.toml` if running standalone.)

## Running the Example

You can run the application directly using `cargo run`. All arguments after `--` will be passed to the application.

Navigate to the root of the repository and run:

### Add a new todo item
```bash
cargo run --manifest-path examples/todo_app/Cargo.toml -- add "Buy milk"
cargo run --manifest-path examples/todo_app/Cargo.toml -- add "Read a book on Rust"
```

### List all todo items
```bash
cargo run --manifest-path examples/todo_app/Cargo.toml -- list
```
Expected output:
```
Using database at: todo_app.db
Todo items:
[ ] 1 - Buy milk
[ ] 2 - Read a book on Rust
```

### Mark a todo item as done
To mark the item with ID `1` ("Buy milk") as done:
```bash
cargo run --manifest-path examples/todo_app/Cargo.toml -- done 1
```

### List items again to see the change
```bash
cargo run --manifest-path examples/todo_app/Cargo.toml -- list
```
Expected output:
```
Using database at: todo_app.db
Todo items:
[x] 1 - Buy milk
[ ] 2 - Read a book on Rust
```

## Available Commands

The application supports the following commands:

-   `add <DESCRIPTION>`: Adds a new todo item with the given description.
    -   Example: `cargo run --manifest-path examples/todo_app/Cargo.toml -- add "Schedule meeting"`
-   `list`: Lists all current todo items, showing their ID, status ([x] for done, [ ] for not done), and description.
    -   Example: `cargo run --manifest-path examples/todo_app/Cargo.toml -- list`
-   `done <ID>`: Marks the todo item with the specified ID as done.
    -   Example: `cargo run --manifest-path examples/todo_app/Cargo.toml -- done 2`

The todo items are stored in a local file named `todo_app.db` in the current directory where you run the command.
If `todo_app.db` does not exist, it will be created automatically.
If the `todos` table does not exist within the database, it will also be created automatically.
