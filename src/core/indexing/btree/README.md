# B+-Tree Index Module (`src/core/indexing/btree/`)

## Module Purpose

This module implements a B+-Tree indexing structure for Oxidb. B+-Trees are chosen for their efficiency in handling range queries and maintaining sorted order, making them suitable for general-purpose database indexing. This implementation provides a persistent, disk-based B+-Tree.

## Key Components

*   **`node.rs`**:
    *   Defines the `BPlusTreeNode` enum, which has variants for `Internal` and `Leaf` nodes.
    *   Handles the structure of keys, child pointers (for internal nodes), and primary key lists (for leaf nodes).
    *   Implements node-level operations such as searching within a node, splitting a full node, and checking if a node is full.
    *   Includes logic for serializing `BPlusTreeNode` instances to a byte representation for disk storage and deserializing them back into memory.

*   **`tree.rs`**:
    *   Defines the `BPlusTreeIndex` struct, which represents an entire B+-Tree.
    *   Manages the tree's data file, including a metadata header (storing tree order, root page ID, next available page ID).
    *   Implements core B+-Tree algorithms:
        *   `insert(key, primary_key)`: Inserts a key and its associated primary key. Handles node splits that propagate up to the root if necessary.
        *   `delete(key, primary_key_option)`: Deletes a key (or a specific primary key from its list). Handles node underflow by performing rebalancing operations:
            *   **Borrowing:** Keys may be borrowed from a sufficiently full sibling node.
            *   **Merging:** Underflowed nodes may be merged with a sibling if borrowing is not possible.
            Rebalancing operations can cascade up to the root, potentially decreasing tree height.
        *   `find_primary_keys(key)`: Searches the tree to find a list of primary keys associated with a given key.
    *   Handles Node I/O: Reads and writes tree nodes to/from the disk file using fixed-size pages. Manages page allocation.

*   **`mod.rs`**:
    *   Serves as the public interface for the `btree` module.
    *   Re-exports `BPlusTreeIndex` and other necessary types.
    *   Implements the common `Index` trait (defined in `src/core/indexing/traits.rs`) for `BPlusTreeIndex`. This makes the B+-Tree compatible with the `IndexManager` and usable by the higher-level query processing components of the database.
    *   Includes mapping from internal `btree::OxidbError` to the common `OxidbError` type used by the `Index` trait.

## Functionality

*   **Ordered Key Storage:** Stores keys in a sorted manner, allowing for efficient range queries (though range query API is not yet explicitly exposed by `IndexManager`).
*   **Efficient Operations:** Provides logarithmic time complexity for lookups, insertions, and deletions on average and in the worst case due to its self-balancing nature.
*   **Rebalancing:** Automatically handles node splits on overflow and node borrowing/merging on underflow to maintain the B+-Tree structural properties.
*   **Duplicate Primary Keys:** For a given indexed value (key), multiple primary keys can be stored, allowing the index to point to all rows that share that indexed value.

## Persistence

*   The B+-Tree is persisted to a single file (e.g., `index_name.btree`).
*   It uses fixed-size pages for storing nodes, simplifying page management and I/O.
*   A metadata header at the beginning of the file stores essential information about the tree, such as its order and the location of the root node.
*   Changes to nodes and metadata are written to disk as they occur, with `save()` on the `Index` trait ensuring all data is flushed.

## Integration

*   The `BPlusTreeIndex` is integrated into the database system via the `IndexManager` (`src/core/indexing/manager.rs`).
*   The `IndexManager` can create, load, and manage instances of `BPlusTreeIndex` alongside other index types (like `HashIndex`).
*   It conforms to the `Index` trait, allowing generic interaction from other parts of the database.

## Known Issues / Future Work
*   **Concurrency Control:** The current implementation is not designed for concurrent access and is single-threaded. Future work would involve adding latching mechanisms (e.g., lock-coupling or B-link tree modifications) for safe multi-threaded operations.
*   **File Handle Mutability for Reads:** The `read_node` method currently uses an `unsafe` block to perform mutable file operations (`seek`, `read_exact`) on a shared file handle when called from `&self` methods (required by the `Index` trait's `find` method). A proper refactor would involve using `RefCell<File>` or opening read-only file handles for such operations.
*   **Page Deallocation:** When nodes are merged (e.g., during deletion), the page of the emptied node is not currently deallocated or added to a free list. This means the database file will only grow.
*   **Bulk Loading:** For creating an index on a table with existing data, individual insertions can be slow. A bulk loading mechanism would improve performance significantly.
*   **Variable-Length Keys:** While keys are `Vec<u8>`, the fixed-size page model assumes keys are generally not excessively large to avoid significant internal fragmentation or nodes exceeding page size. More sophisticated handling for very large keys might be needed.
