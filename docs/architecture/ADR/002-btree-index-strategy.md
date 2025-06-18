# ADR 002: B-Tree Indexing Strategy

**Status:** Accepted
**Date Accepted:** 2024-03-15 (Placeholder Date)

**Context:**

We need an efficient indexing strategy for our database to speed up query performance. Common options include B-Trees, B+-Trees, Hash Indexes, and LSM Trees. Each has its own trade-offs in terms of read/write performance, storage overhead, and complexity. Given our focus on balanced performance with a slight preference for read-heavy workloads and range queries, a B-Tree variant is a strong candidate.

**Decision:**

We will implement a **B+-Tree** based indexing strategy.

**Details:**

The B+-Tree offers several advantages that align with our needs:

*   **Efficient Range Queries:** All data pointers (or actual data, depending on clustered vs. non-clustered) reside in the leaf nodes, which are linked sequentially. This makes range scans (e.g., `WHERE age > 30`) very efficient as it only requires traversing the leaf nodes.
*   **Good Read Performance:** B+-Trees provide logarithmic time complexity for point lookups, updates, and deletes.
*   **Storage Utilization:** Leaf nodes in B+-Trees can typically store more entries compared to internal nodes in a classic B-Tree (where internal nodes also store data pointers), potentially leading to better storage utilization and a shallower tree.
*   **Balanced Structure:** The tree remains balanced through splits and merges, ensuring that worst-case performance remains predictable.

**Implementation Considerations:**

*   **Node Structure:**
    *   Internal Nodes: Will store keys and pointers to child nodes.
    *   Leaf Nodes: Will store keys and pointers to actual data records (for non-clustered indexes) or the data itself (for clustered indexes). Leaf nodes will also have pointers to their next sibling to facilitate range scans.
*   **Concurrency Control:** We will need to implement a robust concurrency control mechanism (e.g., lock coupling or B-link trees) to allow multiple transactions to access and modify the index simultaneously. (Note: Current implementation is single-threaded).
*   **Write Operations (Inserts, Deletes, Updates):**
    *   Inserts: Find the appropriate leaf node. If full, split the node. This might propagate splits up the tree. (Implemented)
    *   Deletes: Find the entry and remove it. If the node falls below a certain fill factor, it may merge with a sibling or redistribute keys. This might propagate changes up the tree. (Implemented, including rebalancing via borrow/merge)
    *   Updates: If the indexed key is updated, it's typically a delete followed by an insert. If only the data record is updated and the key remains the same, the index might not need changes (unless it's a clustered index where data is in leaves). (Implemented via Index trait's update)
*   **Initial Build:** For creating an index on existing data, a bulk loading approach (e.g., sorting data first and then building the tree bottom-up) will be more efficient than individual inserts. (Future consideration)

**Apocrypha:**

*   Initial discussions considered Hash Indexes for their O(1) average-case lookups. However, their inability to efficiently handle range queries and potential performance degradation with high collision rates made them less suitable as a primary indexing strategy.
*   LSM (Log-Structured Merge) Trees were also evaluated, offering excellent write performance. However, their read performance can be less predictable, and compaction can introduce performance stalls. We might reconsider LSM-Trees for specific write-heavy workloads or tables in the future.
*   Classic B-Trees (where data pointers can reside in internal nodes) were considered, but the advantages of B+-Trees for range scans and potentially better leaf node density swayed the decision.

**Concordance:**

*   This decision directly impacts the Storage Engine (ADR 001), as the chosen indexing mechanism will be a core component of how data is stored and retrieved.
*   Future ADRs related to query optimization will rely on the characteristics of this B+-Tree implementation.
*   Transaction management and concurrency control mechanisms will need to be compatible with the chosen B+-Tree implementation strategy.

**Implementation Notes (Added 2024-03-15):**
The B+-Tree has been implemented as `src/core/indexing/btree/BPlusTreeIndex`. Key features include:
- Fixed-size page management for nodes within a single file per index.
- Support for `insert`, `find`, and `delete` operations.
- Delete operations include rebalancing logic (borrowing from siblings and merging nodes) for both leaf and internal nodes to maintain tree properties.
- Integration with the `IndexManager` via the `Index` trait.
- Current implementation is single-threaded; concurrency control is a future consideration.
- The `&self` vs `&mut self` for read operations on the file handle was addressed pragmatically using an `unsafe` block in `read_node` for this iteration, with a note for future refactoring using `RefCell` or similar for the file handle.
