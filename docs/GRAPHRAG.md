# GraphRAG in Oxidb

## Overview

Oxidb now includes comprehensive Graph Database and GraphRAG (Graph-enhanced Retrieval-Augmented Generation) capabilities, making it a powerful alternative to traditional databases like libsql, PostgreSQL, and MySQL for applications requiring both relational and graph-based data processing with vector similarity search.

## Architecture

The GraphRAG implementation follows SOLID design principles:

- **Single Responsibility**: Each module has a focused purpose
- **Open/Closed**: Extensible without modifying existing code
- **Liskov Substitution**: Implementations are interchangeable
- **Interface Segregation**: Minimal, focused interfaces
- **Dependency Inversion**: Depends on abstractions

Additional design principles applied:
- **CUPID**: Composable, Unix-like, Predictable, Idiomatic, Domain-focused
- **GRASP**: High cohesion, low coupling, information expert pattern
- **DRY**: No code duplication
- **YAGNI**: Only implement what's needed
- **ACID**: Atomicity, Consistency, Isolation, Durability for transactions

## Core Components

### 1. Graph Database (`src/core/graph/`)

#### Graph Types (`types.rs`)
```rust
// Core graph data structures
pub struct Node {
    pub id: NodeId,
    pub data: GraphData,
    pub created_at: u64,
    pub updated_at: u64,
}

pub struct Edge {
    pub id: EdgeId,
    pub from_node: NodeId,
    pub to_node: NodeId,
    pub relationship: Relationship,
    pub data: Option<GraphData>,
    pub weight: Option<f64>,
}

pub struct Relationship {
    pub name: String,
    pub direction: RelationshipDirection,
}
```

#### Graph Operations
```rust
// Basic graph operations
pub trait GraphOperations {
    fn add_node(&mut self, data: GraphData) -> Result<NodeId, OxidbError>;
    fn add_edge(&mut self, from: NodeId, to: NodeId, relationship: Relationship, data: Option<GraphData>) -> Result<EdgeId, OxidbError>;
    fn get_node(&self, node_id: NodeId) -> Result<Option<Node>, OxidbError>;
    fn remove_node(&mut self, node_id: NodeId) -> Result<bool, OxidbError>;
    fn get_neighbors(&self, node_id: NodeId, direction: TraversalDirection) -> Result<Vec<NodeId>, OxidbError>;
}

// Advanced graph queries
pub trait GraphQuery {
    fn find_nodes_by_property(&self, property: &str, value: &DataType) -> Result<Vec<NodeId>, OxidbError>;
    fn find_shortest_path(&self, from: NodeId, to: NodeId) -> Result<Option<Vec<NodeId>>, OxidbError>;
    fn traverse(&self, start: NodeId, strategy: TraversalStrategy, max_depth: Option<usize>) -> Result<Vec<NodeId>, OxidbError>;
}
```

#### Graph Algorithms (`algorithms.rs`)
- **Centrality Measures**: Betweenness, closeness, degree centrality
- **Pathfinding**: Dijkstra's algorithm, A* search
- **Community Detection**: Connected components, label propagation
- **Graph Metrics**: Clustering coefficient, graph diameter

#### Graph Traversal (`traversal.rs`)
- **Breadth-First Search (BFS)**
- **Depth-First Search (DFS)**
- **Visitor Pattern**: Custom operations during traversal
- **Path Reconstruction**: Full path tracking with metadata

### 2. GraphRAG Engine (`src/core/rag/graphrag.rs`)

#### Knowledge Graph Entities
```rust
pub struct KnowledgeNode {
    pub id: NodeId,
    pub entity_type: String,
    pub name: String,
    pub description: Option<String>,
    pub embedding: Option<Embedding>,
    pub properties: HashMap<String, DataType>,
    pub confidence_score: f64,
}

pub struct KnowledgeEdge {
    pub id: EdgeId,
    pub from_entity: NodeId,
    pub to_entity: NodeId,
    pub relationship_type: String,
    pub confidence_score: f64,
    pub weight: Option<f64>,
}
```

#### GraphRAG Operations
```rust
#[async_trait]
pub trait GraphRAGEngine: Send + Sync {
    // Build knowledge graph from documents
    async fn build_knowledge_graph(&mut self, documents: &[Document]) -> Result<(), OxidbError>;
    
    // Enhanced retrieval with graph context
    async fn retrieve_with_graph(&self, context: GraphRAGContext) -> Result<GraphRAGResult, OxidbError>;
    
    // Entity and relationship management
    async fn add_entity(&mut self, entity: KnowledgeNode) -> Result<NodeId, OxidbError>;
    async fn add_relationship(&mut self, relationship: KnowledgeEdge) -> Result<EdgeId, OxidbError>;
    
    // Graph-based queries
    async fn find_related_entities(&self, entity_id: NodeId, max_hops: usize) -> Result<Vec<KnowledgeNode>, OxidbError>;
    async fn get_reasoning_paths(&self, from: NodeId, to: NodeId, max_paths: usize) -> Result<Vec<ReasoningPath>, OxidbError>;
}
```

## Usage Examples

### Basic Graph Operations

```rust
use oxidb::core::graph::{GraphFactory, GraphOperations, GraphData, Relationship};
use oxidb::core::types::DataType;

// Create a graph
let mut graph = GraphFactory::create_memory_graph()?;

// Add nodes
let person_data = GraphData::new("Person".to_string())
    .with_property("name".to_string(), DataType::String("Alice".to_string()))
    .with_property("age".to_string(), DataType::Integer(30));
let alice_id = graph.add_node(person_data)?;

let company_data = GraphData::new("Company".to_string())
    .with_property("name".to_string(), DataType::String("TechCorp".to_string()));
let company_id = graph.add_node(company_data)?;

// Add relationship
let works_at = Relationship::new("WORKS_AT".to_string());
graph.add_edge(alice_id, company_id, works_at, None)?;

// Query the graph
let neighbors = graph.get_neighbors(alice_id, TraversalDirection::Outgoing)?;
let path = graph.find_shortest_path(alice_id, company_id)?;
```

### GraphRAG with Knowledge Graphs

```rust
use oxidb::core::rag::{Document, GraphRAGEngine, GraphRAGContext};
use oxidb::core::rag::graphrag::GraphRAGEngineImpl;
use oxidb::core::rag::retriever::InMemoryRetriever;

// Set up documents and retriever
let documents = vec![
    Document {
        id: "doc1".to_string(),
        content: "Alice works at TechCorp as a software engineer.".to_string(),
        embedding: Some(vec![0.1, 0.2, 0.3].into()),
        metadata: Some(HashMap::new()),
    }
];

let retriever = Box::new(InMemoryRetriever::new(documents.clone()));
let mut graphrag_engine = GraphRAGEngineImpl::new(retriever);

// Build knowledge graph
graphrag_engine.build_knowledge_graph(&documents).await?;

// Add custom entities
let alice_entity = KnowledgeNode {
    id: 0,
    entity_type: "PERSON".to_string(),
    name: "Alice".to_string(),
    description: Some("Software Engineer".to_string()),
    embedding: Some(vec![0.1, 0.2, 0.3].into()),
    properties: HashMap::new(),
    confidence_score: 0.9,
};

let alice_id = graphrag_engine.add_entity(alice_entity).await?;

// Perform graph-enhanced retrieval
let query_context = GraphRAGContext {
    query_embedding: vec![0.12, 0.22, 0.32].into(),
    max_hops: 2,
    min_confidence: 0.5,
    include_relationships: vec!["WORKS_AT".to_string()],
    exclude_relationships: vec![],
    entity_types: vec!["PERSON".to_string()],
};

let result = graphrag_engine.retrieve_with_graph(query_context).await?;
```

## Advanced Features

### Transaction Support

All graph operations support ACID transactions:

```rust
// Begin transaction
graph.begin_transaction()?;

// Perform operations
let node_id = graph.add_node(data)?;
graph.add_edge(node_id, other_node, relationship, None)?;

// Commit or rollback
graph.commit_transaction()?; // or graph.rollback_transaction()?
```

### Vector Similarity Integration

GraphRAG seamlessly integrates with Oxidb's vector similarity capabilities:

```rust
// Entities can have embeddings for similarity search
let similar_entities = graphrag_engine.find_similar_entities(&query_embedding, 10, 0.7)?;

// Graph traversal can be guided by embedding similarity
let context_expanded_entities = graphrag_engine.expand_entity_context(&entity_ids, 3)?;
```

### Reasoning Paths

GraphRAG provides explainable reasoning through path analysis:

```rust
let reasoning_paths = graphrag_engine.get_reasoning_paths(alice_id, bob_id, 5).await?;

for path in reasoning_paths {
    println!("Path: {:?}", path.path_nodes);
    println!("Relationships: {:?}", path.path_relationships);
    println!("Score: {:.2}", path.reasoning_score);
    println!("Explanation: {}", path.explanation);
}
```

## Performance Considerations

### Memory Management
- **Copy-on-Write (COW)**: Efficient memory usage for large graphs
- **Lazy Loading**: Nodes and edges loaded on demand
- **Connection Pooling**: Efficient resource management

### Indexing
- **HNSW Indexing**: Hierarchical Navigable Small World for vector similarity
- **B-tree Indexing**: Traditional indexing for property-based queries
- **Graph-specific Indexes**: Optimized for traversal operations

### Concurrency
- **Read-Write Locks**: Concurrent read access with exclusive writes
- **Transaction Isolation**: ACID compliance with configurable isolation levels
- **Async/Await**: Non-blocking operations throughout

## Comparison with Traditional Databases

| Feature | Oxidb GraphRAG | PostgreSQL | MySQL | libsql |
|---------|----------------|------------|-------|--------|
| Graph Operations | ✅ Native | ❌ Limited | ❌ No | ❌ No |
| Vector Similarity | ✅ Native | 🔶 Extension | ❌ No | ❌ No |
| ACID Transactions | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes |
| Horizontal Scaling | 🔶 Planned | 🔶 Complex | 🔶 Complex | ✅ Yes |
| Memory Safety | ✅ Rust | ❌ C/C++ | ❌ C/C++ | ✅ Rust |
| GraphRAG | ✅ Native | ❌ No | ❌ No | ❌ No |

## Integration Examples

### With Existing RAG Systems

```rust
// Replace traditional vector store with GraphRAG
let traditional_retriever = VectorStore::new(embeddings);
let graph_enhanced_retriever = GraphRAGEngineImpl::new(Box::new(traditional_retriever));

// Existing code continues to work, but with graph enhancement
let results = graph_enhanced_retriever.retrieve(&query, 10).await?;
```

### With Knowledge Graph Construction

```rust
// Automatic entity extraction and relationship discovery
let documents = load_documents_from_source()?;
graphrag_engine.build_knowledge_graph(&documents).await?;

// Manual knowledge graph curation
let expert_knowledge = load_expert_annotations()?;
for annotation in expert_knowledge {
    graphrag_engine.add_entity(annotation.entity).await?;
    graphrag_engine.add_relationship(annotation.relationship).await?;
}
```

## Future Roadmap

### Planned Features
1. **Distributed Graph Processing**: Multi-node graph operations
2. **Graph Neural Networks**: Deep learning on graph structures
3. **Temporal Graphs**: Time-aware graph operations
4. **Graph Visualization**: Built-in graph visualization tools
5. **SQL Integration**: Graph queries through SQL interface

### Performance Optimizations
1. **Parallel Graph Algorithms**: Multi-threaded graph processing
2. **GPU Acceleration**: CUDA support for large-scale operations
3. **Incremental Updates**: Efficient graph modification
4. **Compression**: Graph data compression techniques

## Contributing

The GraphRAG implementation follows strict design principles. When contributing:

1. **Follow SOLID Principles**: Each component should have a single responsibility
2. **Maintain ACID Properties**: All operations must be atomic, consistent, isolated, and durable
3. **Add Comprehensive Tests**: Include unit, integration, and performance tests
4. **Document Thoroughly**: Update both code comments and user documentation
5. **Consider Performance**: Profile and optimize critical paths

## Examples

See the `examples/graphrag_demo/` directory for a comprehensive demonstration of GraphRAG capabilities, including:

- Document-based knowledge graph construction
- Custom entity and relationship management
- Graph-enhanced retrieval with reasoning paths
- Pure graph database operations
- Integration with existing RAG systems

Run the demo with:
```bash
cargo run --example graphrag_demo
```