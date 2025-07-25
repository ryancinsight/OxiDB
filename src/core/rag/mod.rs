// src/core/rag/mod.rs

pub mod core_components;
pub mod embedder;
pub mod graphrag;
pub mod retriever;

// Re-export key components for easier access
pub use self::core_components::{Document, Embedding};
pub use self::embedder::{EmbeddingModel, SemanticEmbedder, TfIdfEmbedder};
pub use self::graphrag::{
    GraphRAGContext, GraphRAGEngine, GraphRAGResult, KnowledgeEdge, KnowledgeNode,
};
pub use self::retriever::Retriever;
