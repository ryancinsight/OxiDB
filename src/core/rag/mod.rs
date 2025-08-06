// src/core/rag/mod.rs

pub mod document;
pub mod embedder;
pub mod graphrag;
pub mod hybrid;
pub mod retriever;

// Re-export key components for easier access
pub use self::document::{Document, Embedding};
pub use self::embedder::{EmbeddingModel, SemanticEmbedder, TfIdfEmbedder};
pub use self::graphrag::{
    GraphRAGContext, GraphRAGEngine, GraphRAGResult, KnowledgeEdge, KnowledgeNode,
    GraphRAGConfig, GraphRAGEngineBuilder,
};
pub use self::hybrid::{HybridRAGConfig, HybridRAGEngine, HybridRAGEngineBuilder, HybridRAGResult};
pub use self::retriever::Retriever;
