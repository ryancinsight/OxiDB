// src/core/rag/mod.rs

pub mod core_components;
pub mod embedder;
pub mod graphrag;
pub mod retriever; // Added GraphRAG module
                   // Add other RAG specific modules like document_loader, text_splitter if needed.

// Re-export key components for easier access if desired
pub use self::core_components::{Document, Embedding};
pub use self::embedder::EmbeddingModel;
pub use self::graphrag::{
    GraphRAGContext, GraphRAGEngine, GraphRAGResult, KnowledgeEdge, KnowledgeNode,
};
pub use self::retriever::Retriever;
