//! GraphRAG module - Combines graph database capabilities with RAG
//!
//! This module is organized following SOLID principles:
//! - Single Responsibility: Each submodule handles a specific concern
//! - Open/Closed: Traits allow extension without modification
//! - Liskov Substitution: Implementations are interchangeable
//! - Interface Segregation: Focused traits for specific capabilities
//! - Dependency Inversion: Depend on abstractions, not concretions

pub mod builder;
pub mod engine;
pub mod factory;
pub mod iterators;
pub mod types;

// Re-export key types for convenience
pub use builder::GraphRAGEngineBuilder;
pub use engine::{GraphRAGEngine, GraphRAGEngineImpl};
pub use factory::GraphRAGFactory;
pub use iterators::SimilarityIterator;
pub use types::{
    GraphRAGConfig, GraphRAGContext, GraphRAGResult, KnowledgeEdge, KnowledgeNode, ReasoningPath,
};
