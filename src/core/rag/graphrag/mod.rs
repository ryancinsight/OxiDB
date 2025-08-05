//! GraphRAG module - Combines graph database capabilities with RAG
//!
//! This module is organized following SOLID principles:
//! - Single Responsibility: Each submodule handles a specific concern
//! - Open/Closed: Traits allow extension without modification
//! - Liskov Substitution: Implementations are interchangeable
//! - Interface Segregation: Focused traits for specific capabilities
//! - Dependency Inversion: Depend on abstractions, not concretions

pub mod engine;
pub mod types;
pub mod iterators;
pub mod builder;
pub mod factory;

// Re-export key types for convenience
pub use engine::{GraphRAGEngine, GraphRAGEngineImpl};
pub use types::{GraphRAGContext, GraphRAGResult, ReasoningPath, KnowledgeNode, KnowledgeEdge, GraphRAGConfig};
pub use iterators::SimilarityIterator;
pub use builder::GraphRAGEngineBuilder;
pub use factory::GraphRAGFactory;