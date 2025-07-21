//! GraphRAG implementation for Oxidb
//!
//! This module combines graph database capabilities with Retrieval-Augmented Generation (RAG)
//! to provide enhanced knowledge retrieval and reasoning. Following SOLID principles with
//! modular, extensible design.

use super::core_components::{Document, Embedding};
use super::retriever::Retriever;
use crate::core::graph::{GraphOperations, GraphQuery, NodeId, EdgeId, GraphData, Relationship};
use crate::core::graph::storage::InMemoryGraphStore;
use crate::core::graph::traversal::TraversalDirection;
use crate::core::common::OxidbError;
use crate::core::types::DataType;
use std::collections::{HashMap, HashSet};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Knowledge graph node representing entities in the domain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeNode {
    pub id: NodeId,
    pub entity_type: String,
    pub name: String,
    pub description: Option<String>,
    pub embedding: Option<Embedding>,
    pub properties: HashMap<String, DataType>,
    pub confidence_score: f64, // 0.0 to 1.0
}

/// Knowledge graph edge representing relationships between entities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeEdge {
    pub id: EdgeId,
    pub from_entity: NodeId,
    pub to_entity: NodeId,
    pub relationship_type: String,
    pub description: Option<String>,
    pub confidence_score: f64, // 0.0 to 1.0
    pub weight: Option<f64>,
}

/// GraphRAG query context for enhanced retrieval
#[derive(Debug, Clone)]
pub struct GraphRAGContext {
    pub query_embedding: Embedding,
    pub max_hops: usize,
    pub min_confidence: f64,
    pub include_relationships: Vec<String>,
    pub exclude_relationships: Vec<String>,
    pub entity_types: Vec<String>,
}

/// GraphRAG result containing retrieved information and reasoning paths
#[derive(Debug, Clone)]
pub struct GraphRAGResult {
    pub documents: Vec<Document>,
    pub reasoning_paths: Vec<ReasoningPath>,
    pub relevant_entities: Vec<KnowledgeNode>,
    pub entity_relationships: Vec<KnowledgeEdge>,
    pub confidence_score: f64,
}

/// Reasoning path showing how knowledge was derived
#[derive(Debug, Clone)]
pub struct ReasoningPath {
    pub path_nodes: Vec<NodeId>,
    pub path_relationships: Vec<String>,
    pub reasoning_score: f64,
    pub explanation: String,
}

/// GraphRAG engine trait following Interface Segregation Principle
#[async_trait]
pub trait GraphRAGEngine: Send + Sync {
    /// Build knowledge graph from documents
    async fn build_knowledge_graph(&mut self, documents: &[Document]) -> Result<(), OxidbError>;
    
    /// Retrieve information using graph-enhanced RAG
    async fn retrieve_with_graph(&self, context: GraphRAGContext) -> Result<GraphRAGResult, OxidbError>;
    
    /// Add entity to knowledge graph
    async fn add_entity(&mut self, entity: KnowledgeNode) -> Result<NodeId, OxidbError>;
    
    /// Add relationship to knowledge graph
    async fn add_relationship(&mut self, relationship: KnowledgeEdge) -> Result<EdgeId, OxidbError>;
    
    /// Find related entities
    async fn find_related_entities(&self, entity_id: NodeId, max_hops: usize) -> Result<Vec<KnowledgeNode>, OxidbError>;
    
    /// Get reasoning paths between entities
    async fn get_reasoning_paths(&self, from: NodeId, to: NodeId, max_paths: usize) -> Result<Vec<ReasoningPath>, OxidbError>;
}

/// Implementation of GraphRAG engine
pub struct GraphRAGEngineImpl {
    graph_store: InMemoryGraphStore,
    document_retriever: Box<dyn Retriever>,
    entity_embeddings: HashMap<NodeId, Embedding>,
    relationship_weights: HashMap<String, f64>,
    confidence_threshold: f64,
}

impl GraphRAGEngineImpl {
    /// Create a new GraphRAG engine
    pub fn new(document_retriever: Box<dyn Retriever>) -> Self {
        Self {
            graph_store: InMemoryGraphStore::new(),
            document_retriever,
            entity_embeddings: HashMap::new(),
            relationship_weights: Self::default_relationship_weights(),
            confidence_threshold: 0.5,
        }
    }

    /// Set confidence threshold for filtering results
    pub fn set_confidence_threshold(&mut self, threshold: f64) {
        self.confidence_threshold = threshold.clamp(0.0, 1.0);
    }

    /// Set custom relationship weights
    pub fn set_relationship_weights(&mut self, weights: HashMap<String, f64>) {
        self.relationship_weights = weights;
    }

    /// Default relationship weights for common relationship types
    fn default_relationship_weights() -> HashMap<String, f64> {
        let mut weights = HashMap::new();
        weights.insert("IS_A".to_string(), 1.0);
        weights.insert("PART_OF".to_string(), 0.8);
        weights.insert("RELATED_TO".to_string(), 0.6);
        weights.insert("MENTIONS".to_string(), 0.4);
        weights.insert("SIMILAR_TO".to_string(), 0.7);
        weights.insert("CAUSES".to_string(), 0.9);
        weights.insert("CONTAINS".to_string(), 0.8);
        weights.insert("DEPENDS_ON".to_string(), 0.8);
        weights
    }

    /// Extract entities from document text (simplified implementation)
    fn extract_entities(&self, document: &Document) -> Result<Vec<KnowledgeNode>, OxidbError> {
        // This is a simplified implementation. In practice, you would use
        // Named Entity Recognition (NER) and other NLP techniques
        let mut entities = Vec::new();
        let text = &document.content;
        
        // Simple keyword-based entity extraction (YAGNI - start simple)
        let keywords = text.split_whitespace()
            .filter(|word| word.len() > 3)
            .filter(|word| word.chars().next().unwrap_or(' ').is_uppercase())
            .collect::<HashSet<_>>();
        
        for (i, keyword) in keywords.iter().enumerate() {
            let entity = KnowledgeNode {
                id: i as NodeId + 1000, // Offset to avoid conflicts
                entity_type: "ENTITY".to_string(),
                name: keyword.to_string(),
                description: Some(format!("Entity extracted from document: {}", document.id)),
                embedding: document.embedding.clone(),
                properties: HashMap::new(),
                confidence_score: 0.7, // Default confidence
            };
            entities.push(entity);
        }
        
        Ok(entities)
    }

    /// Extract relationships between entities (simplified implementation)
    fn extract_relationships(&self, entities: &[KnowledgeNode], document: &Document) -> Result<Vec<KnowledgeEdge>, OxidbError> {
        let mut relationships = Vec::new();
        
        // Simple co-occurrence based relationship extraction
        for (i, entity1) in entities.iter().enumerate() {
            for (j, entity2) in entities.iter().enumerate() {
                if i >= j { continue; } // Avoid duplicates and self-relationships
                
                // Check if entities co-occur in the document
                if document.content.contains(&entity1.name) && document.content.contains(&entity2.name) {
                    let relationship = KnowledgeEdge {
                        id: (i * 1000 + j) as EdgeId,
                        from_entity: entity1.id,
                        to_entity: entity2.id,
                        relationship_type: "MENTIONED_WITH".to_string(),
                        description: Some(format!("Co-occurrence in document: {}", document.id)),
                        confidence_score: 0.6,
                        weight: Some(0.5),
                    };
                    relationships.push(relationship);
                }
            }
        }
        
        Ok(relationships)
    }

    /// Calculate entity similarity using embeddings
    #[allow(dead_code)]
    fn calculate_entity_similarity(&self, entity1_id: NodeId, entity2_id: NodeId) -> Result<f64, OxidbError> {
        if let (Some(emb1), Some(emb2)) = (self.entity_embeddings.get(&entity1_id), self.entity_embeddings.get(&entity2_id)) {
            use crate::core::vector::similarity::cosine_similarity;
            let similarity = cosine_similarity(emb1.as_slice(), emb2.as_slice())?;
            Ok(similarity as f64)
        } else {
            Ok(0.0)
        }
    }

    /// Find entities similar to query embedding
    fn find_similar_entities(&self, query_embedding: &Embedding, top_k: usize, min_similarity: f64) -> Result<Vec<(NodeId, f64)>, OxidbError> {
        let mut similarities = Vec::new();
        
        for (&entity_id, entity_embedding) in &self.entity_embeddings {
            use crate::core::vector::similarity::cosine_similarity;
            let similarity = cosine_similarity(query_embedding.as_slice(), entity_embedding.as_slice())?;
            let similarity_f64 = similarity as f64;
            
            if similarity_f64 >= min_similarity {
                similarities.push((entity_id, similarity_f64));
            }
        }
        
        // Sort by similarity (descending)
        similarities.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        similarities.truncate(top_k);
        
        Ok(similarities)
    }

    /// Expand entity context using graph traversal
    fn expand_entity_context(&self, entity_ids: &[NodeId], max_hops: usize) -> Result<Vec<NodeId>, OxidbError> {
        let mut expanded_entities = HashSet::new();
        let mut current_level = entity_ids.to_vec();
        
        expanded_entities.extend(current_level.iter());
        
        for _hop in 0..max_hops {
            let mut next_level = Vec::new();
            
            for &entity_id in &current_level {
                let neighbors = self.graph_store.get_neighbors(entity_id, TraversalDirection::Both)?;
                for neighbor in neighbors {
                    if !expanded_entities.contains(&neighbor) {
                        expanded_entities.insert(neighbor);
                        next_level.push(neighbor);
                    }
                }
            }
            
            if next_level.is_empty() {
                break; // No more entities to expand
            }
            
            current_level = next_level;
        }
        
        Ok(expanded_entities.into_iter().collect())
    }

    /// Calculate reasoning score for a path
    fn calculate_reasoning_score(&self, path: &[NodeId]) -> Result<f64, OxidbError> {
        if path.len() < 2 {
            return Ok(0.0);
        }
        
        let mut total_score = 0.0;
        let mut edge_count = 0;
        
        for i in 0..(path.len() - 1) {
            let from_node = path[i];
            let to_node = path[i + 1];
            
            // Find edges between consecutive nodes
            let neighbors = self.graph_store.get_neighbors(from_node, TraversalDirection::Both)?;
            if neighbors.contains(&to_node) {
                // For simplicity, use a base score. In practice, you'd consider
                // relationship type, confidence, and other factors
                total_score += 1.0;
                edge_count += 1;
            }
        }
        
        if edge_count > 0 {
            Ok(total_score / edge_count as f64)
        } else {
            Ok(0.0)
        }
    }
}

#[async_trait]
impl GraphRAGEngine for GraphRAGEngineImpl {
    async fn build_knowledge_graph(&mut self, documents: &[Document]) -> Result<(), OxidbError> {
        for document in documents {
            // Extract entities from document
            let entities = self.extract_entities(document)?;
            
            // Add entities to graph
            for entity in &entities {
                let graph_data = GraphData::new(entity.entity_type.clone())
                    .with_property("name".to_string(), DataType::String(entity.name.clone()))
                    .with_property("confidence".to_string(), DataType::Float(entity.confidence_score));
                
                let node_id = self.graph_store.add_node(graph_data)?;
                
                // Store embedding if available
                if let Some(embedding) = &entity.embedding {
                    self.entity_embeddings.insert(node_id, embedding.clone());
                }
            }
            
            // Extract and add relationships
            let relationships = self.extract_relationships(&entities, document)?;
            for relationship in relationships {
                if let (Ok(Some(_)), Ok(Some(_))) = (
                    self.graph_store.get_node(relationship.from_entity),
                    self.graph_store.get_node(relationship.to_entity)
                ) {
                    let rel = Relationship::new(relationship.relationship_type.clone());
                    let edge_data = GraphData::new("relationship".to_string())
                        .with_property("confidence".to_string(), DataType::Float(relationship.confidence_score));
                    
                    self.graph_store.add_edge(
                        relationship.from_entity,
                        relationship.to_entity,
                        rel,
                        Some(edge_data)
                    )?;
                }
            }
        }
        
        Ok(())
    }

    async fn retrieve_with_graph(&self, context: GraphRAGContext) -> Result<GraphRAGResult, OxidbError> {
        // Step 1: Find entities similar to query
        let similar_entities = self.find_similar_entities(&context.query_embedding, 10, context.min_confidence)?;
        let entity_ids: Vec<NodeId> = similar_entities.iter().map(|(id, _)| *id).collect();
        
        // Step 2: Expand context using graph traversal
        let expanded_entities = self.expand_entity_context(&entity_ids, context.max_hops)?;
        
        // Step 3: Retrieve relevant documents using traditional RAG
        let documents = self.document_retriever.retrieve(
            &context.query_embedding,
            10,
            crate::core::rag::retriever::SimilarityMetric::Cosine
        ).await?;
        
        // Step 4: Get relevant entities and relationships
        let mut relevant_entities = Vec::new();
        let entity_relationships = Vec::new();
        
        for &entity_id in &expanded_entities {
            if let Ok(Some(node)) = self.graph_store.get_node(entity_id) {
                let knowledge_node = KnowledgeNode {
                    id: entity_id,
                    entity_type: node.data.label.clone(),
                    name: node.data.get_property("name")
                        .and_then(|v| if let DataType::String(s) = v { Some(s.clone()) } else { None })
                        .unwrap_or_else(|| format!("Entity_{}", entity_id)),
                    description: None,
                    embedding: self.entity_embeddings.get(&entity_id).cloned(),
                    properties: node.data.properties.clone(),
                    confidence_score: node.data.get_property("confidence")
                        .and_then(|v| if let DataType::Float(f) = v { Some(*f) } else { None })
                        .unwrap_or(0.5),
                };
                relevant_entities.push(knowledge_node);
            }
        }
        
        // Step 5: Generate reasoning paths
        let mut reasoning_paths = Vec::new();
        if entity_ids.len() >= 2 {
            for i in 0..entity_ids.len().min(3) {
                for j in (i + 1)..entity_ids.len().min(3) {
                    if let Ok(Some(path)) = self.graph_store.find_shortest_path(entity_ids[i], entity_ids[j]) {
                        let reasoning_score = self.calculate_reasoning_score(&path)?;
                        let reasoning_path = ReasoningPath {
                            path_nodes: path.clone(),
                            path_relationships: vec!["CONNECTED".to_string(); path.len().saturating_sub(1)],
                            reasoning_score,
                            explanation: format!("Path from entity {} to entity {} with {} hops", 
                                entity_ids[i], entity_ids[j], path.len() - 1),
                        };
                        reasoning_paths.push(reasoning_path);
                    }
                }
            }
        }
        
        // Step 6: Calculate overall confidence score
        let confidence_score = if !similar_entities.is_empty() {
            similar_entities.iter().map(|(_, score)| score).sum::<f64>() / similar_entities.len() as f64
        } else {
            0.0
        };
        
        Ok(GraphRAGResult {
            documents,
            reasoning_paths,
            relevant_entities,
            entity_relationships,
            confidence_score,
        })
    }

    async fn add_entity(&mut self, entity: KnowledgeNode) -> Result<NodeId, OxidbError> {
        let graph_data = GraphData::new(entity.entity_type.clone())
            .with_property("name".to_string(), DataType::String(entity.name.clone()))
            .with_property("confidence".to_string(), DataType::Float(entity.confidence_score))
            .with_properties(entity.properties);
        
        let node_id = self.graph_store.add_node(graph_data)?;
        
        if let Some(embedding) = entity.embedding {
            self.entity_embeddings.insert(node_id, embedding);
        }
        
        Ok(node_id)
    }

    async fn add_relationship(&mut self, relationship: KnowledgeEdge) -> Result<EdgeId, OxidbError> {
        let rel = Relationship::new(relationship.relationship_type.clone());
        let edge_data = GraphData::new("relationship".to_string())
            .with_property("confidence".to_string(), DataType::Float(relationship.confidence_score));
        
        self.graph_store.add_edge(
            relationship.from_entity,
            relationship.to_entity,
            rel,
            Some(edge_data)
        )
    }

    async fn find_related_entities(&self, entity_id: NodeId, max_hops: usize) -> Result<Vec<KnowledgeNode>, OxidbError> {
        let related_ids = self.expand_entity_context(&[entity_id], max_hops)?;
        let mut related_entities = Vec::new();
        
        for &id in &related_ids {
            if id != entity_id { // Exclude the original entity
                if let Ok(Some(node)) = self.graph_store.get_node(id) {
                    let knowledge_node = KnowledgeNode {
                        id,
                        entity_type: node.data.label.clone(),
                        name: node.data.get_property("name")
                            .and_then(|v| if let DataType::String(s) = v { Some(s.clone()) } else { None })
                            .unwrap_or_else(|| format!("Entity_{}", id)),
                        description: None,
                        embedding: self.entity_embeddings.get(&id).cloned(),
                        properties: node.data.properties.clone(),
                        confidence_score: node.data.get_property("confidence")
                            .and_then(|v| if let DataType::Float(f) = v { Some(*f) } else { None })
                            .unwrap_or(0.5),
                    };
                    related_entities.push(knowledge_node);
                }
            }
        }
        
        Ok(related_entities)
    }

    async fn get_reasoning_paths(&self, from: NodeId, to: NodeId, max_paths: usize) -> Result<Vec<ReasoningPath>, OxidbError> {
        let mut reasoning_paths = Vec::new();
        
        // For simplicity, we'll just find the shortest path
        // In practice, you might want to find multiple paths using different algorithms
        if let Ok(Some(path)) = self.graph_store.find_shortest_path(from, to) {
            let reasoning_score = self.calculate_reasoning_score(&path)?;
            let reasoning_path = ReasoningPath {
                path_nodes: path.clone(),
                path_relationships: vec!["CONNECTED".to_string(); path.len().saturating_sub(1)],
                reasoning_score,
                explanation: format!("Shortest path from {} to {} with {} hops", from, to, path.len() - 1),
            };
            reasoning_paths.push(reasoning_path);
        }
        
        reasoning_paths.truncate(max_paths);
        Ok(reasoning_paths)
    }
}

/// Factory for creating GraphRAG engines
pub struct GraphRAGFactory;

impl GraphRAGFactory {
    /// Create a new GraphRAG engine with default settings
    pub fn create_engine(document_retriever: Box<dyn Retriever>) -> Box<dyn GraphRAGEngine> {
        Box::new(GraphRAGEngineImpl::new(document_retriever))
    }
    
    /// Create a GraphRAG engine with custom configuration
    pub fn create_engine_with_config(
        document_retriever: Box<dyn Retriever>,
        confidence_threshold: f64,
        relationship_weights: HashMap<String, f64>,
    ) -> Box<dyn GraphRAGEngine> {
        let mut engine = GraphRAGEngineImpl::new(document_retriever);
        engine.set_confidence_threshold(confidence_threshold);
        engine.set_relationship_weights(relationship_weights);
        Box::new(engine)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::rag::retriever::InMemoryRetriever;

    #[tokio::test]
    async fn test_graphrag_entity_creation() {
        let retriever = Box::new(InMemoryRetriever::new(vec![]));
        let mut engine = GraphRAGEngineImpl::new(retriever);
        
        let entity = KnowledgeNode {
            id: 0, // Will be assigned by the engine
            entity_type: "PERSON".to_string(),
            name: "Alice".to_string(),
            description: Some("A test person".to_string()),
            embedding: Some(vec![0.1, 0.2, 0.3].into()),
            properties: HashMap::new(),
            confidence_score: 0.9,
        };
        
        let node_id = engine.add_entity(entity).await.unwrap();
        assert!(node_id > 0);
        
        // Verify entity was added
        let related = engine.find_related_entities(node_id, 1).await.unwrap();
        assert!(related.is_empty()); // No relationships yet
    }

    #[tokio::test]
    async fn test_graphrag_relationship_creation() {
        let retriever = Box::new(InMemoryRetriever::new(vec![]));
        let mut engine = GraphRAGEngineImpl::new(retriever);
        
        // Add two entities
        let entity1 = KnowledgeNode {
            id: 0,
            entity_type: "PERSON".to_string(),
            name: "Alice".to_string(),
            description: None,
            embedding: Some(vec![0.1, 0.2, 0.3].into()),
            properties: HashMap::new(),
            confidence_score: 0.9,
        };
        
        let entity2 = KnowledgeNode {
            id: 0,
            entity_type: "PERSON".to_string(),
            name: "Bob".to_string(),
            description: None,
            embedding: Some(vec![0.2, 0.3, 0.4].into()),
            properties: HashMap::new(),
            confidence_score: 0.8,
        };
        
        let node1_id = engine.add_entity(entity1).await.unwrap();
        let node2_id = engine.add_entity(entity2).await.unwrap();
        
        // Add relationship
        let relationship = KnowledgeEdge {
            id: 0,
            from_entity: node1_id,
            to_entity: node2_id,
            relationship_type: "KNOWS".to_string(),
            description: Some("Alice knows Bob".to_string()),
            confidence_score: 0.7,
            weight: Some(1.0),
        };
        
        let edge_id = engine.add_relationship(relationship).await.unwrap();
        assert!(edge_id > 0);
        
        // Verify relationship
        let related = engine.find_related_entities(node1_id, 1).await.unwrap();
        assert_eq!(related.len(), 1);
        assert_eq!(related[0].name, "Bob");
    }

    #[tokio::test]
    async fn test_knowledge_graph_building() {
        let retriever = Box::new(InMemoryRetriever::new(vec![]));
        let mut engine = GraphRAGEngineImpl::new(retriever);
        
        let documents = vec![
            Document {
                id: "doc1".to_string(),
                content: "Alice works at Company and knows Bob".to_string(),
                embedding: Some(vec![0.1, 0.2, 0.3].into()),
                metadata: Some(HashMap::new()),
            },
            Document {
                id: "doc2".to_string(),
                content: "Bob lives in City and works at Company".to_string(),
                embedding: Some(vec![0.2, 0.3, 0.4].into()),
                metadata: Some(HashMap::new()),
            },
        ];
        
        engine.build_knowledge_graph(&documents).await.unwrap();
        
        // The graph should now contain entities extracted from the documents
        // This is a basic test - in practice you'd verify specific entities and relationships
    }
}