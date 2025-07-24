// src/core/rag/embedder.rs

use super::core_components::{Document, Embedding};
use crate::core::common::OxidbError;
use async_trait::async_trait;
use std::collections::HashMap;

/// Trait for models that can generate embeddings for documents.
#[async_trait]
pub trait EmbeddingModel: Send + Sync {
    /// Generates an embedding for a single document.
    async fn embed_document(&self, document: &Document) -> Result<Embedding, OxidbError>;

    /// Generates embeddings for a batch of documents.
    /// Default implementation calls `embed_document` for each document.
    /// Implementers can override this for batch-optimized embedding generation.
    async fn embed_documents(&self, documents: &[Document]) -> Result<Vec<Embedding>, OxidbError> {
        let mut embeddings = Vec::with_capacity(documents.len());
        for doc in documents {
            embeddings.push(self.embed_document(doc).await?);
        }
        Ok(embeddings)
    }
}

/// A TF-IDF based embedding model for more meaningful document representations
pub struct TfIdfEmbedder {
    vocabulary: HashMap<String, usize>,
    idf_scores: HashMap<String, f32>,
    dimension: usize,
}

impl TfIdfEmbedder {
    /// Create a new TF-IDF embedder with the given vocabulary
    pub fn new(documents: &[Document]) -> Self {
        let mut vocabulary = HashMap::new();
        let mut document_frequencies = HashMap::new();
        let total_documents = documents.len() as f32;
        
        // Build vocabulary and document frequencies
        for doc in documents {
            let words: std::collections::HashSet<String> = doc.content
                .to_lowercase()
                .split_whitespace()
                .filter(|word| word.len() > 2) // Filter short words
                .map(|word| word.chars().filter(|c| c.is_alphabetic()).collect())
                .filter(|word: &String| !word.is_empty())
                .collect();
            
            for word in words {
                *document_frequencies.entry(word.clone()).or_insert(0) += 1;
                if !vocabulary.contains_key(&word) {
                    vocabulary.insert(word, vocabulary.len());
                }
            }
        }
        
        // Calculate IDF scores
        let mut idf_scores = HashMap::new();
        for (word, df) in document_frequencies {
            let idf = (total_documents / df as f32).ln();
            idf_scores.insert(word, idf);
        }
        
        let dimension = vocabulary.len().min(512); // Limit dimension for performance
        
        Self {
            vocabulary,
            idf_scores,
            dimension,
        }
    }
    
    /// Calculate term frequency for a document
    fn calculate_tf(&self, document: &Document) -> HashMap<String, f32> {
        let mut tf_map = HashMap::new();
        let words: Vec<String> = document.content
            .to_lowercase()
            .split_whitespace()
            .filter(|word| word.len() > 2)
            .map(|word| word.chars().filter(|c| c.is_alphabetic()).collect())
            .filter(|word: &String| !word.is_empty())
            .collect();
        
        let total_words = words.len() as f32;
        
        for word in words {
            *tf_map.entry(word).or_insert(0.0) += 1.0;
        }
        
        // Normalize by document length
        for tf in tf_map.values_mut() {
            *tf /= total_words;
        }
        
        tf_map
    }
}

#[async_trait]
impl EmbeddingModel for TfIdfEmbedder {
    async fn embed_document(&self, document: &Document) -> Result<Embedding, OxidbError> {
        let tf_map = self.calculate_tf(document);
        let mut embedding = vec![0.0; self.dimension];
        
        for (word, tf) in tf_map {
            if let (Some(&vocab_idx), Some(&idf)) = (self.vocabulary.get(&word), self.idf_scores.get(&word)) {
                if vocab_idx < self.dimension {
                    embedding[vocab_idx] = tf * idf;
                }
            }
        }
        
        // Normalize the embedding vector
        let magnitude: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        if magnitude > 0.0 {
            for val in &mut embedding {
                *val /= magnitude;
            }
        }
        
        Ok(Embedding::from(embedding))
    }
}

/// An improved semantic embedding model for Shakespeare texts
pub struct ShakespeareEmbedder {
    dimension: usize,
    character_weights: HashMap<String, f32>,
    theme_weights: HashMap<String, f32>,
    emotion_weights: HashMap<String, f32>,
}

impl ShakespeareEmbedder {
    /// Create a new Shakespeare-specific embedder
    pub fn new(dimension: usize) -> Self {
        let mut character_weights = HashMap::new();
        character_weights.insert("romeo".to_string(), 1.0);
        character_weights.insert("juliet".to_string(), 1.0);
        character_weights.insert("hamlet".to_string(), 0.9);
        character_weights.insert("macbeth".to_string(), 0.9);
        character_weights.insert("lady".to_string(), 0.8);
        character_weights.insert("king".to_string(), 0.8);
        character_weights.insert("queen".to_string(), 0.8);
        character_weights.insert("prince".to_string(), 0.7);
        character_weights.insert("duke".to_string(), 0.7);
        
        let mut theme_weights = HashMap::new();
        theme_weights.insert("love".to_string(), 1.0);
        theme_weights.insert("death".to_string(), 0.9);
        theme_weights.insert("revenge".to_string(), 0.9);
        theme_weights.insert("betrayal".to_string(), 0.8);
        theme_weights.insert("power".to_string(), 0.8);
        theme_weights.insert("ambition".to_string(), 0.8);
        theme_weights.insert("family".to_string(), 0.7);
        theme_weights.insert("honor".to_string(), 0.7);
        theme_weights.insert("fate".to_string(), 0.7);
        theme_weights.insert("supernatural".to_string(), 0.6);
        
        let mut emotion_weights = HashMap::new();
        emotion_weights.insert("joy".to_string(), 0.8);
        emotion_weights.insert("sorrow".to_string(), 0.8);
        emotion_weights.insert("anger".to_string(), 0.7);
        emotion_weights.insert("fear".to_string(), 0.7);
        emotion_weights.insert("hope".to_string(), 0.6);
        emotion_weights.insert("despair".to_string(), 0.6);
        
        Self {
            dimension,
            character_weights,
            theme_weights,
            emotion_weights,
        }
    }
    
    /// Extract semantic features from Shakespeare text
    fn extract_features(&self, text: &str) -> Vec<f32> {
        let text_lower = text.to_lowercase();
        let mut features = vec![0.0; self.dimension];
        
        // Character presence (first third of dimensions)
        let char_dim = self.dimension / 3;
        let mut char_idx = 0;
        for (character, weight) in &self.character_weights {
            if char_idx >= char_dim { break; }
            if text_lower.contains(character) {
                features[char_idx] = *weight;
            }
            char_idx += 1;
        }
        
        // Theme presence (second third of dimensions)
        let theme_start = char_dim;
        let theme_dim = self.dimension / 3;
        let mut theme_idx = 0;
        for (theme, weight) in &self.theme_weights {
            if theme_idx >= theme_dim { break; }
            if text_lower.contains(theme) {
                features[theme_start + theme_idx] = *weight;
            }
            theme_idx += 1;
        }
        
        // Emotion presence (final third of dimensions)
        let emotion_start = theme_start + theme_dim;
        let emotion_dim = self.dimension - emotion_start;
        let mut emotion_idx = 0;
        for (emotion, weight) in &self.emotion_weights {
            if emotion_idx >= emotion_dim { break; }
            if text_lower.contains(emotion) {
                features[emotion_start + emotion_idx] = *weight;
            }
            emotion_idx += 1;
        }
        
        // Add some content-based features
        let word_count = text.split_whitespace().count() as f32;
        let normalized_length = (word_count / 1000.0).min(1.0); // Normalize to 0-1
        
        // Add length and complexity features to remaining dimensions
        if let Some(last_idx) = features.len().checked_sub(1) {
            features[last_idx] = normalized_length;
        }
        
        features
    }
}

#[async_trait]
impl EmbeddingModel for ShakespeareEmbedder {
    async fn embed_document(&self, document: &Document) -> Result<Embedding, OxidbError> {
        let features = self.extract_features(&document.content);
        
        // Normalize the feature vector
        let magnitude: f32 = features.iter().map(|x| x * x).sum::<f32>().sqrt();
        let normalized_features = if magnitude > 0.0 {
            features.iter().map(|x| x / magnitude).collect()
        } else {
            features
        };
        
        Ok(Embedding::from(normalized_features))
    }
}

/// A simple mock embedding model for testing purposes.
#[cfg(test)]
pub(crate) struct MockEmbeddingModel {
    pub(crate) dimension: usize,
    pub(crate) fixed_embedding_value: Option<f32>, // If set, all dimensions will have this value
}

#[cfg(test)]
#[async_trait]
#[cfg(test)]
impl EmbeddingModel for MockEmbeddingModel {
    async fn embed_document(&self, document: &Document) -> Result<Embedding, OxidbError> {
        let value_to_fill = self.fixed_embedding_value.unwrap_or_else(|| {
            // Create a pseudo-random value based on document content length for some variation
            (document.content.len() % 100) as f32 / 100.0
        });
        let vec = vec![value_to_fill; self.dimension];
        Ok(Embedding::from(vec))
    }

    async fn embed_documents(&self, documents: &[Document]) -> Result<Vec<Embedding>, OxidbError> {
        let mut embeddings = Vec::new();
        for doc in documents {
            let embedding = self.embed_document(doc).await?;
            embeddings.push(embedding);
        }
        Ok(embeddings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::rag::core_components::Document;

    #[tokio::test]
    async fn test_shakespeare_embedder() {
        let embedder = ShakespeareEmbedder::new(30);
        let doc = Document::new(
            "test".to_string(),
            "Romeo loves Juliet with great passion and joy".to_string()
        );
        
        let embedding = embedder.embed_document(&doc).await.unwrap();
        assert_eq!(embedding.vector.len(), 30);
        
        // Should have non-zero values for character and theme features
        let has_character_features = embedding.vector[..10].iter().any(|&x| x > 0.0);
        let has_theme_features = embedding.vector[10..20].iter().any(|&x| x > 0.0);
        
        assert!(has_character_features, "Should detect character features");
        assert!(has_theme_features, "Should detect theme features");
    }
    
    #[tokio::test]
    async fn test_tfidf_embedder() {
        let docs = vec![
            Document::new("1".to_string(), "Romeo loves Juliet".to_string()),
            Document::new("2".to_string(), "Hamlet seeks revenge".to_string()),
            Document::new("3".to_string(), "Macbeth desires power".to_string()),
        ];
        
        let embedder = TfIdfEmbedder::new(&docs);
        let embedding = embedder.embed_document(&docs[0]).await.unwrap();
        
        assert!(!embedding.vector.is_empty());
        
        // Check that the embedding is normalized
        let magnitude: f32 = embedding.vector.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((magnitude - 1.0).abs() < 0.01, "Embedding should be normalized");
    }

    #[tokio::test]
    async fn test_mock_embedding_model_single_document() {
        let model = MockEmbeddingModel { dimension: 3, fixed_embedding_value: Some(0.5) };
        let doc = Document::new("id1".to_string(), "Test content".to_string());
        let embedding = model.embed_document(&doc).await.unwrap();
        assert_eq!(embedding.vector, vec![0.5, 0.5, 0.5]);
        assert_eq!(embedding.vector.len(), 3);
    }

    #[tokio::test]
    async fn test_mock_embedding_model_batch_documents() {
        let model = MockEmbeddingModel { dimension: 2, fixed_embedding_value: None };
        let docs = vec![
            Document::new("id1".to_string(), "Short".to_string()), // len 5 -> 0.05
            Document::new("id2".to_string(), "Longer content".to_string()), // len 14 -> 0.14
        ];
        let embeddings = model.embed_documents(&docs).await.unwrap();
        assert_eq!(embeddings.len(), 2);
        assert_eq!(embeddings[0].vector, vec![0.05, 0.05]);
        assert_eq!(embeddings[1].vector, vec![0.14, 0.14]);
    }

    #[tokio::test]
    async fn test_mock_embedding_model_default_batch_via_single() {
        struct TestModel {
            dimension: usize,
        }
        #[async_trait]
        impl EmbeddingModel for TestModel {
            async fn embed_document(&self, document: &Document) -> Result<Embedding, OxidbError> {
                Ok(Embedding::from(vec![
                    (document.id.chars().last().unwrap_or('0').to_digit(10).unwrap_or(0) % 10)
                        as f32;
                    self.dimension
                ]))
            }
        }

        let model = TestModel { dimension: 1 };
        let docs = vec![
            Document::new("doc1".to_string(), "".to_string()),
            Document::new("doc2".to_string(), "".to_string()),
        ];
        // This test relies on the default implementation of embed_documents
        let embeddings = model.embed_documents(&docs).await.unwrap();
        assert_eq!(embeddings.len(), 2);
        assert_eq!(embeddings[0].vector, vec![1.0]);
        assert_eq!(embeddings[1].vector, vec![2.0]);
    }
}
