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

    /// Generates an embedding for a text string.
    async fn embed(&self, text: &str) -> Result<Embedding, OxidbError>;

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

/// A TF-IDF based embedding model for meaningful document representations
pub struct TfIdfEmbedder {
    vocabulary: HashMap<String, usize>,
    idf_scores: HashMap<String, f32>,
    pub dimension: usize,
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
                .split(|c: char| !c.is_alphanumeric())
                .filter(|s| s.len() > 2)
                .map(String::from)
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
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| s.len() > 2)
            .map(String::from)
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

    async fn embed(&self, text: &str) -> Result<Embedding, OxidbError> {
        // Create a temporary document to embed the text
        let doc = Document::new("temp".to_string(), text.to_string());
        self.embed_document(&doc).await
    }
}

/// A generic semantic embedding model that works with any document type
pub struct SemanticEmbedder {
    dimension: usize,
    feature_extractors: Vec<Box<dyn FeatureExtractor>>,
}

/// Trait for extracting features from documents
pub trait FeatureExtractor: Send + Sync {
    fn extract_features(&self, text: &str) -> Vec<f32>;
    fn feature_count(&self) -> usize;
}

/// Named entity extractor that identifies common entity types
pub struct NamedEntityExtractor {
    entity_patterns: HashMap<String, Vec<String>>,
}

impl NamedEntityExtractor {
    pub fn new() -> Self {
        let mut entity_patterns = HashMap::new();
        
        // Common person indicators
        entity_patterns.insert("PERSON".to_string(), vec![
            "mr".to_string(), "mrs".to_string(), "ms".to_string(), "dr".to_string(),
            "professor".to_string(), "captain".to_string(), "sir".to_string(),
        ]);
        
        // Common location indicators
        entity_patterns.insert("LOCATION".to_string(), vec![
            "city".to_string(), "town".to_string(), "country".to_string(), "street".to_string(),
            "avenue".to_string(), "road".to_string(), "building".to_string(), "house".to_string(),
        ]);
        
        // Common organization indicators
        entity_patterns.insert("ORGANIZATION".to_string(), vec![
            "company".to_string(), "corporation".to_string(), "university".to_string(),
            "school".to_string(), "hospital".to_string(), "government".to_string(),
        ]);
        
        Self { entity_patterns }
    }
}

impl Default for NamedEntityExtractor {
    fn default() -> Self {
        Self::new()
    }
}

impl FeatureExtractor for NamedEntityExtractor {
    fn extract_features(&self, text: &str) -> Vec<f32> {
        let text_lower = text.to_lowercase();
        let mut features = vec![0.0; self.feature_count()];
        
        let mut feature_idx = 0;
        for patterns in self.entity_patterns.values() {
            for pattern in patterns {
                if text_lower.contains(pattern) {
                    features[feature_idx] = 1.0;
                }
                feature_idx += 1;
            }
        }
        
        features
    }
    
    fn feature_count(&self) -> usize {
        self.entity_patterns.values().map(|v| v.len()).sum()
    }
}

/// Content-based feature extractor that analyzes document structure and content
pub struct ContentFeatureExtractor;

impl FeatureExtractor for ContentFeatureExtractor {
    fn extract_features(&self, text: &str) -> Vec<f32> {
        let mut features = Vec::new();
        
        // Document length features
        let word_count = text.split_whitespace().count() as f32;
        let char_count = text.len() as f32;
        let sentence_count = text.matches('.').count() as f32;
        
        // Normalize features
        features.push((word_count / 1000.0).min(1.0)); // Words per 1000
        features.push((char_count / 5000.0).min(1.0)); // Chars per 5000
        features.push((sentence_count / 50.0).min(1.0)); // Sentences per 50
        
        // Lexical diversity
        let unique_words: std::collections::HashSet<&str> = text.split_whitespace().collect();
        let lexical_diversity = if word_count > 0.0 {
            unique_words.len() as f32 / word_count
        } else {
            0.0
        };
        features.push(lexical_diversity);
        
        // Punctuation density
        let punctuation_count = text.chars().filter(|c| c.is_ascii_punctuation()).count() as f32;
        let punctuation_density = if char_count > 0.0 {
            punctuation_count / char_count
        } else {
            0.0
        };
        features.push(punctuation_density);
        
        // Average word length
        let avg_word_length = if word_count > 0.0 {
            text.split_whitespace().map(|w| w.len()).sum::<usize>() as f32 / word_count
        } else {
            0.0
        };
        features.push((avg_word_length / 10.0).min(1.0)); // Normalize by max length 10
        
        features
    }
    
    fn feature_count(&self) -> usize {
        6 // Number of features extracted
    }
}

impl SemanticEmbedder {
    /// Create a new semantic embedder with default feature extractors
    pub fn new(dimension: usize) -> Self {
        let feature_extractors: Vec<Box<dyn FeatureExtractor>> = vec![
            Box::new(NamedEntityExtractor::new()),
            Box::new(ContentFeatureExtractor),
        ];
        
        Self {
            dimension,
            feature_extractors,
        }
    }
    
    /// Create a semantic embedder with custom feature extractors
    pub fn with_extractors(dimension: usize, extractors: Vec<Box<dyn FeatureExtractor>>) -> Self {
        Self {
            dimension,
            feature_extractors: extractors,
        }
    }
    
    /// Extract all features from text
    fn extract_all_features(&self, text: &str) -> Vec<f32> {
        let mut all_features = Vec::new();
        
        for extractor in &self.feature_extractors {
            let mut features = extractor.extract_features(text);
            all_features.append(&mut features);
        }
        
        // Pad or truncate to desired dimension
        all_features.resize(self.dimension, 0.0);
        
        all_features
    }
}

#[async_trait]
impl EmbeddingModel for SemanticEmbedder {
    async fn embed_document(&self, document: &Document) -> Result<Embedding, OxidbError> {
        let features = self.extract_all_features(&document.content);
        
        // Normalize the feature vector
        let magnitude: f32 = features.iter().map(|x| x * x).sum::<f32>().sqrt();
        let normalized_features = if magnitude > 0.0 {
            features.iter().map(|x| x / magnitude).collect()
        } else {
            features
        };
        
        Ok(Embedding::from(normalized_features))
    }

    async fn embed(&self, text: &str) -> Result<Embedding, OxidbError> {
        let features = self.extract_all_features(text);
        
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

    async fn embed(&self, text: &str) -> Result<Embedding, OxidbError> {
        let value_to_fill = self.fixed_embedding_value.unwrap_or_else(|| {
            // Create a pseudo-random value based on text length for some variation
            (text.len() % 100) as f32 / 100.0
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
    async fn test_semantic_embedder() {
        let embedder = SemanticEmbedder::new(30);
        let doc = Document::new(
            "test".to_string(),
            "This is a sample document about technology and innovation. Dr. Smith works at the university.".to_string()
        );
        
        let embedding = embedder.embed_document(&doc).await.unwrap();
        assert_eq!(embedding.vector.len(), 30);
        
        // Should have some non-zero values
        let has_features = embedding.vector.iter().any(|&x| x > 0.0);
        assert!(has_features, "Should extract some features");
    }
    
    #[tokio::test]
    async fn test_named_entity_extractor() {
        let extractor = NamedEntityExtractor::new();
        let features = extractor.extract_features("Dr. Smith works at the university in the city.");
        
        assert!(!features.is_empty());
        assert!(features.iter().any(|&x| x > 0.0), "Should detect entities");
    }
    
    #[tokio::test]
    async fn test_content_feature_extractor() {
        let extractor = ContentFeatureExtractor;
        let features = extractor.extract_features("This is a test. It has multiple sentences. Each sentence ends with a period.");
        
        assert_eq!(features.len(), 6);
        assert!(features.iter().all(|&x| x >= 0.0 && x <= 1.0), "Features should be normalized");
    }
    
    #[tokio::test]
    async fn test_tfidf_embedder() {
        let docs = vec![
            Document::new("1".to_string(), "apple banana cherry".to_string()),
            Document::new("2".to_string(), "banana cherry date".to_string()),
            Document::new("3".to_string(), "cherry date elderberry".to_string()),
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
