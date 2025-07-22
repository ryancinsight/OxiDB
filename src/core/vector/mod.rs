// src/core/vector/mod.rs

//! Vector operations module for oxidb
//!
//! This module provides vector storage, similarity calculations, and search capabilities
//! following SOLID design principles:
//! - Single Responsibility: Each module handles one aspect of vector operations
//! - Open/Closed: Extensible for new similarity metrics without modifying existing code
//! - Liskov Substitution: Vector implementations can be substituted seamlessly
//! - Interface Segregation: Focused interfaces for different vector operations
//! - Dependency Inversion: Depends on abstractions, not concrete implementations

pub mod similarity;
// pub mod search;  // Temporarily disabled due to compilation issues
pub mod storage;
pub mod transaction; // Added ACID transaction support

// Re-export key types and functions for convenience
pub use similarity::{cosine_similarity, dot_product, SimilarityMetric};
// pub use search::{VectorSearchEngine, SearchResult};  // Temporarily disabled
pub use storage::{VectorEntry, VectorStore};
pub use transaction::{TransactionId, VectorTransactionManager};

use crate::core::common::OxidbError;
use crate::core::types::VectorData;

/// Trait for vector operations following the Interface Segregation Principle
pub trait VectorOperations {
    /// Calculate similarity between two vectors
    ///
    /// # Errors
    /// Returns `OxidbError::InvalidOperation` if vectors have different dimensions or contain invalid values
    fn similarity(&self, other: &VectorData, metric: SimilarityMetric) -> Result<f32, OxidbError>;

    /// Normalize a vector to unit length
    ///
    /// # Errors
    /// Returns `OxidbError::InvalidOperation` if the vector has zero magnitude
    fn normalize(&mut self) -> Result<(), OxidbError>;

    /// Check if vector is valid (no NaN or infinite values)
    fn is_valid(&self) -> bool;
}

/// Implementation of `VectorOperations` for `VectorData`
impl VectorOperations for VectorData {
    fn similarity(&self, other: &VectorData, metric: SimilarityMetric) -> Result<f32, OxidbError> {
        if self.dimension != other.dimension {
            return Err(OxidbError::VectorDimensionMismatch {
                dim1: self.dimension as usize,
                dim2: other.dimension as usize,
            });
        }

        match metric {
            SimilarityMetric::Cosine => cosine_similarity(&self.data, &other.data),
            SimilarityMetric::DotProduct => dot_product(&self.data, &other.data),
            SimilarityMetric::Euclidean => {
                // Convert distance to similarity (inverse relationship)
                let distance =
                    self.euclidean_distance(other).ok_or(OxidbError::VectorDimensionMismatch {
                        dim1: self.dimension as usize,
                        dim2: other.dimension as usize,
                    })?;
                Ok(1.0 / (1.0 + distance)) // Normalize to [0,1] range
            }
        }
    }

    fn normalize(&mut self) -> Result<(), OxidbError> {
        let magnitude: f32 = self.data.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();

        if magnitude == 0.0 {
            return Err(OxidbError::VectorMagnitudeZero);
        }

        for value in &mut self.data {
            *value /= magnitude;
        }

        Ok(())
    }

    fn is_valid(&self) -> bool {
        self.data.iter().all(|&x| x.is_finite())
    }
}

/// Factory for creating optimized vector operations following the Factory pattern
pub struct VectorFactory;

impl VectorFactory {
    /// Create a new vector with validation (YAGNI - only what's needed)
    ///
    /// # Errors
    /// Returns `OxidbError::VectorDimensionMismatch` if data length doesn't match dimension,
    /// or `OxidbError::InvalidInput` if vector contains NaN or infinite values
    pub fn create_vector(dimension: u32, data: Vec<f32>) -> Result<VectorData, OxidbError> {
        let data_len = data.len(); // Store length before moving data
        let vector =
            VectorData::new(dimension, data).ok_or(OxidbError::VectorDimensionMismatch {
                dim1: dimension as usize,
                dim2: data_len,
            })?;

        if !vector.is_valid() {
            return Err(OxidbError::InvalidInput {
                message: "Vector contains invalid values (NaN or infinite)".to_string(),
            });
        }

        Ok(vector)
    }

    /// Create a normalized vector
    ///
    /// # Errors
    /// Returns errors from `create_vector` or `normalize` operations
    pub fn create_normalized_vector(
        dimension: u32,
        data: Vec<f32>,
    ) -> Result<VectorData, OxidbError> {
        let mut vector = Self::create_vector(dimension, data)?;
        vector.normalize()?;
        Ok(vector)
    }

    /// Create a zero vector
    ///
    /// # Errors
    /// Returns `OxidbError::InvalidInput` if dimension is 0
    pub fn create_zero_vector(dimension: u32) -> Result<VectorData, OxidbError> {
        if dimension == 0 {
            return Err(OxidbError::InvalidInput {
                message: "Vector dimension must be greater than 0".to_string(),
            });
        }

        let data = vec![0.0; dimension as usize];
        Self::create_vector(dimension, data)
    }

    /// Create a random vector (for testing/initialization)
    ///
    /// # Errors
    /// Returns `OxidbError::InvalidInput` if dimension is 0
    pub fn create_random_vector(dimension: u32) -> Result<VectorData, OxidbError> {
        if dimension == 0 {
            return Err(OxidbError::InvalidInput {
                message: "Vector dimension must be greater than 0".to_string(),
            });
        }

        use rand::Rng;
        let mut rng = rand::thread_rng();
        let data: Vec<f32> = (0..dimension).map(|_| rng.gen_range(-1.0..1.0)).collect();

        Self::create_normalized_vector(dimension, data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    #[test]
    fn test_vector_operations_cosine_similarity() {
        let v1 = VectorFactory::create_vector(3, vec![1.0, 0.0, 0.0]).unwrap();
        let v2 = VectorFactory::create_vector(3, vec![0.0, 1.0, 0.0]).unwrap();

        let similarity = v1.similarity(&v2, SimilarityMetric::Cosine).unwrap();
        assert_relative_eq!(similarity, 0.0, epsilon = 1e-6);
    }

    #[test]
    fn test_vector_normalization() {
        let mut vector = VectorFactory::create_vector(3, vec![3.0, 4.0, 0.0]).unwrap();
        vector.normalize().unwrap();

        let magnitude: f32 = vector.data.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
        assert_relative_eq!(magnitude, 1.0, epsilon = 1e-6);
    }

    #[test]
    fn test_vector_validation() {
        let valid_vector = VectorFactory::create_vector(2, vec![1.0, 2.0]).unwrap();
        assert!(valid_vector.is_valid());

        let invalid_vector = VectorData::new(2, vec![f32::NAN, 2.0]).unwrap();
        assert!(!invalid_vector.is_valid());
    }

    #[test]
    fn test_factory_create_zero_vector() {
        let zero_vector = VectorFactory::create_zero_vector(5).unwrap();
        assert_eq!(zero_vector.dimension, 5);
        assert!(zero_vector.data.iter().all(|&x| x == 0.0));
    }

    #[test]
    fn test_factory_create_random_vector() {
        let random_vector = VectorFactory::create_random_vector(10).unwrap();
        assert_eq!(random_vector.dimension, 10);
        assert!(random_vector.is_valid());

        // Check that it's normalized
        let magnitude: f32 = random_vector.data.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
        assert_relative_eq!(magnitude, 1.0, epsilon = 1e-6);
    }

    #[test]
    fn test_dimension_mismatch_error() {
        let v1 = VectorFactory::create_vector(3, vec![1.0, 0.0, 0.0]).unwrap();
        let v2 = VectorFactory::create_vector(2, vec![0.0, 1.0]).unwrap();

        let result = v1.similarity(&v2, SimilarityMetric::Cosine);
        assert!(matches!(result, Err(OxidbError::VectorDimensionMismatch { .. })));
    }
}
