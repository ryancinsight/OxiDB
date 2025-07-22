// src/core/vector/similarity.rs

use crate::core::common::OxidbError;

/// Enumeration of supported similarity metrics
/// Following the Open/Closed Principle - open for extension, closed for modification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimilarityMetric {
    /// Cosine similarity (angle between vectors)
    Cosine,
    /// Dot product similarity
    DotProduct,
    /// Euclidean distance-based similarity
    Euclidean,
}

impl SimilarityMetric {
    /// Calculate similarity between two vectors using this metric
    pub fn calculate(&self, v1: &[f32], v2: &[f32]) -> Result<f32, OxidbError> {
        match self {
            Self::Cosine => cosine_similarity(v1, v2),
            Self::DotProduct => dot_product(v1, v2),
            Self::Euclidean => {
                // Convert Euclidean distance to similarity
                let distance = euclidean_distance(v1, v2)?;
                Ok(1.0 / (1.0 + distance)) // Normalize to [0,1] range
            }
        }
    }

    /// Get the name of the similarity metric
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Cosine => "cosine",
            Self::DotProduct => "dot_product",
            Self::Euclidean => "euclidean",
        }
    }
}

/// Calculates the dot product of two vectors.
///
/// # Arguments
///
/// * `v1` - A slice of f32 representing the first vector.
/// * `v2` - A slice of f32 representing the second vector.
///
/// # Returns
///
/// * `Result<f32, OxidbError>` - The dot product of the two vectors, or an error if
///   the vectors have different dimensions.
pub fn dot_product(v1: &[f32], v2: &[f32]) -> Result<f32, OxidbError> {
    if v1.len() != v2.len() {
        return Err(OxidbError::VectorDimensionMismatch { dim1: v1.len(), dim2: v2.len() });
    }

    Ok(v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum())
}

/// Calculates the cosine similarity of two vectors.
///
/// # Arguments
///
/// * `v1` - A slice of f32 representing the first vector.
/// * `v2` - A slice of f32 representing the second vector.
///
/// # Returns
///
/// * `Result<f32, OxidbError>` - The cosine similarity of the two vectors, or an error if
///   the vectors have different dimensions or if either vector has a magnitude of zero.
pub fn cosine_similarity(v1: &[f32], v2: &[f32]) -> Result<f32, OxidbError> {
    if v1.len() != v2.len() {
        return Err(OxidbError::VectorDimensionMismatch { dim1: v1.len(), dim2: v2.len() });
    }

    let dot_prod = dot_product(v1, v2)?;
    let magnitude_v1 = v1.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
    let magnitude_v2 = v2.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();

    if magnitude_v1 == 0.0 || magnitude_v2 == 0.0 {
        return Err(OxidbError::VectorMagnitudeZero);
    }

    Ok(dot_prod / (magnitude_v1 * magnitude_v2))
}

/// Calculates the Euclidean distance between two vectors.
///
/// # Arguments
///
/// * `v1` - A slice of f32 representing the first vector.
/// * `v2` - A slice of f32 representing the second vector.
///
/// # Returns
///
/// * `Result<f32, OxidbError>` - The Euclidean distance between the two vectors, or an error if
///   the vectors have different dimensions.
pub fn euclidean_distance(v1: &[f32], v2: &[f32]) -> Result<f32, OxidbError> {
    if v1.len() != v2.len() {
        return Err(OxidbError::VectorDimensionMismatch { dim1: v1.len(), dim2: v2.len() });
    }

    let sum_sq_diff: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| (a - b).powi(2)).sum();

    Ok(sum_sq_diff.sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq; // For floating point comparisons

    #[test]
    fn test_dot_product_success() {
        let v1 = [1.0, 2.0, 3.0];
        let v2 = [4.0, 5.0, 6.0];
        assert_relative_eq!(dot_product(&v1, &v2).unwrap(), 32.0, epsilon = 1e-6);
    }

    #[test]
    fn test_dot_product_empty_vectors() {
        let v1: [f32; 0] = [];
        let v2: [f32; 0] = [];
        assert_relative_eq!(dot_product(&v1, &v2).unwrap(), 0.0, epsilon = 1e-6);
    }

    #[test]
    fn test_dot_product_dimension_mismatch() {
        let v1 = [1.0, 2.0];
        let v2 = [4.0, 5.0, 6.0];
        match dot_product(&v1, &v2) {
            Err(OxidbError::VectorDimensionMismatch { dim1, dim2 }) => {
                assert_eq!(dim1, 2);
                assert_eq!(dim2, 3);
            }
            _ => panic!("Expected VectorDimensionMismatch"),
        }
    }

    #[test]
    fn test_cosine_similarity_success_orthogonal() {
        let v1 = [1.0, 0.0];
        let v2 = [0.0, 1.0];
        assert_relative_eq!(cosine_similarity(&v1, &v2).unwrap(), 0.0, epsilon = 1e-6);
    }

    #[test]
    fn test_cosine_similarity_success_collinear_same_direction() {
        let v1 = [1.0, 2.0, 3.0];
        let v2 = [2.0, 4.0, 6.0];
        assert_relative_eq!(cosine_similarity(&v1, &v2).unwrap(), 1.0, epsilon = 1e-6);
    }

    #[test]
    fn test_cosine_similarity_success_collinear_opposite_direction() {
        let v1 = [1.0, 2.0, 3.0];
        let v2 = [-1.0, -2.0, -3.0];
        assert_relative_eq!(cosine_similarity(&v1, &v2).unwrap(), -1.0, epsilon = 1e-6);
    }

    #[test]
    fn test_cosine_similarity_general_case() {
        let v1 = [1.0, 2.0];
        let v2 = [3.0, 4.0];
        // Dot product = 1*3 + 2*4 = 3 + 8 = 11
        // Magnitude v1 = sqrt(1^2 + 2^2) = sqrt(1 + 4) = sqrt(5)
        // Magnitude v2 = sqrt(3^2 + 4^2) = sqrt(9 + 16) = sqrt(25) = 5
        // Cosine similarity = 11 / (sqrt(5) * 5) = 11 / (2.236067977 * 5) = 11 / 11.180339887 = 0.98386991
        assert_relative_eq!(cosine_similarity(&v1, &v2).unwrap(), 0.98386991, epsilon = 1e-6);
    }

    #[test]
    fn test_cosine_similarity_dimension_mismatch() {
        let v1 = [1.0, 2.0];
        let v2 = [4.0, 5.0, 6.0];
        match cosine_similarity(&v1, &v2) {
            Err(OxidbError::VectorDimensionMismatch { dim1, dim2 }) => {
                assert_eq!(dim1, 2);
                assert_eq!(dim2, 3);
            }
            _ => panic!("Expected VectorDimensionMismatch"),
        }
    }

    #[test]
    fn test_cosine_similarity_zero_magnitude_v1() {
        let v1 = [0.0, 0.0];
        let v2 = [1.0, 2.0];
        match cosine_similarity(&v1, &v2) {
            Err(OxidbError::VectorMagnitudeZero) => {}
            _ => panic!("Expected VectorMagnitudeZero"),
        }
    }

    #[test]
    fn test_cosine_similarity_zero_magnitude_v2() {
        let v1 = [1.0, 2.0];
        let v2 = [0.0, 0.0];
        match cosine_similarity(&v1, &v2) {
            Err(OxidbError::VectorMagnitudeZero) => {}
            _ => panic!("Expected VectorMagnitudeZero"),
        }
    }

    #[test]
    fn test_cosine_similarity_empty_vectors() {
        let v1: [f32; 0] = [];
        let v2: [f32; 0] = [];
        match cosine_similarity(&v1, &v2) {
            Err(OxidbError::VectorMagnitudeZero) => {} // Or handle as 1.0 or error, depending on definition for empty vectors
            _ => panic!("Expected VectorMagnitudeZero for empty vectors or specific handling"),
        }
    }
}
