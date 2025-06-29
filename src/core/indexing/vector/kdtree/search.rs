// src/core/indexing/vector/kdtree/search.rs

//! Logic for performing K-Nearest Neighbor (KNN) search in a KD-Tree.

use std::collections::BinaryHeap;
use std::cmp::Ordering;

use crate::core::types::VectorData;
use super::tree::{KdNode, KdTree};
use super::error::KdTreeError;

/// Represents an item in the KNN search result priority queue.
/// Stores (squared_distance, point_slice_index).
/// Squared distance is used because BinaryHeap is a max-heap, and we want to efficiently
/// find/remove the neighbor with the largest distance.
/// `point_slice_index` is the index into the `points_data_slice` provided to `find_knn`.
#[derive(Debug, Clone, PartialEq)]
struct Neighbor {
    distance_sq: f32,
    point_slice_index: usize,
}

impl Eq for Neighbor {}

// For BinaryHeap (max-heap), Ordering::Less means higher priority.
// We want smaller distances to have higher priority for *keeping* them if the heap is full
// and a new point is closer than the current *largest* distance in heap.
// So, if `self.distance_sq < other.distance_sq`, `self` is "greater" in priority.
// Default `BinaryHeap` pops the largest element. We want to pop the one with largest distance_sq.
// So, `Ord` should be defined such that `a.cmp(b)` returns `Less` if `a.distance_sq > b.distance_sq`.
impl PartialOrd for Neighbor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.distance_sq.partial_cmp(&other.distance_sq)
    }
}

impl Ord for Neighbor {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}


/// Performs a K-Nearest Neighbor search in the KD-Tree.
///
/// # Arguments
/// * `tree`: A reference to the `KdTree`.
/// * `query_vector`: The vector to find neighbors for.
/// * `k`: The number of nearest neighbors to find.
/// * `points_data_slice`: A slice of `VectorData` references. This is the same data
///   (or a compatible view) that was used to build the tree. Indices stored in
///   leaf nodes are indices into this slice.
///
/// # Returns
/// A `Result` containing a `Vec` of `(point_slice_index, distance)` tuples for the K nearest
/// neighbors, sorted by distance. `point_slice_index` is the index into `points_data_slice`.
/// The caller is responsible for mapping this index back to any original ID.
pub fn find_knn<'a>(
    tree: &'a KdTree,
    query_vector: &VectorData,
    k: usize,
    points_data_slice: &'a [&'a VectorData],
) -> Result<Vec<(usize, f32)>, KdTreeError> {
    if k == 0 {
        return Ok(Vec::new());
    }
    if tree.root.is_none() {
        return Ok(Vec::new()); // Empty tree
    }
    if query_vector.dimension != tree.dimension {
        return Err(KdTreeError::DimensionMismatch(format!(
            "Query vector dimension {} does not match tree dimension {}.",
            query_vector.dimension, tree.dimension
        )));
    }
    if points_data_slice.is_empty() && !tree.root.is_none() {
        // This case should ideally not happen if tree was built from points_data_slice.
        // If tree has nodes but points_data_slice is empty, it's an inconsistency.
        return Err(KdTreeError::InternalError(
            "Tree exists but no points data provided for search.".to_string()
        ));
    }


    let mut best_neighbors = BinaryHeap::with_capacity(k + 1); // k+1 to handle push then pop

    search_recursive(
        tree.root.as_ref().unwrap(), // Safe due to check above
        query_vector,
        k,
        0, // initial depth
        tree.dimension as usize,
        &mut best_neighbors,
        points_data_slice,
    )?;

    // Convert heap to sorted list of (point_slice_index, distance)
    // BinaryHeap iterates in arbitrary order. We need to sort it.
    let mut results: Vec<(usize, f32)> = best_neighbors
        .into_iter() // Drains the heap, order is not guaranteed specific like sorted.
        .map(|neighbor| (neighbor.point_slice_index, neighbor.distance_sq.sqrt()))
        .collect();

    // Sort by distance ascending
    results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));

    Ok(results)
}

fn search_recursive<'a>(
    node: &'a KdNode,
    query_vector: &VectorData,
    k: usize,
    depth: usize,
    dimension: usize,
    best_neighbors: &mut BinaryHeap<Neighbor>,
    points_data_slice: &'a [&'a VectorData],
) -> Result<(), KdTreeError> {
    match node {
        KdNode::Leaf { point_indices } => {
            for &slice_idx in point_indices {
                if slice_idx >= points_data_slice.len() {
                     return Err(KdTreeError::InternalError(format!(
                        "Point index {} from leaf is out of bounds for points_data_slice (len {})",
                        slice_idx, points_data_slice.len()
                    )));
                }
                let point_vec = points_data_slice[slice_idx];

                let dist_sq = query_vector.data.iter()
                    .zip(point_vec.data.iter())
                    .map(|(q_comp, p_comp)| (q_comp - p_comp).powi(2))
                    .sum::<f32>();

                if best_neighbors.len() < k {
                    best_neighbors.push(Neighbor { distance_sq: dist_sq, point_slice_index: slice_idx });
                } else {
                    // With current Ord for Neighbor (smaller distance_sq is "smaller"),
                    // BinaryHeap (max-heap) keeps largest distance_sq at peek().
                    // We want to replace if new_dist_sq is smaller than current max in heap.
                    if dist_sq < best_neighbors.peek().unwrap().distance_sq {
                        best_neighbors.pop(); // Remove largest distance
                        best_neighbors.push(Neighbor { distance_sq: dist_sq, point_slice_index: slice_idx });
                    }
                }
            }
        }
        KdNode::Internal { axis, split_value, left_child, right_child } => {
            let current_axis = *axis;
            let query_coord_on_axis = query_vector.data[current_axis];

            let (first_child_to_visit, second_child_to_visit) = if query_coord_on_axis < *split_value {
                (left_child, right_child)
            } else {
                (right_child, left_child)
            };

            search_recursive(
                first_child_to_visit,
                query_vector,
                k,
                depth + 1,
                dimension,
                best_neighbors,
                points_data_slice,
            )?;

            let distance_to_split_plane_sq = (query_coord_on_axis - *split_value).powi(2);

            // If heap is not full, or if the hypersphere (radius = kth current best distance)
            // intersects the splitting plane, we must search the other side.
            if best_neighbors.len() < k || distance_to_split_plane_sq < best_neighbors.peek().unwrap().distance_sq {
                search_recursive(
                    second_child_to_visit,
                    query_vector,
                    k,
                    depth + 1,
                    dimension,
                    best_neighbors,
                    points_data_slice,
                )?;
            }
        }
    }
    Ok(())
}
