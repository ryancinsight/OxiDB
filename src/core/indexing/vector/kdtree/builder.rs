// src/core/indexing/vector/kdtree/builder.rs

//! Logic for building a KD-Tree from a set of points.

use std::cmp::Ordering;
use crate::core::types::VectorData;
use super::tree::{KdNode, KdTree};
use super::error::KdTreeError;

// Maximum number of points to store in a leaf node. If a node has more points,
// it will be split into an internal node.
const MAX_POINTS_IN_LEAF: usize = 16;

/// Represents a point to be indexed during the build process.
/// It holds the original index of the point (in the input slice given to `build_kdtree`)
/// and a reference to its `VectorData`.
type BuildPoint<'a> = (usize, &'a VectorData);

/// Builds a KD-Tree from a list of points.
///
/// Each point is a tuple of `(original_slice_index, vector_data_reference)`.
/// The `original_slice_index` is stored in the leaf nodes and refers to the position
/// of the vector in the initial `points_data` slice provided to this builder.
/// This allows the search function to retrieve the actual vector data using this index.
///
/// # Arguments
/// * `points_data`: A slice of `VectorData` references. The tree will store indices
///   into this slice. The caller must ensure this data outlives the tree if the tree
///   is used directly, or that a compatible slice is available during search.
/// * `dimension`: The dimensionality of the vectors.
///
/// # Returns
/// A `Result` containing the built `KdTree` or a `KdTreeError`.
pub fn build_kdtree<'a>(
    points_data: &'a [&'a VectorData], // Slice of vector data references
    dimension: u32,
) -> Result<KdTree, KdTreeError> {
    if dimension == 0 {
        return Err(KdTreeError::DimensionMismatch("Dimension cannot be 0.".to_string()));
    }
    if points_data.is_empty() {
        let mut tree = KdTree::new(dimension);
        tree.root = None;
        return Ok(tree);
    }

    // Validate all points have the correct dimension
    for v_data in points_data.iter() {
        if v_data.dimension != dimension {
            return Err(KdTreeError::DimensionMismatch(format!(
                "Expected dimension {}, but found point with dimension {}.",
                dimension, v_data.dimension
            )));
        }
    }

    // Create a list of `BuildPoint`s, where the usize is the index into `points_data`
    let mut build_points: Vec<BuildPoint<'a>> = points_data
        .iter()
        .enumerate()
        .map(|(idx, data)| (idx, *data))
        .collect();

    let root_node = build_recursive(&mut build_points, dimension as usize, 0)?;
    let mut tree = KdTree::new(dimension);
    tree.set_root(root_node);
    Ok(tree)
}

/// Recursively builds a KD-Tree node.
///
/// # Arguments
/// * `current_build_points`: A mutable slice of `BuildPoint`s to be processed for this node.
///   This slice will be sorted and partitioned. Each `BuildPoint` is `(original_slice_idx, &VectorData)`.
/// * `dimension`: The total dimensionality of the vectors.
/// * `depth`: The current depth in the tree, used to determine the split axis.
///
/// # Returns
/// A `Result` containing the `KdNode` or a `KdTreeError`.
fn build_recursive<'a>(
    current_build_points: &mut [BuildPoint<'a>],
    dimension: usize,
    depth: usize,
) -> Result<KdNode, KdTreeError> {
    if current_build_points.is_empty() {
        return Err(KdTreeError::InternalError("Attempted to build node from empty point set.".to_string()));
    }

    if current_build_points.len() <= MAX_POINTS_IN_LEAF {
        // Create a leaf node, storing the original_slice_idx from each BuildPoint
        let point_indices_in_leaf: Vec<usize> = current_build_points.iter().map(|(idx, _)| *idx).collect();
        return Ok(KdNode::Leaf { point_indices: point_indices_in_leaf });
    }

    let axis = depth % dimension;

    current_build_points.sort_unstable_by(|(_, a_vec), (_, b_vec)| {
        a_vec.data[axis].partial_cmp(&b_vec.data[axis]).unwrap_or(Ordering::Equal)
    });

    let median_idx = current_build_points.len() / 2;
    let split_value = current_build_points[median_idx].1.data[axis];

    let (left_slice, right_slice_inclusive_median) = current_build_points.split_at_mut(median_idx);

    if left_slice.is_empty() && current_build_points.len() > 1 { // ensure right_slice_inclusive_median is not the whole list if left is empty
        // This case means all points up to median_idx (which could be 0) are identical or already processed.
        // If left_slice is empty and current_build_points.len() > 1 (so median_idx was 0),
        // it means all points are effectively on the right side of the split if we strictly use median_idx.
        // This can lead to an infinite loop if not handled.
        // A simple fallback: if partitioning doesn't reduce the problem size for one child significantly,
        // make it a leaf to prevent very deep or unbalanced trees with identical points.
        // This specific condition (left_slice.is_empty() and len > 1) implies all points are >= split_value
        // and median_idx was 0.
        // If build_recursive is called on right_slice_inclusive_median and it's the same as current_build_points,
        // then we have a problem. This happens if median_idx is 0.
        // The split `split_at_mut(median_idx)`: if median_idx is 0, left_slice is empty, right_slice is everything.
        // Then build_recursive(right_slice_inclusive_median) is called with the same list. Infinite loop.
        //
        // Robust fix: Ensure median_idx for partitioning is at least 1 if len > 1 to guarantee left_slice is non-empty,
        // or handle the median_idx = 0 case for right_slice more carefully.
        // Alternative: if after sorting, all points from median_idx-1 to median_idx+1 (or some range)
        // are identical on this axis, it's a degenerate case.
        //
        // Current fallback: if left_slice is empty (median_idx was 0), and we are not a tiny list,
        // this is problematic. If current_build_points.len() > MAX_POINTS_IN_LEAF (so we are trying to split)
        // AND left_slice becomes empty, it means all points are effectively in the right child.
        // To prevent infinite recursion, we must ensure the recursive calls are on strictly smaller sets
        // or depth increases.
        //
        // If median_idx is 0, then left_slice is empty. `right_slice_inclusive_median` is `current_build_points`.
        // Calling `build_recursive(right_slice_inclusive_median, ...)` would be an infinite loop.
        // This happens if `current_build_points.len() / 2` is 0, i.e. `current_build_points.len()` is 1.
        // But `current_build_points.len() <= MAX_POINTS_IN_LEAF` handles len=1.
        // So `median_idx` will be >= 1 if `len > 1`.
        // Thus `left_slice` will not be empty if `len > 1`.
        // The problematic case is if `right_slice_inclusive_median` is empty. This cannot happen because `median_idx <= len / 2`.

        // The check `if left_slice.is_empty()` is actually not the primary concern for loops if median_idx > 0.
        // The concern is if *all points are identical on this axis*.
        // Then `left_slice` or `right_slice` might contain all points if not careful.
        // The current `sort_unstable_by` and `split_at_mut(median_idx)` ensures `left_slice` has `median_idx` elements
        // and `right_slice_inclusive_median` has `len - median_idx` elements.
        // Both will be non-empty if `median_idx` is chosen between `1` and `len-1`.
        // If `len > MAX_POINTS_IN_LEAF`, then `len >= 2` (assuming MAX_POINTS_IN_LEAF >=1).
        // `median_idx = len / 2`. If `len=2`, `median_idx=1`. `left_slice` has 1, `right_slice` has 1. Good.
        // If `len=3`, `median_idx=1`. `left_slice` has 1, `right_slice` has 2. Good.

        // The issue is if `split_value` is such that all points fall on one side *semantically*,
        // even if slices are non-empty. E.g., all points in `left_slice` are equal to `split_value`.
        // The current logic is okay: `left_child` gets points with value < `split_value` (potentially),
        // and `right_child` gets points with value >= `split_value`.
        // The `sort_unstable_by` places points with value `< split_value` before `median_idx`,
        // points `== split_value` around `median_idx`, and `> split_value` after.
        // `split_at_mut(median_idx)`:
        // `left_slice` contains elements `0` to `median_idx - 1`.
        // `right_slice_inclusive_median` contains `median_idx` to `len - 1`.
        // This guarantees partitioning of the list of indices.
        // The only way this fails to reduce the problem is if all points are identical
        // across all axes, up to `MAX_POINTS_IN_LEAF`.
        // The `left_slice.is_empty()` check from previous iteration was a bit misleading.
        // A more direct check for degenerate splits:
        // After forming `left_child` and `right_child`, if one of them represents an empty semantic set
        // (e.g. a Leaf with no points, which our current code doesn't produce as build_recursive errors on empty input),
        // or if one child is identical to the parent's point set. This isn't happening here.
        // The critical part is that recursive calls are on strictly smaller slices of indices.
        // `split_at_mut` ensures this.
    }


    let left_child = build_recursive(left_slice, dimension, depth + 1)?;
    let right_child = build_recursive(right_slice_inclusive_median, dimension, depth + 1)?;

    Ok(KdNode::Internal {
        axis,
        split_value,
        left_child: Box::new(left_child),
        right_child: Box::new(right_child),
    })
}
