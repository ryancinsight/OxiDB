// src/core/indexing/vector/kdtree/tree.rs

//! Defines the core KD-Tree structures: `KdNode` and `KdTree`.

use crate::core::types::VectorData; // For storing with points if needed, or for access
use super::error::KdTreeError;

/// Represents a node in the KD-Tree.
///
/// A node can be either an internal node, which splits data along an axis,
/// or a leaf node, which stores the actual point identifiers.
/// Point identifiers are `usize` indices, expected to map to an external storage
/// of `VectorData` instances.
#[derive(Debug)]
pub enum KdNode {
    Internal {
        axis: usize,
        split_value: f32,
        left_child: Box<KdNode>,
        right_child: Box<KdNode>,
    },
    Leaf {
        // Stores indices into the original data slice provided during build.
        // These indices allow retrieval of the original point ID and its VectorData.
        point_indices: Vec<usize>,
    },
}

/// Represents a KD-Tree.
///
/// The tree is built from a set of points (vectors with associated identifiers).
/// It stores the root of the tree and the dimensionality of the vectors it indexes.
/// The actual `VectorData` instances are not stored directly within the tree nodes
/// to save space; instead, nodes store indices that refer to an external list
/// of vectors provided during build and search operations.
#[derive(Debug)]
pub struct KdTree {
    pub root: Option<Box<KdNode>>,
    pub dimension: u32, // Dimensionality of the vectors indexed by this tree.
}

impl KdTree {
    /// Creates a new, empty KD-Tree for a given dimension.
    /// The actual tree structure is built using the `build_kdtree` function.
    pub fn new(dimension: u32) -> Self {
        KdTree {
            root: None,
            dimension,
        }
    }

    /// Returns the dimensionality of the vectors this tree is designed for.
    pub fn get_dimension(&self) -> u32 {
        self.dimension
    }

    /// Sets the root node of the tree. Primarily used by the builder.
    pub(super) fn set_root(&mut self, root_node: KdNode) {
        self.root = Some(Box::new(root_node));
    }
}
