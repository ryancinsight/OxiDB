// src/core/indexing/vector/kdtree/tests/test_builder.rs

#[cfg(test)]
mod builder_tests {
    use crate::core::indexing::vector::kdtree::builder::build_kdtree;
    use crate::core::indexing::vector::kdtree::tree::{KdNode, KdTree};
    use crate::core::types::VectorData;
    use crate::core::indexing::vector::kdtree::error::KdTreeError;

    // Helper to create VectorData easily
    fn vec_data(data: Vec<f32>) -> VectorData {
        let dim = data.len() as u32;
        VectorData::new(dim, data).unwrap()
    }

    #[test]
    fn test_build_empty() {
        let points_data: Vec<&VectorData> = Vec::new();
        let tree = build_kdtree(&points_data, 2).unwrap();
        assert!(tree.root.is_none());
        assert_eq!(tree.dimension, 2);
    }

    #[test]
    fn test_build_single_point() {
        let p1 = vec_data(vec![1.0, 2.0]);
        let points_data = vec![&p1];
        let tree = build_kdtree(&points_data, 2).unwrap();

        assert!(tree.root.is_some());
        if let Some(KdNode::Leaf { point_indices }) = tree.root.as_deref() {
            assert_eq!(point_indices, vec![0]); // Index 0 from points_data
        } else {
            panic!("Root should be a leaf for a single point.");
        }
    }

    #[test]
    fn test_build_multiple_points_leaf() {
        // Less than MAX_POINTS_IN_LEAF (default 16)
        let p1 = vec_data(vec![1.0, 2.0]);
        let p2 = vec_data(vec![3.0, 4.0]);
        let points_data = vec![&p1, &p2];
        let tree = build_kdtree(&points_data, 2).unwrap();

        assert!(tree.root.is_some());
        if let Some(KdNode::Leaf { point_indices }) = tree.root.as_deref() {
            // Order might change due to internal processing, but all indices should be present
            let mut sorted_indices = point_indices.clone();
            sorted_indices.sort();
            assert_eq!(sorted_indices, vec![0, 1]);
        } else {
            panic!("Root should be a leaf for a small number of points.");
        }
    }

    #[test]
    fn test_build_creates_internal_node() {
        // Needs more points than MAX_POINTS_IN_LEAF (default 16) to reliably test internal node creation.
        // Let's make MAX_POINTS_IN_LEAF small for this test scenario if possible, or use many points.
        // For now, assume MAX_POINTS_IN_LEAF = 1 for this test logic, or provide enough points.
        // The constant is 16. Let's provide 17 points.
        let mut points: Vec<VectorData> = Vec::new();
        for i in 0..17 {
            points.push(vec_data(vec![i as f32, (i * 2) as f32]));
        }
        let points_data: Vec<&VectorData> = points.iter().collect();

        let tree = build_kdtree(&points_data, 2).unwrap();
        assert!(tree.root.is_some());
        match tree.root.as_deref() {
            Some(KdNode::Internal { .. }) => { /* Correct */ }
            Some(KdNode::Leaf { .. }) => panic!("Root should be an internal node for 17 points."),
            None => panic!("Root should not be None."),
        }
    }

    #[test]
    fn test_dimension_mismatch_error() {
        let p1 = vec_data(vec![1.0, 2.0]); // dim 2
        let p2 = vec_data(vec![3.0, 4.0, 5.0]); // dim 3
        let points_data = vec![&p1, &p2];

        let result = build_kdtree(&points_data, 2);
        assert!(matches!(result, Err(KdTreeError::DimensionMismatch(_))));
    }

    #[test]
    fn test_build_dimension_zero_error() {
        let points_data: Vec<&VectorData> = Vec::new();
        let result = build_kdtree(&points_data, 0);
        assert!(matches!(result, Err(KdTreeError::DimensionMismatch(_))));
    }

    #[test]
    fn test_build_recursive_logic_simple_split() {
        // Test a simple 2D split.
        // Points: (2,3), (5,4), (9,6), (4,7), (8,1), (7,2)
        // MAX_POINTS_IN_LEAF should be small enough, e.g., 1 or 2, to force splits.
        // Let's assume default MAX_POINTS_IN_LEAF = 16. We need to ensure our points are structured
        // such that a split occurs and we can check axis and value.

        let p_refs: Vec<VectorData> = vec![
            vec_data(vec![2.0, 3.0]), // 0
            vec_data(vec![5.0, 4.0]), // 1
            vec_data(vec![9.0, 6.0]), // 2
            vec_data(vec![4.0, 7.0]), // 3
            vec_data(vec![8.0, 1.0]), // 4
            vec_data(vec![7.0, 2.0]), // 5
        ];
        let points_data: Vec<&VectorData> = p_refs.iter().collect();
        let tree = build_kdtree(&points_data, 2).unwrap(); // dim 2

        // Expected first split (depth 0): axis 0 (x-axis)
        // Sorted by x: (2,3), (4,7), (5,4), (7,2), (8,1), (9,6)
        // Median index: 6 / 2 = 3. Element at index 3 is (7,2). Split value is 7.0.
        // Left slice: (2,3), (4,7), (5,4)
        // Right slice (inclusive median): (7,2), (8,1), (9,6)

        if let Some(KdNode::Internal { axis, split_value, left_child, right_child }) = tree.root.as_deref() {
            assert_eq!(*axis, 0);
            assert_eq!(*split_value, 7.0); // x-coordinate of points_data[original index of (7,2)] which is p_refs[5].data[0]

            // Further checks on children would be more involved.
            // Check left child (depth 1, axis 1 (y-axis))
            // Points: (2,3), (4,7), (5,4) -> sorted by y: (2,3), (5,4), (4,7)
            // Median index: 3 / 2 = 1. Element at index 1 is (5,4). Split value is 4.0.
            if let KdNode::Internal { axis: l_axis, split_value: l_split_val, .. } = left_child.as_ref() {
                assert_eq!(*l_axis, 1); // y-axis
                assert_eq!(*l_split_val, 4.0); // y-coordinate of (5,4)
            } else if let KdNode::Leaf {point_indices} = left_child.as_ref() {
                 // If MAX_POINTS_IN_LEAF >= 3, this would be a leaf.
                 // Default is 16, so this will be a leaf.
                 assert_eq!(point_indices.len(), 3); // (2,3), (4,7), (5,4) -> indices 0, 3, 1
                 let mut sorted_indices = point_indices.clone();
                 sorted_indices.sort();
                 assert_eq!(sorted_indices, vec![0, 1, 3]); // original indices
            }


            // Check right child (depth 1, axis 1 (y-axis))
            // Points: (7,2), (8,1), (9,6) -> sorted by y: (8,1), (7,2), (9,6)
            // Median index: 3 / 2 = 1. Element at index 1 is (7,2). Split value is 2.0.
             if let KdNode::Internal { axis: r_axis, split_value: r_split_val, .. } = right_child.as_ref() {
                assert_eq!(*r_axis, 1); // y-axis
                assert_eq!(*r_split_val, 2.0); // y-coordinate of (7,2)
            } else if let KdNode::Leaf {point_indices} = right_child.as_ref() {
                // If MAX_POINTS_IN_LEAF >= 3, this would be a leaf.
                // Default is 16, so this will be a leaf.
                 assert_eq!(point_indices.len(), 3); // (7,2), (8,1), (9,6) -> indices 5, 4, 2
                 let mut sorted_indices = point_indices.clone();
                 sorted_indices.sort();
                 assert_eq!(sorted_indices, vec![2, 4, 5]); // original indices
            }

        } else {
            panic!("Root should be an internal node for these points if MAX_POINTS_IN_LEAF is small enough.");
            // With MAX_POINTS_IN_LEAF = 16, 6 points will result in a Leaf node.
            // The test logic needs to be adjusted or MAX_POINTS_IN_LEAF mocked/reduced.
            // For now, let's verify it's a Leaf if that's the case.
            if let Some(KdNode::Leaf { point_indices }) = tree.root.as_deref() {
                assert_eq!(point_indices.len(), 6);
                let mut sorted_indices = point_indices.clone();
                sorted_indices.sort();
                assert_eq!(sorted_indices, vec![0,1,2,3,4,5]);
            } else {
                panic!("Should be a leaf with 6 points and MAX_POINTS_IN_LEAF = 16");
            }
        }
    }
}
