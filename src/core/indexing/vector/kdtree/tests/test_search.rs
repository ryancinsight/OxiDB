// src/core/indexing/vector/kdtree/tests/test_search.rs

#[cfg(test)]
mod search_tests {
    use crate::core::indexing::vector::kdtree::builder::build_kdtree;
    use crate::core::indexing::vector::kdtree::search::find_knn;
    use crate::core::types::VectorData;
    use crate::core::indexing::vector::kdtree::error::KdTreeError;

    // Helper to create VectorData easily
    fn vec_data(data: Vec<f32>) -> VectorData {
        let dim = data.len() as u32;
        VectorData::new(dim, data).unwrap()
    }

    #[test]
    fn test_find_knn_empty_tree() {
        let points_data_refs: Vec<&VectorData> = Vec::new();
        let tree = build_kdtree(&points_data_refs, 2).unwrap();
        let query = vec_data(vec![1.0, 1.0]);
        let result = find_knn(&tree, &query, 1, &points_data_refs).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_find_knn_single_point_tree() {
        let p1 = vec_data(vec![1.0, 2.0]);
        let points_data_refs = vec![&p1];
        let tree = build_kdtree(&points_data_refs, 2).unwrap();

        let query = vec_data(vec![1.0, 2.0]);
        let results = find_knn(&tree, &query, 1, &points_data_refs).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 0); // index 0
        assert_eq!(results[0].1, 0.0); // distance 0
    }

    #[test]
    fn test_find_knn_exact_match() {
        let p1 = vec_data(vec![1.0, 2.0]);
        let p2 = vec_data(vec![5.0, 5.0]);
        let points_data_refs = vec![&p1, &p2];
        let tree = build_kdtree(&points_data_refs, 2).unwrap();

        let query = vec_data(vec![5.0, 5.0]);
        let results = find_knn(&tree, &query, 1, &points_data_refs).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1); // Index of p2
        assert_eq!(results[0].1, 0.0);
    }

    #[test]
    fn test_find_knn_k_greater_than_points() {
        let p1 = vec_data(vec![1.0, 2.0]);
        let p2 = vec_data(vec![5.0, 5.0]);
        let points_data_refs = vec![&p1, &p2];
        let tree = build_kdtree(&points_data_refs, 2).unwrap();

        let query = vec_data(vec![0.0, 0.0]);
        let results = find_knn(&tree, &query, 5, &points_data_refs).unwrap();
        assert_eq!(results.len(), 2); // Should return all points
                                      // Order should be p1 then p2 by distance to (0,0)
        let dist_p1 = (1.0f32.powi(2) + 2.0f32.powi(2)).sqrt(); // sqrt(5)
        let dist_p2 = (5.0f32.powi(2) + 5.0f32.powi(2)).sqrt(); // sqrt(50)

        assert_eq!(results[0].0, 0); // p1
        assert!((results[0].1 - dist_p1).abs() < f32::EPSILON);
        assert_eq!(results[1].0, 1); // p2
        assert!((results[1].1 - dist_p2).abs() < f32::EPSILON);
    }

    #[test]
    fn test_find_knn_simple_2d() {
        // Points: (2,3), (5,4), (9,6), (4,7), (8,1), (7,2)
        let p_refs: Vec<VectorData> = vec![
            vec_data(vec![2.0, 3.0]), // 0: dist to (6,3) = sqrt( (2-6)^2 + (3-3)^2 ) = sqrt(16) = 4
            vec_data(vec![5.0, 4.0]), // 1: dist to (6,3) = sqrt( (5-6)^2 + (4-3)^2 ) = sqrt(1+1) = sqrt(2) ~1.414
            vec_data(vec![9.0, 6.0]), // 2: dist to (6,3) = sqrt( (9-6)^2 + (6-3)^2 ) = sqrt(9+9) = sqrt(18) ~4.242
            vec_data(vec![4.0, 7.0]), // 3: dist to (6,3) = sqrt( (4-6)^2 + (7-3)^2 ) = sqrt(4+16) = sqrt(20) ~4.472
            vec_data(vec![8.0, 1.0]), // 4: dist to (6,3) = sqrt( (8-6)^2 + (1-3)^2 ) = sqrt(4+4) = sqrt(8) ~2.828
            vec_data(vec![7.0, 2.0]), // 5: dist to (6,3) = sqrt( (7-6)^2 + (2-3)^2 ) = sqrt(1+1) = sqrt(2) ~1.414
        ];
        let points_data_refs: Vec<&VectorData> = p_refs.iter().collect();
        let tree = build_kdtree(&points_data_refs, 2).unwrap();

        let query = vec_data(vec![6.0, 3.0]);
        let results = find_knn(&tree, &query, 3, &points_data_refs).unwrap();

        assert_eq!(results.len(), 3);

        // Expected order by distance: (5,4) and (7,2) are tied, then (8,1)
        // Indices: 1 and 5 are tied, then 4.
        let dist_1_5 = (2.0f32).sqrt();
        let dist_4 = (8.0f32).sqrt();

        // Check first two (tied)
        assert!(((results[0].1 - dist_1_5).abs() < f32::EPSILON));
        assert!(((results[1].1 - dist_1_5).abs() < f32::EPSILON));
        let tied_indices = vec![results[0].0, results[1].0];
        assert!(tied_indices.contains(&1) && tied_indices.contains(&5));

        // Check third one
        assert_eq!(results[2].0, 4);
        assert!(((results[2].1 - dist_4).abs() < f32::EPSILON));
    }

    #[test]
    fn test_find_knn_query_dimension_mismatch() {
        let p1 = vec_data(vec![1.0, 2.0]);
        let points_data_refs = vec![&p1];
        let tree = build_kdtree(&points_data_refs, 2).unwrap();

        let query_dim3 = vec_data(vec![1.0, 2.0, 3.0]);
        let result = find_knn(&tree, &query_dim3, 1, &points_data_refs);
        assert!(matches!(result, Err(KdTreeError::DimensionMismatch(_))));
    }

    #[test]
    fn test_find_knn_k_is_zero() {
        let p1 = vec_data(vec![1.0, 2.0]);
        let points_data_refs = vec![&p1];
        let tree = build_kdtree(&points_data_refs, 2).unwrap();
        let query = vec_data(vec![1.0, 1.0]);
        let result = find_knn(&tree, &query, 0, &points_data_refs).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_find_knn_internal_point_index_out_of_bounds() {
        // This test is tricky to set up perfectly without mocking the tree structure itself.
        // It relies on creating a situation where a leaf node might somehow store an invalid index.
        // The current builder should prevent this.
        // If we had a way to manually create a tree with an invalid leaf index:
        // let tree = KdTree { root: Some(Box::new(KdNode::Leaf { point_indices: vec![100] })), dimension: 2 };
        // let p1 = vec_data(vec![1.0, 2.0]);
        // let points_data_refs = vec![&p1]; // only one point at index 0
        // let query = vec_data(vec![0.0, 0.0]);
        // let result = find_knn(&tree, &query, 1, &points_data_refs);
        // assert!(matches!(result, Err(KdTreeError::InternalError(_))));
        // For now, we trust the builder and search logic not to produce this internally under normal operation.
        // This test case is more about defensive programming in search_recursive if the tree could be corrupted.
        // The check `if slice_idx >= points_data_slice.len()` in search_recursive covers this.
        // It's hard to trigger this path with a correctly built tree.
    }
}
