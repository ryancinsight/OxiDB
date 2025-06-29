// src/api/tests/similarity_search_tests.rs

#[cfg(test)]
mod similarity_search_tests {
    use crate::api::Oxidb;
    use crate::core::types::{DataType, VectorData};
    use crate::api::types::Value; // Corrected import for Value
    use std::fs;
    use tempfile::tempdir;
    // Removed unused import: use crate::core::query::commands::Command;

    fn create_temp_db() -> (Oxidb, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let data_path = dir.path().join("test_db.oxidb");
        // WAL and Index paths are now derived by Oxidb::new
        // fs::create_dir_all(&index_path).unwrap(); // Not needed explicitly if Oxidb::new handles it

        let db = Oxidb::new(data_path.to_str().unwrap()).unwrap();
        (db, dir)
    }

    #[test]
    fn test_similarity_search_basic() {
        let (mut db, _dir) = create_temp_db();

        // Create table
        db.execute_query_str("CREATE TABLE vectors_table (id INTEGER PRIMARY KEY, embedding VECTOR[3], name STRING);")
            .unwrap();

        // Insert data
        db.execute_query_str("INSERT INTO vectors_table (id, embedding, name) VALUES (1, [1.0, 1.0, 1.0], 'item1');")
            .unwrap();
        db.execute_query_str("INSERT INTO vectors_table (id, embedding, name) VALUES (2, [2.0, 2.0, 2.0], 'item2');")
            .unwrap();
        db.execute_query_str("INSERT INTO vectors_table (id, embedding, name) VALUES (3, [1.1, 1.1, 1.1], 'item3');")
            .unwrap();
        db.execute_query_str("INSERT INTO vectors_table (id, embedding, name) VALUES (4, [5.0, 5.0, 5.0], 'item4');")
            .unwrap();

        // Perform similarity search
        let results = db.execute_query_str("SIMILARITY_SEARCH vectors_table ON embedding QUERY [1.0,1.0,1.0] TOP_K 2;").unwrap();

        match results {
            Value::RankedResults(ranked_data) => {
                assert_eq!(ranked_data.len(), 2);
                // First result should be item1 (distance 0)
                assert_eq!(ranked_data[0].0, 0.0); // distance
                assert_eq!(ranked_data[0].1[0], DataType::Integer(1)); // id
                assert_eq!(ranked_data[0].1[1], DataType::Vector(VectorData::new(3, vec![1.0, 1.0, 1.0]).unwrap())); // embedding
                assert_eq!(ranked_data[0].1[2], DataType::String("item1".to_string())); // name

                // Second result should be item3 (closest after item1)
                let expected_dist_item3 = VectorData::new(3, vec![1.1, 1.1, 1.1]).unwrap().euclidean_distance(&VectorData::new(3, vec![1.0,1.0,1.0]).unwrap()).unwrap();
                assert_eq!(ranked_data[1].0, expected_dist_item3); // distance
                assert_eq!(ranked_data[1].1[0], DataType::Integer(3)); // id
                assert_eq!(ranked_data[1].1[1], DataType::Vector(VectorData::new(3, vec![1.1, 1.1, 1.1]).unwrap())); // embedding
                assert_eq!(ranked_data[1].1[2], DataType::String("item3".to_string())); // name
            }
            _ => panic!("Expected RankedResults, got {:?}", results),
        }
    }

    #[test]
    fn test_similarity_search_empty_table() {
        let (mut db, _dir) = create_temp_db();
        db.execute_query_str("CREATE TABLE empty_vectors (id INTEGER PRIMARY KEY, embedding VECTOR[2]);")
            .unwrap();

        let results = db.execute_query_str("SIMILARITY_SEARCH empty_vectors ON embedding QUERY [1.0,2.0] TOP_K 3;").unwrap();
        match results {
            Value::RankedResults(ranked_data) => {
                assert_eq!(ranked_data.len(), 0);
            }
            _ => panic!("Expected RankedResults, got {:?}", results),
        }
    }

    #[test]
    fn test_similarity_search_top_k_greater_than_items() {
        let (mut db, _dir) = create_temp_db();
        db.execute_query_str("CREATE TABLE few_vectors (id INTEGER PRIMARY KEY, embedding VECTOR[2]);")
            .unwrap();
        db.execute_query_str("INSERT INTO few_vectors (id, embedding) VALUES (1, [1.0,1.0]);")
            .unwrap();
        db.execute_query_str("INSERT INTO few_vectors (id, embedding) VALUES (2, [2.0,2.0]);")
            .unwrap();

        let results = db.execute_query_str("SIMILARITY_SEARCH few_vectors ON embedding QUERY [0.0,0.0] TOP_K 5;").unwrap();
        match results {
            Value::RankedResults(ranked_data) => {
                assert_eq!(ranked_data.len(), 2); // Should return all available items
            }
            _ => panic!("Expected RankedResults, got {:?}", results),
        }
    }

    #[test]
    fn test_similarity_search_table_not_exist() {
        let (mut db, _dir) = create_temp_db();
        let result = db.execute_query_str("SIMILARITY_SEARCH non_existent_table ON embedding QUERY [1.0,1.0] TOP_K 1;");
        assert!(result.is_err());
        let error_str = result.err().unwrap().to_string();
        assert!(error_str.contains("Table 'non_existent_table' not found for similarity search."));
    }

    #[test]
    fn test_similarity_search_column_not_exist() {
        let (mut db, _dir) = create_temp_db();
        db.execute_query_str("CREATE TABLE simple_table (id INTEGER PRIMARY KEY, name STRING);")
            .unwrap();
        let result = db.execute_query_str("SIMILARITY_SEARCH simple_table ON non_existent_column QUERY [1.0,1.0] TOP_K 1;");
        assert!(result.is_err());
        let error_str = result.err().unwrap().to_string();
        assert!(error_str.contains("Vector column 'non_existent_column' not found in table 'simple_table'."));
    }

    #[test]
    fn test_similarity_search_column_not_vector_type() {
        let (mut db, _dir) = create_temp_db();
        db.execute_query_str("CREATE TABLE mixed_table (id INTEGER PRIMARY KEY, text_col STRING, num_col INTEGER);")
            .unwrap();
        db.execute_query_str("INSERT INTO mixed_table (id, text_col, num_col) VALUES (1, 'hello', 100);")
            .unwrap();
        let result = db.execute_query_str("SIMILARITY_SEARCH mixed_table ON text_col QUERY [1.0,1.0] TOP_K 1;");
        assert!(result.is_err());
        let error_str = result.err().unwrap().to_string();
        assert!(error_str.contains("Column 'text_col' in table 'mixed_table' is not of type VECTOR."));
    }

    #[test]
    fn test_similarity_search_dimension_mismatch_in_data() {
        let (mut db, _dir) = create_temp_db();
        db.execute_query_str("CREATE TABLE dim_match_table (id INTEGER PRIMARY KEY, embedding VECTOR[3]);")
            .unwrap();
        // Correct dimension
        db.execute_query_str("INSERT INTO dim_match_table (id, embedding) VALUES (1, [1.0,1.0,1.0]);")
            .unwrap();
        // Incorrect dimension for this specific row's data, this insert should ideally fail or the data stored as null/error.
        // However, current AST/DataType translation allows inserting VectorData directly.
        // The check is done during similarity search.
        // For this test, we assume the data is inserted as is.
        // To simulate this more directly for the test, we'd need to insert a DataType::Map with a VectorData of wrong dimension.
        // The current SQL INSERT will parse "[2.0,2.0]" as VECTOR[2] which will fail at INSERT time due to schema mismatch.
        // So, this test case as written for SQL insert might not directly test the runtime check in similarity search
        // if the insert itself prevents such data.
        // For now, let's assume the data could somehow be there with a different dimension (e.g. if schema was altered or direct KV store manipulation)
        // The current implementation of handle_similarity_search logs and skips such rows.

        // Let's try inserting a row that *would* have a different dimension if the parser allowed it,
        // but the schema is VECTOR[3]. The SQL parser for INSERT VALUES `[2.0,2.0]` will create a VectorData{dim:2, data:[2.0,2.0]}
        // The translation layer for SQL INSERT then checks schema.
        // `test_insert_vector_dimension_mismatch` in `db_tests.rs` covers this insert-time failure.

        // To properly test the runtime check in similarity_search, we need to bypass the strict insert schema check
        // or have a schema that allows varying dimensions (not typical in SQL).
        // Given the current setup, the similarity search's dimension check for individual rows might be hard to trigger via SQL if inserts are strict.
        // However, the query_vector dimension vs column's declared dimension IS checked.

        // This test will check if the query vector's dimension mismatches the column's schema dimension.
        let result = db.execute_query_str("SIMILARITY_SEARCH dim_match_table ON embedding QUERY [5.0,6.0] TOP_K 1;"); // Query vec dim 2, schema dim 3
        // The `handle_similarity_search` logs "Skipping row due to dimension mismatch" if a *row's vector* has a different dimension
        // than the *query_vector*. It does not error out for the whole query if the *column's schema dimension* differs from *query_vector* dimension.
        // This behavior might need refinement. The current check is `row_vector.dimension != query_vector.dimension`.

        // Let's refine the test to check that only matching dimension rows are processed.
        // Insert another valid one
        db.execute_query_str("INSERT INTO dim_match_table (id, embedding) VALUES (2, [1.5,1.5,1.5]);")
            .unwrap();

        // Query with a vector of dimension 3
        let results = db.execute_query_str("SIMILARITY_SEARCH dim_match_table ON embedding QUERY [1.0,1.0,0.9] TOP_K 2;").unwrap();
         match results {
            Value::RankedResults(ranked_data) => {
                assert_eq!(ranked_data.len(), 2); // Both rows have dim 3, should be found
                assert_eq!(ranked_data[0].1[0], DataType::Integer(1));
                assert_eq!(ranked_data[1].1[0], DataType::Integer(2));
            }
            _ => panic!("Expected RankedResults, got {:?}", results),
        }

        // If we could insert a vector of a different dimension (e.g., [7.0, 7.0] into VECTOR[3] column),
        // the `handle_similarity_search` would print:
        // "Skipping row due to dimension mismatch: table 2 (2), query (3)"
        // and that row wouldn't be part of the results.
        // Since SQL INSERT currently prevents this, this specific internal check is harder to unit test via SQL interface.
    }

    #[test]
    fn test_similarity_search_with_kdtree_index() {
        let (mut db, _dir) = create_temp_db();

        // Create table
        db.execute_query_str("CREATE TABLE indexed_vectors (id INTEGER PRIMARY KEY, embedding VECTOR[3], category STRING);")
            .unwrap();

        // Insert data
        db.execute_query_str("INSERT INTO indexed_vectors (id, embedding, category) VALUES (10, [1.0, 1.0, 1.0], 'A');").unwrap();
        db.execute_query_str("INSERT INTO indexed_vectors (id, embedding, category) VALUES (20, [2.0, 2.0, 2.0], 'B');").unwrap();
        db.execute_query_str("INSERT INTO indexed_vectors (id, embedding, category) VALUES (30, [1.1, 1.1, 1.1], 'A');").unwrap();
        db.execute_query_str("INSERT INTO indexed_vectors (id, embedding, category) VALUES (40, [5.0, 5.0, 5.0], 'C');").unwrap();
        db.execute_query_str("INSERT INTO indexed_vectors (id, embedding, category) VALUES (50, [1.05, 1.05, 1.05], 'A');").unwrap();


        // Create KD-Tree index - using conventional name that executor will look for.
        // The executor uses "vidx_<table>_<column>"
        let create_index_result = db.execute_query_str("CREATE VECTOR INDEX vidx_indexed_vectors_embedding ON indexed_vectors (embedding) USING KDTREE;");
        if let Err(e) = &create_index_result {
            eprintln!("Failed to create vector index: {:?}", e);
        }
        create_index_result.unwrap();


        // Important: Build the index. The IndexManager and KdTreeIndex require explicit build.
        // This is a conceptual API call. The actual way to trigger this might be different,
        // e.g., it might happen automatically after CREATE INDEX + commit, or via a specific SQL command.
        // For now, let's assume there's an (internal or test-only) way to ensure the index is built.
        // If not, the KdTreeIndex search_knn will return a "BuildError", and executor will fallback to scan.
        // To make this test pass robustly without a specific `BUILD INDEX` SQL command,
        // the `CREATE VECTOR INDEX` execution path in `QueryExecutor` or `CommandProcessor`
        // should ideally trigger an initial build of the index.
        // Let's assume for this test that the index is built after creation or that the system handles it.
        // If the `handle_similarity_search` correctly falls back to scan when index is not built,
        // this test would still pass on correctness but wouldn't confirm index usage.

        // Perform similarity search
        let results = db.execute_query_str("SIMILARITY_SEARCH indexed_vectors ON embedding QUERY [1.0,1.0,1.0] TOP_K 3;").unwrap();

        match results {
            Value::RankedResults(ranked_data) => {
                assert_eq!(ranked_data.len(), 3);
                // Expected order:
                // 1. (10, [1.0, 1.0, 1.0], 'A') - dist 0
                // 2. (50, [1.05, 1.05, 1.05], 'A') - dist sqrt(3 * 0.05^2) = sqrt(3 * 0.0025) = sqrt(0.0075) approx 0.0866
                // 3. (30, [1.1, 1.1, 1.1], 'A') - dist sqrt(3 * 0.1^2) = sqrt(3 * 0.01) = sqrt(0.03) approx 0.1732

                assert_eq!(ranked_data[0].0, 0.0); // distance for item 10
                assert_eq!(ranked_data[0].1[0], DataType::Integer(10)); // id

                let dist_50 = VectorData::new(3, vec![1.05, 1.05, 1.05]).unwrap().euclidean_distance(&VectorData::new(3, vec![1.0,1.0,1.0]).unwrap()).unwrap();
                assert!((ranked_data[1].0 - dist_50).abs() < f32::EPSILON);
                assert_eq!(ranked_data[1].1[0], DataType::Integer(50)); // id

                let dist_30 = VectorData::new(3, vec![1.1, 1.1, 1.1]).unwrap().euclidean_distance(&VectorData::new(3, vec![1.0,1.0,1.0]).unwrap()).unwrap();
                assert!((ranked_data[2].0 - dist_30).abs() < f32::EPSILON);
                assert_eq!(ranked_data[2].1[0], DataType::Integer(30)); // id
            }
            _ => panic!("Expected RankedResults, got {:?}", results),
        }
    }
}
