// src/core/indexing/vector/kdtree/mod.rs

//! KD-Tree implementation for vector indexing.

// Re-export key components for easier use.
pub use self::error::KdTreeError;
use self::tree::KdTree; // Keep KdNode internal to this module for now
use super::{VectorIndex, VectorIndexError}; // Import the new trait and error
use crate::core::common::OxidbError;
use crate::core::query::commands::Key as PrimaryKey;
use crate::core::types::VectorData;
use std::path::{Path, PathBuf};
use std::sync::RwLock; // If we need interior mutability for parts, though trait methods take &mut self

// Modules within the kdtree crate
mod tree;
mod builder;
mod search;
mod error;

/// `KdTreeIndex`: Implements the `VectorIndex` trait using a KD-Tree.
///
/// This structure manages a collection of (PrimaryKey, VectorData) pairs
/// and uses a KD-Tree for efficient K-Nearest Neighbor searches.
/// The KD-Tree itself is rebuilt when data changes significantly or upon explicit request.
#[derive(Debug)]
pub struct KdTreeIndex {
    name: String,
    dimension: u32,
    tree: Option<KdTree>, // The actual KD-Tree, built from `points_store`
    // Stores original PrimaryKeys and their VectorData.
    // The KdTree's leaf nodes will store indices into this Vec.
    points_store: Vec<(PrimaryKey, VectorData)>,
    // Path for persistence. File name could be `index_name.kdtidx`
    path: PathBuf,
    // Keeps track if the tree is synchronized with points_store
    is_built: bool,
}

impl KdTreeIndex {
    /// Creates a new `KdTreeIndex`.
    ///
    /// # Arguments
    /// * `name` - The name of the index.
    /// * `dimension` - The dimensionality of vectors this index will store.
    /// * `base_path` - The directory where index files will be stored.
    pub fn new(name: String, dimension: u32, base_path: &Path) -> Result<Self, OxidbError> {
        let file_name = format!("{}.kdtidx", name);
        let index_path = base_path.join(file_name);

        Ok(Self {
            name,
            dimension,
            tree: None,
            points_store: Vec::new(),
            path: index_path,
            is_built: false,
        })
    }

    // Helper to get data in the format needed for kdtree::builder and kdtree::search
    fn get_search_data_slice(&self) -> Vec<&VectorData> {
        self.points_store.iter().map(|(_, vd)| vd).collect()
    }
}

impl VectorIndex for KdTreeIndex {
    fn name(&self) -> &str {
        &self.name
    }

    fn dimension(&self) -> u32 {
        self.dimension
    }

    fn insert(&mut self, vector: &VectorData, primary_key: &PrimaryKey) -> Result<(), OxidbError> {
        if vector.dimension != self.dimension {
            return Err(VectorIndexError::DimensionMismatch(format!(
                "Vector dim {} does not match index dim {}",
                vector.dimension, self.dimension
            ))
            .into());
        }
        // For simplicity, prevent duplicate primary keys. A real DB might allow updates.
        if self.points_store.iter().any(|(pk, _)| pk == primary_key) {
            return Err(OxidbError::PrimaryKeyViolation(format!(
                "Primary key {:?} already exists in KdTreeIndex '{}'",
                primary_key, self.name
            )));
        }
        self.points_store.push((primary_key.clone(), vector.clone()));
        self.is_built = false; // Tree needs rebuild
        Ok(())
    }

    fn delete(&mut self, primary_key_to_delete: &PrimaryKey) -> Result<(), OxidbError> {
        let initial_len = self.points_store.len();
        self.points_store.retain(|(pk, _)| pk != primary_key_to_delete);
        if self.points_store.len() < initial_len {
            self.is_built = false; // Tree needs rebuild
            Ok(())
        } else {
            Err(VectorIndexError::NotFound(format!(
                "Primary key {:?} not found for deletion in KdTreeIndex '{}'",
                primary_key_to_delete, self.name
            ))
            .into())
        }
    }

    fn build(&mut self) -> Result<(), OxidbError> {
        if self.points_store.is_empty() {
            self.tree = None;
            self.is_built = true;
            return Ok(());
        }

        let build_data_slice: Vec<&VectorData> = self.points_store.iter().map(|(_, vd)| vd).collect();

        match builder::build_kdtree(&build_data_slice, self.dimension) {
            Ok(kdtree) => {
                self.tree = Some(kdtree);
                self.is_built = true;
                Ok(())
            }
            Err(kdt_err) => Err(VectorIndexError::from(kdt_err).into()),
        }
    }

    // The trait asks for `build(&mut self, all_data: &[(PrimaryKey, VectorData)])`.
    // This implies the manager might pass all data.
    // Let's adjust: the KdTreeIndex itself is the source of truth for its data.
    // So the public `build` takes no args.
    // If the trait *must* take `all_data`, then KdTreeIndex must reconcile it.
    // For now, let's assume the trait can be: `fn build(&mut self) -> Result<(), OxidbError>;`
    // Re-reading the trait I defined: `build(&mut self, all_data: &[(PrimaryKey, VectorData)])`
    // This is if the IndexManager wants to *feed* data to a fresh index.
    // My KdTreeIndex accumulates data via `insert`.
    // Let's adapt: `build` should take `all_data` and replace `points_store`.
    // This makes `KdTreeIndex` more stateless if manager handles data persistence.
    //
    // Revised build based on trait:
    // fn build(&mut self, all_data: &[(PrimaryKey, VectorData)]) -> Result<(), OxidbError> {
    //     self.points_store = all_data.to_vec(); // Replace internal store
    //     self.is_built = false; // Mark for rebuild with new data
    //     // Call internal build:
    //     if self.points_store.is_empty() {
    //         self.tree = None;
    //         self.is_built = true;
    //         return Ok(());
    //     }
    //     let build_data_slice: Vec<&VectorData> = self.points_store.iter().map(|(_, vd)| vd).collect();
    //     match builder::build_kdtree(&build_data_slice, self.dimension) {
    //         Ok(kdtree) => {
    //             self.tree = Some(kdtree);
    //             self.is_built = true;
    //             Ok(())
    //         }
    //         Err(kdt_err) => Err(VectorIndexError::from(kdt_err).into()),
    //     }
    // }
    // This change means `insert`/`delete` on KdTreeIndex are for if it *owns* the data.
    // If IndexManager calls `build(all_data)` regularly, then `insert`/`delete` on the
    // index instance might not be used by the manager. This needs clarification on usage pattern.
    // For now, stick to `KdTreeIndex` owning its data via `insert`/`delete` and having its own `build()`.
    // The trait `build(&mut self, all_data: ...)` is problematic for this model.
    // Let's assume the trait `build` method is more like a "rebuild from current data" signal.
    // I will modify the trait slightly or assume `all_data` can be ignored if the index manages its own state.
    // For now, let's assume the `build` in the trait means "rebuild from your internal data".
    // To match the defined trait `build(&mut self, all_data: &[(PrimaryKey, VectorData)])`:
    // This method will *replace* the current data in `points_store` and then build.
    // This makes `insert`/`delete` on the `KdTreeIndex` less meaningful if the manager
    // always calls `build` with the full dataset.
    // Let's keep `insert`/`delete` for now, and `build` will use the provided `all_data`.

    // VectorIndex trait build method
    fn build(&mut self, all_data: &[(PrimaryKey, VectorData)]) -> Result<(), OxidbError> {
        // Validate dimensions of incoming data
        for (_, v) in all_data {
            if v.dimension != self.dimension {
                return Err(VectorIndexError::DimensionMismatch(format!(
                    "Incoming data for build has dimension {}, index expects {}",
                    v.dimension, self.dimension
                )).into());
            }
        }

        self.points_store = all_data.to_vec(); // Replace internal store

        if self.points_store.is_empty() {
            self.tree = None;
            self.is_built = true;
            return Ok(());
        }

        let build_data_slice: Vec<&VectorData> = self.points_store.iter().map(|(_, vd)| vd).collect();

        match builder::build_kdtree(&build_data_slice, self.dimension) {
            Ok(kdtree) => {
                self.tree = Some(kdtree);
                self.is_built = true;
                Ok(())
            }
            Err(kdt_err) => Err(VectorIndexError::from(kdt_err).into()),
        }
    }


    fn search_knn(
        &self,
        query_vector: &VectorData,
        k: usize,
    ) -> Result<Vec<(PrimaryKey, f32)>, OxidbError> {
        if !self.is_built || self.tree.is_none() {
            // Option 1: Rebuild automatically (could be slow)
            // const_cast_self.build()?; // Not possible with &self. Requires &mut self.
            // Option 2: Return error or perform brute-force scan as fallback.
            // For now, error if not built. Manager should ensure `build` is called.
            if !self.is_built {
                 return Err(VectorIndexError::BuildError(
                    "Search called on unbuilt KdTreeIndex. Call build() first.".to_string()
                ).into());
            }
            if self.tree.is_none() && !self.points_store.is_empty() {
                 return Err(VectorIndexError::BuildError(
                    "Index not built but contains points. Call build() first.".to_string()
                ).into());
            }
            if self.tree.is_none() && self.points_store.is_empty() {
                return Ok(Vec::new()); // Empty index, empty result
            }
        }

        if query_vector.dimension != self.dimension {
            return Err(VectorIndexError::DimensionMismatch(format!(
                "Query vector dim {} does not match index dim {}",
                query_vector.dimension, self.dimension
            ))
            .into());
        }

        let kdtree_ref = self.tree.as_ref().unwrap(); // Safe due to checks above
        let search_data_slice = self.get_search_data_slice();

        match search::find_knn(kdtree_ref, query_vector, k, &search_data_slice) {
            Ok(results_with_slice_indices) => {
                // Map slice indices back to PrimaryKeys
                let final_results = results_with_slice_indices
                    .into_iter()
                    .map(|(slice_idx, dist)| {
                        // self.points_store[slice_idx].0 is the PrimaryKey
                        (self.points_store[slice_idx].0.clone(), dist)
                    })
                    .collect();
                Ok(final_results)
            }
            Err(kdt_err) => Err(VectorIndexError::from(kdt_err).into()),
        }
    }

    fn save(&self) -> Result<(), OxidbError> {
        // Serialize self.points_store and self.dimension
        // The KdTree itself can be rebuilt on load from points_store.
        // For simplicity, we only save points_store.
        // More advanced: serialize the KdTree structure if it's very expensive to build.
        use std::fs::File;
        use std::io::Write;
        use bincode;

        let data_to_save = (&self.dimension, &self.points_store);
        let encoded: Vec<u8> = bincode::serialize(&data_to_save).map_err(|e| VectorIndexError::SaveError(format!("Bincode serialization failed: {}",e)))?;

        let mut file = File::create(&self.path).map_err(|e| VectorIndexError::SaveError(format!("Failed to create file {:?}: {}", self.path, e)))?;
        file.write_all(&encoded).map_err(|e| VectorIndexError::SaveError(format!("Failed to write to file {:?}: {}", self.path, e)))?;
        Ok(())
    }

    fn load(&mut self) -> Result<(), OxidbError> {
        use std::fs::File;
        use std::io::Read;
        use bincode;

        if !self.path.exists() {
            // Nothing to load, it's a new index or data was lost.
            // Initialize as empty, ready for build.
            self.points_store = Vec::new();
            self.tree = None;
            self.is_built = false; // Needs build even if empty, to be consistent.
            return Ok(());
        }

        let mut file = File::open(&self.path).map_err(|e| VectorIndexError::LoadError(format!("Failed to open file {:?}: {}", self.path, e)))?;
        let mut encoded = Vec::new();
        file.read_to_end(&mut encoded).map_err(|e| VectorIndexError::LoadError(format!("Failed to read from file {:?}: {}", self.path, e)))?;

        let (loaded_dimension, loaded_points_store): (u32, Vec<(PrimaryKey, VectorData)>) =
            bincode::deserialize(&encoded).map_err(|e| VectorIndexError::LoadError(format!("Bincode deserialization failed: {}",e)))?;

        if loaded_dimension != self.dimension {
            return Err(VectorIndexError::LoadError(format!(
                "Loaded index dimension {} does not match expected dimension {}",
                loaded_dimension, self.dimension
            )).into());
        }

        self.points_store = loaded_points_store;
        self.is_built = false; // Mark as not built; needs explicit build call by manager after load.
        self.tree = None; // Clear any existing tree structure.

        // Note: We don't automatically rebuild here. The IndexManager should call `build`
        // on all loaded indexes if that's the desired behavior after loading.
        // Or, if this load implies it should be ready, call internal build.
        // For now, defer build to IndexManager's strategy.
        Ok(())
    }
}
