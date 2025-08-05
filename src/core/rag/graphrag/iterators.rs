//! Zero-cost iterator abstractions for GraphRAG
//!
//! This module provides efficient iterators that avoid intermediate allocations,
//! following the zero-cost abstraction principle.

use super::types::KnowledgeNode;
use crate::core::graph::NodeId;
use crate::core::vector::similarity::cosine_similarity;
use std::collections::hash_map;

/// Custom iterator for efficient similarity calculations without intermediate collections
pub struct SimilarityIterator<'a> {
    entities: hash_map::Iter<'a, NodeId, KnowledgeNode>,
    query_embedding: &'a [f32],
    threshold: f64,
}

impl<'a> SimilarityIterator<'a> {
    /// Create a new similarity iterator
    #[inline]
    pub fn new(
        entities: hash_map::Iter<'a, NodeId, KnowledgeNode>,
        query_embedding: &'a [f32],
        threshold: f64,
    ) -> Self {
        Self {
            entities,
            query_embedding,
            threshold,
        }
    }
}

impl<'a> Iterator for SimilarityIterator<'a> {
    type Item = (NodeId, f64);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        // Use iterator combinators to find next matching entity efficiently
        self.entities.find_map(|(node_id, entity)| {
            entity.embedding.as_ref().and_then(|embedding| {
                // Check cache first
                if let Some(&cached_similarity) = self.similarity_cache.get(node_id) {
                    if cached_similarity >= self.threshold {
                        return Some((*node_id, cached_similarity));
                    } else {
                        return None;
                    }
                }
                match cosine_similarity(self.query_embedding, &embedding.vector) {
                    Ok(similarity) => {
                        let similarity_f64 = f64::from(similarity);
                        // Store in cache
                        self.similarity_cache.insert(*node_id, similarity_f64);
                        if similarity_f64 >= self.threshold {
                            Some((*node_id, similarity_f64))
                        } else {
                            None
                        }
                    }
                    Err(_) => None,
                }
            })
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        // Conservative estimate: we don't know how many will match threshold
        (0, Some(self.entities.len()))
    }
}

/// Iterator adapter for filtering nodes by type
pub struct NodeTypeFilter<'a, I> {
    inner: I,
    node_type: &'a str,
}

impl<'a, I> NodeTypeFilter<'a, I> {
    #[inline]
    pub fn new(inner: I, node_type: &'a str) -> Self {
        Self { inner, node_type }
    }
}

impl<'a, I> Iterator for NodeTypeFilter<'a, I>
where
    I: Iterator<Item = &'a KnowledgeNode>,
{
    type Item = &'a KnowledgeNode;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.find(|node| node.node_type == self.node_type)
    }
}

/// Iterator combinator for chaining multiple similarity results
pub struct ChainedSimilarityIterator<I1, I2> {
    first: I1,
    second: I2,
    first_exhausted: bool,
}

impl<I1, I2> ChainedSimilarityIterator<I1, I2>
where
    I1: Iterator<Item = (NodeId, f64)>,
    I2: Iterator<Item = (NodeId, f64)>,
{
    #[inline]
    pub fn new(first: I1, second: I2) -> Self {
        Self {
            first,
            second,
            first_exhausted: false,
        }
    }
}

impl<I1, I2> Iterator for ChainedSimilarityIterator<I1, I2>
where
    I1: Iterator<Item = (NodeId, f64)>,
    I2: Iterator<Item = (NodeId, f64)>,
{
    type Item = (NodeId, f64);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if !self.first_exhausted {
            match self.first.next() {
                Some(item) => Some(item),
                None => {
                    self.first_exhausted = true;
                    self.second.next()
                }
            }
        } else {
            self.second.next()
        }
    }
}

/// Extension trait for iterator combinators
pub trait GraphRAGIteratorExt: Iterator {
    /// Filter by minimum similarity threshold
    fn filter_by_similarity(self, threshold: f64) -> FilterBySimilarity<Self>
    where
        Self: Sized + Iterator<Item = (NodeId, f64)>,
    {
        FilterBySimilarity::new(self, threshold)
    }

    /// Take top K results by score
    fn top_k(self, k: usize) -> TopK<Self>
    where
        Self: Sized + Iterator<Item = (NodeId, f64)>,
    {
        TopK::new(self, k)
    }
}

impl<T: Iterator> GraphRAGIteratorExt for T {}

/// Iterator adapter for filtering by similarity threshold
pub struct FilterBySimilarity<I> {
    inner: I,
    threshold: f64,
}

impl<I> FilterBySimilarity<I> {
    #[inline]
    fn new(inner: I, threshold: f64) -> Self {
        Self { inner, threshold }
    }
}

impl<I> Iterator for FilterBySimilarity<I>
where
    I: Iterator<Item = (NodeId, f64)>,
{
    type Item = (NodeId, f64);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.find(|(_, score)| *score >= self.threshold)
    }
}

/// Iterator adapter for taking top K results
pub struct TopK<I> {
    inner: Option<I>,
    k: usize,
    collected: Vec<(NodeId, f64)>,
    index: usize,
}

impl<I> TopK<I>
where
    I: Iterator<Item = (NodeId, f64)>,
{
    fn new(inner: I, k: usize) -> Self {
        Self {
            inner: Some(inner),
            k,
            collected: Vec::new(),
            index: 0,
        }
    }
}

impl<I> Iterator for TopK<I>
where
    I: Iterator<Item = (NodeId, f64)>,
{
    type Item = (NodeId, f64);

    fn next(&mut self) -> Option<Self::Item> {
        // Collect and sort on first call
        if self.collected.is_empty() && self.index == 0 {
            if let Some(inner) = self.inner.take() {
                self.collected = inner.collect();
                self.collected.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                self.collected.truncate(self.k);
            }
        }

        // Return next item from collected results
        if self.index < self.collected.len() {
            let result = self.collected[self.index];
            self.index += 1;
            Some(result)
        } else {
            None
        }
    }
}