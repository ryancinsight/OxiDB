//! Query Planning and Optimization
//! 
//! This module provides cost-based query optimization following SOLID and CUPID principles.

use crate::core::common::OxidbError;
use crate::core::query::sql::ast::{SelectStatement, ConditionTree, JoinType};
use crate::core::types::{Value, Schema};
use std::collections::HashMap;
use std::fmt;

/// Trait for query plan nodes
/// Follows SOLID's Open/Closed Principle - extensible without modification
pub trait PlanNode: fmt::Debug + Send + Sync {
    /// Get the estimated cost of executing this node
    fn estimated_cost(&self) -> f64;
    
    /// Get the estimated number of rows this node will produce
    fn estimated_rows(&self) -> usize;
    
    /// Get the output schema of this node
    fn output_schema(&self) -> &Schema;
}

/// Table scan plan node
#[derive(Debug)]
pub struct TableScanNode {
    pub table_name: String,
    pub schema: Schema,
    pub estimated_rows: usize,
    pub cost_per_row: f64,
}

impl PlanNode for TableScanNode {
    fn estimated_cost(&self) -> f64 {
        self.estimated_rows as f64 * self.cost_per_row
    }
    
    fn estimated_rows(&self) -> usize {
        self.estimated_rows
    }
    
    fn output_schema(&self) -> &Schema {
        &self.schema
    }
}

/// Filter plan node
#[derive(Debug)]
pub struct FilterNode {
    pub input: Box<dyn PlanNode>,
    pub condition: ConditionTree,
    pub selectivity: f64, // Estimated fraction of rows that pass the filter
}

impl PlanNode for FilterNode {
    fn estimated_cost(&self) -> f64 {
        self.input.estimated_cost() + (self.input.estimated_rows() as f64 * 0.1)
    }
    
    fn estimated_rows(&self) -> usize {
        ((self.input.estimated_rows() as f64) * self.selectivity) as usize
    }
    
    fn output_schema(&self) -> &Schema {
        self.input.output_schema()
    }
}

/// Join plan node
#[derive(Debug)]
pub struct JoinNode {
    pub left: Box<dyn PlanNode>,
    pub right: Box<dyn PlanNode>,
    pub join_type: JoinType,
    pub condition: Option<ConditionTree>,
    pub selectivity: f64,
}

impl PlanNode for JoinNode {
    fn estimated_cost(&self) -> f64 {
        let left_cost = self.left.estimated_cost();
        let right_cost = self.right.estimated_cost();
        let join_cost = (self.left.estimated_rows() * self.right.estimated_rows()) as f64 * 0.01;
        
        left_cost + right_cost + join_cost
    }
    
    fn estimated_rows(&self) -> usize {
        let base_rows = self.left.estimated_rows() * self.right.estimated_rows();
        ((base_rows as f64) * self.selectivity) as usize
    }
    
    fn output_schema(&self) -> &Schema {
        // For simplicity, return left schema. In practice, would merge schemas
        self.left.output_schema()
    }
}

/// Table statistics for cost estimation
#[derive(Debug, Clone)]
pub struct TableStats {
    pub row_count: usize,
    pub average_row_size: usize,
    pub column_stats: HashMap<String, ColumnStats>,
}

#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub distinct_values: usize,
    pub null_count: usize,
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
}

/// Cost-based query planner
/// Follows SOLID's Single Responsibility Principle
pub struct CostBasedPlanner {
    table_stats: HashMap<String, TableStats>,
    schemas: HashMap<String, Schema>,
}

impl CostBasedPlanner {
    /// Create a new cost-based planner
    pub fn new() -> Self {
        Self {
            table_stats: HashMap::new(),
            schemas: HashMap::new(),
        }
    }
    
    /// Add table statistics
    pub fn add_table_stats(&mut self, table_name: String, stats: TableStats) {
        self.table_stats.insert(table_name, stats);
    }
    
    /// Add table schema
    pub fn add_schema(&mut self, table_name: String, schema: Schema) {
        self.schemas.insert(table_name, schema);
    }
    
    /// Create an optimized query plan
    pub fn create_plan(&self, stmt: &SelectStatement) -> Result<Box<dyn PlanNode>, OxidbError> {
        // Create base scan plan
        let mut plan = self.create_scan_plan(&stmt.from_clause.name)?;
        
        // Add filter if condition exists
        if let Some(ref condition) = stmt.condition {
            plan = Box::new(FilterNode {
                input: plan,
                condition: condition.clone(),
                selectivity: self.estimate_selectivity(condition, &stmt.from_clause.name),
            });
        }
        
        // Add joins
        for join in &stmt.joins {
            let right_plan = self.create_scan_plan(&join.right_source.name)?;
            plan = Box::new(JoinNode {
                left: plan,
                right: right_plan,
                join_type: join.join_type.clone(),
                condition: join.on_condition.clone(),
                selectivity: 0.1, // Default selectivity
            });
        }
        
        Ok(plan)
    }
    
    /// Create a table scan plan
    fn create_scan_plan(&self, table_name: &str) -> Result<Box<dyn PlanNode>, OxidbError> {
        let schema = self.schemas.get(table_name)
            .ok_or_else(|| OxidbError::TableNotFound(table_name.to_string()))?;
            
        let stats = self.table_stats.get(table_name);
        let estimated_rows = stats.map(|s| s.row_count).unwrap_or(1000);
        
        Ok(Box::new(TableScanNode {
            table_name: table_name.to_string(),
            schema: schema.clone(),
            estimated_rows,
            cost_per_row: 1.0,
        }))
    }
    
    /// Estimate selectivity of a condition
    fn estimate_selectivity(&self, _condition: &ConditionTree, _table_name: &str) -> f64 {
        // Simplified selectivity estimation
        // In practice, would analyze condition and use column statistics
        0.1
    }
}

impl Default for CostBasedPlanner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{ColumnDef, DataType};
    use crate::core::query::sql::ast::{TableReference, JoinClause, SelectColumn};

    #[test]
    fn test_table_scan_cost_estimation() {
        let schema = Schema::new(vec![
            ColumnDef::new("id".to_string(), DataType::Integer, false),
            ColumnDef::new("name".to_string(), DataType::Text, false),
        ]);
        
        let node = TableScanNode {
            table_name: "users".to_string(),
            schema,
            estimated_rows: 1000,
            cost_per_row: 1.0,
        };
        
        assert_eq!(node.estimated_cost(), 1000.0);
        assert_eq!(node.estimated_rows(), 1000);
    }
    
    #[test]
    fn test_planner_creation() {
        let planner = CostBasedPlanner::new();
        assert!(planner.table_stats.is_empty());
        assert!(planner.schemas.is_empty());
    }
    
    #[test]
    fn test_add_table_stats() {
        let mut planner = CostBasedPlanner::new();
        let stats = TableStats {
            row_count: 1000,
            average_row_size: 100,
            column_stats: HashMap::new(),
        };
        
        planner.add_table_stats("users".to_string(), stats);
        assert!(planner.table_stats.contains_key("users"));
    }
}