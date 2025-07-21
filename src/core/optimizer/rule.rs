//! Optimization Rule Trait
//! 
//! This module defines the trait for query optimization rules,
//! following SOLID's Open/Closed Principle for extensible optimization.

use crate::core::common::OxidbError;
use crate::core::query::sql::ast::ConditionTree;

/// Trait for query optimization rules
/// Follows SOLID's Open/Closed Principle - rules can be added without modifying existing code
pub trait OptimizationRule {
    /// Apply the optimization rule to a condition tree
    fn apply(&self, condition: &ConditionTree) -> Result<ConditionTree, OxidbError>;
    
    /// Get the name of this optimization rule
    fn name(&self) -> &'static str;
    
    /// Check if this rule is applicable to the given condition
    fn is_applicable(&self, _condition: &ConditionTree) -> bool {
        true // By default, rules are always applicable
    }
    
    /// Get the priority of this rule (higher values are applied first)
    fn priority(&self) -> u32 {
        100 // Default priority
    }
}

/// Optimization rule manager
/// Follows SOLID's Single Responsibility Principle - manages rule application
pub struct RuleManager {
    rules: Vec<Box<dyn OptimizationRule>>,
}

impl RuleManager {
    /// Create a new rule manager
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
        }
    }
    
    /// Add a rule to the manager
    pub fn add_rule(&mut self, rule: Box<dyn OptimizationRule>) {
        self.rules.push(rule);
        // Sort by priority (highest first)
        self.rules.sort_by(|a, b| b.priority().cmp(&a.priority()));
    }
    
    /// Apply all applicable rules to a condition tree
    pub fn apply_rules(&self, condition: &ConditionTree) -> Result<ConditionTree, OxidbError> {
        let mut current_condition = condition.clone();
        
        for rule in &self.rules {
            if rule.is_applicable(&current_condition) {
                current_condition = rule.apply(&current_condition)?;
            }
        }
        
        Ok(current_condition)
    }
    
    /// Get the number of rules
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }
}

impl Default for RuleManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::query::sql::ast::{Condition, AstExpressionValue, AstLiteralValue};

    struct TestRule;
    
    impl OptimizationRule for TestRule {
        fn apply(&self, condition: &ConditionTree) -> Result<ConditionTree, OxidbError> {
            // Simple test rule that just returns the input
            Ok(condition.clone())
        }
        
        fn name(&self) -> &'static str {
            "TestRule"
        }
    }
    
    #[test]
    fn test_rule_manager() {
        let mut manager = RuleManager::new();
        manager.add_rule(Box::new(TestRule));
        
        assert_eq!(manager.rule_count(), 1);
        
        let condition = ConditionTree::Comparison(Condition {
            column: "test".to_string(),
            operator: "=".to_string(),
            value: AstExpressionValue::Literal(AstLiteralValue::Boolean(true)),
        });
        
        let result = manager.apply_rules(&condition).unwrap();
        assert_eq!(result, condition);
    }
}