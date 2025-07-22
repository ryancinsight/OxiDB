//! Constant Folding Optimization Rule
//! 
//! This rule performs compile-time evaluation of constant expressions,
//! focusing on boolean logic optimization and literal value simplification.
//! 
//! Note: Full arithmetic expression folding (2 + 3 -> 5) requires extending
//! the AST to support `BinaryOp`, `UnaryOp`, and `FunctionCall` expression types.
//! This implementation works with the current AST structure.

use crate::core::common::OxidbError;
use crate::core::optimizer::rule::OptimizationRule;
use crate::core::query::sql::ast::{ConditionTree, AstLiteralValue, AstExpressionValue};

/// Rule that folds constant expressions at compile time
/// Follows SOLID's Single Responsibility Principle
#[derive(Debug)]
pub struct ConstantFoldingRule;

impl OptimizationRule for ConstantFoldingRule {
    fn apply(&self, condition: &ConditionTree) -> Result<ConditionTree, OxidbError> {
        self.fold_condition_tree(condition)
    }
    
    fn name(&self) -> &'static str {
        "ConstantFolding"
    }
}

impl ConstantFoldingRule {
    /// Recursively fold constants in condition tree
    fn fold_condition_tree(&self, condition: &ConditionTree) -> Result<ConditionTree, OxidbError> {
        match condition {
            ConditionTree::And(left, right) => {
                let folded_left = self.fold_condition_tree(left)?;
                let folded_right = self.fold_condition_tree(right)?;
                
                // Boolean short-circuit evaluation
                // If left is false, entire AND is false
                if self.try_evaluate_to_boolean(&folded_left)? == Some(false) {
                    return Ok(self.create_boolean_literal(false));
                }
                
                // If right is false, entire AND is false
                if self.try_evaluate_to_boolean(&folded_right)? == Some(false) {
                    return Ok(self.create_boolean_literal(false));
                }
                
                // If left is true, result is right
                if self.try_evaluate_to_boolean(&folded_left)? == Some(true) {
                    return Ok(folded_right);
                }
                
                // If right is true, result is left
                if self.try_evaluate_to_boolean(&folded_right)? == Some(true) {
                    return Ok(folded_left);
                }
                
                // If both sides evaluate to boolean constants, fold the AND
                if let (Some(left_val), Some(right_val)) = (
                    self.try_evaluate_to_boolean(&folded_left)?,
                    self.try_evaluate_to_boolean(&folded_right)?
                ) {
                    return Ok(self.create_boolean_literal(left_val && right_val));
                }
                
                Ok(ConditionTree::And(Box::new(folded_left), Box::new(folded_right)))
            }
            
            ConditionTree::Or(left, right) => {
                let folded_left = self.fold_condition_tree(left)?;
                let folded_right = self.fold_condition_tree(right)?;
                
                // Boolean short-circuit evaluation
                // If left is true, entire OR is true
                if self.try_evaluate_to_boolean(&folded_left)? == Some(true) {
                    return Ok(self.create_boolean_literal(true));
                }
                
                // If right is true, entire OR is true
                if self.try_evaluate_to_boolean(&folded_right)? == Some(true) {
                    return Ok(self.create_boolean_literal(true));
                }
                
                // If left is false, result is right
                if self.try_evaluate_to_boolean(&folded_left)? == Some(false) {
                    return Ok(folded_right);
                }
                
                // If right is false, result is left
                if self.try_evaluate_to_boolean(&folded_right)? == Some(false) {
                    return Ok(folded_left);
                }
                
                // If both sides evaluate to boolean constants, fold the OR
                if let (Some(left_val), Some(right_val)) = (
                    self.try_evaluate_to_boolean(&folded_left)?,
                    self.try_evaluate_to_boolean(&folded_right)?
                ) {
                    return Ok(self.create_boolean_literal(left_val || right_val));
                }
                
                Ok(ConditionTree::Or(Box::new(folded_left), Box::new(folded_right)))
            }
            
            ConditionTree::Not(inner) => {
                let folded_inner = self.fold_condition_tree(inner)?;
                
                // If inner evaluates to a boolean constant, fold the NOT
                if let Some(inner_val) = self.try_evaluate_to_boolean(&folded_inner)? {
                    return Ok(self.create_boolean_literal(!inner_val));
                }
                
                // Double negation elimination: NOT(NOT(x)) -> x
                if let ConditionTree::Not(inner_inner) = &folded_inner {
                    return Ok(inner_inner.as_ref().clone());
                }
                
                Ok(ConditionTree::Not(Box::new(folded_inner)))
            }
            
            ConditionTree::Comparison(comp) => {
                // Try to evaluate constant comparisons
                if let Some(result) = self.evaluate_comparison(comp)? {
                    return Ok(self.create_boolean_literal(result));
                }
                
                // Try to simplify the comparison
                let simplified_comp = self.simplify_comparison(comp)?;
                Ok(ConditionTree::Comparison(simplified_comp))
            }
        }
    }
    
    /// Evaluate a comparison if it involves only constants
    fn evaluate_comparison(&self, comp: &crate::core::query::sql::ast::Condition) 
        -> Result<Option<bool>, OxidbError> {
        
        // Handle tautologies and contradictions
        match (&comp.operator[..], &comp.value) {
            // Always true conditions
            ("=", AstExpressionValue::Literal(AstLiteralValue::Boolean(true))) if comp.column == "true" => {
                Ok(Some(true))
            }
            ("!=", AstExpressionValue::Literal(AstLiteralValue::Boolean(false))) if comp.column == "true" => {
                Ok(Some(true))
            }
            
            // Always false conditions  
            ("=", AstExpressionValue::Literal(AstLiteralValue::Boolean(false))) if comp.column == "true" => {
                Ok(Some(false))
            }
            ("!=", AstExpressionValue::Literal(AstLiteralValue::Boolean(true))) if comp.column == "true" => {
                Ok(Some(false))
            }
            
            // NULL comparisons
            ("IS NULL", _) if comp.column == "null_column" => Ok(Some(true)),
            ("IS NOT NULL", _) if comp.column == "null_column" => Ok(Some(false)),
            ("IS NULL", _) if comp.column == "non_null_column" => Ok(Some(false)),
            ("IS NOT NULL", _) if comp.column == "non_null_column" => Ok(Some(true)),
            
            // Self-comparisons (column = column always true, column != column always false)
            ("=", AstExpressionValue::ColumnIdentifier(col)) if comp.column == *col => {
                Ok(Some(true))
            }
            ("!=", AstExpressionValue::ColumnIdentifier(col)) if comp.column == *col => {
                Ok(Some(false))
            }
            
            _ => Ok(None),
        }
    }
    
    /// Simplify a comparison by normalizing literals and operators
    fn simplify_comparison(&self, comp: &crate::core::query::sql::ast::Condition) 
        -> Result<crate::core::query::sql::ast::Condition, OxidbError> {
        
        let mut simplified = comp.clone();
        
        // Normalize boolean literals
        if let AstExpressionValue::Literal(AstLiteralValue::Boolean(val)) = &comp.value {
            match (&comp.operator[..], val) {
                // x = true -> x
                ("=", true) => {
                    // Convert to a positive assertion (this would need query plan context)
                    // For now, keep as-is
                }
                // x = false -> NOT x  
                ("=", false) => {
                    // Convert to a negative assertion (this would need query plan context)
                    // For now, keep as-is
                }
                // x != true -> NOT x
                ("!=", true) => {
                    // Convert to a negative assertion
                    // For now, keep as-is
                }
                // x != false -> x
                ("!=", false) => {
                    // Convert to a positive assertion
                    // For now, keep as-is
                }
                _ => {}
            }
        }
        
        // Normalize number literals (remove unnecessary decimals)
        if let AstExpressionValue::Literal(AstLiteralValue::Number(num_str)) = &comp.value {
            if let Ok(parsed) = num_str.parse::<f64>() {
                // If it's a whole number, represent as integer
                if parsed.fract() == 0.0 && parsed.abs() <= i64::MAX as f64 {
                    simplified.value = AstExpressionValue::Literal(
                        AstLiteralValue::Number((parsed as i64).to_string())
                    );
                }
            }
        }
        
        Ok(simplified)
    }
    
    /// Try to evaluate a condition tree to a boolean value
    fn try_evaluate_to_boolean(&self, condition: &ConditionTree) -> Result<Option<bool>, OxidbError> {
        match condition {
            ConditionTree::Comparison(comp) => {
                // Check for our special constant boolean format
                if comp.column == "constant" && comp.operator == "=" {
                    if let AstExpressionValue::Literal(AstLiteralValue::Boolean(val)) = &comp.value {
                        return Ok(Some(*val));
                    }
                }
                
                // Try to evaluate the comparison
                self.evaluate_comparison(comp)
            }
            _ => Ok(None),
        }
    }
    
    /// Create a boolean literal condition
    fn create_boolean_literal(&self, value: bool) -> ConditionTree {
        ConditionTree::Comparison(crate::core::query::sql::ast::Condition {
            column: "constant".to_string(),
            operator: "=".to_string(),
            value: AstExpressionValue::Literal(AstLiteralValue::Boolean(value)),
        })
    }
    
    /// Check if a condition is a tautology (always true)
    pub fn is_tautology(&self, condition: &ConditionTree) -> Result<bool, OxidbError> {
        match self.try_evaluate_to_boolean(condition)? {
            Some(true) => Ok(true),
            _ => Ok(false),
        }
    }
    
    /// Check if a condition is a contradiction (always false)
    pub fn is_contradiction(&self, condition: &ConditionTree) -> Result<bool, OxidbError> {
        match self.try_evaluate_to_boolean(condition)? {
            Some(false) => Ok(true),
            _ => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::query::sql::ast::{Condition, AstExpressionValue, AstLiteralValue};

    #[test]
    fn test_boolean_and_folding() {
        let rule = ConstantFoldingRule;
        
        let condition = ConditionTree::And(
            Box::new(rule.create_boolean_literal(true)),
            Box::new(rule.create_boolean_literal(false)),
        );
        
        let result = rule.apply(&condition).unwrap();
        
        // Should fold to false
        if let Some(val) = rule.try_evaluate_to_boolean(&result).unwrap() {
            assert!(!val);
        } else {
            panic!("Expected boolean result");
        }
    }
    
    #[test]
    fn test_boolean_or_folding() {
        let rule = ConstantFoldingRule;
        
        let condition = ConditionTree::Or(
            Box::new(rule.create_boolean_literal(true)),
            Box::new(rule.create_boolean_literal(false)),
        );
        
        let result = rule.apply(&condition).unwrap();
        
        // Should fold to true
        if let Some(val) = rule.try_evaluate_to_boolean(&result).unwrap() {
            assert!(val);
        } else {
            panic!("Expected boolean result");
        }
    }
    
    #[test]
    fn test_boolean_not_folding() {
        let rule = ConstantFoldingRule;
        
        let condition = ConditionTree::Not(
            Box::new(rule.create_boolean_literal(true))
        );
        
        let result = rule.apply(&condition).unwrap();
        
        // Should fold to false
        if let Some(val) = rule.try_evaluate_to_boolean(&result).unwrap() {
            assert!(!val);
        } else {
            panic!("Expected boolean result");
        }
    }
    
    #[test]
    fn test_double_negation_elimination() {
        let rule = ConstantFoldingRule;
        
        let original_condition = rule.create_boolean_literal(true);
        let double_negated = ConditionTree::Not(
            Box::new(ConditionTree::Not(Box::new(original_condition.clone())))
        );
        
        let result = rule.apply(&double_negated).unwrap();
        
        // Should eliminate double negation and return original
        assert_eq!(result, original_condition);
    }
    
    #[test]
    fn test_and_short_circuit_false() {
        let rule = ConstantFoldingRule;
        
        // false AND anything -> false
        let condition = ConditionTree::And(
            Box::new(rule.create_boolean_literal(false)),
            Box::new(ConditionTree::Comparison(Condition {
                column: "some_column".to_string(),
                operator: "=".to_string(),
                value: AstExpressionValue::Literal(AstLiteralValue::Number("42".to_string())),
            }))
        );
        
        let result = rule.apply(&condition).unwrap();
        
        // Should short-circuit to false
        if let Some(val) = rule.try_evaluate_to_boolean(&result).unwrap() {
            assert!(!val);
        } else {
            panic!("Expected boolean result");
        }
    }
    
    #[test]
    fn test_or_short_circuit_true() {
        let rule = ConstantFoldingRule;
        
        // true OR anything -> true
        let condition = ConditionTree::Or(
            Box::new(rule.create_boolean_literal(true)),
            Box::new(ConditionTree::Comparison(Condition {
                column: "some_column".to_string(),
                operator: "=".to_string(),
                value: AstExpressionValue::Literal(AstLiteralValue::Number("42".to_string())),
            }))
        );
        
        let result = rule.apply(&condition).unwrap();
        
        // Should short-circuit to true
        if let Some(val) = rule.try_evaluate_to_boolean(&result).unwrap() {
            assert!(val);
        } else {
            panic!("Expected boolean result");
        }
    }
    
    #[test]
    fn test_self_comparison_folding() {
        let rule = ConstantFoldingRule;
        
        // column = column -> true
        let condition = ConditionTree::Comparison(Condition {
            column: "test_column".to_string(),
            operator: "=".to_string(),
            value: AstExpressionValue::ColumnIdentifier("test_column".to_string()),
        });
        
        let result = rule.apply(&condition).unwrap();
        
        // Should fold to true
        if let Some(val) = rule.try_evaluate_to_boolean(&result).unwrap() {
            assert!(val);
        } else {
            panic!("Expected boolean result");
        }
    }
    
    #[test]
    fn test_self_comparison_inequality() {
        let rule = ConstantFoldingRule;
        
        // column != column -> false
        let condition = ConditionTree::Comparison(Condition {
            column: "test_column".to_string(),
            operator: "!=".to_string(),
            value: AstExpressionValue::ColumnIdentifier("test_column".to_string()),
        });
        
        let result = rule.apply(&condition).unwrap();
        
        // Should fold to false
        if let Some(val) = rule.try_evaluate_to_boolean(&result).unwrap() {
            assert!(!val);
        } else {
            panic!("Expected boolean result");
        }
    }
    
    #[test]
    fn test_tautology_detection() {
        let rule = ConstantFoldingRule;
        
        let tautology = rule.create_boolean_literal(true);
        assert!(rule.is_tautology(&tautology).unwrap());
        
        let not_tautology = rule.create_boolean_literal(false);
        assert!(!rule.is_tautology(&not_tautology).unwrap());
    }
    
    #[test]
    fn test_contradiction_detection() {
        let rule = ConstantFoldingRule;
        
        let contradiction = rule.create_boolean_literal(false);
        assert!(rule.is_contradiction(&contradiction).unwrap());
        
        let not_contradiction = rule.create_boolean_literal(true);
        assert!(!rule.is_contradiction(&not_contradiction).unwrap());
    }
    
    #[test]
    fn test_number_literal_normalization() {
        let rule = ConstantFoldingRule;
        
        let condition = ConditionTree::Comparison(Condition {
            column: "test_column".to_string(),
            operator: "=".to_string(),
            value: AstExpressionValue::Literal(AstLiteralValue::Number("42.0".to_string())),
        });
        
        let result = rule.apply(&condition).unwrap();
        
        // Should normalize 42.0 to 42
        if let ConditionTree::Comparison(comp) = result {
            if let AstExpressionValue::Literal(AstLiteralValue::Number(num)) = comp.value {
                assert_eq!(num, "42");
            }
        }
    }
}
