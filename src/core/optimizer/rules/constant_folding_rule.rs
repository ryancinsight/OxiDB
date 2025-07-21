//! Constant Folding Optimization Rule
//! 
//! This rule performs compile-time evaluation of constant expressions
//! following KISS and DRY principles.

use crate::core::common::OxidbError;
use crate::core::optimizer::rule::OptimizationRule;
use crate::core::query::sql::ast::{ConditionTree, AstLiteralValue, AstExpressionValue};
use crate::core::types::Value;

/// Rule that folds constant expressions at compile time
/// Follows SOLID's Single Responsibility Principle
pub struct ConstantFoldingRule;

impl OptimizationRule for ConstantFoldingRule {
    fn apply(&self, condition: &ConditionTree) -> Result<ConditionTree, OxidbError> {
        self.fold_condition(condition)
    }
    
    fn name(&self) -> &'static str {
        "ConstantFolding"
    }
}

impl ConstantFoldingRule {
    /// Recursively fold constants in condition tree
    fn fold_condition(&self, condition: &ConditionTree) -> Result<ConditionTree, OxidbError> {
        match condition {
            ConditionTree::And(left, right) => {
                let folded_left = self.fold_condition(left)?;
                let folded_right = self.fold_condition(right)?;
                
                // Try to evaluate boolean AND if both sides are constants
                match (&folded_left, &folded_right) {
                    (ConditionTree::Comparison(l), ConditionTree::Comparison(r)) => {
                        if let (Some(l_val), Some(r_val)) = (
                            self.evaluate_constant_condition(l)?,
                            self.evaluate_constant_condition(r)?
                        ) {
                            if let (Value::Boolean(l_bool), Value::Boolean(r_bool)) = (l_val, r_val) {
                                return Ok(ConditionTree::Comparison(crate::core::query::sql::ast::Condition {
                                    column: "constant".to_string(),
                                    operator: "=".to_string(),
                                    value: AstExpressionValue::Literal(AstLiteralValue::Boolean(l_bool && r_bool)),
                                }));
                            }
                        }
                    }
                    _ => {}
                }
                
                Ok(ConditionTree::And(Box::new(folded_left), Box::new(folded_right)))
            }
            
            ConditionTree::Or(left, right) => {
                let folded_left = self.fold_condition(left)?;
                let folded_right = self.fold_condition(right)?;
                
                // Try to evaluate boolean OR if both sides are constants
                match (&folded_left, &folded_right) {
                    (ConditionTree::Comparison(l), ConditionTree::Comparison(r)) => {
                        if let (Some(l_val), Some(r_val)) = (
                            self.evaluate_constant_condition(l)?,
                            self.evaluate_constant_condition(r)?
                        ) {
                            if let (Value::Boolean(l_bool), Value::Boolean(r_bool)) = (l_val, r_val) {
                                return Ok(ConditionTree::Comparison(crate::core::query::sql::ast::Condition {
                                    column: "constant".to_string(),
                                    operator: "=".to_string(),
                                    value: AstExpressionValue::Literal(AstLiteralValue::Boolean(l_bool || r_bool)),
                                }));
                            }
                        }
                    }
                    _ => {}
                }
                
                Ok(ConditionTree::Or(Box::new(folded_left), Box::new(folded_right)))
            }
            
            ConditionTree::Not(inner) => {
                let folded_inner = self.fold_condition(inner)?;
                
                // Try to evaluate boolean NOT if inner is constant
                if let ConditionTree::Comparison(comp) = &folded_inner {
                    if let Some(val) = self.evaluate_constant_condition(comp)? {
                        if let Value::Boolean(bool_val) = val {
                            return Ok(ConditionTree::Comparison(crate::core::query::sql::ast::Condition {
                                column: "constant".to_string(),
                                operator: "=".to_string(),
                                value: AstExpressionValue::Literal(AstLiteralValue::Boolean(!bool_val)),
                            }));
                        }
                    }
                }
                
                Ok(ConditionTree::Not(Box::new(folded_inner)))
            }
            
            ConditionTree::Comparison(comp) => {
                // Try to fold constant comparisons
                if let Some(result) = self.evaluate_constant_condition(comp)? {
                    if let Value::Boolean(bool_result) = result {
                        return Ok(ConditionTree::Comparison(crate::core::query::sql::ast::Condition {
                            column: "constant".to_string(),
                            operator: "=".to_string(),
                            value: AstExpressionValue::Literal(AstLiteralValue::Boolean(bool_result)),
                        }));
                    }
                }
                
                Ok(condition.clone())
            }
        }
    }
    
    /// Evaluate a constant condition if possible
    fn evaluate_constant_condition(&self, condition: &crate::core::query::sql::ast::Condition) 
        -> Result<Option<Value>, OxidbError> {
        // For now, only handle literal comparisons
        if let AstExpressionValue::Literal(literal) = &condition.value {
            match (&condition.operator[..], literal) {
                ("=", AstLiteralValue::Boolean(b)) => Ok(Some(Value::Boolean(*b))),
                ("=", AstLiteralValue::Number(n)) => {
                    if let Ok(int_val) = n.parse::<i64>() {
                        Ok(Some(Value::Integer(int_val)))
                    } else {
                        Ok(None)
                    }
                }
                ("=", AstLiteralValue::String(s)) => Ok(Some(Value::Text(s.clone()))),
                ("=", AstLiteralValue::Null) => Ok(Some(Value::Null)),
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::query::sql::ast::{Condition, AstExpressionValue, AstLiteralValue};

    #[test]
    fn test_constant_boolean_folding() {
        let rule = ConstantFoldingRule;
        
        let condition = ConditionTree::And(
            Box::new(ConditionTree::Comparison(Condition {
                column: "test".to_string(),
                operator: "=".to_string(),
                value: AstExpressionValue::Literal(AstLiteralValue::Boolean(true)),
            })),
            Box::new(ConditionTree::Comparison(Condition {
                column: "test2".to_string(),
                operator: "=".to_string(),
                value: AstExpressionValue::Literal(AstLiteralValue::Boolean(false)),
            })),
        );
        
        let result = rule.apply(&condition).unwrap();
        
        // Should fold to false
        if let ConditionTree::Comparison(comp) = result {
            if let AstExpressionValue::Literal(AstLiteralValue::Boolean(val)) = comp.value {
                assert!(!val);
            }
        }
    }
}
