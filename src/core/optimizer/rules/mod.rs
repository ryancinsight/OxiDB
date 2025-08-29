pub mod expression_evaluator;
pub use expression_evaluator::ExpressionEvaluator;

pub mod noop_filter_removal_rule;
pub use noop_filter_removal_rule::apply_noop_filter_removal_rule;

// For tests, to be created next
/// Contains unit tests for the optimizer rules.
mod tests;
