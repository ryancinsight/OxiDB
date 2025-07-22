use crate::core::optimizer::QueryPlanNode;

#[must_use] pub const fn apply_noop_filter_removal_rule(plan: QueryPlanNode) -> QueryPlanNode {
    plan
}
