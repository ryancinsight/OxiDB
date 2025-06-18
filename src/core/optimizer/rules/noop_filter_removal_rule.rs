use crate::core::optimizer::QueryPlanNode;

pub fn apply_noop_filter_removal_rule(plan: QueryPlanNode) -> QueryPlanNode {
    plan
}
