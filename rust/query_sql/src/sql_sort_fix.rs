//! Contains code to replace the sort stage that incorrectly drops when we add an SQL query to the end of a Sleeper query.
/*
* Copyright 2022-2025 Crown Copyright
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
use datafusion::{
    common::tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    error::Result,
    logical_expr::{LogicalPlan, Sort, SortExpr},
    prelude::DataFrame,
};
use std::{cell::RefCell, sync::Arc};

/// Insert sort stage back into logical plan for a query.
///
/// Searches bottom-up for the first non-Filter node that has a Filter child,
/// then inserts a Sort stage between them in a single pass.
///
/// # Errors
/// If the plan transformation can't proceed for some reason.
pub fn inject_sort_stage(frame: DataFrame, expr: Vec<SortExpr>) -> Result<DataFrame> {
    let (state, plan) = frame.into_parts();
    // RefCell allows moving `expr` out of the FnMut closure. We stop traversal after
    // the first match so the closure only fires once, but rustc can't prove that.
    let expr = RefCell::new(expr);

    let new_plan = plan
        .transform_up(|node| {
            if matches!(node, LogicalPlan::Filter(_)) {
                return Ok(Transformed::no(node));
            }
            if node.inputs().len() != 1 || !matches!(node.inputs()[0], LogicalPlan::Filter(_)) {
                return Ok(Transformed::no(node));
            }
            let sort_node = LogicalPlan::Sort(Sort {
                expr: expr.take(),
                input: Arc::new(node.inputs()[0].clone()),
                fetch: None,
            });
            let new_node = node.with_new_exprs(node.expressions(), vec![sort_node])?;
            Ok(Transformed::new(new_node, true, TreeNodeRecursion::Stop))
        })?
        .data;

    Ok(DataFrame::new(state, new_plan))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::{
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        },
        execution::context::SessionContext,
        logical_expr::{col, lit},
    };

    fn create_test_context() -> SessionContext {
        SessionContext::new()
    }

    fn create_sort_exprs() -> Vec<SortExpr> {
        vec![col("a").sort(true, true)]
    }

    fn create_simple_dataframe(ctx: &SessionContext) -> DataFrame {
        ctx.read_empty().unwrap()
    }

    fn create_dataframe_with_filter(ctx: &SessionContext) -> DataFrame {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        ctx.read_batch(RecordBatch::new_empty(Arc::new(schema)))
            .unwrap()
            .filter(col("a").gt(lit(0)))
            .unwrap()
    }

    fn create_dataframe_with_filter_and_projection(ctx: &SessionContext) -> DataFrame {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        ctx.read_batch(RecordBatch::new_empty(Arc::new(schema)))
            .unwrap()
            .filter(col("a").gt(lit(0)))
            .unwrap()
            .select(vec![col("a")])
            .unwrap()
    }

    fn create_dataframe_with_nested_filters(ctx: &SessionContext) -> DataFrame {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        ctx.read_batch(RecordBatch::new_empty(Arc::new(schema)))
            .unwrap()
            .filter(col("a").gt(lit(0)))
            .unwrap()
            .filter(col("a").lt(lit(100)))
            .unwrap()
    }

    fn count_sort_nodes(plan: &LogicalPlan) -> usize {
        let self_count = usize::from(matches!(plan, LogicalPlan::Sort(_)));
        self_count
            + plan
                .inputs()
                .iter()
                .map(|p| count_sort_nodes(p))
                .sum::<usize>()
    }

    fn find_sort(plan: &LogicalPlan) -> Option<&Sort> {
        if let LogicalPlan::Sort(sort) = plan {
            return Some(sort);
        }
        plan.inputs().iter().find_map(|p| find_sort(p))
    }

    #[test]
    fn should_inject_sort_between_projection_and_filter() {
        // Given
        let ctx = create_test_context();
        let frame = create_dataframe_with_filter_and_projection(&ctx);
        let sort_exprs = create_sort_exprs();

        // Verify no Sort node exists before injection
        let (_, plan_before) = frame.clone().into_parts();
        assert_eq!(count_sort_nodes(&plan_before), 0);

        // When
        let result = inject_sort_stage(frame, sort_exprs).unwrap();

        // Then
        let (_, plan_after) = result.into_parts();
        assert_eq!(count_sort_nodes(&plan_after), 1);

        // Verify sort is in correct place: Projection -> Sort -> Filter
        assert!(
            matches!(plan_after, LogicalPlan::Projection(_)),
            "Root should be Projection"
        );
        let projection_input = &plan_after.inputs()[0];
        assert!(
            matches!(projection_input, LogicalPlan::Sort(_)),
            "Projection child should be Sort"
        );
        let sort_input = &projection_input.inputs()[0];
        assert!(
            matches!(sort_input, LogicalPlan::Filter(_)),
            "Sort child should be Filter"
        );
    }

    #[test]
    fn should_place_sort_above_filter() {
        // Given
        let ctx = create_test_context();
        let frame = create_dataframe_with_filter_and_projection(&ctx);
        let sort_exprs = create_sort_exprs();

        // When
        let result = inject_sort_stage(frame, sort_exprs).unwrap();

        // Then
        let (_, plan) = result.into_parts();
        let sort = find_sort(&plan).expect("Sort node should exist");
        assert!(
            matches!(sort.input.as_ref(), LogicalPlan::Filter(_)),
            "Sort child should be Filter"
        );
    }

    #[test]
    fn should_not_inject_sort_when_only_nested_filters() {
        // Given - nested filters with no non-Filter node above them
        let ctx = create_test_context();
        let frame = create_dataframe_with_nested_filters(&ctx);
        let sort_exprs = create_sort_exprs();

        // When
        let result = inject_sort_stage(frame, sort_exprs).unwrap();

        // Then - no sort should be added since root is a Filter
        let (_, plan) = result.into_parts();
        assert_eq!(count_sort_nodes(&plan), 0);
    }

    #[test]
    fn should_not_modify_plan_without_filter() {
        // Given
        let ctx = create_test_context();
        let frame = create_simple_dataframe(&ctx);
        let sort_exprs = create_sort_exprs();

        // Verify no Sort node exists before
        let (_, plan_before) = frame.clone().into_parts();
        assert_eq!(count_sort_nodes(&plan_before), 0);

        // When
        let result = inject_sort_stage(frame, sort_exprs).unwrap();

        // Then - no sort should be added since there's no filter
        let (_, plan_after) = result.into_parts();
        assert_eq!(count_sort_nodes(&plan_after), 0);
    }

    #[test]
    fn should_handle_empty_sort_expressions() {
        // Given
        let ctx = create_test_context();
        let frame = create_dataframe_with_filter_and_projection(&ctx);
        let sort_exprs: Vec<SortExpr> = vec![];

        // When
        let result = inject_sort_stage(frame, sort_exprs).unwrap();

        // Then - sort node should still be injected, just with empty expressions
        let (_, plan) = result.into_parts();
        assert_eq!(count_sort_nodes(&plan), 1);

        // Verify the sort node has empty expressions
        let sort = find_sort(&plan).expect("Sort node should exist");
        assert!(sort.expr.is_empty());
    }

    #[test]
    fn should_handle_multiple_sort_expressions() {
        // Given
        let ctx = create_test_context();
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Utf8, false),
        ]);
        let frame = ctx
            .read_batch(RecordBatch::new_empty(Arc::new(schema)))
            .unwrap()
            .filter(col("a").gt(lit(0)))
            .unwrap()
            .select(vec![col("a"), col("b")])
            .unwrap();

        let sort_exprs = vec![col("a").sort(true, true), col("b").sort(false, false)];

        // When
        let result = inject_sort_stage(frame, sort_exprs).unwrap();

        // Then
        let (_, plan) = result.into_parts();
        assert_eq!(count_sort_nodes(&plan), 1);

        // Find the sort node and verify it has 2 expressions
        let sort = find_sort(&plan).expect("Sort node should exist");
        let sort_exprs = &sort.expr;
        assert_eq!(sort_exprs.len(), 2);

        // Verify first sort expr: col("a") ASC NULLS FIRST
        assert_eq!(sort_exprs[0].expr.schema_name().to_string(), "a");
        assert!(sort_exprs[0].asc);
        assert!(sort_exprs[0].nulls_first);

        // Verify second sort expr: col("b") DESC NULLS LAST
        assert_eq!(sort_exprs[1].expr.schema_name().to_string(), "b");
        assert!(!sort_exprs[1].asc);
        assert!(!sort_exprs[1].nulls_first);
    }

    #[test]
    fn should_only_inject_one_sort_even_with_multiple_filter_boundaries() {
        // Given - a plan with projection -> filter -> filter -> table scan
        let ctx = create_test_context();
        let frame = create_dataframe_with_nested_filters(&ctx)
            .select(vec![col("a")])
            .unwrap();
        let sort_exprs = create_sort_exprs();

        // When
        let result = inject_sort_stage(frame, sort_exprs).unwrap();

        // Then - only one sort should be injected (at the topmost filter boundary)
        let (_, plan) = result.into_parts();
        assert_eq!(
            count_sort_nodes(&plan),
            1,
            "Should inject exactly one Sort node"
        );

        // Verify sort is between projection and first filter
        // Plan should be: Projection -> Sort -> Filter -> Filter -> TableScan
        assert!(
            matches!(plan, LogicalPlan::Projection(_)),
            "Root should be Projection"
        );
        let projection_input = &plan.inputs()[0];
        assert!(
            matches!(projection_input, LogicalPlan::Sort(_)),
            "Projection child should be Sort"
        );
        let sort_input = &projection_input.inputs()[0];
        assert!(
            matches!(sort_input, LogicalPlan::Filter(_)),
            "Sort child should be Filter"
        );
    }

    #[test]
    fn should_preserve_dataframe_state() {
        // Given
        let ctx = create_test_context();
        let frame = create_dataframe_with_filter_and_projection(&ctx);
        let schema_before = frame.schema().clone();
        let sort_exprs = create_sort_exprs();

        // When
        let result = inject_sort_stage(frame, sort_exprs).unwrap();

        // Then
        let schema_after = result.schema().clone();
        assert_eq!(schema_before, schema_after);
    }

    #[test]
    fn should_handle_filter_as_root_node() {
        // Given - filter is the root node (no projection on top)
        let ctx = create_test_context();
        let frame = create_dataframe_with_filter(&ctx);
        let sort_exprs = create_sort_exprs();

        // When
        let result = inject_sort_stage(frame, sort_exprs).unwrap();

        // Then - no sort should be added since filter is root
        // (there's no non-Filter node with Filter child)
        let (_, plan) = result.into_parts();
        // Filter as root means there's no parent to insert between
        assert_eq!(count_sort_nodes(&plan), 0);
    }

    #[test]
    fn should_inject_sort_at_deepest_non_filter_to_filter_boundary() {
        // Given - plan: Projection -> Filter -> Limit -> Filter -> Filter -> TableScan
        let ctx = create_test_context();
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        let frame = ctx
            .read_batch(RecordBatch::new_empty(Arc::new(schema)))
            .unwrap()
            .filter(col("a").gt(lit(0))) // Filter3 (deepest)
            .unwrap()
            .filter(col("a").lt(lit(100))) // Filter2
            .unwrap()
            .limit(0, Some(10)) // Limit
            .unwrap()
            .filter(col("a").gt(lit(5))) // Filter
            .unwrap()
            .select(vec![col("a")]) // Projection
            .unwrap();
        let sort_exprs = create_sort_exprs();

        // When
        let result = inject_sort_stage(frame, sort_exprs).unwrap();

        // Then
        let (_, plan) = result.into_parts();
        assert_eq!(count_sort_nodes(&plan), 1);

        // Verify structure: Projection -> Filter -> Limit -> Sort -> Filter -> Filter -> TableScan
        // Sort should be inserted between Limit (NonFilter2) and Filter2
        assert!(
            matches!(plan, LogicalPlan::Projection(_)),
            "Root should be Projection"
        );
        let proj_input = &plan.inputs()[0];
        assert!(
            matches!(proj_input, LogicalPlan::Filter(_)),
            "Projection child should be Filter"
        );
        let filter_input = &proj_input.inputs()[0];
        assert!(
            matches!(filter_input, LogicalPlan::Limit(_)),
            "Filter child should be Limit"
        );
        let limit_input = &filter_input.inputs()[0];
        assert!(
            matches!(limit_input, LogicalPlan::Sort(_)),
            "Limit child should be Sort"
        );
        let sort_input = &limit_input.inputs()[0];
        assert!(
            matches!(sort_input, LogicalPlan::Filter(_)),
            "Sort child should be Filter"
        );
    }
}
