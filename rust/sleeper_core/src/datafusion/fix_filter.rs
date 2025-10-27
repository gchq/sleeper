//! Contains a workaround for filtering conditions bug. See <https://github.com/apache/datafusion/issues/18214>
//!
//! Tests also contains a 'fuse' test that will blow (fail) if the bug in `DataFusion` gets fixed so we can remove this workaround.
//! When this bug is fixed, we can remove this module.
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
    error::DataFusionError,
    execution::{SessionState, SessionStateBuilder},
    logical_expr::{Expr, Filter, LogicalPlan, Operator, TableScan},
};

/// Fixes the filtering conditions on a [`LogicalPlan`]. This is a workaround for `DataFusion` bug
/// <https://github.com/apache/datafusion/issues/18214>.
///
/// We use the unoptimised plan filter conditions in the repaired plan and also swap out filtering predicates
/// on a [`TableScan`] which would have been pushed down during logical plan optimization.
///
/// # Errors
/// If a transformation fails, then the appropriate error is returned.
pub fn fix_filter_exprs(
    optimised_plan: LogicalPlan,
    unoptimised_plan: &LogicalPlan,
    state: &SessionState,
) -> Result<LogicalPlan, DataFusionError> {
    let filters = collect_filters(unoptimised_plan)?;
    if filters.is_empty() {
        Ok(optimised_plan.clone())
    } else {
        // Create combined filter of all expressions with AND
        let combined_expr = filters
            .clone()
            .into_iter()
            .reduce(datafusion::prelude::Expr::and)
            .expect("Expression list should not be empty!");

        // Now traverse plan looking for Filter or TableScan items to replace
        let fixed_plan = optimised_plan
            .transform_down(|node| {
                Ok(match node {
                    LogicalPlan::Filter(filter) => Transformed::yes(LogicalPlan::Filter(
                        Filter::try_new(combined_expr.clone(), filter.input)?,
                    )),
                    LogicalPlan::TableScan(table_scan) => {
                        Transformed::yes(LogicalPlan::TableScan(TableScan::try_new(
                            table_scan.table_name,
                            table_scan.source,
                            table_scan.projection,
                            filters.clone(),
                            table_scan.fetch,
                        )?))
                    }
                    _ => Transformed::no(node),
                })
            })?
            .data;
        // Since this fix may also de-optimise some type conversions (e.g. Utf8 to Utf8View), we now re-optimise
        // the plan with the PushDownFilter optimizer rule disabled (to prevent it un-doing the fixes above)
        let mut state_builder = SessionStateBuilder::from(state.clone());
        if let Some(optimizer) = state_builder.optimizer() {
            optimizer
                .rules
                .retain(|rule| rule.name() != "push_down_filter");
        }
        let temp_state = state_builder.build();
        // Re-optimise plan
        temp_state.optimize(&fixed_plan)
    }
}

/// Collect all the [`Expr`]s that are used in filter conditions from any [`LogicalPlan::Filter`]
/// nodes in this plan.
///
/// # Errors
/// Will fail if tree node recursion fails in plan traversal.
fn collect_filters(plan: &LogicalPlan) -> Result<Vec<Expr>, DataFusionError> {
    let mut filters = vec![];
    plan.apply(|node| {
        if let LogicalPlan::Filter(filter) = node {
            recurse_binary_exprs(filter.predicate.clone(), &mut filters);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(filters)
}

// Recursively add binary expression to the list of [`Expr`]s
// and recurse into AND operators if they are `BinaryExpr`.
fn recurse_binary_exprs(expr: Expr, filters: &mut Vec<Expr>) {
    match expr {
        Expr::BinaryExpr(bin_expr) if matches!(bin_expr.op, Operator::And) => {
            recurse_binary_exprs(*bin_expr.left, filters);
            recurse_binary_exprs(*bin_expr.right, filters);
        }
        _ => {
            filters.push(expr);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::datafusion::fix_filter::{collect_filters, fix_filter_exprs};
    use datafusion::{
        common::tree_node::TreeNode, dataframe::DataFrameWriteOptions, error::DataFusionError,
        logical_expr::LogicalPlan, prelude::*, scalar::ScalarValue,
    };
    use tempfile::{TempDir, tempdir};

    #[test]
    pub fn should_return_empty_filters() -> Result<(), DataFusionError> {
        // Given
        let df = dataframe![ "a" => [1]]?.sort(vec![col("a").sort(true, false)])?;

        // When
        let filters = collect_filters(df.logical_plan())?;

        // Then
        assert!(filters.is_empty());
        Ok(())
    }

    #[test]
    pub fn should_return_single_filter() -> Result<(), DataFusionError> {
        // Given
        let df = dataframe![ "a" => [1]]?
            .filter(col("a").gt(lit(5)))?
            .sort(vec![col("a").sort(true, false)])?;

        // When
        let filters = collect_filters(df.logical_plan())?;

        // Then
        assert_eq!(filters.len(), 1);
        assert_eq!(
            filters[0],
            col(Column::new(Some("?table?"), "a")).gt(lit(5))
        );
        Ok(())
    }

    #[test]
    pub fn should_return_multiple_filters() -> Result<(), DataFusionError> {
        // Given
        let df = dataframe![
            "a" => [1],
            "b" => ["test"]
        ]?
        .filter(col("a").gt(lit(5)))?
        .filter(col("b").lt_eq(lit("value")))?
        .sort(vec![col("a").sort(true, false)])?;

        // When
        let filters = collect_filters(df.logical_plan())?;

        // Then
        assert_eq!(filters.len(), 2);
        assert!(filters.contains(&col(Column::new(Some("?table?"), "a")).gt(lit(5))));
        assert!(filters.contains(&col(Column::new(Some("?table?"), "b")).lt_eq(lit("value"))));
        Ok(())
    }

    #[test]
    pub fn should_return_multiple_filters_break_apart_conjunctions() -> Result<(), DataFusionError>
    {
        // Given
        let df = dataframe![
            "a" => [1],
            "b" => ["test"]
        ]?
        .filter(col("a").gt(lit(5)))?
        .filter(
            col("a")
                .gt(lit(1))
                .and(col("a").lt(lit(6)))
                .and(col("a").not_eq(lit(4))),
        )?
        .sort(vec![col("a").sort(true, false)])?;

        // When
        let filters = collect_filters(df.logical_plan())?;

        // Then
        assert_eq!(filters.len(), 4);
        assert!(filters.contains(&col(Column::new(Some("?table?"), "a")).gt(lit(5))));
        assert!(filters.contains(&col(Column::new(Some("?table?"), "a")).gt(lit(1))));
        assert!(filters.contains(&col(Column::new(Some("?table?"), "a")).lt(lit(6))));
        assert!(filters.contains(&col(Column::new(Some("?table?"), "a")).not_eq(lit(4))));

        Ok(())
    }

    #[test]
    pub fn should_return_multiple_filters_keep_disjunction() -> Result<(), DataFusionError> {
        // Given
        let df = dataframe![
            "a" => [1],
            "b" => ["test"]
        ]?
        .filter(col("a").gt(lit(5)))?
        .filter(col("a").gt(lit(1)).or(col("a").lt(lit(6))))?
        .sort(vec![col("a").sort(true, false)])?;

        // When
        let filters = collect_filters(df.logical_plan())?;

        // Then
        assert_eq!(filters.len(), 2);
        assert!(filters.contains(&col(Column::new(Some("?table?"), "a")).gt(lit(5))));
        assert!(
            filters.contains(
                &col(Column::new(Some("?table?"), "a"))
                    .gt(lit(1))
                    .or(col(Column::new(Some("?table?"), "a")).lt(lit(6)))
            )
        );

        Ok(())
    }

    #[tokio::test]
    pub async fn should_not_alter_plan_no_filters() -> Result<(), DataFusionError> {
        let tmpdir = tempdir().expect("Couldn't create temp directory");
        // Given
        let ctx = SessionContext::new();
        // Write to an actual Parquet file so the TableScan in plan will support predicate pushdown
        let path = write_test_parquet(&tmpdir).await?;
        let df = ctx
            .read_parquet(&path, ParquetReadOptions::default())
            .await?
            .sort(vec![col("a").sort(true, false)])?;

        let (state, plan) = df.into_parts();
        let optimised_plan = state.optimize(&plan)?;

        // When
        let fixed_plan = fix_filter_exprs(optimised_plan.clone(), &plan, &state)?;

        // Then
        assert_eq!(optimised_plan, fixed_plan);
        Ok(())
    }

    #[tokio::test]
    pub async fn should_return_plan_single_filter() -> Result<(), DataFusionError> {
        let tmpdir = tempdir().expect("Couldn't create temp directory");
        // Given
        let ctx = SessionContext::new();
        // Write to an actual Parquet file so the TableScan in plan will support predicate pushdown
        let path = write_test_parquet(&tmpdir).await?;
        let df = ctx
            .read_parquet(&path, ParquetReadOptions::default())
            .await?
            .filter(col("a").gt(lit(5)).and(col("a").lt_eq(lit(15))))?
            .sort(vec![col("a").sort(true, false)])?;

        let (state, plan) = df.into_parts();
        let optimised_plan = state.optimize(&plan)?;

        // When
        let fixed_plan = fix_filter_exprs(optimised_plan.clone(), &plan, &state)?;

        // Then
        assert_eq!(optimised_plan, fixed_plan);
        Ok(())
    }

    #[tokio::test]
    pub async fn should_return_plan_multiple_filter() -> Result<(), DataFusionError> {
        let tmpdir = tempdir().expect("Couldn't create temp directory");
        // Given
        let ctx = SessionContext::new();
        // Write to an actual Parquet file so the TableScan in plan will support predicate pushdown
        let path = write_test_parquet(&tmpdir).await?;
        let df = ctx
            .read_parquet(&path, ParquetReadOptions::default())
            .await?
            .filter(col("a").gt(lit(1)).and(col("a").lt(lit(10))))?
            .filter(col("a").gt_eq(lit(1)).and(col("a").lt_eq(lit(10))))?
            .filter(col("b").gt(lit("aaa")))?
            .sort(vec![col("a").sort(true, false)])?;

        let (state, plan) = df.into_parts();
        let optimised_plan = state.optimize(&plan)?;

        // When
        let fixed_plan = fix_filter_exprs(optimised_plan.clone(), &plan, &state)?;

        // Then
        // Manually inspect the transformed plan
        let check_filter = fixed_plan.exists(|node| {
            Ok(match node {
                LogicalPlan::Filter(filter) => {
                    let expected = col(Column::new(Some("?table?"), "b"))
                        .gt(Expr::Literal(
                            ScalarValue::Utf8View(Some("aaa".into())),
                            None,
                        ))
                        .and(col(Column::new(Some("?table?"), "a")).gt_eq(lit(1)))
                        .and(col(Column::new(Some("?table?"), "a")).lt_eq(lit(10)))
                        .and(col(Column::new(Some("?table?"), "a")).gt(lit(1)))
                        .and(col(Column::new(Some("?table?"), "a")).lt(lit(10)));
                    filter.predicate == expected
                }
                _ => false,
            })
        })?;
        assert!(check_filter, "Filter expression not correct");

        let check_table_scan = fixed_plan.exists(|node| {
            Ok(match node {
                LogicalPlan::TableScan(table_scan) => {
                    let filter_exprs = vec![
                        col(Column::new(Some("?table?"), "b")).gt(Expr::Literal(
                            ScalarValue::Utf8View(Some("aaa".into())),
                            None,
                        )),
                        col(Column::new(Some("?table?"), "a")).gt_eq(lit(1)),
                        col(Column::new(Some("?table?"), "a")).lt_eq(lit(10)),
                        col(Column::new(Some("?table?"), "a")).gt(lit(1)),
                        col(Column::new(Some("?table?"), "a")).lt(lit(10)),
                    ];
                    table_scan.filters == filter_exprs
                }
                _ => false,
            })
        })?;
        assert!(check_table_scan, "TableScan expression not correct");

        Ok(())
    }

    async fn write_test_parquet(t: &TempDir) -> Result<String, DataFusionError> {
        let df = dataframe![
            "a" => [1, 2, 3],
            "b" => ["test", "val1", "val2"]
        ]?;
        let path = t
            .path()
            .join("test.parquet")
            .to_str()
            .expect("Couldn't convert path")
            .to_owned();
        df.write_parquet(&path, DataFrameWriteOptions::default(), None)
            .await?;
        Ok(path)
    }

    // Fuse test. This test is designed to fail once the filtering bug is fixed in DataFusion.
    #[test]
    fn fuse_should_break_when_bug_fixed() -> Result<(), DataFusionError> {
        // Given
        let df = dataframe!["a" => [1,2,3,4], "b" => ["1","2","3","4"]]?;
        let df = df.filter(col("a").gt(lit(1)).and(col("a").lt(lit(10))))?;
        let df = df.filter(col("a").gt_eq(lit(1)).and(col("a").lt_eq(lit(10))))?;
        // Convert to logical plan
        let (state, plan) = df.into_parts();
        let expected = col(Column::new(Some("?table?"), "a"))
            .gt_eq(lit(1))
            .and(col(Column::new(Some("?table?"), "a")).lt_eq(lit(10)));

        // When
        let optimised_plan = state.optimize(&plan)?;

        // Then
        // Look for Filter with wrong condition
        let bug_occurred = optimised_plan.exists(|node| {
            if let LogicalPlan::Filter(filter) = node {
                let expr = &filter.predicate;
                Ok(expr == &expected)
            } else {
                Ok(false)
            }
        })?;

        assert!(
            bug_occurred,
            "Couldn't find expected Filter condition. Has bug https://github.com/apache/datafusion/issues/18214 been fixed?"
        );
        Ok(())
    }
}
