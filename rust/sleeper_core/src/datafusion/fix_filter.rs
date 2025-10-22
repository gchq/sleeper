//! Contains a workaround for filtering conditions bug. See <https://github.com/apache/datafusion/issues/18214>
//!
//! Tests also contains a 'fuse' test that will blow (fail) if the bug in DataFusion gets fixed so we can remove this workaround.
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
    common::tree_node::{TreeNode, TreeNodeRecursion},
    error::DataFusionError,
    logical_expr::{Expr, LogicalPlan},
};

pub async fn fix_filter_exprs(
    incorrect_plan: &LogicalPlan,
    unoptimised_plan: &LogicalPlan,
) -> Result<LogicalPlan, DataFusionError> {
    let filters = collect_filters(unoptimised_plan);

    Ok(incorrect_plan.clone())
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
            filters.push(filter.predicate.clone())
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(filters)
}

#[cfg(test)]
mod tests {
    use crate::datafusion::fix_filter::collect_filters;
    use datafusion::dataframe;
    use datafusion::error::DataFusionError;
    use datafusion::prelude::*;

    #[test]
    pub fn should_return_empty_filters() -> Result<(), DataFusionError> {
        // Given
        let df = dataframe![ "a"=> [1]]?.sort(vec![col("a").sort(true, false)])?;

        // When
        let filters = collect_filters(df.logical_plan())?;

        // Then
        assert!(filters.is_empty());
        Ok(())
    }

    #[test]
    pub fn should_return_single_filter() -> Result<(), DataFusionError> {
        // Given
        let df = dataframe![ "a"=> [1]]?
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
}
