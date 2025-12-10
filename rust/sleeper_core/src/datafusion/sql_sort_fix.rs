//! Contains code to replace the sort stage that DataFrame as SQL table functionality drops.
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

use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    error::Result,
    logical_expr::{LogicalPlan, Sort, SortExpr},
    prelude::DataFrame,
};

/// Insert sort stage back into logical plan for a query.
///
/// We search the query plan bottom up, looking for the first non-Filter stage (1) that has a Filter stage (2)
/// as input (i.e. direct child). On the second pass, we insert a Sort stage betweem (1) and (2).
pub fn inject_sort_stage(frame: DataFrame, mut sorting_exprs: Vec<SortExpr>) -> Result<DataFrame> {
    let (state, plan) = frame.into_parts();
    // Look for last "Filter" stage working up from bottom of plan.
    // Since we can't find out who the "parent" plan stage is in advance and it
    // seems we can't easily duplicate the stage under examination, we scan upwards
    // and count stages
    let mut stage_found = false;
    let mut stage_no = 0usize;
    let mut new_plan = plan
        .transform_up(|node| {
            Ok(
                // if we are a non filter node with a filter node child, then stop
                if !matches!(node, LogicalPlan::Filter(_))
                    && node.inputs().len() == 1
                    && matches!(node.inputs()[0], LogicalPlan::Filter(_))
                {
                    stage_found = true;
                    stage_no -= 1;
                    Transformed::new(node, false, TreeNodeRecursion::Stop)
                } else {
                    stage_no += 1;
                    Transformed::no(node)
                },
            )
        })?
        .data;
    if stage_found {
        // Now we can scan again, stop in the right plan stage and inject the new sort stage
        new_plan = new_plan
            .transform_up(|node| {
                Ok(if stage_no == 0 {
                    let new_node = LogicalPlan::Sort(Sort {
                        expr: std::mem::take(&mut sorting_exprs),
                        input: Arc::new(node),
                        fetch: None,
                    });
                    Transformed::complete(new_node)
                } else {
                    stage_no -= 1;
                    Transformed::no(node)
                })
            })?
            .data;
    }
    Ok(DataFrame::new(state, new_plan))
}
