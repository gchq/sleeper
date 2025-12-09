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

use datafusion::{
    common::tree_node::{Transformed, TreeNodeRewriter},
    error::{DataFusionError, Result},
    logical_expr::LogicalPlan,
    prelude::DataFrame,
};

struct FilterFinder {
    parent_node_last_filter: bool,
}

impl Default for FilterFinder {
    fn default() -> Self {
        Self {
            parent_node_last_filter: false,
        }
    }
}

impl TreeNodeRewriter for FilterFinder {
    type Node = LogicalPlan;
    fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        // Just record if this node type is a Filter
        self.parent_node_last_filter = matches!(node, LogicalPlan::Filter(_));
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        // If is a filter, but parent is not
        Ok(
            if !self.parent_node_last_filter && matches!(node, LogicalPlan::Filter(_)) {
            } else {
                Transformed::no(node)
            },
        )
    }
}

pub fn replace_sort_stage(frame: DataFrame) -> Result<DataFrame> {
    let (state, plan) = frame.into_parts();
    // Look for last "Filter" stage working up from bottom of plan
    let new_plan = plan
        .rewrite_with_subqueries(&mut FilterFinder::default())
        .map(|v| v.data)?;
    Ok(DataFrame::new(state, new_plan))
}
