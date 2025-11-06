//! Functions to remove aliasing added by `DataFusion` when it converts
//! Arrow view columns back to their normal counterpart.
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
use arrow::datatypes::SchemaRef;
use datafusion::{
    common::tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    error::DataFusionError,
    physical_plan::{
        ExecutionPlan,
        projection::{ProjectionExec, ProjectionExpr},
    },
};
use std::{cmp::Reverse, sync::Arc};

/// Returns string from schema that most closely matches qualified name.
fn unalias(qualified_name: &str, original_schema: &SchemaRef) -> String {
    let mut col_names = original_schema
        .fields()
        .iter()
        .map(|f| f.name())
        .collect::<Vec<_>>();
    // Need keys in reverse length order
    // This ensures we find the longest matching suffix.
    col_names.sort_by_key(|s| Reverse(s.len()));
    // Find first that matches
    (*col_names
        .iter()
        .find(|&&s| qualified_name.ends_with(s))
        .expect("Can't find unaliased column name"))
    .to_string()
}

/// Unalias column names that were changed due to a [`ProjectionExec`].
///
/// Recursion stops after the first [`ProjectionExec`] has been found.
///
/// The Java Arrow FFI library can't handle view types, so we tell `DataFusion` to expand view types
/// in queries. However, the projection in the plan renames columns when we do this. This function
/// transforms a physical plan to remove that aliasing.
pub fn unalias_view_projection_columns(
    physical_plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    physical_plan
        // Recurse down plan looking for specific node
        .transform_down(|plan_node| {
            Ok(
                if let Some(projection) = plan_node.as_any().downcast_ref::<ProjectionExec>() {
                    // Schema of stage before projection
                    let schema = projection.input().schema();
                    // Unalias column names
                    let phys_exprs = projection
                        .expr()
                        .iter()
                        .map(|e| ProjectionExpr::new(e.expr.clone(), unalias(&e.alias, &schema)))
                        .collect::<Vec<_>>();

                    // Make replacement stage
                    let replacement =
                        ProjectionExec::try_new(phys_exprs, projection.input().clone())?;
                    // Stop searching down the query plan after making one replacement
                    Transformed::new(Arc::new(replacement), true, TreeNodeRecursion::Stop)
                } else {
                    Transformed::no(plan_node)
                },
            )
        })
        .map(|v| v.data)
}

#[cfg(test)]
mod tests {
    use crate::datafusion::unalias::{unalias, unalias_view_projection_columns};
    use arrow::{
        array::RecordBatch,
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::{
        catalog::memory::MemorySourceConfig,
        physical_plan::{
            PhysicalExpr, displayable, expressions::Column, projection::ProjectionExec,
        },
    };
    use std::sync::Arc;

    #[test]
    fn should_unalias_exact_match() {
        // Given
        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Int32, false),
            Field::new("col2", DataType::Utf8, false),
        ]));

        let qualified_name = "col1";

        // When
        let result = unalias(qualified_name, &schema);

        // Then
        assert_eq!(result, "col1");
    }

    #[test]
    fn should_unalias_suffix_match() {
        // Given
        let schema = Arc::new(Schema::new(vec![
            Field::new("field", DataType::Int32, false),
            Field::new("data", DataType::Utf8, false),
        ]));

        let qualified_name = "prefix_field";

        // When
        let result = unalias(qualified_name, &schema);

        // Then
        assert_eq!(result, "field");
    }

    #[test]
    fn should_unalias_multiple_suffix_candidates() {
        // Given
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("user_id", DataType::Int64, false),
        ]));

        // "prefix_user_id" ends with "user_id" and "id", it should pick "user_id" because of longer length
        let qualified_name = "prefix_user_id";

        // When
        let result = unalias(qualified_name, &schema);

        // Then
        assert_eq!(result, "user_id");
    }

    #[test]
    #[should_panic(expected = "Can't find unaliased column name")]
    fn should_panic_when_no_match() {
        // Given
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));

        let qualified_name = "unknown";

        // When
        // Then should panic
        let _ = unalias(qualified_name, &schema);
    }

    #[test]
    fn should_unalias_projection_columns_correctly() {
        // Given
        let schema_before_proj = Arc::new(Schema::new(vec![Field::new(
            "original_col",
            DataType::Int32,
            false,
        )]));

        let input_batch = RecordBatch::new_empty(schema_before_proj.clone());
        let input_exec = MemorySourceConfig::try_new_exec(
            &[vec![input_batch]],
            schema_before_proj.clone(),
            None,
        )
        .unwrap();

        // Projection with an alias in the column name
        let phys_exprs = vec![(
            Arc::new(Column::new("original_col", 0)) as Arc<dyn PhysicalExpr>,
            "alias_original_col".to_string(),
        )];

        // Now create a physical plan which includes the projection
        let physical_plan =
            Arc::new(ProjectionExec::try_new(phys_exprs.clone(), input_exec).unwrap());

        // When
        let result = unalias_view_projection_columns(physical_plan.clone());

        // Then
        let Ok(new_plan) = result else {
            panic!("Expected removal to succeed");
        };

        // The new projection should have unaliased column names matching original schema
        if let Some(p) = new_plan.as_any().downcast_ref::<ProjectionExec>() {
            let exprs = p.expr();
            // Alias should be removed to original_col
            assert_eq!(exprs[0].alias, "original_col");
        } else {
            panic!("Resulting plan node is not a ProjectionExec");
        }
    }

    #[test]
    fn should_return_same_plan_if_no_projection_exec_found() {
        // Given
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let input_batch = arrow::record_batch::RecordBatch::new_empty(schema.clone());
        let memory_exec =
            MemorySourceConfig::try_new_exec(&[vec![input_batch]], schema.clone(), None).unwrap();

        // When
        let result = unalias_view_projection_columns(memory_exec.clone());

        // Then
        let Ok(new_plan) = result else {
            panic!("Expected removal to succeed");
        };

        // Should be the same plan
        let orig_display = displayable(memory_exec.as_ref()).one_line();
        let new_display = displayable(new_plan.as_ref()).one_line();
        assert_eq!(format!("{orig_display}"), format!("{new_display}"));
    }

    #[test]
    fn should_stop_after_first_projection_exec() {
        // Given
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let input_batch = arrow::record_batch::RecordBatch::new_empty(schema.clone());
        let memory_exec =
            MemorySourceConfig::try_new_exec(&[vec![input_batch]], schema.clone(), None).unwrap();
        // Inner projection
        let phys_exprs_inner = vec![(
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
            "alias_a".to_string(),
        )];
        let inner_projection = ProjectionExec::try_new(phys_exprs_inner, memory_exec).unwrap();
        // Outer projection
        let phys_exprs_outer = vec![(
            Arc::new(Column::new("alias_a", 0)) as Arc<dyn PhysicalExpr>,
            "alias_alias_a".to_string(),
        )];
        let outer_projection =
            ProjectionExec::try_new(phys_exprs_outer, Arc::new(inner_projection)).unwrap();

        let physical_plan = Arc::new(outer_projection);

        // When
        let result = unalias_view_projection_columns(physical_plan.clone());

        // Then
        let Ok(new_plan) = result else {
            panic!("Expected removal to succeed");
        };

        // The top projection should be unaliased but inner remains aliased
        if let Some(top_proj) = new_plan.as_any().downcast_ref::<ProjectionExec>() {
            let exprs = top_proj.expr();
            assert_eq!(exprs[0].alias, "alias_a");
            // The input to this projection should still be inner_projection
            if let Some(inner_proj) = top_proj.input().as_any().downcast_ref::<ProjectionExec>() {
                // Inner projection remains unchanged
                let exprs = inner_proj.expr();
                assert_eq!(exprs[0].alias, "alias_a");
            } else {
                panic!("Inner projection should remain unchanged");
            }
        } else {
            panic!("Resulting plan node is not a ProjectionExec");
        }
    }
}
