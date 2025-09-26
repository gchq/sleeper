//! Sleeper `DataFusion` utility functions.
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
use arrow::{datatypes::SchemaRef, util::pretty::pretty_format_batches};
use datafusion::{
    common::{
        plan_err,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    config::ExecutionOptions,
    dataframe::DataFrame,
    error::DataFusionError,
    execution::{SessionStateBuilder, context::SessionContext},
    physical_expr::LexOrdering,
    physical_plan::{
        ExecutionPlan, Partitioning, accept,
        coalesce_partitions::CoalescePartitionsExec,
        projection::ProjectionExec,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
    },
};
use log::info;
use num_format::{Locale, ToFormattedString};
use objectstore_ext::s3::ObjectStoreFactory;
use std::{cmp::Reverse, sync::Arc};
use url::Url;

use crate::datafusion::metrics::RowCounts;

/// Maximum number of file upload parts to generate. Implementation will
/// try to match this, but may not get there exactly.
pub const MAX_PART_COUNT: u64 = 5000;

/// Write explanation of logical query plan to log output.
///
/// # Errors
/// If explanation fails.
pub async fn explain_plan(frame: &DataFrame) -> Result<(), DataFusionError> {
    let mut config = frame.task_ctx().session_config().clone();
    // Ensure physical plan output is disabled
    config.options_mut().explain.logical_plan_only = true;
    let explained = DataFrame::new(
        SessionStateBuilder::new_with_default_features()
            .with_config(config)
            .build(),
        frame.logical_plan().clone(),
    )
    .explain(false, false)?
    .collect()
    .await?;
    info!("DataFusion plan:\n{}", pretty_format_batches(&explained)?);
    Ok(())
}

/// Calculate the upload size based on the total input data size. This prevents uploads to S3 failing due to uploading
/// too many small parts. We conseratively set the upload size so that fewer, larger uploads are created.
pub fn calculate_upload_size(total_input_size: u64) -> Result<usize, DataFusionError> {
    let upload_size = std::cmp::max(
        ExecutionOptions::default().objectstore_writer_buffer_size,
        usize::try_from(total_input_size / MAX_PART_COUNT)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );
    info!(
        "Use upload buffer of {} bytes.",
        upload_size.to_formatted_string(&Locale::en)
    );
    Ok(upload_size)
}

/// Checks if a physical plan contains a `SortExec` stage.
///
/// We must ensure the physical plans don't contains a full sort stage as this entails
/// reading all input data before performing a sort which requires enough memory/disk space
/// to contain all the data. By ensuring plans only contain streaming merge sort stages we
/// will get a streaming merge of records for compaction.
///
/// # Errors
/// If a `SortExec` stage is found in the given plan.
pub fn check_for_sort_exec(plan: &Arc<dyn ExecutionPlan>) -> Result<(), DataFusionError> {
    let contains_sort_exec =
        plan.exists(|node| Ok(node.as_any().downcast_ref::<SortExec>().is_some()))?;
    if contains_sort_exec {
        plan_err!("Physical plan contains SortExec stage. Please file a bug report.")
    } else {
        Ok(())
    }
}

/// Takes the urls in `input_paths` list and `output_path`
/// and registers the appropriate [`object_store::ObjectStore`] for it.
///
/// # Errors
/// If we can't create an [`object_store::ObjectStore`] for a known URL then this will fail.
///
pub fn register_store(
    store_factory: &ObjectStoreFactory,
    input_paths: &[Url],
    output_path: Option<&Url>,
    ctx: &SessionContext,
) -> Result<(), DataFusionError> {
    for input_path in input_paths {
        let in_store = store_factory
            .get_object_store(input_path)
            .map_err(|e| DataFusionError::External(e.into()))?;
        ctx.runtime_env()
            .register_object_store(input_path, in_store);
    }

    if let Some(output) = output_path {
        let out_store = store_factory
            .get_object_store(output)
            .map_err(|e| DataFusionError::External(e.into()))?;
        ctx.runtime_env().register_object_store(output, out_store);
    }
    Ok(())
}

/// Calculate the total size of all `input_paths` objects.
///
/// # Errors
/// Fails if we can't obtain the size of the input files from the object store.
pub async fn retrieve_input_size(
    input_paths: &[Url],
    store_factory: &ObjectStoreFactory,
) -> Result<u64, DataFusionError> {
    let mut total_input = 0u64;
    for input_path in input_paths {
        let store = store_factory
            .get_object_store(input_path)
            .map_err(|e| DataFusionError::External(e.into()))?;
        let p = input_path.path();
        total_input += store.head(&p.into()).await?.size;
    }
    Ok(total_input)
}

/// Searches down a physical plan and removes the top most [`CoalescePartitionsExec`] stage.
/// The stage is replaced with a [`SortPreservingMergeExec`] stage.
///
/// This is a workaround for a sorting issue. After a [`SortPreservingMergeExec`] stage, if
/// `DataFusion` parallelised the output into multiple streams, for example to make a projection
/// stage quicker, it uses a [`CoalescePartitionsExec`] stage afterwards to re-merge the separate
/// streams. This forgets the ordering of chunks of data, leading to them be re-merged out of order,
/// thus breaking the overall order.
///
/// After the first [`CoalescePartitionsExec`] has been found, this function will not recurse further down the tree.
///
/// For example, if we had chunks `[A-F], [G-H], [I-M], ...` they might be parallelised into 2 streams for some
/// other unrelated plan stage. Afterwards, they might be re-merged to `[A-F], [I-M], [G-H], ...` due to
/// the coalescing stage not knowing the correct order. By using a secong sort preserving merge instead, we
/// are guaranteed the chunks are sequenced back into a single stream with the correct ordering. Since the individual
/// chunks are sorted, then this re-merging operation is exceedingly quick.
///
/// # Errors
/// If the transformation walking of the tree fails, an error results.
pub fn remove_coalesce_physical_stage(
    ordering: &LexOrdering,
    physical_plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    physical_plan
        // Recurse down plan looking for specific node
        .transform_down(|plan_node| {
            Ok(
                if let Some(coalesce) = plan_node.as_any().downcast_ref::<CoalescePartitionsExec>()
                {
                    // Swap it out for a SortPreservingMergeExec
                    let replacement =
                        SortPreservingMergeExec::new(ordering.clone(), coalesce.input().clone());
                    // Stop searching down the query plan after making one replacement
                    Transformed::new(Arc::new(replacement), true, TreeNodeRecursion::Stop)
                } else {
                    Transformed::no(plan_node)
                },
            )
        })
        .map(|v| v.data)
}

/// Returns string from schema that most closely matches qualified name.
pub fn unalias(qualified_name: &str, original_schema: &SchemaRef) -> String {
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
                        .map(|(expr, name)| (expr.clone(), unalias(name, &schema)))
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

/// Returns the number of output partitions for a given physical execution plan.
///
/// This inspects the output partitioning property of the plan and returns the partition count.
pub fn output_partition_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let partitions = plan.properties().output_partitioning();
    match partitions {
        Partitioning::Hash(_, count)
        | Partitioning::RoundRobinBatch(count)
        | Partitioning::UnknownPartitioning(count) => *count,
    }
}

/// Collect statistics from an executed physical plan.
///
/// The rows read and written are returned in the [`RowCounts`] object.
///
/// # Errors
/// The physical plan accepts a visitor to collect the statistics, if this can't be done
/// an error will be returned.
pub fn collect_stats(
    input_paths: &[Url],
    physical_plan: &Arc<dyn ExecutionPlan>,
) -> Result<RowCounts, DataFusionError> {
    let mut stats = RowCounts::new(input_paths);
    accept(physical_plan.as_ref(), &mut stats)?;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use crate::datafusion::util::{
        remove_coalesce_physical_stage, unalias, unalias_view_projection_columns,
    };
    use arrow::{
        array::RecordBatch,
        compute::SortOptions,
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::physical_plan::PhysicalExpr;
    use datafusion::{
        catalog::memory::MemorySourceConfig,
        common::tree_node::TreeNode,
        physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column},
        physical_plan::{
            coalesce_partitions::CoalescePartitionsExec, displayable, projection::ProjectionExec,
            sorts::sort_preserving_merge::SortPreservingMergeExec,
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

    fn build_ordering(schema: &Arc<Schema>) -> LexOrdering {
        vec![PhysicalSortExpr {
            expr: Arc::new(Column::new(schema.field(0).name(), 0)),
            options: SortOptions::default(),
        }]
        .into()
    }

    fn build_coalesce_exec_with_memory(schema: &Arc<Schema>) -> Arc<CoalescePartitionsExec> {
        let input_batch = RecordBatch::new_empty(schema.clone());
        let memory_exec =
            MemorySourceConfig::try_new_exec(&[vec![input_batch]], schema.clone(), None).unwrap();
        Arc::new(CoalescePartitionsExec::new(memory_exec))
    }

    #[test]
    fn should_replace_top_most_coalesce_with_sort_preserving_merge() {
        // Given
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let coalesce = build_coalesce_exec_with_memory(&schema);
        let ordering = build_ordering(&schema);

        // When
        let result = remove_coalesce_physical_stage(&ordering, coalesce.clone());

        // Then
        let Ok(new_plan) = result else {
            panic!("Expected removal to succeed");
        };

        let contains_sort_preserving_merge = new_plan
            .exists(|node| {
                Ok(node
                    .as_any()
                    .downcast_ref::<SortPreservingMergeExec>()
                    .is_some())
            })
            .unwrap();
        assert!(
            contains_sort_preserving_merge,
            "Should contain SortPreservingMergeExec"
        );
    }

    #[test]
    fn should_return_same_plan_if_no_coalesce_found() {
        // Given
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let input_batch = arrow::record_batch::RecordBatch::new_empty(schema.clone());
        let memory_exec =
            MemorySourceConfig::try_new_exec(&[vec![input_batch]], schema.clone(), None).unwrap();
        let ordering = build_ordering(&schema);

        // When
        let result = remove_coalesce_physical_stage(&ordering, memory_exec.clone());

        // Then
        let Ok(new_plan) = result else {
            panic!("Expected removal to succeed");
        };

        let original_display = displayable(memory_exec.as_ref()).one_line();
        let new_display = displayable(new_plan.as_ref()).one_line();
        assert_eq!(format!("{original_display}"), format!("{new_display}"));
    }

    #[test]
    fn should_stop_replacement_after_first_coalesce() {
        // Given
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let input_batch = arrow::record_batch::RecordBatch::new_empty(schema.clone());
        let memory_exec =
            MemorySourceConfig::try_new_exec(&[vec![input_batch]], schema.clone(), None).unwrap();
        let coalesce_inner = Arc::new(CoalescePartitionsExec::new(memory_exec));
        let coalesce_outer = Arc::new(CoalescePartitionsExec::new(coalesce_inner));
        let ordering = build_ordering(&schema);

        // When
        let result = remove_coalesce_physical_stage(&ordering, coalesce_outer.clone());

        // Then
        let Ok(new_plan) = result else {
            panic!("Expected removal to succeed");
        };

        let contains_coalesce_inner = new_plan
            .exists(|node| {
                Ok(node
                    .as_any()
                    .downcast_ref::<CoalescePartitionsExec>()
                    .is_some())
            })
            .unwrap();
        assert!(contains_coalesce_inner, "Inner coalesce should remain");

        let root_is_coalesce = new_plan
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .is_some();
        assert!(!root_is_coalesce, "Outer root should not be coalesce");
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
            assert_eq!(exprs[0].1, "original_col");
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
            assert_eq!(exprs[0].1, "alias_a");
            // The input to this projection should still be inner_projection
            if let Some(inner_proj) = top_proj.input().as_any().downcast_ref::<ProjectionExec>() {
                // Inner projection remains unchanged
                let exprs = inner_proj.expr();
                assert_eq!(exprs[0].1, "alias_a");
            } else {
                panic!("Inner projection should remain unchanged");
            }
        } else {
            panic!("Resulting plan node is not a ProjectionExec");
        }
    }
}
