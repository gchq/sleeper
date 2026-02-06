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
use arrow::{datatypes::DataType, util::pretty::pretty_format_batches};
use datafusion::{
    common::{
        DFSchema, plan_err,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    config::ExecutionOptions,
    dataframe::DataFrame,
    error::DataFusionError,
    execution::{SessionStateBuilder, context::SessionContext},
    logical_expr::{ScalarUDF, ident},
    physical_expr::LexOrdering,
    physical_plan::{
        ExecutionPlan, Partitioning, accept,
        coalesce_partitions::CoalescePartitionsExec,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
    },
};
use log::{debug, info};
use num_format::{Locale, ToFormattedString};
use object_store::ObjectMeta;
use objectstore_ext::s3::ObjectStoreFactory;
use std::sync::Arc;
use url::Url;

use crate::datafusion::{cast_udf::CastUDF, metrics::RowCounts};

/// Maximum number of file upload parts to generate. Implementation will
/// try to match this, but may not get there exactly.
pub const MAX_PART_COUNT: u64 = 5000;
/// The fraction of total file size to estimate for Parquet metadata. This errs
/// on the larger side. We will bound this between a minimum of 512KiB and maximum
/// of 10MiB.
pub const META_DATA_SIZE_FRACTION: f64 = 0.0001;
const _: () = assert!(
    0f64 <= META_DATA_SIZE_FRACTION && META_DATA_SIZE_FRACTION <= 1f64,
    "META_DATA_SIZE_FRACTION out of range"
);

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
    debug!(
        "Use upload buffer of {} bytes.",
        upload_size.to_formatted_string(&Locale::en)
    );
    Ok(upload_size)
}

/// Calculate the metadata size hint to use based on the largest file size. This value will be clamped
/// between 512KiB and 10MiB.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
pub fn calculate_metadata_size_hint(largest_file: u64) -> u64 {
    ((largest_file as f64 * META_DATA_SIZE_FRACTION) as u64).clamp(512 * 1024, 10 * 1024 * 1024)
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

/// Retrieves the [`ObjectMeta`]s for each URL passed.
///
/// # Errors
/// Fails if we can't retrieve object data from an object store.
pub async fn retrieve_object_metas(
    input_paths: &[Url],
    store_factory: &ObjectStoreFactory,
) -> Result<Vec<ObjectMeta>, DataFusionError> {
    let mut metas = Vec::new();
    for input_path in input_paths {
        let store = store_factory
            .get_object_store(input_path)
            .map_err(|e| DataFusionError::External(e.into()))?;
        let p = input_path.path();
        metas.push(store.head(&p.into()).await?);
    }
    Ok(metas)
}

/// Calculate the total size of all `input_paths` object metas.
///
/// # Returns
/// A tuple of (total input size, largest single file).
pub fn retrieve_input_size(inputs: &[ObjectMeta]) -> (u64, u64) {
    let mut total_input = 0u64;
    let mut largest_file = 0u64;
    for input in inputs {
        let size = input.size;
        total_input += size;
        largest_file = std::cmp::max(largest_file, size);
    }
    (total_input, largest_file)
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

/// Applies a complete sort ordering to all [`SortPreservingMergeExec`] nodes in a physical plan.
///
/// This function traverses the given physical plan and, for every `SortPreservingMergeExec` node found,
/// updates it with the provided full sort ordering. All other nodes are left unchanged. This can be used
/// to ensure that sort-preserving merge operations throughout the plan are executed with a consistent and
/// comprehensive ordering, which is sometimes necessary when downstream consumers depend on a global ordering.
///
/// # Returns
/// A new physical plan with updated sort ordering on all `SortPreservingMergeExec` nodes, or an error if
/// the transformation fails.
///
/// # Errors
/// Returns a [`DataFusionError`] if there is an issue traversing or transforming the plan tree.
pub fn apply_full_sort_ordering(
    ordering: &LexOrdering,
    physical_plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    physical_plan
        // Recurse down plan looking for specific node
        .transform_down(|plan_node| {
            Ok(
                if let Some(sort_preserve) =
                    plan_node.as_any().downcast_ref::<SortPreservingMergeExec>()
                {
                    // Swap for a sort merging stage with complete sort order
                    let replacement = SortPreservingMergeExec::new(
                        ordering.clone(),
                        sort_preserve.input().clone(),
                    )
                    .with_fetch(sort_preserve.fetch());
                    // Keep searching down the query plan after making one replacement
                    Transformed::new(Arc::new(replacement), true, TreeNodeRecursion::Continue)
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

/// Checks the frame for columns with differing numeric schemas.
///
/// If a column that has changed from 32 to 64 bit signed integers or vice versa,
/// then a simple numeric cast is added. This is an infallible operation, but will
/// truncate the high 32 bits on a 64 to 32 bit cast.
pub fn add_numeric_casts(
    agg_frame: DataFrame,
    orig_schema: &DFSchema,
    agg_schema: &DFSchema,
) -> Result<DataFrame, DataFusionError> {
    let mut column_proj = vec![];
    for types in orig_schema.fields().iter().zip(agg_schema.fields().iter()) {
        match (types.0.data_type(), types.1.data_type()) {
            (orig_type @ DataType::Int32, agg_type @ DataType::Int64)
            | (orig_type @ DataType::Int64, agg_type @ DataType::Int32) => {
                column_proj.push(
                    ScalarUDF::from(CastUDF::new(agg_type, orig_type, types.0.is_nullable()))
                        .call(vec![ident(types.0.name())])
                        .alias(types.0.name()),
                );
            }
            (left, right) if left == right => column_proj.push(ident(types.0.name())),
            (left, right) => return plan_err!("Type {left} cannot be cast to type {right}"),
        }
    }
    agg_frame.select(column_proj)
}

#[cfg(test)]
mod tests {
    use crate::datafusion::util::{
        add_numeric_casts, apply_full_sort_ordering, calculate_metadata_size_hint,
        remove_coalesce_physical_stage,
    };
    use arrow::{
        array::RecordBatch,
        compute::SortOptions,
        datatypes::{DataType, Field, Schema},
    };
    use color_eyre::eyre::Error;
    use datafusion::{
        catalog::memory::MemorySourceConfig,
        common::{DFSchema, tree_node::TreeNode},
        dataframe,
        physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column},
        physical_plan::{
            coalesce_partitions::CoalescePartitionsExec, displayable,
            sorts::sort_preserving_merge::SortPreservingMergeExec,
        },
    };
    use std::{collections::HashMap, sync::Arc};

    #[test]
    fn should_cast_numeric_columns() -> Result<(), Error> {
        // Given
        let df = dataframe!(
            "int8" => [ 0i8, 1i8, 2i8 ],
            "int16" => [ 0i16, 1i16, 2i16],
            "int32_to_keep" => [ 0i32, 1i32, 2i32],
            "int32_to_change" => [ 0i32, 1i32, 2i32],
            "int64_to_keep" => [ 0i64, 1i64, 2i64],
            "int64_to_change" => [ 0i64, 1i64, 2i64],
            "some_string" => [ "a".to_owned(), "b".to_owned(), "c".to_owned() ],
        )?;

        let original_schema = df.schema().clone();

        // Assume some aggregations have changed the *_to_change columns from 32 bit and vice versa
        let new_df = dataframe!(
            "int8" => [ 0i8, 1i8, 2i8 ],
            "int16" => [ 0i16, 1i16, 2i16],
            "int32_to_keep" => [ 0i32, 1i32, 2i32],
            "int32_to_change" => [ 0i64, 1i64, 2i64],
            "int64_to_keep" => [ 0i64, 1i64, 2i64],
            "int64_to_change" => [ 0i32, 1i32, 2i32],
            "some_string" => [ "a".to_owned(), "b".to_owned(), "c".to_owned() ],
        )?;

        let new_schema = new_df.schema().clone();

        // When
        let cast_df = add_numeric_casts(new_df, &original_schema, &new_schema)?;
        let orig_data_types = original_schema.iter().map(|e| e.1.data_type());
        let new_data_types = cast_df.schema().iter().map(|e| e.1.data_type());

        // Then
        assert!(
            orig_data_types.eq(new_data_types),
            "Datatypes in original and modified schema should match!"
        );

        Ok(())
    }

    #[test]
    fn should_error_on_invalid_cast() -> Result<(), Error> {
        // Given
        let df = dataframe!(
            "int8" => [ 0i8, 1i8, 2i8 ],
        )?;

        let original_schema = df.schema().clone();

        // Pretend we somehow managed to aggregate an int into a string...
        let new_schema = DFSchema::from_unqualified_fields(
            vec![Field::new("int8", DataType::Utf8, true)].into(),
            HashMap::new(),
        )?;

        // When
        let cast_df = add_numeric_casts(df, &original_schema, &new_schema);

        // Then
        assert!(cast_df.is_err());
        let err_msg = format!("{}", cast_df.unwrap_err());
        assert_eq!(
            err_msg,
            "Error during planning: Type Int8 cannot be cast to type Utf8"
        );

        Ok(())
    }

    fn build_ordering(schema: &Arc<Schema>, field_count: usize) -> LexOrdering {
        let mut exprs = vec![];
        for _ in 0..field_count {
            exprs.push(PhysicalSortExpr {
                expr: Arc::new(Column::new(schema.field(0).name(), 0)),
                options: SortOptions::default(),
            });
        }
        LexOrdering::new(exprs).unwrap()
    }

    fn build_coalesce_exec_with_memory(schema: &Arc<Schema>) -> Arc<CoalescePartitionsExec> {
        let input_batch = RecordBatch::new_empty(schema.clone());
        let memory_exec =
            MemorySourceConfig::try_new_exec(&[vec![input_batch]], schema.clone(), None).unwrap();
        Arc::new(CoalescePartitionsExec::new(memory_exec))
    }

    #[test]
    fn should_replace_top_most_coalesce_with_sort_preserving_merge() -> Result<(), Error> {
        // Given
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let coalesce = build_coalesce_exec_with_memory(&schema);
        let ordering = build_ordering(&schema, 1);

        // When
        let result = remove_coalesce_physical_stage(&ordering, coalesce.clone())?;

        // Then
        let contains_sort_preserving_merge = result
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
        Ok(())
    }

    #[test]
    fn should_return_same_plan_if_no_coalesce_found() -> Result<(), Error> {
        // Given
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let input_batch = arrow::record_batch::RecordBatch::new_empty(schema.clone());
        let memory_exec =
            MemorySourceConfig::try_new_exec(&[vec![input_batch]], schema.clone(), None).unwrap();
        let ordering = build_ordering(&schema, 1);

        // When
        let result = remove_coalesce_physical_stage(&ordering, memory_exec.clone())?;

        // Then
        let original_display = displayable(memory_exec.as_ref()).one_line();
        let new_display = displayable(result.as_ref()).one_line();
        assert_eq!(format!("{original_display}"), format!("{new_display}"));
        Ok(())
    }

    #[test]
    fn should_stop_replacement_after_first_coalesce() -> Result<(), Error> {
        // Given
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let input_batch = arrow::record_batch::RecordBatch::new_empty(schema.clone());
        let memory_exec =
            MemorySourceConfig::try_new_exec(&[vec![input_batch]], schema.clone(), None).unwrap();
        let coalesce_inner = Arc::new(CoalescePartitionsExec::new(memory_exec));
        let coalesce_outer = Arc::new(CoalescePartitionsExec::new(coalesce_inner));
        let ordering = build_ordering(&schema, 1);

        // When
        let result = remove_coalesce_physical_stage(&ordering, coalesce_outer.clone())?;

        // Then
        let contains_coalesce_inner = result
            .exists(|node| {
                Ok(node
                    .as_any()
                    .downcast_ref::<CoalescePartitionsExec>()
                    .is_some())
            })
            .unwrap();
        assert!(contains_coalesce_inner, "Inner coalesce should remain");

        let root_is_coalesce = result
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .is_some();
        assert!(!root_is_coalesce, "Outer root should not be coalesce");
        Ok(())
    }

    #[test]
    fn should_return_same_plan_if_no_sort_preserve_merge_exec_found() -> Result<(), Error> {
        // Given
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));
        let input_batch = arrow::record_batch::RecordBatch::new_empty(schema.clone());
        let memory_exec =
            MemorySourceConfig::try_new_exec(&[vec![input_batch]], schema.clone(), None).unwrap();
        let ordering = build_ordering(&schema, 3);

        // When
        let result = apply_full_sort_ordering(&ordering, memory_exec.clone())?;

        // Then
        let original_display = displayable(memory_exec.as_ref()).one_line();
        let new_display = displayable(result.as_ref()).one_line();
        assert_eq!(format!("{original_display}"), format!("{new_display}"));
        Ok(())
    }

    #[test]
    fn should_replacement_all_sort_stages() -> Result<(), Error> {
        // Given
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let input_batch = arrow::record_batch::RecordBatch::new_empty(schema.clone());
        let memory_exec =
            MemorySourceConfig::try_new_exec(&[vec![input_batch]], schema.clone(), None).unwrap();
        // Create two sort stages that only apply ordering to first column
        let ordering = build_ordering(&schema, 1);
        let sort_inner = Arc::new(SortPreservingMergeExec::new(ordering.clone(), memory_exec));
        let sort_outer = Arc::new(SortPreservingMergeExec::new(ordering.clone(), sort_inner));

        // When
        // Create ordering over 3 columns
        let full_ordering = build_ordering(&schema, 3);
        let result = apply_full_sort_ordering(&full_ordering, sort_outer.clone())?;

        // Then
        // Look down tree for sort stage without full sort ordering
        let contains_partial_sort = result
            .exists(|node| {
                Ok(
                    if let Some(stage) = node.as_any().downcast_ref::<SortPreservingMergeExec>() {
                        // Does this stage only contain the original partial ordering?
                        *stage.expr() != full_ordering
                    } else {
                        false
                    },
                )
            })
            .unwrap();
        assert!(
            !contains_partial_sort,
            "Not all SortPreservingMergeExecs modified"
        );
        Ok(())
    }

    #[test]
    fn should_report_minimum_metadata_size() {
        // Given
        let size = 1024;

        // When
        let metadata_size = calculate_metadata_size_hint(size);

        // Then
        assert_eq!(metadata_size, 512 * 1024);
    }

    #[test]
    fn should_report_minimum_metadata_size_from_scaled() {
        // Given
        let size = 500 * 1024 * 1024;

        // When
        let metadata_size = calculate_metadata_size_hint(size);

        // Then
        assert_eq!(metadata_size, 512 * 1024);
    }

    #[test]
    fn should_report_valid_metadata_size() {
        // Given
        let size = 5 * 1024 * 1024 * 1024;

        // When
        let metadata_size = calculate_metadata_size_hint(size);

        // Then
        assert_eq!(metadata_size, 536_870);
    }

    #[test]
    fn should_report_maximum_metadata_size_from_scaled() {
        // Given
        let size = 300 * 1024 * 1024 * 1024;

        // When
        let metadata_size = calculate_metadata_size_hint(size);

        // Then
        assert_eq!(metadata_size, 10 * 1024 * 1024);
    }
    #[test]
    fn should_report_maximum_metadata_size() {
        // Given
        let size = u64::MAX;

        // When
        let metadata_size = calculate_metadata_size_hint(size);

        // Then
        assert_eq!(metadata_size, 10 * 1024 * 1024);
    }
}
