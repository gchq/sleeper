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
use arrow::util::pretty::pretty_format_batches;
use datafusion::{
    common::{
        plan_err,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    config::ExecutionOptions,
    error::DataFusionError,
    execution::SessionStateBuilder,
    physical_expr::LexOrdering,
    physical_plan::{
        ExecutionPlan, accept,
        coalesce_partitions::CoalescePartitionsExec,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
    },
    prelude::{DataFrame, SessionContext},
};
use log::info;
use num_format::{Locale, ToFormattedString};
use objectstore_ext::s3::ObjectStoreFactory;
use std::sync::Arc;
use url::Url;

use crate::datafusion::metrics::RowCounts;

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
        usize::try_from(total_input_size / 5000)
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
/// `DataFusion` doesn't seem to like loading a single file set from different object stores
/// so we only register the first one.
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
