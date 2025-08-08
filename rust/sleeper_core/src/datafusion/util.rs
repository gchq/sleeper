//! Sleeper `DataFusion` utility functions.
use std::sync::Arc;

use arrow::util::pretty::pretty_format_batches;
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
    common::{plan_err, tree_node::TreeNode},
    config::ExecutionOptions,
    error::DataFusionError,
    execution::{SessionState, SessionStateBuilder},
    logical_expr::LogicalPlan,
    physical_plan::{ExecutionPlan, sorts::sort::SortExec},
    prelude::{DataFrame, SessionContext},
};
use log::info;
use num_format::{Locale, ToFormattedString};
use objectstore_ext::s3::ObjectStoreFactory;
use url::Url;

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
