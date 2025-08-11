//! Contains the implementation for performing Sleeper compactions
//! using Apache `DataFusion`.
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
use crate::{
    CommonConfig, CompactionResult, OperationOutput,
    datafusion::{
        ParquetWriterConfigurer, SleeperOperations,
        metrics::RowCounts,
        sketch::{Sketcher, output_sketch},
        util::{collect_stats, explain_plan},
    },
};
use datafusion::{
    common::plan_err,
    error::DataFusionError,
    physical_plan::{collect, displayable},
    prelude::{DataFrame, SessionConfig, SessionContext},
};
use log::info;
use objectstore_ext::s3::ObjectStoreFactory;
use std::sync::Arc;

/// Starts a Sleeper compaction.
///
/// The object store factory must be able to produce an [`object_store::ObjectStore`] capable of reading
/// from the input URLs and writing to the output URL. A sketch file will be produced for
/// the output file.
pub async fn compact(
    store_factory: &ObjectStoreFactory,
    config: &CommonConfig<'_>,
) -> Result<CompactionResult, DataFusionError> {
    let ops = SleeperOperations::new(config);
    info!("DataFusion compaction: {ops}");

    // Retrieve Parquet output options
    let OperationOutput::File {
        output_file,
        opts: parquet_options,
    } = &config.output
    else {
        return plan_err!("Sleeper compactions must output to a file");
    };

    // Create Parquet configuration object based on requested output
    let configurer = ParquetWriterConfigurer { parquet_options };

    // Make compaction DataFrame
    let (sketcher, frame) = build_compaction_dataframe(&ops, &configurer, store_factory).await?;

    // Explain logical plan
    explain_plan(&frame).await?;

    // Run plan
    let stats = execute_compaction_plan(&ops, frame).await?;

    // Write the frame out and collect stats
    output_sketch(store_factory, output_file, sketcher.sketch()).await?;

    stats.log_metrics();
    Ok(CompactionResult::from(&stats))
}

/// Creates the dataframe for a compaction.
///
/// This applies necessary filtering, sorting and sketch creation
/// steps to the plan.
///
/// # Errors
/// Each step of compaction may produce an error. Any are reported back to the caller.
async fn build_compaction_dataframe<'a>(
    ops: &'a SleeperOperations<'a>,
    configurer: &'a ParquetWriterConfigurer<'a>,
    store_factory: &ObjectStoreFactory,
) -> Result<(Sketcher<'a>, DataFrame), DataFusionError> {
    let sf = ops
        .apply_config(SessionConfig::new(), store_factory)
        .await?;
    let sf = configurer.apply_parquet_config(sf);
    let ctx = ops.apply_to_context(SessionContext::new_with_config(sf), store_factory)?;
    let mut frame = ops.create_initial_partitioned_read(&ctx).await?;
    frame = ops.apply_user_filters(frame)?;
    frame = ops.apply_general_sort(frame)?;
    frame = ops.apply_aggregations(frame)?;
    let sketcher = ops.create_sketcher(frame.schema());
    frame = sketcher.apply_sketch(frame)?;
    frame = ops.plan_with_parquet_output(frame, configurer)?;
    Ok((sketcher, frame))
}

/// Runs the plan in the frame.
///
/// The plan will be optimised into a physical plan, then statistics collected and returned.
///
/// # Errors
/// Any error that occurs during execution will be returned.
async fn execute_compaction_plan(
    ops: &SleeperOperations<'_>,
    frame: DataFrame,
) -> Result<RowCounts, DataFusionError> {
    let task_ctx = Arc::new(frame.task_ctx());
    let physical_plan = ops.to_physical_plan(frame).await?;
    info!(
        "Physical plan\n{}",
        displayable(&*physical_plan).indent(true)
    );
    collect(physical_plan.clone(), task_ctx).await?;
    let stats = collect_stats(&ops.config.input_files, &physical_plan)?;
    Ok(stats)
}
