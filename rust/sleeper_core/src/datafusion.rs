//! `DataFusion` contains the implementation for performing Sleeper compactions
//! using Apache `DataFusion`.
//!
//! This allows for multi-threaded compaction and optimised Parquet reading.
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
    CompactionInput, CompactionResult,
    datafusion::{
        config::{ParquetWriterConfigurer, apply_sleeper_config},
        filter_aggregation_config::{FilterAggregationConfig, validate_aggregations},
        region::region_filter,
        sketch::{create_sketch_udf, output_sketch},
    },
};
use aggregator_udfs::nonnull::register_non_nullable_aggregate_udfs;
use arrow::{compute::SortOptions, util::pretty::pretty_format_batches};
use datafusion::{
    common::{
        plan_err,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    config::{ExecutionOptions, TableParquetOptions},
    datasource::file_format::{format_as_file_type, parquet::ParquetFormatFactory},
    error::DataFusionError,
    execution::{
        SessionState, config::SessionConfig, context::SessionContext, options::ParquetReadOptions,
    },
    logical_expr::{LogicalPlan, LogicalPlanBuilder, SortExpr},
    physical_expr::{LexOrdering, PhysicalSortExpr},
    physical_plan::{
        ExecutionPlan, accept,
        coalesce_partitions::CoalescePartitionsExec,
        collect, displayable,
        expressions::Column,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
    },
    prelude::*,
};
use log::{info, warn};
use metrics::RowCounts;
use num_format::{Locale, ToFormattedString};
use objectstore_ext::s3::ObjectStoreFactory;
use std::{collections::HashMap, sync::Arc};
use url::Url;

mod config;
mod filter_aggregation_config;
mod metrics;
mod region;
pub mod sketch;
mod sketch_udf;

/// Starts a Sleeper compaction.
///
/// The object store factory must be able to produce an [`object_store::ObjectStore`] capable of reading
/// from the input URLs and writing to the output URL. A sketch file will be produced for
/// the output file.
pub async fn compact(
    store_factory: &ObjectStoreFactory,
    input_data: &CompactionInput<'_>,
    input_paths: &[Url],
    output_path: &Url,
) -> Result<CompactionResult, DataFusionError> {
    info!(
        "DataFusion compaction of files {:?}",
        input_paths.iter().map(Url::as_str).collect::<Vec<_>>()
    );
    info!("DataFusion output file {}", output_path.as_str());
    info!("Compaction partition region {:?}", input_data.region);

    let total_input_size = retrieve_input_size(input_paths, store_factory)
        .await
        .inspect_err(|e| warn!("Error getting total input size {e}"))?;
    // Use Sleeper Parquet options
    let configurer = ParquetWriterConfigurer {
        parquet_options: &input_data.parquet_options,
    };
    let sf = apply_sleeper_config(
        SessionConfig::new(),
        input_data,
        Some(calculate_upload_size(total_input_size)?),
    );
    let sf = configurer.apply_parquet_config(sf);

    let mut ctx = SessionContext::new_with_config(sf);
    register_non_nullable_aggregate_udfs(&mut ctx);

    // Register object stores for input files and output file
    register_store(store_factory, input_paths, output_path, &ctx)?;

    // Sort on row key columns then sort key columns (nulls last)
    let sort_order = sort_order(input_data);
    info!("Row key and sort key column order {sort_order:?}");

    // Tell DataFusion that the row key columns and sort columns are already sorted
    let po = ParquetReadOptions::default().file_sort_order(vec![sort_order.clone()]);
    let mut frame = ctx.read_parquet(input_paths.to_owned(), po).await?;

    // If we have a partition region, apply it first
    if let Some(expr) = region_filter(&input_data.region) {
        frame = frame.filter(expr)?;
    }

    // Parse Sleeper iterator configuration and apply
    let filter_agg_conf = parse_iterator_config(input_data.iterator_config.as_ref())?;
    frame = apply_general_row_filters(frame, filter_agg_conf.as_ref())?;

    // Create the sketch function
    let sketch_func = create_sketch_udf(&input_data.row_key_cols, frame.schema());

    // Extract all column names
    let col_names = frame.schema().clone().strip_qualifiers().field_names();
    let row_key_exprs = input_data.row_key_cols.iter().map(col).collect::<Vec<_>>();

    // Iterate through column names, mapping each into an `Expr` of the name UNLESS
    // we find the first row key column which should be mapped to the sketch function
    let col_names_expr = col_names
        .iter()
        .map(|col_name| {
            // Have we found the first row key column?
            if *col_name == input_data.row_key_cols[0] {
                // Map to the sketch function
                sketch_func
                    // Sketch function needs to be called with each row key column
                    .call(row_key_exprs.clone())
                    .alias(col_name)
            } else {
                col(col_name)
            }
        })
        .collect::<Vec<_>>();

    // Apply sort to DataFrame, then aggregate if necessary, then project for DataSketch
    frame = frame.sort(sort_order)?;
    frame = apply_aggregations(&input_data.row_key_cols, frame, filter_agg_conf.as_ref())?;
    frame = frame.select(col_names_expr)?;

    // Figure out which columns should be dictionary encoded
    let pqo = configurer.apply_dictionary_encoding(
        ctx.copied_table_options().parquet,
        input_data,
        frame.schema(),
    );

    // Create column list of row keys and sort key cols
    let sorting_columns = input_data
        .row_key_cols
        .iter()
        .chain(input_data.sort_key_cols.iter())
        .map(String::as_str)
        .collect::<Vec<_>>();

    // Write the frame out and collect stats
    let stats = collect_stats(
        frame.clone(),
        input_paths,
        output_path,
        &sorting_columns,
        pqo,
    )
    .await?;
    output_sketch(store_factory, output_path, &sketch_func).await?;

    Ok(CompactionResult::from(&stats))
}

/// Apply any configured filters to the `DataFusion` operation if any are present.
fn apply_general_row_filters(
    frame: DataFrame,
    filter_agg_conf: Option<&FilterAggregationConfig>,
) -> Result<DataFrame, DataFusionError> {
    Ok(
        if let Some(FilterAggregationConfig {
            agg_cols: _,
            filter: Some(f),
            aggregation: _,
        }) = filter_agg_conf
        {
            info!("Applying Sleeper filter iterator: {f:?}");
            frame.filter(f.create_filter_expr()?)?
        } else {
            frame
        },
    )
}

/// If any are present, apply Sleeper aggregations to this `DataFusion` plan.
///
/// # Errors
/// If any configuration errors are present in the aggregations, e.g. duplicates or row key columns specified.
fn apply_aggregations(
    row_key_cols: &[String],
    frame: DataFrame,
    filter_agg_conf: Option<&FilterAggregationConfig>,
) -> Result<DataFrame, DataFusionError> {
    Ok(
        if let Some(FilterAggregationConfig {
            agg_cols,
            filter: _,
            aggregation: Some(aggregation),
        }) = &filter_agg_conf
        {
            info!("Applying Sleeper aggregation: {aggregation:?}");
            // Grab initial row key columns
            let mut group_by_cols = row_key_cols;
            let mut extra_agg_cols = vec![];
            // If we have any extra "group by" columns, concatenate them all together
            if let Some(more_columns) = agg_cols {
                extra_agg_cols.extend(
                    row_key_cols
                        .iter()
                        .chain(more_columns)
                        .map(ToOwned::to_owned),
                );
                group_by_cols = &extra_agg_cols;
            }
            // Check aggregations meet validity checks
            validate_aggregations(group_by_cols, frame.schema(), aggregation)?;
            let aggregations = aggregation
                .iter()
                .map(|agg| agg.to_expr(&frame))
                .collect::<Result<Vec<_>, _>>()?;
            frame.aggregate(group_by_cols.iter().map(col).collect(), aggregations)?
        } else {
            frame
        },
    )
}

// Process the iterator configuration and create a filter and aggregation object from it.
//
// # Errors
// If there is an error in parsing the configuration string.
fn parse_iterator_config(
    iterator_config: Option<&String>,
) -> Result<Option<FilterAggregationConfig>, DataFusionError> {
    let filter_agg_conf = iterator_config
        .map(|s| FilterAggregationConfig::try_from(s.as_str()))
        .transpose()?;
    Ok(filter_agg_conf)
}

/// Calculate the upload size based on the total input data size. This prevents uploads to S3 failing due to uploading
/// too many small parts. We conseratively set the upload size so that fewer, larger uploads are created.
fn calculate_upload_size(total_input_size: u64) -> Result<usize, DataFusionError> {
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

/// Calculate the total size of all `input_paths` objects.
///
/// # Errors
/// Fails if we can't obtain the size of the input files from the object store.
async fn retrieve_input_size(
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

/// Write the frame out to the output path and collect statistics.
///
/// The rows read and written are returned in the [`RowCounts`] object.
/// These are read from different stages in the physical plan, rows read
/// are determined by the number of filtered rows, output rows are determined
/// from the number of rows coalsced before being written.
async fn collect_stats(
    frame: DataFrame,
    input_paths: &[Url],
    output_path: &Url,
    sorting_columns: &[&str],
    pqo: TableParquetOptions,
) -> Result<RowCounts, DataFusionError> {
    // Deconstruct frame into parts, we need to do this so we can extract the physical plan before executing it.
    let task_ctx = frame.task_ctx();
    let plan_schema = frame.schema().as_arrow();
    // Convert list of columns into physical sorting expressions
    let ordering = LexOrdering::new(
        sorting_columns
            .iter()
            .map(|col_name| {
                Ok(PhysicalSortExpr::new(
                    Arc::new(Column::new_with_schema(col_name, plan_schema)?),
                    SortOptions::new(false, false),
                ))
            })
            .collect::<Result<Vec<_>, DataFusionError>>()?,
    );

    let (session_state, logical_plan) = frame.into_parts();
    let logical_plan = LogicalPlanBuilder::copy_to(
        logical_plan,
        output_path.as_str().into(),
        format_as_file_type(Arc::new(ParquetFormatFactory::new_with_options(pqo))),
        HashMap::default(),
        Vec::new(),
    )?
    .build()?;

    // Explain plan
    explain_plan(session_state.clone(), logical_plan.clone()).await?;

    // Optimise plan and generate physical plan
    let physical_plan = session_state.create_physical_plan(&logical_plan).await?;

    // Modify upper most CoalescePartitionsExec into a SortPreservingExec to avoid
    // sorting bug where partitions are re-merged out of order
    let new_plan = physical_plan
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
        })?
        .data;

    info!("Physical plan\n{}", displayable(&*new_plan).indent(true));

    // Check physical plan is free of `SortExec` stages.
    // Issue <https://github.com/gchq/sleeper/issues/5248>
    check_for_sort_exec(&new_plan)?;

    let _ = collect(new_plan.clone(), Arc::new(task_ctx)).await?;
    let mut stats = RowCounts::new(input_paths);
    accept(new_plan.as_ref(), &mut stats)?;
    stats.log_metrics();
    Ok(stats)
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
fn check_for_sort_exec(plan: &Arc<dyn ExecutionPlan>) -> Result<(), DataFusionError> {
    let contains_sort_exec =
        plan.exists(|node| Ok(node.as_any().downcast_ref::<SortExec>().is_some()))?;
    if contains_sort_exec {
        plan_err!("Physical plan contains SortExec stage. Please file a bug report.")
    } else {
        Ok(())
    }
}

/// Write explanation of query plan to log output.
///
/// # Errors
/// If explanation fails.
async fn explain_plan(
    session_state: SessionState,
    logical_plan: LogicalPlan,
) -> Result<(), DataFusionError> {
    let explained = DataFrame::new(session_state, logical_plan)
        .explain(false, false)?
        .collect()
        .await?;
    info!("DataFusion plan:\n {}", pretty_format_batches(&explained)?);
    Ok(())
}

/// Creates the sort order for a given schema.
///
/// This is a list of the row key columns followed by the sort key columns.
///
fn sort_order(input_data: &CompactionInput) -> Vec<SortExpr> {
    input_data
        .row_key_cols
        .iter()
        .chain(input_data.sort_key_cols.iter())
        .map(|s| col(s).sort(true, false))
        .collect::<Vec<_>>()
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
fn register_store(
    store_factory: &ObjectStoreFactory,
    input_paths: &[Url],
    output_path: &Url,
    ctx: &SessionContext,
) -> Result<(), DataFusionError> {
    for input_path in input_paths {
        let in_store = store_factory
            .get_object_store(input_path)
            .map_err(|e| DataFusionError::External(e.into()))?;
        ctx.runtime_env()
            .register_object_store(input_path, in_store);
    }

    let out_store = store_factory
        .get_object_store(output_path)
        .map_err(|e| DataFusionError::External(e.into()))?;
    ctx.runtime_env()
        .register_object_store(output_path, out_store);
    Ok(())
}
