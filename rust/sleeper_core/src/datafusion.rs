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
    CommonConfig, CompactionResult, OperationOutput, SleeperCompactionConfig,
    datafusion::{
        filter_aggregation_config::{FilterAggregationConfig, validate_aggregations},
        sketch::{create_sketch_udf, output_sketch},
        util::{
            calculate_upload_size, check_for_sort_exec, explain_plan, register_store,
            retrieve_input_size,
        },
    },
};
use aggregator_udfs::nonnull::register_non_nullable_aggregate_udfs;
use arrow::compute::SortOptions;
use datafusion::{
    common::{
        plan_err,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    config::TableParquetOptions,
    datasource::file_format::{format_as_file_type, parquet::ParquetFormatFactory},
    error::DataFusionError,
    execution::{config::SessionConfig, context::SessionContext, options::ParquetReadOptions},
    logical_expr::{LogicalPlanBuilder, SortExpr},
    physical_expr::{LexOrdering, PhysicalSortExpr},
    physical_plan::{
        accept, coalesce_partitions::CoalescePartitionsExec, collect, displayable,
        expressions::Column, sorts::sort_preserving_merge::SortPreservingMergeExec,
    },
    prelude::*,
};
use log::{info, warn};
use metrics::RowCounts;
use objectstore_ext::s3::ObjectStoreFactory;
use std::{collections::HashMap, sync::Arc};
use url::Url;

mod config;
mod filter_aggregation_config;
mod metrics;
mod region;
pub mod sketch;
mod sketch_udf;
mod util;

pub use config::ParquetWriterConfigurer;
pub use region::SleeperPartitionRegion;

/// Drives common operations in processing of `DataFusion` for Sleeper.
#[derive(Debug)]
pub struct SleeperOperations<'a> {
    config: &'a CommonConfig<'a>,
}

impl<'a> SleeperOperations<'a> {
    /// Create a new `DataFusion` operations processor.
    #[must_use]
    pub fn new(config: &'a CommonConfig) -> Self {
        Self { config }
    }

    /// Create the `DataFusion` session configuration for a given session.
    ///
    /// This sets as many parameters as possible from the given input data.
    ///
    pub async fn apply_config(
        &self,
        mut cfg: SessionConfig,
        store_factory: &ObjectStoreFactory,
    ) -> Result<SessionConfig, DataFusionError> {
        // In order to avoid a costly "Sort" stage in the physical plan, we must make
        // sure the target partitions as at least as big as number of input files.
        cfg.options_mut().execution.target_partitions = std::cmp::max(
            cfg.options().execution.target_partitions,
            self.config.input_files.len(),
        );
        // Disable page indexes since we won't benefit from them as we are reading large contiguous file regions
        cfg.options_mut().execution.parquet.enable_page_index = false;
        // Disable repartition_aggregations to workaround sorting bug where DataFusion partitions are concatenated back
        // together in wrong order.
        cfg.options_mut().optimizer.repartition_aggregations = false;
        // Set upload size if outputting to a file
        if let OperationOutput::File {
            output_file: _,
            opts: _,
        } = self.config.output
        {
            let total_input_size = retrieve_input_size(&self.config.input_files, store_factory)
                .await
                .inspect_err(|e| warn!("Error getting total input data size {e}"))?;
            cfg.options_mut().execution.objectstore_writer_buffer_size =
                calculate_upload_size(total_input_size)?;
        }
        Ok(cfg)
    }

    // Configure a [`SessionContext`].
    pub fn apply_to_context(
        &self,
        mut ctx: SessionContext,
        store_factory: &ObjectStoreFactory,
    ) -> Result<SessionContext, DataFusionError> {
        register_non_nullable_aggregate_udfs(&mut ctx);
        // Register object stores for input files and output file
        register_store(
            store_factory,
            &self.config.input_files,
            match &self.config.output {
                OperationOutput::ArrowRecordBatch => None,
                OperationOutput::File {
                    output_file,
                    opts: _,
                } => Some(&output_file),
            },
            &ctx,
        )?;
        Ok(ctx)
    }
}

impl std::fmt::Display for SleeperOperations<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.config)
    }
}

/// Starts a Sleeper compaction.
///
/// The object store factory must be able to produce an [`object_store::ObjectStore`] capable of reading
/// from the input URLs and writing to the output URL. A sketch file will be produced for
/// the output file.
pub async fn compact(
    store_factory: &ObjectStoreFactory,
    input_data: &SleeperCompactionConfig<'_>,
) -> Result<CompactionResult, DataFusionError> {
    let ops = SleeperOperations::new(&input_data.common);
    info!("DataFusion compaction: {ops}");

    let sf = ops
        .apply_config(SessionConfig::new(), store_factory)
        .await?;

    // Retrieve Parquet output options
    let OperationOutput::File {
        output_file: _,
        opts: parquet_options,
    } = &input_data.common.output
    else {
        return plan_err!("Sleeper compactions must output to a file");
    };
    let configurer = ParquetWriterConfigurer { parquet_options };
    let sf = configurer.apply_parquet_config(sf);
    let mut ctx = ops.apply_to_context(SessionContext::new_with_config(sf), store_factory)?;

    // Sort on row key columns then sort key columns (nulls last)
    let sort_order = sort_order(&input_data.common);
    info!("Row key and sort key column order {sort_order:?}");

    // Tell DataFusion that the row key columns and sort columns are already sorted
    let po = ParquetReadOptions::default().file_sort_order(vec![sort_order.clone()]);
    let mut frame = ctx
        .read_parquet(input_data.input_files().to_owned(), po)
        .await?;

    // If we have a partition region, apply it first
    if let Some(expr) = Into::<Option<Expr>>::into(input_data.region()) {
        frame = frame.filter(expr)?;
    }

    // Parse Sleeper iterator configuration and apply
    let filter_agg_conf = parse_iterator_config(input_data.iterator_config.as_ref())?;
    frame = apply_general_row_filters(frame, filter_agg_conf.as_ref())?;

    // Create the sketch function
    let sketch_func = create_sketch_udf(&input_data.common.row_key_cols, frame.schema());

    // Extract all column names
    let col_names = frame.schema().clone().strip_qualifiers().field_names();
    let row_key_exprs = input_data
        .common
        .row_key_cols
        .iter()
        .map(col)
        .collect::<Vec<_>>();

    // Iterate through column names, mapping each into an `Expr` of the name UNLESS
    // we find the first row key column which should be mapped to the sketch function
    let col_names_expr = col_names
        .iter()
        .map(|col_name| {
            // Have we found the first row key column?
            if *col_name == input_data.common.row_key_cols[0] {
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
    frame = apply_aggregations(
        &input_data.common.row_key_cols,
        frame,
        filter_agg_conf.as_ref(),
    )?;
    frame = frame.select(col_names_expr)?;

    // Figure out which columns should be dictionary encoded
    let pqo = configurer.apply_dictionary_encoding(
        ctx.copied_table_options().parquet,
        input_data,
        frame.schema(),
    );

    // Create column list of row keys and sort key cols
    let sorting_columns = input_data
        .common
        .row_key_cols
        .iter()
        .chain(input_data.common.sort_key_cols.iter())
        .map(String::as_str)
        .collect::<Vec<_>>();

    // Write the frame out and collect stats
    let stats = collect_stats(
        frame.clone(),
        &input_data.input_files(),
        input_data
            .output_file()
            .map_err(|e| DataFusionError::External(e.into()))?,
        &sorting_columns,
        pqo,
    )
    .await?;
    output_sketch(
        store_factory,
        input_data
            .output_file()
            .map_err(|e| DataFusionError::External(e.into()))?,
        &sketch_func,
    )
    .await?;

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

/// Creates the sort order for a given schema.
///
/// This is a list of the row key columns followed by the sort key columns.
///
fn sort_order(config: &CommonConfig) -> Vec<SortExpr> {
    config
        .row_key_cols
        .iter()
        .chain(config.sort_key_cols.iter())
        .map(|s| col(s).sort(true, false))
        .collect::<Vec<_>>()
}
