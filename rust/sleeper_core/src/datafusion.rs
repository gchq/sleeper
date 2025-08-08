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
        sketch::{Sketcher, output_sketch},
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
        DFSchema, plan_err,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    config::TableParquetOptions,
    datasource::file_format::{format_as_file_type, parquet::ParquetFormatFactory},
    error::DataFusionError,
    execution::{
        TaskContext, config::SessionConfig, context::SessionContext, options::ParquetReadOptions,
    },
    logical_expr::{LogicalPlanBuilder, SortExpr},
    physical_expr::{LexOrdering, PhysicalSortExpr},
    physical_plan::{
        ExecutionPlan, accept, coalesce_partitions::CoalescePartitionsExec, collect, displayable,
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
                } => Some(output_file),
            },
            &ctx,
        )?;
        Ok(ctx)
    }

    /// Create the initial [`DataFrame`] from the configuration.
    ///
    /// This frame's plan will load the input Parquet files and filter
    /// according to the partition region.
    ///
    /// # Errors
    /// If reading or filtering fail, then an error occurs.
    pub async fn create_initial_partitioned_read(
        &self,
        ctx: &SessionContext,
    ) -> Result<DataFrame, DataFusionError> {
        let po = if self.config.input_files_sorted {
            let sort_order = self.create_sort_order();
            info!("Row and sort key column order: {sort_order:?}");
            ParquetReadOptions::default().file_sort_order(vec![sort_order.clone()])
        } else {
            warn!(
                "Reading input files that are not individually sorted! Did you mean to set input_files_sorted to true instead?"
            );
            ParquetReadOptions::default()
        };
        // Read Parquet files and apply sort order
        let frame = ctx
            .read_parquet(self.config.input_files.clone(), po)
            .await?;
        // Do we have partition bounds?
        Ok(
            if let Some(expr) = Into::<Option<Expr>>::into(&self.config.region) {
                frame.filter(expr)?
            } else {
                frame
            },
        )
    }

    /// Creates the sort order for a given schema.
    ///
    /// This is a list of the row key columns followed by the sort key columns.
    ///
    pub fn create_sort_order(&self) -> Vec<SortExpr> {
        self.config
            .sorting_columns_iter()
            .map(|s| col(s).sort(true, false))
            .collect::<Vec<_>>()
    }

    // Process the iterator configuration and create a filter and aggregation object from it.
    //
    // # Errors
    // If there is an error in parsing the configuration string.
    pub fn parse_iterator_config(
        &self,
    ) -> Result<Option<FilterAggregationConfig>, DataFusionError> {
        self.config
            .iterator_config
            .as_ref()
            .map(|s| FilterAggregationConfig::try_from(s.as_str()))
            .transpose()
    }

    /// Apply any configured filters to the `DataFusion` operation if any are present.
    ///
    /// # Errors
    /// An error will result if the frame cannot be filtered according to the given
    /// expression.
    fn apply_user_filters(&self, frame: DataFrame) -> Result<DataFrame, DataFusionError> {
        Ok(
            if let Some(filter) = self
                .parse_iterator_config()?
                .as_ref()
                .and_then(FilterAggregationConfig::filter)
            {
                info!("Applying Sleeper filters: {filter:?}");
                frame.filter(filter.create_filter_expr()?)?
            } else {
                frame
            },
        )
    }

    /// Apply a general sort to the frame based on the sort ordering from row keys and
    /// sort keys.
    ///
    /// If sort ordering is specified on the input files, then this should give a streaming
    /// merge sort, not a full in memory sort.
    ///
    /// # Errors
    /// If any errors result from adding the sort to the plan
    pub fn apply_general_sort(&self, frame: DataFrame) -> Result<DataFrame, DataFusionError> {
        frame.sort(self.create_sort_order())
    }

    /// If any are present, apply Sleeper aggregations to the given frame.
    ///
    /// # Errors
    /// If any configuration errors are present in the aggregations, e.g. duplicates or row key columns specified,
    /// then an error will result.
    fn apply_aggregations(&self, frame: DataFrame) -> Result<DataFrame, DataFusionError> {
        Ok(
            if let Some(aggregations) = self
                .parse_iterator_config()?
                .as_ref()
                .and_then(FilterAggregationConfig::aggregation)
            {
                info!("Applying Sleeper aggregations: {aggregations:?}");
                // Grab row and sort key columns
                let group_by_cols = self.config.sorting_columns();

                // Check aggregations meet validity checks
                validate_aggregations(&group_by_cols, frame.schema(), aggregations)?;
                let aggregations_to_apply = aggregations
                    .iter()
                    .map(|agg| agg.to_expr(&frame))
                    .collect::<Result<Vec<_>, _>>()?;
                frame.aggregate(
                    group_by_cols.iter().map(|e| col(*e)).collect(),
                    aggregations_to_apply,
                )?
            } else {
                frame
            },
        )
    }

    /// Create a sketching object to manage creation of quantile sketches.
    #[must_use]
    pub fn create_sketcher(&self, schema: &DFSchema) -> Sketcher<'_> {
        Sketcher::new(&self.config.row_key_cols, schema)
    }

    pub fn plan_with_parquet_output(
        &self,
        frame: DataFrame,
        configurer: &ParquetWriterConfigurer<'_>,
    ) -> Result<DataFrame, DataFusionError> {
        let OperationOutput::File {
            output_file,
            opts: _,
        } = &self.config.output
        else {
            return plan_err!("Parquet output not selected!");
        };
        let (session_state, logical_plan) = frame.into_parts();
        // Figure out which columns should be dictionary encoded
        let pqo = configurer.apply_dictionary_encoding(
            session_state.default_table_options().parquet,
            self.config,
            logical_plan.schema(),
        );
        let logical_plan = LogicalPlanBuilder::copy_to(
            logical_plan,
            output_file.as_str().into(),
            format_as_file_type(Arc::new(ParquetFormatFactory::new_with_options(pqo))),
            HashMap::default(),
            Vec::new(),
        )?
        .build()?;
        Ok(DataFrame::new(session_state, logical_plan))
    }

    pub async fn to_modified_physical_plan(
        &self,
        frame: DataFrame,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // Create lexical column ordering
        let ordering = self.create_sort_expr_ordering(&frame)?;

        // Consume frame and generate initial physical plan
        let physical_plan = frame.create_physical_plan().await?;
    }

    ///Create a lexical ordering for sorting columns on a frame.
    ///
    /// The lexical ordering is based on the row-keys and sort-keys for the Sleeper
    /// operation.
    ///
    /// # Errors
    /// The columns in the schema must match the row and sort key column names.
    pub fn create_sort_expr_ordering(
        &self,
        frame: &DataFrame,
    ) -> Result<LexOrdering, DataFusionError> {
        let plan_schema = frame.schema().as_arrow();
        let sorting_columns = self.config.sorting_columns();
        Ok(LexOrdering::new(
            sorting_columns
                .iter()
                .map(|col_name| {
                    Ok(PhysicalSortExpr::new(
                        Arc::new(Column::new_with_schema(col_name, plan_schema)?),
                        SortOptions::new(false, false),
                    ))
                })
                .collect::<Result<Vec<_>, DataFusionError>>()?,
        ))
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

    // Retrieve Parquet output options
    let OperationOutput::File {
        output_file: _,
        opts: parquet_options,
    } = &input_data.common.output
    else {
        return plan_err!("Sleeper compactions must output to a file");
    };

    // Create Parquet configuration object based on requested output
    let configurer = ParquetWriterConfigurer { parquet_options };

    // Make compaction DataFrame
    let (sketcher, frame) = build_compaction_dataframe(&ops, &configurer, store_factory).await?;

    // Explain plan
    explain_plan(&frame).await?;

    // Create column list of row keys and sort key cols
    let sorting_columns = input_data.common.sorting_columns();

    // Write the frame out and collect stats
    let stats = collect_stats(
        frame.clone(),
        input_data.input_files(),
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
        sketcher.sketch(),
    )
    .await?;

    Ok(CompactionResult::from(&stats))
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

    let _ = collect(
        new_plan.clone(),
        Arc::new(TaskContext::from(&session_state)),
    )
    .await?;
    let mut stats = RowCounts::new(input_paths);
    accept(new_plan.as_ref(), &mut stats)?;
    stats.log_metrics();
    Ok(stats)
}

/// Creates the dataframe for a compaction.
///
/// This applies necessary filtering, sorting and sketch creation
/// steps to the plan.
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
    frame = ops.plan_with_parquet_output(frame, &configurer)?;
    Ok((sketcher, frame))
}
