//! `DataFusion` contains the implementation for performing Sleeper operations
//! using Apache `DataFusion`.
//!
//! This allows for multi-threaded data processing and optimised Parquet reading.
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
    CommonConfig,
    datafusion::{
        filter_aggregation_config::validate_aggregations,
        metadata_cache::prepopulate_metadata_cache,
        output::Completer,
        sketch::Sketcher,
        unalias::unalias_view_projection_columns,
        util::{
            add_numeric_casts, apply_full_sort_ordering, calculate_metadata_size_hint,
            calculate_upload_size, check_for_sort_exec, output_partition_count, register_store,
            remove_coalesce_physical_stage, retrieve_input_size,
        },
    },
    filter_aggregation_config::aggregate::Aggregate,
};
use aggregator_udfs::nonnull::register_non_nullable_aggregate_udfs;
use arrow::compute::SortOptions;
use datafusion::{
    common::{DFSchema, plan_err},
    dataframe::DataFrame,
    datasource::file_format::{format_as_file_type, parquet::ParquetFormatFactory},
    error::DataFusionError,
    execution::{config::SessionConfig, context::SessionContext, options::ParquetReadOptions},
    logical_expr::{Expr, LogicalPlanBuilder, SortExpr, ident},
    physical_expr::{LexOrdering, PhysicalSortExpr},
    physical_plan::{
        ExecutionPlan, expressions::Column, sorts::sort_preserving_merge::SortPreservingMergeExec,
    },
};
use log::{debug, info, warn};
use num_format::{Locale, ToFormattedString};
use object_store::ObjectMeta;
use objectstore_ext::s3::ObjectStoreFactory;
use std::{collections::HashMap, sync::Arc};

mod arrow_stream;
mod cast_udf;
mod compact;
mod config;
mod filter_aggregation_config;
mod leaf_partition_query;
mod metadata_cache;
mod metrics;
pub mod output;
mod region;
pub mod sketch;
mod sketch_udf;
mod unalias;
mod util;

pub use arrow_stream::stream_to_ffi_arrow_stream;
pub use compact::{CompactionResult, compact};
pub use config::ParquetWriterConfigurer;
pub use leaf_partition_query::{LeafPartitionQuery, LeafPartitionQueryConfig};
pub use output::{OutputType, SleeperParquetOptions};
pub use region::{ColRange, PartitionBound, SleeperRegion};

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

    /// Sets as many parameters as possible from the given input data.
    ///
    pub fn apply_config(
        &self,
        mut cfg: SessionConfig,
        object_metas: &[ObjectMeta],
    ) -> Result<SessionConfig, DataFusionError> {
        if matches!(self.config.output(), OutputType::ArrowRecordBatch) {
            // Java's Arrow FFI layer can't handle view types, so expand them at output
            cfg.options_mut().optimizer.expand_views_at_output = true;
        }
        // In order to avoid a costly "Sort" stage in the physical plan, we must make
        // sure the target partitions as at least as big as number of input files.
        cfg.options_mut().execution.target_partitions = std::cmp::max(
            cfg.options().execution.target_partitions,
            self.config.input_files().len(),
        );
        // Set `DataFusion`s configuration setting for reading page indexes from Parquet files. We set this partially
        // for completeness, since DF's cached Parquet metadata reader will read the page indexes regardless. When
        // reading large Parquet files (50's GiB+) the page indexes alone can be 50 MiB+ which can add latency to Sleeper
        // query time, for very little benefit (especially in range queries).
        //
        // We workaround the issue by prepopulating the DataFusion file metadata cache before any DF work starts. This
        // can be seen below in [`create_initial_partitioned_read()`]. If reading of page indexes is disabled in our
        // configuration settings, we will prepopulate the cache. Implementation is in [`crate::datafusion::metadata_cache`]
        // module.
        cfg.options_mut().execution.parquet.enable_page_index = self.config.read_page_indexes();
        // Disable repartition_aggregations to workaround sorting bug where DataFusion partitions are concatenated back
        // together in wrong order.
        cfg.options_mut().optimizer.repartition_aggregations = false;
        let (total_input_size, largest_file) = retrieve_input_size(object_metas);
        // Set metadata size hint to scaled value
        let metadata_size_hint = calculate_metadata_size_hint(largest_file);
        debug!(
            "Set metadata_size_hint to {}",
            metadata_size_hint.to_formatted_string(&Locale::en)
        );
        cfg.options_mut().execution.parquet.metadata_size_hint = Some(
            usize::try_from(metadata_size_hint)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        );
        // Set upload size if outputting to a file
        if let OutputType::File {
            output_file: _,
            write_sketch_file: _,
            opts: parquet_options,
        } = self.config.output()
        {
            cfg.options_mut().execution.objectstore_writer_buffer_size =
                calculate_upload_size(total_input_size)?;
            // Create Parquet configuration object based on requested output
            let configurer = ParquetWriterConfigurer { parquet_options };
            cfg = configurer.apply_parquet_config(cfg);
        }
        Ok(cfg)
    }

    // Configure a [`SessionContext`].
    pub fn configure_context(
        &self,
        mut ctx: SessionContext,
        store_factory: &ObjectStoreFactory,
    ) -> Result<SessionContext, DataFusionError> {
        register_non_nullable_aggregate_udfs(&mut ctx);
        // Register object stores for input files and output file
        register_store(
            store_factory,
            self.config.input_files(),
            match self.config.output() {
                OutputType::ArrowRecordBatch => None,
                OutputType::File {
                    output_file,
                    write_sketch_file: _,
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
        metas: &[ObjectMeta],
    ) -> Result<DataFrame, DataFusionError> {
        let po = if self.config.input_files_sorted() {
            let sort_order = self.create_sort_order();
            info!("Row and sort key field order: {sort_order:?}");
            ParquetReadOptions::default().file_sort_order(vec![sort_order.clone()])
        } else {
            warn!(
                "Reading input files that are not individually sorted! Did you mean to set input_files_sorted to true instead?"
            );
            ParquetReadOptions::default()
        };
        // As `DataFusion`s cached Parquet metadata reader will read (and therefore cache) page indexes
        // https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/parquet/metadata/struct.DFParquetMetadata.html
        // then if the read_page_indexes is true, we don't need to take any other action. If read_page_indexes is false,
        // then we need to take action to prepopulate DF's metadata cache. We read the Parquet metadata without page indexes
        // and put it in the cache. Therefore, when DF runs the query, it will already see the metadata in the cache and
        // will not attempt to do so itself.
        if !self.config.read_page_indexes() {
            let metadata_size_hint = ctx
                .state_ref()
                .read()
                .config()
                .options()
                .execution
                .parquet
                .metadata_size_hint;
            prepopulate_metadata_cache(ctx, self.config.input_files(), metas, metadata_size_hint)
                .await?;
        }
        // Read Parquet files and apply sort order
        let frame = ctx
            .read_parquet(self.config.input_files().clone(), po)
            .await?;
        // Do we have partition bounds?
        Ok(
            if let Some(expr) = Option::<Expr>::from(self.config.region()) {
                frame.filter(expr)?
            } else {
                frame
            },
        )
    }

    /// Creates the sort order for a given schema.
    ///
    /// This is a list of the row key fields followed by the sort key fields.
    ///
    pub fn create_sort_order(&self) -> Vec<SortExpr> {
        self.config
            .sorting_columns_iter()
            .map(|s| ident(s).sort(true, false))
            .collect::<Vec<_>>()
    }

    /// Apply any configured filters to the `DataFusion` operation if any are present.
    ///
    /// # Errors
    /// An error will result if the frame cannot be filtered according to the given
    /// expression.
    fn apply_user_filters(&self, frame: DataFrame) -> Result<DataFrame, DataFusionError> {
        let mut out_frame = frame;
        for filter in self.config.filters() {
            out_frame = out_frame.filter(filter.create_filter_expr()?)?;
        }
        Ok(out_frame)
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
    /// If any configuration errors are present in the aggregations, e.g. duplicates or row key fields specified,
    /// then an error will result.
    fn apply_aggregations(&self, frame: DataFrame) -> Result<DataFrame, DataFusionError> {
        let unsorted_aggregates = self.config.aggregates();
        let aggregates = sort_aggregates_by_schema(unsorted_aggregates, frame.schema());
        if aggregates.is_empty() {
            return Ok(frame);
        }
        info!("Applying Sleeper aggregation: {aggregates:?}");
        let orig_schema = frame.schema().clone();
        let group_by_cols = self.config.sorting_columns();
        validate_aggregations(&group_by_cols, frame.schema(), &aggregates)?;

        let aggregation_expressions = aggregates
            .iter()
            .map(|agg| agg.to_expr(&frame))
            .collect::<Result<Vec<_>, _>>()?;

        let agg_frame = frame.aggregate(
            group_by_cols.iter().map(|e| ident(*e)).collect(),
            aggregation_expressions,
        )?;
        let agg_schema = agg_frame.schema().clone();

        // Cast column schemas back if necessary
        add_numeric_casts(agg_frame, &orig_schema, &agg_schema)
    }

    /// Create a sketching object to manage creation of quantile sketches.
    #[must_use]
    pub fn create_sketcher(&self, schema: &DFSchema) -> Sketcher<'_> {
        Sketcher::new(self.config.row_key_cols(), schema)
    }

    /// Add a Parquet output stage on to a frame.
    ///
    /// # Errors
    /// If the result logical plan could not be built.
    pub fn plan_with_parquet_output(
        &self,
        frame: DataFrame,
        configurer: &ParquetWriterConfigurer<'_>,
    ) -> Result<DataFrame, DataFusionError> {
        let OutputType::File {
            output_file,
            write_sketch_file: _,
            opts: _,
        } = self.config.output()
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

    /// Convert a frame to a physical plan.
    ///
    /// The physical will be modified to ensure the sort ordering
    /// is maintained.
    pub async fn to_physical_plan(
        &self,
        frame: DataFrame,
        sort_ordering: Option<&LexOrdering>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // We manually go through the stages of getting the plan, optimising it and then converting to a physical
        // plan so that we have a place to fix the optimised logical plan due to <https://github.com/apache/datafusion/issues/18214>.
        let (state, logical_plan) = frame.into_parts();
        let optimised_plan = state.optimize(&logical_plan)?;
        // Consume frame and generate initial physical plan
        let mut physical_plan = state
            .query_planner()
            .create_physical_plan(&optimised_plan, &state)
            .await?;
        if let Some(order) = sort_ordering {
            // Unalias field names if this is going to be Arrow output
            physical_plan = self.remove_aliased_columns(physical_plan, order)?;
            // Apply workaround to sorting problem to remove CoalescePartitionsExec from top of plan
            physical_plan = remove_coalesce_physical_stage(order, physical_plan)?;
            // Apply full sort ordering to all SortPreservingMergeExec stages to workaround sorting bug where only some row
            // key fields appear in sort expression
            physical_plan = apply_full_sort_ordering(order, physical_plan)?;
        }

        // Check physical plan is free of `SortExec` stages.
        // Issue <https://github.com/gchq/sleeper/issues/5248>
        if self.config.input_files_sorted() {
            check_for_sort_exec(&physical_plan)?;
        }
        Ok(physical_plan)
    }

    /// Remove any potentially aliased column names.
    ///
    /// This will look for the first projection and remove any column names aliasing
    /// it might have introduced. This ensures the client asking for Arrow output gets
    /// the correct column names.
    fn remove_aliased_columns(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
        ordering: &LexOrdering,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(
            if matches!(self.config.output(), OutputType::ArrowRecordBatch) {
                // Remove aliased column names to make things look correct for Java
                let physical_plan = unalias_view_projection_columns(physical_plan)?;
                // This may have been done in parallel, which will break sort order, so add a SortPreservingMergeExec stage
                if output_partition_count(&physical_plan) > 1 {
                    Arc::new(SortPreservingMergeExec::new(
                        ordering.clone(),
                        physical_plan,
                    ))
                } else {
                    physical_plan
                }
            } else {
                physical_plan
            },
        )
    }

    ///Create a lexical ordering for sorting fields on a frame.
    ///
    /// The lexical ordering is based on the row-keys and sort-keys for the Sleeper
    /// operation.
    ///
    /// Returns [`None`] if there are no sorting columns.
    ///
    /// # Errors
    /// The columns in the schema must match the row and sort key field names.
    pub fn create_sort_expr_ordering(
        &self,
        frame: &DataFrame,
    ) -> Result<Option<LexOrdering>, DataFusionError> {
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

    /// Create appropriate output completer.
    #[must_use]
    pub fn create_output_completer(&self) -> Box<dyn Completer + '_> {
        self.config.output().finisher(self)
    }
}

impl std::fmt::Display for SleeperOperations<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.config)
    }
}

/// Sort aggregate expressions according to position in schema.
#[must_use]
fn sort_aggregates_by_schema(aggregates: &[Aggregate], schema: &DFSchema) -> Vec<Aggregate> {
    let mut sorted_aggregates = aggregates.to_vec();
    sorted_aggregates.sort_by_key(|e| schema.index_of_column_by_name(None, &e.column));
    sorted_aggregates
}

#[cfg(test)]
mod tests {
    use crate::{
        datafusion::sort_aggregates_by_schema, filter_aggregation_config::aggregate::Aggregate,
    };
    use color_eyre::eyre::Result;
    use datafusion::dataframe;

    #[test]
    fn should_not_change_sorted_order() -> Result<()> {
        // Given
        let d = dataframe![
            "col1" => [1, 2, 3],
            "col2" => ["a", "b", "c"],
            "col3" => [true, false, true],
        ]?;
        let aggregates = Aggregate::parse_config("sum(col1),min(col2)")?;

        // When
        let result = sort_aggregates_by_schema(&aggregates, d.schema());

        // Then
        assert_eq!(result, Aggregate::parse_config("sum(col1),min(col2)")?);
        Ok(())
    }

    #[test]
    fn should_not_change_sorted_order_all_columns() -> Result<()> {
        // Given
        let d = dataframe![
            "col1" => [1, 2, 3],
            "col2" => ["a", "b", "c"],
            "col3" => [true, false, true],
        ]?;
        let aggregates = Aggregate::parse_config("sum(col1),min(col2),max(col3)")?;

        // When
        let result = sort_aggregates_by_schema(&aggregates, d.schema());

        // Then
        assert_eq!(
            result,
            Aggregate::parse_config("sum(col1),min(col2),max(col3)")?
        );
        Ok(())
    }

    #[test]
    fn should_reorder_incorrect_order() -> Result<()> {
        // Given
        let d = dataframe![
            "col1" => [1, 2, 3],
            "col2" => ["a", "b", "c"],
            "col3" => [true, false, true],
        ]?;
        let aggregates = Aggregate::parse_config("min(col2),sum(col1)")?;

        // When
        let result = sort_aggregates_by_schema(&aggregates, d.schema());

        // Then
        assert_eq!(result, Aggregate::parse_config("sum(col1),min(col2)")?);
        Ok(())
    }

    #[test]
    fn should_handle_single_col() -> Result<()> {
        // Given
        let d = dataframe![
            "col1" => [1, 2, 3],
            "col2" => ["a", "b", "c"],
            "col3" => [true, false, true],
        ]?;
        let aggregates = Aggregate::parse_config("min(col2)")?;

        // When
        let result = sort_aggregates_by_schema(&aggregates, d.schema());

        // Then
        assert_eq!(result, Aggregate::parse_config("min(col2)")?);
        Ok(())
    }
}
