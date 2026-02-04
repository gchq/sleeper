//! Contains the implementation for performing Sleeper leaf queries
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
    CommonConfig, SleeperRegion,
    datafusion::{
        OutputType, SleeperOperations,
        output::CompletedOutput,
        sketch::{Sketcher, output_sketch},
        util::{explain_plan, retrieve_object_metas},
    },
};
#[cfg(doc)]
use arrow::record_batch::RecordBatch;
use datafusion::{
    common::plan_err,
    dataframe::DataFrame,
    error::DataFusionError,
    execution::{config::SessionConfig, context::SessionContext, runtime_env::RuntimeEnv},
    logical_expr::{Expr, ident},
    physical_plan::displayable,
};
use log::info;
use objectstore_ext::s3::ObjectStoreFactory;
use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};

/// All information needed for a Sleeper leaf partition query.
#[derive(Debug)]
pub struct LeafPartitionQueryConfig<'a> {
    /// Basic information
    pub common: CommonConfig<'a>,
    /// Query ranges
    pub ranges: Vec<SleeperRegion<'a>>,
    /// Requested value fields for Sleeper query
    pub requested_value_fields: Option<Vec<String>>,
    /// Should logical/physical plan explanation be logged?
    pub explain_plans: bool,
}

impl Display for LeafPartitionQueryConfig<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Query config: {}, query ranges: {:?}, requested_value_fields: {:?}",
            self.common, self.ranges, self.requested_value_fields
        )
    }
}

/// Manages and executes a Sleeper leaf partition query.
#[derive(Debug)]
pub struct LeafPartitionQuery<'a> {
    /// The configuration information for the leaf query
    config: &'a LeafPartitionQueryConfig<'a>,
    /// Used to create object store implementations
    store_factory: &'a ObjectStoreFactory,
    /// Runtime for this query
    runtime: Arc<RuntimeEnv>,
}

impl<'a> LeafPartitionQuery<'a> {
    pub fn new(
        config: &'a LeafPartitionQueryConfig<'a>,
        store_factory: &'a ObjectStoreFactory,
        runtime: Arc<RuntimeEnv>,
    ) -> LeafPartitionQuery<'a> {
        Self {
            config,
            store_factory,
            runtime,
        }
    }

    /// Executes a Sleeper leaf partition query.
    ///
    /// The object store factory must be able to produce an [`object_store::ObjectStore`] capable of reading
    /// from the input URLs and writing to the output URL (if writing results to a file).
    ///
    /// # Errors
    /// There must be at least one query region specified.
    pub async fn run_query(&self) -> Result<CompletedOutput, DataFusionError> {
        if self.config.ranges.is_empty() {
            return plan_err!("No query regions specified");
        }
        let ops = SleeperOperations::new(&self.config.common);
        info!("DataFusion query: {}", self.config);
        // Create query frame and sketches if it has been enabled
        let (sketcher, frame) = self.build_query_dataframe(&ops).await?;

        // Convert to physical plan
        let sort_ordering = ops.create_sort_expr_ordering(&frame)?;
        let completer = ops.create_output_completer();

        let frame = completer.complete_frame(frame)?;
        let task_ctx = Arc::new(frame.task_ctx());

        if self.config.explain_plans {
            explain_plan(&frame).await?;
        }

        let physical_plan = ops.to_physical_plan(frame, sort_ordering.as_ref()).await?;

        if self.config.explain_plans {
            info!(
                "Physical plan\n{}",
                displayable(&*physical_plan).indent(true)
            );
        }

        // Run query
        let result = completer.execute_frame(physical_plan, task_ctx).await?;

        // Do we have some sketch output to write?
        if let Some(sketch_func) = sketcher
            && let OutputType::File {
                output_file,
                write_sketch_file,
                opts: _,
            } = self.config.common.output()
            && *write_sketch_file
        {
            output_sketch(self.store_factory, output_file, sketch_func.sketch()).await?;
        }

        Ok(result)
    }

    /// Adds a quantile sketch to a query plan if sketch generation is enabled.
    ///
    /// # Errors
    /// If sketch output is requested, then file output must be chosen in the query config.
    fn maybe_add_sketch_output(
        &self,
        ops: &'a SleeperOperations<'a>,
        frame: DataFrame,
    ) -> Result<(Option<Sketcher<'a>>, DataFrame), DataFusionError> {
        match self.config.common.output() {
            OutputType::File {
                output_file: _,
                write_sketch_file: true,
                opts: _,
            } => {
                let sketcher = ops.create_sketcher(frame.schema());
                let frame = sketcher.apply_sketch(frame)?;
                Ok((Some(sketcher), frame))
            }
            OutputType::File {
                output_file: _,
                write_sketch_file: _,
                opts: _,
            }
            | OutputType::ArrowRecordBatch => Ok((None, frame)),
        }
    }

    /// If only certain value fields have been requested, then project them.
    ///
    /// Row key and sort key fields are always projected.
    fn maybe_project_columns(&self, frame: DataFrame) -> Result<DataFrame, DataFusionError> {
        if let Some(value_fields) = &self.config.requested_value_fields {
            let project_columns = self
                .config
                .common
                .sorting_columns_iter()
                .chain(value_fields.iter().map(String::as_str))
                .map(ident);
            frame.select(project_columns)
        } else {
            Ok(frame)
        }
    }

    /// Creates the [`DataFrame`] for a leaf partition query.
    ///
    /// This reads the Parquet and configures the frame's plan
    /// to sort, filter and aggregate as necessary
    ///
    /// # Errors
    /// Each step of query may produce an error. Any are reported back to the caller.
    async fn build_query_dataframe(
        &self,
        ops: &'a SleeperOperations<'a>,
    ) -> Result<(Option<Sketcher<'a>>, DataFrame), DataFusionError> {
        let object_metas =
            retrieve_object_metas(ops.config.input_files(), self.store_factory).await?;
        let sf = ops
            .apply_config(SessionConfig::new(), &object_metas)
            .await?;
        let ctx = ops.configure_context(
            SessionContext::new_with_config_rt(sf, self.runtime.clone()),
            self.store_factory,
        )?;
        let mut frame = ops
            .create_initial_partitioned_read(&ctx, &object_metas)
            .await?;
        frame = self.apply_query_regions(frame)?;
        frame = ops.apply_user_filters(frame)?;
        frame = ops.apply_general_sort(frame)?;
        frame = ops.apply_aggregations(frame)?;
        frame = self.maybe_project_columns(frame)?;
        self.maybe_add_sketch_output(ops, frame)
    }
}

impl LeafPartitionQuery<'_> {
    /// Apply the query regions to the frame.
    ///
    /// The list of query regions are created and then OR'd together and
    /// added to the [`DataFrame`], this will ultimately be AND'd with the
    /// initial Sleeper partition region.
    pub fn apply_query_regions(&self, frame: DataFrame) -> Result<DataFrame, DataFusionError> {
        let mut query_expr: Option<Expr> = None;
        for region in &self.config.ranges {
            if let Some(expr) = Option::<Expr>::from(region) {
                query_expr = match query_expr {
                    Some(original) => Some(original.or(expr)),
                    None => Some(expr),
                }
            }
        }
        // If we have any filters apply to frame (will AND with any previous filter)
        Ok(if let Some(expr) = query_expr {
            frame.filter(expr)?
        } else {
            frame
        })
    }
}
