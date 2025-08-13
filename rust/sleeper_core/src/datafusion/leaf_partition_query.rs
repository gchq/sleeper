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
    CommonConfig, CompletionOptions, SleeperPartitionRegion,
    datafusion::{
        ParquetWriterConfigurer, SleeperOperations, sketch::Sketcher, util::explain_plan,
    },
};
#[cfg(doc)]
use arrow::record_batch::RecordBatch;
use datafusion::{common::plan_err, logical_expr::Expr};
use datafusion::{
    dataframe::DataFrame,
    error::DataFusionError,
    execution::{config::SessionConfig, context::SessionContext},
};
use log::info;
use objectstore_ext::s3::ObjectStoreFactory;
use std::fmt::{Display, Formatter};

/// All information needed for a Sleeper leaf partition query.
#[derive(Debug, Default)]
pub struct LeafPartitionQueryConfig<'a> {
    /// Basic information
    pub common: CommonConfig<'a>,
    /// Query ranges
    pub ranges: Vec<SleeperPartitionRegion<'a>>,
    /// Should sketches be produced?
    pub write_quantile_sketch: bool,
    /// Should logical/physical plan explanation be logged?
    pub explain_plans: bool,
}

impl Display for LeafPartitionQueryConfig<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Query config: {}, ranges: {:?} write quantile sketches: {}",
            self.common, self.ranges, self.write_quantile_sketch
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
}

impl<'a> LeafPartitionQuery<'a> {
    pub fn new(
        config: &'a LeafPartitionQueryConfig<'a>,
        store_factory: &'a ObjectStoreFactory,
    ) -> LeafPartitionQuery<'a> {
        Self {
            config,
            store_factory,
        }
    }

    /// Executes a Sleeper leaf partition query.
    ///
    /// The object store factory must be able to produce an [`object_store::ObjectStore`] capable of reading
    /// from the input URLs and writing to the output URL (if writing results to a file).
    pub async fn run_query(&self) -> Result<(), DataFusionError> {
        let ops = SleeperOperations::new(&self.config.common);
        info!("DataFusion compaction: {ops}");
        // Create query frame an sketches if it has been enabled
        let (sketcher, frame) = self.build_query_dataframe(&ops).await?;

        if self.config.explain_plans {
            explain_plan(&frame).await?;
        }
        todo!();
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
        if self.config.write_quantile_sketch {
            match self.config.common.output {
                CompletionOptions::File {
                    output_file: _,
                    opts: _,
                } => {
                    let sketcher = ops.create_sketcher(frame.schema());
                    let frame = sketcher.apply_sketch(frame)?;
                    Ok((Some(sketcher), frame))
                }
                CompletionOptions::ArrowRecordBatch => plan_err!(
                    "Quantile sketch output cannot be enabled if file output not selected"
                ),
            }
        } else {
            Ok((None, frame))
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
        let sf = ops
            .apply_config(SessionConfig::new(), &self.store_factory)
            .await?;
        let ctx = ops.configure_context(SessionContext::new_with_config(sf), self.store_factory)?;
        let mut frame = ops.create_initial_partitioned_read(&ctx).await?;
        frame = self.apply_query_regions(frame)?;
        frame = ops.apply_user_filters(frame)?;
        frame = ops.apply_general_sort(frame)?;
        frame = ops.apply_aggregations(frame)?;
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
