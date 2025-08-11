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
    CommonConfig, OperationOutput, SleeperPartitionRegion,
    datafusion::{ParquetWriterConfigurer, SleeperOperations},
};
#[cfg(doc)]
use arrow::record_batch::RecordBatch;
use datafusion::{
    error::DataFusionError,
    prelude::{DataFrame, SessionConfig, SessionContext},
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

/// Contains the query output result.
#[derive(Debug, Default)]
pub enum LeafQueryOutput {
    /// Results are returned via an iterator of Arrow [`RecordBatch`]es.
    #[default]
    ArrowBatches,
    /// Results have been written to a Parquet file.
    ParquetFile,
}

/// Executes a Sleeper leaf partition query.
///
/// The object store factory must be able to produce an [`object_store::ObjectStore`] capable of reading
/// from the input URLs and writing to the output URL.
pub async fn query(
    store_factory: &ObjectStoreFactory,
    config: &LeafPartitionQueryConfig<'_>,
) -> Result<LeafQueryOutput, DataFusionError> {
    let ops = SleeperOperations::new(&config.common);
    info!("DataFusion compaction: {ops}");

    todo!();
}

/// Creates the [`DataFrame`] for a leaf partition query.
///
/// This reads the Parquet and configures the frame's plan
/// to sort, filter and aggregate as necessary
///
/// # Errors
/// Each step of query may produce an error. Any are reported back to the caller.
async fn build_query_dataframe<'a>(
    ops: &'a SleeperOperations<'a>,
    store_factory: &ObjectStoreFactory,
) -> Result<DataFrame, DataFusionError> {
    let sf = prepare_session_config(ops, store_factory).await?;
    let ctx = ops.apply_to_context(SessionContext::new_with_config(sf), store_factory)?;
    let mut frame = ops.create_initial_partitioned_read(&ctx).await?;
    frame = ops.apply_user_filters(frame)?;
    frame = ops.apply_general_sort(frame)?;
    frame = ops.apply_aggregations(frame)?;
    let sketcher = ops.create_sketcher(frame.schema());
    frame = sketcher.apply_sketch(frame)?;

    Ok(frame)
}

/// Create the [`SessionConfig`] for a query.
async fn prepare_session_config<'a>(
    ops: &'a SleeperOperations<'a>,
    store_factory: &ObjectStoreFactory,
) -> Result<SessionConfig, DataFusionError> {
    let sf = ops
        .apply_config(SessionConfig::new(), store_factory)
        .await?;
    Ok(
        if let OperationOutput::File {
            output_file,
            opts: parquet_options,
        } = &ops.config.output
        {
            // Create Parquet configuration object based on requested output
            let configurer = ParquetWriterConfigurer { parquet_options };
            configurer.apply_parquet_config(sf)
        } else {
            sf
        },
    )
}
