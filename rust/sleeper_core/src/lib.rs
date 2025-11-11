//! The `compaction` crate implements all the core functionality for running Sleeper
//! Parquet data compaction in Rust. We provide a C library interface wrapper which
//! will serve as the interface from Java code in Sleeper. We are careful to adhere to C style
//! conventions here such as libc error codes.
//!
//! We have an internal "details" module that encapsulates the internal workings. All the
//! public API should be in this module.
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
    datafusion::{CompactionResult, LeafPartitionQuery},
    sleeper_context::SleeperContext,
};
use color_eyre::eyre::Result;

mod common_config;
mod datafusion;
pub mod filter_aggregation_config;
pub mod sleeper_context;
#[cfg(test)]
mod test_utils;

pub use crate::datafusion::output::CompletedOutput;
pub use common_config::{AwsConfig, CommonConfig, CommonConfigBuilder};
pub use datafusion::{
    ColRange, LeafPartitionQueryConfig, OutputType, PartitionBound, SleeperParquetOptions,
    SleeperRegion,
    sketch::{DataSketchVariant, deserialise_sketches},
    stream_to_ffi_arrow_stream,
};

/// Compacts the given Parquet files and reads the schema from the first.
///
/// The `aws_creds` are optional if you are not attempting to read/write files from S3.
///
/// # Examples
/// ```no_run
/// # use url::Url;
/// # use aws_types::region::Region;
/// # use std::collections::HashMap;
/// # use crate::sleeper_core::{sleeper_context::SleeperContext, run_compaction, CommonConfig, CommonConfigBuilder,
/// # PartitionBound, ColRange, OutputType, SleeperParquetOptions, SleeperRegion};
/// # fn main() -> Result<(), color_eyre::eyre::Report> {
/// let mut region : HashMap<String, ColRange<'_>> = HashMap::new();
/// region.insert("key".into(), ColRange {
///     lower : PartitionBound::String("a"),
///     lower_inclusive: true,
///     upper: PartitionBound::String("h"),
///     upper_inclusive: true,
/// });
/// let mut compaction_input = CommonConfigBuilder::new()
///     .input_files_sorted(true)
///     .input_files(vec![Url::parse("file:///path/to/file1.parquet").unwrap()])
///     .output(OutputType::File{ output_file: Url::parse("file:///path/to/output").unwrap(),
///         write_sketch_file: true,
///         opts: SleeperParquetOptions::default() })
///     .row_key_cols(vec!["key".into()])
///     .region(SleeperRegion::new(region))
///     .build()?;
/// # tokio_test::block_on(async {
/// let result = run_compaction(&compaction_input, &SleeperContext::default()).await;
/// # });
/// # Ok(())
/// # }
/// ```
///
/// # Errors
/// There must be at least one input file.
///
pub async fn run_compaction(
    config: &CommonConfig<'_>,
    sleeper_context: &SleeperContext,
) -> Result<CompactionResult> {
    let store_factory = config.create_object_store_factory().await;
    let rt = sleeper_context.retrieve_runtime_env()?;
    crate::datafusion::compact(&store_factory, config, rt)
        .await
        .map_err(Into::into)
}

/// Runs the given Sleeper leaf partition query on the given Parquet files and reads the schema from the first.
///
/// The `aws_creds` are optional if you are not attempting to read/write files from S3.
///
/// # Examples
/// ```no_run
/// # use url::Url;
/// # use aws_types::region::Region;
/// # use std::collections::HashMap;
/// # use crate::sleeper_core::{sleeper_context::SleeperContext, run_query, CommonConfigBuilder,
/// PartitionBound, ColRange, OutputType, SleeperParquetOptions, SleeperRegion};
/// # use sleeper_core::LeafPartitionQueryConfig;
/// # fn main() -> Result<(), color_eyre::eyre::Report> {
/// let mut region : HashMap<String, ColRange<'_>> = HashMap::new();
/// region.insert("key".into(), ColRange {
///     lower : PartitionBound::String("a"),
///     lower_inclusive: true,
///     upper: PartitionBound::String("h"),
///     upper_inclusive: true,
/// });
/// let mut common = CommonConfigBuilder::new()
///     .input_files_sorted(true)
///     .input_files(vec![Url::parse("file:///path/to/file1.parquet").unwrap()])
///     .output(OutputType::File{ output_file: Url::parse("file:///path/to/output").unwrap(),
///         write_sketch_file: true,
///         opts: SleeperParquetOptions::default() })
///     .row_key_cols(vec!["key".into()])
///     .region(SleeperRegion::new(region))
///     .build()?;
/// let mut leaf_config = LeafPartitionQueryConfig::default();
/// leaf_config.common = common;
/// let mut query_region : HashMap<String, ColRange<'_>> = HashMap::new();
/// query_region.insert("key".into(), ColRange {
///     lower : PartitionBound::String("a"),
///     lower_inclusive: true,
///     upper: PartitionBound::String("h"),
///     upper_inclusive: true,
/// });
/// leaf_config.ranges = vec![SleeperRegion::new(query_region)];
///
/// # tokio_test::block_on(async {
/// let result = run_query(&leaf_config, &SleeperContext::default()).await;
/// # });
/// # Ok(())
/// # }
/// ```
///
/// # Errors
/// There must be at least one input file.
/// There must be at least one query region specified.
///
pub async fn run_query(
    config: &LeafPartitionQueryConfig<'_>,
    sleeper_context: &SleeperContext,
) -> Result<CompletedOutput> {
    let store_factory = config.common.create_object_store_factory().await;
    let rt = sleeper_context.retrieve_runtime_env()?;

    LeafPartitionQuery::new(config, &store_factory, rt)
        .run_query()
        .await
        .map_err(Into::into)
}
