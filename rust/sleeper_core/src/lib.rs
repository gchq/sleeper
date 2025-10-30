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
use crate::datafusion::{CompactionResult, LeafPartitionQuery};
use aws_config::Region;
use aws_credential_types::Credentials;
use color_eyre::eyre::Result;
use object_store::aws::AmazonS3Builder;
use objectstore_ext::s3::{ObjectStoreFactory, config_for_s3_module, default_creds_store};

mod common_config;
mod datafusion;
pub mod filter_aggregation_config;
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
/// # use crate::sleeper_core::{run_compaction, CommonConfig, CommonConfigBuilder, PartitionBound, ColRange,
/// # OutputType, SleeperParquetOptions, SleeperRegion};
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
/// let result = run_compaction(&compaction_input).await;
/// # });
/// # Ok(())
/// # }
/// ```
///
/// # Errors
/// There must be at least one input file.
///
pub async fn run_compaction(config: &CommonConfig<'_>) -> Result<CompactionResult> {
    let store_factory = create_object_store_factory(config.aws_config()).await;
    crate::datafusion::compact(&store_factory, config)
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
/// # use crate::sleeper_core::{run_query, CommonConfigBuilder, PartitionBound, ColRange,
/// # OutputType, SleeperParquetOptions, SleeperRegion};
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
/// let result = run_query(&leaf_config).await;
/// # });
/// # Ok(())
/// # }
/// ```
///
/// # Errors
/// There must be at least one input file.
/// There must be at least one query region specified.
///
pub async fn run_query(config: &LeafPartitionQueryConfig<'_>) -> Result<CompletedOutput> {
    let store_factory = create_object_store_factory(config.common.aws_config()).await;

    LeafPartitionQuery::new(config, &store_factory)
        .run_query()
        .await
        .map_err(Into::into)
}

async fn create_object_store_factory(
    aws_config_override: Option<&AwsConfig>,
) -> ObjectStoreFactory {
    let s3_config = match aws_config_override {
        Some(aws_config) => Some(to_s3_config(aws_config)),
        None => default_creds_store().await.ok(),
    };
    ObjectStoreFactory::new(s3_config)
}

/// Create an [`AmazonS3Builder`] from the given configuration object.
///
/// Credentials are extracted from the given configuration object.
#[must_use]
pub fn to_s3_config(aws_config: &AwsConfig) -> AmazonS3Builder {
    let creds = Credentials::from_keys(&aws_config.access_key, &aws_config.secret_key, None);
    let region = Region::new(String::from(&aws_config.region));
    let mut builder = config_for_s3_module(&creds, &region);
    if !aws_config.endpoint.is_empty() {
        builder = builder.with_endpoint(&aws_config.endpoint);
    }
    builder.with_allow_http(aws_config.allow_http)
}
