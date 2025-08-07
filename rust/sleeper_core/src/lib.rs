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
use ::datafusion::{common::plan_err, error::DataFusionError};
use aws_config::Region;
use aws_credential_types::Credentials;
use color_eyre::eyre::{Result, bail, eyre};
use object_store::aws::AmazonS3Builder;
use objectstore_ext::s3::{ObjectStoreFactory, config_for_s3_module, default_creds_store};
use url::Url;

pub use datafusion::{
    SleeperPartitionRegion,
    sketch::{DataSketchVariant, deserialise_sketches},
};

mod datafusion;

/// Type safe variant for Sleeper partition boundary
#[derive(Debug, Copy, Clone)]
pub enum PartitionBound<'a> {
    Int32(i32),
    Int64(i64),
    String(&'a str),
    ByteArray(&'a [u8]),
    /// Represented by a NULL in Java
    Unbounded,
}

/// Defines a partition range of a single column.
#[derive(Debug, Copy, Clone)]
pub struct ColRange<'a> {
    pub lower: PartitionBound<'a>,
    pub lower_inclusive: bool,
    pub upper: PartitionBound<'a>,
    pub upper_inclusive: bool,
}

/// All Parquet output options supported by Sleeper.
#[derive(Debug)]
pub struct SleeperParquetOptions {
    pub max_row_group_size: usize,
    pub max_page_size: usize,
    pub compression: String,
    pub writer_version: String,
    pub column_truncate_length: usize,
    pub stats_truncate_length: usize,
    pub dict_enc_row_keys: bool,
    pub dict_enc_sort_keys: bool,
    pub dict_enc_values: bool,
}

impl Default for SleeperParquetOptions {
    fn default() -> Self {
        Self {
            max_row_group_size: 1_000_000,
            max_page_size: 65535,
            compression: "zstd".into(),
            writer_version: "v2".into(),
            column_truncate_length: usize::MAX,
            stats_truncate_length: usize::MAX,
            dict_enc_row_keys: true,
            dict_enc_sort_keys: true,
            dict_enc_values: true,
        }
    }
}

/// Common items necessary to perform any DataFusion related
/// work for Sleeper.
#[derive(Debug, Default)]
pub struct CommonConfig<'a> {
    /// Aws credentials configuration
    pub aws_config: Option<AwsConfig>,
    /// Input file URLs
    pub input_files: Vec<Url>,
    /// Names of row-key columns
    pub row_key_cols: Vec<String>,
    /// Names of sort-key columns
    pub sort_key_cols: Vec<String>,
    /// Ranges for each column to filter input files
    pub region: SleeperPartitionRegion<'a>,
    /// How output from operation should be returned
    pub output: OperationOutput,
}

/// Defines how operation output should be given.
#[derive(Debug, Default)]
pub enum OperationOutput {
    /// `DataFusion` results will be returned as a stream of Arrow [`RecordBatch`]es.
    #[default]
    ArrowRecordBatch,
    /// `DataFusion` results will be written to a file with given Parquet options.
    File {
        /// Output file Url
        output_file: Url,
        /// Parquet output options
        opts: SleeperParquetOptions,
    },
}

/// All the information for a Sleeper compaction.
#[derive(Debug)]
pub struct SleeperCompactionConfig<'a> {
    /// Common configuration
    pub common: CommonConfig<'a>,
    /// Iterator config. Filters, aggregators, etc.
    pub iterator_config: Option<String>,
}

impl Default for SleeperCompactionConfig<'_> {
    fn default() -> Self {
        Self {
            common: CommonConfig::default(),
            iterator_config: Option::default(),
        }
    }
}

impl SleeperCompactionConfig<'_> {
    /// Convenience function to return region.
    pub fn region(&self) -> &SleeperPartitionRegion {
        &self.common.region
    }

    pub fn output_file(&self) -> Result<&Url, DataFusionError> {
        if let OperationOutput::File {
            output_file,
            opts: _,
        } = &self.common.output
        {
            Ok(output_file)
        } else {
            plan_err!("Sleeper compactions must output to a file")
        }
    }
}

#[derive(Debug)]
pub struct AwsConfig {
    pub region: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub allow_http: bool,
}

/// Contains compaction results.
///
/// This provides the details of compaction results that Sleeper
/// will use to update its record keeping.
///
pub struct CompactionResult {
    /// The total number of rows read by a compaction.
    pub rows_read: usize,
    /// The total number of rows written by a compaction.
    pub rows_written: usize,
}

/// Merges the given Parquet files and reads the schema from the first.
///
/// This function reads the schema from the first file, then calls
/// `merge_sorted_files_with_schema(...)`.
///
/// The `aws_creds` are optional if you are not attempting to read/write files from S3.
///
/// # Examples
/// ```no_run
/// # use url::Url;
/// # use aws_types::region::Region;
/// # use std::collections::HashMap;
/// # use crate::sleeper_core::{run_compaction, CompactionInput, PartitionBound, ColRange};
/// let mut compaction_input = CompactionInput::default();
/// compaction_input.common.input_files = vec![Url::parse("file:///path/to/file1.parquet").unwrap()];
/// compaction_input.output_file = Url::parse("file:///path/to/output").unwrap();
/// compaction_input.common.row_key_cols = vec!["key".into()];
/// let mut region : HashMap<String, ColRange<'_>> = HashMap::new();
/// region.insert("key".into(), ColRange {
///     lower : PartitionBound::String("a"),
///     lower_inclusive: true,
///     upper: PartitionBound::String("h"),
///     upper_inclusive: true,
/// });
///
/// # tokio_test::block_on(async {
/// let result = run_compaction(&compaction_input).await;
/// # })
/// ```
///
/// # Errors
/// There must be at least one input file.
///
pub async fn run_compaction(input_data: &SleeperCompactionConfig<'_>) -> Result<CompactionResult> {
    // Read the schema from the first file
    if input_data.common.input_files.is_empty() {
        Err(eyre!("No input paths supplied"))
    } else {
        // Java tends to use s3a:// URI scheme instead of s3:// so map it here
        let input_file_paths: Vec<Url> = input_data
            .common
            .input_files
            .iter()
            .map(|u| {
                let mut t = u.clone();
                if t.scheme() == "s3a" {
                    let _ = t.set_scheme("s3");
                }
                t
            })
            .collect();

        // Compactions must write back to a file

        // Change output file scheme
        let mut output_file_path = input_data.output_file()?.clone();
        if output_file_path.scheme() == "s3a" {
            let _ = output_file_path.set_scheme("s3");
        }

        if input_data.common.row_key_cols.len() != input_data.common.region.len() {
            bail!(
                "Length mismatch between row keys {} and partition region bounds {}",
                input_data.common.row_key_cols.len(),
                input_data.common.region.len()
            );
        }

        let store_factory =
            create_object_store_factory(input_data.common.aws_config.as_ref()).await;

        crate::datafusion::compact(
            &store_factory,
            input_data,
            &input_file_paths,
            &output_file_path,
        )
        .await
        .map_err(Into::into)
    }
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
    config_for_s3_module(&creds, &region)
        .with_endpoint(&aws_config.endpoint)
        .with_allow_http(aws_config.allow_http)
}
