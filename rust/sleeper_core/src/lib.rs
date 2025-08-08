//! The `compaction` crate implements all the core functionality for running Sleeper
//! Parquet data compaction in Rust. We provide a C library interface wrapper which
//! will serve as the interface from Java code in Sleeper. We are careful to adhere to C style
//! conventions here such as libc error codes.
//!
//! We have an internal "details" module that encapsulates the internal workings. All the
//! public API should be in this module.
use std::fmt::Formatter;

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
#[cfg(doc)]
use arrow::record_batch::RecordBatch;
use aws_config::Region;
use aws_credential_types::Credentials;
use color_eyre::eyre::{Result, bail, eyre};
pub use datafusion::{
    SleeperPartitionRegion,
    sketch::{DataSketchVariant, deserialise_sketches},
};
use object_store::aws::AmazonS3Builder;
use objectstore_ext::s3::{ObjectStoreFactory, config_for_s3_module, default_creds_store};
use url::Url;

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

/// Common items necessary to perform any `DataFusion` related
/// work for Sleeper.
#[derive(Debug, Default)]
pub struct CommonConfig<'a> {
    /// Aws credentials configuration
    pub aws_config: Option<AwsConfig>,
    /// Input file URLs
    pub input_files: Vec<Url>,
    /// Are input files individually sorted?
    pub input_files_sorted: bool,
    /// Names of row-key columns
    pub row_key_cols: Vec<String>,
    /// Names of sort-key columns
    pub sort_key_cols: Vec<String>,
    /// Ranges for each column to filter input files
    pub region: SleeperPartitionRegion<'a>,
    /// How output from operation should be returned
    pub output: OperationOutput,
    /// Iterator config. Filters, aggregators, etc.
    pub iterator_config: Option<String>,
}

impl CommonConfig<'_> {
    /// Convert all Java "s3a" URLs in input and output to "s3."
    pub fn sanitise_java_s3_urls(&mut self) {
        self.input_files.iter_mut().for_each(|t| {
            if t.scheme() == "s3a" {
                let _ = t.set_scheme("s3");
            }
        });

        if let OperationOutput::File {
            output_file,
            opts: _,
        } = &mut self.output
            && output_file.scheme() == "s3a"
        {
            let _ = output_file.set_scheme("s3");
        }
    }

    /// Checks for simple configuration errors
    ///
    /// # Errors
    /// It is an error for input paths to be empty or for a length
    /// mismatch between row key columns length and number of ranges in
    /// partition region.
    pub fn validate(&self) -> Result<()> {
        if self.input_files.is_empty() {
            bail!("No input paths supplied");
        }
        if self.row_key_cols.len() != self.region.len() {
            bail!(
                "Length mismatch between row keys {} and partition region bounds {}",
                self.row_key_cols.len(),
                self.region.len()
            );
        }
        Ok(())
    }
}

impl std::fmt::Display for CommonConfig<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "input files {:?}, partition region {:?}, ",
            self.input_files.iter().map(Url::as_str).collect::<Vec<_>>(),
            self.region
        )?;
        match &self.output {
            OperationOutput::ArrowRecordBatch => write!(f, " output is Arrow RecordBatches"),
            OperationOutput::File {
                output_file,
                opts: _,
            } => write!(f, "output file {output_file:?}"),
        }
    }
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
#[derive(Debug, Default)]
pub struct SleeperCompactionConfig<'a> {
    /// Common configuration
    pub common: CommonConfig<'a>,
}

impl SleeperCompactionConfig<'_> {
    /// Convenience function to return region.
    #[must_use]
    pub fn region(&self) -> &SleeperPartitionRegion<'_> {
        &self.common.region
    }

    /// Get input files for Sleeper compaction.
    #[must_use]
    pub fn input_files(&self) -> &Vec<Url> {
        &self.common.input_files
    }

    /// Get the output file for a Sleeper compaction.
    ///
    /// # Errors
    /// Since compactions must output to a file, an error will occur
    /// if Arrow stream output is selected.
    pub fn output_file(&self) -> Result<&Url> {
        if let OperationOutput::File {
            output_file,
            opts: _,
        } = &self.common.output
        {
            Ok(output_file)
        } else {
            Err(eyre!("Sleeper compactions must output to a file"))
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
/// # use crate::sleeper_core::{run_compaction, SleeperCompactionConfig, PartitionBound, ColRange,
/// # OperationOutput, SleeperParquetOptions};
/// let mut compaction_input = SleeperCompactionConfig::default();
/// compaction_input.common.input_files_sorted = true;
/// compaction_input.common.input_files = vec![Url::parse("file:///path/to/file1.parquet").unwrap()];
/// compaction_input.common.output = OperationOutput::File{ output_file: Url::parse("file:///path/to/output").unwrap(), opts: SleeperParquetOptions::default() };
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
    input_data.common.validate()?;
    let store_factory = create_object_store_factory(input_data.common.aws_config.as_ref()).await;

    crate::datafusion::compact(&store_factory, input_data)
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
    config_for_s3_module(&creds, &region)
        .with_endpoint(&aws_config.endpoint)
        .with_allow_http(aws_config.allow_http)
}
