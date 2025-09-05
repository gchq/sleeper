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
#[cfg(doc)]
use arrow::record_batch::RecordBatch;
use aws_config::Region;
use aws_credential_types::Credentials;
use color_eyre::eyre::{Result, bail};
use object_store::aws::AmazonS3Builder;
use objectstore_ext::s3::{ObjectStoreFactory, config_for_s3_module, default_creds_store};
use std::fmt::{Display, Formatter};
use url::Url;

mod datafusion;

pub use crate::datafusion::output::CompletedOutput;
pub use datafusion::{
    LeafPartitionQueryConfig, OutputType, SleeperRegion,
    sketch::{DataSketchVariant, deserialise_sketches},
    stream_to_ffi_arrow_stream,
};

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
#[derive(Debug)]
pub struct CommonConfig<'a> {
    /// Aws credentials configuration
    aws_config: Option<AwsConfig>,
    /// Input file URLs
    input_files: Vec<Url>,
    /// Are input files individually sorted?
    input_files_sorted: bool,
    /// Names of row-key columns
    row_key_cols: Vec<String>,
    /// Names of sort-key columns
    sort_key_cols: Vec<String>,
    /// Ranges for each column to filter input files
    region: SleeperRegion<'a>,
    /// How output from operation should be returned
    output: OutputType,
    /// Iterator config. Filters, aggregators, etc.
    iterator_config: Option<String>,
}

impl Default for CommonConfig<'_> {
    fn default() -> Self {
        Self {
            aws_config: Option::default(),
            input_files: Vec::default(),
            input_files_sorted: true,
            row_key_cols: Vec::default(),
            sort_key_cols: Vec::default(),
            region: SleeperRegion::default(),
            output: OutputType::default(),
            iterator_config: Option::default(),
        }
    }
}

/// Change all input and output URLS from s3a to s3 scheme.
fn normalise_s3a_urls(mut input_files: Vec<Url>, mut output: OutputType) -> (Vec<Url>, OutputType) {
    for t in &mut input_files {
        if t.scheme() == "s3a" {
            let _ = t.set_scheme("s3");
        }
    }

    if let OutputType::File {
        output_file,
        opts: _,
    } = &mut output
        && output_file.scheme() == "s3a"
    {
        let _ = output_file.set_scheme("s3");
    }
    (input_files, output)
}

impl CommonConfig<'_> {
    /// Get iterator for row and sort key columns in order
    pub fn sorting_columns_iter(&self) -> impl Iterator<Item = &str> {
        self.row_key_cols
            .iter()
            .chain(&self.sort_key_cols)
            .map(String::as_str)
    }

    /// List all roy and sort key columns in order
    #[must_use]
    pub fn sorting_columns(&self) -> Vec<&str> {
        self.sorting_columns_iter().collect::<Vec<_>>()
    }
}

impl Display for CommonConfig<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "input files {:?}, partition region {:?}, ",
            self.input_files.iter().map(Url::as_str).collect::<Vec<_>>(),
            self.region
        )?;
        match &self.output {
            OutputType::ArrowRecordBatch => write!(f, " output is Arrow RecordBatches"),
            OutputType::File {
                output_file,
                opts: _,
            } => write!(f, "output file {output_file:?}"),
        }
    }
}

/// Builder for `CommonConfig`.
#[derive(Default)]
pub struct CommonConfigBuilder<'a> {
    aws_config: Option<AwsConfig>,
    input_files: Vec<Url>,
    input_files_sorted: bool,
    row_key_cols: Vec<String>,
    sort_key_cols: Vec<String>,
    region: SleeperRegion<'a>,
    output: OutputType,
    iterator_config: Option<String>,
}

impl<'a> CommonConfigBuilder<'a> {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn aws_config(mut self, aws_config: Option<AwsConfig>) -> Self {
        self.aws_config = aws_config;
        self
    }

    #[must_use]
    pub fn input_files(mut self, input_files: Vec<Url>) -> Self {
        self.input_files = input_files;
        self
    }

    #[must_use]
    pub fn input_files_sorted(mut self, input_files_sorted: bool) -> Self {
        self.input_files_sorted = input_files_sorted;
        self
    }

    #[must_use]
    pub fn row_key_cols(mut self, row_key_cols: Vec<String>) -> Self {
        self.row_key_cols = row_key_cols;
        self
    }

    #[must_use]
    pub fn sort_key_cols(mut self, sort_key_cols: Vec<String>) -> Self {
        self.sort_key_cols = sort_key_cols;
        self
    }

    #[must_use]
    pub fn region(mut self, region: SleeperRegion<'a>) -> Self {
        self.region = region;
        self
    }

    #[must_use]
    pub fn output(mut self, output: OutputType) -> Self {
        self.output = output;
        self
    }

    #[must_use]
    pub fn iterator_config(mut self, iterator_config: Option<String>) -> Self {
        self.iterator_config = iterator_config;
        self
    }

    /// Build the `CommonConfig`, consuming the builder and validating required fields.
    ///
    /// # Errors
    /// The configuration must validate. Input files mustn't be empty
    /// and the number of row key columns must match the number of region
    /// dimensions.
    pub fn build(self) -> Result<CommonConfig<'a>> {
        self.validate()?;

        // s3a normalization
        let (input_files, output) = normalise_s3a_urls(self.input_files, self.output);

        Ok(CommonConfig {
            aws_config: self.aws_config,
            input_files,
            input_files_sorted: self.input_files_sorted,
            row_key_cols: self.row_key_cols,
            sort_key_cols: self.sort_key_cols,
            region: self.region,
            output,
            iterator_config: self.iterator_config,
        })
    }

    /// Performs validity checks on parameters.
    ///
    /// # Errors
    /// There must be at least one input file.
    /// The length of `row_key_cols` must match the number of region dimensions.
    fn validate(&self) -> Result<()> {
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

#[derive(Debug)]
pub struct AwsConfig {
    pub region: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub allow_http: bool,
}

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
/// # OutputType, SleeperParquetOptions, SleeperPartitionRegion};
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
///     .output(OutputType::File{ output_file: Url::parse("file:///path/to/output").unwrap(), opts: SleeperParquetOptions::default() })
///     .row_key_cols(vec!["key".into()])
///     .region(SleeperPartitionRegion::new(region))
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
    let store_factory = create_object_store_factory(config.aws_config.as_ref()).await;
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
/// # OutputType, SleeperParquetOptions, SleeperPartitionRegion};
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
///     .output(OutputType::File{ output_file: Url::parse("file:///path/to/output").unwrap(), opts: SleeperParquetOptions::default() })
///     .row_key_cols(vec!["key".into()])
///     .region(SleeperPartitionRegion::new(region))
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
/// leaf_config.ranges = vec![SleeperPartitionRegion::new(query_region)];
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
    let store_factory = create_object_store_factory(config.common.aws_config.as_ref()).await;

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
    config_for_s3_module(&creds, &region)
        .with_endpoint(&aws_config.endpoint)
        .with_allow_http(aws_config.allow_http)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use url::Url;

    #[test]
    fn test_convert_s3a_scheme_in_input_files() {
        // Given
        let input_files = vec![
            Url::parse("s3a://bucket/key1").unwrap(),
            Url::parse("s3a://bucket/key2").unwrap(),
        ];
        let output = OutputType::File {
            output_file: Url::parse("https://example.com/output").unwrap(),
            opts: SleeperParquetOptions::default(),
        };

        // When
        let (new_files, new_output) = normalise_s3a_urls(input_files, output);

        // Then
        for url in new_files {
            assert_eq!(url.scheme(), "s3");
        }
        if let OutputType::File { output_file, .. } = new_output {
            assert_eq!(output_file.scheme(), "https"); // unchanged
        } else {
            panic!("Output option changed unexpectedly")
        }
    }

    #[test]
    fn test_no_change_for_non_s3a_urls() {
        // Given
        let input_files = vec![Url::parse("https://example.com/key").unwrap()];
        let output = OutputType::File {
            output_file: Url::parse("https://example.com/output").unwrap(),
            opts: SleeperParquetOptions::default(),
        };

        // When
        let (new_files, new_output) = normalise_s3a_urls(input_files, output);

        // Then
        assert_eq!(new_files[0].scheme(), "https");
        if let OutputType::File { output_file, .. } = new_output {
            assert_eq!(output_file.scheme(), "https"); // unchanged
        } else {
            panic!("Output option changed unexpectedly")
        }
    }

    #[test]
    fn test_convert_output_scheme_when_s3a() {
        // Given
        let input_files = vec![Url::parse("https://example.com/key").unwrap()];
        let output = OutputType::File {
            output_file: Url::parse("s3a://bucket/output").unwrap(),
            opts: SleeperParquetOptions::default(),
        };

        // When
        let (_, new_output) = normalise_s3a_urls(input_files, output);

        // Then
        if let OutputType::File { output_file, .. } = new_output {
            assert_eq!(output_file.scheme(), "s3");
        } else {
            panic!("Unexpected output option type")
        }
    }

    #[test]
    fn test_empty_input_files() {
        // Given
        let input_files: Vec<Url> = vec![];
        let output = OutputType::File {
            output_file: Url::parse("https://example.com/output").unwrap(),
            opts: SleeperParquetOptions::default(),
        };

        // When
        let (new_files, _) = normalise_s3a_urls(input_files, output);

        // Then
        assert!(new_files.is_empty());
    }

    #[test]
    fn test_normalise_s3a_urls_arrow_record_batch() {
        // Given
        let input_files = vec![
            Url::parse("s3a://bucket/key1").unwrap(),
            Url::parse("s3a://bucket/key2").unwrap(),
        ];
        let output = OutputType::ArrowRecordBatch;

        // When
        let (new_files, new_output) = normalise_s3a_urls(input_files.clone(), output);

        // Then
        for url in new_files {
            assert_eq!(url.scheme(), "s3");
        }

        match new_output {
            OutputType::ArrowRecordBatch => {}
            OutputType::File { .. } => panic!("Output should be ArrowRecordBatch"),
        }
    }

    #[test]
    fn test_validate_no_input_files() {
        // Given
        let input_files: Vec<Url> = vec![];
        let row_key_cols = vec!["key".to_string()];
        let region = SleeperRegion::default();
        let builder = CommonConfigBuilder::new()
            .input_files(input_files)
            .row_key_cols(row_key_cols)
            .region(region);

        // When
        let result = builder.validate();

        // Then
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().to_string(), "No input paths supplied");
    }

    #[test]
    fn test_validate_row_key_mismatch() {
        // Given
        let input_files = vec![Url::parse("file:///path/to/file.parquet").unwrap()];
        let row_key_cols = vec!["key1".to_string(), "key2".to_string()];
        let region = SleeperRegion::new(HashMap::from([(
            "col".to_string(),
            ColRange {
                lower: PartitionBound::String("a"),
                lower_inclusive: true,
                upper: PartitionBound::String("z"),
                upper_inclusive: true,
            },
        )]));

        // When
        let builder = CommonConfigBuilder::new()
            .input_files(input_files)
            .row_key_cols(row_key_cols)
            .region(region);

        // When
        let result = builder.validate();

        // Then
        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("Length mismatch")
        );
    }

    #[test]
    fn test_validate_success() {
        // Given
        let input_files = vec![Url::parse("file:///path/to/file.parquet").unwrap()];
        let row_key_cols = vec!["key".to_string()];
        let region = SleeperRegion::new(HashMap::from([(
            "col".to_string(),
            ColRange {
                lower: PartitionBound::String("a"),
                lower_inclusive: true,
                upper: PartitionBound::String("z"),
                upper_inclusive: true,
            },
        )]));

        let builder = CommonConfigBuilder::new()
            .input_files(input_files)
            .row_key_cols(row_key_cols)
            .region(region);

        // When
        let result = builder.validate();

        // Then
        assert!(result.is_ok());
    }
}
