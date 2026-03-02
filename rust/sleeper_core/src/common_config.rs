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
    datafusion::{OutputType, SleeperRegion},
    filter_aggregation_config::{aggregate::Aggregate, filter::Filter},
};
use aws_config::Region;
use aws_credential_types::Credentials;
use color_eyre::eyre::{Result, bail};
use object_store::aws::AmazonS3Builder;
use objectstore_ext::s3::{ObjectStoreFactory, config_for_s3_module, default_creds_store};
use std::fmt::{Display, Formatter};
use url::Url;

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
    /// Should we use readahead when reading from S3?
    use_readahead_store: bool,
    /// Should Parquet page indexes be read?
    read_page_indexes: bool,
    /// Names of row-key fields
    row_key_cols: Vec<String>,
    /// Names of sort-key fields
    sort_key_cols: Vec<String>,
    /// Ranges for each field to filter input files
    region: SleeperRegion<'a>,
    /// How output from operation should be returned
    output: OutputType,
    /// Aggregate functions to be applied AFTER merge-sorting
    aggregates: Vec<Aggregate>,
    /// Row filters to be applied before aggregation
    filters: Vec<Filter>,
}

impl CommonConfig<'_> {
    /// Get iterator for row and sort key fields in order
    pub(crate) fn sorting_columns_iter(&self) -> impl Iterator<Item = &str> {
        self.row_key_cols
            .iter()
            .chain(&self.sort_key_cols)
            .map(String::as_str)
    }

    /// List all row and sort key fields in order
    #[must_use]
    pub(crate) fn sorting_columns(&self) -> Vec<&str> {
        self.sorting_columns_iter().collect::<Vec<_>>()
    }

    pub(crate) async fn create_object_store_factory(&self) -> ObjectStoreFactory {
        let s3_config = match &self.aws_config {
            Some(aws_config) => Some(aws_config.to_s3_config()),
            None => default_creds_store().await.ok(),
        };
        ObjectStoreFactory::new(s3_config, self.use_readahead_store)
    }

    pub(crate) fn output(&self) -> &OutputType {
        &self.output
    }

    pub(crate) fn input_files(&self) -> &Vec<Url> {
        &self.input_files
    }

    pub(crate) fn read_page_indexes(&self) -> bool {
        self.read_page_indexes
    }

    pub(crate) fn input_files_sorted(&self) -> bool {
        self.input_files_sorted
    }

    pub(crate) fn row_key_cols(&self) -> &Vec<String> {
        &self.row_key_cols
    }

    pub(crate) fn sort_key_cols(&self) -> &Vec<String> {
        &self.sort_key_cols
    }

    pub(crate) fn region(&self) -> &SleeperRegion<'_> {
        &self.region
    }

    pub(crate) fn aggregates(&self) -> &Vec<Aggregate> {
        &self.aggregates
    }

    pub(crate) fn filters(&self) -> &Vec<Filter> {
        &self.filters
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
            OutputType::ArrowRecordBatch => write!(f, "output is Arrow RecordBatches"),
            OutputType::File {
                output_file,
                write_sketch_file,
                opts: _,
            } => write!(
                f,
                "output file {output_file:?} write sketch file {write_sketch_file}"
            ),
        }
    }
}

/// Builder for `CommonConfig`.
pub struct CommonConfigBuilder<'a> {
    aws_config: Option<AwsConfig>,
    input_files: Vec<Url>,
    input_files_sorted: bool,
    use_readahead_store: bool,
    read_page_indexes: bool,
    row_key_cols: Vec<String>,
    sort_key_cols: Vec<String>,
    region: SleeperRegion<'a>,
    output: OutputType,
    aggregates: Vec<Aggregate>,
    filters: Vec<Filter>,
}

impl Default for CommonConfigBuilder<'_> {
    fn default() -> Self {
        Self {
            aws_config: None,
            input_files: Vec::default(),
            input_files_sorted: true,
            use_readahead_store: true,
            read_page_indexes: false,
            row_key_cols: Vec::default(),
            sort_key_cols: Vec::default(),
            region: SleeperRegion::default(),
            output: OutputType::default(),
            aggregates: Vec::default(),
            filters: Vec::default(),
        }
    }
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
    pub fn use_readahead_store(mut self, use_readahead_store: bool) -> Self {
        self.use_readahead_store = use_readahead_store;
        self
    }

    #[must_use]
    pub fn read_page_indexes(mut self, read_page_indexes: bool) -> Self {
        self.read_page_indexes = read_page_indexes;
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
    pub fn aggregates(mut self, aggregates: Vec<Aggregate>) -> Self {
        self.aggregates = aggregates;
        self
    }

    #[must_use]
    pub fn filters(mut self, filters: Vec<Filter>) -> Self {
        self.filters = filters;
        self
    }

    /// Build the `CommonConfig`, consuming the builder and validating required fields.
    ///
    /// # Errors
    /// The configuration must validate. Input files mustn't be empty
    /// and the number of row key fields must match the number of region
    /// dimensions.
    pub fn build(mut self) -> Result<CommonConfig<'a>> {
        self.validate()?;
        self.normalise_s3a_urls();

        Ok(CommonConfig {
            aws_config: self.aws_config,
            input_files: self.input_files,
            input_files_sorted: self.input_files_sorted,
            use_readahead_store: self.use_readahead_store,
            read_page_indexes: self.read_page_indexes,
            row_key_cols: self.row_key_cols,
            sort_key_cols: self.sort_key_cols,
            region: self.region,
            output: self.output,
            aggregates: self.aggregates,
            filters: self.filters,
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

    /// Change all input and output URLS from s3a to s3 scheme.
    fn normalise_s3a_urls(&mut self) {
        for t in &mut self.input_files {
            if t.scheme() == "s3a" {
                let _ = t.set_scheme("s3");
            }
        }

        if let OutputType::File {
            output_file,
            write_sketch_file: _,
            opts: _,
        } = &mut self.output
            && output_file.scheme() == "s3a"
        {
            let _ = output_file.set_scheme("s3");
        }
    }
}

#[derive(Debug)]
pub struct AwsConfig {
    pub region: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
    pub allow_http: bool,
}

impl AwsConfig {
    /// Create an [`AmazonS3Builder`] from the given configuration object.
    ///
    /// Credentials are extracted from the given configuration object.
    #[must_use]
    fn to_s3_config(&self) -> AmazonS3Builder {
        let creds = Credentials::from_keys(
            &self.access_key,
            &self.secret_key,
            self.session_token.clone(),
        );
        let region = Region::new(String::from(&self.region));
        let mut builder = config_for_s3_module(&creds, &region);
        if !self.endpoint.is_empty() {
            builder = builder.with_endpoint(&self.endpoint);
        }
        builder.with_allow_http(self.allow_http)
    }
}

#[cfg(test)]
mod tests {
    use crate::datafusion::{ColRange, PartitionBound, SleeperParquetOptions};
    use std::collections::HashMap;

    use super::*;
    use url::Url;

    fn normalise_s3a_urls(input_files: Vec<Url>, output: OutputType) -> (Vec<Url>, OutputType) {
        let config = CommonConfigBuilder::new()
            .input_files(input_files)
            .output(output)
            .build()
            .unwrap();
        (config.input_files, config.output)
    }

    #[test]
    fn test_convert_s3a_scheme_in_input_files() {
        // Given
        let input_files = vec![
            Url::parse("s3a://bucket/key1").unwrap(),
            Url::parse("s3a://bucket/key2").unwrap(),
        ];
        let output = OutputType::File {
            output_file: Url::parse("https://example.com/output").unwrap(),
            write_sketch_file: true,
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
            write_sketch_file: true,
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
            write_sketch_file: true,
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
        let result = builder.build();

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
        let result = builder.build();

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
        let result = builder.build();

        // Then
        assert!(result.is_ok());
    }
}
