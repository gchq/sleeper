//! The `internal` module contains the internal functionality and error conditions
//! to actually implement the compaction library.
/*
 * Copyright 2022-2024 Crown Copyright
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
use crate::aws_s3::{default_s3_config, s3_config, ObjectStoreFactory};
use aws_config::Region;
use aws_credential_types::Credentials;
use color_eyre::eyre::{eyre, Result};
use object_store::aws::AmazonS3Builder;

use std::{collections::HashMap, path::PathBuf};
use url::Url;

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

/// All the information for a a Sleeper compaction.
#[derive(Debug)]
pub struct CompactionInput<'a> {
    pub aws_config: Option<AwsConfig>,
    // File URLs
    pub input_files: Vec<Url>,
    pub output_file: Url,
    // Names of row & sort key columns
    pub row_key_cols: Vec<String>,
    pub sort_key_cols: Vec<String>,
    // Parquet options
    pub max_row_group_size: usize,
    pub max_page_size: usize,
    pub compression: String,
    pub writer_version: String,
    pub column_truncate_length: usize,
    pub stats_truncate_length: usize,
    pub dict_enc_row_keys: bool,
    pub dict_enc_sort_keys: bool,
    pub dict_enc_values: bool,
    // Ranges for each column to filter input files
    pub region: HashMap<String, ColRange<'a>>,
}

impl Default for CompactionInput<'_> {
    fn default() -> Self {
        Self {
            aws_config: None,
            input_files: Vec::default(),
            output_file: Url::parse("file:///").unwrap(),
            row_key_cols: Vec::default(),
            sort_key_cols: Vec::default(),
            max_row_group_size: 1_000_000,
            max_page_size: 65535,
            compression: "zstd".into(),
            writer_version: "v2".into(),
            column_truncate_length: usize::MAX,
            stats_truncate_length: usize::MAX,
            dict_enc_row_keys: true,
            dict_enc_sort_keys: true,
            dict_enc_values: true,
            region: HashMap::default(),
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

/// Defines a partition range of a single column.
#[derive(Debug, Copy, Clone)]
pub struct ColRange<'a> {
    pub lower: PartitionBound<'a>,
    pub lower_inclusive: bool,
    pub upper: PartitionBound<'a>,
    pub upper_inclusive: bool,
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
/// # use crate::compaction::{merge_sorted_files, CompactionInput, PartitionBound, ColRange};
/// let mut compaction_input = CompactionInput::default();
/// compaction_input.input_files = vec![Url::parse("file:///path/to/file1.parquet").unwrap()];
/// compaction_input.output_file = Url::parse("file:///path/to/output").unwrap();
/// compaction_input.row_key_cols = vec!["key".into()];
/// let mut region : HashMap<String, ColRange<'_>> = HashMap::new();
/// region.insert("key".into(), ColRange {
///     lower : PartitionBound::String("a"),
///     lower_inclusive: true,
///     upper: PartitionBound::String("h"),
///     upper_inclusive: true,
/// });
///
/// # tokio_test::block_on(async {
/// let result = merge_sorted_files(&compaction_input).await;
/// # })
/// ```
///
/// # Errors
/// There must be at least one input file.
///
pub async fn merge_sorted_files(input_data: &CompactionInput<'_>) -> Result<CompactionResult> {
    // Read the schema from the first file
    if input_data.input_files.is_empty() {
        Err(eyre!("No input paths supplied"))
    } else {
        // Java tends to use s3a:// URI scheme instead of s3:// so map it here
        let input_file_paths: Vec<Url> = input_data
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

        // Change output file scheme
        let mut output_file_path = input_data.output_file.clone();
        if output_file_path.scheme() == "s3a" {
            let _ = output_file_path.set_scheme("s3");
        }

        let store_factory = create_object_store_factory(input_data.aws_config.as_ref()).await;

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
        None => default_s3_config().await.ok(),
    };
    ObjectStoreFactory::new(s3_config)
}

fn to_s3_config(aws_config: &AwsConfig) -> AmazonS3Builder {
    let creds = Credentials::from_keys(&aws_config.access_key, &aws_config.secret_key, None);
    let region = Region::new(String::from(&aws_config.region));
    s3_config(&creds, &region)
        .with_endpoint(&aws_config.endpoint)
        .with_allow_http(aws_config.allow_http)
}

/// Creates a file path suitable for writing sketches to.
///
#[must_use]
pub fn create_sketch_path(output_path: &Url) -> Url {
    let mut res = output_path.clone();
    res.set_path(
        &PathBuf::from(output_path.path())
            .with_extension("sketches")
            .to_string_lossy(),
    );
    res
}
