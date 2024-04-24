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
use crate::aws_s3::ObjectStoreFactory;
use aws_config::BehaviorVersion;
use aws_credential_types::provider::ProvideCredentials;
use color_eyre::eyre::{eyre, Result};

use std::{collections::HashMap, path::PathBuf};
use url::Url;

/// Type safe variant for Sleeper partition boundary
#[derive(Debug, Copy, Clone)]
pub enum PartitionBound {
    Int32(i32),
    Int64(i64),
    String(&'static str),
    ByteArray(&'static [u8]),
}

/// All the information for a a Sleeper compaction.
#[derive(Debug)]
pub struct CompactionInput {
    pub input_files: Vec<Url>,
    pub output_file: Url,
    pub row_key_cols: Vec<String>,
    pub sort_key_cols: Vec<String>,
    pub max_row_group_size: usize,
    pub max_page_size: usize,
    pub compression: String,
    pub writer_version: String,
    pub column_truncate_length: usize,
    pub stats_truncate_length: usize,
    pub dict_enc_row_keys: bool,
    pub dict_enc_sort_keys: bool,
    pub dict_enc_values: bool,
    pub region: HashMap<String, ColRange>,
}

/// Defines a partition range of a single column.
#[derive(Debug, Copy, Clone)]
pub struct ColRange {
    pub lower: PartitionBound,
    pub lower_inclusive: bool,
    pub upper: PartitionBound,
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
/// # use arrow::error::ArrowError;
/// # use url::Url;
/// # use aws_types::region::Region;
/// # use crate::compaction::merge_sorted_files;
/// # tokio_test::block_on(async {
/// let result = merge_sorted_files(None, &Region::new("eu-west-2"), &vec![Url::parse("file:///path/to/file1.parquet").unwrap()], &Url::parse("file:///path/to/output").unwrap(), 65535, 1_000_000, vec![0], vec![0]).await;
/// # })
/// ```
///
/// # Errors
/// There must be at least one input file.
///
pub async fn merge_sorted_files(input_data: &CompactionInput) -> Result<CompactionResult> {
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

        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let region = config
            .region()
            .ok_or(eyre!("Couldn't retrieve AWS region"))?;
        let creds: aws_credential_types::Credentials = config
            .credentials_provider()
            .ok_or(eyre!("Couldn't retrieve AWS credentials"))?
            .provide_credentials()
            .await?;

        // Create our object store factory
        let store_factory = ObjectStoreFactory::new(Some(creds), region);

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
