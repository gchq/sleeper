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
    pub region: HashMap<String, ColRange<'a>>,
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
