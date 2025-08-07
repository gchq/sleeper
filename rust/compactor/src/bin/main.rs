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
use chrono::Local;
use clap::Parser;
use color_eyre::eyre::bail;
use human_panic::setup_panic;
use log::info;
use num_format::{Locale, ToFormattedString};
use sleeper_core::{
    ColRange, CommonConfig, CompactionInput, PartitionBound, SleeperParquetOptions,
    SleeperPartitionRegion, run_compaction,
};
use std::{collections::HashMap, io::Write, path::Path};
use url::Url;

/// Implements a Sleeper compaction algorithm in Rust.
///
/// A sequence of Parquet files is read and compacted into a single output Parquet file. The input
/// files must be individually sorted according to the row key columns and then the sort columns. A sketches file containing
/// serialised Apache Data Sketches quantiles sketches is written for reach row key column.
///
#[derive(Parser, Debug)]
#[command(author, version)]
struct CmdLineArgs {
    /// The output file URL for the compacted Parquet data
    output: String,
    /// Set the maximum number of rows in a row group
    #[arg(short = 'r', long, default_value = "1000000")]
    row_group_size: usize,
    /// Set the maximum number of bytes per data page (hint)
    #[arg(short = 'p', long, default_value = "65535")]
    max_page_size: usize,
    /// List of input Parquet files (must be sorted) as URLs
    #[arg(num_args=1.., required=true)]
    input: Vec<String>,
    /// Column names for a row key columns
    #[arg(short = 'k', long, num_args=1.., required=true)]
    row_keys: Vec<String>,
    /// Column names for sort key columns
    #[arg(short = 's', long)]
    sort_keys: Vec<String>,
    /// Partition region minimum keys (inclusive). Must be one per row key specified.
    #[arg(short='m',long,required=true,num_args=1..)]
    region_mins: Vec<String>,
    /// Partition region maximum keys (exclusive). Must be one per row key specified.
    #[arg(short='n',long,required=true,num_args=1..)]
    region_maxs: Vec<String>,
    /// Sleeper iterator configuration
    #[arg(short = 'i', long, required = false, num_args = 1)]
    iterator_config: Option<String>,
}

/// Converts a [`Path`] reference to an absolute path (if not already absolute)
/// and returns it as a String.
///
/// # Panics
/// If the path can't be made absolute due to not being able to get the current
/// directory or the path is not valid.
fn path_absolute<T: ?Sized + AsRef<Path>>(path: &T) -> String {
    std::path::absolute(path).unwrap().to_str().unwrap().into()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> color_eyre::Result<()> {
    // Install coloured errors
    color_eyre::install().unwrap();

    // Install human readable panics
    setup_panic!();

    // Install and configure environment logger
    env_logger::builder()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] {}:{} - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.file().unwrap_or("??"),
                record.line().unwrap_or(0),
                record.args()
            )
        })
        .format_timestamp(Some(env_logger::TimestampPrecision::Millis))
        .filter_level(log::LevelFilter::Info)
        .format_target(false)
        .init();

    let args = CmdLineArgs::parse();

    // Check URL conversion
    let input_urls = args
        .input
        .iter()
        .map(|x| {
            Url::parse(x).or_else(|_e| Url::parse(&("file://".to_owned() + &path_absolute(x))))
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Convert output URL
    let output_url = Url::parse(&args.output)
        .or_else(|_e| Url::parse(&("file://".to_owned() + &path_absolute(&args.output))))?;

    if args.row_keys.len() != args.region_maxs.len() {
        bail!("quantity of region maximums != quantity of row key columns");
    }
    if args.row_keys.len() != args.region_mins.len() {
        bail!("quantity of region minimums != quantity of row key columns");
    }
    let mut map = HashMap::new();
    for (key, bounds) in args
        .row_keys
        .iter()
        .zip(args.region_mins.iter().zip(args.region_maxs.iter()))
    {
        map.insert(
            key.into(),
            ColRange {
                lower: PartitionBound::String(bounds.0),
                lower_inclusive: true,
                upper: PartitionBound::String(bounds.1),
                upper_inclusive: false,
            },
        );
    }

    let parquet_options = SleeperParquetOptions {
        max_page_size: args.max_page_size,
        max_row_group_size: args.row_group_size,
        column_truncate_length: 1_048_576,
        stats_truncate_length: 1_048_576,
        compression: "zstd".into(),
        writer_version: "v2".into(),
        dict_enc_row_keys: true,
        dict_enc_sort_keys: true,
        dict_enc_values: true,
    };

    let details = CompactionInput {
        common: CommonConfig {
            aws_config: None,
            input_files: input_urls,
            row_key_cols: args.row_keys,
            sort_key_cols: args.sort_keys,
            region: SleeperPartitionRegion::new(map),
        },
        output_file: output_url,
        parquet_options,
        iterator_config: args.iterator_config,
    };

    let result = run_compaction(&details).await;
    match result {
        Ok(r) => {
            info!(
                "Compaction read {} rows and wrote {} rows",
                r.rows_read.to_formatted_string(&Locale::en),
                r.rows_written.to_formatted_string(&Locale::en)
            );
        }
        Err(e) => {
            bail!(e);
        }
    }
    Ok(())
}

#[cfg(test)]
mod path_test {
    use crate::path_absolute;

    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    fn cd_to_tmp() {
        std::env::set_current_dir("/tmp").unwrap();
    }

    #[test]
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    fn relative_path_converts() {
        cd_to_tmp();
        assert_eq!("/tmp/foo/bar/baz.txt", path_absolute("foo/bar/baz.txt"));
    }

    #[test]
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    fn relative_path_converts_with_one_dot() {
        cd_to_tmp();
        assert_eq!("/tmp/foo/bar/baz.txt", path_absolute("./foo/bar/baz.txt"));
    }

    #[test]
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    fn relative_path_converts_with_double_dot() {
        cd_to_tmp();
        assert_eq!(
            "/tmp/../foo/bar/baz.txt",
            path_absolute("../foo/bar/baz.txt")
        );
    }

    #[test]
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    fn absolute_path_unchanged() {
        cd_to_tmp();
        assert_eq!("/tmp/foo/bar", path_absolute("/tmp/foo/bar"));
    }

    #[test]
    #[should_panic(expected = "cannot make an empty path absolute")]
    fn empty_path_panic() {
        let _ = path_absolute("");
    }
}
