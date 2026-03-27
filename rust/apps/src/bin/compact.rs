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
use apps::path_absolute;
use chrono::Local;
use clap::Parser;
use color_eyre::eyre::bail;
use env_logger::Env;
use human_panic::setup_panic;
use log::info;
use num_format::{Locale, ToFormattedString};
use sleeper_core::{
    ColRange, CommonConfigBuilder, OutputType, PartitionBound, SleeperParquetOptions,
    SleeperRegion,
    filter_aggregation_config::{aggregate::Aggregate, filter::Filter},
    run_compaction,
    sleeper_context::SleeperContext,
};
use std::{collections::HashMap, io::Write};
use url::Url;

/// Runs a Sleeper compaction algorithm.
///
/// A sequence of Parquet files is read and compacted into a single output Parquet file. The input
/// files must be individually sorted according to the row key fields and then the sort fields. A sketches file containing
/// serialised Apache Data Sketches quantiles sketches is written for reach row key field.
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
    /// Column names for a row key fields
    #[arg(short = 'k', long, num_args=1.., required=true)]
    row_keys: Vec<String>,
    /// Column names for sort key fields
    #[arg(short = 's', long)]
    sort_keys: Vec<String>,
    /// Partition region minimum keys (inclusive). Must be one per row key specified.
    #[arg(short='m',long,required=true,num_args=1..)]
    region_mins: Vec<String>,
    /// Partition region maximum keys (exclusive). Must be one per row key specified.
    #[arg(short='n',long,required=true,num_args=1..)]
    region_maxs: Vec<String>,
    /// Sleeper aggregation configuration
    #[arg(short = 'a', long, required = false, num_args = 1)]
    aggregation_config: Option<String>,
    /// Sleeper filter configuration
    #[arg(short = 'f', long, required = false, num_args = 1)]
    filter_config: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> color_eyre::Result<()> {
    // Install coloured errors
    color_eyre::install().unwrap();

    // Install human readable panics
    setup_panic!();

    // Install and configure environment logger
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
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
    let output_file = Url::parse(&args.output)
        .or_else(|_e| Url::parse(&("file://".to_owned() + &path_absolute(&args.output))))?;

    if args.row_keys.len() != args.region_maxs.len() {
        bail!("quantity of region maximums != quantity of row key fields");
    }
    if args.row_keys.len() != args.region_mins.len() {
        bail!("quantity of region minimums != quantity of row key fields");
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

    let details = CommonConfigBuilder::new()
        .aws_config(None)
        .input_files(input_urls)
        .input_files_sorted(true)
        .row_key_cols(args.row_keys)
        .sort_key_cols(args.sort_keys)
        .region(SleeperRegion::new(map))
        .output(OutputType::File {
            output_file,
            write_sketch_file: true,
            opts: parquet_options,
        })
        .aggregates(Aggregate::parse_config(
            &args.aggregation_config.unwrap_or_default(),
        )?)
        .filters(Filter::parse_config(
            &args.filter_config.unwrap_or_default(),
        )?)
        .build()?;

    let result = run_compaction(&details, &SleeperContext::default()).await?;
    info!(
        "Compaction read {} rows and wrote {} rows",
        result.rows_read.to_formatted_string(&Locale::en),
        result.rows_written.to_formatted_string(&Locale::en)
    );

    Ok(())
}
