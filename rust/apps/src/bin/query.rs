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
use arrow::util::pretty::print_batches;
use chrono::Local;
use clap::Parser;
use color_eyre::eyre::bail;
use env_logger::Env;
use futures::StreamExt;
use human_panic::setup_panic;
use sleeper_core::{
    ColRange, CommonConfigBuilder, CompletedOutput, LeafPartitionQueryConfig, OutputType,
    PartitionBound, SleeperRegion,
    filter_aggregation_config::{aggregate::Aggregate, filter::Filter},
    run_query,
    sleeper_context::SleeperContext,
};
use std::{collections::HashMap, io::Write};
use url::Url;

/// Implements a Sleeper query algorithm.
///
/// A sequence of Parquet files is read and merge sorted. The input
/// files must be individually sorted according to the row key fields and then the sort fields. A selection
/// of query results are written to standard output.
///
#[derive(Parser, Debug)]
#[command(author, version)]
struct CmdLineArgs {
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
    /// Query region minimum keys (inclusive). Must be one per row key specified.
    #[arg(short='q',long,required=true,num_args=1..)]
    query_mins: Vec<String>,
    /// Query region maximum keys (exclusive). Must be one per row key specified.
    #[arg(short='w',long,required=true,num_args=1..)]
    query_maxs: Vec<String>,
    /// Row count to write to standard output
    #[arg(short = 'c', long, required = false, default_value_t = 1000)]
    row_count: usize,
    /// Sleeper aggregation configuration
    #[arg(short = 'a', long, required = false, num_args = 1)]
    aggregation_config: Option<String>,
    /// Sleeper filter configuration
    #[arg(short = 'f', long, required = false, num_args = 1)]
    filter_config: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
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

    if args.row_keys.len() != args.region_maxs.len() {
        bail!("quantity of region maximums != quantity of row key fields");
    }
    if args.row_keys.len() != args.region_mins.len() {
        bail!("quantity of region minimums != quantity of row key fields");
    }
    if args.row_keys.len() != args.query_maxs.len() {
        bail!("quantity of query region maximums != quantity of row key fields");
    }
    if args.row_keys.len() != args.query_mins.len() {
        bail!("quantity of query region minimums != quantity of row key fields");
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

    let mut query_map = HashMap::new();
    for (key, bounds) in args
        .row_keys
        .iter()
        .zip(args.query_mins.iter().zip(args.query_maxs.iter()))
    {
        query_map.insert(
            key.into(),
            ColRange {
                lower: PartitionBound::String(bounds.0),
                lower_inclusive: true,
                upper: PartitionBound::String(bounds.1),
                upper_inclusive: false,
            },
        );
    }

    let common = CommonConfigBuilder::new()
        .aws_config(None)
        .input_files(input_urls)
        .input_files_sorted(true)
        .row_key_cols(args.row_keys)
        .sort_key_cols(args.sort_keys)
        .region(SleeperRegion::new(map))
        .output(OutputType::ArrowRecordBatch)
        .aggregates(Aggregate::parse_config(
            &args.aggregation_config.unwrap_or_default(),
        )?)
        .filters(Filter::parse_config(
            &args.filter_config.unwrap_or_default(),
        )?)
        .build()?;

    let query_config = LeafPartitionQueryConfig {
        common,
        ranges: vec![SleeperRegion::new(query_map)],
        requested_value_fields: None,
        explain_plans: true,
    };

    let result = run_query(&query_config, &SleeperContext::default()).await?;
    let CompletedOutput::ArrowRecordBatch(mut stream) = result else {
        bail!("Expected ArrowRecordBatch output");
    };

    let mut batches = Vec::new();
    let mut rows_needed = args.row_count;
    while let Some(Ok(batch)) = stream.next().await
        && rows_needed > 0
    {
        let batch_size = batch.num_rows();
        let rows_from_batch = std::cmp::min(rows_needed, batch_size);
        let partial = batch.slice(0, rows_from_batch);
        batches.push(partial);
        rows_needed -= rows_from_batch;
    }

    print_batches(&batches)?;
    Ok(())
}
