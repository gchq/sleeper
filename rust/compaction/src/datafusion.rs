/// `DataFusion` contains the implementation for performing Sleeper compactions
/// using Apache `DataFusion`.
///
/// This allows for multi-threaded compaction and optimised Parquet reading.
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
use crate::{
    aws_s3::{CountingObjectStore, ObjectStoreFactory},
    ColRange, CompactionInput, CompactionResult, PartitionBound,
};
use arrow::{array::RecordBatch, error::ArrowError, util::pretty::pretty_format_batches};
use datafusion::{
    dataframe::DataFrameWriteOptions,
    error::DataFusionError,
    execution::{config::SessionConfig, context::SessionContext, options::ParquetReadOptions},
    parquet::{
        arrow::{async_reader::ParquetObjectReader, ParquetRecordBatchStreamBuilder},
        basic::{BrotliLevel, GzipLevel, ZstdLevel},
    },
    prelude::*,
};
use log::{error, info};
use num_format::{Locale, ToFormattedString};
use std::{collections::HashMap, sync::Arc};
use url::Url;

/// Starts a Sleeper compaction.
///
/// The object store factory must be able to produce an [`ObjectStore`] capable of reading
/// from the input URLs and writing to the output URL. A sketch file will be produced for
/// the output file.
pub async fn compact(
    store_factory: &ObjectStoreFactory,
    input_data: &CompactionInput,
    input_paths: &[Url],
    output_path: &Url,
) -> Result<CompactionResult, DataFusionError> {
    info!("DataFusion compaction of {input_paths:?}");
    info!("Compaction region {:?}", input_data.region);
    let sf = create_session_cfg(input_data);
    let ctx = SessionContext::new_with_config(sf);

    // Register some object store from first input file and output file
    let store = register_store(store_factory, input_paths, output_path, &ctx)?;

    // Sort on row key columns then sort columns (nulls last)
    let sort_order = sort_order(input_data);
    info!("Row key and sort column order {sort_order:?}");

    // Tell DataFusion that the row key columns and sort columns are already sorted
    let po = ParquetReadOptions::default().file_sort_order(vec![sort_order.clone()]);
    let mut frame = ctx.read_parquet(input_paths.to_owned(), po).await?;

    // Extract all column names
    let col_names = frame.schema().clone().strip_qualifiers().field_names();
    info!("All columns in schema {col_names:?}");

    // If we have a partition region, apply it first
    if let Some(expr) = region_filter(&input_data.region) {
        frame = frame.filter(expr)?;
    }

    // Perform sort of row key and sort columns and projection of all columns
    let col_names_expr = frame
        .schema()
        .clone()
        .strip_qualifiers()
        .field_names()
        .iter()
        .map(col)
        .collect::<Vec<_>>();
    frame = frame.sort(sort_order)?.select(col_names_expr)?;

    // Show explanation of plan
    let explained = frame.clone().explain(false, false)?.collect().await?;
    let output = pretty_format_batches(&explained)?;
    info!("DataFusion plan:\n {output}");

    let mut pqo = ctx.copied_table_options().parquet;
    // Figure out which columns should be dictionary encoded
    for col in &col_names {
        let col_opts = pqo.column_specific_options.entry(col.into()).or_default();
        let dict_encode = (input_data.dict_enc_row_keys && input_data.row_key_cols.contains(col))
            || (input_data.dict_enc_sort_keys && input_data.sort_key_cols.contains(col))
            // Check value columns
            || (input_data.dict_enc_values
                && !input_data.row_key_cols.contains(col)
                && !input_data.sort_key_cols.contains(col));
        col_opts.dictionary_enabled = Some(dict_encode);
    }

    let _ = frame
        .write_parquet(
            output_path.as_str(),
            DataFrameWriteOptions::new(),
            Some(pqo),
        )
        .await?;

    info!(
        "Object store read {} bytes from {} GETs",
        store
            .get_bytes_read()
            .unwrap_or(0)
            .to_formatted_string(&Locale::en),
        store
            .get_count()
            .unwrap_or(0)
            .to_formatted_string(&Locale::en)
    );

    // Find rows just written to output Parquet file
    let pq_reader = get_parquet_builder(store_factory, output_path)
        .await
        .map_err(|e| DataFusionError::External(e.into()))?;
    let num_rows = pq_reader.metadata().file_metadata().num_rows();

    // The rows read will be same as rows_written.
    Ok(CompactionResult {
        rows_read: num_rows as usize,
        rows_written: num_rows as usize,
    })
}

/// Create an asynchronous builder for reading Parquet files from an object store.
///
/// The URL must start with a scheme that the object store recognises, e.g. "file" or "s3".
///
/// # Errors
/// This function will return an error if it couldn't connect to S3 or open a valid
/// Parquet file.
pub async fn get_parquet_builder(
    store_factory: &ObjectStoreFactory,
    src: &Url,
) -> color_eyre::Result<ParquetRecordBatchStreamBuilder<ParquetObjectReader>> {
    let store = store_factory.get_object_store(src)?;
    // HEAD the file to get metadata
    let path = object_store::path::Path::from(src.path());
    let object_meta = store
        .head(&path)
        .await
        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
    // Create a reader for the target file, use it to construct a Stream
    let reader = ParquetObjectReader::new(store.as_object_store(), object_meta);
    Ok(ParquetRecordBatchStreamBuilder::new(reader).await?)
}

/// Create the `DataFusion` filtering expression from a Sleeper region.
///
/// For each column in the row keys, we look up the partition range for that
/// column and create a expression tree that combines all the various filtering conditions.
///
fn region_filter(region: &HashMap<String, ColRange>) -> Option<Expr> {
    let mut col_expr: Option<Expr> = None;
    for (name, range) in region {
        let lower_expr = lower_bound_expr(range, name);
        let upper_expr = upper_bound_expr(range, name);
        let expr = lower_expr.and(upper_expr);
        // Combine this column filter with any previous column filter
        col_expr = match col_expr {
            Some(original) => Some(original.and(expr)),
            None => Some(expr),
        }
    }
    col_expr
}

/// Calculate the upper bound expression on a given [`ColRange`].
///
/// This takes into account the inclusive/exclusive nature of the bound.
///
fn upper_bound_expr(range: &ColRange, name: &String) -> Expr {
    let max_bound = bound_to_lit_expr(&range.upper);
    let upper_expr = if range.upper_inclusive {
        col(name).lt_eq(max_bound)
    } else {
        col(name).lt(max_bound)
    };
    upper_expr
}

/// Calculate the lower bound expression on a given [`ColRange`].
///
/// This takes into account the inclusive/exclusive nature of the bound.
///
fn lower_bound_expr(range: &ColRange, name: &String) -> Expr {
    let min_bound = bound_to_lit_expr(&range.lower);
    let lower_expr = if range.lower_inclusive {
        col(name).gt_eq(min_bound)
    } else {
        col(name).gt(min_bound)
    };
    lower_expr
}

/// Convert a [`PartitionBound`] to an [`Expr`] that can be
/// used in a bigger expression.
///
fn bound_to_lit_expr(bound: &PartitionBound) -> Expr {
    let expr = match bound {
        PartitionBound::Int32(val) => lit(*val),
        PartitionBound::Int64(val) => lit(*val),
        PartitionBound::String(val) => lit(val.to_owned()),
        PartitionBound::ByteArray(val) => lit(val.to_owned()),
    };
    expr
}

/// Convert a Sleeper compression codec string to one `DataFusion` understands.
fn get_compression(compression: &str) -> String {
    match compression.to_lowercase().as_str() {
        x @ ("uncompressed" | "snappy" | "lzo" | "lz4") => x.into(),
        "gzip" => format!("gzip({})", GzipLevel::default().compression_level()),
        "brotli" => format!("brotli({})", BrotliLevel::default().compression_level()),
        "zstd" => format!("zstd({})", ZstdLevel::default().compression_level()),
        _ => {
            error!("Unknown compression");
            unimplemented!()
        }
    }
}

/// Create the `DataFusion` session configuration for a given compaction.
///
/// This sets as many parameters as possible from the given input data.
///
fn create_session_cfg(input_data: &CompactionInput) -> SessionConfig {
    let mut sf = SessionConfig::new();
    sf.options_mut().execution.parquet.max_row_group_size = input_data.max_row_group_size;
    sf.options_mut().execution.parquet.data_pagesize_limit = input_data.max_page_size;
    sf.options_mut().execution.parquet.compression = Some(get_compression(&input_data.compression));
    sf.options_mut().execution.parquet.writer_version = input_data.writer_version.clone();
    sf.options_mut()
        .execution
        .parquet
        .column_index_truncate_length = Some(input_data.column_truncate_length);
    sf.options_mut().execution.parquet.max_statistics_size = Some(input_data.stats_truncate_length);
    sf
}

/// Creates the sort order for a given schema.
///
/// This is a list of the row key columns followed by the sort columns.
///
fn sort_order(input_data: &CompactionInput) -> Vec<Expr> {
    let sort_order = input_data
        .row_key_cols
        .iter()
        .chain(input_data.sort_key_cols.iter())
        .map(|s| col(s).sort(true, false))
        .collect::<Vec<_>>();
    sort_order
}

/// Takes the first Url in `input_paths` list and `output_path`
/// and registers the appropriate [`ObjectStore`] for it.
///
/// `DataFusion` doesn't seem to like loading a single file set from different object stores
/// so we only register the first one.
///
/// # Errors
/// If we can't create an [`ObjectStore`] for a known URL then this will fail.
///
fn register_store(
    store_factory: &ObjectStoreFactory,
    input_paths: &[Url],
    output_path: &Url,
    ctx: &SessionContext,
) -> Result<Arc<dyn CountingObjectStore>, DataFusionError> {
    let in_store = store_factory
        .get_object_store(&input_paths[0])
        .map_err(|e| DataFusionError::External(e.into()))?;
    ctx.runtime_env()
        .register_object_store(&input_paths[0], in_store.clone().as_object_store());
    let out_store = store_factory
        .get_object_store(output_path)
        .map_err(|e| DataFusionError::External(e.into()))?;
    ctx.runtime_env()
        .register_object_store(output_path, out_store.clone().as_object_store());
    Ok(in_store)
}
