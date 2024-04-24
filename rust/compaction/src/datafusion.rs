use std::sync::Arc;

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
    CompactionInput, CompactionResult,
};
use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
use datafusion::{
    dataframe::DataFrameWriteOptions,
    error::DataFusionError,
    execution::{config::SessionConfig, context::SessionContext, options::ParquetReadOptions},
    parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel},
    prelude::*,
};
use log::{error, info};
use num_format::{Locale, ToFormattedString};
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

    let sf = create_session_cfg(input_data);
    let ctx = SessionContext::new_with_config(sf);

    // Register some objects store from first object
    let store = register_store(store_factory, input_paths, output_path, &ctx)?;

    // Sort on row key columns then sort columns (nulls last)
    let sort_order = sort_order(input_data);
    info!("Row key and sort column order {sort_order:?}");

    let po = ParquetReadOptions::default().file_sort_order(vec![sort_order.clone()]);
    let frame = ctx.read_parquet(input_paths.to_owned(), po).await?;

    // Extract all column names
    let col_names = frame.schema().clone().strip_qualifiers().field_names();
    info!("All columns in schema {col_names:?}");

    // Perform projection of all columns
    let col_names_expr = frame
        .schema()
        .clone()
        .strip_qualifiers()
        .field_names()
        .iter()
        .map(col)
        .collect::<Vec<_>>();
    let frame = frame.sort(sort_order)?.select(col_names_expr)?;

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

    let result = frame
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
    let rows_written = result.iter().map(RecordBatch::num_rows).sum::<usize>();
    // The rows read will be same as rows_written.
    Ok(CompactionResult {
        rows_read: rows_written,
        rows_written,
    })
    // TODO: Sketches
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
