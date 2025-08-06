//! `DataFusion` configuration facilities. Used to configure Parquet writing options and setting default
//! session configuration options.
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
use crate::{CompactionInput, SleeperParquetOptions};
use datafusion::{
    common::DFSchema,
    config::TableParquetOptions,
    parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel},
    prelude::SessionConfig,
};
use log::{error, info};

/// Keeps an ownerless reference to [`SleeperParquetOptions`].
///
/// Methods in this class can be used to configure a [`SessionConfig`] and [`TableParquetOptions`] to
/// use Sleeper's current configuration.
#[derive(Debug)]
pub struct ParquetWriterConfigurer<'a> {
    pub parquet_options: &'a SleeperParquetOptions,
}

impl ParquetWriterConfigurer<'_> {
    /// Configure a given configuration with Sleeper Parquet writing options.
    pub fn apply_parquet_config(&self, mut cfg: SessionConfig) -> SessionConfig {
        cfg.options_mut().execution.parquet.max_row_group_size =
            self.parquet_options.max_row_group_size;
        cfg.options_mut().execution.parquet.data_pagesize_limit =
            self.parquet_options.max_page_size;
        cfg.options_mut().execution.parquet.compression = Some(self.get_compression());
        cfg.options_mut().execution.parquet.writer_version = self.get_parquet_writer_version();
        cfg.options_mut()
            .execution
            .parquet
            .column_index_truncate_length = Some(self.parquet_options.column_truncate_length);
        cfg.options_mut()
            .execution
            .parquet
            .statistics_truncate_length = Some(self.parquet_options.stats_truncate_length);
        cfg
    }

    /// Configure the per column dictionary encoding based on the input configuration.
    ///
    /// This ensures the output configuration matches what Sleeper is expecting.
    pub fn apply_dictionary_encoding(
        &self,
        mut opts: TableParquetOptions,
        input_data: &CompactionInput<'_>,
        schema: &DFSchema,
    ) -> TableParquetOptions {
        for column in schema.columns() {
            let col_name = column.name().to_owned();
            info!("TODO: REMOVE THIS {col_name}");
            let col_opts = opts
                .column_specific_options
                .entry(col_name.clone())
                .or_default();
            let dict_encode = (self.parquet_options.dict_enc_row_keys && input_data.row_key_cols.contains(&col_name))
            || (self.parquet_options.dict_enc_sort_keys && input_data.sort_key_cols.contains(&col_name))
            // Check value columns
            || (self.parquet_options.dict_enc_values
                && !input_data.row_key_cols.contains(&col_name)
                && !input_data.sort_key_cols.contains(&col_name));
            col_opts.dictionary_enabled = Some(dict_encode);
        }
        opts
    }

    /// Convert a Sleeper compression codec string to one `DataFusion` understands.
    fn get_compression(&self) -> String {
        match self.parquet_options.compression.to_lowercase().as_str() {
            x @ ("uncompressed" | "snappy" | "lzo" | "lz4") => x.into(),
            "gzip" => format!("gzip({})", GzipLevel::default().compression_level()),
            "brotli" => format!("brotli({})", BrotliLevel::default().compression_level()),
            "zstd" => format!("zstd({})", ZstdLevel::default().compression_level()),
            x => {
                error!(
                    "Unknown compression {x}, valid values: uncompressed, snappy, lzo, lz4, gzip, brotli, zstd"
                );
                unimplemented!(
                    "Unknown compression {x}, valid values: uncompressed, snappy, lzo, lz4, gzip, brotli, zstd"
                );
            }
        }
    }

    /// Convert a Sleeper Parquet version to one `DataFusion` understands.
    fn get_parquet_writer_version(&self) -> String {
        match self.parquet_options.writer_version.to_lowercase().as_str() {
            "v1" => "1.0".into(),
            "v2" => "2.0".into(),
            x => {
                error!("Parquet writer version invalid {x}, valid values: v1, v2");
                unimplemented!("Parquet writer version invalid {x}, valid values: v1, v2");
            }
        }
    }
}

/// Create the `DataFusion` session configuration for a given session.
///
/// This sets as many parameters as possible from the given input data.
///
pub fn apply_sleeper_config(
    mut cfg: SessionConfig,
    input_data: &CompactionInput,
    upload_size: Option<usize>,
) -> SessionConfig {
    // In order to avoid a costly "Sort" stage in the physical plan, we must make
    // sure the target partitions as at least as big as number of input files.
    cfg.options_mut().execution.target_partitions = std::cmp::max(
        cfg.options().execution.target_partitions,
        input_data.input_files.len(),
    );
    // Disable page indexes since we won't benefit from them as we are reading large contiguous file regions
    cfg.options_mut().execution.parquet.enable_page_index = false;
    // Disable repartition_aggregations to workaround sorting bug where DataFusion partitions are concatenated back
    // together in wrong order.
    cfg.options_mut().optimizer.repartition_aggregations = false;
    // Physical plan explanation not needed here
    cfg.options_mut().explain.logical_plan_only = true;
    if let Some(size) = upload_size {
        cfg.options_mut().execution.objectstore_writer_buffer_size = size;
    }
    cfg
}
