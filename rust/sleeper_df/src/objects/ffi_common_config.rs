//! Common FFI structs that are used in multiple operations.
/*
* Copyright 2022-2026 Crown Copyright
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
    objects::{
        FFIBytes,
        aws_config::{FFIAwsConfig, unpack_aws_config},
        ffi_parquet_options::FFIParquetOptions,
        sleeper_region::FFISleeperRegion,
    },
    unpack::{unpack_str, unpack_string},
};
use color_eyre::eyre::{Result, bail};
use core::slice;
use sleeper_core::{
    CommonConfig, CommonConfigBuilder, OutputType, SleeperParquetOptions,
    filter_aggregation_config::{aggregate::Aggregate, filter::Filter},
};
use std::ffi::c_char;
use url::Url;

/// Contains all the common input data for setting up a Sleeper `DataFusion` operation.
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/common/foreign-bridge/src/main/java/sleeper/foreign/datafusion/FFICommonConfig.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
pub struct FFICommonConfig {
    // If this field is NULL use defaults.
    pub aws_config: *const FFIAwsConfig,
    pub input_files_len: usize,
    pub input_files: *const *const c_char,
    pub input_files_sorted: bool,
    pub output_file: *const c_char,
    pub write_sketch_file: bool,
    pub use_readahead_store: bool,
    pub row_key_cols_len: usize,
    pub row_key_cols: *const FFIBytes,
    pub sort_key_cols_len: usize,
    pub sort_key_cols: *const FFIBytes,
    pub region: *const FFISleeperRegion,
    pub aggregation_config: *const c_char,
    pub filtering_config: *const c_char,
    // If this field is NULL, then use defaults
    pub parquet_options: *const FFIParquetOptions,
}

impl FFICommonConfig {
    /// Get row key field names.
    pub fn row_key_cols(&self) -> Result<Vec<String>, color_eyre::Report> {
        unsafe { slice::from_raw_parts(self.row_key_cols, self.row_key_cols_len) }
            .iter()
            .map(|bytes| Ok(String::from(TryInto::<&str>::try_into(bytes)?)))
            .collect()
    }

    /// Convert to a Rust native struct.
    ///
    /// All pointers must be valid. Pointers are NULL checked, but we can't vouch for validity.
    ///
    /// # Errors
    /// Errors if: any pointer is NULL, any array lengths invalid, region invalid, etc.
    pub fn to_common_config<'a>(
        &self,
        file_output_enabled: bool,
    ) -> Result<CommonConfig<'a>, color_eyre::Report> {
        if file_output_enabled && self.output_file.is_null() {
            bail!("FFICommonConfig output_file is NULL, file output selected");
        }
        if self.aggregation_config.is_null() {
            bail!("FFICommonConfig aggregation_config is NULL");
        }
        if self.filtering_config.is_null() {
            bail!("FFICommonConfig filtering_config is NULL");
        }
        if self.region.is_null() {
            bail!("FFICommonConfig region is NULL");
        }
        if self.input_files.is_null() {
            bail!("FFICommonConfig input_files is NULL");
        }
        if self.row_key_cols.is_null() {
            bail!("FFICommonConfig row_key_cols is NULL");
        }

        let parquet_options = if let Some(options) = unsafe { self.parquet_options.as_ref() } {
            options
        } else {
            &FFIParquetOptions::default()
        };
        parquet_options.check_for_nulls()?;

        // We do this separately since we need the values for computing the region
        let row_key_cols = self.row_key_cols()?;

        let ffi_region = unsafe { self.region.as_ref() }.unwrap();
        let region = FFISleeperRegion::to_sleeper_region(ffi_region, &row_key_cols)?;

        let output = if file_output_enabled {
            let opts = SleeperParquetOptions {
                max_row_group_size: parquet_options.max_row_group_size,
                max_page_size: parquet_options.max_page_size,
                compression: unpack_string(parquet_options.compression)?,
                writer_version: unpack_string(parquet_options.writer_version)?,
                column_truncate_length: parquet_options.column_truncate_length,
                stats_truncate_length: parquet_options.stats_truncate_length,
                dict_enc_row_keys: parquet_options.dict_enc_row_keys,
                dict_enc_sort_keys: parquet_options.dict_enc_sort_keys,
                dict_enc_values: parquet_options.dict_enc_values,
            };
            OutputType::File {
                output_file: unpack_str(self.output_file).map(Url::parse)??,
                write_sketch_file: self.write_sketch_file,
                opts,
            }
        } else {
            OutputType::ArrowRecordBatch
        };

        let ins = unsafe { slice::from_raw_parts(self.input_files, self.input_files_len) }
            .iter()
            .map(|e| Url::parse(unpack_str(*e)?).map_err(color_eyre::Report::from))
            .collect::<Result<Vec<_>, _>>()?;

        CommonConfigBuilder::new()
            .aws_config(unpack_aws_config(self))
            .input_files(ins)
            .input_files_sorted(self.input_files_sorted)
            .read_page_indexes(parquet_options.read_page_indexes)
            .use_readahead_store(self.use_readahead_store)
            .row_key_cols(row_key_cols)
            .sort_key_cols(if self.sort_key_cols_len == 0 {
                Vec::new()
            } else {
                unsafe { slice::from_raw_parts(self.sort_key_cols, self.sort_key_cols_len) }
                    .iter()
                    .map(|bytes| {
                        Ok::<_, color_eyre::Report>(String::from(TryInto::<&str>::try_into(bytes)?))
                    })
                    .collect::<Result<Vec<_>, _>>()?
            })
            .region(region)
            .output(output)
            .aggregates(Aggregate::parse_config(unpack_str(
                self.aggregation_config,
            )?)?)
            .filters(Filter::parse_config(unpack_str(self.filtering_config)?)?)
            .build()
    }
}
