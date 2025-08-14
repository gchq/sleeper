//! All Foreign Function Interface compatible structs are here.
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
use crate::unpack::{
    unpack_aws_config, unpack_primitive_array, unpack_string_array, unpack_variant_array,
};
use color_eyre::eyre::bail;
use sleeper_core::{
    ColRange, CommonConfig, CompletionOptions, SleeperParquetOptions, SleeperPartitionRegion,
};
use std::{
    borrow::Borrow,
    collections::HashMap,
    ffi::{CStr, c_char, c_int, c_void},
};
use url::Url;

/// Contains all the common input data for setting up a Sleeper DataFusion operation.
///
/// See `java/compaction/compaction-datafusion/src/main/java/sleeper/compaction/datafusion/DataFusionFunctions.java`
/// for details. Field ordering and types MUST match between the two definitions!
#[repr(C)]
pub struct FFICommonConfig {
    pub override_aws_config: bool,
    pub aws_region: *const c_char,
    pub aws_endpoint: *const c_char,
    pub aws_access_key: *const c_char,
    pub aws_secret_key: *const c_char,
    pub aws_allow_http: bool,
    pub input_files_len: usize,
    pub input_files: *const *const c_char,
    pub input_files_sorted: bool,
    pub output_file: *const c_char,
    pub row_key_cols_len: usize,
    pub row_key_cols: *const *const c_char,
    pub row_key_schema_len: usize,
    pub row_key_schema: *const *const c_int,
    pub sort_key_cols_len: usize,
    pub sort_key_cols: *const *const c_char,
    pub max_row_group_size: usize,
    pub max_page_size: usize,
    pub compression: *const c_char,
    pub writer_version: *const c_char,
    pub column_truncate_length: usize,
    pub stats_truncate_length: usize,
    pub dict_enc_row_keys: bool,
    pub dict_enc_sort_keys: bool,
    pub dict_enc_values: bool,
    pub region: *const FFISleeperRegion,
    pub iterator_config: *const c_char,
}

impl<'a> TryFrom<&'a FFICommonConfig> for CommonConfig<'a> {
    type Error = color_eyre::eyre::Report;

    fn try_from(params: &'a FFICommonConfig) -> Result<CommonConfig<'a>, Self::Error> {
        if params.iterator_config.is_null() {
            bail!("FFICompactionsParams iterator_config is NULL");
        }
        if params.output_file.is_null() {
            bail!("FFICompactionParams output_file is NULL");
        }
        if params.compression.is_null() {
            bail!("FFICompactionParams compression is NULL");
        }
        if params.writer_version.is_null() {
            bail!("FFICompactionParams writer_version is NULL");
        }
        if params.region.is_null() {
            bail!("FFICompactionParams region is NULL");
        }
        // We do this separately since we need the values for computing the region
        let row_key_cols = unpack_string_array(params.row_key_cols, params.row_key_cols_len)?
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>();

        // Contains numeric types to indicate schema types
        let schema_types =
            unpack_primitive_array(params.row_key_schema, params.row_key_schema_len)?;

        let ffi_region = unsafe { params.region.as_ref() }.unwrap();
        let region = FFISleeperRegion::to_sleeper_region(ffi_region, &row_key_cols, &schema_types)?;

        // Extract iterator config
        let iterator_config = Some(
            unsafe { CStr::from_ptr(params.iterator_config) }
                .to_str()?
                .to_owned(),
        )
        // Set option to None if config is empty
        .and_then(|v| if v.trim().is_empty() { None } else { Some(v) });

        let opts = SleeperParquetOptions {
            max_row_group_size: params.max_row_group_size,
            max_page_size: params.max_page_size,
            compression: unsafe { CStr::from_ptr(params.compression) }
                .to_str()?
                .to_owned(),
            writer_version: unsafe { CStr::from_ptr(params.writer_version) }
                .to_str()?
                .to_owned(),
            column_truncate_length: params.column_truncate_length,
            stats_truncate_length: params.stats_truncate_length,
            dict_enc_row_keys: params.dict_enc_row_keys,
            dict_enc_sort_keys: params.dict_enc_sort_keys,
            dict_enc_values: params.dict_enc_values,
        };

        Self::try_new(
            unpack_aws_config(params)?,
            unpack_string_array(params.input_files, params.input_files_len)?
                .into_iter()
                .map(Url::parse)
                .collect::<Result<Vec<_>, _>>()?,
            params.input_files_sorted,
            row_key_cols,
            unpack_string_array(params.sort_key_cols, params.sort_key_cols_len)?
                .into_iter()
                .map(String::from)
                .collect(),
            region,
            CompletionOptions::File {
                output_file: unsafe { CStr::from_ptr(params.output_file) }
                    .to_str()
                    .map(Url::parse)??,
                opts,
            },
            iterator_config,
        )
    }
}

/// Represents a Sleeper region in a C ABI struct.
///
/// Java arrays are transferred with a length. They should all be the same length in this struct.
#[repr(C)]
pub struct FFISleeperRegion {
    pub region_mins_len: usize,
    pub region_mins: *const *const c_void,
    pub region_maxs_len: usize,
    // The region_maxs array may contain null pointers!!
    pub region_maxs: *const *const c_void,
    pub region_mins_inclusive_len: usize,
    pub region_mins_inclusive: *const *const bool,
    pub region_maxs_inclusive_len: usize,
    pub region_maxs_inclusive: *const *const bool,
}

impl<'a> FFISleeperRegion {
    fn to_sleeper_region<T: Borrow<str>>(
        region: &'a FFISleeperRegion,
        row_key_cols: &[T],
        schema_types: &Vec<i32>,
    ) -> Result<SleeperPartitionRegion<'a>, color_eyre::eyre::Report> {
        if region.region_mins_len != region.region_maxs_len
            || region.region_mins_len != region.region_mins_inclusive_len
            || region.region_mins_len != region.region_maxs_inclusive_len
        {
            bail!("All array lengths in a SleeperRegion must be same length");
        }
        let region_mins_inclusive = unpack_primitive_array(
            region.region_mins_inclusive,
            region.region_mins_inclusive_len,
        )?;
        let region_maxs_inclusive = unpack_primitive_array(
            region.region_maxs_inclusive,
            region.region_maxs_inclusive_len,
        )?;

        let region_mins = unpack_variant_array(
            region.region_mins,
            region.region_mins_len,
            &schema_types,
            false,
        )?;

        let region_maxs = unpack_variant_array(
            region.region_maxs,
            region.region_maxs_len,
            &schema_types,
            true,
        )?;

        let mut map = HashMap::with_capacity(row_key_cols.len());
        for (idx, row_key) in row_key_cols.iter().enumerate() {
            map.insert(
                String::from(row_key.borrow()),
                ColRange {
                    lower: region_mins[idx],
                    lower_inclusive: region_mins_inclusive[idx],
                    upper: region_maxs[idx],
                    upper_inclusive: region_maxs_inclusive[idx],
                },
            );
        }
        Ok(SleeperPartitionRegion::new(map))
    }
}

/// Contains all output data from a compaction operation.
#[repr(C)]
pub struct FFICompactionResult {
    /// The total number of rows read by a compaction.
    pub rows_read: usize,
    /// The total number of rows written by a compaction.
    pub rows_written: usize,
}

/// Contains all information needed for a Sleeper leaf partition query from a foreign function.
#[repr(C)]
pub struct FFILeafPartitionQueryConfig {
    /// Common configuration
    pub common: *const FFICommonConfig,
    /// Should quantile data sketches be written out?
    pub write_quantile_sketch: bool,
    /// Should logical and physical query plans be written to log?
    pub explain_plans: bool,
}
