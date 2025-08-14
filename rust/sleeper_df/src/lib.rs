//! All Foreign Function Interface logic is in this crate.
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
use ::log::{error, warn};
use color_eyre::eyre::bail;
use libc::{EFAULT, EINVAL, EIO, size_t};
use sleeper_core::{
    ColRange, CommonConfig, CompletionOptions, SleeperParquetOptions,
    SleeperPartitionRegion, run_compaction,
};
use std::{
    borrow::Borrow,
    collections::HashMap,
    ffi::{CStr, c_char, c_int, c_void},
};
use url::Url;

use crate::{
    log::maybe_cfg_log,
    unpack::{
        unpack_aws_config, unpack_primitive_array, unpack_string_array, unpack_variant_array,
    },
};

mod log;
mod unpack;

/// Contains all the input data for setting up a compaction.
///
/// See java/compaction/compaction-rust/src/main/java/sleeper/compaction/jobexecution/RustBridge.java
/// for details. Field ordering and types MUST match between the two definitions!
#[repr(C)]
pub struct FFICommonConfig {
    override_aws_config: bool,
    aws_region: *const c_char,
    aws_endpoint: *const c_char,
    aws_access_key: *const c_char,
    aws_secret_key: *const c_char,
    aws_allow_http: bool,
    input_files_len: usize,
    input_files: *const *const c_char,
    output_file: *const c_char,
    row_key_cols_len: usize,
    row_key_cols: *const *const c_char,
    row_key_schema_len: usize,
    row_key_schema: *const *const c_int,
    sort_key_cols_len: usize,
    sort_key_cols: *const *const c_char,
    max_row_group_size: usize,
    max_page_size: usize,
    compression: *const c_char,
    writer_version: *const c_char,
    column_truncate_length: usize,
    stats_truncate_length: usize,
    dict_enc_row_keys: bool,
    dict_enc_sort_keys: bool,
    dict_enc_values: bool,
    region_mins_len: usize,
    region_mins: *const *const c_void,
    region_maxs_len: usize,
    // The region_maxs array may contain null pointers!!
    region_maxs: *const *const c_void,
    region_mins_inclusive_len: usize,
    region_mins_inclusive: *const *const bool,
    region_maxs_inclusive_len: usize,
    region_maxs_inclusive: *const *const bool,
    iterator_config: *const c_char,
}

impl<'a> TryFrom<&'a FFICommonConfig> for CommonConfig<'a> {
    type Error = color_eyre::eyre::Report;

    fn try_from(params: &'a FFICommonConfig) -> color_eyre::Result<CommonConfig<'a>, Self::Error> {
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
        // We do this separately since we need the values for computing the region
        let row_key_cols = unpack_string_array(params.row_key_cols, params.row_key_cols_len)?
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>();
        let region = compute_region(params, &row_key_cols)?;
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
            true,
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

fn compute_region<'a, T: Borrow<str>>(
    params: &'a FFICommonConfig,
    row_key_cols: &[T],
) -> color_eyre::Result<SleeperPartitionRegion<'a>> {
    let region_mins_inclusive = unpack_primitive_array(
        params.region_mins_inclusive,
        params.region_mins_inclusive_len,
    )?;
    let region_maxs_inclusive = unpack_primitive_array(
        params.region_maxs_inclusive,
        params.region_maxs_inclusive_len,
    )?;
    let schema_types = unpack_primitive_array(params.row_key_schema, params.row_key_schema_len)?;
    let region_mins = unpack_variant_array(
        params.region_mins,
        params.region_mins_len,
        &schema_types,
        false,
    )?;
    let region_maxs = unpack_variant_array(
        params.region_maxs,
        params.region_maxs_len,
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

/// Contains all output data from a compaction operation.
#[repr(C)]
pub struct FFICompactionResult {
    /// The total number of rows read by a compaction.
    rows_read: size_t,
    /// The total number of rows written by a compaction.
    rows_written: size_t,
}

/// Provides the C FFI interface to calling the [`merge_sorted_files`] function.
///
/// This function takes an `FFICompactionParams` struct which contains all the  This function validates the pointers are valid strings (or
/// at least attempts to), but undefined behaviour will result if bad pointers are passed.
///
/// It is also undefined behaviour to specify and incorrect array length for any array.
///
/// The `output_data` field is an out parameter. It is assumed the caller has allocated valid
/// memory at the address pointed to!
///
/// The return value for this function gives the result for the compaction. *If an error code is returned,
/// then the result of all other fields in the `output_data` field are
/// undefined and should not be read!*
///
/// # Panics
/// If we are unable to transfer vector ownership to foreign code properly.
///
/// # Errors
/// The following result codes are returned.
///
/// | Code | Meaning |
/// |-|-|
/// | 0 | Success |
/// | -1 | Arrow error (see log) |
/// | EFAULT | if pointers are null
/// | EINVAL | if can't convert string to Rust string (invalid UTF-8?) |
/// | EINVAL | if row key column numbers or sort column numbers are empty |
/// | EIO    | if Rust tokio runtime couldn't be created |
///
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[unsafe(no_mangle)]
pub extern "C" fn merge_sorted_files(
    input_data: *mut FFICommonConfig,
    output_data: *mut FFICompactionResult,
) -> c_int {
    maybe_cfg_log();
    if let Err(e) = color_eyre::install() {
        warn!("Couldn't install color_eyre error handler {e}");
    }
    let Some(params) = (unsafe { input_data.as_ref() }) else {
        error!("input data pointer is null");
        return EFAULT;
    };

    // Start async runtime
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(v) => v,
        Err(e) => {
            error!("Couldn't create Rust tokio runtime {e}");
            return EIO;
        }
    };

    let details = match TryInto::<CommonConfig>::try_into(params) {
        Ok(d) => d,
        Err(e) => {
            error!("Couldn't convert compaction input data {e}");
            return EINVAL;
        }
    };

    // Run compaction
    let result = rt.block_on(run_compaction(&details));
    match result {
        Ok(res) => {
            if let Some(data) = unsafe { output_data.as_mut() } {
                data.rows_read = res.rows_read;
                data.rows_written = res.rows_written;
            } else {
                error!("output data pointer is null");
                return EFAULT;
            }
            0
        }
        Err(e) => {
            error!("merging error {e}");
            -1
        }
    }
}
