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
use crate::{
    ffi_objects::{FFICommonConfig, FFICompactionResult},
    log::maybe_cfg_log,
    unpack::{
        unpack_primitive_array, unpack_variant_array,
    },
};
use ::log::{error, warn};
use libc::{EFAULT, EINVAL, EIO};
use sleeper_core::{
    ColRange, CommonConfig, SleeperPartitionRegion,
    run_compaction,
};
use std::{
    borrow::Borrow,
    collections::HashMap,
    ffi::c_int,
};

mod ffi_objects;
mod log;
mod unpack;

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
