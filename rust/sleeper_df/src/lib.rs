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
    context::FFIContext,
    log::maybe_cfg_log,
    objects::{FFICommonConfig, FFICompactionResult},
};
use ::log::{error, warn};
use libc::{EFAULT, EINVAL};
use sleeper_core::{CommonConfig, run_compaction};
use std::ffi::c_int;

mod context;
mod log;
mod objects;
mod unpack;

/// Provides the C FFI interface to calling the [`merge_sorted_files`] function.
///
/// This function takes an [`FFICommonConfig`] struct which contains all the information needed for a compaction.
/// This function validates the pointers are valid strings (or at least attempts to), but undefined behaviour will
/// result if bad pointers are passed.
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
///
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[unsafe(no_mangle)]
pub extern "C" fn native_compact(
    ctx_ptr: *const FFIContext,
    input_data: *mut FFICommonConfig,
    output_data: *mut FFICompactionResult,
) -> c_int {
    maybe_cfg_log();
    if let Err(e) = color_eyre::install() {
        warn!("Couldn't install color_eyre error handler {e}");
    }

    // Null check the context pointer
    let Some(context) = (unsafe { ctx_ptr.as_ref() }) else {
        error!("Null context pointer");
        return EFAULT;
    };

    let Some(params) = (unsafe { input_data.as_ref() }) else {
        error!("input data pointer is null");
        return EFAULT;
    };

    let details = match TryInto::<CommonConfig>::try_into(params) {
        Ok(d) => d,
        Err(e) => {
            error!("Couldn't convert compaction input data {e}");
            return EINVAL;
        }
    };

    // Run compaction
    let result = context.rt.block_on(run_compaction(&details));
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
