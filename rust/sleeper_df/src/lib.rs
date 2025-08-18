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
    objects::{FFICommonConfig, FFICompactionResult, FFILeafPartitionQueryConfig},
};
use ::log::{error, warn};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use libc::{EFAULT, EINVAL};
use sleeper_core::{
    CommonConfig, CompletedOutput, LeafPartitionQueryConfig, run_compaction, run_query,
    stream_to_ffi_arrow_stream,
};
use std::ffi::c_int;

mod context;
mod log;
mod objects;
mod unpack;

/// Provides the C FFI interface to calling the [`run_compaction`] function.
///
/// This function takes an [`FFICommonConfig`] struct which contains all the information needed for a compaction.
/// This function validates the pointers are valid strings (or at least attempts to), but undefined behaviour will
/// result if bad pointers are passed.
///
/// It is also undefined behaviour to specify an incorrect array length for any array.
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
/// | EFAULT | if pointers are NULL
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
        error!("NULL context pointer");
        return EFAULT;
    };

    let Some(params) = (unsafe { input_data.as_ref() }) else {
        error!("input data pointer is NULL");
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
                error!("output data pointer is NULL");
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

/// Provides the C FFI interface to calling the [`run_query`] function.
///
/// This function takes an [`FFILeafPartitionQueryConfig`] struct which contains all the information needed for a Sleeper
/// leaf partition query.
///
/// The resulting stream of Arrow [`RecordBatch`]es is wrapped into an FFI
/// compatible Arrow stream and the output pointer is set to it.
///
/// The `output_results` is an "out" parameter.
/// No assumptions about the value of the pointer object is assumed upon entry to
/// this function and the contents are overwritten on successful query.
/// This function is intended to be callable by an external language via FFI,
/// therefore it is marked as `#[unsafe(no_mangle)]` to ensure the function
/// name is not changed and as `extern "C"` to ensure C linkage rules are followed.
///
/// # Memory management
///
/// The stream of results is owned by the [`FFI_ArrowArrayStream`].
/// It is the responsibility of the caller to release the memory by calling the
/// `release()` function inside the Array stream when the stream is no longer required.
/// Even if done so in another language, this will cause Rust to release all internal
/// resources needed by this stream.
///
/// # Safety
///
/// It is the callers responsibility to ensure all pointers are valid and point
/// to valid data before calling this function. While null pointers are detected,
/// invalid pointers cannot be.
///
/// It is undefined behaviour to specify an incorrect array length for any array.
///
/// The `ctx_ptr` value must point to a valid [`FFI_Context`] which contains
/// an active runtime.
///
/// It is safe to release the [`FFI_Context`] object (see [`destroy_context`](crate::destroy_context)) even
/// if this stream is still being read from. The underlying Tokio runtime will not be released
/// until all remaining [`FFI_ArrowArrayStream`]s created by this function are
/// released.
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
/// | EFAULT | if pointers are NULL
/// | EINVAL | if can't convert string to Rust string (invalid UTF-8?) |
/// | EINVAL | if row key column numbers or sort column numbers are empty |
///
#[allow(clippy::not_unsafe_ptr_arg_deref, unused_assignments, unused_variables)]
#[unsafe(no_mangle)]
pub extern "C" fn native_query_stream(
    ctx_ptr: *const FFIContext,
    input_data: *const FFILeafPartitionQueryConfig,
    mut output_results: *mut FFI_ArrowArrayStream,
) -> c_int {
    maybe_cfg_log();
    if let Err(e) = color_eyre::install() {
        warn!("Couldn't install color_eyre error handler {e}");
    }

    // Null check the context pointer
    let Some(context) = (unsafe { ctx_ptr.as_ref() }) else {
        error!("NULL context pointer");
        return EFAULT;
    };

    let Some(params) = (unsafe { input_data.as_ref() }) else {
        error!("input data pointer is NULL");
        return EFAULT;
    };

    let details = match TryInto::<LeafPartitionQueryConfig>::try_into(params) {
        Ok(d) => d,
        Err(e) => {
            error!("Couldn't convert compaction input data {e}");
            return EINVAL;
        }
    };

    // Run compaction
    let result = context.rt.block_on(run_query(&details));
    match result {
        Ok(res) => {
            let CompletedOutput::ArrowRecordBatch(batch_stream) = res else {
                panic!("Expected ArrowRecordBatch results from query");
            };
            // Convert the DataFusion stream of data to an FFI compatible Arrow stream
            let ffi_arrow_stream =
                Box::new(stream_to_ffi_arrow_stream(batch_stream, context.rt.clone()));
            // Leak pointer from Box. At this point Rust gives up ownership management of that object
            let leaked_ptr = Box::into_raw(ffi_arrow_stream);
            output_results = leaked_ptr;
            0
        }
        Err(e) => {
            error!("merging error {e}");
            -1
        }
    }
}
