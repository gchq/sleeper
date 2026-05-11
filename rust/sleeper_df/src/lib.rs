//! All Foreign Function Interface logic is in this crate.
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
    context::FFIContext,
    log::maybe_cfg_log,
    objects::{
        FFIFileResult,
        ffi_common_config::FFICommonConfig,
        query::{FFILeafPartitionQueryConfig, FFIQueryResults},
    },
    unpack::unpack_string,
};
use ::log::{error, warn};
#[cfg(doc)]
use datafusion::arrow::{ffi_stream::FFI_ArrowArrayStream, record_batch::RecordBatch};
use libc::{EFAULT, EINVAL};
use sleeper_core::{
    CompletedOutput, run_compaction, run_query, simulate_compaction_row_reads,
    stream_to_ffi_arrow_stream,
};
use std::ffi::{c_char, c_int};

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
/// | EINVAL | if row key field numbers or sort field numbers are empty |
///
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[unsafe(no_mangle)]
pub extern "C" fn native_compact(
    ctx_ptr: *const FFIContext,
    input_ptr: *mut FFICommonConfig,
    output_ptr: *mut FFIFileResult,
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

    let Some(params) = (unsafe { input_ptr.as_ref() }) else {
        error!("input data pointer is NULL");
        return EFAULT;
    };

    let details = match params.to_common_config(true) {
        Ok(d) => d,
        Err(e) => {
            error!("Couldn't convert compaction input data {e}");
            return EINVAL;
        }
    };

    // Run compaction
    let result = context
        .rt
        .block_on(run_compaction(&details, &context.sleeper_context));
    match result {
        Ok(res) => {
            if let Some(data) = unsafe { output_ptr.as_mut() } {
                data.rows_read = res.rows_read;
                data.rows_written = res.rows_written;
            } else {
                error!("output_data pointer is NULL");
                return EFAULT;
            }
            0
        }
        Err(e) => {
            error!("compacting error {e}");
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
/// The `query_results` is an "out" parameter.
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
/// The `ctx_ptr` value must point to a valid [`FFIContext`] which contains
/// an active runtime.
///
/// It is safe to release the [`FFIContext`] object (see [`destroy_context`](crate::context::destroy_context)) even
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
/// | -1 | Arrow/DataFusion error (see log) |
/// | EFAULT | if pointers are NULL
/// | EINVAL | if can't convert string to Rust string (invalid UTF-8?) |
/// | EINVAL | if row key field numbers or sort field numbers are empty |
#[allow(clippy::not_unsafe_ptr_arg_deref, unused_assignments, unused_variables)]
#[unsafe(no_mangle)]
pub extern "C" fn native_query_stream(
    ctx_ptr: *const FFIContext,
    input_ptr: *const FFILeafPartitionQueryConfig,
    query_results_ptr: *mut FFIQueryResults,
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

    let Some(params) = (unsafe { input_ptr.as_ref() }) else {
        error!("input data pointer is NULL");
        return EFAULT;
    };

    // Null check the results object pointer
    let Some(query_results) = (unsafe { query_results_ptr.as_mut() }) else {
        eprintln!("query_results pointer is NULL");
        return EFAULT;
    };

    let details = match params.to_leaf_config(false) {
        Ok(d) => d,
        Err(e) => {
            error!("Couldn't convert query input data {e}");
            return EINVAL;
        }
    };

    // Run compaction
    let result = context
        .rt
        .block_on(run_query(&details, &context.sleeper_context));
    match result {
        Ok(res) => {
            let CompletedOutput::ArrowRecordBatch(batch_stream) = res else {
                error!("Expected CompletedOutput::ArrowRecordBatch results from query");
                return -1;
            };
            // Convert the DataFusion stream of data to an FFI compatible Arrow stream
            let ffi_arrow_stream =
                Box::new(stream_to_ffi_arrow_stream(batch_stream, context.rt.clone()));
            // Leak pointer from Box. At this point Rust gives up ownership management of that object
            let leaked_ptr = Box::into_raw(ffi_arrow_stream);
            query_results.arrow_array_stream = leaked_ptr;
            0
        }
        Err(e) => {
            error!("query error {e}");
            -1
        }
    }
}

/// Provides the C FFI interface to calling the [`run_query`] function.
///
/// This function takes an [`FFILeafPartitionQueryConfig`] struct which contains all the information needed for a Sleeper
/// leaf partition query.
///
/// The `output_data` field is an out parameter. It is assumed the caller has allocated valid
/// memory at the address pointed to!
///
/// This function is intended to be callable by an external language via FFI,
/// therefore it is marked as `#[unsafe(no_mangle)]` to ensure the function
/// name is not changed and as `extern "C"` to ensure C linkage rules are followed.
///
/// # Safety
///
/// It is the callers responsibility to ensure all pointers are valid and point
/// to valid data before calling this function. While null pointers are detected,
/// invalid pointers cannot be.
///
/// It is undefined behaviour to specify an incorrect array length for any array.
///
/// The `ctx_ptr` value must point to a valid [`FFIContext`] which contains
/// an active runtime.
///
/// It is safe to release the [`FFIContext`] object (see [`destroy_context`](crate::context::destroy_context)) even
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
/// | -1 | Arrow/DataFusion error (see log) |
/// | EFAULT | if pointers are NULL
/// | EINVAL | if can't convert string to Rust string (invalid UTF-8?) |
/// | EINVAL | if row key field numbers or sort field numbers are empty |
#[allow(clippy::not_unsafe_ptr_arg_deref, unused_assignments, unused_variables)]
#[unsafe(no_mangle)]
pub extern "C" fn native_query_file(
    ctx_ptr: *const FFIContext,
    input_ptr: *const FFILeafPartitionQueryConfig,
    output_ptr: *mut FFIFileResult,
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

    let Some(params) = (unsafe { input_ptr.as_ref() }) else {
        error!("input data pointer is NULL");
        return EFAULT;
    };

    let details = match params.to_leaf_config(true) {
        Ok(d) => d,
        Err(e) => {
            error!("Couldn't convert query input data {e}");
            return EINVAL;
        }
    };

    // Run compaction
    let result = context
        .rt
        .block_on(run_query(&details, &context.sleeper_context));
    match result {
        Ok(res) => {
            let CompletedOutput::File(row_counts) = res else {
                error!("Expected CompletedOutput::File results from query");
                return -1;
            };
            if let Some(data) = unsafe { output_ptr.as_mut() } {
                data.rows_read = row_counts.rows_read;
                data.rows_written = row_counts.rows_written;
            } else {
                error!("output_data pointer is NULL");
                return EFAULT;
            }
            0
        }
        Err(e) => {
            error!("query error {e}");
            -1
        }
    }
}

/// Provides the C FFI interface to query the number of rows read so far by an in-progress compaction.
///
/// This function looks up the compaction identified by `c_job_id` in the [`FFIContext`]'s Sleeper
/// context and writes the current rows read count into the `output_ptr`'s `rows_read` field. The
/// `rows_written` field is set to 0, as this function only reports progress on the read side.
///
/// The `c_job_id` parameter is a pointer to a null-terminated C string identifying the job whose
/// progress is being queried.
///
/// The `output_ptr` field is an out parameter. It is assumed the caller has allocated valid
/// memory at the address pointed to!
///
/// This function validates the pointers are valid strings (or at least attempts to), but undefined behaviour will
/// result if bad pointers are passed.
///
/// # Safety
///
/// It is the callers responsibility to ensure all pointers are valid and point
/// to valid data before calling this function. While null pointers are detected,
/// invalid pointers cannot be.
///
/// The `ctx_ptr` value must point to a valid [`FFIContext`].
///
/// # Errors
/// The following result codes are returned.
///
/// | Code | Meaning |
/// |-|-|
/// | 0 | Success |
/// | -1 | No compaction is registered for the given job ID |
/// | EFAULT | if pointers are NULL
/// | EINVAL | if can't convert string to Rust string (invalid UTF-8?) |
///
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[unsafe(no_mangle)]
pub extern "C" fn native_get_compaction_rows_read(
    ctx_ptr: *const FFIContext,
    c_job_id: *const c_char,
    output_ptr: *mut FFIFileResult,
) -> c_int {
    // Null check the context pointer
    let Some(context) = (unsafe { ctx_ptr.as_ref() }) else {
        error!("NULL context pointer");
        return EFAULT;
    };

    // Null check the output pointer
    let Some(output) = (unsafe { output_ptr.as_mut() }) else {
        error!("NULL output pointer");
        return EFAULT;
    };

    // Null check job ID
    let Some(job_id_result) = unsafe { c_job_id.as_ref() }.map(|p| unpack_string(p)) else {
        error!("NULL job_id pointer");
        return EFAULT;
    };

    let Ok(job_id) = job_id_result else {
        error!("Non UTF-8 job ID value");
        return EINVAL;
    };

    if let Some(result) = context.sleeper_context.get_compaction_rows_read(job_id) {
        output.rows_read = result;
        output.rows_written = 0;
        0
    } else {
        -1
    }
}

/// Run a simulated compaction. This is for testing purposes only.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[unsafe(no_mangle)]
pub extern "C" fn native_simulate_compaction(ctx_ptr: *const FFIContext) -> c_int {
    // Null check the context pointer
    let Some(context) = (unsafe { ctx_ptr.as_ref() }) else {
        error!("NULL context pointer");
        return EFAULT;
    };

    let sleeper_context = &context.sleeper_context;

    context
        .rt
        .block_on(simulate_compaction_row_reads(sleeper_context));

    0
}
