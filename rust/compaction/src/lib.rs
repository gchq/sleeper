//! The `compaction` crate implements all the core functionality for running Sleeper
//! Parquet data compaction in Rust. We provide a C library interface wrapper which
//! will serve as the interface from Java code in Sleeper. We are careful to adhere to C style
//! conventions here such as libc error codes.
//!
//! We have an internal "details" module that encapsulates the internal workings. All the
//! public API should be in this module.
/*
 * Copyright 2022-2023 Crown Copyright
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

mod aws_s3;
mod details;
mod sketch;

use arrow::error::ArrowError;
use aws_credential_types::provider::ProvideCredentials;
use chrono::Local;
use futures::TryFutureExt;
use libc::{size_t, EFAULT, EINVAL, EIO};
use log::{debug, error, LevelFilter};
use std::io::Write;
use std::sync::Once;
use std::{
    ffi::{c_char, c_int, c_uchar, CStr},
    slice,
};
use url::Url;

// Just publicly expose this function
pub use aws_s3::ObjectStoreFactory;
pub use details::create_sketch_path;
pub use details::get_file_iterator;
pub use details::get_parquet_builder;
pub use details::merge_sorted_files;
pub use details::read_schema;
pub use details::validate_schemas_same;
pub use details::CompactionResult;

/// An object guaranteed to only initialise once. Thread safe.
static LOG_CFG: Once = Once::new();

/// A one time initialisation of the logging library.
///
/// This function uses a [`Once`] object to ensure
/// initialisation only happens once. This is safe even
/// if called from multiple threads.
fn maybe_cfg_log() {
    LOG_CFG.call_once(|| {
        // Install and configure environment logger
        env_logger::builder()
            .format(|buf, record| {
                writeln!(
                    buf,
                    "{} [{}] - {}",
                    Local::now().format("%Y-%m-%dT%H:%M:%S"),
                    record.level(),
                    record.args()
                )
            })
            .format_timestamp(Some(env_logger::TimestampPrecision::Millis))
            .format_target(false)
            .filter_level(LevelFilter::Info)
            .init();
    });
}

/// Create a vector from a C pointer to a type.
///
/// # Errors
/// If the array length is invalid, then behaviour is undefined.
#[must_use]
fn array_helper<T: Clone>(array: *const T, len: usize) -> Vec<T> {
    unsafe { slice::from_raw_parts(array, len).to_vec() }
}

/// Obtains AWS credentials from normal places and then calls merge function
/// with obtained credentials.
///
async fn credentials_and_merge(
    input_paths: &[Url],
    output_path: &Url,
    row_group_size: size_t,
    max_page_size: size_t,
    row_fields: &[usize],
    sort_cols: &[usize],
) -> Result<CompactionResult, ArrowError> {
    let config = aws_config::from_env().load().await;
    let region = config
        .region()
        .ok_or(ArrowError::IoError("Couldn't retrieve AWS region".into()))?;
    let creds: aws_credential_types::Credentials = config
        .credentials_provider()
        .ok_or(ArrowError::IoError(
            "Couldn't retrieve AWS credentials".into(),
        ))?
        .provide_credentials()
        .map_err(|e| ArrowError::ExternalError(Box::new(e)))
        .await?;
    merge_sorted_files(
        Some(creds),
        region,
        input_paths,
        output_path,
        row_group_size,
        max_page_size,
        row_fields,
        sort_cols,
    )
    .await
}

/// Contains all output data from a compaction operation.
#[repr(C)]
pub struct FFICompactionResult {
    /// The minimum key seen in column zero.
    min_key: *const c_uchar,
    /// Length of minimum key
    min_key_len: size_t,
    /// The maximum key seen in column zero.
    max_key: *const c_uchar,
    /// Length of maximum key
    max_key_len: size_t,
    /// The total number of rows read by a compaction.
    rows_read: size_t,
    /// The total number of rows written by a compaction.
    rows_written: size_t,
}

/// Creates and returns a pointer to a [`FFICompactionResult`].
///
/// This allocates space for the object and initialises it with zeroes
/// and null pointers.
///
/// The result of this function can be safely passed to [`ffi_merge_sorted_files()`] and
/// must be de-allocated by calling [`free_result()`].
///
#[no_mangle]
pub extern "C" fn allocate_result() -> *const FFICompactionResult {
    maybe_cfg_log();
    let p = Box::into_raw(Box::new(FFICompactionResult {
        min_key: std::ptr::null(),
        min_key_len: 0,
        max_key: std::ptr::null(),
        max_key_len: 0,
        rows_read: 0,
        rows_written: 0,
    }));
    debug!("Compaction result allocated @ {:p}", p);
    p
}

/// Provides the C FFI interface to calling the [`merge_sorted_files`] function.
///
/// This function has the same signature as [`merge_sorted_files`], but with
/// C FFI bindings. This function validates the pointers are valid strings (or
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
#[no_mangle]
pub extern "C" fn ffi_merge_sorted_files(
    input_file_paths: *const *const c_char,
    input_file_paths_len: size_t,
    output_file_path: *const c_char,
    row_group_size: size_t,
    max_page_size: size_t,
    row_key_columns: *const size_t,
    row_key_columns_len: size_t,
    sort_columns: *const size_t,
    sort_columns_len: size_t,
    output_data: *mut FFICompactionResult,
) -> c_int {
    maybe_cfg_log();

    // Check for nulls
    if input_file_paths.is_null() || output_file_path.is_null() || output_data.is_null() {
        error!("Either input or output array pointer or output_data struct pointer is null");
        return EFAULT;
    }

    // First convert the C string array to an array of Rust string slices.
    let Ok(input_paths) = (unsafe {
        // create a slice from the pointer
        slice::from_raw_parts(input_file_paths, input_file_paths_len)
            .iter()
            // transform pointer to a non-owned string
            .map(|s| {
                // is pointer valid?
                if s.is_null() {
                    return Err(ArrowError::IoError(String::new()));
                }
                // convert to string and check it's valid
                CStr::from_ptr(*s)
                .to_str()
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))
            })
            // now convert to a vector if all strings OK, else Err
            .map(|x| x.map(Url::parse))
            .collect::<Result<Vec<_>,_>>()
    }) else {
        error!("Error converting input paths as valid UTF-8");
        return EINVAL;
    };

    // Now unwrap the URL parsing errors
    let input_paths = match input_paths.into_iter().collect::<Result<Vec<_>, _>>() {
        Ok(v) => v,
        Err(e) => {
            error!("URL parsing error on input paths {}", e);
            return EINVAL;
        }
    };

    // Get output file URL
    let Ok(Ok(output_path)) = (unsafe {
        CStr::from_ptr(output_file_path).to_str()
    }).map(Url::parse) else {
        error!("URL parsing error on output path");
        return EINVAL;
    };

    // Convert C pointer to dynamic arrays
    let row_fields = array_helper(row_key_columns, row_key_columns_len);
    let sort_cols = array_helper(sort_columns, sort_columns_len);

    // Start async runtime
    let rt = match tokio::runtime::Runtime::new() {
        Ok(v) => v,
        Err(e) => {
            error!("Couldn't create Rust tokio runtime {}", e);
            return EIO;
        }
    };

    // Run compaction
    let result = rt.block_on(credentials_and_merge(
        &input_paths,
        &output_path,
        row_group_size,
        max_page_size,
        &row_fields,
        &sort_cols,
    ));

    match result {
        Ok(mut res) => {
            // Safely take the min/max key vectors out of the compaction result
            let mut min_vec = std::mem::take(&mut res.min_key);
            // shrink the vector as necessary and confirm size
            min_vec.shrink_to_fit();
            assert!(min_vec.len() == min_vec.capacity());
            let min_key_len = min_vec.len();
            let min_vec_ptr = min_vec.as_mut_ptr();
            // make sure we don't deallocate the vector!
            std::mem::forget(min_vec);

            // Safely take the min/max key vectors out of the compaction result
            let mut max_vec = std::mem::take(&mut res.max_key);
            // shrink the vector as necessary and confirm size
            max_vec.shrink_to_fit();
            assert!(max_vec.len() == max_vec.capacity());
            let max_key_len = max_vec.len();
            let max_vec_ptr = max_vec.as_mut_ptr();
            // make sure we don't deallocate the vector!
            std::mem::forget(max_vec);

            if let Some(data) = unsafe { output_data.as_mut() } {
                data.rows_read = res.rows_read;
                data.rows_written = res.rows_written;
                data.min_key_len = min_key_len;
                data.min_key = min_vec_ptr;
                data.max_key_len = max_key_len;
                data.max_key = max_vec_ptr;
            }
            0
        }
        Err(e) => {
            error!("merging error {}", e);
            -1
        }
    }
}

/// Free the compaction result previously allocated by [`allocate_result()`].
///
/// This function must only be called on pointers to objects allocated by Rust.
///
#[allow(clippy::missing_panics_doc, clippy::not_unsafe_ptr_arg_deref)]
#[no_mangle]
pub extern "C" fn free_result(ob: *mut FFICompactionResult) {
    maybe_cfg_log();
    if !ob.is_null() {
        // we  need to de-allocate the two byte vectors inside the result
        debug!("Compaction result destructed at {:p}", ob);
        let res = unsafe { Box::from_raw(ob) };
        if !res.min_key.is_null() {
            unsafe {
                Vec::from_raw_parts(
                    res.min_key as *mut c_uchar,
                    res.min_key_len,
                    res.min_key_len,
                );
            }
        }
        if !res.max_key.is_null() {
            unsafe {
                Vec::from_raw_parts(
                    res.max_key as *mut c_uchar,
                    res.max_key_len,
                    res.max_key_len,
                );
            }
        }
    }
}
