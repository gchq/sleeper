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
use aws_config::BehaviorVersion;
use aws_credential_types::provider::ProvideCredentials;
use chrono::Local;
use futures::TryFutureExt;
use libc::{c_void, size_t, EFAULT, EINVAL, EIO};
use log::{error, info, LevelFilter};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::io::Write;
use std::str::Utf8Error;
use std::sync::Once;
use std::{
    ffi::{c_char, c_int, CStr},
    slice,
};
use url::Url;

pub use details::merge_sorted_files;
pub use details::{ColRange, CompactionInput, CompactionResult, PartitionBound};

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
                    "{} [{}] {}:{} - {}",
                    Local::now().format("%Y-%m-%dT%H:%M:%S"),
                    record.level(),
                    record.file().unwrap_or("??"),
                    record.line().unwrap_or(0),
                    record.args()
                )
            })
            .format_timestamp(Some(env_logger::TimestampPrecision::Millis))
            .format_target(false)
            .filter_level(LevelFilter::Info)
            .init();
    });
}

/// Obtains AWS credentials from normal places and then calls merge function
/// with obtained credentials.
///
async fn credentials_and_merge(
    input_data: &CompactionInput,
) -> Result<CompactionResult, ArrowError> {
    let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let region = config.region().ok_or(ArrowError::InvalidArgumentError(
        "Couldn't retrieve AWS region".into(),
    ))?;
    let creds: aws_credential_types::Credentials = config
        .credentials_provider()
        .ok_or(ArrowError::InvalidArgumentError(
            "Couldn't retrieve AWS credentials".into(),
        ))?
        .provide_credentials()
        .map_err(|e| ArrowError::ExternalError(Box::new(e)))
        .await?;
    merge_sorted_files(Some(creds), region, input_data).await
}

/// Contains all the input data for setting up a compaction.
///
/// See java/compaction/compaction-rust/src/main/java/sleeper/compaction/jobexecution/RustBridge.java
/// for details. Field ordering and types MUST match between the two definitions!
#[repr(C)]
pub struct FFICompactionParams {
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
    region_maxs: *const *const c_void,
    region_mins_inclusive_len: usize,
    region_mins_inclusive: *const *const bool,
    region_maxs_inclusive_len: usize,
    region_maxs_inclusive: *const *const bool,
}

impl TryFrom<&FFICompactionParams> for CompactionInput {
    type Error = anyhow::Error;

    fn try_from(params: &FFICompactionParams) -> Result<Self, Self::Error> {
        // We do this separately since we need the values for computing the region
        let row_key_cols = unpack_string_array(params.row_key_cols, params.row_key_cols_len)?
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>();
        let region = compute_region(params, &row_key_cols)?;

        Ok(Self {
            input_files: unpack_string_array(params.input_files, params.input_files_len)?
                .into_iter()
                .map(Url::parse)
                .collect::<Result<Vec<_>, _>>()?,
            output_file: unsafe { CStr::from_ptr(params.output_file) }
                .to_str()
                .map(Url::parse)??,
            row_key_cols,
            sort_key_cols: unpack_string_array(params.sort_key_cols, params.sort_key_cols_len)?
                .into_iter()
                .map(String::from)
                .collect(),
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
            region,
        })
    }
}

fn compute_region<T: Borrow<str>>(
    params: &FFICompactionParams,
    row_key_cols: &[T],
) -> Result<HashMap<String, ColRange>, anyhow::Error> {
    let region_mins_inclusive = unpack_primitive_array(
        params.region_mins_inclusive,
        params.region_mins_inclusive_len,
    );
    let region_maxs_inclusive = unpack_primitive_array(
        params.region_maxs_inclusive,
        params.region_maxs_inclusive_len,
    );
    let schema_types = unpack_primitive_array(params.row_key_schema, params.row_key_schema_len);
    let region_mins =
        unpack_variant_array(params.region_mins, params.region_mins_len, &schema_types)?;
    let region_maxs =
        unpack_variant_array(params.region_maxs, params.region_maxs_len, &schema_types)?;

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
    Ok(map)
}

/// Contains all output data from a compaction operation.
#[repr(C)]
pub struct FFICompactionResult {
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
        rows_read: 0,
        rows_written: 0,
    }));
    info!("Compaction result allocated @ {:p}", p);
    p
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
#[no_mangle]
pub extern "C" fn ffi_merge_sorted_files(
    input_data: *mut FFICompactionParams,
    output_data: *mut FFICompactionResult,
) -> c_int {
    maybe_cfg_log();
    let Some(params) = (unsafe { input_data.as_ref() }) else {
        error!("input data pointer is null");
        return EFAULT;
    };

    // Start async runtime
    let rt = match tokio::runtime::Runtime::new() {
        Ok(v) => v,
        Err(e) => {
            error!("Couldn't create Rust tokio runtime {}", e);
            return EIO;
        }
    };

    let details = match TryInto::<CompactionInput>::try_into(params) {
        Ok(d) => d,
        Err(e) => {
            error!("Couldn't convert compaction input data {}", e);
            return EINVAL;
        }
    };

    // Run compaction
    let result = rt.block_on(credentials_and_merge(&details));
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
            error!("merging error {}", e);
            -1
        }
    }
}

/// Create a vector from a C pointer to an array of strings.
///
/// # Errors
/// If the array length is invalid, then behaviour is undefined.
fn unpack_string_array(
    array_base: *const *const c_char,
    len: usize,
) -> Result<Vec<&'static str>, ArrowError> {
    unsafe {
        // create a slice from the pointer
        slice::from_raw_parts(array_base, len)
    }
    .iter()
    .inspect(|p| {
        if p.is_null() {
            error!("Found NULL pointer in string array");
        }
    })
    // transform pointer to a non-owned string
    .map(|s| {
        //unpack length (signed because it's from Java)
        // This will have been allocated in Java so alignment will be ok
        #[allow(clippy::cast_ptr_alignment)]
        let str_len = unsafe { *(*s).cast::<i32>() };
        if str_len < 0 {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Illegal string length in FFI array: {str_len}"
            )));
        }
        // convert to string and check it's valid
        std::str::from_utf8(unsafe {
            #[allow(clippy::cast_sign_loss)]
            slice::from_raw_parts(s.byte_add(4).cast::<u8>(), str_len as usize)
        })
        .map_err(|e| ArrowError::ExternalError(Box::new(e)))
    })
    // now convert to a vector if all strings OK, else Err
    .collect::<Result<Vec<_>, _>>()
}

/// Create a vector of a primitive array type.
///
/// # Errors
/// If the array length is invalid, then behaviour is undefined.
fn unpack_primitive_array<T: Copy>(array_base: *const *const T, len: usize) -> Vec<T> {
    unsafe { slice::from_raw_parts(array_base, len) }
        .iter()
        .inspect(|p| {
            if p.is_null() {
                error!("Found NULL pointer in string array");
            }
        })
        .map(|&bptr| unsafe { *bptr })
        .collect()
}

/// Create a vector of a variant array type. Each element may be a
/// i32, i64, String or byte array. The schema types define what decoding is attempted.
///
/// # Errors
/// If the array length is invalid, then behaviour is undefined.
/// If the schema types are incorrect, then behaviour is undefined.
///
/// # Panics
/// If the length of the `schema_types` array doesn't match the length specified.
///
/// Also panics if a negative array length is found in decoding byte arrays or strings.
fn unpack_variant_array(
    array_base: *const *const c_void,
    len: usize,
    schema_types: &[i32],
) -> Result<Vec<PartitionBound>, Utf8Error> {
    assert_eq!(len, schema_types.len());
    unsafe { slice::from_raw_parts(array_base, len) }
        .iter()
        .inspect(|p| {
            if p.is_null() {
                error!("Found NULL pointer in string array");
            }
        })
        .zip(schema_types.iter())
        .map(|(&bptr, type_id)| match type_id {
            1 => Ok(PartitionBound::Int32 {
                val: unsafe { *bptr.cast::<i32>() },
            }),
            2 => Ok(PartitionBound::Int64 {
                val: unsafe { *bptr.cast::<i64>() },
            }),
            3 => {
                //unpack length (signed because it's from Java)
                let str_len = unsafe { *bptr.cast::<i32>() };
                if str_len < 0 {
                    error!("Illegal string length in FFI array: {str_len}");
                    panic!("Illegal string length in FFI array: {str_len}");
                }
                std::str::from_utf8(unsafe {
                    #[allow(clippy::cast_sign_loss)]
                    slice::from_raw_parts(bptr.byte_add(4).cast::<u8>(), str_len as usize)
                })
                .map(|v| PartitionBound::String { val: v })
            }
            4 => {
                //unpack length (signed because it's from Java)
                let byte_len = unsafe { *bptr.cast::<i32>() };
                if byte_len < 0 {
                    error!("Illegal byte array length in FFI array: {byte_len}");
                    panic!("Illegal byte array length in FFI array: {byte_len}");
                }
                Ok(PartitionBound::ByteArray {
                    val: unsafe {
                        #[allow(clippy::cast_sign_loss)]
                        slice::from_raw_parts(bptr.byte_add(4).cast::<i8>(), byte_len as usize)
                    },
                })
            }
            x => {
                error!("Unexpected type id {x}");
                panic!("Unexpected type id {x}");
            }
        })
        .collect()
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
        info!("Compaction result destructed at {:p}", ob);
        let _ = unsafe { Box::from_raw(ob) };
    }
}
