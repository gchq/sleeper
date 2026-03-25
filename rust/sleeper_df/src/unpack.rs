//! Utility functions for unpacking FFI arrays and objects.
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
use crate::objects::{FFIBytes, RowKeySchemaType};
use color_eyre::eyre::{Result, bail, eyre};
use sleeper_core::PartitionBound;
use std::{
    ffi::{CStr, c_char, c_void},
    slice,
};

pub fn unpack_str<'a>(pointer: *const c_char) -> Result<&'a str> {
    Ok(unsafe { CStr::from_ptr(pointer) }.to_str()?)
}

pub fn unpack_string(pointer: *const c_char) -> Result<String> {
    unpack_str(pointer).map(ToOwned::to_owned)
}

/// Create a vector of a generic type.
///
/// # Errors
/// If the array length is invalid, then behaviour is undefined.
pub fn unpack_typed_array<T: Copy>(array_base: *const *const T, len: usize) -> Result<Vec<T>> {
    if array_base.is_null() {
        bail!("NULL pointer for array_base in generic typed array");
    }
    unsafe { slice::from_raw_parts(array_base, len) }
        .iter()
        .map(|p| {
            if p.is_null() {
                Err(eyre!("Found NULL pointer in generic typed array"))
            } else {
                Ok(unsafe { **p })
            }
        })
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
/// If `nulls_present` is false and a NULL pointer is found.
///
/// Also panics if a negative array length is found in decoding byte arrays or strings.
pub fn unpack_variant_array<'a>(
    array_base: *const *const c_void,
    len: usize,
    schema_types: &[RowKeySchemaType],
    nulls_present: bool,
) -> Result<Vec<PartitionBound<'a>>> {
    assert!(
        len <= schema_types.len(),
        "More array elements than schema_types!"
    );
    if array_base.is_null() {
        bail!("NULL pointer for array_base in variant array");
    }
    unsafe { slice::from_raw_parts(array_base, len) }
        .iter()
        .zip(schema_types.iter())
        .map(|(&bptr, type_id)| {
            if !nulls_present && bptr.is_null() {
                Err(eyre!("Found NULL pointer in variant array"))
            } else {
                match type_id {
                    RowKeySchemaType::Int32 => Ok(match unsafe { bptr.cast::<i32>().as_ref() } {
                        Some(v) => PartitionBound::Int32(*v),
                        None => PartitionBound::Unbounded,
                    }),
                    RowKeySchemaType::Int64 => Ok(match unsafe { bptr.cast::<i64>().as_ref() } {
                        Some(v) => PartitionBound::Int64(*v),
                        None => PartitionBound::Unbounded,
                    }),
                    RowKeySchemaType::String => {
                        match unsafe { bptr.cast::<FFIBytes>().as_ref() } {
                            //unpack length (signed because it's from Java)
                            Some(bytes) => Ok(PartitionBound::String(bytes.try_into()?)),
                            None => Ok(PartitionBound::Unbounded),
                        }
                    }
                    RowKeySchemaType::ByteArray => {
                        match unsafe { bptr.cast::<FFIBytes>().as_ref() } {
                            //unpack length (signed because it's from Java)
                            Some(bytes) => Ok(PartitionBound::ByteArray(bytes.into())),
                            None => Ok(PartitionBound::Unbounded),
                        }
                    }
                }
            }
        })
        .collect()
}
