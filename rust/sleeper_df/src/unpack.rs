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
use crate::objects::FFIRowKeySchemaType;
use color_eyre::{
    Report,
    eyre::{Result, bail, eyre},
};
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

/// Create a vector from a C pointer to an array of strings.
///
/// # Errors
/// If the array length is invalid, then behaviour is undefined.
pub fn unpack_string_array(
    array_base: *const *const c_char,
    len: usize,
) -> Result<Vec<&'static str>> {
    if len == 0 {
        return Ok(Vec::new());
    }
    if array_base.is_null() {
        bail!("NULL pointer for array_base in string array");
    }
    unsafe {
        // create a slice from the pointer
        slice::from_raw_parts(array_base, len)
    }
    .iter()
    // transform pointer to a non-owned string
    .map(|s| {
        if s.is_null() {
            Err(eyre!("Found NULL pointer in string array"))
        } else {
            //unpack length (signed because it's from Java)
            // This will have been allocated in Java so alignment will be ok
            #[allow(clippy::cast_ptr_alignment)]
            let str_len = unsafe { *(*s).cast::<i32>() };
            if str_len < 0 {
                bail!("Illegal string length in FFI array: {str_len}");
            }
            // convert to string and check it's valid
            std::str::from_utf8(unsafe {
                #[allow(clippy::cast_sign_loss)]
                slice::from_raw_parts(s.byte_add(4).cast::<u8>(), str_len as usize)
            })
            .map_err(Into::into)
        }
    })
    // now convert to a vector if all strings OK, else Err
    .collect()
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
    schema_types: &[FFIRowKeySchemaType],
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
                    FFIRowKeySchemaType::Int32 => {
                        Ok(match unsafe { bptr.cast::<i32>().as_ref() } {
                            Some(v) => PartitionBound::Int32(*v),
                            None => PartitionBound::Unbounded,
                        })
                    }
                    FFIRowKeySchemaType::Int64 => {
                        Ok(match unsafe { bptr.cast::<i64>().as_ref() } {
                            Some(v) => PartitionBound::Int64(*v),
                            None => PartitionBound::Unbounded,
                        })
                    }
                    FFIRowKeySchemaType::String => {
                        match unsafe { bptr.cast::<i32>().as_ref() } {
                            //unpack length (signed because it's from Java)
                            Some(str_len) => {
                                if *str_len < 0 {
                                    bail!("Illegal string variant length in FFI array: {str_len}");
                                }
                                std::str::from_utf8(unsafe {
                                    #[allow(clippy::cast_sign_loss)]
                                    slice::from_raw_parts(
                                        bptr.byte_add(4).cast::<u8>(),
                                        *str_len as usize,
                                    )
                                })
                                .map_err(Report::from)
                                .map(PartitionBound::String)
                            }
                            None => Ok(PartitionBound::Unbounded),
                        }
                    }
                    FFIRowKeySchemaType::ByteArray => {
                        match unsafe { bptr.cast::<i32>().as_ref() } {
                            //unpack length (signed because it's from Java)
                            Some(byte_len) => {
                                if *byte_len < 0 {
                                    bail!(
                                        "Illegal byte array variant length in FFI array: {byte_len}"
                                    );
                                }
                                Ok(PartitionBound::ByteArray(unsafe {
                                    #[allow(clippy::cast_sign_loss)]
                                    slice::from_raw_parts(
                                        bptr.byte_add(4).cast::<u8>(),
                                        *byte_len as usize,
                                    )
                                }))
                            }
                            None => Ok(PartitionBound::Unbounded),
                        }
                    }
                }
            }
        })
        .collect()
}
