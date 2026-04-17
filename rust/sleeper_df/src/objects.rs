//! All Foreign Function Interface compatible structs are here.
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
use color_eyre::{Report, eyre::eyre};
use std::{
    ffi::{c_char, c_uchar},
    fmt::Display,
    slice,
};

pub mod aws_config;
pub mod ffi_common_config;
pub mod ffi_parquet_options;
pub mod query;
pub mod sleeper_region;

/// Contains all output data from a file output operation.
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/common/foreign-bridge/src/main/java/sleeper/foreign/FFIFileResult.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
pub struct FFIFileResult {
    /// The total number of rows read by a query/compaction.
    pub rows_read: usize,
    /// The total number of rows written by a query/compaction.
    pub rows_written: usize,
}

/// Data type for row key fields in Sleeper schema.
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/common/foreign-bridge/src/main/java/sleeper/foreign/FFIElementType.java.
/// The order and types of the fields must match exactly.
#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub enum FFIElementType {
    Int32 = 1,
    Int64 = 2,
    String = 3,
    ByteArray = 4,
}

impl FFIElementType {
    pub fn display_type(&self) -> &str {
        match self {
            FFIElementType::Int32 => "i32",
            FFIElementType::Int64 => "i64",
            FFIElementType::String => "*const c_char",
            FFIElementType::ByteArray => "*const c_uchar",
        }
    }
}

impl TryFrom<&usize> for FFIElementType {
    type Error = color_eyre::Report;

    fn try_from(ordinal: &usize) -> Result<Self, Self::Error> {
        match ordinal {
            1 => Ok(FFIElementType::Int32),
            2 => Ok(FFIElementType::Int64),
            3 => Ok(FFIElementType::String),
            4 => Ok(FFIElementType::ByteArray),
            _ => Err(eyre!("Invalid FFIElementType ordinal value")),
        }
    }
}

impl Display for FFIElementType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FFIElementType {}", self.display_type())
    }
}

/// Variant type for encoding a Sleeper row key element of a specific type.
/// This is a union type, storage for all members overlaps!
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/common/foreign-bridge/src/main/java/sleeper/foreign/FFIElementData.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
pub union FFIElementData {
    int32: i32,
    int64: i64,
    string: *const c_char,
    bytes: *const c_uchar,
}

/// Describes a single Sleeper row key item. This uses an explicit tagged union that determines that active union member.
///
///  *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/common/foreign-bridge/src/main/java/sleeper/foreign/FFIElement.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
pub struct FFIElement {
    contained: FFIElementType,
    item: FFIElementData,
}

impl TryFrom<&FFIElement> for i32 {
    type Error = Report;

    fn try_from(value: &FFIElement) -> Result<Self, Self::Error> {
        match value.contained {
            FFIElementType::Int32 => Ok(unsafe { value.item.int32 }),
            _ => Err(eyre!("Can't extract i32 from {}", value.contained)),
        }
    }
}

impl TryFrom<&FFIElement> for i64 {
    type Error = Report;

    fn try_from(value: &FFIElement) -> Result<Self, Self::Error> {
        match value.contained {
            FFIElementType::Int64 => Ok(unsafe { value.item.int64 }),
            _ => Err(eyre!("Can't extract i64 from {}", value.contained)),
        }
    }
}

impl TryFrom<&FFIElement> for *const c_char {
    type Error = Report;

    fn try_from(value: &FFIElement) -> Result<Self, Self::Error> {
        match value.contained {
            FFIElementType::String => Ok(unsafe { value.item.string }),
            _ => Err(eyre!(
                "Can't extract *const c_char from {}",
                value.contained
            )),
        }
    }
}

impl TryFrom<&FFIElement> for *const c_uchar {
    type Error = Report;

    fn try_from(value: &FFIElement) -> Result<Self, Self::Error> {
        match value.contained {
            FFIElementType::ByteArray => Ok(unsafe { value.item.bytes }),
            _ => Err(eyre!(
                "Can't extract *const c_uchar from {}",
                value.contained
            )),
        }
    }
}

/// Represents an array of bytes (unsigned char in C, u8 in Rust) with a length.
///
/// This can be converted to a byte array slice or a string slice (assuming it is valid UTF-8).
///
/// Whilst this is a C compatible FFI struct. It has a pure Java definition in
/// java/common/foreign-bridge/src/main/java/sleeper/foreign/FFIBytes.java. If you change the ordering or types
/// of fields in this struct, you MUST update the writeTo/readFrom methods in that class!
#[repr(C)]
#[derive(Copy, Clone)]
pub struct FFIBytes {
    pub length: usize,
    pub buffer: *const c_uchar,
}

impl TryFrom<&FFIBytes> for &str {
    type Error = Report;

    fn try_from(value: &FFIBytes) -> Result<Self, Self::Error> {
        Ok(std::str::from_utf8(<&[u8]>::from(value))?)
    }
}

impl From<&FFIBytes> for &[u8] {
    fn from(value: &FFIBytes) -> Self {
        unsafe { slice::from_raw_parts(value.buffer, value.length) }
    }
}
