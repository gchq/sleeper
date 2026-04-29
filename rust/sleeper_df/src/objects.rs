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
use sleeper_core::PartitionBound;
use std::{ffi::c_uchar, fmt::Display, slice};

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
/// you MUST update the corresponding Java definition in java/common/foreign-bridge/src/main/java/sleeper/foreign/FFIRowKeyValueType.java.
/// The order and types of the fields must match exactly.
#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub enum FFIRowKeyValueType {
    Int32 = 1,
    Int64 = 2,
    String = 3,
    ByteArray = 4,
    Empty = 5,
}

impl FFIRowKeyValueType {
    pub fn display_type(&self) -> &str {
        match self {
            FFIRowKeyValueType::Int32 => "i32",
            FFIRowKeyValueType::Int64 => "i64",
            FFIRowKeyValueType::String => "*const c_char",
            FFIRowKeyValueType::ByteArray => "*const c_uchar",
            FFIRowKeyValueType::Empty => "<<<empty>>>",
        }
    }
}

impl TryFrom<&usize> for FFIRowKeyValueType {
    type Error = color_eyre::Report;

    fn try_from(ordinal: &usize) -> Result<Self, Self::Error> {
        match ordinal {
            1 => Ok(FFIRowKeyValueType::Int32),
            2 => Ok(FFIRowKeyValueType::Int64),
            3 => Ok(FFIRowKeyValueType::String),
            4 => Ok(FFIRowKeyValueType::ByteArray),
            5 => Ok(FFIRowKeyValueType::Empty),
            _ => Err(eyre!("Invalid FFIElementType ordinal value")),
        }
    }
}

impl Display for FFIRowKeyValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FFIElementType {}", self.display_type())
    }
}

/// Variant type for encoding a Sleeper row key element of a specific type.
/// This is a union type, storage for all members overlaps!
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/common/foreign-bridge/src/main/java/sleeper/foreign/FFIRowKeyValueData.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
pub union FFIRowKeyValueData {
    int32: i32,
    int64: i64,
    string: *const FFIBytes,
    bytes: *const FFIBytes,
}

/// Describes a single Sleeper row key item. This uses an explicit tagged union that determines that active union member.
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/common/foreign-bridge/src/main/java/sleeper/foreign/FFIRowKeyValue.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
pub struct FFIRowKeyValue {
    /// Specifies the active type inside the element data
    contained: FFIRowKeyValueType,
    /// Element data unspecified if contained type is [`FFIElementType::Empty`]
    item: FFIRowKeyValueData,
}

impl TryFrom<&FFIRowKeyValue> for PartitionBound<'_> {
    type Error = Report;

    fn try_from(value: &FFIRowKeyValue) -> Result<Self, Self::Error> {
        Ok(match value.contained {
            FFIRowKeyValueType::Int32 => PartitionBound::Int32(unsafe { value.item.int32 }),
            FFIRowKeyValueType::Int64 => PartitionBound::Int64(unsafe { value.item.int64 }),
            FFIRowKeyValueType::String => PartitionBound::String(
                unsafe { value.item.string.as_ref() }
                    .ok_or(eyre!("FFIElement string pointer is NULL"))?
                    .try_into()?,
            ),
            FFIRowKeyValueType::ByteArray => PartitionBound::ByteArray(Into::<&[u8]>::into(
                unsafe { value.item.bytes.as_ref() }
                    .ok_or(eyre!("FFIElement byte array pointer is NULL"))?,
            )),
            FFIRowKeyValueType::Empty => PartitionBound::Unbounded,
        })
    }
}

/// Represents an array of bytes (unsigned char in C, u8 in Rust) with a length.
///
/// This can be converted to a byte array slice or a string slice (assuming it is valid UTF-8).
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/common/foreign-bridge/src/main/java/sleeper/foreign/FFIBytes.java.
/// The order and types of the fields must match exactly.
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
