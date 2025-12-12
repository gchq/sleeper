//! All Foreign Function Interface compatible structs are here.
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
use color_eyre::eyre::eyre;

pub mod aws_config;
pub mod ffi_common_config;
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
/// Encoded as integer type for FFI compatibility.
#[derive(Copy, Clone)]
pub enum RowKeySchemaType {
    Int32 = 1,
    Int64 = 2,
    String = 3,
    ByteArray = 4,
}

impl TryFrom<&usize> for RowKeySchemaType {
    type Error = color_eyre::Report;

    fn try_from(ordinal: &usize) -> Result<Self, Self::Error> {
        match ordinal {
            1 => Ok(RowKeySchemaType::Int32),
            2 => Ok(RowKeySchemaType::Int64),
            3 => Ok(RowKeySchemaType::String),
            4 => Ok(RowKeySchemaType::ByteArray),
            _ => Err(eyre!("Invalid FFIRowKeySchemaType ordinal value")),
        }
    }
}
