//! FFI extension core structs.
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
use crate::objects::query_extensions::FFISQLExtension;
use color_eyre::eyre::eyre;
use std::fmt::Display;

/// Type tag for an extension.
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/common/foreign-bridge/src/main/java/sleeper/foreign/datafusion/extension/FFIExtensionVariant.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
#[derive(Debug)]
pub enum FFIExtensionVariant {
    SQL = 1,
}

impl FFIExtensionVariant {
    pub fn display_type(&self) -> &str {
        match self {
            FFIExtensionVariant::SQL => "SQL",
        }
    }
}

impl TryFrom<&usize> for FFIExtensionVariant {
    type Error = color_eyre::Report;

    fn try_from(ordinal: &usize) -> Result<Self, Self::Error> {
        match ordinal {
            1 => Ok(FFIExtensionVariant::SQL),
            _ => Err(eyre!("Invalid FFIExtensionVariant ordinal value")),
        }
    }
}

impl Display for FFIExtensionVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FFIExtensionVariant {}", self.display_type())
    }
}

/// Variant type for an extension of a specific type.
/// This is a union type, storage for all members overlaps!
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/common/foreign-bridge/src/main/java/sleeper/foreign/datafusion/extension/FFIExtensionData.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
pub union FFIExtensionData {
    pub sql: *const FFISQLExtension,
}

/// Contains extra data relating to a Sleeper compaction or query. Each extension type maybe specified once or multiple
/// times depending on its purpose.
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/common/foreign-bridge/src/main/java/sleeper/foreign/datafusion/extension/FFIExtension.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
pub struct FFIExtension {
    /// The type of extension
    pub variant: FFIExtensionVariant,
    /// Information specific to this extension type
    pub data: FFIExtensionData,
}
