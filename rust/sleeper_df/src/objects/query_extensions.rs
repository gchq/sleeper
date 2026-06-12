//! FFI extension structs.
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
use crate::objects::ExtensionFFIDetails;
use std::ffi::c_char;

/// Defines data required for the SQL in queries.
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/common/foreign-bridge/src/main/java/sleeper/foreign/datafusion/extensions/FFISQLExtension.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
#[derive(Debug)]
pub struct FFISQLExtension {
    /// The SQL query string to use for filtering a Sleeper query.
    pub sql: *const c_char,
}

impl ExtensionFFIDetails for FFISQLExtension {
    /// Maximum of one instance per query.
    const MAX_CARDINALITY: usize = 1;
}
