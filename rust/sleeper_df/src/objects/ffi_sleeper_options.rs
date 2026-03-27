//! Sleeper options configuration that clients may choose to alter. Defaults should be sensible for most clients.
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

use std::ffi::{CStr, c_char};

pub const USE_READAHEAD_STORE: bool = false;
pub const READ_PAGE_INDEXES: bool = false;
pub const MAX_ROW_GROUP_SIZE: usize = 100_000;
pub const MAX_PAGE_SIZE: usize = 128 * 1024;
pub const COMPRESSION_CODEC: &CStr = c"zstd";
pub const WRITER_VERSION: &CStr = c"v2";
pub const COLUMN_TRUNCATE_LENGTH: usize = 128;
pub const STATS_TRUNCATE_LENGTH: usize = 2147483647;
pub const DICT_ENCODE_ROW_KEYS: bool = false;
pub const DICT_ENCODE_SORT_KEYS: bool = false;
pub const DICT_ENCODE_VALUES: bool = false;

/// Contains all the Sleeper options for DataFusion operation. These may not be needed for every DataFusion usage,
/// so come with reasonable defaults. See Java side documentation for explanation of fields.
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/common/foreign-bridge/src/main/java/sleeper/foreign/datafusion/FFISleeperOptions.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
pub struct FFISleeperOptions {
    pub use_readahead_store: bool,
    pub read_page_indexes: bool,
    pub max_row_group_size: usize,
    pub max_page_size: usize,
    pub compression: *const c_char,
    pub writer_version: *const c_char,
    pub column_truncate_length: usize,
    pub stats_truncate_length: usize,
    pub dict_enc_row_keys: bool,
    pub dict_enc_sort_keys: bool,
    pub dict_enc_values: bool,
}

impl Default for FFISleeperOptions {
    fn default() -> Self {
        Self {
            use_readahead_store: false,
            read_page_indexes: false,
            max_row_group_size: MAX_ROW_GROUP_SIZE,
            max_page_size: MAX_PAGE_SIZE,
            compression: COMPRESSION_CODEC.as_ptr(),
            writer_version: WRITER_VERSION.as_ptr(),
            column_truncate_length: COLUMN_TRUNCATE_LENGTH,
            stats_truncate_length: STATS_TRUNCATE_LENGTH,
            dict_enc_row_keys: DICT_ENCODE_ROW_KEYS,
            dict_enc_sort_keys: DICT_ENCODE_SORT_KEYS,
            dict_enc_values: DICT_ENCODE_VALUES,
        }
    }
}
