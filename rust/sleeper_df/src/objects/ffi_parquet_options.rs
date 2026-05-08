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
use color_eyre::eyre::bail;
use std::ffi::{CStr, c_char};

pub const READ_PAGE_INDEXES: bool = true;
pub const MAX_ROW_GROUP_SIZE: usize = 100_000;
pub const MAX_PAGE_SIZE: usize = 128 * 1024;
pub const COMPRESSION_CODEC: &CStr = c"zstd";
pub const WRITER_VERSION: &CStr = c"v2";
pub const COLUMN_TRUNCATE_LENGTH: usize = 128;
pub const STATS_TRUNCATE_LENGTH: usize = 2_147_483_647;
pub const DICT_ENCODE_ROW_KEYS: bool = false;
pub const DICT_ENCODE_SORT_KEYS: bool = false;
pub const DICT_ENCODE_VALUES: bool = false;

/// Contains all the Parquet options for Sleeper `DataFusion` operation. These may not be needed for every `DataFusion` usage,
/// so come with reasonable defaults. See Java side documentation for explanation of fields.
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/common/foreign-bridge/src/main/java/sleeper/foreign/datafusion/FFIParquetOptions.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
#[derive(Debug)]
pub struct FFIParquetOptions {
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

impl Default for FFIParquetOptions {
    fn default() -> Self {
        Self {
            read_page_indexes: READ_PAGE_INDEXES,
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

impl FFIParquetOptions {
    /// Checks string pointers are not null.
    pub fn check_for_nulls(&self) -> color_eyre::Result<()> {
        if self.compression.is_null() {
            bail!("FFISleeperOptions compression is NULL");
        }
        if self.writer_version.is_null() {
            bail!("FFISleeperOptions writer_version is NULL");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::objects::ffi_parquet_options::FFIParquetOptions;
    use std::ptr::null;

    #[test]
    pub fn should_fail_on_null_compression() {
        // Given
        let options = FFIParquetOptions {
            compression: null(),
            ..FFIParquetOptions::default()
        };

        // When
        let result = options.check_for_nulls();

        // Then
        assert!(result.is_err());
    }

    #[test]
    pub fn should_fail_on_writer_version() {
        // Given
        let options = FFIParquetOptions {
            writer_version: null(),
            ..FFIParquetOptions::default()
        };

        // When
        let result = options.check_for_nulls();

        // Then
        assert!(result.is_err());
    }
}
