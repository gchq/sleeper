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
use crate::{
    objects::extensions::{
        ExtensionFFIDetails, FFIExtension, FFIExtensionVariant, find_extensions_type,
    },
    unpack::unpack_string,
};
use color_eyre::eyre::bail;
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

    fn validate(&self) -> Result<(), color_eyre::Report> {
        if self.sql.is_null() {
            bail!("SQL query string is NULL");
        } else {
            // Is string valid?
            let _ = unpack_string(self.sql)?;
        }
        Ok(())
    }
}

/// Retrieve the SQL extension from an extension array.
///
/// # Assumptions
///
/// Extension array has been validated with [`validate_extensions`].
/// Contents of array and SQL extension are therefore valid.
pub fn find_ffi_sql_extension<'a>(
    extensions: *const FFIExtension,
    extensions_len: usize,
) -> Option<&'a FFISQLExtension> {
    let sql_extension = find_extensions_type(FFIExtensionVariant::SQL, extensions, extensions_len);
    // should have max length 1
    if sql_extension.is_empty() {
        None
    } else {
        unsafe { sql_extension[0].data.sql.as_ref() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::c_char;

    fn create_sql_extension(sql: *const c_char) -> FFIExtension {
        let sql_ext = Box::leak(Box::new(FFISQLExtension { sql }));
        FFIExtension {
            variant: FFIExtensionVariant::SQL,
            data: crate::objects::extensions::FFIExtensionData {
                sql: sql_ext as *const FFISQLExtension,
            },
        }
    }

    #[test]
    pub fn find_ffi_sql_extension_returns_none_for_empty_array() {
        // Given - empty extension array
        let extensions: Vec<FFIExtension> = vec![];

        // When
        let result = find_ffi_sql_extension(extensions.as_ptr(), extensions.len());

        // Then
        assert!(result.is_none());
    }

    #[test]
    pub fn find_ffi_sql_extension_returns_sql_extension() {
        // Given - array with one SQL extension
        let extensions = [create_sql_extension(c"SELECT * FROM test".as_ptr())];

        // When
        let result = find_ffi_sql_extension(extensions.as_ptr(), extensions.len());

        // Then
        assert!(result.is_some());
        let sql_ext = result.unwrap();
        assert!(!sql_ext.sql.is_null());
    }

    #[test]
    pub fn find_ffi_sql_extension_returns_first_sql_extension() {
        // Given - array with two SQL extensions
        let ext1 = create_sql_extension(c"SELECT 1".as_ptr());
        let ext2 = create_sql_extension(c"SELECT 2".as_ptr());
        let extensions = [ext1, ext2];

        // When
        let result = find_ffi_sql_extension(extensions.as_ptr(), extensions.len());

        // Then - should return the first one
        assert!(result.is_some());
        let sql_ext = result.unwrap();
        assert_eq!(sql_ext as *const _, unsafe { extensions[0].data.sql });
    }
}

