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
use color_eyre::eyre::{bail, eyre};
use std::{collections::HashMap, ffi::c_char, fmt::Display, slice};

/// Type tag for an extension.
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/common/foreign-bridge/src/main/java/sleeper/foreign/datafusion/extension/FFIExtensionVariant.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
#[allow(clippy::upper_case_acronyms)]
pub enum FFIExtensionVariant {
    SQL = 1,
}

pub trait ExtensionFFIDetails {
    const MAX_CARDINALITY: usize;
}

impl FFIExtensionVariant {
    pub fn display_type(&self) -> &str {
        match self {
            FFIExtensionVariant::SQL => "SQL",
        }
    }

    /// The maximum number of instances of this extension type allowed in one request.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn max_cardinality(&self) -> usize {
        match self {
            Self::SQL => <FFISQLExtension as ExtensionFFIDetails>::MAX_CARDINALITY,
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

impl FFIExtension {
    /// Validates the inner member of the struct is not a NULL pointer.
    ///
    /// # Errors
    /// Fails if a NULL is found
    fn check_non_null_data(&self) -> Result<(), color_eyre::Report> {
        match self.variant {
            FFIExtensionVariant::SQL => {
                if unsafe { self.data.sql.is_null() } {
                    bail!("SQL variant of FFIExtension contains NULL union member pointer");
                }
            }
        }
        Ok(())
    }
}

/// Checks the extension array and ensures all extensions are permitted.
///
/// # Errors
/// Error will occur if any extension is type is contained more than its permitted maximum.
pub fn validate_extensions(
    extensions: *const FFIExtension,
    extensions_len: usize,
) -> Result<(), color_eyre::Report> {
    let Some(_) = (unsafe { extensions.as_ref() }) else {
        bail!("FFIExtension array is NULL");
    };

    // Check counts of different extensions
    let mut ext_map = HashMap::new();
    let ext_slice = unsafe { slice::from_raw_parts(extensions, extensions_len) };
    for ext in ext_slice {
        ext.check_non_null_data()?;
        let val = ext_map.entry(ext.variant as usize).or_insert(0);
        *val += 1;
        if *val > ext.variant.max_cardinality() {
            bail!(
                "extension array contains {} instances of type {}, the maximum is {}",
                *val,
                ext.variant,
                ext.variant.max_cardinality()
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_extension_with_data(variant: FFIExtensionVariant, data_ptr: *const FFISQLExtension) -> FFIExtension {
        FFIExtension {
            variant,
            data: FFIExtensionData { sql: data_ptr },
        }
    }

    fn create_extension(variant: FFIExtensionVariant) -> FFIExtension {
        // DANGER: This creates an FFIExtension with pointers to stack-allocated data.
        // THIS FUNCTION IS ONLY FOR NULL-CHECKING TESTS — NEVER for cardinality tests
        // or any test that would call check_non_null_data() or validate_extensions().
        // The pointer must NEVER be dereferenced. Use only to test:
        // - validate_extensions() with null pointer checks
        // - error message formatting
        // ANY DEREFERENCE WILL CAUSE USE-AFTER-FREE when the stack frame exits.
        match variant {
            FFIExtensionVariant::SQL => {
                let sql_ext = FFISQLExtension {
                    sql: c"SELECT * FROM test".as_ptr(),
                };
                create_extension_with_data(variant, std::ptr::addr_of!(sql_ext) as *const FFISQLExtension)
            }
        }
    }

    #[test]
    pub fn should_fail_on_null_pointer() {
        // Given
        let null_ptr: *const FFIExtension = std::ptr::null();

        // When
        let result = validate_extensions(null_ptr, 0);

        // Then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("FFIExtension array is NULL")
        );
    }

    #[test]
    pub fn should_succeed_with_empty_array() {
        // Given
        let extensions: Vec<FFIExtension> = vec![];

        // When
        let result = validate_extensions(extensions.as_ptr(), 0);

        // Then
        assert!(result.is_ok());
    }

    #[test]
    pub fn should_succeed_with_single_extension() {
        // Given
        let extensions = [create_extension(FFIExtensionVariant::SQL)];

        // When
        let result = validate_extensions(extensions.as_ptr(), 1);

        // Then
        assert!(result.is_ok());
    }

    #[test]
    pub fn should_fail_when_exceeding_cardinality() {
        // Given
        let extensions = [
            create_extension(FFIExtensionVariant::SQL),
            create_extension(FFIExtensionVariant::SQL),
        ];

        // When
        let result = validate_extensions(extensions.as_ptr(), 2);

        // Then
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("2 instances"));
        assert!(error.contains("maximum is 1"));
    }

    #[test]
    pub fn should_fail_on_null_data_pointer() {
        // Given
        let ext = create_extension_with_data(FFIExtensionVariant::SQL, std::ptr::null());

        // When
        let result = ext.check_non_null_data();

        // Then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("NULL union member pointer")
        );
    }

    #[test]
    pub fn should_succeed_with_valid_data_pointer() {
        // Given
        let ext = create_extension(FFIExtensionVariant::SQL);

        // When
        let result = ext.check_non_null_data();

        // Then
        assert!(result.is_ok());
    }
}
