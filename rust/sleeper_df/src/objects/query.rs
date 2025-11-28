//! Query related FFI structs.
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
use crate::{
    objects::{ffi_common_config::FFICommonConfig, sleeper_region::FFISleeperRegion},
    unpack::{unpack_string, unpack_string_array},
};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use color_eyre::eyre::bail;
use sleeper_core::LeafPartitionQueryConfig;
use std::{ffi::c_char, slice};

/// Contains all information needed for a Sleeper leaf partition query from a foreign function.
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/query/query-datafusion/src/main/java/sleeper/query/datafusion/FFILeafPartitionQueryConfig.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
pub struct FFILeafPartitionQueryConfig {
    /// Common configuration
    pub common: *const FFICommonConfig,
    /// Length of query region array
    pub query_regions_len: usize,
    /// Pointers to query regions,
    pub query_regions: *const FFISleeperRegion,
    /// Are there any requested value fields? This is different to there being zero.
    pub requested_value_fields_set: bool,
    /// Length of requested value fields
    pub requested_value_fields_len: usize,
    /// Requested value fields.
    pub requested_value_fields: *const *const c_char,
    /// Should logical and physical query plans be written to logging output?
    pub explain_plans: bool,
    /// An extra SQL string to run
    pub sql_string: *const c_char,
}

impl FFILeafPartitionQueryConfig {
    /// Convert to a Rust native struct.
    ///
    /// All pointers must be valid. Pointers are NULL checked, but we can't vouch for validity.
    ///
    /// # Errors
    /// Errors if: any pointer is NULL, any array lengths invalid, region invalid, etc.
    pub fn to_leaf_config<'a>(
        &self,
        file_output_enabled: bool,
    ) -> Result<LeafPartitionQueryConfig<'a>, color_eyre::Report> {
        let Some(ffi_common) = (unsafe { self.common.as_ref() }) else {
            bail!("FFILeafPartitionQueryConfig common is NULL");
        };
        let common = ffi_common.to_common_config(file_output_enabled)?;
        let row_key_cols = ffi_common.row_key_cols()?;
        let schema_types = ffi_common.schema_types()?;

        let Some(_) = (unsafe { self.query_regions.as_ref() }) else {
            bail!("FFILeafPartitionQueryConfig query_regions is NULL");
        };

        let ranges = unsafe { slice::from_raw_parts(self.query_regions, self.query_regions_len) }
            .iter()
            .map(|ffi_reg| {
                FFISleeperRegion::to_sleeper_region(ffi_reg, &row_key_cols, &schema_types)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let requested_value_columns = if self.requested_value_fields_set {
            Some(
                unpack_string_array(self.requested_value_fields, self.requested_value_fields_len)?
                    .into_iter()
                    .map(String::from)
                    .collect(),
            )
        } else {
            None
        };

        let sql_query = if !self.sql_string.is_null() {
            Some(unpack_string(self.sql_string)?)
        } else {
            None
        };

        Ok(LeafPartitionQueryConfig {
            common,
            ranges,
            requested_value_fields: requested_value_columns,
            explain_plans: self.explain_plans,
            sql_query,
        })
    }
}

/// This is a simple struct that contains a single pointer to the [`FFI_ArrowArrayStream`].
///
/// The consumer should create one of these objects and then pass it to
/// a query function which will populate the pointer.
///
/// As the contents of this struct are read and written by external code,
/// we need it to be FFI compatible, so we apply the `#[repr(C)]` attribute
/// to ensure Rust uses C compatible ordering, alignment and padding.
///
/// # Safety
/// The Rust side of this function, should NOT read incoming value of
/// [`arrow_array_stream`](FFIQueryResults::arrow_array_stream) as it is undefined.
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/query/query-datafusion/src/main/java/sleeper/query/datafusion/FFIQueryResults.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
#[derive(Debug)]
pub struct FFIQueryResults {
    /// Pointer to an Arrow array stream for use by consumer.
    pub arrow_array_stream: *const FFI_ArrowArrayStream,
}

impl Default for FFIQueryResults {
    fn default() -> Self {
        Self {
            arrow_array_stream: std::ptr::null(),
        }
    }
}
