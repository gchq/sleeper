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
use crate::unpack::{
    unpack_str, unpack_string, unpack_string_array, unpack_typed_array, unpack_variant_array,
};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use color_eyre::eyre::{bail, eyre};
use sleeper_core::{
    AwsConfig, ColRange, CommonConfig, CommonConfigBuilder, LeafPartitionQueryConfig, OutputType,
    SleeperParquetOptions, SleeperRegion,
    filter_aggregation_config::{aggregate::Aggregate, filter::Filter},
};
use std::{
    borrow::Borrow,
    collections::HashMap,
    ffi::{c_char, c_void},
    slice,
};
use url::Url;

/// Contains all output data from a file output operation.
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/foreign-bridge/src/main/java/sleeper/foreign/FFIFileResult.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
pub struct FFIFileResult {
    /// The total number of rows read by a query/compaction.
    pub rows_read: usize,
    /// The total number of rows written by a query/compaction.
    pub rows_written: usize,
}

/// Represents a Sleeper region in a C ABI struct.
///
/// Java arrays are transferred with a length. They should all be the same length in this struct.
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/foreign-bridge/src/main/java/sleeper/foreign/FFISleeperRegion.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
#[derive(Copy, Clone)]
pub struct FFISleeperRegion {
    pub mins_len: usize,
    // The mins array may NOT contain null pointers
    pub mins: *const *const c_void,
    pub maxs_len: usize,
    // The maxs array may contain null pointers!!
    pub maxs: *const *const c_void,
    pub mins_inclusive_len: usize,
    pub mins_inclusive: *const *const bool,
    pub maxs_inclusive_len: usize,
    pub maxs_inclusive: *const *const bool,
    pub dimension_indexes_len: usize,
    pub dimension_indexes: *const *const i32,
}

/// Data type for row key fields in Sleeper schema.
/// Encoded as integer type for FFI compatibility.
#[derive(Copy, Clone)]
pub enum FFIRowKeySchemaType {
    Int32 = 1,
    Int64 = 2,
    String = 3,
    ByteArray = 4,
}

impl TryFrom<&usize> for FFIRowKeySchemaType {
    type Error = color_eyre::Report;

    fn try_from(ordinal: &usize) -> Result<Self, Self::Error> {
        match ordinal {
            1 => Ok(FFIRowKeySchemaType::Int32),
            2 => Ok(FFIRowKeySchemaType::Int64),
            3 => Ok(FFIRowKeySchemaType::String),
            4 => Ok(FFIRowKeySchemaType::ByteArray),
            _ => Err(eyre!("Invalid FFIRowKeySchemaType ordinal value")),
        }
    }
}

impl<'a> FFISleeperRegion {
    fn to_sleeper_region<T: Borrow<str>>(
        region: &'a FFISleeperRegion,
        row_key_cols: &[T],
        schema_types: &[FFIRowKeySchemaType],
    ) -> Result<SleeperRegion<'a>, color_eyre::Report> {
        if region.mins_len != region.maxs_len
            || region.mins_len != region.mins_inclusive_len
            || region.mins_len != region.maxs_inclusive_len
            || region.mins_len != region.dimension_indexes_len
        {
            bail!("All array lengths in a SleeperRegion must be same length");
        }
        let region_mins_inclusive =
            unpack_typed_array(region.mins_inclusive, region.mins_inclusive_len)?;
        let region_maxs_inclusive =
            unpack_typed_array(region.maxs_inclusive, region.maxs_inclusive_len)?;

        let region_mins = unpack_variant_array(region.mins, region.mins_len, schema_types, false)?;

        let region_maxs = unpack_variant_array(region.maxs, region.maxs_len, schema_types, true)?;

        let dimension_indexes =
            unpack_typed_array(region.dimension_indexes, region.dimension_indexes_len)?;

        let mut map = HashMap::with_capacity(row_key_cols.len());

        for dimension in dimension_indexes {
            let idx = usize::try_from(dimension)?;
            let row_key = &row_key_cols[idx];
            map.insert(
                String::from(row_key.borrow()),
                ColRange {
                    lower: region_mins[idx],
                    lower_inclusive: region_mins_inclusive[idx],
                    upper: region_maxs[idx],
                    upper_inclusive: region_maxs_inclusive[idx],
                },
            );
        }
        Ok(SleeperRegion::new(map))
    }
}

/// Contains the FFI compatible configuration data for AWS.
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/foreign-bridge/src/main/java/sleeper/foreign/FFIAwsConfig.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
pub struct FFIAwsConfig {
    pub region: *const c_char,
    pub endpoint: *const c_char,
    pub access_key: *const c_char,
    pub secret_key: *const c_char,
    pub allow_http: bool,
}

/// Contains all the common input data for setting up a Sleeper `DataFusion` operation.
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/foreign-bridge/src/main/java/sleeper/foreign/datafusion/FFICommonConfig.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
pub struct FFICommonConfig {
    pub override_aws_config: bool,
    pub aws_config: *const FFIAwsConfig,
    pub input_files_len: usize,
    pub input_files: *const *const c_char,
    pub input_files_sorted: bool,
    pub output_file: *const c_char,
    pub write_sketch_file: bool,
    pub row_key_cols_len: usize,
    pub row_key_cols: *const *const c_char,
    pub row_key_schema_len: usize,
    pub row_key_schema: *const *const usize,
    pub sort_key_cols_len: usize,
    pub sort_key_cols: *const *const c_char,
    pub max_row_group_size: usize,
    pub max_page_size: usize,
    pub compression: *const c_char,
    pub writer_version: *const c_char,
    pub column_truncate_length: usize,
    pub stats_truncate_length: usize,
    pub dict_enc_row_keys: bool,
    pub dict_enc_sort_keys: bool,
    pub dict_enc_values: bool,
    pub region: *const FFISleeperRegion,
    pub aggregation_config: *const c_char,
    pub filtering_config: *const c_char,
}

impl FFICommonConfig {
    /// The schema types for the row key fields in this Sleeper schema.
    ///
    /// # Errors
    /// If an invalid row key type is found, e.g. type ordinal number is outside range. See [`FFIRowKeySchemaType`].
    pub fn schema_types(&self) -> Result<Vec<FFIRowKeySchemaType>, color_eyre::Report> {
        unpack_typed_array(self.row_key_schema, self.row_key_schema_len)?
            .iter()
            .map(FFIRowKeySchemaType::try_from)
            .collect::<Result<Vec<_>, _>>()
    }

    /// Get row key field names.
    pub fn row_key_cols(&self) -> Result<Vec<String>, color_eyre::Report> {
        Ok(
            unpack_string_array(self.row_key_cols, self.row_key_cols_len)?
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>(),
        )
    }

    /// Convert to a Rust native struct.
    ///
    /// All pointers must be valid. Pointers are NULL checked, but we can't vouch for validity.
    ///
    /// # Errors
    /// Errors if: any pointer is NULL, any array lengths invalid, region invalid, etc.
    pub fn to_common_config<'a>(
        &self,
        file_output_enabled: bool,
    ) -> Result<CommonConfig<'a>, color_eyre::Report> {
        if file_output_enabled && self.output_file.is_null() {
            bail!("FFICommonConfig output_file is NULL, file output selected");
        }
        if self.aggregation_config.is_null() {
            bail!("FFICommonConfig aggregation_config is NULL");
        }
        if self.filtering_config.is_null() {
            bail!("FFICommonConfig filtering_config is NULL");
        }
        if self.compression.is_null() {
            bail!("FFICommonConfig compression is NULL");
        }
        if self.writer_version.is_null() {
            bail!("FFICommonConfig writer_version is NULL");
        }
        if self.region.is_null() {
            bail!("FFICommonConfig region is NULL");
        }
        // We do this separately since we need the values for computing the region
        let row_key_cols = self.row_key_cols()?;
        // Contains numeric types to indicate schema types
        let schema_types = self.schema_types()?;

        let ffi_region = unsafe { self.region.as_ref() }.unwrap();
        let region = FFISleeperRegion::to_sleeper_region(ffi_region, &row_key_cols, &schema_types)?;

        let output = if file_output_enabled {
            let opts = SleeperParquetOptions {
                max_row_group_size: self.max_row_group_size,
                max_page_size: self.max_page_size,
                compression: unpack_string(self.compression)?,
                writer_version: unpack_string(self.writer_version)?,
                column_truncate_length: self.column_truncate_length,
                stats_truncate_length: self.stats_truncate_length,
                dict_enc_row_keys: self.dict_enc_row_keys,
                dict_enc_sort_keys: self.dict_enc_sort_keys,
                dict_enc_values: self.dict_enc_values,
            };
            OutputType::File {
                output_file: unpack_str(self.output_file).map(Url::parse)??,
                write_sketch_file: self.write_sketch_file,
                opts,
            }
        } else {
            OutputType::ArrowRecordBatch
        };

        CommonConfigBuilder::new()
            .aws_config(unpack_aws_config(self)?)
            .input_files(
                unpack_string_array(self.input_files, self.input_files_len)?
                    .into_iter()
                    .map(Url::parse)
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .input_files_sorted(self.input_files_sorted)
            .row_key_cols(row_key_cols)
            .sort_key_cols(
                unpack_string_array(self.sort_key_cols, self.sort_key_cols_len)?
                    .into_iter()
                    .map(String::from)
                    .collect(),
            )
            .region(region)
            .output(output)
            .aggregates(Aggregate::parse_config(unpack_str(
                self.aggregation_config,
            )?)?)
            .filters(Filter::parse_config(unpack_str(self.filtering_config)?)?)
            .build()
    }
}

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

        Ok(LeafPartitionQueryConfig {
            common,
            ranges,
            requested_value_fields: requested_value_columns,
            explain_plans: self.explain_plans,
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

fn unpack_aws_config(params: &FFICommonConfig) -> Result<Option<AwsConfig>, color_eyre::Report> {
    Ok(if params.override_aws_config {
        let Some(ffi_aws) = (unsafe { params.aws_config.as_ref() }) else {
            bail!("override_aws_config is true, but aws_config pointer is NULL");
        };
        AwsConfig::try_from(ffi_aws).ok()
    } else {
        None
    })
}

impl TryFrom<&FFIAwsConfig> for AwsConfig {
    type Error = color_eyre::Report;

    fn try_from(value: &FFIAwsConfig) -> Result<Self, Self::Error> {
        if value.region.is_null() {
            bail!("FFIAwsConfig region pointer is NULL");
        }
        if value.endpoint.is_null() {
            bail!("FFIAwsConfig endpoint pointer is NULL");
        }
        if value.access_key.is_null() {
            bail!("FFIAwsConfig access_key pointer is NULL");
        }
        if value.secret_key.is_null() {
            bail!("FFIAwsConfig secret_key pointer is NULL");
        }
        Ok(AwsConfig {
            region: unpack_string(value.region)?,
            endpoint: unpack_string(value.endpoint)?,
            access_key: unpack_string(value.access_key)?,
            secret_key: unpack_string(value.secret_key)?,
            allow_http: value.allow_http,
        })
    }
}
