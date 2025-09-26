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
    unpack_aws_config, unpack_str, unpack_string, unpack_string_array, unpack_typed_array,
    unpack_typed_ref_array, unpack_variant_array,
};
use color_eyre::eyre::{bail, eyre};
use sleeper_core::{
    ColRange, CommonConfig, CommonConfigBuilder, LeafPartitionQueryConfig, OutputType,
    SleeperParquetOptions, SleeperRegion,
    filter_aggregation_config::{aggregate::Aggregate, filter::Filter},
};
use std::{
    borrow::Borrow,
    collections::HashMap,
    ffi::{c_char, c_void},
};
use url::Url;

/// Contains all output data from a file output operation.
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/foreign-bridge/src/main/java/sleeper/foreign/FFIFileResult.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
pub struct FFIFileResult {
    /// The total number of rows read by a compaction.
    pub rows_read: usize,
    /// The total number of rows written by a compaction.
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
}

/// Column type for row key columns in Sleeper schema.
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
        {
            bail!("All array lengths in a SleeperRegion must be same length");
        }
        let region_mins_inclusive =
            unpack_typed_array(region.mins_inclusive, region.mins_inclusive_len)?;
        let region_maxs_inclusive =
            unpack_typed_array(region.maxs_inclusive, region.maxs_inclusive_len)?;

        let region_mins = unpack_variant_array(region.mins, region.mins_len, schema_types, false)?;

        let region_maxs = unpack_variant_array(region.maxs, region.maxs_len, schema_types, true)?;

        let mut map = HashMap::with_capacity(row_key_cols.len());
        for (idx, row_key) in row_key_cols.iter().enumerate() {
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

/// Contains all the common input data for setting up a Sleeper `DataFusion` operation.
///
/// See `java/compaction/compaction-datafusion/src/main/java/sleeper/compaction/datafusion/DataFusionFunctions.java`
/// for details. Field ordering and types MUST match between the two definitions!
#[repr(C)]
pub struct FFICommonConfig {
    pub override_aws_config: bool,
    pub aws_region: *const c_char,
    pub aws_endpoint: *const c_char,
    pub aws_access_key: *const c_char,
    pub aws_secret_key: *const c_char,
    pub aws_allow_http: bool,
    pub input_files_len: usize,
    pub input_files: *const *const c_char,
    pub input_files_sorted: bool,
    pub output_file: *const c_char,
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
    /// The schema types for the row key columns in this Sleeper schema.
    ///
    /// # Errors
    /// If an invalid row key type is found, e.g. type ordinal number is outside range. See [`FFIRowKeySchemaType`].
    pub fn schema_types(&self) -> Result<Vec<FFIRowKeySchemaType>, color_eyre::Report> {
        unpack_typed_array(self.row_key_schema, self.row_key_schema_len)?
            .iter()
            .map(FFIRowKeySchemaType::try_from)
            .collect::<Result<Vec<_>, _>>()
    }

    /// Get row key column names.
    pub fn row_key_cols(&self) -> Result<Vec<String>, color_eyre::Report> {
        Ok(
            unpack_string_array(self.row_key_cols, self.row_key_cols_len)?
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>(),
        )
    }
}

impl<'a> TryFrom<&'a FFICommonConfig> for CommonConfig<'a> {
    type Error = color_eyre::Report;

    fn try_from(params: &'a FFICommonConfig) -> Result<CommonConfig<'a>, Self::Error> {
        if params.aggregation_config.is_null() {
            bail!("FFICommonConfig aggregation_config is NULL");
        }
        if params.filtering_config.is_null() {
            bail!("FFICommonConfig filtering_config is NULL");
        }
        if params.output_file.is_null() {
            bail!("FFICommonConfig output_file is NULL");
        }
        if params.compression.is_null() {
            bail!("FFICommonConfig compression is NULL");
        }
        if params.writer_version.is_null() {
            bail!("FFICommonConfig writer_version is NULL");
        }
        if params.region.is_null() {
            bail!("FFICommonConfig region is NULL");
        }
        // We do this separately since we need the values for computing the region
        let row_key_cols = params.row_key_cols()?;
        // Contains numeric types to indicate schema types
        let schema_types = params.schema_types()?;

        let ffi_region = unsafe { params.region.as_ref() }.unwrap();
        let region = FFISleeperRegion::to_sleeper_region(ffi_region, &row_key_cols, &schema_types)?;

        let opts = SleeperParquetOptions {
            max_row_group_size: params.max_row_group_size,
            max_page_size: params.max_page_size,
            compression: unpack_string(params.compression)?,
            writer_version: unpack_string(params.writer_version)?,
            column_truncate_length: params.column_truncate_length,
            stats_truncate_length: params.stats_truncate_length,
            dict_enc_row_keys: params.dict_enc_row_keys,
            dict_enc_sort_keys: params.dict_enc_sort_keys,
            dict_enc_values: params.dict_enc_values,
        };

        CommonConfigBuilder::new()
            .aws_config(unpack_aws_config(params)?)
            .input_files(
                unpack_string_array(params.input_files, params.input_files_len)?
                    .into_iter()
                    .map(Url::parse)
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .input_files_sorted(true)
            .row_key_cols(row_key_cols)
            .sort_key_cols(
                unpack_string_array(params.sort_key_cols, params.sort_key_cols_len)?
                    .into_iter()
                    .map(String::from)
                    .collect(),
            )
            .region(region)
            .output(OutputType::File {
                output_file: unpack_str(params.output_file).map(Url::parse)??,
                opts,
            })
            .aggregates(Aggregate::parse_config(unpack_str(
                params.aggregation_config,
            )?)?)
            .filters(Filter::parse_config(unpack_str(params.filtering_config)?)?)
            .build()
    }
}

/// Contains all information needed for a Sleeper leaf partition query from a foreign function.
#[repr(C)]
pub struct FFILeafPartitionQueryConfig {
    /// Common configuration
    pub common: *const FFICommonConfig,
    /// Length of query region array
    pub query_regions_len: usize,
    /// Pointers to query regions,
    pub query_regions: *const *const FFISleeperRegion,
    /// Should quantile data sketches be written out?
    pub write_quantile_sketch: bool,
    /// Should logical and physical query plans be written to log?
    pub explain_plans: bool,
}

impl<'a> TryFrom<&'a FFILeafPartitionQueryConfig> for LeafPartitionQueryConfig<'a> {
    type Error = color_eyre::Report;

    fn try_from(config: &'a FFILeafPartitionQueryConfig) -> Result<Self, Self::Error> {
        let Some(ffi_common) = (unsafe { config.common.as_ref() }) else {
            bail!("FFILeafPartitionQueryConfig common is NULL");
        };
        let common = CommonConfig::try_from(ffi_common)?;
        let row_key_cols = ffi_common.row_key_cols()?;
        let schema_types = ffi_common.schema_types()?;

        let ranges = unpack_typed_ref_array(config.query_regions, config.query_regions_len)?
            .iter()
            .map(|ffi_reg| {
                let Some(ffi_reg) = (unsafe { ffi_reg.as_ref() }) else {
                    bail!("NULL pointer found in query ranges")
                };
                FFISleeperRegion::to_sleeper_region(ffi_reg, &row_key_cols, &schema_types)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            common,
            ranges,
            explain_plans: config.explain_plans,
            write_quantile_sketch: config.write_quantile_sketch,
        })
    }
}
