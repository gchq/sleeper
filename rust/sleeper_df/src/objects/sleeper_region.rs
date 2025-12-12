//! Sleeper region FFI structs.
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
use color_eyre::eyre::bail;
use sleeper_core::{ColRange, SleeperRegion};
use std::{borrow::Borrow, collections::HashMap, ffi::c_void};

use crate::{
    objects::RowKeySchemaType,
    unpack::{unpack_typed_array, unpack_variant_array},
};

/// Represents a Sleeper region in a C ABI struct.
///
/// Java arrays are transferred with a length. They should all be the same length in this struct.
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/common/foreign-bridge/src/main/java/sleeper/foreign/FFISleeperRegion.java.
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

impl<'a> FFISleeperRegion {
    pub fn to_sleeper_region<T: Borrow<str>>(
        region: &'a FFISleeperRegion,
        row_key_cols: &[T],
        schema_types: &[RowKeySchemaType],
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
