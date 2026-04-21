//! Sleeper region FFI structs.
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
use crate::objects::FFIElement;
use color_eyre::eyre::bail;
use sleeper_core::{ColRange, PartitionBound, SleeperRegion};
use std::{borrow::Borrow, collections::HashMap, slice};

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
    pub len: usize,
    // The mins array may NOT contain null pointers
    pub mins: *const FFIElement,
    // The maxs array may contain null pointers!!
    pub maxs: *const FFIElement,
    pub mins_inclusive: *const bool,
    pub maxs_inclusive: *const bool,
    pub dimension_indexes: *const i32,
}

impl<'a> FFISleeperRegion {
    pub fn to_sleeper_region<T: Borrow<str>>(
        region: &'a FFISleeperRegion,
        row_key_cols: &[T],
    ) -> Result<SleeperRegion<'a>, color_eyre::Report> {
        if region.len < 1 {
            bail!("FFISleeperRegion len cannot be 0");
        }
        if region.mins.is_null() {
            bail!("FFISleeperRegion mins cannot be NULL");
        }
        if region.maxs.is_null() {
            bail!("FFISleeperRegion maxs cannot be NULL");
        }
        if region.mins_inclusive.is_null() {
            bail!("FFISleeperRegion mins_inclusive cannot be NULL");
        }
        if region.maxs_inclusive.is_null() {
            bail!("FFISleeperRegion maxs_inclusive cannot be NULL");
        }
        if region.dimension_indexes.is_null() {
            bail!("FFISleeperRegion dimension_indexes cannot be NULL");
        }

        let region_mins_inclusive =
            unsafe { slice::from_raw_parts(region.mins_inclusive, region.len) };
        let region_maxs_inclusive =
            unsafe { slice::from_raw_parts(region.maxs_inclusive, region.len) };

        let region_mins = unsafe { slice::from_raw_parts(region.mins, region.len) }
            .iter()
            .map(PartitionBound::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        // Sleeper region minimums cannot contain unbounded values
        if region_mins
            .iter()
            .any(|e| matches!(e, PartitionBound::Unbounded))
        {
            bail!("FFISleeperRegion mins array contained unbounded element");
        }
        // but maximums can
        let region_maxs = unsafe { slice::from_raw_parts(region.maxs, region.len) }
            .iter()
            .map(PartitionBound::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        let dimension_indexes =
            unsafe { slice::from_raw_parts(region.dimension_indexes, region.len) };

        let mut map = HashMap::with_capacity(row_key_cols.len());

        for dimension in dimension_indexes {
            let idx = usize::try_from(*dimension)?;
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
