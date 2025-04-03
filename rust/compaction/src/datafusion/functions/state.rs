/// A specialised version of [`NullState`] for map column types.
/// Adapted from <`https://docs.rs/datafusion/latest/datafusion/physical_expr/struct.NullState.html`>
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
use arrow::{
    array::{Array, BooleanArray, BooleanBufferBuilder, MapArray, StructArray},
    buffer::{BooleanBuffer, NullBuffer},
};
use datafusion::logical_expr::EmitTo;

#[derive(Debug)]
pub struct MapNullState {
    /// Have we seen any non-filtered input values for `group_index`?
    ///
    /// If `seen_values[i]` is true, have seen at least one non null
    /// value for group `i`
    ///
    /// If `seen_values[i]` is false, have not seen any values that
    /// pass the filter yet for group `i`
    seen_values: BooleanBufferBuilder,
}

impl Default for MapNullState {
    fn default() -> Self {
        Self::new()
    }
}

impl MapNullState {
    pub fn new() -> Self {
        Self {
            seen_values: BooleanBufferBuilder::new(0),
        }
    }

    pub fn new_with_capacity(capacity: usize) -> Self {
        Self {
            seen_values: BooleanBufferBuilder::new(capacity),
        }
    }

    /// return the size of all buffers allocated by this null state, not including self
    pub fn size(&self) -> usize {
        // capacity is in bits, so convert to bytes
        self.seen_values.capacity() / 8
    }

    /// Invokes `value_fn(group_index, value)` for each non null, non
    /// filtered value of `value`, while tracking which groups have
    /// seen null inputs and which groups have seen any inputs if necessary
    //
    /// # Arguments:
    ///
    /// * `values`: the input arguments to the accumulator
    /// * `group_indices`:  To which groups do the rows in `values` belong, (aka `group_index`)
    /// * `opt_filter`: if present, only rows for which is Some(true) are included
    /// * `value_fn`: function invoked for  (`group_index`, value) where value is non null
    ///
    /// See [`accumulate`], for more details on how `value_fn` is called
    ///
    /// When `value_fn` is called it also sets
    ///
    /// 1. `self.seen_values[group_index]` to true for all rows that had a non null value
    pub fn accumulate<F>(
        &mut self,
        group_indices: &[usize],
        values: &MapArray,
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
        mut value_fn: F,
    ) where
        F: FnMut(usize, StructArray) + Send,
    {
        // ensure the seen_values is big enough (start everything at
        // "not seen" valid)
        let seen_values = initialize_builder(&mut self.seen_values, total_num_groups, false);
        accumulate(group_indices, values, opt_filter, |group_index, value| {
            seen_values.set_bit(group_index, true);
            value_fn(group_index, value);
        });
    }

    /// Creates the a [`NullBuffer`] representing which `group_indices`
    /// should have null values (because they never saw any values)
    /// for the `emit_to` rows.
    ///
    /// resets the internal state appropriately
    pub fn build(&mut self, emit_to: EmitTo) -> NullBuffer {
        let nulls: BooleanBuffer = self.seen_values.finish();

        let nulls = match emit_to {
            EmitTo::All => nulls,
            EmitTo::First(n) => {
                // split off the first N values in seen_values
                //
                // TODO make this more efficient rather than two
                // copies and bitwise manipulation
                let first_n_null: BooleanBuffer = nulls.iter().take(n).collect();
                // reset the existing seen buffer
                for seen in nulls.iter().skip(n) {
                    self.seen_values.append(seen);
                }
                first_n_null
            }
        };
        NullBuffer::new(nulls)
    }
}

/// Invokes `value_fn(group_index, value)` for each non null, non
/// filtered value of `value`,
///
/// # Arguments:
///
/// * `group_indices`:  To which groups do the rows in `values` belong, (aka `group_index`)
/// * `values`: the input arguments to the accumulator
/// * `opt_filter`: if present, only rows for which is Some(true) are included
/// * `value_fn`: function invoked for  (`group_index`, value) where value is non null
///
/// # Example
///
/// ```text
///  ┌─────────┐   ┌─────────┐   ┌ ─ ─ ─ ─ ┐
///  │ ┌─────┐ │   │ ┌─────┐ │     ┌─────┐
///  │ │  2  │ │   │ │ 200 │ │   │ │  t  │ │
///  │ ├─────┤ │   │ ├─────┤ │     ├─────┤
///  │ │  2  │ │   │ │ 100 │ │   │ │  f  │ │
///  │ ├─────┤ │   │ ├─────┤ │     ├─────┤
///  │ │  0  │ │   │ │ 200 │ │   │ │  t  │ │
///  │ ├─────┤ │   │ ├─────┤ │     ├─────┤
///  │ │  1  │ │   │ │ 200 │ │   │ │NULL │ │
///  │ ├─────┤ │   │ ├─────┤ │     ├─────┤
///  │ │  0  │ │   │ │ 300 │ │   │ │  t  │ │
///  │ └─────┘ │   │ └─────┘ │     └─────┘
///  └─────────┘   └─────────┘   └ ─ ─ ─ ─ ┘
///
/// group_indices   values        opt_filter
/// ```
///
/// In the example above, `value_fn` is invoked for each (`group_index`,
/// value) pair where `opt_filter[i]` is true and values is non null
///
/// ```text
/// value_fn(2, 200)
/// value_fn(0, 200)
/// value_fn(0, 300)
/// ```
pub fn accumulate<F>(
    group_indices: &[usize],
    values: &MapArray,
    opt_filter: Option<&BooleanArray>,
    mut value_fn: F,
) where
    F: FnMut(usize, StructArray) + Send,
{
    let data = values;
    assert_eq!(data.len(), group_indices.len());

    match (values.null_count() > 0, opt_filter) {
        // no nulls, no filter,
        (false, None) => {
            let iter = group_indices.iter().zip(data.iter());
            for (&group_index, new_value) in iter {
                value_fn(
                    group_index,
                    new_value.expect("Nulls shouldn't be present in non-nullable column"),
                );
            }
        }
        // nulls, no filter
        (true, None) => {
            let nulls = values.nulls().unwrap();
            group_indices
                .iter()
                .zip(data.iter())
                .zip(nulls.iter())
                .for_each(|((group_index, item), is_valid)| {
                    if is_valid {
                        value_fn(*group_index, item.expect("Null in unexpected index"));
                    }
                });
        }
        // no nulls, but a filter
        (false, Some(filter)) => {
            assert_eq!(filter.len(), group_indices.len());
            // The performance with a filter could be improved by
            // iterating over the filter in chunks, rather than a single
            // iterator. TODO file a ticket
            group_indices
                .iter()
                .zip(data.iter())
                .zip(filter.iter())
                .for_each(|((&group_index, new_value), filter_value)| {
                    if let Some(true) = filter_value {
                        value_fn(
                            group_index,
                            new_value.expect("Nulls shouldn't be present in non-nullable column"),
                        );
                    }
                });
        }
        // both null values and filters
        (true, Some(filter)) => {
            assert_eq!(filter.len(), group_indices.len());
            // The performance with a filter could be improved by
            // iterating over the filter in chunks, rather than using
            // iterators. TODO file a ticket
            filter
                .iter()
                .zip(group_indices.iter())
                .zip(values.iter())
                .for_each(|((filter_value, &group_index), new_value)| {
                    if let Some(true) = filter_value {
                        if let Some(new_value) = new_value {
                            value_fn(group_index, new_value);
                        }
                    }
                });
        }
    }
}

/// Ensures that `builder` contains a `BooleanBufferBuilder` with at
/// least `total_num_groups`.
///
/// All new entries are initialized to `default_value`
fn initialize_builder(
    builder: &mut BooleanBufferBuilder,
    total_num_groups: usize,
    default_value: bool,
) -> &mut BooleanBufferBuilder {
    if builder.len() < total_num_groups {
        let new_groups = total_num_groups - builder.len();
        builder.append_n(new_groups, default_value);
    }
    builder
}
