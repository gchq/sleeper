/// [`GroupAccumulator`] implementations for byte group map aggregation.
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
use crate::datafusion::aggregate_udf::{
    MapAggregatorOp,
    map_aggregate::{aggregator::PrimBuilderType, state::MapNullState},
};
use arrow::{
    array::{
        ArrayBuilder, ArrayRef, ArrowPrimitiveType, AsArray, BinaryBuilder, BooleanArray,
        MapBuilder, MapFieldNames, StructArray,
    },
    datatypes::{DataType, Field},
};
use datafusion::{
    common::{HashMap, exec_err, plan_err},
    error::Result,
    logical_expr::{EmitTo, GroupsAccumulator},
};
use nohash::BuildNoHashHasher;
use num_traits::NumAssign;
use std::{hash::BuildHasher, sync::Arc};

use super::{INITIAL_GROUP_MAP_SIZE, INITIAL_KEY_CACHE_SIZE};

/* Implementation note:
* This group accumulator implementation holds the maps for many aggregation groups at once. Since the same map keys
* may be seen duplicated across many groups, we want to avoid that storage cost, e.g. if we are storing 10,000 aggregation
* groups, each with the key "this long map key is being aggregated", we don't want to store that binary 10,000 times.
* Therefore, we use a cache to store each binary key seen in "key_cache". The cache maps to an index number. This is the
* index into the "keys" vector which contains the owned key binary.
*
* When new values are placed into the group accumulator, lookups into the group_maps are exceedingly quick since we use
* a [`NoHashHasher`] with a `usize` key, essentially making the hashing operation a no-op.
*
* When [`evaluate`] is called, the original key can be found using the group_maps key to index into the "keys" vector.
*/
/// An enhanced accumulator for maps of primitive values that implements [`GroupsAccumulator`].
#[derive(Debug)]
pub struct ByteGroupMapAccumulator<VBuilder>
where
    VBuilder: ArrayBuilder + PrimBuilderType,
{
    inner_field_type: DataType,
    key_cache: HashMap<Arc<Vec<u8>>, usize>,
    keys: Vec<Arc<Vec<u8>>>,
    group_maps: Vec<
        HashMap<
            usize,
            <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native,
            BuildNoHashHasher<usize>,
        >,
    >,
    nulls: MapNullState,
    op: MapAggregatorOp,
}

impl<VBuilder> ByteGroupMapAccumulator<VBuilder>
where
    VBuilder: ArrayBuilder + PrimBuilderType,
{
    // Creates a new group accumulator.
    //
    // The type of the map must be specified so that the correct sort
    // of map builder can be created.
    //
    // # Errors
    // If the incorrect type of data type is provided. Must me a map type with an
    // inner Struct type.
    pub fn try_new(map_type: &DataType, op: MapAggregatorOp) -> Result<Self> {
        if let DataType::Map(field, _) = map_type {
            let DataType::Struct(_) = field.data_type() else {
                return plan_err!(
                    "ByteGroupMapAccumulator inner field type should be a DataType::Struct"
                );
            };
            Ok(Self {
                inner_field_type: field.data_type().clone(),
                group_maps: Vec::with_capacity(INITIAL_GROUP_MAP_SIZE),
                keys: Vec::with_capacity(INITIAL_KEY_CACHE_SIZE),
                nulls: MapNullState::new(),
                key_cache: HashMap::with_capacity(INITIAL_KEY_CACHE_SIZE),
                op,
            })
        } else {
            plan_err!("Invalid datatype for ByteGroupMapAccumulator {map_type:?}")
        }
    }

    /// Makes a map builder type suitable for this accumulator. The runtime inner type
    /// of the map this accumulator is working with is used as the basis to determine the
    /// types of the builder that are placed in the returned [`MapBuilder`].
    ///
    /// # Panics
    /// If an invalid map type is found. This condition shouldn't occur as it is checked
    /// upon construction.
    fn make_map_builder(&self, cap: usize) -> MapBuilder<BinaryBuilder, VBuilder> {
        match &self.inner_field_type {
            DataType::Struct(fields) => {
                let names = MapFieldNames {
                    key: fields[0].name().clone(),
                    value: fields[1].name().clone(),
                    entry: "key_value".into(),
                };
                let key_builder = BinaryBuilder::with_capacity(cap, 1024);
                let value_builder = VBuilder::default();
                MapBuilder::with_capacity(Some(names), key_builder, value_builder, cap)
                    .with_keys_field(fields[0].clone())
                    .with_values_field(fields[1].clone())
            }
            _ => unreachable!(
                "Invalid datatype inside ByteGroupMapAccumulator {:?}",
                self.inner_field_type
            ),
        }
    }
}

/// Given an Arrow [`StructArray`] of keys and values, update the given map.
///
/// This implementation is for maps with binary keys and primitive values.
///
/// All nulls keys/values are skipped over.
fn update_binary_map_group<VBuilder>(
    input: Option<&StructArray>,
    map: &mut HashMap<
        usize,
        <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native,
        BuildNoHashHasher<usize>,
    >,
    key_cache: &mut HashMap<Arc<Vec<u8>>, usize>,
    keys: &mut Vec<Arc<Vec<u8>>>,
    op: &MapAggregatorOp,
) where
    VBuilder: ArrayBuilder + PrimBuilderType,
    <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native: NumAssign + Ord,
{
    if let Some(entries) = input {
        let map_keys = entries.column(0).as_binary::<i32>();
        let map_vals = entries
            .column(1)
            .as_primitive::<<VBuilder as PrimBuilderType>::ArrowType>();
        for (k, v) in map_keys.iter().zip(map_vals) {
            match (k, v) {
                (Some(key), Some(value)) => {
                    // Compute hash key for raw lookup
                    let key_hash = key_cache.hasher().hash_one(key);
                    let (_, i) = key_cache
                        .raw_entry_mut()
                        // Use raw lookup to avoid having to create new Arc from string reference
                        .from_hash(key_hash, |stored_key| stored_key.as_ref() == key)
                        .or_insert_with(|| {
                            // Only pay cost to create binary if needed
                            let owned_key = Arc::new(key.to_owned());
                            // Clone pointer into word list
                            keys.push(owned_key.clone());
                            (owned_key, keys.len() - 1)
                        });
                    // Do no-op hash lookup into group map
                    map.entry(*i)
                        .and_modify(|current_value| *current_value = op.op(*current_value, value))
                        .or_insert(value);
                }
                _ => panic!("Nullable entries aren't supported"),
            }
        }
    }
}

impl<VBuilder> GroupsAccumulator for ByteGroupMapAccumulator<VBuilder>
where
    VBuilder: ArrayBuilder + PrimBuilderType,
    <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native: NumAssign + Ord,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        if values.len() != 1 {
            return exec_err!("ByteGroupMapAccumulator only accepts single column input");
        }
        let data = values[0].as_map();
        // make sure we have room for the groups count
        self.group_maps.resize(total_num_groups, HashMap::default());
        self.nulls.accumulate(
            group_indices,
            data,
            opt_filter,
            total_num_groups,
            |g_idx, val| {
                let agg_map = &mut self.group_maps[g_idx];
                update_binary_map_group::<VBuilder>(
                    Some(val).as_ref(),
                    agg_map,
                    &mut self.key_cache,
                    &mut self.keys,
                    &self.op,
                );
            },
        );
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let result = emit_to.take_needed(&mut self.group_maps);
        let nulls = self.nulls.build(emit_to);
        assert_eq!(result.len(), nulls.len());
        // if nulls are present in data, we have to make sure we don't try to sum them
        let mut builder = self.make_map_builder(result.len());
        for (v, is_valid) in result.into_iter().zip(nulls.iter()) {
            if is_valid {
                for (key, val) in &v {
                    builder.keys().append_value(self.keys[*key].as_ref());
                    builder.values().append_value(val);
                }
                builder.append(true).expect("Can't finish MapBuilder");
            } else {
                builder.append(false).expect("Can't finish MapBuilder");
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.evaluate(emit_to).map(|arr| vec![arr])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.update_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn size(&self) -> usize {
        (std::mem::size_of::<
            HashMap<
                String,
                <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native,
            >,
        >() * self.group_maps.capacity())
            + self.nulls.size()
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let mut grouper = ByteGroupMapAccumulator::<VBuilder>::try_new(
            &DataType::Map(
                Arc::new(Field::new(
                    "key_value",
                    self.inner_field_type.clone(),
                    false,
                )),
                false,
            ),
            self.op.clone(),
        )?;
        grouper.update_batch(
            values,
            &(0usize..values[0].len()).collect::<Vec<_>>(),
            opt_filter,
            values[0].len(),
        )?;
        grouper.state(EmitTo::All)
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        assert_error,
        datafusion::aggregate_udf::{
            MapAggregatorOp,
            map_aggregate::{
                aggregator::map_test_common::make_map_datatype,
                group_accumulator::byte_group_accumulator::{
                    ByteGroupMapAccumulator, update_binary_map_group,
                },
                state::MapNullState,
            },
        },
    };
    use arrow::{
        array::{
            AsArray, BinaryArray, BinaryBuilder, Int64Array, Int64Builder, StructBuilder,
            UInt16Builder,
        },
        datatypes::{DataType, Field, Fields, Int64Type},
    };
    use datafusion::{
        common::HashMap,
        error::DataFusionError,
        logical_expr::{EmitTo, GroupsAccumulator},
    };
    use nohash::BuildNoHashHasher;
    use std::sync::Arc;

    // Macro to stringify a literal
    macro_rules! s {
        ($l:literal) => {
            Arc::new(String::from($l).into_bytes())
        };
    }

    #[test]
    fn try_new_should_succeed() {
        // Given
        let mt = make_map_datatype(DataType::Binary, DataType::Int64);

        // When
        let acc = ByteGroupMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum);

        // Then
        assert!(acc.is_ok());
    }

    #[test]
    fn try_new_should_error_on_non_map_type() {
        // Given
        let mt = DataType::Int16;
        let acc = ByteGroupMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum);

        // Then
        assert_error!(
            acc,
            DataFusionError::Plan,
            "Invalid datatype for ByteGroupMapAccumulator Int16"
        );
    }

    #[test]
    fn try_new_should_error_on_wrong_inner_type() {
        // Given
        let mt = DataType::Map(Arc::new(Field::new("test", DataType::Int16, false)), false);
        let acc = ByteGroupMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum);

        // Then
        assert_error!(
            acc,
            DataFusionError::Plan,
            "ByteGroupMapAccumulator inner field type should be a DataType::Struct"
        );
    }

    #[test]
    #[should_panic(expected = "Invalid datatype inside ByteGroupMapAccumulator Int16")]
    fn make_map_builder_check_unreachable() {
        // Given
        // Build instance directly via private constructor
        let acc = ByteGroupMapAccumulator::<Int64Builder> {
            inner_field_type: DataType::Int16,
            group_maps: Vec::with_capacity(1000),
            keys: Vec::with_capacity(200),
            nulls: MapNullState::new(),
            key_cache: HashMap::with_capacity(200),
            op: MapAggregatorOp::Sum,
        };

        // Then - should panic
        acc.make_map_builder(10);
    }

    #[test]
    fn make_map_builder_field_names_equal() {
        // Given
        let acc = ByteGroupMapAccumulator::<Int64Builder> {
            inner_field_type: DataType::Struct(Fields::from(vec![
                Field::new("key1_name", DataType::Binary, false),
                Field::new("value1_name", DataType::Int64, false),
            ])),
            group_maps: Vec::with_capacity(1000),
            keys: Vec::with_capacity(200),
            nulls: MapNullState::new(),
            key_cache: HashMap::with_capacity(200),
            op: MapAggregatorOp::Sum,
        };

        // When
        let array = acc.make_map_builder(10).finish();

        // Then
        assert_eq!(
            &array.entries().column_names(),
            &["key1_name", "value1_name"]
        );
    }

    #[test]
    fn make_map_builder_field_types_equal() {
        // Given
        let acc = ByteGroupMapAccumulator::<UInt16Builder> {
            inner_field_type: DataType::Struct(Fields::from(vec![
                Field::new("key1_name", DataType::Binary, false),
                Field::new("value1_name", DataType::UInt16, false),
            ])),
            group_maps: Vec::with_capacity(1000),
            keys: Vec::with_capacity(200),
            nulls: MapNullState::new(),
            key_cache: HashMap::with_capacity(200),
            op: MapAggregatorOp::Sum,
        };

        // When
        let array = acc.make_map_builder(10).finish();

        // Then
        assert_eq!(*array.key_type(), DataType::Binary);
        assert_eq!(*array.value_type(), DataType::UInt16);
    }

    #[test]
    fn update_byte_map_none() {
        // Given
        let mut map = HashMap::<usize, i64, BuildNoHashHasher<usize>>::default();
        map.insert(0, 2);
        map.insert(1, 4);
        let expected_map = map.clone();

        let mut key_cache = HashMap::<Arc<Vec<u8>>, usize>::new();
        key_cache.insert(s!["1"], 0);
        key_cache.insert(s!["3"], 1);
        let expected_key_cache = key_cache.clone();

        let mut klist = vec![s!["1"], s!["3"]];
        let expected_klist = klist.clone();

        // When
        update_binary_map_group::<Int64Builder>(
            None,
            &mut map,
            &mut key_cache,
            &mut klist,
            &MapAggregatorOp::Sum,
        );

        // Then - expect no changes
        assert_eq!(map, expected_map);
        assert_eq!(key_cache, expected_key_cache);
        assert_eq!(klist, expected_klist);
    }

    #[test]
    fn update_byte_map_empty_values() {
        // Given
        let mut map = HashMap::<usize, i64, BuildNoHashHasher<usize>>::default();
        map.insert(0, 2);
        map.insert(1, 4);
        let expected_map = map.clone();

        let mut key_cache = HashMap::<Arc<Vec<u8>>, usize>::new();
        key_cache.insert(s!["1"], 0);
        key_cache.insert(s!["3"], 1);
        let expected_key_cache = key_cache.clone();

        let mut klist = vec![s!["1"], s!["3"]];
        let expected_klist = klist.clone();

        let mut entry_builder = StructBuilder::new(
            Fields::from(vec![
                Field::new("key", DataType::Binary, false),
                Field::new("value", DataType::Int64, false),
            ]),
            vec![
                Box::new(BinaryBuilder::new()),
                Box::new(Int64Builder::new()),
            ],
        );

        // When
        update_binary_map_group::<Int64Builder>(
            Some(&entry_builder.finish()),
            &mut map,
            &mut key_cache,
            &mut klist,
            &MapAggregatorOp::Sum,
        );

        // Then - expect no changes
        assert_eq!(map, expected_map);
        assert_eq!(key_cache, expected_key_cache);
        assert_eq!(klist, expected_klist);
    }

    #[test]
    fn update_byte_map_enters_first_values() {
        // Given
        let mut map = HashMap::<usize, i64, BuildNoHashHasher<usize>>::default();
        let mut expected_map = map.clone();
        expected_map.insert(0, 2);
        expected_map.insert(1, 4);
        expected_map.insert(2, -10);

        let mut key_cache = HashMap::<Arc<Vec<u8>>, usize>::new();
        let mut expected_key_cache = key_cache.clone();
        expected_key_cache.insert(s!["1"], 0);
        expected_key_cache.insert(s!["3"], 1);
        expected_key_cache.insert(s!["8"], 2);

        let mut klist = vec![];
        let expected_klist = vec![s!["1"], s!["3"], s!["8"]];

        let mut entry_builder = StructBuilder::new(
            Fields::from(vec![
                Field::new("key", DataType::Binary, false),
                Field::new("value", DataType::Int64, false),
            ]),
            vec![
                Box::new(BinaryBuilder::new()),
                Box::new(Int64Builder::new()),
            ],
        );
        for (k, v) in &[(s!["1"], 2), (s!["3"], 4), (s!["8"], -10)] {
            entry_builder
                .field_builder::<BinaryBuilder>(0)
                .unwrap()
                .append_value(k.as_ref());
            entry_builder
                .field_builder::<Int64Builder>(1)
                .unwrap()
                .append_value(*v);
            entry_builder.append(true);
        }

        // When
        update_binary_map_group::<Int64Builder>(
            Some(&entry_builder.finish()),
            &mut map,
            &mut key_cache,
            &mut klist,
            &MapAggregatorOp::Sum,
        );

        // Then
        assert_eq!(map, expected_map);
        assert_eq!(key_cache, expected_key_cache);
        assert_eq!(klist, expected_klist);
    }

    #[test]
    fn update_byte_map_enters_values_and_sums() {
        // Given
        let mut map = HashMap::<usize, i64, BuildNoHashHasher<usize>>::default();
        map.insert(0, 2);
        map.insert(1, 4);
        let mut expected_map = map.clone();
        expected_map.insert(0, 6);
        expected_map.insert(1, 8);
        expected_map.insert(2, 2);

        let mut key_cache = HashMap::<Arc<Vec<u8>>, usize>::new();
        key_cache.insert(s!["1"], 0);
        key_cache.insert(s!["3"], 1);
        let mut expected_key_cache = key_cache.clone();
        expected_key_cache.insert(s!["1"], 0);
        expected_key_cache.insert(s!["3"], 1);
        expected_key_cache.insert(s!["4"], 2);

        let mut klist = vec![s!["1"], s!["3"]];
        let expected_klist = vec![s!["1"], s!["3"], s!["4"]];

        let mut entry_builder = StructBuilder::new(
            Fields::from(vec![
                Field::new("key", DataType::Binary, false),
                Field::new("value", DataType::Int64, false),
            ]),
            vec![
                Box::new(BinaryBuilder::new()),
                Box::new(Int64Builder::new()),
            ],
        );
        for (k, v) in [(s!["1"], 2), (s!["1"], 2), (s!["3"], 4), (s!["4"], 2)] {
            entry_builder
                .field_builder::<BinaryBuilder>(0)
                .unwrap()
                .append_value(k.as_ref());
            entry_builder
                .field_builder::<Int64Builder>(1)
                .unwrap()
                .append_value(v);
            entry_builder.append(true);
        }

        // When
        update_binary_map_group::<Int64Builder>(
            Some(&entry_builder.finish()),
            &mut map,
            &mut key_cache,
            &mut klist,
            &MapAggregatorOp::Sum,
        );

        // Then
        assert_eq!(map, expected_map);
        assert_eq!(key_cache, expected_key_cache);
        assert_eq!(klist, expected_klist);
    }

    #[test]
    #[should_panic(expected = "Nullable entries aren't supported")]
    fn update_byte_map_panics_on_null() {
        // Given
        let mut map = HashMap::<usize, i64, BuildNoHashHasher<usize>>::default();
        let mut key_cache = HashMap::<Arc<Vec<u8>>, usize>::new();
        let mut klist = vec![];

        let mut entry_builder = StructBuilder::new(
            Fields::from(vec![
                Field::new("key", DataType::Binary, true),
                Field::new("value", DataType::Int64, true),
            ]),
            vec![
                Box::new(BinaryBuilder::new()),
                Box::new(Int64Builder::new()),
            ],
        );
        for (k, v) in [(s!["1"], 2), (s!["1"], 2), (s!["3"], 4), (s!["4"], 2)] {
            entry_builder
                .field_builder::<BinaryBuilder>(0)
                .unwrap()
                .append_value(k.as_ref());
            entry_builder
                .field_builder::<Int64Builder>(1)
                .unwrap()
                .append_value(v);
            entry_builder.append(true);
        }
        // Add null elements
        entry_builder
            .field_builder::<BinaryBuilder>(0)
            .unwrap()
            .append_null();
        entry_builder
            .field_builder::<Int64Builder>(1)
            .unwrap()
            .append_null();
        entry_builder.append(true);

        // When - panic
        update_binary_map_group::<Int64Builder>(
            Some(&entry_builder.finish()),
            &mut map,
            &mut key_cache,
            &mut klist,
            &MapAggregatorOp::Sum,
        );
    }

    #[test]
    fn update_batch_should_fail_on_zero_or_multi_column() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Binary, DataType::Int64);
        let mut acc = ByteGroupMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum)?;

        // When
        let result = acc.update_batch(&[], &[1], None, 1);

        // Then
        assert_error!(
            result,
            DataFusionError::Execution,
            "ByteGroupMapAccumulator only accepts single column input"
        );

        // When
        let arr1 = BinaryArray::from_vec(vec![b"test", b"test2"]);
        let arr2 = Int64Array::from_value(1, 1);
        let result = acc.update_batch(&[Arc::new(arr1), Arc::new(arr2)], &[1], None, 1);

        // Then
        assert_error!(
            result,
            DataFusionError::Execution,
            "ByteGroupMapAccumulator only accepts single column input"
        );

        Ok(())
    }

    #[test]
    fn update_batch_should_not_update_zero_maps() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Binary, DataType::Int64);
        let mut acc = ByteGroupMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum)?;
        let mut builder = acc.make_map_builder(10);

        // When
        let array = builder.finish();
        acc.update_batch(&[Arc::new(array)], &[], None, 0)?;

        // Then
        assert!(acc.key_cache.is_empty());
        assert!(acc.keys.is_empty());
        assert_eq!(acc.group_maps.len(), 0);
        Ok(())
    }

    #[test]
    fn update_batch_should_not_update_single_empty_map() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Binary, DataType::Int64);
        let mut acc = ByteGroupMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum)?;
        let mut builder = acc.make_map_builder(10);

        // When
        builder.append(true)?;
        let array = builder.finish();
        acc.update_batch(&[Arc::new(array)], &[0], None, 1)?;

        // Then
        assert!(acc.key_cache.is_empty());
        assert!(acc.keys.is_empty());
        assert_eq!(acc.group_maps.len(), 1);
        assert!(acc.group_maps[0].is_empty());
        Ok(())
    }

    #[test]
    fn update_batch_should_update_single_map() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Binary, DataType::Int64);
        let mut acc = ByteGroupMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum)?;
        let mut builder = acc.make_map_builder(10);
        let expected_cache = HashMap::<Arc<Vec<u8>>, usize>::from([(s!["3"], 0)]);
        let mut expected_group_map = HashMap::<usize, i64, BuildNoHashHasher<usize>>::default();
        expected_group_map.insert(0, 10);
        let expected_group_maps = vec![expected_group_map];
        let expected_klist = vec![s!["3"]];

        // When
        builder.keys().append_value(s!["3"].as_ref());
        builder.values().append_value(10);
        builder.append(true)?;
        let array = builder.finish();
        acc.update_batch(&[Arc::new(array)], &[0], None, 1)?;

        // Then
        assert_eq!(acc.key_cache, expected_cache);
        assert_eq!(acc.keys, expected_klist);
        assert_eq!(acc.group_maps, expected_group_maps);
        Ok(())
    }

    #[test]
    fn update_batch_should_update_two_single_map() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Binary, DataType::Int64);
        let mut acc = ByteGroupMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum)?;
        let mut builder = acc.make_map_builder(10);
        let expected_cache = HashMap::<Arc<Vec<u8>>, usize>::from([(s!["3"], 0), (s!["2"], 1)]);
        let mut expected_group_map = HashMap::<usize, i64, BuildNoHashHasher<usize>>::default();
        expected_group_map.insert(0, 10);
        expected_group_map.insert(1, 5);
        let expected_group_maps = vec![expected_group_map];
        let expected_klist = vec![s!["3"], s!["2"]];

        // When
        builder.keys().append_value(s!["3"].as_ref());
        builder.values().append_value(10);
        builder.append(true)?;
        builder.keys().append_value(s!["2"].as_ref());
        builder.values().append_value(5);
        builder.append(true)?;

        let array = builder.finish();
        acc.update_batch(&[Arc::new(array)], &[0, 0], None, 1)?;

        // Then
        assert_eq!(acc.key_cache, expected_cache);
        assert_eq!(acc.keys, expected_klist);
        assert_eq!(acc.group_maps, expected_group_maps);

        Ok(())
    }

    #[test]
    fn update_batch_should_update_multiple_maps() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Binary, DataType::Int64);
        let mut acc = ByteGroupMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum)?;
        let mut builder = acc.make_map_builder(10);
        let expected_cache = HashMap::<Arc<Vec<u8>>, usize>::from([
            (s!["3"], 0),
            (s!["1"], 1),
            (s!["2"], 2),
            (s!["4"], 3),
        ]);
        let mut expected_group_map = HashMap::<usize, i64, BuildNoHashHasher<usize>>::default();
        expected_group_map.insert(0, 10);
        expected_group_map.insert(1, 13);
        expected_group_map.insert(2, 7);
        expected_group_map.insert(3, 10);
        let expected_group_maps = vec![expected_group_map];
        let expected_klist = vec![s!["3"], s!["1"], s!["2"], s!["4"]];

        // When
        for (k, v) in [(s!["3"], 10), (s!["1"], 4), (s!["1"], 3), (s!["2"], 7)] {
            builder.keys().append_value(k.as_ref());
            builder.values().append_value(v);
        }
        builder.append(true)?;

        builder.keys().append_value(s!["1"].as_ref());
        builder.values().append_value(3);

        builder.append(true)?;

        for (k, v) in [(s!["1"], 3), (s!["4"], 10)] {
            builder.keys().append_value(k.as_ref());
            builder.values().append_value(v);
        }
        builder.append(true)?;

        let array = builder.finish();
        acc.update_batch(&[Arc::new(array)], &[0, 0, 0], None, 1)?;

        // Then
        assert_eq!(acc.key_cache, expected_cache);
        assert_eq!(acc.keys, expected_klist);
        assert_eq!(acc.group_maps, expected_group_maps);

        Ok(())
    }

    #[test]
    fn update_batch_should_update_multiple_maps_multiple_groups() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Binary, DataType::Int64);
        let mut acc = ByteGroupMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum)?;
        let mut builder = acc.make_map_builder(10);
        let expected_cache = HashMap::<Arc<Vec<u8>>, usize>::from([
            (s!["3"], 0),
            (s!["1"], 1),
            (s!["2"], 2),
            (s!["4"], 3),
            (s!["5"], 4),
        ]);
        let mut expected_first_group_map =
            HashMap::<usize, i64, BuildNoHashHasher<usize>>::default();

        expected_first_group_map.insert(0, 10);
        expected_first_group_map.insert(1, 10);
        expected_first_group_map.insert(2, 7);
        expected_first_group_map.insert(3, 11);
        let mut expected_second_group_map =
            HashMap::<usize, i64, BuildNoHashHasher<usize>>::default();

        expected_second_group_map.insert(0, 5);
        expected_second_group_map.insert(1, 11);
        expected_second_group_map.insert(4, 13);

        let expected_group_maps = vec![expected_first_group_map, expected_second_group_map];
        let expected_klist = vec![s!["3"], s!["1"], s!["2"], s!["4"], s!["5"]];

        // When
        // Create values for first group
        for (k, v) in [(s!["3"], 10), (s!["1"], 4), (s!["1"], 3), (s!["2"], 7)] {
            builder.keys().append_value(k.as_ref());
            builder.values().append_value(v);
        }
        builder.append(true)?;
        for (k, v) in [(s!["1"], 3), (s!["4"], 11)] {
            builder.keys().append_value(k.as_ref());
            builder.values().append_value(v);
        }
        builder.append(true)?;

        // Create values for second group
        for (k, v) in [(s!["3"], 5), (s!["1"], 3), (s!["1"], 5), (s!["5"], 9)] {
            builder.keys().append_value(k.as_ref());
            builder.values().append_value(v);
        }
        builder.append(true)?;
        for (k, v) in [(s!["1"], 3), (s!["5"], 4)] {
            builder.keys().append_value(k.as_ref());
            builder.values().append_value(v);
        }
        builder.append(true)?;

        let array = builder.finish();
        acc.update_batch(&[Arc::new(array)], &[0, 0, 1, 1], None, 2)?;

        // Then
        assert_eq!(acc.key_cache, expected_cache);
        assert_eq!(acc.keys, expected_klist);
        assert_eq!(acc.group_maps, expected_group_maps);

        Ok(())
    }

    #[test]
    fn evaluate_should_produce_results_single_map() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Binary, DataType::Int64);
        let mut acc = ByteGroupMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum)?;
        let mut builder = acc.make_map_builder(10);
        let expected = HashMap::from([(s!["3"], 10), (s!["1"], 9), (s!["2"], 7)]);

        // When
        for (k, v) in [
            (s!["3"], 10),
            (s!["1"], 4),
            (s!["1"], 3),
            (s!["2"], 7),
            (s!["1"], 2),
        ] {
            builder.keys().append_value(k.as_ref());
            builder.values().append_value(v);
        }

        builder.append(true)?;
        acc.update_batch(&[Arc::new(builder.finish())], &[0], None, 1)?;
        let result = acc.evaluate(EmitTo::All)?;

        // Then
        // Iteration order of maps is not guaranteed, so read the scalar value results into a new map
        let result_map = result.as_map();
        let keys = result_map.keys().as_binary::<i32>();
        let values = result_map.values().as_primitive::<Int64Type>();
        let mut accumulated_map = HashMap::new();
        for item in keys.iter().zip(values.iter()) {
            if let (Some(opt_k), Some(opt_v)) = item {
                accumulated_map.insert(Arc::new(opt_k.into()), opt_v);
            }
        }

        assert_eq!(expected, accumulated_map);
        Ok(())
    }

    #[test]
    fn evaluate_should_produce_results_multiple_groups() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Binary, DataType::Int64);
        let mut acc = ByteGroupMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum)?;
        let mut builder = acc.make_map_builder(10);
        let expected_first_group =
            HashMap::from([(s!["3"], 10), (s!["1"], 10), (s!["2"], 7), (s!["4"], 11)]);
        let expected_second_group = HashMap::from([(s!["3"], 5), (s!["1"], 11), (s!["5"], 13)]);

        // When
        // Create values for first group
        for (k, v) in [(s!["3"], 10), (s!["1"], 4), (s!["1"], 3), (s!["2"], 7)] {
            builder.keys().append_value(k.as_ref());
            builder.values().append_value(v);
        }
        builder.append(true)?;
        for (k, v) in [(s!["1"], 3), (s!["4"], 11)] {
            builder.keys().append_value(k.as_ref());
            builder.values().append_value(v);
        }
        builder.append(true)?;

        // Create values for second group
        for (k, v) in [(s!["3"], 5), (s!["1"], 3), (s!["1"], 5), (s!["5"], 9)] {
            builder.keys().append_value(k.as_ref());
            builder.values().append_value(v);
        }
        builder.append(true)?;
        for (k, v) in [(s!["1"], 3), (s!["5"], 4)] {
            builder.keys().append_value(k.as_ref());
            builder.values().append_value(v);
        }
        builder.append(true)?;

        let array = builder.finish();
        acc.update_batch(&[Arc::new(array)], &[0, 0, 1, 1], None, 2)?;
        let result = acc.evaluate(EmitTo::All)?;

        // Then
        // Iteration order of maps is not guaranteed, so read the scalar value results into a new map
        let result_map = result.as_map();
        // Iterate for each map group
        for (expected_map, map_array) in [expected_first_group, expected_second_group]
            .into_iter()
            .zip(result_map.iter())
        {
            let Some(inner_struct) = map_array else {
                panic!("inner_struct should never be null in this test");
            };
            // Read inner map array into a HashMap, then compare against expected values
            let keys = inner_struct.column(0).as_binary::<i32>();
            let values = inner_struct.column(1).as_primitive::<Int64Type>();
            let mut accumulated_map = HashMap::new();
            for item in keys.iter().zip(values.iter()) {
                if let (Some(opt_k), Some(opt_v)) = item {
                    accumulated_map.insert(Arc::new(opt_k.into()), opt_v);
                }
            }
            assert_eq!(expected_map, accumulated_map);
        }

        Ok(())
    }
}
