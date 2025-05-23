/// [`GroupAccumulator`] implementations for primitive group map aggregation.
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
use crate::datafusion::functions::{
    MapAggregatorOp,
    map_aggregate::{aggregator::PrimBuilderType, state::MapNullState},
};
use arrow::{
    array::{
        ArrayBuilder, ArrayRef, ArrowPrimitiveType, AsArray, BooleanArray, MapBuilder,
        MapFieldNames, StructArray,
    },
    datatypes::{DataType, Field},
};
use datafusion::{
    common::{HashMap, exec_err, plan_err},
    error::Result,
    logical_expr::{EmitTo, GroupsAccumulator},
};
use nohash::{BuildNoHashHasher, IsEnabled};
use num_traits::NumAssign;
use std::{hash::Hash, sync::Arc};

use super::INITIAL_GROUP_MAP_SIZE;

type PrimitiveMap<KBuilder, VBuilder> = HashMap<
    <<KBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native,
    <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native,
    BuildNoHashHasher<<<KBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native>,
>;

/// An enhanced accumulator for maps of primitive values that implements [`GroupAccumulator`].
#[derive(Debug)]
pub struct PrimGroupMapAccumulator<KBuilder, VBuilder>
where
    KBuilder: ArrayBuilder + PrimBuilderType,
    VBuilder: ArrayBuilder + PrimBuilderType,
{
    inner_field_type: DataType,
    group_maps: Vec<PrimitiveMap<KBuilder, VBuilder>>,
    nulls: MapNullState,
    op: MapAggregatorOp,
}

impl<KBuilder, VBuilder> PrimGroupMapAccumulator<KBuilder, VBuilder>
where
    KBuilder: ArrayBuilder + PrimBuilderType,
    VBuilder: ArrayBuilder + PrimBuilderType,
{
    // Creates a new accumulator.
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
                    "PrimGroupMapAccumulator inner field type should be a DataType::Struct"
                );
            };
            Ok(Self {
                inner_field_type: field.data_type().clone(),
                group_maps: Vec::with_capacity(INITIAL_GROUP_MAP_SIZE),
                nulls: MapNullState::new(),
                op,
            })
        } else {
            plan_err!("Invalid datatype for PrimGroupMapAccumulator {map_type:?}")
        }
    }

    /// Makes a map builder type suitable for this accumulator. The runtime inner type
    /// of the map this accumulator is working with is used as the basis to determine the
    /// types of the builder that are placed in the returned [`MapBuilder`].
    ///
    /// # Panics
    /// If an invalid map type is found. This condition shouldn't occur as it is checked
    /// upon construction.
    fn make_map_builder(&self, cap: usize) -> MapBuilder<KBuilder, VBuilder> {
        match &self.inner_field_type {
            DataType::Struct(fields) => {
                let names = MapFieldNames {
                    key: fields[0].name().clone(),
                    value: fields[1].name().clone(),
                    entry: "key_value".into(),
                };
                let key_builder = KBuilder::default();
                let value_builder = VBuilder::default();
                MapBuilder::with_capacity(Some(names), key_builder, value_builder, cap)
                    .with_keys_field(fields[0].clone())
                    .with_values_field(fields[1].clone())
            }
            _ => unreachable!(
                "Invalid datatype inside PrimGroupMapAccumulator {:?}",
                self.inner_field_type
            ),
        }
    }
}

/// Given an Arrow [`StructArray`] of keys and values, update the given map.
///
/// This implementation is for maps with string keys and primitive values.
///
/// All nulls keys/values are skipped over.
fn update_prim_map_group<KBuilder, VBuilder>(
    input: Option<&StructArray>,
    map: &mut PrimitiveMap<KBuilder, VBuilder>,
    op: &MapAggregatorOp,
) where
    KBuilder: ArrayBuilder + PrimBuilderType,
    VBuilder: ArrayBuilder + PrimBuilderType,
    <<KBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native: Hash + Eq + IsEnabled,
    <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native: NumAssign + Ord,
{
    if let Some(entries) = input {
        let map_keys = entries
            .column(0)
            .as_primitive::<<KBuilder as PrimBuilderType>::ArrowType>();
        let map_vals = entries
            .column(1)
            .as_primitive::<<VBuilder as PrimBuilderType>::ArrowType>();
        for (k, v) in map_keys.iter().zip(map_vals) {
            match (k, v) {
                (Some(key), Some(value)) => {
                    map.entry(key)
                        .and_modify(|current_value| *current_value = op.op(*current_value, value))
                        .or_insert(value);
                }
                _ => panic!("Nullable entries aren't supported"),
            }
        }
    }
}

impl<KBuilder, VBuilder> GroupsAccumulator for PrimGroupMapAccumulator<KBuilder, VBuilder>
where
    KBuilder: ArrayBuilder + PrimBuilderType,
    VBuilder: ArrayBuilder + PrimBuilderType,
    <<KBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native: Hash + Eq + IsEnabled,
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
            return exec_err!("PrimGroupMapAccumulator only accepts single column input");
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
                update_prim_map_group::<KBuilder, VBuilder>(Some(val).as_ref(), agg_map, &self.op);
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
                    builder.keys().append_value(key);
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
                <<KBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native,
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
        let mut grouper = PrimGroupMapAccumulator::<KBuilder, VBuilder>::try_new(
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
        datafusion::functions::{
            MapAggregatorOp,
            map_aggregate::{
                aggregator::map_test_common::make_map_datatype,
                group_accumulator::prim_group_accumulator::{
                    PrimGroupMapAccumulator, update_prim_map_group,
                },
                state::MapNullState,
            },
        },
    };
    use arrow::{
        array::{AsArray, Int64Array, Int64Builder, StructBuilder, UInt16Builder},
        datatypes::{DataType, Field, Fields, Int64Type},
    };
    use datafusion::{
        common::HashMap,
        error::DataFusionError,
        logical_expr::{EmitTo, GroupsAccumulator},
    };
    use nohash::BuildNoHashHasher;
    use std::sync::Arc;

    #[test]
    fn try_new_should_succeed() {
        // Given
        let mt = make_map_datatype(DataType::Int64, DataType::Int64);

        // When
        let acc = PrimGroupMapAccumulator::<Int64Builder, Int64Builder>::try_new(
            &mt,
            MapAggregatorOp::Sum,
        );

        // Then
        assert!(acc.is_ok());
    }

    #[test]
    fn try_new_should_error_on_non_map_type() {
        // Given
        let mt = DataType::Int16;
        let acc = PrimGroupMapAccumulator::<Int64Builder, Int64Builder>::try_new(
            &mt,
            MapAggregatorOp::Sum,
        );

        // Then
        assert_error!(
            acc,
            DataFusionError::Plan,
            "Invalid datatype for PrimGroupMapAccumulator Int16"
        );
    }

    #[test]
    fn try_new_should_error_on_wrong_inner_type() {
        // Given
        let mt = DataType::Map(Arc::new(Field::new("test", DataType::Int16, false)), false);
        let acc = PrimGroupMapAccumulator::<Int64Builder, Int64Builder>::try_new(
            &mt,
            MapAggregatorOp::Sum,
        );

        // Then
        assert_error!(
            acc,
            DataFusionError::Plan,
            "PrimGroupMapAccumulator inner field type should be a DataType::Struct"
        );
    }

    #[test]
    #[should_panic(expected = "Invalid datatype inside PrimGroupMapAccumulator Int16")]
    fn make_map_builder_check_unreachable() {
        // Given
        // Build instance directly via private constructor
        let acc = PrimGroupMapAccumulator::<Int64Builder, Int64Builder> {
            inner_field_type: DataType::Int16,
            group_maps: Vec::with_capacity(1000),
            nulls: MapNullState::new(),
            op: MapAggregatorOp::Sum,
        };

        // Then - should panic
        acc.make_map_builder(10);
    }

    #[test]
    fn make_map_builder_field_names_equal() {
        // Given
        let acc = PrimGroupMapAccumulator::<Int64Builder, Int64Builder> {
            inner_field_type: DataType::Struct(Fields::from(vec![
                Field::new("key1_name", DataType::Int64, false),
                Field::new("value1_name", DataType::Int64, false),
            ])),
            group_maps: Vec::with_capacity(1000),
            nulls: MapNullState::new(),
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
        let acc = PrimGroupMapAccumulator::<Int64Builder, UInt16Builder> {
            inner_field_type: DataType::Struct(Fields::from(vec![
                Field::new("key1_name", DataType::Int64, false),
                Field::new("value1_name", DataType::UInt16, false),
            ])),
            group_maps: Vec::with_capacity(1000),
            nulls: MapNullState::new(),
            op: MapAggregatorOp::Sum,
        };

        // When
        let array = acc.make_map_builder(10).finish();

        // Then
        assert_eq!(*array.key_type(), DataType::Int64);
        assert_eq!(*array.value_type(), DataType::UInt16);
    }

    #[test]
    fn update_prim_map_none() {
        // Given
        let mut map = HashMap::<i64, i64, BuildNoHashHasher<i64>>::default();
        map.insert(1, 2);
        map.insert(3, 4);
        let expected_map = map.clone();

        // When
        update_prim_map_group::<Int64Builder, Int64Builder>(None, &mut map, &MapAggregatorOp::Sum);

        // Then - expect no changes
        assert_eq!(map, expected_map);
    }

    #[test]
    fn update_prim_map_empty_values() {
        // Given
        let mut map = HashMap::<i64, i64, BuildNoHashHasher<i64>>::default();
        map.insert(1, 2);
        map.insert(3, 4);
        let expected_map = map.clone();

        let mut entry_builder = StructBuilder::new(
            Fields::from(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
            ]),
            vec![Box::new(Int64Builder::new()), Box::new(Int64Builder::new())],
        );

        // When
        update_prim_map_group::<Int64Builder, Int64Builder>(
            Some(&entry_builder.finish()),
            &mut map,
            &MapAggregatorOp::Sum,
        );

        // Then - expect no changes
        assert_eq!(map, expected_map);
    }

    #[test]
    fn update_prim_map_enters_first_values() {
        // Given
        let mut map = HashMap::<i64, i64, BuildNoHashHasher<i64>>::default();
        let mut expected_map = map.clone();
        expected_map.insert(1, 2);
        expected_map.insert(3, 4);
        expected_map.insert(8, -10);

        let mut entry_builder = StructBuilder::new(
            Fields::from(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
            ]),
            vec![Box::new(Int64Builder::new()), Box::new(Int64Builder::new())],
        );
        for (k, v) in &[(1, 2), (3, 4), (8, -10)] {
            entry_builder
                .field_builder::<Int64Builder>(0)
                .unwrap()
                .append_value(*k);
            entry_builder
                .field_builder::<Int64Builder>(1)
                .unwrap()
                .append_value(*v);
            entry_builder.append(true);
        }

        // When
        update_prim_map_group::<Int64Builder, Int64Builder>(
            Some(&entry_builder.finish()),
            &mut map,
            &MapAggregatorOp::Sum,
        );

        // Then
        assert_eq!(map, expected_map);
    }

    #[test]
    fn update_prim_map_enters_values_and_sums() {
        // Given
        let mut map = HashMap::<i64, i64, BuildNoHashHasher<i64>>::default();
        map.insert(1, 2);
        map.insert(3, 4);
        let mut expected_map = map.clone();
        expected_map.insert(1, 6);
        expected_map.insert(3, 8);
        expected_map.insert(4, 2);

        let mut entry_builder = StructBuilder::new(
            Fields::from(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
            ]),
            vec![Box::new(Int64Builder::new()), Box::new(Int64Builder::new())],
        );
        for (k, v) in [(1, 2), (1, 2), (3, 4), (4, 2)] {
            entry_builder
                .field_builder::<Int64Builder>(0)
                .unwrap()
                .append_value(k);
            entry_builder
                .field_builder::<Int64Builder>(1)
                .unwrap()
                .append_value(v);
            entry_builder.append(true);
        }

        // When
        update_prim_map_group::<Int64Builder, Int64Builder>(
            Some(&entry_builder.finish()),
            &mut map,
            &MapAggregatorOp::Sum,
        );

        // Then
        assert_eq!(map, expected_map);
    }

    #[test]
    #[should_panic(expected = "Nullable entries aren't supported")]
    fn update_prim_map_panics_on_null() {
        // Given
        let mut map = HashMap::<i64, i64, BuildNoHashHasher<i64>>::default();

        let mut entry_builder = StructBuilder::new(
            Fields::from(vec![
                Field::new("key", DataType::Int64, true),
                Field::new("value", DataType::Int64, true),
            ]),
            vec![Box::new(Int64Builder::new()), Box::new(Int64Builder::new())],
        );
        for (k, v) in [(1, 2), (1, 2), (3, 4), (4, 2)] {
            entry_builder
                .field_builder::<Int64Builder>(0)
                .unwrap()
                .append_value(k);
            entry_builder
                .field_builder::<Int64Builder>(1)
                .unwrap()
                .append_value(v);
            entry_builder.append(true);
        }
        // Add null elements
        entry_builder
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_null();
        entry_builder
            .field_builder::<Int64Builder>(1)
            .unwrap()
            .append_null();
        entry_builder.append(true);

        // When - panic
        update_prim_map_group::<Int64Builder, Int64Builder>(
            Some(&entry_builder.finish()),
            &mut map,
            &MapAggregatorOp::Sum,
        );
    }

    #[test]
    fn update_batch_should_fail_on_zero_or_multi_column() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Int64, DataType::Int64);
        let mut acc = PrimGroupMapAccumulator::<Int64Builder, Int64Builder>::try_new(
            &mt,
            MapAggregatorOp::Sum,
        )?;

        // When
        let result = acc.update_batch(&[], &[1], None, 1);

        // Then
        assert_error!(
            result,
            DataFusionError::Execution,
            "PrimGroupMapAccumulator only accepts single column input"
        );

        // When
        let arr1 = Int64Array::from_value(1, 1);
        let arr2 = arr1.clone();
        let result = acc.update_batch(&[Arc::new(arr1), Arc::new(arr2)], &[1], None, 1);

        // Then
        assert_error!(
            result,
            DataFusionError::Execution,
            "PrimGroupMapAccumulator only accepts single column input"
        );

        Ok(())
    }

    #[test]
    fn update_batch_should_not_update_zero_maps() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Int64, DataType::Int64);
        let mut acc = PrimGroupMapAccumulator::<Int64Builder, Int64Builder>::try_new(
            &mt,
            MapAggregatorOp::Sum,
        )?;
        let mut builder = acc.make_map_builder(10);

        // When
        let array = builder.finish();
        acc.update_batch(&[Arc::new(array)], &[], None, 0)?;

        // Then
        assert_eq!(acc.group_maps.len(), 0);
        Ok(())
    }

    #[test]
    fn update_batch_should_not_update_single_empty_map() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Int64, DataType::Int64);
        let mut acc = PrimGroupMapAccumulator::<Int64Builder, Int64Builder>::try_new(
            &mt,
            MapAggregatorOp::Sum,
        )?;
        let mut builder = acc.make_map_builder(10);

        // When
        builder.append(true)?;
        let array = builder.finish();
        acc.update_batch(&[Arc::new(array)], &[0], None, 1)?;

        // Then
        assert_eq!(acc.group_maps.len(), 1);
        assert!(acc.group_maps[0].is_empty());
        Ok(())
    }

    #[test]
    fn update_batch_should_update_single_map() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Int64, DataType::Int64);
        let mut acc = PrimGroupMapAccumulator::<Int64Builder, Int64Builder>::try_new(
            &mt,
            MapAggregatorOp::Sum,
        )?;
        let mut builder = acc.make_map_builder(10);
        let mut expected_group_map = HashMap::<i64, i64, BuildNoHashHasher<i64>>::default();
        expected_group_map.insert(3, 10);
        let expected_group_maps = vec![expected_group_map];

        // When
        builder.keys().append_value(3);
        builder.values().append_value(10);
        builder.append(true)?;
        let array = builder.finish();
        acc.update_batch(&[Arc::new(array)], &[0], None, 1)?;

        // Then
        assert_eq!(acc.group_maps, expected_group_maps);
        Ok(())
    }

    #[test]
    fn update_batch_should_update_two_single_map() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Int64, DataType::Int64);
        let mut acc = PrimGroupMapAccumulator::<Int64Builder, Int64Builder>::try_new(
            &mt,
            MapAggregatorOp::Sum,
        )?;
        let mut builder = acc.make_map_builder(10);
        let mut expected_group_map = HashMap::<i64, i64, BuildNoHashHasher<i64>>::default();
        expected_group_map.insert(3, 10);
        expected_group_map.insert(2, 5);
        let expected_group_maps = vec![expected_group_map];

        // When
        builder.keys().append_value(3);
        builder.values().append_value(10);
        builder.append(true)?;
        builder.keys().append_value(2);
        builder.values().append_value(5);
        builder.append(true)?;

        let array = builder.finish();
        acc.update_batch(&[Arc::new(array)], &[0, 0], None, 1)?;

        // Then
        assert_eq!(acc.group_maps, expected_group_maps);

        Ok(())
    }

    #[test]
    fn update_batch_should_update_multiple_maps() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Int64, DataType::Int64);
        let mut acc = PrimGroupMapAccumulator::<Int64Builder, Int64Builder>::try_new(
            &mt,
            MapAggregatorOp::Sum,
        )?;
        let mut builder = acc.make_map_builder(10);
        let mut expected_group_map = HashMap::<i64, i64, BuildNoHashHasher<i64>>::default();
        expected_group_map.insert(3, 10);
        expected_group_map.insert(1, 13);
        expected_group_map.insert(2, 7);
        expected_group_map.insert(4, 10);
        let expected_group_maps = vec![expected_group_map];

        // When
        for (k, v) in [(3, 10), (1, 4), (1, 3), (2, 7)] {
            builder.keys().append_value(k);
            builder.values().append_value(v);
        }
        builder.append(true)?;

        builder.keys().append_value(1);
        builder.values().append_value(3);

        builder.append(true)?;

        for (k, v) in [(1, 3), (4, 10)] {
            builder.keys().append_value(k);
            builder.values().append_value(v);
        }
        builder.append(true)?;

        let array = builder.finish();
        acc.update_batch(&[Arc::new(array)], &[0, 0, 0], None, 1)?;

        // Then
        assert_eq!(acc.group_maps, expected_group_maps);

        Ok(())
    }

    #[test]
    fn update_batch_should_update_multiple_maps_multiple_groups() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Int64, DataType::Int64);
        let mut acc = PrimGroupMapAccumulator::<Int64Builder, Int64Builder>::try_new(
            &mt,
            MapAggregatorOp::Sum,
        )?;
        let mut builder = acc.make_map_builder(10);
        let mut expected_first_group_map = HashMap::<i64, i64, BuildNoHashHasher<i64>>::default();

        expected_first_group_map.insert(3, 10);
        expected_first_group_map.insert(1, 10);
        expected_first_group_map.insert(2, 7);
        expected_first_group_map.insert(4, 11);
        let mut expected_second_group_map = HashMap::<i64, i64, BuildNoHashHasher<i64>>::default();

        expected_second_group_map.insert(3, 5);
        expected_second_group_map.insert(1, 11);
        expected_second_group_map.insert(5, 13);

        let expected_group_maps = vec![expected_first_group_map, expected_second_group_map];

        // When
        // Create values for first group
        for (k, v) in [(3, 10), (1, 4), (1, 3), (2, 7)] {
            builder.keys().append_value(k);
            builder.values().append_value(v);
        }
        builder.append(true)?;
        for (k, v) in [(1, 3), (4, 11)] {
            builder.keys().append_value(k);
            builder.values().append_value(v);
        }
        builder.append(true)?;

        // Create values for second group
        for (k, v) in [(3, 5), (1, 3), (1, 5), (5, 9)] {
            builder.keys().append_value(k);
            builder.values().append_value(v);
        }
        builder.append(true)?;
        for (k, v) in [(1, 3), (5, 4)] {
            builder.keys().append_value(k);
            builder.values().append_value(v);
        }
        builder.append(true)?;

        let array = builder.finish();
        acc.update_batch(&[Arc::new(array)], &[0, 0, 1, 1], None, 2)?;

        // Then
        assert_eq!(acc.group_maps, expected_group_maps);

        Ok(())
    }

    #[test]
    fn evaluate_should_produce_results_single_map() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Int64, DataType::Int64);
        let mut acc = PrimGroupMapAccumulator::<Int64Builder, Int64Builder>::try_new(
            &mt,
            MapAggregatorOp::Sum,
        )?;
        let mut builder = acc.make_map_builder(10);
        let expected = HashMap::from([(3, 10), (1, 9), (2, 7)]);

        // When
        for (k, v) in [(3, 10), (1, 4), (1, 3), (2, 7), (1, 2)] {
            builder.keys().append_value(k);
            builder.values().append_value(v);
        }

        builder.append(true)?;
        acc.update_batch(&[Arc::new(builder.finish())], &[0], None, 1)?;
        let result = acc.evaluate(EmitTo::All)?;

        // Then
        // Iteration order of maps is not guaranteed, so read the scalar value results into a new map
        let result_map = result.as_map();
        let keys = result_map.keys().as_primitive::<Int64Type>();
        let values = result_map.values().as_primitive::<Int64Type>();
        let mut accumulated_map = HashMap::new();
        for item in keys.iter().zip(values.iter()) {
            if let (Some(opt_k), Some(opt_v)) = item {
                accumulated_map.insert(opt_k, opt_v);
            }
        }

        assert_eq!(expected, accumulated_map);
        Ok(())
    }

    #[test]
    fn evaluate_should_produce_results_multiple_groups() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Int64, DataType::Int64);
        let mut acc = PrimGroupMapAccumulator::<Int64Builder, Int64Builder>::try_new(
            &mt,
            MapAggregatorOp::Sum,
        )?;
        let mut builder = acc.make_map_builder(10);
        let expected_first_group = HashMap::from([(3, 10), (1, 10), (2, 7), (4, 11)]);
        let expected_second_group = HashMap::from([(3, 5), (1, 11), (5, 13)]);

        // When
        // Create values for first group
        for (k, v) in [(3, 10), (1, 4), (1, 3), (2, 7)] {
            builder.keys().append_value(k);
            builder.values().append_value(v);
        }
        builder.append(true)?;
        for (k, v) in [(1, 3), (4, 11)] {
            builder.keys().append_value(k);
            builder.values().append_value(v);
        }
        builder.append(true)?;

        // Create values for second group
        for (k, v) in [(3, 5), (1, 3), (1, 5), (5, 9)] {
            builder.keys().append_value(k);
            builder.values().append_value(v);
        }
        builder.append(true)?;
        for (k, v) in [(1, 3), (5, 4)] {
            builder.keys().append_value(k);
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
            let keys = inner_struct.column(0).as_primitive::<Int64Type>();
            let values = inner_struct.column(1).as_primitive::<Int64Type>();
            let mut accumulated_map = HashMap::new();
            for item in keys.iter().zip(values.iter()) {
                if let (Some(opt_k), Some(opt_v)) = item {
                    accumulated_map.insert(opt_k, opt_v);
                }
            }
            assert_eq!(expected_map, accumulated_map);
        }

        Ok(())
    }
}
