/// [`GroupAccumulator`] implementations for map aggregation.
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
use super::{MapAggregatorOp, map_agg::PrimBuilderType, state::MapNullState};
use arrow::{
    array::{
        ArrayBuilder, ArrayRef, ArrowPrimitiveType, AsArray, BinaryBuilder, BooleanArray,
        MapBuilder, MapFieldNames, StringBuilder, StructArray,
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
use std::{hash::Hash, sync::Arc};

/// An enhanced accumulator for maps of primitive values that implements [`GroupAccumulator`].
#[derive(Debug)]
pub struct PrimGroupMapAccumulator<KBuilder, VBuilder>
where
    KBuilder: ArrayBuilder + PrimBuilderType,
    VBuilder: ArrayBuilder + PrimBuilderType,
{
    inner_field_type: DataType,
    word_cache:
        HashMap<<<KBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native, usize>,
    words: Vec<<<KBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native>,
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
                group_maps: Vec::with_capacity(1000),
                words: Vec::with_capacity(200),
                nulls: MapNullState::new(),
                word_cache: HashMap::with_capacity(200),
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
                "Invalid datatype inside StringGroupMapAccumulator {:?}",
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
    map: &mut HashMap<
        usize,
        <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native,
        BuildNoHashHasher<usize>,
    >,
    word_cache: &mut HashMap<
        <<KBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native,
        usize,
    >,
    words: &mut Vec<<<KBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native>,
    op: &MapAggregatorOp,
) where
    KBuilder: ArrayBuilder + PrimBuilderType,
    VBuilder: ArrayBuilder + PrimBuilderType,
    <<KBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native: Hash + Eq,
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
                    let i = word_cache.entry(key).or_insert_with(|| {
                        words.push(key);
                        words.len() - 1
                    });
                    map.entry(*i)
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
    <<KBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native: Hash + Eq,
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
                update_prim_map_group::<KBuilder, VBuilder>(
                    Some(val).as_ref(),
                    agg_map,
                    &mut self.word_cache,
                    &mut self.words,
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
                    builder.keys().append_value(&self.words[*key]);
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

/// An enhanced accumulator for maps of primitive values that implements [`GroupAccumulator`].
#[derive(Debug)]
pub struct StringGroupMapAccumulator<VBuilder>
where
    VBuilder: ArrayBuilder + PrimBuilderType,
{
    inner_field_type: DataType,
    word_cache: HashMap<String, usize>,
    words: Vec<String>,
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

impl<VBuilder> StringGroupMapAccumulator<VBuilder>
where
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
                    "StringGroupMapAccumulator inner field type should be a DataType::Struct"
                );
            };
            Ok(Self {
                inner_field_type: field.data_type().clone(),
                group_maps: Vec::with_capacity(1000),
                words: Vec::with_capacity(200),
                nulls: MapNullState::new(),
                word_cache: HashMap::with_capacity(200),
                op,
            })
        } else {
            plan_err!("Invalid datatype for StringGroupMapAccumulator {map_type:?}")
        }
    }

    /// Makes a map builder type suitable for this accumulator. The runtime inner type
    /// of the map this accumulator is working with is used as the basis to determine the
    /// types of the builder that are placed in the returned [`MapBuilder`].
    ///
    /// # Panics
    /// If an invalid map type is found. This condition shouldn't occur as it is checked
    /// upon construction.
    fn make_map_builder(&self, cap: usize) -> MapBuilder<StringBuilder, VBuilder> {
        match &self.inner_field_type {
            DataType::Struct(fields) => {
                let names = MapFieldNames {
                    key: fields[0].name().clone(),
                    value: fields[1].name().clone(),
                    entry: "key_value".into(),
                };
                let key_builder = StringBuilder::with_capacity(cap, 1024);
                let value_builder = VBuilder::default();
                MapBuilder::with_capacity(Some(names), key_builder, value_builder, cap)
                    .with_keys_field(fields[0].clone())
                    .with_values_field(fields[1].clone())
            }
            _ => unreachable!(
                "Invalid datatype inside StringGroupMapAccumulator {:?}",
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
fn update_string_map_group<VBuilder>(
    input: Option<&StructArray>,
    map: &mut HashMap<
        usize,
        <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native,
        BuildNoHashHasher<usize>,
    >,
    word_cache: &mut HashMap<String, usize>,
    words: &mut Vec<String>,
    op: &MapAggregatorOp,
) where
    VBuilder: ArrayBuilder + PrimBuilderType,
    <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native: NumAssign + Ord,
{
    if let Some(entries) = input {
        let map_keys = entries.column(0).as_string::<i32>();
        let map_vals = entries
            .column(1)
            .as_primitive::<<VBuilder as PrimBuilderType>::ArrowType>();
        for (k, v) in map_keys.iter().zip(map_vals) {
            match (k, v) {
                (Some(key), Some(value)) => {
                    let i = word_cache.entry_ref(key).or_insert_with(|| {
                        words.push(key.into());
                        words.len() - 1
                    });
                    map.entry(*i)
                        .and_modify(|current_value| *current_value = op.op(*current_value, value))
                        .or_insert(value);
                }
                _ => panic!("Nullable entries aren't supported"),
            }
        }
    }
}

impl<VBuilder> GroupsAccumulator for StringGroupMapAccumulator<VBuilder>
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
            return exec_err!("StringGroupMapAccumulator only accepts single column input");
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
                update_string_map_group::<VBuilder>(
                    Some(val).as_ref(),
                    agg_map,
                    &mut self.word_cache,
                    &mut self.words,
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
                    builder.keys().append_value(self.words[*key].clone());
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
        let mut grouper = StringGroupMapAccumulator::<VBuilder>::try_new(
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

/// An enhanced accumulator for maps of primitive values that implements [`GroupAccumulator`].
#[derive(Debug)]
pub struct ByteGroupMapAccumulator<VBuilder>
where
    VBuilder: ArrayBuilder + PrimBuilderType,
{
    inner_field_type: DataType,
    word_cache: HashMap<Vec<u8>, usize>,
    words: Vec<Vec<u8>>,
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
                    "ByteGroupMapAccumulator inner field type should be a DataType::Struct"
                );
            };
            Ok(Self {
                inner_field_type: field.data_type().clone(),
                group_maps: Vec::with_capacity(1000),
                words: Vec::with_capacity(200),
                nulls: MapNullState::new(),
                word_cache: HashMap::with_capacity(200),
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
/// This implementation is for maps with byte keys and primitive values.
///
/// All nulls keys/values are skipped over.
fn update_byte_map_group<VBuilder>(
    input: Option<&StructArray>,
    map: &mut HashMap<
        usize,
        <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native,
        BuildNoHashHasher<usize>,
    >,
    word_cache: &mut HashMap<Vec<u8>, usize>,
    words: &mut Vec<Vec<u8>>,
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
                    let i = word_cache.entry_ref(key).or_insert_with(|| {
                        words.push(key.into());
                        words.len() - 1
                    });
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
                update_byte_map_group::<VBuilder>(
                    Some(val).as_ref(),
                    agg_map,
                    &mut self.word_cache,
                    &mut self.words,
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
                    builder.keys().append_value(self.words[*key].clone());
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
                Vec<u8>,
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
