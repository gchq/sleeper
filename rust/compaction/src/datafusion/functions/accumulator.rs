/// Simple ['Accumulator`] implementations for map aggregation.
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
    array::{
        ArrayBuilder, ArrayRef, ArrowPrimitiveType, AsArray, BinaryBuilder, MapBuilder,
        MapFieldNames, PrimitiveBuilder, StringBuilder, StructArray,
    },
    datatypes::DataType,
};
use datafusion::{
    common::{exec_err, internal_err, HashMap},
    error::Result,
    logical_expr::Accumulator,
    scalar::ScalarValue,
};
use std::{fmt::Debug, hash::Hash, marker::PhantomData, ops::AddAssign, sync::Arc};

/// Trait to allow all `PrimitiveBuilder` types to be used as builders in evaluate function in accumulator implementations.
pub trait PrimBuilderType: Default + Debug {
    /// The Arrow data type that contains associated types.
    type ArrowType: ArrowPrimitiveType;
    /// Allow access of underlying append_value function.
    fn append_value(&mut self, v: &<Self::ArrowType as ArrowPrimitiveType>::Native);
}

impl<T: ArrowPrimitiveType + Debug> PrimBuilderType for PrimitiveBuilder<T> {
    type ArrowType = T;
    fn append_value(&mut self, v: &<Self::ArrowType as ArrowPrimitiveType>::Native) {
        self.append_value(*v);
    }
}

/// Given an Arrow [`StructArray`] of keys and values, update the given map.
///
/// This implementation is for maps with primitive keys and values.
///
/// All nulls keys/values are skipped over.
fn update_primitive_map<KBuilder, VBuilder>(
    input: &Option<StructArray>,
    map: &mut HashMap<
        <<KBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native,
        <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native,
    >,
) where
    KBuilder: ArrayBuilder + PrimBuilderType,
    VBuilder: ArrayBuilder + PrimBuilderType,
    <<KBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native: Hash + Eq,
    <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native: AddAssign,
{
    if let Some(entries) = input {
        let col1 = entries
            .column(0)
            .as_primitive::<<KBuilder as PrimBuilderType>::ArrowType>();
        let col2 = entries
            .column(1)
            .as_primitive::<<VBuilder as PrimBuilderType>::ArrowType>();
        for (k, v) in col1.into_iter().zip(col2) {
            match (k, v) {
                (Some(key), Some(value)) => {
                    map.entry(key).and_modify(|v| *v += value).or_insert(value);
                }
                _ => panic!("Nullable entries aren't supported"),
            }
        }
    }
}

/// Single value primitive accumulator function for maps.
#[derive(Debug)]
pub struct PrimMapAccumulator<KBuilder, VBuilder>
where
    KBuilder: ArrayBuilder + PrimBuilderType,
    VBuilder: ArrayBuilder + PrimBuilderType,
{
    inner_field_type: DataType,
    values: HashMap<
        <<KBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native,
        <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native,
    >,
    _p: PhantomData<KBuilder>,
    _p2: PhantomData<VBuilder>,
}

impl<KBuilder, VBuilder> PrimMapAccumulator<KBuilder, VBuilder>
where
    KBuilder: ArrayBuilder + PrimBuilderType,
    VBuilder: ArrayBuilder + PrimBuilderType,
{
    // Creates a new accumulator.
    //
    // The type of the map must be specified so that the correct sort
    // of map builder can be created.
    pub fn try_new(map_type: &DataType) -> Result<Self> {
        if let DataType::Map(field, _) = map_type {
            let DataType::Struct(_) = field.data_type() else {
                return internal_err!(
                    "PrimMapAccumulator inner field type should be a DataType::Struct"
                );
            };
            Ok(Self {
                inner_field_type: field.data_type().clone(),
                values: HashMap::default(),
                _p: PhantomData,
                _p2: PhantomData,
            })
        } else {
            return internal_err!("Invalid datatype for PrimMapAccumulator {map_type:?}");
        }
    }

    fn make_map_builder(&self, cap: usize) -> Result<MapBuilder<KBuilder, VBuilder>> {
        match &self.inner_field_type {
            DataType::Struct(fields) => {
                let names = MapFieldNames {
                    key: fields[0].name().clone(),
                    value: fields[1].name().clone(),
                    entry: "key_value".into(),
                };
                let key_builder = KBuilder::default();
                let value_builder = VBuilder::default();
                Ok(
                    MapBuilder::with_capacity(Some(names), key_builder, value_builder, cap)
                        .with_keys_field(fields[0].clone())
                        .with_values_field(fields[1].clone()),
                )
            }
            _ => unreachable!(
                "Invalid datatype inside PrimMapAccumulator {:?}",
                self.inner_field_type
            ),
        }
    }
}

impl<KBuilder, VBuilder> Accumulator for PrimMapAccumulator<KBuilder, VBuilder>
where
    KBuilder: ArrayBuilder + PrimBuilderType,
    VBuilder: ArrayBuilder + PrimBuilderType,
    <<KBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native: Hash + Eq,
    <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native: AddAssign,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 1 {
            return exec_err!("PrimMapAccumulator only accepts single column input");
        }

        let input = values[0].as_map();
        // For each map we get, feed it into our internal aggregated map
        for map in input.iter() {
            update_primitive_map::<KBuilder, VBuilder>(&map, &mut self.values);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut builder = self.make_map_builder(self.values.len())?;
        for (key, val) in &self.values {
            builder.keys().append_value(key);
            builder.values().append_value(val);
        }
        builder.append(true).expect("Can't finish MapBuilder");
        Ok(ScalarValue::Map(Arc::new(builder.finish())))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.evaluate().map(|e| vec![e])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }
}

/// Given an Arrow [`StructArray`] of keys and values, update the given map.
///
/// This implementation is for maps with string keys and primitive values.
///
/// All nulls keys/values are skipped over.
fn update_string_map<VBuilder>(
    input: &Option<StructArray>,
    map: &mut HashMap<
        String,
        <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native,
    >,
) where
    VBuilder: ArrayBuilder + PrimBuilderType,
    <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native: AddAssign,
{
    if let Some(entries) = input {
        let col1 = entries.column(0).as_string::<i32>();
        let col2 = entries
            .column(1)
            .as_primitive::<<VBuilder as PrimBuilderType>::ArrowType>();
        for (k, v) in col1.into_iter().zip(col2) {
            match (k, v) {
                (Some(key), Some(value)) => {
                    map.entry_ref(key)
                        .and_modify(|v| *v += value)
                        .or_insert(value);
                }
                _ => panic!("Nullable entries aren't supported"),
            }
        }
    }
}

/// Single value primitive accumulator function for maps.
#[derive(Debug)]
pub struct StringMapAccumulator<VBuilder>
where
    VBuilder: ArrayBuilder + PrimBuilderType,
{
    inner_field_type: DataType,
    values:
        HashMap<String, <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native>,
    _p: PhantomData<VBuilder>,
}

impl<VBuilder> StringMapAccumulator<VBuilder>
where
    VBuilder: ArrayBuilder + PrimBuilderType,
{
    // Creates a new accumulator.
    //
    // The type of the map must be specified so that the correct sort
    // of map builder can be created.
    pub fn try_new(map_type: &DataType) -> Result<Self> {
        if let DataType::Map(field, _) = map_type {
            let DataType::Struct(_) = field.data_type() else {
                return internal_err!(
                    "StringMapAccumulator inner field type should be a DataType::Struct"
                );
            };
            Ok(Self {
                inner_field_type: field.data_type().clone(),
                values: HashMap::default(),
                _p: PhantomData,
            })
        } else {
            return internal_err!("Invalid datatype for StringMapAccumulator {map_type:?}");
        }
    }

    fn make_map_builder(&self, cap: usize) -> Result<MapBuilder<StringBuilder, VBuilder>> {
        match &self.inner_field_type {
            DataType::Struct(fields) => {
                let names = MapFieldNames {
                    key: fields[0].name().clone(),
                    value: fields[1].name().clone(),
                    entry: "key_value".into(),
                };
                let key_builder = StringBuilder::with_capacity(cap, 1024);
                let value_builder = VBuilder::default();
                Ok(
                    MapBuilder::with_capacity(Some(names), key_builder, value_builder, cap)
                        .with_keys_field(fields[0].clone())
                        .with_values_field(fields[1].clone()),
                )
            }
            _ => unreachable!(
                "Invalid datatype inside StringMapAccumulator {:?}",
                self.inner_field_type
            ),
        }
    }
}

impl<VBuilder> Accumulator for StringMapAccumulator<VBuilder>
where
    VBuilder: ArrayBuilder + PrimBuilderType,
    <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native: AddAssign,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 1 {
            return exec_err!("StringMapAccumulator only accepts single column input");
        }

        let input = values[0].as_map();
        // For each map we get, feed it into our internal aggregated map
        for map in input.iter() {
            update_string_map::<VBuilder>(&map, &mut self.values);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut builder = self.make_map_builder(self.values.len())?;
        for (key, val) in &self.values {
            builder.keys().append_value(key);
            builder.values().append_value(val);
        }
        builder.append(true).expect("Can't finish MapBuilder");
        Ok(ScalarValue::Map(Arc::new(builder.finish())))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.evaluate().map(|e| vec![e])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }
}

/// Given an Arrow [`StructArray`] of keys and values, update the given map.
///
/// This implementation is for maps with byte keys and primitive values.
///
/// All nulls keys/values are skipped over.
fn update_byte_map<VBuilder>(
    input: &Option<StructArray>,
    map: &mut HashMap<
        Vec<u8>,
        <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native,
    >,
) where
    VBuilder: ArrayBuilder + PrimBuilderType,
    <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native: AddAssign,
{
    if let Some(entries) = input {
        let col1 = entries.column(0).as_binary::<i32>();
        let col2 = entries
            .column(1)
            .as_primitive::<<VBuilder as PrimBuilderType>::ArrowType>();
        for (k, v) in col1.into_iter().zip(col2) {
            match (k, v) {
                (Some(key), Some(value)) => {
                    map.entry_ref(key)
                        .and_modify(|v| *v += value)
                        .or_insert(value);
                }
                _ => panic!("Nullable entries aren't supported"),
            }
        }
    }
}

/// Single value primitive accumulator function for maps.
#[derive(Debug)]
pub struct ByteMapAccumulator<VBuilder>
where
    VBuilder: ArrayBuilder + PrimBuilderType,
{
    inner_field_type: DataType,
    values:
        HashMap<Vec<u8>, <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native>,
    _p: PhantomData<VBuilder>,
}

impl<VBuilder> ByteMapAccumulator<VBuilder>
where
    VBuilder: ArrayBuilder + PrimBuilderType,
{
    // Creates a new accumulator.
    //
    // The type of the map must be specified so that the correct sort
    // of map builder can be created.
    pub fn try_new(map_type: &DataType) -> Result<Self> {
        if let DataType::Map(field, _) = map_type {
            let DataType::Struct(_) = field.data_type() else {
                return internal_err!(
                    "ByteMapAccumulator inner field type should be a DataType::Struct"
                );
            };
            Ok(Self {
                inner_field_type: field.data_type().clone(),
                values: HashMap::default(),
                _p: PhantomData,
            })
        } else {
            return internal_err!("Invalid datatype for ByteMapAccumulator {map_type:?}");
        }
    }

    fn make_map_builder(&self, cap: usize) -> Result<MapBuilder<BinaryBuilder, VBuilder>> {
        match &self.inner_field_type {
            DataType::Struct(fields) => {
                let names = MapFieldNames {
                    key: fields[0].name().clone(),
                    value: fields[1].name().clone(),
                    entry: "key_value".into(),
                };
                let key_builder = BinaryBuilder::with_capacity(cap, 1024);
                let value_builder = VBuilder::default();
                Ok(
                    MapBuilder::with_capacity(Some(names), key_builder, value_builder, cap)
                        .with_keys_field(fields[0].clone())
                        .with_values_field(fields[1].clone()),
                )
            }
            _ => unreachable!(
                "Invalid datatype inside ByteMapAccumulator {:?}",
                self.inner_field_type
            ),
        }
    }
}

impl<VBuilder> Accumulator for ByteMapAccumulator<VBuilder>
where
    VBuilder: ArrayBuilder + PrimBuilderType,
    <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native: AddAssign,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 1 {
            return exec_err!("ByteMapAccumulator only accepts single column input");
        }

        let input = values[0].as_map();
        // For each map we get, feed it into our internal aggregated map
        for map in input.iter() {
            update_byte_map::<VBuilder>(&map, &mut self.values);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut builder = self.make_map_builder(self.values.len())?;
        for (key, val) in &self.values {
            builder.keys().append_value(key);
            builder.values().append_value(val);
        }
        builder.append(true).expect("Can't finish MapBuilder");
        Ok(ScalarValue::Map(Arc::new(builder.finish())))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.evaluate().map(|e| vec![e])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }
}
