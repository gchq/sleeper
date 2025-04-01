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
        make_builder, ArrayBuilder, ArrayRef, ArrowPrimitiveType, AsArray, GenericByteBuilder,
        MapBuilder, PrimitiveBuilder, StringBuilder, StructArray,
    },
    datatypes::{ByteArrayType, DataType},
};
use datafusion::{
    common::{exec_err, internal_err, HashMap},
    error::Result,
    logical_expr::Accumulator,
    scalar::ScalarValue,
};
use std::{fmt::Debug, marker::PhantomData, ops::AddAssign, sync::Arc};
use std::{hash::Hash, ops::Deref};

/// Trait to allow all `PrimitiveBuilder` types to be used as builders in evaluate function in accumulator implementations.
pub trait PrimBuilderType {
    /// The Arrow data type that contains associated types.
    type ArrowType: ArrowPrimitiveType;
    /// Allow access of underlying append_value function.
    fn append_value(&mut self, v: &<Self::ArrowType as ArrowPrimitiveType>::Native);
}

impl<T: ArrowPrimitiveType> PrimBuilderType for PrimitiveBuilder<T> {
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
    KBuilder: ArrayBuilder + Debug + PrimBuilderType,
    VBuilder: ArrayBuilder + Debug + PrimBuilderType,
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
    KBuilder: ArrayBuilder + Debug + PrimBuilderType,
    VBuilder: ArrayBuilder + Debug + PrimBuilderType,
{
    map_type: DataType,
    values: HashMap<
        <<KBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native,
        <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native,
    >,
    _p: PhantomData<KBuilder>,
    _p2: PhantomData<VBuilder>,
}

impl<KBuilder, VBuilder> PrimMapAccumulator<KBuilder, VBuilder>
where
    KBuilder: ArrayBuilder + Debug + PrimBuilderType,
    VBuilder: ArrayBuilder + Debug + PrimBuilderType,
{
    // Creates a new accumulator.
    //
    // The type of the map must be specified so that the correct sort
    // of map builder can be created.
    pub fn new(map_type: &DataType) -> Result<Self> {
        if !matches!(*map_type, DataType::Map(_, _)) {
            internal_err!("Invalid datatype for primitive map accumulator {map_type:?}")
        } else {
            Ok(Self {
                map_type: map_type.clone(),
                values: HashMap::default(),
                _p: PhantomData,
                _p2: PhantomData,
            })
        }
    }
}

impl<KBuilder, VBuilder> Accumulator for PrimMapAccumulator<KBuilder, VBuilder>
where
    KBuilder: ArrayBuilder + Debug + PrimBuilderType,
    VBuilder: ArrayBuilder + Debug + PrimBuilderType,
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
        let mut build_binding = make_builder(&self.map_type, self.values.len());
        let builder = build_binding
            .as_any_mut()
            .downcast_mut::<MapBuilder<KBuilder, VBuilder>>()
            .expect("Builder downcast failed");
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
    VBuilder: ArrayBuilder + Debug + PrimBuilderType,
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
    VBuilder: ArrayBuilder + Debug + PrimBuilderType,
{
    map_type: DataType,
    values:
        HashMap<String, <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native>,
    _p2: PhantomData<VBuilder>,
}

impl<VBuilder> StringMapAccumulator<VBuilder>
where
    VBuilder: ArrayBuilder + Debug + PrimBuilderType,
{
    // Creates a new accumulator.
    //
    // The type of the map must be specified so that the correct sort
    // of map builder can be created.
    pub fn new(map_type: &DataType) -> Result<Self> {
        if !matches!(*map_type, DataType::Map(_, _)) {
            internal_err!("Invalid datatype for string map accumulator {map_type:?}")
        } else {
            Ok(Self {
                map_type: map_type.clone(),
                values: HashMap::default(),
                _p2: PhantomData,
            })
        }
    }
}

impl<VBuilder> Accumulator for StringMapAccumulator<VBuilder>
where
    VBuilder: ArrayBuilder + Debug + PrimBuilderType,
    <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native: AddAssign,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 1 {
            return exec_err!("MapAccumulator only accepts single column input");
        }

        let input = values[0].as_map();
        // For each map we get, feed it into our internal aggregated map
        for map in input.iter() {
            update_string_map::<VBuilder>(&map, &mut self.values);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut build_binding = make_builder(&self.map_type, self.values.len());
        let builder = build_binding
            .as_any_mut()
            .downcast_mut::<MapBuilder<StringBuilder, VBuilder>>()
            .expect("Builder downcast failed");
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
