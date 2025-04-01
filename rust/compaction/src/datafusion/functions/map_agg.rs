#![allow(unused_imports)]
/// Implementation of [`AggregateUDFImpl`] for aggregating maps of integers.
/// For example, given:
///
/// | Key |Total | Items |
/// |--|--|--|
/// | a | 6 | { 'k1' = 2, 'k2' = 1, 'k3' = 4} |
/// | a | 3 | { 'k2' = 2, 'k4' = 1} |
/// | b | 1 | { 'k1' = 1 } |
/// | b | 2 | { 'k1' = 1, 'k3' = 1 } |
/// | ....|
///
///
/// Given a "map_sum" aggregator configured, the query `SELECT Key, sum(Total), map_sum(Items) FROM table` we would get:
/// | Key |Total | Items |
/// |--|--|--|
/// | a | 9 | { 'k1' = 2, 'k2' = 3, 'k3' = 4, 'k4' = 1} |
/// | b | 3 | { 'k1' = 2, 'k3 = 1 } |
/// | ....|
///
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
use std::{any::Any, fmt::Debug, hash::Hash, marker::PhantomData, ops::AddAssign, sync::Arc};

use arrow::{
    array::{
        downcast_array, make_builder, ArrayAccessor, ArrayBuilder, ArrayData, ArrayIter, ArrayRef,
        ArrowPrimitiveType, AsArray, GenericByteBuilder, Int64Builder, MapBuilder, PrimitiveArray,
        PrimitiveBuilder, StringArray, StringBuilder, StructArray,
    },
    datatypes::{ByteArrayType, DataType, Field, Int64Type},
};
use datafusion::{
    common::{exec_err, internal_err, not_impl_err, HashMap},
    error::Result,
    logical_expr::{
        function::AccumulatorArgs, utils::AggregateOrderSensitivity, Accumulator, AggregateUDFImpl,
        GroupsAccumulator, ReversedUDAF, SetMonotonicity, Signature, Volatility,
    },
    scalar::ScalarValue,
};

/// A aggregator for map columns. See module documentation.
#[derive(Debug)]
pub struct MapAggregator {
    /// Defines what column types this function can work on
    signature: Signature,
}

impl MapAggregator {
    /// Create a map aggregation function for the given column.
    ///
    /// # Errors
    /// If the given column is not a map column
    pub fn new(column_type: &DataType) -> Result<Self> {
        if !matches!(column_type, DataType::Map(_, _)) {
            internal_err!("MapAggregator can only be used on Map column types")
        } else {
            Ok(Self {
                signature: Signature::exact(vec![column_type.clone()], Volatility::Immutable),
            })
        }
    }
}

impl AggregateUDFImpl for MapAggregator {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "map_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            internal_err!("MapAggregator expects a single column of Map type")
        } else {
            Ok(arg_types[0].clone())
        }
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(super::accumulator::ByteMapAccumulator::<
            Int64Builder,
        >::new(acc_args.return_type)?))
    }

    fn is_nullable(&self) -> bool {
        true
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        false
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        // Ok(Box::new(GroupMapAccumulator::new()))
        unimplemented!();
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        // Aggregation doesn't have an opinion on ordering
        AggregateOrderSensitivity::Insensitive
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    /// Although the aggregations of individual map values may increase or decrease if signed integers are used,
    /// the size of the maps will only increase as more map keys are aggregated in, hence
    /// this function is monotonically increasing.
    fn set_monotonicity(&self, _data_type: &DataType) -> SetMonotonicity {
        SetMonotonicity::Increasing
    }
}

// fn update_string_map<'a, VBuilder>(
//     input: &'a Option<StructArray>,
//     map: &mut HashMap<String, <<VBuilder as AppendValue>::ArrowType as ArrowPrimitiveType>::Native>,
// ) where
//     VBuilder: ArrayBuilder + Debug + AppendValue,
//     <VBuilder as AppendValue>::ArrowType: ArrowPrimitiveType,
//     <<VBuilder as AppendValue>::ArrowType as ArrowPrimitiveType>::Native: AddAssign,
// {
//     if let Some(entries) = input {
//         let col1 = entries.column(0).as_string::<i32>();
//         let col2 = entries
//             .column(1)
//             .as_primitive::<<VBuilder as AppendValue>::ArrowType>();
//         for (k, v) in col1.into_iter().zip(col2) {
//             match (k, v) {
//                 (Some(key), Some(value)) => {
//                     map.entry_ref(key)
//                         .and_modify(|v| *v += value)
//                         .or_insert(value);
//                 }
//                 _ => panic!("Nullable entries aren't supported"),
//             }
//         }
//     }
// }

// #[derive(Debug, Default)]
// struct GroupMapAccumulator {
//     word_cache: HashMap<String, usize>,
//     words: Vec<String>,
//     group_maps: Vec<HashMap<usize, i64, BuildNoHashHasher<usize>>>,
//     nulls: MapNullState,
// }

// impl GroupMapAccumulator {
//     pub fn new() -> Self {
//         Self {
//             group_maps: Vec::with_capacity(10000),
//             words: Vec::with_capacity(200),
//             nulls: MapNullState::new(),
//             word_cache: HashMap::with_capacity(200),
//         }
//     }
// }

// fn update_map_group(
//     input: &Option<StructArray>,
//     map: &mut HashMap<usize, i64, BuildNoHashHasher<usize>>,
//     word_cache: &mut HashMap<String, usize>,
//     words: &mut Vec<String>,
// ) {
//     if let Some(entries) = input {
//         let map_keys = entries.column(0).as_string::<i32>();
//         let map_vals = entries.column(1).as_primitive::<Int64Type>();
//         for (k, v) in map_keys.iter().zip(map_vals) {
//             match (k, v) {
//                 (Some(key), Some(value)) => {
//                     let i = word_cache.entry_ref(key).or_insert_with(|| {
//                         words.push(key.into());
//                         words.len() - 1
//                     });
//                     map.entry(*i).and_modify(|v| *v += value).or_insert(value);
//                 }
//                 _ => panic!("Nullable entries aren't supported"),
//             }
//         }
//     }
// }

// impl GroupsAccumulator for GroupMapAccumulator {
//     fn update_batch(
//         &mut self,
//         values: &[ArrayRef],
//         group_indices: &[usize],
//         opt_filter: Option<&BooleanArray>,
//         total_num_groups: usize,
//     ) -> Result<()> {
//         if values.len() != 1 {
//             return Err(DataFusionError::Execution(
//                 "GroupMapAccumulator only accepts single column input".into(),
//             ));
//         }
//         let data = values[0].as_map();
//         // make sure we have room for the groups count
//         self.group_maps.resize(total_num_groups, Default::default());
//         self.nulls.accumulate(
//             group_indices,
//             data,
//             opt_filter,
//             total_num_groups,
//             |g_idx, val| {
//                 let agg_map = &mut self.group_maps[g_idx];
//                 update_map_group(&Some(val), agg_map, &mut self.word_cache, &mut self.words);
//             },
//         );
//         Ok(())
//     }

//     fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
//         let result = emit_to.take_needed(&mut self.group_maps);
//         let nulls = self.nulls.build(emit_to);
//         assert_eq!(result.len(), nulls.len());
//         // if nulls are present in data, we have to make sure we don't try to sum them
//         let mut builder = MapBuilder::new(
//             Some(MapFieldNames {
//                 entry: "key_value".into(),
//                 key: "key".into(),
//                 value: "value".into(),
//             }),
//             StringBuilder::new(),
//             Int64Builder::new(),
//         )
//         .with_values_field(Field::new("value", DataType::Int64, false));
//         for (v, is_valid) in result.into_iter().zip(nulls.iter()) {
//             if is_valid {
//                 for (key, val) in &v {
//                     builder.keys().append_value(self.words[*key].clone());
//                     builder.values().append_value(*val);
//                 }
//                 builder.append(true).expect("Can't finish Map");
//             } else {
//                 builder.append(false).expect("Can't finish Map");
//             }
//         }
//         Ok(Arc::new(builder.finish()))
//     }

//     fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
//         self.evaluate(emit_to).map(|arr| vec![arr])
//     }

//     fn merge_batch(
//         &mut self,
//         values: &[ArrayRef],
//         group_indices: &[usize],
//         opt_filter: Option<&BooleanArray>,
//         total_num_groups: usize,
//     ) -> Result<()> {
//         self.update_batch(values, group_indices, opt_filter, total_num_groups)
//     }

//     fn size(&self) -> usize {
//         (std::mem::size_of::<HashMap<String, i64>>() * self.group_maps.capacity())
//             + self.nulls.size()
//     }

//     fn convert_to_state(
//         &self,
//         values: &[ArrayRef],
//         opt_filter: Option<&BooleanArray>,
//     ) -> Result<Vec<ArrayRef>> {
//         let mut grouper = GroupMapAccumulator::new();
//         grouper.update_batch(
//             values,
//             &(0usize..values[0].len()).collect::<Vec<_>>(),
//             opt_filter,
//             values[0].len(),
//         )?;
//         grouper.state(EmitTo::All)
//     }

//     fn supports_convert_to_state(&self) -> bool {
//         true
//     }
// }
