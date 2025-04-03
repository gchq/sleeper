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
/// Given a "`map_sum`" aggregator configured, the query `SELECT Key, sum(Total), map_sum(Items) FROM table` we would get:
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
use std::fmt::Debug;

use crate::datafusion::functions::map_accumulator::ByteMapAccumulator;

use super::map_accumulator::StringMapAccumulator;
use arrow::{
    array::{downcast_primitive, ArrowPrimitiveType, PrimitiveBuilder},
    datatypes::DataType,
};
use datafusion::{
    common::{exec_err, internal_err, plan_err},
    error::Result,
    logical_expr::{
        function::AccumulatorArgs, utils::AggregateOrderSensitivity, Accumulator, AggregateUDFImpl,
        GroupsAccumulator, ReversedUDAF, SetMonotonicity, Signature, Volatility,
    },
};

/// Trait to allow all `PrimitiveBuilder` types to be used as builders in evaluate function in accumulator implementations.
pub trait PrimBuilderType: Default + Debug {
    /// The Arrow data type that contains associated types.
    type ArrowType: ArrowPrimitiveType;
    /// Allow access of underlying `append_value` function.
    fn append_value(&mut self, v: &<Self::ArrowType as ArrowPrimitiveType>::Native);
}

impl<T: ArrowPrimitiveType + Debug> PrimBuilderType for PrimitiveBuilder<T> {
    type ArrowType = T;
    fn append_value(&mut self, v: &<Self::ArrowType as ArrowPrimitiveType>::Native) {
        self.append_value(*v);
    }
}

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
    /// If the given column is not a map column.
    pub fn try_new(column_type: &DataType) -> Result<Self> {
        if matches!(column_type, DataType::Map(_, _)) {
            Ok(Self {
                signature: Signature::exact(vec![column_type.clone()], Volatility::Immutable),
            })
        } else {
            plan_err!("MapAggregator can only be used on Map column types")
        }
    }
}

impl AggregateUDFImpl for MapAggregator {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "map_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() == 1 {
            Ok(arg_types[0].clone())
        } else {
            internal_err!("MapAggregator expects a single column of Map type")
        }
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let DataType::Map(field, _) = acc_args.return_type else {
            return exec_err!("MapAggregator can only be used on Map column types");
        };
        let DataType::Struct(struct_fields) = field.data_type() else {
            return exec_err!("MapAggregator Map field is not a Struct type!");
        };
        if struct_fields.len() != 2 {
            return exec_err!("MapAggregator Map inner struct length is not 2");
        }
        // This macros adapted from https://github.com/apache/datafusion/blob/main/datafusion/functions-aggregate/src/sum.rs
        macro_rules! helper {
            ($t:ty, $acc:ident, $dt:expr) => {
                Ok(Box::new($acc::<PrimitiveBuilder<$t>>::try_new($dt)?))
            };
        }
        let value_type = struct_fields[1].data_type();
        let map_type = acc_args.return_type;

        match struct_fields[0].data_type() {
            DataType::Utf8 => downcast_primitive! {
                value_type => (helper, StringMapAccumulator, map_type),
                _ => unreachable!()
            },
            DataType::Binary => downcast_primitive! {
                value_type => (helper, ByteMapAccumulator, map_type),
                _ => unreachable!()
            },
            _ => exec_err!("MapAggregator can only process maps with String keys!"),
        }
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
