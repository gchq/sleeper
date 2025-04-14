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
use crate::datafusion::functions::{
    map_accumulator::{ByteMapAccumulator, PrimMapAccumulator, StringMapAccumulator},
    map_group_accumulator::{
        ByteGroupMapAccumulator, PrimGroupMapAccumulator, StringGroupMapAccumulator,
    },
};
use arrow::{
    array::{ArrowPrimitiveType, PrimitiveBuilder, downcast_integer},
    datatypes::{DataType, Fields},
};
use datafusion::{
    common::{exec_err, internal_err, plan_err},
    error::Result,
    logical_expr::{
        Accumulator, AggregateUDFImpl, GroupsAccumulator, ReversedUDAF, SetMonotonicity, Signature,
        Volatility, function::AccumulatorArgs, utils::AggregateOrderSensitivity,
    },
};
use num_traits::NumAssign;
use std::fmt::Debug;

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

/// Check that the struct type from a map type is compatible.
///
/// # Errors
/// Produces an execution error if the field is not a struct type with 2 inner fields.
fn validate_map_struct_type<'a>(acc_args: &'a AccumulatorArgs<'_>) -> Result<&'a Fields> {
    let DataType::Map(field, _) = acc_args.return_type else {
        return exec_err!("MapAggregator can only be used on Map column types");
    };
    let DataType::Struct(struct_fields) = field.data_type() else {
        return exec_err!("MapAggregator Map field is not a Struct type!");
    };
    if struct_fields.len() != 2 {
        return exec_err!("MapAggregator Map inner struct length is not 2");
    }
    Ok(struct_fields)
}

/// The aggregation operation to peform inside of each map. The values
/// of identical keys will be aggregated according to the specified operation.
#[derive(Debug, Clone)]
pub enum MapAggregatorOp {
    Sum,
    Count,
    Min,
    Max,
}

impl MapAggregatorOp {
    pub fn op<T>(&self, acc: T, value: T) -> T
    where
        T: NumAssign + Ord,
    {
        match self {
            Self::Sum => acc + value,
            Self::Count => acc.add(T::one()),
            Self::Min => std::cmp::min(acc, value),
            Self::Max => std::cmp::max(acc, value),
        }
    }
}

/// A aggregator for map columns. See module documentation.
#[derive(Debug)]
pub struct MapAggregator {
    /// Defines what column types this function can work on
    signature: Signature,
    /// What sort of aggregation operation to perform
    op: MapAggregatorOp,
}

impl MapAggregator {
    /// Create a map aggregation function for the given column.
    ///
    /// # Errors
    /// If the given column is not a map column.
    pub fn try_new(column_type: &DataType, op: MapAggregatorOp) -> Result<Self> {
        if matches!(column_type, DataType::Map(_, _)) {
            Ok(Self {
                signature: Signature::exact(vec![column_type.clone()], Volatility::Immutable),
                op,
            })
        } else {
            plan_err!("MapAggregator can only be used on Map column types")
        }
    }
}
// This macros adapted from https://github.com/apache/datafusion/blob/main/datafusion/functions-aggregate/src/sum.rs
macro_rules! value_only_helper {
    ($t: ty, $acc: ident, $dt: expr, $op: expr) => {
        Ok(Box::new($acc::<PrimitiveBuilder<$t>>::try_new($dt, $op)?))
    };
}

macro_rules! value_type_helper {
    ($value_type: ty, $key_type: ty, $dt: expr, $op: expr) => {{
        Ok(Box::new(PrimMapAccumulator::<
            PrimitiveBuilder<$key_type>,
            PrimitiveBuilder<$value_type>,
        >::try_new($dt, $op)?))
    }};
}

macro_rules! key_type_helper {
    ($key_type: ty, $value_datatype: expr, $dt: expr, $op: expr) => {{
        downcast_integer! {
            $value_datatype => (value_type_helper, $key_type, $dt, $op),
            _ => unreachable!()
        }
    }};
}

macro_rules! value_type_group_helper {
    ($value_type: ty, $key_type: ty, $dt: expr, $op: expr) => {{
        Ok(Box::new(PrimGroupMapAccumulator::<
            PrimitiveBuilder<$key_type>,
            PrimitiveBuilder<$value_type>,
        >::try_new($dt, $op)?))
    }};
}

macro_rules! key_type_group_helper {
    ($key_type: ty, $value_datatype: expr, $dt: expr, $op: expr) => {{
        downcast_integer! {
            $value_datatype => (value_type_group_helper, $key_type, $dt, $op),
            _ => unreachable!()
        }
    }};
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
        let struct_fields = validate_map_struct_type(&acc_args)?;

        let key_type = struct_fields[0].data_type();
        let value_type = struct_fields[1].data_type();
        let map_type = acc_args.return_type;
        let op_type = self.op.clone();

        if key_type.is_integer() {
            downcast_integer! {
                key_type => (key_type_helper, value_type, map_type, op_type),
                _ => unreachable!()
            }
        } else {
            match key_type {
                DataType::Utf8 => downcast_integer! {
                    value_type => (value_only_helper, StringMapAccumulator, map_type, op_type),
                    _ => unreachable!()
                },
                DataType::Binary => downcast_integer! {
                    value_type => (value_only_helper, ByteMapAccumulator, map_type, op_type),
                    _ => unreachable!()
                },
                _ => exec_err!("MapAggregator can't process this data type {map_type:?}"),
            }
        }
    }

    fn is_nullable(&self) -> bool {
        true
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let struct_fields = validate_map_struct_type(&args)?;

        let key_type = struct_fields[0].data_type();
        let value_type = struct_fields[1].data_type();
        let map_type = args.return_type;
        let op_type = self.op.clone();

        if key_type.is_integer() {
            downcast_integer! {
                key_type => (key_type_group_helper, value_type, map_type, op_type),
                _ => unreachable!()
            }
        } else {
            match key_type {
                DataType::Utf8 => downcast_integer! {
                    value_type => (value_only_helper, StringGroupMapAccumulator, map_type, op_type),
                    _ => unreachable!()
                },
                DataType::Binary => downcast_integer! {
                    value_type => (value_only_helper, ByteGroupMapAccumulator, map_type, op_type),
                    _ => unreachable!()
                },
                _ => exec_err!("MapAggregator can't process this data type {map_type:?}"),
            }
        }
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
