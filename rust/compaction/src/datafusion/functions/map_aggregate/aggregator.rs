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
    MapAggregatorOp,
    map_aggregate::{
        accumulator::map_accumulator::{
            ByteMapAccumulator, PrimMapAccumulator, StringMapAccumulator,
        },
        group_accumulator::map_group_accumulator::{
            ByteGroupMapAccumulator, PrimGroupMapAccumulator, StringGroupMapAccumulator,
        },
    },
};
use arrow::{
    array::{ArrowPrimitiveType, PrimitiveBuilder, downcast_integer},
    datatypes::{DataType, Fields},
};
use datafusion::{
    common::{exec_err, plan_err},
    error::Result,
    logical_expr::{
        Accumulator, AggregateUDFImpl, GroupsAccumulator, ReversedUDAF, SetMonotonicity, Signature,
        Volatility, function::AccumulatorArgs, utils::AggregateOrderSensitivity,
    },
};
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
        return exec_err!("MapAggregator Map field is not a Struct type");
    };
    if struct_fields.len() != 2 {
        return exec_err!("MapAggregator Map inner struct length is not 2");
    }
    Ok(struct_fields)
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

/// Used with [`downcast_integer`] to take an Arrow datatype and create the named map accumulator with a [`PrimitiveBuilder`]
/// given value type provided as the `$t` argument.
macro_rules! value_only_helper {
    // This macros adapted from https://github.com/apache/datafusion/blob/main/datafusion/functions-aggregate/src/sum.rs
    ($t: ty, $acc: ident, $dt: expr, $op: expr) => {
        Ok(Box::new($acc::<PrimitiveBuilder<$t>>::try_new($dt, $op)?))
    };
}

/* The following is a two level `downcast_integer` macro expansion. Since `PrimitiveMapAccumulator` needs two generic arguments,
 * one for the key and one for the value of the map. We can only expand one thing at once with `downcast_integer`, so the outer
 * call is used with `key_type_helper` which contains a second downcast_integer call with `value_type_helper` which then
 * has both the key and value types available so can expand into the final Rust code.
 */

/// Used by [`key_type_helper`], this expands into creating a class specified with `$acc` with key type `$key_type` and value
/// type `$value_type`. `$acc` should be either [`PrimMapAccumulator`] or [`PrimGroupMapAccumulator`].
macro_rules! value_type_helper {
    ($value_type: ty, $key_type: ty, $acc: ident, $dt: expr, $op: expr) => {{
        Ok(Box::new($acc::<
            PrimitiveBuilder<$key_type>,
            PrimitiveBuilder<$value_type>,
        >::try_new($dt, $op)?))
    }};
}

/// Used by [`downcast_integer`] to take a Arrow datatype and recursively call [`value_type_helper`] inside another
/// [`downcast_integer`] to determine the value datatype of the map.
macro_rules! key_type_helper {
    ($key_type: ty, $value_datatype: expr, $acc: ident, $dt: expr, $op: expr) => {{
        downcast_integer! {
            $value_datatype => (value_type_helper, $key_type, $acc, $dt, $op),
            _ => exec_err!("MapAggregator value type must be an integer type not {}", $value_datatype)
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
            plan_err!("MapAggregator expects a single column of Map type")
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
                key_type => (key_type_helper, value_type, PrimMapAccumulator, map_type, op_type),
                _ => exec_err!("MapAggregator value type must be an integer type not {value_type}")
            }
        } else {
            match key_type {
                DataType::Utf8 => downcast_integer! {
                    value_type => (value_only_helper, StringMapAccumulator, map_type, op_type),
                    _ => exec_err!("MapAggregator value type must be an integer type not {value_type}")
                },
                DataType::Binary => downcast_integer! {
                    value_type => (value_only_helper, ByteMapAccumulator, map_type, op_type),
                    _ => exec_err!("MapAggregator value type must be an integer type not {value_type}")
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
                key_type => (key_type_helper, value_type, PrimGroupMapAccumulator, map_type, op_type),
                _ => exec_err!("MapAggregator value type must be an integer type not {value_type}")
            }
        } else {
            match key_type {
                DataType::Utf8 => downcast_integer! {
                    value_type => (value_only_helper, StringGroupMapAccumulator, map_type, op_type),
                    _ => exec_err!("MapAggregator value type must be an integer type not {value_type}")
                },
                DataType::Binary => downcast_integer! {
                    value_type => (value_only_helper, ByteGroupMapAccumulator, map_type, op_type),
                    _ => exec_err!("MapAggregator value type must be an integer type not {value_type}")
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

#[cfg(test)]
pub mod map_test_common {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Fields};

    pub fn make_map_datatype(key_type: DataType, value_type: DataType) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "map",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", key_type, false),
                    Field::new("value", value_type, false),
                ])),
                false,
            )),
            false,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{
            ArrowPrimitiveType, BinaryBuilder, Int64Builder, MapBuilder, MapFieldNames,
            StringBuilder,
        },
        datatypes::{DataType, Field, Fields, Schema},
    };
    use datafusion::{
        error::DataFusionError,
        logical_expr::{AggregateUDFImpl, function::AccumulatorArgs},
        physical_expr::LexOrdering,
    };

    use crate::{
        assert_error,
        datafusion::functions::map_aggregate::aggregator::map_test_common::make_map_datatype,
    };

    use super::{MapAggregator, MapAggregatorOp, PrimBuilderType, validate_map_struct_type};

    fn append<T>(mut builder: T, v: <T::ArrowType as ArrowPrimitiveType>::Native)
    where
        T: PrimBuilderType,
    {
        builder.append_value(&v);
    }

    #[test]
    fn primitive_type_should_append() {
        // Given
        let intbuilder = Int64Builder::new();

        // Then
        append(intbuilder, 64);
    }

    #[test]
    fn validate_error_on_non_map_type() {
        // Given
        let args = AccumulatorArgs {
            name: "test",
            exprs: &[],
            return_type: &DataType::Int64,
            schema: &Schema::empty(),
            ignore_nulls: true,
            ordering_req: LexOrdering::empty(),
            is_reversed: false,
            is_distinct: false,
        };
        let result = validate_map_struct_type(&args);

        // Then
        assert_error!(
            result,
            DataFusionError::Execution,
            "MapAggregator can only be used on Map column types"
        );
    }

    #[test]
    fn validate_error_on_non_mapfield_not_struct() {
        // Given
        let args = AccumulatorArgs {
            name: "test",
            exprs: &[],
            return_type: &DataType::Map(
                Arc::new(Field::new("test", DataType::Binary, false)),
                true,
            ),
            schema: &Schema::empty(),
            ignore_nulls: true,
            ordering_req: LexOrdering::empty(),
            is_reversed: false,
            is_distinct: false,
        };
        let result = validate_map_struct_type(&args);

        // Then
        assert_error!(
            result,
            DataFusionError::Execution,
            "MapAggregator Map field is not a Struct type"
        );
    }

    #[test]
    fn validate_error_on_map_inner_struct_length() {
        // Given
        let args = AccumulatorArgs {
            name: "test",
            exprs: &[],
            return_type: &DataType::Map(
                Arc::new(Field::new(
                    "test",
                    DataType::Struct(Fields::from(vec![Field::new(
                        "key",
                        DataType::Boolean,
                        true,
                    )])),
                    true,
                )),
                true,
            ),
            schema: &Schema::empty(),
            ignore_nulls: true,
            ordering_req: LexOrdering::empty(),
            is_reversed: false,
            is_distinct: false,
        };
        let result = validate_map_struct_type(&args);

        // Then
        assert_error!(
            result,
            DataFusionError::Execution,
            "MapAggregator Map inner struct length is not 2"
        );
    }

    #[test]
    fn validate_correct_type() {
        // Given
        let dt = make_map_datatype(DataType::Boolean, DataType::Boolean);
        let schema = Schema::empty();
        let lex = LexOrdering::empty();
        let args = make_accumulator_args(&schema, lex, &dt);
        let result = validate_map_struct_type(&args).map_err(|e| e.to_string());

        // Then
        assert!(result.is_ok());
    }

    #[test]
    fn aggregator_op_sum_correct() {
        // Given
        let op = MapAggregatorOp::Sum;

        // When
        let result = op.op(5, 6);

        // Then
        assert_eq!(result, 11);
    }

    #[test]
    fn aggregator_op_max_correct() {
        // Given
        let op = MapAggregatorOp::Max;

        // When
        let result = op.op(5, 6);

        // Then
        assert_eq!(result, 6);
    }

    #[test]
    fn aggregator_op_min_correct() {
        // Given
        let op = MapAggregatorOp::Min;

        // When
        let result = op.op(5, 6);

        // Then
        assert_eq!(result, 5);
    }

    #[test]
    fn map_aggregator_new_type_check() {
        // Given
        let map = MapAggregator::try_new(&DataType::Boolean, MapAggregatorOp::Sum);

        // Then
        assert_error!(
            map,
            DataFusionError::Plan,
            "MapAggregator can only be used on Map column types"
        );
    }

    fn make_accumulator_args<'a>(
        schema: &'a Schema,
        lex: &'a LexOrdering,
        map_type: &'a DataType,
    ) -> AccumulatorArgs<'a> {
        AccumulatorArgs {
            return_type: map_type,
            schema,
            ignore_nulls: true,
            ordering_req: lex,
            is_reversed: false,
            name: "test",
            is_distinct: false,
            exprs: &[],
        }
    }

    #[test]
    fn should_be_nullable() -> Result<(), DataFusionError> {
        // Given
        let map_type = make_map_datatype(DataType::Int64, DataType::Int64);
        let agg = MapAggregator::try_new(&map_type, MapAggregatorOp::Sum)?;

        // Then
        assert!(agg.is_nullable());
        Ok(())
    }

    #[test]
    fn should_support_group_accumulator() -> Result<(), DataFusionError> {
        // Given
        let map_type = make_map_datatype(DataType::Int64, DataType::Int64);
        let agg = MapAggregator::try_new(&map_type, MapAggregatorOp::Sum)?;

        // Then
        assert!(agg.groups_accumulator_supported(make_accumulator_args(
            &Schema::empty(),
            LexOrdering::empty(),
            &map_type
        )));
        Ok(())
    }

    #[test]
    fn should_error_on_multiple_arg_types() -> Result<(), DataFusionError> {
        // Given
        let map_type = make_map_datatype(DataType::Int64, DataType::Int64);
        let agg = MapAggregator::try_new(&map_type, MapAggregatorOp::Sum)?;

        // When
        let result = agg.return_type(&[DataType::Int64, DataType::Int64]);

        //Then
        assert_error!(
            result,
            DataFusionError::Plan,
            "MapAggregator expects a single column of Map type"
        );

        Ok(())
    }

    #[test]
    fn should_get_prim_accumulator() -> Result<(), DataFusionError> {
        // Given
        let map_type = make_map_datatype(DataType::Int64, DataType::Int64);
        let agg = MapAggregator::try_new(&map_type, MapAggregatorOp::Sum)?;
        let _acc = agg.accumulator(make_accumulator_args(
            &Schema::empty(),
            LexOrdering::empty(),
            &map_type,
        ))?;
        Ok(())
    }

    #[test]
    fn should_error_on_accumulator_binary_value_type_prim_key() -> Result<(), DataFusionError> {
        // Given
        let map_type = make_map_datatype(DataType::Int64, DataType::LargeBinary);
        let agg = MapAggregator::try_new(&map_type, MapAggregatorOp::Sum)?;
        let acc = agg.accumulator(make_accumulator_args(
            &Schema::empty(),
            LexOrdering::empty(),
            &map_type,
        ));

        // Then
        assert_error!(
            acc,
            DataFusionError::Execution,
            "MapAggregator value type must be an integer type not LargeBinary"
        );
        Ok(())
    }

    #[test]
    fn should_error_on_accumulator_binary_value_type_string_key() -> Result<(), DataFusionError> {
        // Given
        let map_type = make_map_datatype(DataType::Utf8, DataType::LargeBinary);
        let agg = MapAggregator::try_new(&map_type, MapAggregatorOp::Sum)?;
        let acc = agg.accumulator(make_accumulator_args(
            &Schema::empty(),
            LexOrdering::empty(),
            &map_type,
        ));

        // Then
        assert_error!(
            acc,
            DataFusionError::Execution,
            "MapAggregator value type must be an integer type not LargeBinary"
        );
        Ok(())
    }

    #[test]
    fn should_error_on_accumulator_binary_value_type_binary_key() -> Result<(), DataFusionError> {
        // Given
        let map_type = make_map_datatype(DataType::Binary, DataType::LargeBinary);
        let agg = MapAggregator::try_new(&map_type, MapAggregatorOp::Sum)?;
        let acc = agg.accumulator(make_accumulator_args(
            &Schema::empty(),
            LexOrdering::empty(),
            &map_type,
        ));

        // Then
        assert_error!(
            acc,
            DataFusionError::Execution,
            "MapAggregator value type must be an integer type not LargeBinary"
        );

        Ok(())
    }

    #[test]
    fn should_make_primitive_key_accumlator() -> Result<(), DataFusionError> {
        // Given
        let map_type = make_map_datatype(DataType::Int64, DataType::Int64);
        let agg = MapAggregator::try_new(&map_type, MapAggregatorOp::Sum)?;
        // Make accumulator
        let mut acc = agg.accumulator(make_accumulator_args(
            &Schema::empty(),
            LexOrdering::empty(),
            &map_type,
        ))?;

        // When
        // Create builder
        let names = MapFieldNames {
            key: "key".into(),
            value: "value".into(),
            entry: "key_value".into(),
        };
        let key_builder = Int64Builder::new();
        let value_builder = Int64Builder::new();
        let mut builder = MapBuilder::new(Some(names), key_builder, value_builder);

        // Feed values to it
        builder.keys().append_value(1);
        builder.values().append_value(2);
        builder.append(true)?;

        // Then - should accumulate without panic
        acc.update_batch(&[Arc::new(builder.finish())])?;

        Ok(())
    }

    #[test]
    fn should_make_string_key_accumlator() -> Result<(), DataFusionError> {
        // Given
        let map_type = make_map_datatype(DataType::Utf8, DataType::Int64);
        let agg = MapAggregator::try_new(&map_type, MapAggregatorOp::Sum)?;
        // Make accumulator
        let mut acc = agg.accumulator(make_accumulator_args(
            &Schema::empty(),
            LexOrdering::empty(),
            &map_type,
        ))?;

        // When
        // Create builder
        let names = MapFieldNames {
            key: "key".into(),
            value: "value".into(),
            entry: "key_value".into(),
        };
        let key_builder = StringBuilder::new();
        let value_builder = Int64Builder::new();
        let mut builder = MapBuilder::new(Some(names), key_builder, value_builder);

        // Feed values to it
        builder.keys().append_value("test");
        builder.values().append_value(2);
        builder.append(true)?;

        // Then - should accumulate without panic
        acc.update_batch(&[Arc::new(builder.finish())])?;

        Ok(())
    }

    #[test]
    fn should_make_binary_key_accumlator() -> Result<(), DataFusionError> {
        // Given
        let map_type = make_map_datatype(DataType::Binary, DataType::Int64);
        let agg = MapAggregator::try_new(&map_type, MapAggregatorOp::Sum)?;
        // Make accumulator
        let mut acc = agg.accumulator(make_accumulator_args(
            &Schema::empty(),
            LexOrdering::empty(),
            &map_type,
        ))?;

        // When
        // Create builder
        let names = MapFieldNames {
            key: "key".into(),
            value: "value".into(),
            entry: "key_value".into(),
        };
        let key_builder = BinaryBuilder::new();
        let value_builder = Int64Builder::new();
        let mut builder = MapBuilder::new(Some(names), key_builder, value_builder);

        // Feed values to it
        builder.keys().append_value(vec![1, 2, 3]);
        builder.values().append_value(2);
        builder.append(true)?;

        // Then - should accumulate without panic
        acc.update_batch(&[Arc::new(builder.finish())])?;

        Ok(())
    }

    #[test]
    fn should_get_prim_group_accumulator() -> Result<(), DataFusionError> {
        // Given
        let map_type = make_map_datatype(DataType::Int64, DataType::Int64);
        let agg = MapAggregator::try_new(&map_type, MapAggregatorOp::Sum)?;
        let _acc = agg.create_groups_accumulator(make_accumulator_args(
            &Schema::empty(),
            LexOrdering::empty(),
            &map_type,
        ))?;
        Ok(())
    }

    #[test]
    fn should_error_on_group_accumulator_binary_value_type_prim_key() -> Result<(), DataFusionError>
    {
        // Given
        let map_type = make_map_datatype(DataType::Int64, DataType::LargeBinary);
        let agg = MapAggregator::try_new(&map_type, MapAggregatorOp::Sum)?;
        let acc = agg.create_groups_accumulator(make_accumulator_args(
            &Schema::empty(),
            LexOrdering::empty(),
            &map_type,
        ));

        // Then
        assert_error!(
            acc,
            DataFusionError::Execution,
            "MapAggregator value type must be an integer type not LargeBinary"
        );

        Ok(())
    }

    #[test]
    fn should_error_on_group_accumulator_binary_value_type_string_key()
    -> Result<(), DataFusionError> {
        // Given
        let map_type = make_map_datatype(DataType::Utf8, DataType::LargeBinary);
        let agg = MapAggregator::try_new(&map_type, MapAggregatorOp::Sum)?;
        let acc = agg.create_groups_accumulator(make_accumulator_args(
            &Schema::empty(),
            LexOrdering::empty(),
            &map_type,
        ));

        // Then
        assert_error!(
            acc,
            DataFusionError::Execution,
            "MapAggregator value type must be an integer type not LargeBinary"
        );

        Ok(())
    }

    #[test]
    fn should_error_on_group_accumulator_binary_value_type_binary_key()
    -> Result<(), DataFusionError> {
        // Given
        let map_type = make_map_datatype(DataType::Binary, DataType::LargeBinary);
        let agg = MapAggregator::try_new(&map_type, MapAggregatorOp::Sum)?;
        let acc = agg.create_groups_accumulator(make_accumulator_args(
            &Schema::empty(),
            LexOrdering::empty(),
            &map_type,
        ));

        // Then
        assert_error!(
            acc,
            DataFusionError::Execution,
            "MapAggregator value type must be an integer type not LargeBinary"
        );

        Ok(())
    }

    #[test]
    fn should_make_primitive_key_group_accumlator() -> Result<(), DataFusionError> {
        // Given
        let map_type = make_map_datatype(DataType::Int64, DataType::Int64);
        let agg = MapAggregator::try_new(&map_type, MapAggregatorOp::Sum)?;
        // Make accumulator
        let mut acc = agg.create_groups_accumulator(make_accumulator_args(
            &Schema::empty(),
            LexOrdering::empty(),
            &map_type,
        ))?;

        // When
        // Create builder
        let names = MapFieldNames {
            key: "key".into(),
            value: "value".into(),
            entry: "key_value".into(),
        };
        let key_builder = Int64Builder::new();
        let value_builder = Int64Builder::new();
        let mut builder = MapBuilder::new(Some(names), key_builder, value_builder);

        // Feed values to it
        builder.keys().append_value(1);
        builder.values().append_value(2);
        builder.append(true)?;

        // Then - should accumulate without panic
        acc.update_batch(&[Arc::new(builder.finish())], &[1], None, 2)?;

        Ok(())
    }

    #[test]
    fn should_make_string_key_group_accumlator() -> Result<(), DataFusionError> {
        // Given
        let map_type = make_map_datatype(DataType::Utf8, DataType::Int64);
        let agg = MapAggregator::try_new(&map_type, MapAggregatorOp::Sum)?;
        // Make accumulator
        let mut acc = agg.create_groups_accumulator(make_accumulator_args(
            &Schema::empty(),
            LexOrdering::empty(),
            &map_type,
        ))?;

        // When
        // Create builder
        let names = MapFieldNames {
            key: "key".into(),
            value: "value".into(),
            entry: "key_value".into(),
        };
        let key_builder = StringBuilder::new();
        let value_builder = Int64Builder::new();
        let mut builder = MapBuilder::new(Some(names), key_builder, value_builder);

        // Feed values to it
        builder.keys().append_value("test");
        builder.values().append_value(2);
        builder.append(true)?;

        // Then - should accumulate without panic
        acc.update_batch(&[Arc::new(builder.finish())], &[1], None, 2)?;

        Ok(())
    }

    #[test]
    fn should_make_binary_key_group_accumlator() -> Result<(), DataFusionError> {
        // Given
        let map_type = make_map_datatype(DataType::Binary, DataType::Int64);
        let agg = MapAggregator::try_new(&map_type, MapAggregatorOp::Sum)?;
        // Make accumulator
        let mut acc = agg.create_groups_accumulator(make_accumulator_args(
            &Schema::empty(),
            LexOrdering::empty(),
            &map_type,
        ))?;

        // When
        // Create builder
        let names = MapFieldNames {
            key: "key".into(),
            value: "value".into(),
            entry: "key_value".into(),
        };
        let key_builder = BinaryBuilder::new();
        let value_builder = Int64Builder::new();
        let mut builder = MapBuilder::new(Some(names), key_builder, value_builder);

        // Feed values to it
        builder.keys().append_value(vec![1, 2, 3]);
        builder.values().append_value(2);
        builder.append(true)?;

        // Then - should accumulate without panic
        acc.update_batch(&[Arc::new(builder.finish())], &[1], None, 2)?;

        Ok(())
    }
}
