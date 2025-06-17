/// Simple [`Accumulator`] implementations for string map aggregation.
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
        ArrayBuilder, ArrayRef, ArrowPrimitiveType, AsArray, MapBuilder, MapFieldNames,
        StringBuilder, StructArray,
    },
    datatypes::DataType,
};
use datafusion::{
    common::{HashMap, exec_err, plan_err},
    error::Result,
    logical_expr::Accumulator,
    scalar::ScalarValue,
};
use num_traits::NumAssign;
use std::{fmt::Debug, sync::Arc};

use crate::datafusion::aggregate_udf::{MapAggregatorOp, map_aggregate::aggregator::PrimBuilderType};

/// Given an Arrow [`StructArray`] of keys and values, update the given map.
///
/// This implementation is for maps with string keys and primitive values.
///
/// All nulls keys/values are skipped over.
fn update_string_map<VBuilder>(
    input: Option<&StructArray>,
    map: &mut HashMap<
        String,
        <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native,
    >,
    op: &MapAggregatorOp,
) where
    VBuilder: ArrayBuilder + PrimBuilderType,
    <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native: NumAssign + Ord,
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
                        .and_modify(|current_value| *current_value = op.op(*current_value, value))
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
    op: MapAggregatorOp,
}

impl<VBuilder> StringMapAccumulator<VBuilder>
where
    VBuilder: ArrayBuilder + PrimBuilderType,
{
    /// Creates a new accumulator.
    ///
    /// The type of the map must be specified so that the correct sort
    /// of map builder can be created.
    ///
    /// # Errors
    /// If the incorrect type of data type is provided. Must me a map type with an
    /// inner Struct type.
    pub fn try_new(map_type: &DataType, op: MapAggregatorOp) -> Result<Self> {
        if let DataType::Map(field, _) = map_type {
            let DataType::Struct(_) = field.data_type() else {
                return plan_err!(
                    "StringMapAccumulator inner field type should be a DataType::Struct"
                );
            };
            Ok(Self {
                inner_field_type: field.data_type().clone(),
                values: HashMap::default(),
                op,
            })
        } else {
            plan_err!("Invalid datatype for StringMapAccumulator {map_type:?}")
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
                "Invalid datatype inside StringMapAccumulator {:?}",
                self.inner_field_type
            ),
        }
    }
}

impl<VBuilder> Accumulator for StringMapAccumulator<VBuilder>
where
    VBuilder: ArrayBuilder + PrimBuilderType,
    <<VBuilder as PrimBuilderType>::ArrowType as ArrowPrimitiveType>::Native: NumAssign + Ord,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 1 {
            return exec_err!("StringMapAccumulator only accepts single column input");
        }

        let input = values[0].as_map();
        // For each map we get, feed it into our internal aggregated map
        for map in input.iter() {
            update_string_map::<VBuilder>(map.as_ref(), &mut self.values, &self.op);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut builder = self.make_map_builder(self.values.len());
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

#[cfg(test)]
mod tests {
    use crate::{
        assert_error,
        datafusion::aggregate_udf::{
            MapAggregatorOp,
            map_aggregate::{
                accumulator::string_accumulator::{StringMapAccumulator, update_string_map},
                aggregator::map_test_common::make_map_datatype,
            },
        },
    };
    use arrow::{
        array::{
            AsArray, Int64Array, Int64Builder, StringArray, StringBuilder, StructBuilder,
            UInt16Builder,
        },
        datatypes::{DataType, Field, Fields, Int64Type},
    };
    use datafusion::{
        common::HashMap, error::DataFusionError, logical_expr::Accumulator, scalar::ScalarValue,
    };
    use std::sync::Arc;

    // Macro to stringify a literal
    macro_rules! s {
        ($l:literal) => {
            String::from($l)
        };
    }

    #[test]
    fn update_string_map_none() {
        // Given
        let mut map = HashMap::<String, i64>::new();
        map.insert(s!["test"], 2);
        map.insert(s!["test2"], 4);
        let expected = map.clone();

        // When
        update_string_map::<Int64Builder>(None, &mut map, &MapAggregatorOp::Sum);

        // Then - expect no changes
        assert_eq!(map, expected);
    }

    #[test]
    fn update_string_map_empty_values() {
        // Given
        let mut map = HashMap::<String, i64>::new();
        map.insert(s!["test"], 2);
        map.insert(s!["test2"], 4);
        let expected = map.clone();

        let mut entry_builder = StructBuilder::new(
            Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, false),
            ]),
            vec![
                Box::new(StringBuilder::new()),
                Box::new(Int64Builder::new()),
            ],
        );

        // When
        update_string_map::<Int64Builder>(
            Some(&entry_builder.finish()),
            &mut map,
            &MapAggregatorOp::Sum,
        );

        // Then - expect no changes
        assert_eq!(map, expected);
    }

    #[test]
    fn update_string_map_enters_first_values() {
        // Given
        let mut map = HashMap::<String, i64>::new();
        let mut expected = HashMap::new();
        expected.insert(s!["1"], 2);
        expected.insert(s!["3"], 4);
        expected.insert(s!["8"], -10);

        let mut entry_builder = StructBuilder::new(
            Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, false),
            ]),
            vec![
                Box::new(StringBuilder::new()),
                Box::new(Int64Builder::new()),
            ],
        );
        for (k, v) in [(s!["1"], 2), (s!["3"], 4), (s!["8"], -10)] {
            entry_builder
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value(k);
            entry_builder
                .field_builder::<Int64Builder>(1)
                .unwrap()
                .append_value(v);
            entry_builder.append(true);
        }

        // When
        update_string_map::<Int64Builder>(
            Some(&entry_builder.finish()),
            &mut map,
            &MapAggregatorOp::Sum,
        );

        // Then
        assert_eq!(map, expected);
    }

    #[test]
    fn update_string_map_enters_values_and_sums() {
        // Given
        let mut map = HashMap::<String, i64>::new();
        map.insert(s!["1"], 2);
        map.insert(s!["3"], 4);
        let mut expected = HashMap::new();
        expected.insert(s!["1"], 6);
        expected.insert(s!["3"], 8);
        expected.insert(s!["4"], 2);

        let mut entry_builder = StructBuilder::new(
            Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, false),
            ]),
            vec![
                Box::new(StringBuilder::new()),
                Box::new(Int64Builder::new()),
            ],
        );
        for (k, v) in [(s!["1"], 2), (s!["1"], 2), (s!["3"], 4), (s!["4"], 2)] {
            entry_builder
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value(k);
            entry_builder
                .field_builder::<Int64Builder>(1)
                .unwrap()
                .append_value(v);
            entry_builder.append(true);
        }

        // When
        update_string_map::<Int64Builder>(
            Some(&entry_builder.finish()),
            &mut map,
            &MapAggregatorOp::Sum,
        );

        // Then
        assert_eq!(map, expected);
    }

    #[test]
    #[should_panic(expected = "Nullable entries aren't supported")]
    fn update_string_map_panics_on_null() {
        // Given
        let mut map = HashMap::<String, i64>::new();

        let mut entry_builder = StructBuilder::new(
            Fields::from(vec![
                Field::new("key", DataType::Utf8, true),
                Field::new("value", DataType::Int64, true),
            ]),
            vec![
                Box::new(StringBuilder::new()),
                Box::new(Int64Builder::new()),
            ],
        );
        for (k, v) in [(s!["1"], 2), (s!["1"], 2), (s!["3"], 4), (s!["4"], 2)] {
            entry_builder
                .field_builder::<StringBuilder>(0)
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
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_null();
        entry_builder
            .field_builder::<Int64Builder>(1)
            .unwrap()
            .append_null();
        entry_builder.append(true);

        // When - panic
        update_string_map::<Int64Builder>(
            Some(&entry_builder.finish()),
            &mut map,
            &MapAggregatorOp::Sum,
        );
    }

    #[test]
    fn try_new_should_succeed() {
        // Given
        let mt = make_map_datatype(DataType::Utf8, DataType::Int64);

        // When
        let acc = StringMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum);

        // Then
        assert!(acc.is_ok());
    }

    #[test]
    fn try_new_should_error_on_non_map_type() {
        // Given
        let mt = DataType::Int16;
        let acc = StringMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum);

        // Then
        assert_error!(
            acc,
            DataFusionError::Plan,
            "Invalid datatype for StringMapAccumulator Int16"
        );
    }

    #[test]
    fn try_new_should_error_on_wrong_inner_type() {
        // Given
        let mt = DataType::Map(Arc::new(Field::new("test", DataType::Int16, false)), false);
        let acc: Result<_, DataFusionError> =
            StringMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum);

        // Then
        assert_error!(
            acc,
            DataFusionError::Plan,
            "StringMapAccumulator inner field type should be a DataType::Struct"
        );
    }

    #[test]
    #[should_panic(expected = "Invalid datatype inside StringMapAccumulator Int16")]
    fn make_map_builder_check_unreachable() {
        // Given
        // Build instance directly via private constructor
        let acc = StringMapAccumulator::<Int64Builder> {
            inner_field_type: DataType::Int16,
            values: HashMap::new(),
            op: MapAggregatorOp::Sum,
        };

        // Then - should panic
        acc.make_map_builder(10);
    }

    #[test]
    fn make_map_builder_field_names_equal() {
        // Given
        let acc = StringMapAccumulator::<Int64Builder> {
            inner_field_type: DataType::Struct(Fields::from(vec![
                Field::new("key1_name", DataType::Utf8, false),
                Field::new("value1_name", DataType::Int64, false),
            ])),
            values: HashMap::new(),
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
        let acc = StringMapAccumulator::<UInt16Builder> {
            inner_field_type: DataType::Struct(Fields::from(vec![
                Field::new("key1_name", DataType::Utf8, false),
                Field::new("value1_name", DataType::UInt16, false),
            ])),
            values: HashMap::new(),
            op: MapAggregatorOp::Sum,
        };

        // When
        let array = acc.make_map_builder(10).finish();

        // Then
        assert_eq!(*array.key_type(), DataType::Utf8);
        assert_eq!(*array.value_type(), DataType::UInt16);
    }

    #[test]
    fn update_batch_should_fail_on_zero_or_multi_column() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Utf8, DataType::Int64);
        let mut acc = StringMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum)?;

        // When
        let result = acc.update_batch(&[]);

        // Then
        assert_error!(
            result,
            DataFusionError::Execution,
            "StringMapAccumulator only accepts single column input"
        );

        // When
        let arr1 = StringArray::from(vec!["a"]);
        let arr2 = Int64Array::from_value(1, 1);
        let result = acc.update_batch(&[Arc::new(arr1), Arc::new(arr2)]);

        // Then
        assert_error!(
            result,
            DataFusionError::Execution,
            "StringMapAccumulator only accepts single column input"
        );

        Ok(())
    }

    #[test]
    fn update_batch_should_not_update_zero_maps() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Utf8, DataType::Int64);
        let mut acc = StringMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum)?;
        let mut builder = acc.make_map_builder(10);

        // When
        let array = builder.finish();
        acc.update_batch(&[Arc::new(array)])?;

        // Then
        assert!(acc.values.is_empty());

        Ok(())
    }

    #[test]
    fn update_batch_should_not_update_single_empty_map() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Utf8, DataType::Int64);
        let mut acc = StringMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum)?;
        let mut builder = acc.make_map_builder(10);

        // When
        builder.append(true)?;
        let array = builder.finish();
        acc.update_batch(&[Arc::new(array)])?;

        // Then
        assert!(acc.values.is_empty());

        Ok(())
    }

    #[test]
    fn update_batch_should_update_single_map() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Utf8, DataType::Int64);
        let mut acc = StringMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum)?;
        let mut builder = acc.make_map_builder(10);
        let expected = HashMap::from([(s!["3"], 10)]);

        // When
        builder.keys().append_value(s!["3"]);
        builder.values().append_value(10);
        builder.append(true)?;
        let array = builder.finish();
        acc.update_batch(&[Arc::new(array)])?;

        // Then
        assert_eq!(acc.values, expected);

        Ok(())
    }

    #[test]
    fn update_batch_should_update_two_single_map() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Utf8, DataType::Int64);
        let mut acc = StringMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum)?;
        let mut builder = acc.make_map_builder(10);
        let expected = HashMap::from([(s!["3"], 10), (s!["2"], 5)]);

        // When
        builder.keys().append_value(s!["3"]);
        builder.values().append_value(10);
        builder.append(true)?;
        builder.keys().append_value(s!["2"]);
        builder.values().append_value(5);
        builder.append(true)?;

        let array = builder.finish();
        acc.update_batch(&[Arc::new(array)])?;

        // Then
        assert_eq!(acc.values, expected);

        Ok(())
    }

    #[test]
    fn update_batch_should_update_multiple_maps() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Utf8, DataType::Int64);
        let mut acc = StringMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum)?;
        let mut builder = acc.make_map_builder(10);
        let expected = HashMap::from([(s!["3"], 10), (s!["1"], 13), (s!["2"], 7), (s!["4"], 10)]);

        // When
        for (k, v) in [(s!["3"], 10), (s!["1"], 4), (s!["1"], 3), (s!["2"], 7)] {
            builder.keys().append_value(k);
            builder.values().append_value(v);
        }
        builder.append(true)?;

        builder.keys().append_value(s!["1"]);
        builder.values().append_value(3);

        builder.append(true)?;

        for (k, v) in [(s!["1"], 3), (s!["4"], 10)] {
            builder.keys().append_value(k);
            builder.values().append_value(v);
        }
        builder.append(true)?;

        let array = builder.finish();
        acc.update_batch(&[Arc::new(array)])?;

        // Then
        assert_eq!(acc.values, expected);

        Ok(())
    }

    #[test]
    fn evaluate_should_produce_results() -> Result<(), DataFusionError> {
        // Given
        let mt = make_map_datatype(DataType::Utf8, DataType::Int64);
        let mut acc = StringMapAccumulator::<Int64Builder>::try_new(&mt, MapAggregatorOp::Sum)?;
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
            builder.keys().append_value(k);
            builder.values().append_value(v);
        }

        builder.append(true)?;
        acc.update_batch(&[Arc::new(builder.finish())])?;
        let result = acc.evaluate()?;

        // Then
        // Iteration order of maps is not guaranteed, so read the scalar value results into a new map
        let ScalarValue::Map(result_map) = result else {
            panic!("result type not a map");
        };
        let keys = result_map.keys().as_string::<i32>();
        let values = result_map.values().as_primitive::<Int64Type>();
        let mut accumulated_map = HashMap::new();
        for item in keys.iter().zip(values.iter()) {
            if let (Some(opt_k), Some(opt_v)) = item {
                accumulated_map.insert(opt_k.to_string(), opt_v);
            }
        }

        assert_eq!(expected, accumulated_map);
        Ok(())
    }
}
