//! Contains the code to cast primitive types without causing an error.
//! Values that undergo a narrowing cast are truncated.
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
    array::Array,
    array::AsArray,
    compute::CastOptions,
    datatypes::{DataType, Field, FieldRef, Int32Type, Int64Type},
    util::display::FormatOptions,
};
use datafusion::{
    common::{exec_err, internal_err, plan_datafusion_err, plan_err},
    error::Result,
    logical_expr::{
        ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
        interval_arithmetic::Interval, sort_properties::ExprProperties,
    },
    scalar::ScalarValue,
};
use std::{hash::Hash, sync::Arc};

/// A UDF for performing primitive casting, but without producing errors if a narrowing conversion
/// would be outside the target type's range. Casts are performed in the "standard" way, i.e. truncating
/// upper bits on a narrowing conversion.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CastUDF {
    signature: Signature,
    // Function output type
    output_type: DataType,
    // If nulls may be present
    nullable: bool,
}

impl CastUDF {
    /// Constructs a new `CastUDF` for casting values from the specified input column type to the target output type
    /// without error on narrowing conversions.
    pub fn new(column_type: &DataType, output_type: &DataType, nullable: bool) -> Self {
        Self {
            signature: Signature::exact(vec![column_type.clone()], Volatility::Immutable),
            output_type: output_type.clone(),
            nullable,
        }
    }

    /// Returns a reference to the output type of the cast produced by this UDF.
    pub fn output_type(&self) -> &DataType {
        &self.output_type
    }
}

impl ScalarUDFImpl for CastUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "cast_simple"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[arrow::datatypes::DataType],
    ) -> datafusion::error::Result<arrow::datatypes::DataType> {
        internal_err!("Expected return_type_from_args, found call to return_type")
    }

    // Allow this warning as we explicitly want narrowing conversions to truncate
    #[allow(clippy::cast_possible_truncation)]
    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() > 1 {
            return exec_err!(
                "{} only supports single column input, received {}",
                self.name(),
                args.args.len()
            );
        }
        // If datatypes match, then do nothing
        if args.arg_fields[0].data_type() == self.output_type() {
            Ok(args.args.remove(0))
        } else {
            // Cast to output type
            match &args.args[0] {
                ColumnarValue::Array(array) => match array.data_type() {
                    DataType::Int32 => match self.output_type() {
                        DataType::Int32 => unreachable!("Shouldn't need to cast!"),
                        DataType::Int64 => Ok(ColumnarValue::Array(Arc::new(
                            array
                                .as_primitive::<Int32Type>()
                                .unary::<_, Int64Type>(i64::from),
                        ))),
                        _ => exec_err!("Can't cast to {}", self.output_type()),
                    },
                    DataType::Int64 => match self.output_type() {
                        DataType::Int32 => Ok(ColumnarValue::Array(Arc::new(
                            array
                                .as_primitive::<Int64Type>()
                                .unary::<_, Int32Type>(|v| v as i32),
                        ))),
                        DataType::Int64 => unreachable!("Shouldn't need to cast!"),
                        _ => exec_err!("Can't cast to {}", self.output_type()),
                    },
                    _ => {
                        exec_err!(
                            "Column type {} not supported for {}",
                            array.data_type(),
                            self.name()
                        )
                    }
                },
                ColumnarValue::Scalar(ScalarValue::Int32(Some(value))) => {
                    match self.output_type() {
                        DataType::Int32 => unreachable!("Shouldn't need to cast!"),
                        DataType::Int64 => Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(
                            i64::from(*value),
                        )))),
                        _ => exec_err!("Can't cast to {}", self.output_type()),
                    }
                }
                ColumnarValue::Scalar(ScalarValue::Int64(Some(value))) => {
                    match self.output_type() {
                        DataType::Int32 => Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(
                            *value as i32,
                        )))),
                        DataType::Int64 => unreachable!("Shouldn't need to cast!"),
                        _ => exec_err!("Can't cast to {}", self.output_type()),
                    }
                }
                x @ ColumnarValue::Scalar(_) => {
                    exec_err!(
                        "Column type {} not supported for {}",
                        x.data_type(),
                        self.name()
                    )
                }
            }
        }
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            "",
            self.output_type().clone(),
            self.nullable,
        )))
    }

    fn evaluate_bounds(&self, input: &[&Interval]) -> Result<Interval> {
        if input.is_empty() {
            return create_full_range(self.output_type());
        }

        let mut result: Option<Interval> = None;
        for interval in input {
            if let Some(casted) = create_output_interval(interval, self.output_type())? {
                result = match result {
                    None => Some(casted),
                    Some(i) => Some(i.union(casted)?),
                };
            }
        }

        Ok(result.unwrap_or(create_full_range(self.output_type())?))
    }

    fn preserves_lex_ordering(&self, _inputs: &[ExprProperties]) -> Result<bool> {
        // narrowing conversions will not preserve lex ordering
        Ok(false)
    }
}

const CAST_OPTIONS: CastOptions = CastOptions {
    safe: false,
    format_options: FormatOptions::new(),
};

/// Perform an interval type cast if output type is narrower.
fn create_output_interval(interval: &Interval, output_type: &DataType) -> Result<Option<Interval>> {
    if !output_type.is_numeric() {
        return plan_err!("output type is not numeric: {}", output_type);
    }
    let full_range_output = create_full_range(output_type)?;

    let lower = interval.lower();
    // Will input interval fit in range of output data type?
    Ok(
        if lower.data_type().primitive_width().unwrap() <= output_type.primitive_width().unwrap() {
            Some(interval.cast_to(output_type, &CAST_OPTIONS)?)
        } else {
            // Input interval doesn't fit into output type (e.g. 64 bit interval into a 32bit interval)
            let widen_cast = full_range_output.cast_to(&lower.data_type(), &CAST_OPTIONS)?;
            if let Some(intersection) = interval.intersect(widen_cast)? {
                Some(intersection.cast_to(output_type, &CAST_OPTIONS)?)
            } else {
                None
            }
        },
    )
}

/// Get an [`Interval`] respresenting full range of a given type.
fn create_full_range(output_type: &DataType) -> Result<Interval> {
    let full_range_output = Interval::try_new(
        ScalarValue::min(output_type).ok_or(plan_datafusion_err!(
            "Bounds type is not a numeric type: {:?}",
            output_type
        ))?,
        ScalarValue::max(output_type).ok_or(plan_datafusion_err!(
            "Bounds type is not a numeric type: {:?}",
            output_type
        ))?,
    )?;
    Ok(full_range_output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use datafusion::logical_expr::interval_arithmetic::Interval;
    use datafusion::scalar::ScalarValue;

    fn make_interval(lower: &ScalarValue, upper: &ScalarValue) -> Interval {
        Interval::try_new(lower.clone(), upper.clone()).unwrap()
    }

    #[test]
    fn should_error_on_create_full_range_non_numeric_type() {
        // When
        let result = create_full_range(&DataType::LargeUtf8);

        // Then
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("Bounds type is not a numeric type: LargeUtf8"));
    }

    #[test]
    fn should_widen_bounds_from_int32_to_int64() {
        // Given
        let udf = CastUDF::new(&DataType::Int32, &DataType::Int64, false);
        let intervals = vec![
            make_interval(&ScalarValue::Int32(Some(1)), &ScalarValue::Int32(Some(10))),
            make_interval(
                &ScalarValue::Int32(Some(-100)),
                &ScalarValue::Int32(Some(200)),
            ),
        ];
        let inputs = intervals.iter().collect::<Vec<_>>();

        // When
        let result = udf.evaluate_bounds(&inputs).unwrap();

        // Then
        assert_eq!(*result.lower(), ScalarValue::Int64(Some(-100)));
        assert_eq!(*result.upper(), ScalarValue::Int64(Some(200)));
    }

    #[test]
    fn should_narrow_bounds_from_int64_to_int32_and_truncate() {
        // Given
        let udf = CastUDF::new(&DataType::Int64, &DataType::Int32, false);
        let intervals = vec![
            make_interval(
                &ScalarValue::Int64(Some(i64::from(i32::MIN) - 1)), // -2147483649
                &ScalarValue::Int64(Some(i64::from(i32::MAX) + 1)), // 2147483648
            ),
            make_interval(
                &ScalarValue::Int64(Some(50)),
                &ScalarValue::Int64(Some(100)),
            ),
        ];
        let inputs = intervals.iter().collect::<Vec<_>>();

        // When
        let result = udf.evaluate_bounds(&inputs).unwrap();

        // Then
        assert_eq!(*result.lower(), ScalarValue::Int32(Some(i32::MIN)));
        assert_eq!(*result.upper(), ScalarValue::Int32(Some(i32::MAX)));
    }

    #[test]
    fn should_return_same_bounds_when_types_match() {
        // Given
        let udf = CastUDF::new(&DataType::Int32, &DataType::Int32, false);
        let intervals = vec![
            make_interval(
                &ScalarValue::Int32(Some(50)),
                &ScalarValue::Int32(Some(200)),
            ),
            make_interval(
                &ScalarValue::Int64(Some(150)),
                &ScalarValue::Int64(Some(300)),
            ),
        ];
        let inputs = intervals.iter().collect::<Vec<_>>();

        // When
        let result = udf.evaluate_bounds(&inputs).unwrap();

        // Then
        assert_eq!(*result.lower(), ScalarValue::Int32(Some(50)));
        assert_eq!(*result.upper(), ScalarValue::Int32(Some(300)));
    }

    #[test]
    fn should_error_on_non_numeric_bounds_type() {
        // Given
        let udf = CastUDF::new(&DataType::Utf8, &DataType::Utf8, false);
        let intervals = [make_interval(
            &ScalarValue::Utf8(Some("a".to_string())),
            &ScalarValue::Utf8(Some("z".to_string())),
        )];
        let inputs = intervals.iter().collect::<Vec<_>>();

        // When
        let result = udf.evaluate_bounds(&inputs);

        // Then
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("output type is not numeric: Utf8"));
    }

    #[test]
    fn should_return_max_interval_for_empty_input() {
        // Given
        let udf = CastUDF::new(&DataType::Int32, &DataType::Int64, false);
        let inputs: Vec<&Interval> = vec![];

        // When
        let result = udf.evaluate_bounds(&inputs).unwrap();

        // Then
        assert_eq!(*result.lower(), ScalarValue::Int64(Some(i64::MIN)));
        assert_eq!(*result.upper(), ScalarValue::Int64(Some(i64::MAX)));
    }

    #[test]
    fn should_error_on_mixed_input_interval_types() {
        // Given
        let udf = CastUDF::new(&DataType::Int32, &DataType::Int64, false);
        // One input interval is Int32, another is Int64.
        let intervals = vec![
            make_interval(&ScalarValue::Int32(Some(1)), &ScalarValue::Int32(Some(10))),
            make_interval(&ScalarValue::Int64(Some(20)), &ScalarValue::Int64(Some(30))),
        ];
        let inputs = intervals.iter().collect::<Vec<_>>();

        // When
        let result = udf.evaluate_bounds(&inputs);

        // Then
        assert!(result.is_err());
    }
}
