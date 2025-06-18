//! Implementation of [`ScalarUDFImpl`] for age off filtering.
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
use crate::datafusion::aggregate_udf::Filter;
use arrow::{
    array::{AsArray, BooleanBuilder},
    datatypes::{DataType, Int64Type},
};
use datafusion::{
    common::{exec_err, plan_datafusion_err},
    error::{DataFusionError, Result},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
        interval_arithmetic::Interval,
    },
    scalar::ScalarValue,
};
use std::{
    any::Any,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// A filtering expression (returns bool) for an integer (typically 64-bit, known as "long" elsewhere in the codebase,
/// other widths will be automatically co-erced) column based upon a given threshold. If the value in a given column is
/// lower than the given threshold, it will be filtered out.
#[derive(Debug)]
pub struct AgeOff {
    /// Threshold value (seconds since UNIX epoch)
    threshold: i64,
    /// Signature for this filter expression
    signature: Signature,
}

impl AgeOff {
    /// Create a new age-off filter expression with the given threshold.
    ///
    /// Threshold is measured in seconds since the UNIX epoch.
    pub fn new(threshold: i64) -> Self {
        Self {
            threshold,
            signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
        }
    }

    /// Tests if the timestamp `t` >= the threshold for retention. Returns true if the record
    /// with timestamp `t` should be retained.
    #[inline]
    fn retain(&self, t: i64) -> bool {
        t >= self.threshold
    }
}

impl TryFrom<&Filter> for AgeOff {
    type Error = DataFusionError;

    #[allow(irrefutable_let_patterns, clippy::cast_possible_wrap)]
    fn try_from(value: &Filter) -> std::result::Result<Self, Self::Error> {
        match value {
            Filter::Ageoff { column: _, max_age } => {
                // Figure out max_age in as a millisecond threshold from current time
                let threshold = if *max_age >= 0 {
                    SystemTime::now().checked_sub(Duration::from_secs(max_age.unsigned_abs()))
                } else {
                    SystemTime::now().checked_add(Duration::from_secs(max_age.unsigned_abs()))
                }
                // Convert Option to Result with DataFusionError
                .ok_or(plan_datafusion_err!(
                    "Age off filter max_age not representable as timestamp"
                ))?
                // Figure out duration since unix epoch
                .duration_since(UNIX_EPOCH)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                // Finally, convert to raw integer
                .as_secs() as i64;
                Ok(AgeOff::new(threshold))
            }
        }
    }
}

impl ScalarUDFImpl for AgeOff {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "age_off"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!(
                "AgeOff UDF called with {} input columns, only accepts 1",
                args.args.len()
            );
        }
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Boolean(Some(self.retain(*v))),
            )),
            ColumnarValue::Scalar(ScalarValue::Int64(None)) => {
                exec_err!("Age off called with null Int64")
            }
            ColumnarValue::Array(arr) => {
                let prim_arr = arr.as_primitive::<Int64Type>();
                let mut result_builder = BooleanBuilder::with_capacity(args.number_rows);
                for v in prim_arr {
                    if let Some(num) = v {
                        result_builder.append_value(self.retain(num));
                    } else {
                        result_builder.append_null();
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(result_builder.finish())))
            }
            ColumnarValue::Scalar(_) => exec_err!(
                "Age off called with unsupported column datatype {:?}",
                args.args[0].data_type()
            ),
        }
    }

    fn evaluate_bounds(&self, _input: &[&Interval]) -> Result<Interval> {
        Interval::make_unbounded(&DataType::Boolean)
    }
}

#[cfg(test)]
mod tests {
    use super::AgeOff;
    use crate::{assert_error, datafusion::aggregate_udf::Filter};
    use arrow::{
        array::{AsArray, Int64Builder},
        datatypes::{DataType, Field},
    };
    use datafusion::{
        common::exec_err,
        error::DataFusionError,
        logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl},
        scalar::ScalarValue,
    };
    use std::sync::Arc;

    fn run_retain_check(threshold: i64, candidate: i64) -> bool {
        let ageoff = AgeOff::new(threshold);
        ageoff.retain(candidate)
    }

    #[test]
    fn threshold_test() {
        // Given
        // Sequence of tuples: (threshold, candidate, retain)
        // where retain should be the whether the candidate is retained or not
        let tests = vec![
            (10, 9, false),
            (10, 10, true),
            (10, 11, true),
            (10, 0, false),
            (10, -999_999, false),
            (0, 0, true),
            (0, 1, true),
            (0, 2, true),
            (0, -1, false),
            (1, 0, false),
            (-1, 0, true),
            (123_566, -8_775_435, false),
            (-54_678, 74_556_344, true),
            (i64::MAX, i64::MAX, true),
            (i64::MAX, i64::MAX - 1, false),
            (i64::MAX - 1, i64::MAX, true),
            (i64::MIN, i64::MIN, true),
            (i64::MIN, i64::MIN + 1, true),
            (i64::MIN + 1, i64::MIN, false),
        ];

        // When - Then
        for (threshold, candidate, retain) in &tests {
            assert_eq!(
                *retain,
                run_retain_check(*threshold, *candidate),
                "Age-off check threshold {threshold} candidate {candidate} should be {retain}"
            );
        }
    }

    #[test]
    fn try_from_should_create_from_filter() -> Result<(), DataFusionError> {
        // Given
        let filter = Filter::Ageoff {
            column: "test".into(),
            max_age: 1000,
        };

        // When
        AgeOff::try_from(&filter)?;
        Ok(())
    }

    #[test]
    fn try_from_should_produce_error_on_large_timestamp() {
        // Given
        let filter = Filter::Ageoff {
            column: "test".into(),
            max_age: i64::MAX,
        };

        // When
        let result = AgeOff::try_from(&filter);

        // Then
        assert!(result.is_err());
    }

    #[test]
    fn try_from_should_produce_error_on_large_negative_timestamp() {
        // Given
        let filter = Filter::Ageoff {
            column: "test".into(),
            max_age: i64::MIN,
        };

        // When
        let result = AgeOff::try_from(&filter);

        // Then
        assert_error!(
            result,
            DataFusionError::Plan,
            "Age off filter max_age not representable as timestamp"
        );
    }

    #[test]
    fn invoke_should_fail_with_zero_columns() {
        // Given
        let ageoff = AgeOff::new(1000);

        // When
        let result = ageoff.invoke_with_args(ScalarFunctionArgs {
            number_rows: 1,
            args: vec![],
            return_field: Arc::new(Field::new("", DataType::Boolean, false)),
            arg_fields: vec![],
        });

        assert_error!(
            result,
            DataFusionError::Execution,
            "AgeOff UDF called with 0 input columns, only accepts 1"
        );
    }

    #[test]
    fn invoke_should_fail_with_two_columns() {
        // Given
        let ageoff = AgeOff::new(1000);

        // When
        let result = ageoff.invoke_with_args(ScalarFunctionArgs {
            number_rows: 1,
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))),
            ],
            return_field: Arc::new(Field::new("", DataType::Boolean, false)),
            arg_fields: vec![
                Arc::new(Field::new("", DataType::Boolean, false)),
                Arc::new(Field::new("", DataType::Boolean, false)),
            ],
        });

        assert_error!(
            result,
            DataFusionError::Execution,
            "AgeOff UDF called with 2 input columns, only accepts 1"
        );
    }

    #[test]
    fn invoke_should_fail_with_unsupported_type() {
        // Given
        let ageoff = AgeOff::new(1000);

        // When
        let result = ageoff.invoke_with_args(ScalarFunctionArgs {
            number_rows: 1,
            args: vec![ColumnarValue::Scalar(ScalarValue::Float32(None))],
            return_field: Arc::new(Field::new("", DataType::Boolean, false)),
            arg_fields: vec![Arc::new(Field::new("", DataType::Float32, false))],
        });

        assert_error!(
            result,
            DataFusionError::Execution,
            "Age off called with unsupported column datatype Float32"
        );
    }

    #[test]
    fn invoke_should_fail_null_value() {
        // Given
        let ageoff = AgeOff::new(1000);

        // When
        let result = ageoff.invoke_with_args(ScalarFunctionArgs {
            number_rows: 1,
            args: vec![ColumnarValue::Scalar(ScalarValue::Int64(None))],
            return_field: Arc::new(Field::new("", DataType::Boolean, false)),
            arg_fields: vec![Arc::new(Field::new("", DataType::Int64, true))],
        });

        assert_error!(
            result,
            DataFusionError::Execution,
            "Age off called with null Int64"
        );
    }

    #[test]
    fn invoke_should_retain_single_value() -> Result<(), DataFusionError> {
        // Given
        let ageoff = AgeOff::new(1000);

        // When
        let result = ageoff.invoke_with_args(ScalarFunctionArgs {
            number_rows: 1,
            args: vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(1000)))],
            return_field: Arc::new(Field::new("", DataType::Boolean, false)),
            arg_fields: vec![Arc::new(Field::new("", DataType::Int64, true))],
        })?;

        // Then
        if let ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) = result {
            Ok(())
        } else {
            exec_err!("Test should have returned a single scalar boolean true")
        }
    }

    #[test]
    fn invoke_should_not_retain_single_value() -> Result<(), DataFusionError> {
        // Given
        let ageoff = AgeOff::new(1000);

        // When
        let result = ageoff.invoke_with_args(ScalarFunctionArgs {
            number_rows: 1,
            args: vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(999)))],
            return_field: Arc::new(Field::new("", DataType::Boolean, false)),
            arg_fields: vec![Arc::new(Field::new("", DataType::Int64, true))],
        })?;

        // Then
        if let ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))) = result {
            Ok(())
        } else {
            exec_err!("Test should have returned a single scalar boolean false")
        }
    }

    #[test]
    fn invoke_test_with_array() -> Result<(), DataFusionError> {
        // Given
        let ageoff = AgeOff::new(1000);
        let mut vals_array = Int64Builder::new();
        vals_array.append_values(
            &[-45, 0, 1, 999, 1000, 1001, 10_000],
            &[true, true, true, true, true, true, true],
        );
        let vals = Arc::new(vals_array.finish());

        // When
        let result = ageoff.invoke_with_args(ScalarFunctionArgs {
            number_rows: 1,
            args: vec![ColumnarValue::Array(vals)],
            return_field: Arc::new(Field::new("", DataType::Boolean, false)),
            arg_fields: vec![Arc::new(Field::new("", DataType::Int64, true))],
        })?;

        // Then
        if let ColumnarValue::Array(result_array) = result {
            assert_eq!(result_array.len(), 7);
            let bool_array = result_array
                .as_boolean()
                .into_iter()
                .collect::<Option<Vec<_>>>();
            if let Some(values) = bool_array {
                assert_eq!(values, vec![false, false, false, false, true, true, true]);
                Ok(())
            } else {
                exec_err!("Test should not have returned nulls")
            }
        } else {
            exec_err!("Test should have returned a scalar array")
        }
    }
}
