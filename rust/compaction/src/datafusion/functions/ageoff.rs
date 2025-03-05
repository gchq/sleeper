/// Implementation of [`ScalarUDF`] for age off filtering.
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
    array::{AsArray, BooleanBuilder},
    datatypes::{DataType, Int64Type},
};
use datafusion::{
    common::internal_err,
    error::{DataFusionError, Result},
    logical_expr::{
        interval_arithmetic::Interval, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature,
        Volatility,
    },
    scalar::ScalarValue,
};
use std::{
    any::Any,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use super::Filter;

/// A filtering expression (returns bool) for an integer column based upon a given threshold.
/// If the value in a given column is lower than the given threshold, it will be filtered out.
#[derive(Debug)]
pub struct AgeOff {
    /// Threshold value (seconds since UNIX epoch)
    threshold: i64,
    /// Signature for this filter expression
    signature: Signature,
}

impl AgeOff {
    /// Create a new sketch function based on the schema of the row key fields.
    ///
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

    fn try_from(value: &Filter) -> std::result::Result<Self, Self::Error> {
        if let Filter::Ageoff { column: _, max_age } = value {
            // Figure out max_age in as a millisecond threshold from current time
            let threshold = if *max_age >= 0 {
                SystemTime::now().checked_sub(Duration::from_secs(max_age.unsigned_abs()))
            } else {
                SystemTime::now().checked_add(Duration::from_secs(max_age.unsigned_abs()))
            }
            // Convert Option to Result with DataFusionError
            .ok_or(DataFusionError::Configuration(
                "Age off filter max_age not representable as timestamp".into(),
            ))?
            // Figure out duration since unix epoch
            .duration_since(UNIX_EPOCH)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            // Finally, convert to raw integer
            .as_secs() as i64;
            Ok(AgeOff::new(threshold))
        } else {
            internal_err!("AgeOff try_from called on non Filter::AgeOff variant")
        }
    }
}

impl ScalarUDFImpl for AgeOff {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
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
            return internal_err!(
                "AgeOff UDF called with {} input columns, only accepts 1",
                args.args.len()
            );
        }
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Boolean(Some(self.retain(*v))),
            )),
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
            _ => internal_err!(
                "Age off called with unsupported column datatype {:?}",
                args.args[0].data_type()
            ),
        }
    }

    fn evaluate_bounds(&self, _input: &[&Interval]) -> Result<Interval> {
        Interval::make_unbounded(&DataType::Boolean)
    }
}
