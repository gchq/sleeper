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
use arrow::datatypes::DataType;
use datafusion::{
    common::internal_err,
    error::{DataFusionError, Result},
    logical_expr::{
        interval_arithmetic::Interval, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature,
        Volatility,
    },
};
use std::{
    any::Any,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use super::Filter;

/// A filtering expression (returns bool) for an integer column based upon a given threshold.
/// If the value in a given column is lower than the given threshold, it will be filtered out.
#[derive(Debug)]
pub struct AgeOff {
    /// Threshold value
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
            signature: Signature::numeric(1, Volatility::Immutable),
        }
    }
}

impl TryFrom<&Filter> for AgeOff {
    type Error = DataFusionError;

    fn try_from(value: &Filter) -> std::result::Result<Self, Self::Error> {
        if let Filter::Ageoff { column, max_age } = value {
            // Figure out max_age in as a millisecond threshold from current time
            let threshold = if *max_age >= 0 {
                SystemTime::now().checked_sub(Duration::from_millis(max_age.unsigned_abs()))
            } else {
                SystemTime::now().checked_add(Duration::from_millis(max_age.unsigned_abs()))
            }
            // Convert Option to Result with DataFusionError
            .ok_or(DataFusionError::Configuration(
                "Age off filter max_age not representable as timestamp".into(),
            ))?
            // Figure out duration since unix epoch
            .duration_since(UNIX_EPOCH)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            // Finally, convert to raw integer
            .as_millis() as i64;
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("Expected return_type_from_args, found call to return_type")
    }

    fn return_type_from_args(
        &self,
        args: datafusion::logical_expr::ReturnTypeArgs,
    ) -> datafusion::error::Result<datafusion::logical_expr::ReturnInfo> {
        let return_type = self.return_type(args.arg_types)?;
        Ok(datafusion::logical_expr::ReturnInfo::new_nullable(
            return_type,
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.invoke_batch(&args.args, args.number_rows)
    }

    fn evaluate_bounds(&self, _input: &[&Interval]) -> Result<Interval> {
        Interval::make_unbounded(&DataType::Boolean)
    }
}
