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
    common::{exec_err, internal_err, plan_datafusion_err},
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
}

impl CastUDF {
    /// Constructs a new `CastUDF` for casting values from the specified input column type to the target output type
    /// without error on narrowing conversions.
    pub fn new(column_type: &DataType, output_type: &DataType) -> Self {
        Self {
            signature: Signature::exact(vec![column_type.clone()], Volatility::Immutable),
            output_type: output_type.clone(),
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
                ColumnarValue::Array(array) => {
                    // dynamic dispatch. Match the datatype to the type of sketch to update.
                    match array.data_type() {
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
                                "Row type {} not supported for {}",
                                array.data_type(),
                                self.name()
                            )
                        }
                    }
                }
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
                        "Row type {} not supported for {}",
                        x.data_type(),
                        self.name()
                    )
                }
            }
        }
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new("", self.output_type().clone(), false)))
    }

    fn evaluate_bounds(&self, input: &[&Interval]) -> Result<Interval> {
        const CAST_OPTIONS: CastOptions = CastOptions {
            safe: false,
            format_options: FormatOptions::new(),
        };
        let full_range_output = Interval::try_new(
            ScalarValue::min(self.output_type()).ok_or(plan_datafusion_err!(
                "Bounds type is not a numeric type {:?}",
                self.output_type()
            ))?,
            ScalarValue::max(self.output_type()).ok_or(plan_datafusion_err!(
                "Bounds type is not a numeric type {:?}",
                self.output_type()
            ))?,
        )?;

        let mut result = Interval::make_zero(self.output_type())?;
        for interval in input {
            let lower = interval.lower();
            // Will input interval fit in range of output data type?
            if lower.data_type().size() < self.output_type.size() {
                let casted = interval.cast_to(self.output_type(), &CAST_OPTIONS)?;
                result = result.union(casted)?;
            } else {
                // Input interval doesn't fit into output type (e.g. 64 bit interval into a 32bit interval)
                let widen_cast = full_range_output.cast_to(&lower.data_type(), &CAST_OPTIONS)?;
                if let Some(intersection) = interval.intersect(widen_cast)? {
                    let output_cast = intersection.cast_to(self.output_type(), &CAST_OPTIONS)?;
                    result = result.union(output_cast)?;
                }
            }
        }
        Ok(result)
    }

    fn preserves_lex_ordering(&self, _inputs: &[ExprProperties]) -> Result<bool> {
        // narrowing conversions will not preserve lex ordering
        Ok(false)
    }
}
