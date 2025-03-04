use std::any::Any;

use arrow::datatypes::DataType;
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
use datafusion::{
    common::{internal_datafusion_err, internal_err},
    error::Result,
    logical_expr::{Expr, ScalarUDFImpl, Signature, Volatility},
};

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

    fn schema_name(&self, args: &[datafusion::prelude::Expr]) -> datafusion::error::Result<String> {
        Ok(std::format!(
            "{}({})",
            self.name(),
            schema_name_from_exprs_comma_separated_without_space(args)?
        ))
    }

    fn return_type_from_exprs(
        &self,
        _args: &[datafusion::prelude::Expr],
        _schema: &dyn datafusion::common::ExprSchema,
        arg_types: &[arrow::datatypes::DataType],
    ) -> datafusion::error::Result<arrow::datatypes::DataType> {
        self.return_type(arg_types)
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

    fn is_nullable(
        &self,
        _args: &[datafusion::prelude::Expr],
        _schema: &dyn datafusion::common::ExprSchema,
    ) -> bool {
        true
    }

    fn invoke(
        &self,
        _args: &[datafusion::logical_expr::ColumnarValue],
    ) -> datafusion::error::Result<datafusion::logical_expr::ColumnarValue> {
        datafusion::common::not_impl_err!(
            "Function {} does not implement invoke but called",
            self.name()
        )
    }

    fn invoke_batch(
        &self,
        args: &[datafusion::logical_expr::ColumnarValue],
        number_rows: usize,
    ) -> datafusion::error::Result<datafusion::logical_expr::ColumnarValue> {
        match args.is_empty() {
            true =>
            {
                #[allow(deprecated)]
                self.invoke_no_args(number_rows)
            }
            false =>
            {
                #[allow(deprecated)]
                self.invoke(args)
            }
        }
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::error::Result<datafusion::logical_expr::ColumnarValue> {
        self.invoke_batch(&args.args, args.number_rows)
    }

    fn invoke_no_args(
        &self,
        _number_rows: usize,
    ) -> datafusion::error::Result<datafusion::logical_expr::ColumnarValue> {
        datafusion::common::not_impl_err!(
            "Function {} does not implement invoke_no_args but called",
            self.name()
        )
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn simplify(
        &self,
        args: Vec<datafusion::prelude::Expr>,
        _info: &dyn datafusion::logical_expr::simplify::SimplifyInfo,
    ) -> datafusion::error::Result<datafusion::logical_expr::simplify::ExprSimplifyResult> {
        Ok(datafusion::logical_expr::simplify::ExprSimplifyResult::Original(args))
    }

    fn short_circuits(&self) -> bool {
        false
    }

    fn evaluate_bounds(
        &self,
        _input: &[&datafusion::logical_expr::interval_arithmetic::Interval],
    ) -> datafusion::error::Result<datafusion::logical_expr::interval_arithmetic::Interval> {
        // We cannot assume the input datatype is the same of output type.
        datafusion::logical_expr::interval_arithmetic::Interval::make_unbounded(
            &arrow::datatypes::DataType::Null,
        )
    }

    fn propagate_constraints(
        &self,
        _interval: &datafusion::logical_expr::interval_arithmetic::Interval,
        _inputs: &[&datafusion::logical_expr::interval_arithmetic::Interval],
    ) -> datafusion::error::Result<
        Option<Vec<datafusion::logical_expr::interval_arithmetic::Interval>>,
    > {
        Ok(Some(std::vec![]))
    }

    fn output_ordering(
        &self,
        inputs: &[datafusion::logical_expr::sort_properties::ExprProperties],
    ) -> datafusion::error::Result<datafusion::logical_expr::sort_properties::SortProperties> {
        if !self.preserves_lex_ordering(inputs)? {
            return Ok(datafusion::logical_expr::sort_properties::SortProperties::Unordered);
        }

        let Some(first_order) = inputs.first().map(|p| &p.sort_properties) else {
            return Ok(datafusion::logical_expr::sort_properties::SortProperties::Singleton);
        };

        if inputs
            .iter()
            .skip(1)
            .all(|input| &input.sort_properties == first_order)
        {
            Ok(*first_order)
        } else {
            Ok(datafusion::logical_expr::sort_properties::SortProperties::Unordered)
        }
    }

    fn preserves_lex_ordering(
        &self,
        _inputs: &[datafusion::logical_expr::sort_properties::ExprProperties],
    ) -> datafusion::error::Result<bool> {
        Ok(false)
    }

    fn coerce_types(
        &self,
        _arg_types: &[arrow::datatypes::DataType],
    ) -> datafusion::error::Result<Vec<arrow::datatypes::DataType>> {
        datafusion::common::not_impl_err!(
            "Function {} does not implement coerce_types",
            self.name()
        )
    }

    fn equals(&self, other: &dyn ScalarUDFImpl) -> bool {
        self.name() == other.name() && self.signature() == other.signature()
    }

    fn hash_value(&self) -> u64 {
        let hasher = &mut std::hash::DefaultHasher::new();
        self.name().hash(hasher);
        self.signature().hash(hasher);
        hasher.finish()
    }

    fn documentation(&self) -> Option<&datafusion::logical_expr::Documentation> {
        None
    }
}
