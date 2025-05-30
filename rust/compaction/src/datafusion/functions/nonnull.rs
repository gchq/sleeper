//! Contains code and functions for implementation of a _non-nullable_ wrapper
//! for aggregate UDF that may be nullable.
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
    array::{ArrayRef, BooleanArray},
    datatypes::{DataType, Field},
};
use datafusion::{
    common::exec_err,
    error::Result,
    logical_expr::{
        Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, EmitTo, GroupsAccumulator,
        ReversedUDAF, SetMonotonicity, Signature, StatisticsArgs,
        expr::AggregateFunction,
        function::{AccumulatorArgs, AggregateFunctionSimplification, StateFieldsArgs},
        simplify::SimplifyInfo,
        utils::AggregateOrderSensitivity,
    },
    prelude::Expr,
    scalar::ScalarValue,
};
use std::{any::Any, sync::Arc};

/// Wraps an aggregate UDF function, but makes it non-nullable. If any nulls are found
/// during execution, an error is raised.
#[derive(Debug)]
pub struct NonNullable {
    /// The aggregate UDF function that performs some computation.
    pub inner: Arc<dyn AggregateUDFImpl>,
}

impl NonNullable {
    /// Wrap the given [`AggregateUDFImpl`] in a non-nullable
    /// wrapper.
    pub fn new(inner: Arc<dyn AggregateUDFImpl>) -> Self {
        Self { inner }
    }
}

impl From<Arc<dyn AggregateUDFImpl>> for NonNullable {
    fn from(value: Arc<dyn AggregateUDFImpl>) -> Self {
        Self { inner: value }
    }
}

pub fn nullable_check(array: &ArrayRef) -> Result<()> {
    if array.is_nullable() {
        exec_err!("Null found in array in an Non-nullable aggregation")
    } else {
        Ok(())
    }
}

impl AggregateUDFImpl for NonNullable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "nonnullable"
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner
            .accumulator(acc_args)
            .map(|acc| Box::new(NonNullableAccumulator(acc)) as Box<dyn Accumulator>)
    }

    /// This aggregation function doesn't contain nullable values.
    fn is_nullable(&self) -> bool {
        false
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        self.inner.state_fields(args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.inner.groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        self.inner
            .create_groups_accumulator(args)
            .map(|acc| Box::new(NonNullableGroupAccumulator(acc)) as Box<dyn GroupsAccumulator>)
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner
            .create_sliding_accumulator(args)
            .map(|acc| Box::new(NonNullableAccumulator(acc)) as Box<dyn Accumulator>)
    }

    fn with_beneficial_ordering(
        self: Arc<Self>,
        beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        self.inner
            .clone()
            .with_beneficial_ordering(beneficial_ordering)
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner.order_sensitivity()
    }

    fn simplify(&self) -> Option<AggregateFunctionSimplification> {
        self.inner.simplify().map(|inner_func| {
            Box::new(
                move |func: AggregateFunction, simplify_info: &dyn SimplifyInfo| {
                    inner_func(func, simplify_info).map(|original_expr| match original_expr {
                        Expr::AggregateFunction(udf) => {
                            let f = udf.func;
                            let g = f.inner().to_owned();
                            let nonnull = NonNullable::new(g);
                            let p = AggregateUDF::new_from_impl(nonnull);
                            let foo = AggregateFunction::new_udf(
                                Arc::new(p),
                                udf.params.args,
                                udf.params.distinct,
                                udf.params.filter,
                                udf.params.order_by,
                                udf.params.null_treatment,
                            );
                            Expr::AggregateFunction(foo)
                        }
                        _ => {
                            unimplemented!();
                        }
                    })
                },
            ) as Box<dyn Fn(AggregateFunction, &dyn SimplifyInfo) -> Result<Expr>>
        })
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn is_descending(&self) -> Option<bool> {
        self.inner.is_descending()
    }

    fn value_from_stats(&self, statistics_args: &StatisticsArgs) -> Option<ScalarValue> {
        self.inner.value_from_stats(statistics_args)
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        self.inner.default_value(data_type)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }

    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        self.inner.set_monotonicity(data_type)
    }
}

/// Wraps the accumulation function with an [`Accumulator`] that checks the incoming
/// arrays for nulls.
#[derive(Debug)]
struct NonNullableAccumulator(Box<dyn Accumulator>);

impl Accumulator for NonNullableAccumulator {
    /// Performs a null check on the array.
    ///
    /// # Errors
    /// Will produce an error if there are null values present.
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for v in values {
            nullable_check(v)?;
        }
        self.0.update_batch(values)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.0.evaluate()
    }

    fn size(&self) -> usize {
        self.0.size() + std::mem::size_of::<Self>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.0.state()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.0.merge_batch(states)
    }

    /// Performs a null check on the array.
    ///
    /// # Errors
    /// Will produce an error if there are null values present.
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for v in values {
            nullable_check(v)?;
        }
        self.0.retract_batch(values)
    }

    fn supports_retract_batch(&self) -> bool {
        self.0.supports_retract_batch()
    }
}

/// Wraps the accumulation function with an [`Accumulator`] that checks the incoming
/// arrays for nulls.
struct NonNullableGroupAccumulator(Box<dyn GroupsAccumulator>);

impl GroupsAccumulator for NonNullableGroupAccumulator {
    /// Performs a null check on the array.
    ///
    /// # Errors
    /// Will produce an error if there are null values present.
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        for v in values {
            nullable_check(v)?;
        }
        self.0
            .update_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        self.0.evaluate(emit_to)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.0.state(emit_to)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.0
            .merge_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn size(&self) -> usize {
        self.0.size() + std::mem::size_of::<Self>()
    }

    /// Performs a null check on the array.
    ///
    /// # Errors
    /// Will produce an error if there are null values present.
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        for v in values {
            nullable_check(v)?;
        }
        self.0.convert_to_state(values, opt_filter)
    }

    fn supports_convert_to_state(&self) -> bool {
        self.0.supports_convert_to_state()
    }
}
