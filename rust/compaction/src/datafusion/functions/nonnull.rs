//! Contains code and functions for implementation of a non-nullable wrapper
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
    array::{Array, ArrayRef, BooleanArray},
    datatypes::{DataType, Field},
};
use datafusion::{
    common::{exec_err, plan_err},
    error::Result,
    functions_aggregate::{
        min_max::{max_udaf, min_udaf},
        sum::sum_udaf,
    },
    logical_expr::{
        Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, EmitTo, GroupsAccumulator,
        ReversedUDAF, SetMonotonicity, Signature, StatisticsArgs,
        expr::{AggregateFunction, AggregateFunctionParams},
        function::{AccumulatorArgs, AggregateFunctionSimplification, StateFieldsArgs},
        simplify::SimplifyInfo,
        utils::AggregateOrderSensitivity,
    },
    prelude::Expr,
    scalar::ScalarValue,
};
use std::{
    any::Any,
    sync::{Arc, LazyLock},
};

static NON_NULL_SUM_UDAF: LazyLock<Arc<AggregateUDF>> = std::sync::LazyLock::new(|| {
    std::sync::Arc::new(AggregateUDF::from(NonNullable::new(
        sum_udaf().inner().clone(),
    )))
});

static NON_NULL_MIN_UDAF: LazyLock<Arc<AggregateUDF>> = std::sync::LazyLock::new(|| {
    std::sync::Arc::new(AggregateUDF::from(NonNullable::new(
        min_udaf().inner().clone(),
    )))
});

static NON_NULL_MAX_UDAF: LazyLock<Arc<AggregateUDF>> = std::sync::LazyLock::new(|| {
    std::sync::Arc::new(AggregateUDF::from(NonNullable::new(
        max_udaf().inner().clone(),
    )))
});

pub fn non_null_sum(expression: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new_udf(
        NON_NULL_SUM_UDAF.clone(),
        vec![expression],
        false,
        None,
        None,
        None,
    ))
}

pub fn non_null_min(expression: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new_udf(
        NON_NULL_MIN_UDAF.clone(),
        vec![expression],
        false,
        None,
        None,
        None,
    ))
}

pub fn non_null_max(expression: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new_udf(
        NON_NULL_MAX_UDAF.clone(),
        vec![expression],
        false,
        None,
        None,
        None,
    ))
}

/// Wraps an aggregate expression in a non-nullable version of it.
///
/// Some aggregate expression like [`datafusion::functions_aggregate::sum::Sum`] always return nullable results. This can change the result when summed
/// on a non-nullable column to include nullable columns. This may not be desirable, so this function will wrap a
/// non-nullable wrapper around it.
///
/// The returned [`Accumulator`] and [`GroupsAccumulator`] implementations perform a null-check on input values to ensure
/// the non-nullability of the input.
///
/// # Errors
/// This function must only be called on [`Expr::AggregateFunction`] variants.
pub fn non_nullable(func: Arc<AggregateUDF>) -> Arc<AggregateUDF> {
    let non_null: Arc<NonNullable> = Arc::new(func.inner().clone().into());
    Arc::new(AggregateUDF::new_from_shared_impl(non_null))
}

/// Checks the arrays for null values.
///
/// This is based on the result of calling [`Array::is_nullable`].
///
/// # Errors
/// If any array in the given slice reports nullable values.
pub fn nullable_check(arrays: &[ArrayRef]) -> Result<&[ArrayRef]> {
    for (index, array) in arrays.iter().enumerate() {
        if array.is_nullable() {
            return exec_err!(
                "Null found in array index {index} in an non-nullable aggregation operator. Array datatype is {:?}, length {:?}, logical null count {:?}, null count {:?}",
                array.data_type(),
                array.len(),
                array.logical_null_count(),
                array.null_count()
            );
        }
    }
    Ok(arrays)
}

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
                    inner_func(func, simplify_info).and_then(|original_expr| match original_expr {
                        Expr::AggregateFunction(AggregateFunction { func, params }) => {
                            let AggregateFunctionParams {
                                args,
                                distinct,
                                filter,
                                null_treatment,
                                order_by,
                            } = params;
                            Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                                Arc::new(AggregateUDF::new_from_impl(NonNullable::new(
                                    func.inner().clone(),
                                ))),
                                args,
                                distinct,
                                filter,
                                order_by,
                                null_treatment,
                            )))
                        }
                        _ => {
                            plan_err!("Invalid aggregate expression {:?}", original_expr)
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
        self.0.update_batch(nullable_check(values)?)
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
        self.0.retract_batch(nullable_check(values)?)
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
        self.0.update_batch(
            nullable_check(values)?,
            group_indices,
            opt_filter,
            total_num_groups,
        )
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
        self.0.convert_to_state(nullable_check(values)?, opt_filter)
    }

    fn supports_convert_to_state(&self) -> bool {
        self.0.supports_convert_to_state()
    }
}
