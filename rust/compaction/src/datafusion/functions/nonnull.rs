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
    execution::FunctionRegistry,
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

/// Lazily created user defined aggregate function (UDAF) for a non-nullable sum function.
static NON_NULL_SUM_UDAF: LazyLock<Arc<AggregateUDF>> = std::sync::LazyLock::new(|| {
    std::sync::Arc::new(AggregateUDF::from(NonNullable::new(
        sum_udaf().inner().clone(),
    )))
});

/// Lazily created user defined aggregate function (UDAF) for a non-nullable min function.
static NON_NULL_MIN_UDAF: LazyLock<Arc<AggregateUDF>> = std::sync::LazyLock::new(|| {
    std::sync::Arc::new(AggregateUDF::from(NonNullable::new(
        min_udaf().inner().clone(),
    )))
});

/// Lazily created user defined aggregate function (UDAF) for a non-nullable max function.
static NON_NULL_MAX_UDAF: LazyLock<Arc<AggregateUDF>> = std::sync::LazyLock::new(|| {
    std::sync::Arc::new(AggregateUDF::from(NonNullable::new(
        max_udaf().inner().clone(),
    )))
});

/// Summing aggregate function that won't produce nulls. If a null value is found, an execution error will occur.
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

/// Minimum aggregate function that won't produce nulls. If a null value is found, an execution error will occur.
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

/// Maxmimum aggregate function that won't produce nulls. If a null value is found, an execution error will occur.
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

/// Register the non-nullable versions of some simple aggregators so they
/// can be used within SQL queries.
pub fn register_non_nullables(registry: &mut dyn FunctionRegistry) {
    let _ = registry.register_udaf(NON_NULL_SUM_UDAF.clone());
    let _ = registry.register_udaf(NON_NULL_MIN_UDAF.clone());
    let _ = registry.register_udaf(NON_NULL_MAX_UDAF.clone());
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
#[allow(dead_code)]
pub fn non_nullable(func: &Arc<AggregateUDF>) -> Arc<AggregateUDF> {
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

/// Wraps an aggregate UDF function, but makes it non-nullable.
///
/// If any nulls are found during execution, an error is raised. Most functions are passed through to the wrapped
/// implementation. Some are exempted since the default implementation computes results based on other functions in
/// this trait. Specifically the following functions are not wrapped:
///  * [`AggregateUDFImpl::schema_name`]
///  * [`AggregateUDFImpl::window_function_schema_name`]
///  * [`AggregateUDFImpl::display_name`]
///  * [`AggregateUDFImpl::window_function_display_name`]
///
#[derive(Debug)]
pub struct NonNullable {
    /// The aggregate UDF function that performs some computation.
    pub inner: Arc<dyn AggregateUDFImpl>,
    pub func_name: String,
}

impl NonNullable {
    /// Wrap the given [`AggregateUDFImpl`] in a non-nullable
    /// wrapper.
    pub fn new(inner: Arc<dyn AggregateUDFImpl>) -> Self {
        let func_name = String::from("nonnullable_") + inner.name();
        Self { inner, func_name }
    }
}

impl From<Arc<dyn AggregateUDFImpl>> for NonNullable {
    fn from(value: Arc<dyn AggregateUDFImpl>) -> Self {
        Self::new(value)
    }
}

impl AggregateUDFImpl for NonNullable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.func_name
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
            .map(|acc| Box::new(NonNullableGroupsAccumulator(acc)) as Box<dyn GroupsAccumulator>)
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
struct NonNullableGroupsAccumulator(Box<dyn GroupsAccumulator>);

impl GroupsAccumulator for NonNullableGroupsAccumulator {
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

#[cfg(test)]
mod tests {
    use crate::{
        assert_error,
        datafusion::functions::nonnull::{
            NON_NULL_MAX_UDAF, NON_NULL_MIN_UDAF, NON_NULL_SUM_UDAF, NonNullable,
            NonNullableAccumulator, NonNullableGroupsAccumulator, non_null_max, non_null_min,
            non_null_sum, non_nullable, nullable_check, register_non_nullables,
        },
    };
    use arrow::{
        array::{ArrayRef, BooleanArray, Int64Builder},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::{
        common::{Statistics, arrow::array::Array, internal_err},
        error::{DataFusionError, Result},
        execution::{SessionStateBuilder, context::ExecutionProps},
        functions_aggregate::{
            min_max::{max_udaf, min_udaf},
            sum::{sum, sum_udaf},
        },
        logical_expr::{
            Accumulator, AggregateUDFImpl, Documentation, EmitTo, GroupsAccumulator, ReversedUDAF,
            SetMonotonicity, Signature, StatisticsArgs, Volatility,
            expr::{AggregateFunction, AggregateFunctionParams, WindowFunctionParams},
            function::{AccumulatorArgs, AggregateFunctionSimplification, StateFieldsArgs},
            lit,
            simplify::SimplifyContext,
            utils::AggregateOrderSensitivity,
        },
        physical_expr::LexOrdering,
        prelude::Expr,
        scalar::ScalarValue,
    };
    use mockall::predicate::*;
    use mockall::*;
    use std::{any::Any, collections::HashMap, sync::Arc};

    mock! {
        #[allow(clippy::ref_option_ref)]
        #[derive(Debug)]
        UDFImpl {}
        impl AggregateUDFImpl for UDFImpl {
            fn as_any(&self) -> &dyn Any;
            fn name(&self) -> &str;
            fn signature(&self) -> &Signature;
            fn return_type(&self, arg_types: &[DataType]) -> Result<DataType>;
            fn accumulator<'a>(&self, acc_args: AccumulatorArgs<'a>) -> Result<Box<dyn Accumulator>>;
            fn schema_name(&self, params: &AggregateFunctionParams) -> Result<String>;
            fn window_function_schema_name(
                &self,
                params: &WindowFunctionParams,
            ) -> Result<String>;
            fn display_name(&self, params: &AggregateFunctionParams) -> Result<String>;
            fn window_function_display_name(
                &self,
                params: &WindowFunctionParams,
            ) -> Result<String>;
            fn is_nullable(&self) -> bool;
            fn state_fields<'a>(&self, args: StateFieldsArgs<'a>) -> Result<Vec<Field>>;
            fn groups_accumulator_supported<'a>(&self, args: AccumulatorArgs<'a>) -> bool;
            fn create_groups_accumulator<'a>(
                &self,
                args: AccumulatorArgs<'a>,
            ) -> Result<Box<dyn GroupsAccumulator>>;
            fn aliases(&self) -> &[String];
            fn create_sliding_accumulator<'a>(
                &self,
                args: AccumulatorArgs<'a>,
            ) -> Result<Box<dyn Accumulator>> ;
            fn with_beneficial_ordering(
                self: Arc<Self>,
                beneficial_ordering: bool,
            ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> ;
            fn order_sensitivity(&self) -> AggregateOrderSensitivity;
            fn simplify(&self) -> Option<AggregateFunctionSimplification>;
            fn reverse_expr(&self) -> ReversedUDAF;
            fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> ;
            fn equals(&self, other: &dyn AggregateUDFImpl) -> bool;
            fn hash_value(&self) -> u64 ;
            fn is_descending(&self) -> Option<bool> ;
            fn value_from_stats<'a>(&self, statistics_args: &StatisticsArgs<'a>) -> Option<ScalarValue> ;
            fn default_value(&self, data_type: &DataType) -> Result<ScalarValue>;
            fn documentation(&self) -> Option<&'static Documentation>;
            fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity;
        }
        unsafe impl Send for UDFImpl {}
        unsafe impl Sync for UDFImpl {}
    }

    mock! {
        #[derive(Debug)]
        UDFAcc{}
        #[allow(clippy::ref_option_ref)]
        impl Accumulator for UDFAcc{
            fn update_batch(
                &mut self,
                values: &[Arc<dyn Array>],
            ) -> Result<(), DataFusionError>;
            fn evaluate(&mut self) -> Result<ScalarValue, DataFusionError>;
            fn size(&self) -> usize;
            fn state(&mut self) -> Result<Vec<ScalarValue>, DataFusionError>;
            fn merge_batch(
                &mut self,
                states: &[Arc<dyn Array>],
            ) -> Result<(), DataFusionError>;
            fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()>;
            fn supports_retract_batch(&self) -> bool;
        }
    }

    mock! {
        #[derive(Debug)]
        UDFGroupAcc{}
        #[allow(clippy::ref_option_ref)]
        impl GroupsAccumulator for UDFGroupAcc{
            fn update_batch<'a>(
                &mut self,
                values: &[Arc<dyn Array>],
                group_indices: &[usize],
                opt_filter: Option<&'a BooleanArray>,
                total_num_groups: usize,
            ) -> Result<(), DataFusionError>;
            fn evaluate(
                &mut self,
                emit_to: EmitTo,
            ) -> Result<Arc<dyn Array>, DataFusionError>;
            fn state(
                &mut self,
                emit_to: EmitTo,
            ) -> Result<Vec<Arc<dyn Array>>, DataFusionError>;
            fn merge_batch<'a>(
                &mut self,
                values: &[Arc<dyn Array>],
                group_indices: &[usize],
                opt_filter: Option<&'a BooleanArray>,
                total_num_groups: usize,
            ) -> Result<(), DataFusionError>;
            fn size(&self) -> usize;
            fn convert_to_state<'a>(
                &self,
                values: &[ArrayRef],
                opt_filter: Option<&'a BooleanArray>,
            ) -> Result<Vec<ArrayRef>>;
            fn supports_convert_to_state(&self) -> bool;
        }
    }

    #[test]
    fn non_null_sum_returns_correct_expr() -> Result<()> {
        // Given
        let expr = non_null_sum(lit(2));

        // When
        let Expr::AggregateFunction(aggregate) = expr else {
            return internal_err!("Expected a non nullable sum");
        };

        let expect_nonnullable = aggregate.func;
        let inner_func = expect_nonnullable
            .inner()
            .as_any()
            .downcast_ref::<NonNullable>()
            .expect("Inner implementation should be NonNullable")
            .inner
            .clone();

        // Then
        assert_eq!(*expect_nonnullable, **NON_NULL_SUM_UDAF);
        assert_eq!(&inner_func, &sum_udaf().inner().clone());
        Ok(())
    }

    #[test]
    fn non_null_max_returns_correct_expr() -> Result<()> {
        // Given
        let expr = non_null_max(lit(2));

        // When
        let Expr::AggregateFunction(aggregate) = expr else {
            return internal_err!("Expected a non nullable sum");
        };

        let expect_nonnullable = aggregate.func;
        let inner_func = expect_nonnullable
            .inner()
            .as_any()
            .downcast_ref::<NonNullable>()
            .expect("Inner implementation should be NonNullable")
            .inner
            .clone();

        // Then
        assert_eq!(*expect_nonnullable, **NON_NULL_MAX_UDAF);
        assert_eq!(&inner_func, &max_udaf().inner().clone());
        Ok(())
    }

    #[test]
    fn non_null_min_returns_correct_expr() -> Result<()> {
        // Given
        let expr = non_null_min(lit(2));

        // When
        let Expr::AggregateFunction(aggregate) = expr else {
            return internal_err!("Expected a non nullable sum");
        };

        let expect_nonnullable = aggregate.func;
        let inner_func = expect_nonnullable
            .inner()
            .as_any()
            .downcast_ref::<NonNullable>()
            .expect("Inner implementation should be NonNullable")
            .inner
            .clone();

        // Then
        assert_eq!(*expect_nonnullable, **NON_NULL_MIN_UDAF);
        assert_eq!(&inner_func, &min_udaf().inner().clone());
        Ok(())
    }

    #[test]
    fn should_register_functions() {
        // Given
        let mut sc = SessionStateBuilder::new().build();
        let expected = HashMap::from([
            ("nonnullable_sum".to_owned(), NON_NULL_SUM_UDAF.clone()),
            ("nonnullable_min".to_owned(), NON_NULL_MIN_UDAF.clone()),
            ("nonnullable_max".to_owned(), NON_NULL_MAX_UDAF.clone()),
        ]);

        // When
        register_non_nullables(&mut sc);
        let actual = sc.aggregate_functions();

        // Then
        assert_eq!(&expected, actual);
    }

    #[test]
    fn non_nullable_wraps_function() {
        // Given
        let aggregate = sum_udaf();

        // When
        let actual = non_nullable(&aggregate);
        let non_nullable = actual
            .inner()
            .as_any()
            .downcast_ref::<NonNullable>()
            .expect("Inner implementation should be NonNullable");
        let inner_func = non_nullable.inner.clone();

        // Then
        assert_eq!(&inner_func, &sum_udaf().inner().clone());
    }

    #[test]
    fn nullable_check_succeed() -> Result<()> {
        // Given
        let mut array_builder = Int64Builder::new();
        array_builder.append_values(&[1, 2, 3, 4, 5], &[true, true, true, true, true]);

        let array1 = array_builder.finish();

        let mut array_builder = Int64Builder::new();
        array_builder.append_values(&[7, 8], &[true, true]);

        let array2 = array_builder.finish();

        // When
        let input = [
            Arc::new(array1) as Arc<dyn Array>,
            Arc::new(array2) as Arc<dyn Array>,
        ];
        let output = nullable_check(&input)?;

        // Then
        assert_eq!(input, output);
        Ok(())
    }

    #[test]
    fn nullable_check_fail() {
        // Given
        let mut array_builder = Int64Builder::new();
        array_builder.append_values(&[1, 2, 3, 4, 5], &[true, true, true, true, true]);

        let array1 = array_builder.finish();

        let mut array_builder = Int64Builder::new();
        array_builder.append_values(&[7, 8], &[true, true]);
        array_builder.append_null();

        let array2 = array_builder.finish();

        // When
        let input = [
            Arc::new(array1) as Arc<dyn Array>,
            Arc::new(array2) as Arc<dyn Array>,
        ];
        let output = nullable_check(&input);

        // Then
        assert_error!(
            output,
            DataFusionError::Execution,
            "Null found in array index 1 in an non-nullable aggregation operator. Array datatype is Int64, length 3, logical null count 1, null count 1"
        );
    }

    #[test]
    fn should_not_call_as_any() {
        // Given
        let mut mock_udf = MockUDFImpl::new();
        let mock_any = Box::new("test") as Box<dyn Any>;
        mock_udf.expect_as_any().never().return_const(mock_any);
        mock_udf.expect_name().return_const("mockudf".to_owned());

        let nonnull = NonNullable::new(Arc::new(mock_udf));

        // When
        let any = nonnull.as_any();

        // Then
        any.downcast_ref::<NonNullable>()
            .expect("couldn't downcast to NonNullable");
    }

    #[test]
    fn should_build_name_appended() {
        // Given
        let mut mock_udf = MockUDFImpl::new();
        mock_udf.expect_name().return_const("mockudf".to_owned());

        let nonnull = NonNullable::new(Arc::new(mock_udf));

        // When
        let name = nonnull.name();

        // Then
        assert_eq!("nonnullable_mockudf", name);
    }

    #[test]
    fn should_call_signature() {
        // Given
        let mut mock_udf = MockUDFImpl::new();
        mock_udf.expect_name().return_const("mockudf".to_owned());
        mock_udf
            .expect_signature()
            .once()
            .return_const(Signature::nullary(Volatility::Immutable));

        let nonnull = NonNullable::new(Arc::new(mock_udf));

        // When
        let sig = nonnull.signature();

        // Then
        assert_eq!(&Signature::nullary(Volatility::Immutable), sig);
    }

    #[test]
    fn should_call_return_type() {
        // Given
        let mut mock_udf = MockUDFImpl::new();
        mock_udf.expect_name().return_const("mockudf".to_owned());
        mock_udf
            .expect_return_type()
            .once()
            .returning(|_| Ok(DataType::Int64));

        let nonnull = NonNullable::new(Arc::new(mock_udf));

        // When
        let ret = nonnull.return_type(&[]);

        // Then
        assert_eq!(
            DataType::Int64,
            ret.expect("Couldn't unwrap return_type result")
        );
    }

    #[test]
    fn should_call_accumulator_with_wrapped_instance() {
        // Given
        let mut mock_udf = MockUDFImpl::new();
        mock_udf.expect_name().return_const("mockudf".to_owned());
        mock_udf
            .expect_accumulator()
            .once()
            .return_once(|_| Result::Ok(Box::new(MockUDFAcc::new()) as Box<dyn Accumulator>));

        let nonnull = NonNullable::new(Arc::new(mock_udf));

        // When
        nonnull
            .accumulator(AccumulatorArgs {
                return_type: &DataType::Int64,
                schema: &Schema::empty(),
                ignore_nulls: false,
                ordering_req: LexOrdering::empty(),
                is_reversed: false,
                name: "test",
                is_distinct: false,
                exprs: &[],
            })
            .expect("No Accumulator instance");

        // Then - shouldn't fail
    }

    #[test]
    fn is_nullable_should_be_false() {
        // Given
        let mut mock_udf = MockUDFImpl::new();
        mock_udf.expect_name().return_const("mockudf".to_owned());
        let nonnull = NonNullable::new(Arc::new(mock_udf));

        // When
        let f = nonnull.is_nullable();

        // Then
        assert!(!f);
    }

    #[test]
    fn should_call_state_fields() {
        // Given
        let mut mock_udf = MockUDFImpl::new();
        mock_udf.expect_name().return_const("mockudf".to_owned());
        mock_udf
            .expect_state_fields()
            .once()
            .return_once(|_| Result::Ok(vec![]));

        let nonnull = NonNullable::new(Arc::new(mock_udf));

        // When
        let states = nonnull
            .state_fields(StateFieldsArgs {
                name: "test",
                input_types: &[],
                return_type: &DataType::Int64,
                ordering_fields: &[],
                is_distinct: false,
            })
            .expect("Couldn't unwrap state_fields result");

        // Then
        assert_eq!(Vec::<Field>::new(), states);
    }

    #[test]
    fn should_call_groups_accumulator_supported() {
        // Given
        let mut mock_udf = MockUDFImpl::new();
        mock_udf.expect_name().return_const("mockudf".to_owned());
        mock_udf
            .expect_groups_accumulator_supported()
            .once()
            .return_const(true);

        let nonnull = NonNullable::new(Arc::new(mock_udf));

        // When
        let group_supported = nonnull.groups_accumulator_supported(AccumulatorArgs {
            return_type: &DataType::Int64,
            schema: &Schema::empty(),
            ignore_nulls: false,
            ordering_req: LexOrdering::empty(),
            is_reversed: false,
            name: "test",
            is_distinct: false,
            exprs: &[],
        });

        // Then
        assert!(group_supported);
    }

    #[test]
    fn should_call_create_groups_accumulator_with_wrapped_instance() {
        // Given
        let mut mock_udf = MockUDFImpl::new();
        mock_udf.expect_name().return_const("mockudf".to_owned());
        mock_udf
            .expect_create_groups_accumulator()
            .once()
            .return_once(|_| {
                Result::Ok(Box::new(MockUDFGroupAcc::new()) as Box<dyn GroupsAccumulator>)
            });

        let nonnull = NonNullable::new(Arc::new(mock_udf));

        // When
        nonnull
            .create_groups_accumulator(AccumulatorArgs {
                return_type: &DataType::Int64,
                schema: &Schema::empty(),
                ignore_nulls: false,
                ordering_req: LexOrdering::empty(),
                is_reversed: false,
                name: "test",
                is_distinct: false,
                exprs: &[],
            })
            .expect("No GroupsAccumulator instance");

        // Then - shouldn't fail
    }

    #[test]
    fn should_return_no_aliases() {
        // Given
        let mut mock_udf = MockUDFImpl::new();
        mock_udf.expect_name().return_const("mockudf".to_owned());
        let nonnull = NonNullable::new(Arc::new(mock_udf));

        // When
        let aliases = nonnull.aliases();

        // Then
        assert_eq!(&[] as &[String], aliases);
    }

    #[test]
    fn should_call_create_sliding_accumulator_with_wrapped_instance() {
        // Given
        let mut mock_udf = MockUDFImpl::new();
        mock_udf.expect_name().return_const("mockudf".to_owned());
        mock_udf
            .expect_create_sliding_accumulator()
            .once()
            .return_once(|_| Result::Ok(Box::new(MockUDFAcc::new()) as Box<dyn Accumulator>));

        let nonnull = NonNullable::new(Arc::new(mock_udf));

        // When
        nonnull
            .create_sliding_accumulator(AccumulatorArgs {
                return_type: &DataType::Int64,
                schema: &Schema::empty(),
                ignore_nulls: false,
                ordering_req: LexOrdering::empty(),
                is_reversed: false,
                name: "test",
                is_distinct: false,
                exprs: &[],
            })
            .expect("No sliding accumulator instance");

        // Then - shouldn't fail
    }

    #[test]
    fn should_call_with_beneficial_ordering() {
        // Given
        let mut mock_udf = MockUDFImpl::new();
        mock_udf.expect_name().return_const("mockudf".to_owned());
        mock_udf
            .expect_with_beneficial_ordering()
            .once()
            .return_once(|_| Ok(None));

        let nonnull = Arc::new(NonNullable::new(Arc::new(mock_udf)));

        // When
        let ordering = nonnull
            .with_beneficial_ordering(false)
            .expect("couldn't unwrap beneficial_ordering result");

        // Then
        assert_eq!(None, ordering);
    }

    #[test]
    fn should_call_order_sensitivity() {
        // Given
        let mut mock_udf = MockUDFImpl::new();
        mock_udf.expect_name().return_const("mockudf".to_owned());
        mock_udf
            .expect_order_sensitivity()
            .once()
            .return_const(AggregateOrderSensitivity::Insensitive);
        let nonnull = NonNullable::new(Arc::new(mock_udf));

        // When
        let ordering = nonnull.order_sensitivity();

        // Then
        assert_eq!(AggregateOrderSensitivity::Insensitive, ordering);
    }

    #[test]
    fn should_call_simplify() {
        //Given
        let mut mock_udf = MockUDFImpl::new();
        mock_udf.expect_name().return_const("mockudf".to_owned());
        mock_udf
            .expect_simplify()
            .once()
            .returning(|| Some(Box::new(|_, _| Ok(sum(lit(1))))));
        let nonnull = NonNullable::new(Arc::new(mock_udf));
        let test_agg_function =
            AggregateFunction::new_udf(sum_udaf(), vec![], false, None, None, None);

        // When
        let simplified_expr = nonnull.simplify().expect("couldn't unwrap simplify result");
        // call the function
        let simplified_result = simplified_expr(
            test_agg_function,
            &SimplifyContext::new(&ExecutionProps::new()),
        );

        // Then - check called function returns a non-null with the correct inner expression
        if let Ok(Expr::AggregateFunction(AggregateFunction { func, params: _ })) =
            simplified_result
        {
            let nonnull = func
                .inner()
                .as_any()
                .downcast_ref::<NonNullable>()
                .expect("Should be a nonnullable intance");

            assert_eq!(nonnull.inner.name(), "sum");
        } else {
            panic!("Incorrect result from call to simplify");
        }
    }

    #[test]
    fn should_return_identical_reverse() {
        // Given
        let mut mock_udf = MockUDFImpl::new();
        mock_udf.expect_name().return_const("mockudf".to_owned());
        let nonnull = NonNullable::new(Arc::new(mock_udf));

        // When
        let reverse = nonnull.reverse_expr();

        // Then
        assert!(matches!(reverse, ReversedUDAF::Identical));
    }

    #[test]
    fn should_call_coerce_types() {
        // Given
        let mut mock_udf = MockUDFImpl::new();
        mock_udf.expect_name().return_const("mockudf".to_owned());
        mock_udf
            .expect_coerce_types()
            .once()
            .return_once(|_| Ok(vec![]));
        let nonnull = NonNullable::new(Arc::new(mock_udf));

        // When
        let coerced = nonnull
            .coerce_types(&[])
            .expect("couldn't unwrap coerced_types result");

        // Then
        assert_eq!(Vec::<DataType>::new(), coerced);
    }

    #[test]
    fn should_call_is_descending() {
        // Given
        let mut mock_udf = MockUDFImpl::new();
        mock_udf.expect_name().return_const("mockudf".to_owned());
        mock_udf
            .expect_is_descending()
            .once()
            .return_const(Some(false));
        let nonnull = NonNullable::new(Arc::new(mock_udf));

        // When
        let descending = nonnull.is_descending();

        // Then
        assert_eq!(Some(false), descending);
    }

    #[test]
    fn should_call_value_from_stats() {
        // Given
        let mut mock_udf = MockUDFImpl::new();
        mock_udf.expect_name().return_const("mockudf".to_owned());
        mock_udf.expect_value_from_stats().once().return_const(None);
        let nonnull = NonNullable::new(Arc::new(mock_udf));

        // When
        let stats_value = nonnull.value_from_stats(&StatisticsArgs {
            statistics: &Statistics::new_unknown(&Schema::empty()),
            return_type: &DataType::Int64,
            is_distinct: false,
            exprs: &[],
        });

        // Then
        assert_eq!(None, stats_value);
    }

    #[test]
    fn should_call_default_value() {
        // Given
        let mut mock_udf = MockUDFImpl::new();
        mock_udf.expect_name().return_const("mockudf".to_owned());
        mock_udf
            .expect_default_value()
            .once()
            .returning(|_| Ok(ScalarValue::Null));
        let nonnull = NonNullable::new(Arc::new(mock_udf));

        // When
        let default = nonnull
            .default_value(&DataType::Int64)
            .expect("couldn't unwrap default value");

        // Then
        assert_eq!(ScalarValue::Null, default);
    }

    #[test]
    fn should_call_documentation() {
        // Given
        let mut mock_udf = MockUDFImpl::new();
        mock_udf.expect_name().return_const("mockudf".to_owned());
        mock_udf.expect_documentation().once().return_const(None);
        let nonnull = NonNullable::new(Arc::new(mock_udf));

        // When
        let docs = nonnull.documentation();

        // Then
        assert!(docs.is_none());
    }

    #[test]
    fn should_call_set_monotonicity() {
        // Given
        let mut mock_udf = MockUDFImpl::new();
        mock_udf.expect_name().return_const("mockudf".to_owned());
        mock_udf
            .expect_set_monotonicity()
            .once()
            .return_const(SetMonotonicity::NotMonotonic);
        let nonnull = NonNullable::new(Arc::new(mock_udf));

        // When
        let monotonicity = nonnull.set_monotonicity(&DataType::Int64);

        // Then
        assert_eq!(SetMonotonicity::NotMonotonic, monotonicity);
    }

    // NonNullableAccumulator tests

    #[test]
    fn accumulator_should_call_update_batch_success() {
        // Given
        let mut mock_acc = MockUDFAcc::new();
        mock_acc
            .expect_update_batch()
            .once()
            .return_once(|_| Ok(()));
        let mut acc = NonNullableAccumulator(Box::new(mock_acc));

        // When
        let result = acc.update_batch(&[]);

        // Then
        assert!(result.is_ok());
    }

    #[test]
    fn accumulator_should_fail_on_nullable_entry_update_batch() {
        // Given
        let mut mock_acc = MockUDFAcc::new();
        mock_acc.expect_update_batch().never();
        let mut acc = NonNullableAccumulator(Box::new(mock_acc));
        let mut builder = Int64Builder::new();
        builder.append_value(1);
        builder.append_null();
        let array = builder.finish();

        // When
        let result = acc.update_batch(&[Arc::new(array)]);

        // Then
        assert!(result.is_err());
    }

    #[test]
    fn accumulator_should_call_evaluate() {
        // Given
        let mut mock_acc = MockUDFAcc::new();
        mock_acc
            .expect_evaluate()
            .once()
            .return_once(|| Ok(ScalarValue::Null));
        let mut acc = NonNullableAccumulator(Box::new(mock_acc));

        // When
        let _ = acc.evaluate();

        // Then - mock should report one call
    }

    #[test]
    fn accumulator_should_call_size() {
        // Given
        let mut mock_acc = MockUDFAcc::new();
        mock_acc.expect_size().once().return_const(32usize);
        let acc = NonNullableAccumulator(Box::new(mock_acc));

        // When
        let _ = acc.size();

        // Then - mock should report one call
    }

    #[test]
    fn accumulator_should_call_state() {
        // Given
        let mut mock_acc = MockUDFAcc::new();
        mock_acc.expect_state().once().return_once(|| Ok(vec![]));
        let mut acc = NonNullableAccumulator(Box::new(mock_acc));

        // When
        let _ = acc.state();

        // Then - mock should report one call
    }

    #[test]
    fn accumulator_should_call_merge_batch() {
        // Given
        let mut mock_acc = MockUDFAcc::new();
        mock_acc.expect_merge_batch().once().return_once(|_| Ok(()));
        let mut acc = NonNullableAccumulator(Box::new(mock_acc));

        // When
        let _ = acc.merge_batch(&[]);

        // Then - mock should report one call
    }

    #[test]
    fn accumulator_should_call_retract_batch_success() {
        // Given
        let mut mock_acc = MockUDFAcc::new();
        mock_acc
            .expect_retract_batch()
            .once()
            .return_once(|_| Ok(()));
        let mut acc = NonNullableAccumulator(Box::new(mock_acc));

        // When
        let result = acc.retract_batch(&[]);

        // Then
        assert!(result.is_ok());
    }

    #[test]
    fn accumulator_should_fail_on_nullable_entry_retract_batch() {
        // Given
        let mut mock_acc = MockUDFAcc::new();
        mock_acc.expect_retract_batch().never();
        let mut acc = NonNullableAccumulator(Box::new(mock_acc));
        let mut builder = Int64Builder::new();
        builder.append_value(1);
        builder.append_null();
        let array = builder.finish();

        // When
        let result = acc.retract_batch(&[Arc::new(array)]);

        // Then
        assert!(result.is_err());
    }

    #[test]
    fn accumulator_should_call_supports_merge_batch_return_true() {
        // Given
        let mut mock_acc = MockUDFAcc::new();
        mock_acc
            .expect_supports_retract_batch()
            .once()
            .return_const(true);
        let acc = NonNullableAccumulator(Box::new(mock_acc));

        // When
        let result = acc.supports_retract_batch();

        // Then
        assert!(result);
    }

    // NonNullableGroupsAccumulator tests

    #[test]
    fn groups_accumulator_should_call_update_batch_success() {
        // Given
        let mut mock_group_acc = MockUDFGroupAcc::new();
        mock_group_acc
            .expect_update_batch()
            .once()
            .return_once(|_, _, _, _| Ok(()));
        let mut group_acc = NonNullableGroupsAccumulator(Box::new(mock_group_acc));

        // When
        let result = group_acc.update_batch(&[], &[], None, 1);

        // Then
        assert!(result.is_ok());
    }

    #[test]
    fn groups_accumulator_should_fail_on_nullable_entry_update_batch() {
        // Given
        let mut mock_group_acc = MockUDFGroupAcc::new();
        mock_group_acc.expect_update_batch().never();
        let mut group_acc = NonNullableGroupsAccumulator(Box::new(mock_group_acc));
        let mut builder = Int64Builder::new();
        builder.append_value(1);
        builder.append_null();
        let array = builder.finish();

        // When
        let result = group_acc.update_batch(&[Arc::new(array)], &[], None, 1);

        // Then
        assert!(result.is_err());
    }

    #[test]
    fn groups_accumulator_should_call_evaluate() {
        // Given
        let mut mock_group_acc = MockUDFGroupAcc::new();
        mock_group_acc
            .expect_evaluate()
            .once()
            .return_once(|_| Ok(Arc::new(Int64Builder::new().finish())));
        let mut group_acc = NonNullableGroupsAccumulator(Box::new(mock_group_acc));

        // When
        let _ = group_acc.evaluate(EmitTo::All);

        // Then - mock should report one call
    }

    #[test]
    fn groups_accumulator_should_call_state() {
        // Given
        let mut mock_group_acc = MockUDFGroupAcc::new();
        mock_group_acc
            .expect_state()
            .once()
            .return_once(|_| Ok(vec![]));
        let mut group_acc = NonNullableGroupsAccumulator(Box::new(mock_group_acc));

        // When
        let _ = group_acc.state(EmitTo::All);

        // Then - mock should report one call
    }

    #[test]
    fn groups_accumulator_should_call_merge_batch() {
        // Given
        let mut mock_group_acc = MockUDFGroupAcc::new();
        mock_group_acc
            .expect_merge_batch()
            .once()
            .return_once(|_, _, _, _| Ok(()));
        let mut group_acc = NonNullableGroupsAccumulator(Box::new(mock_group_acc));

        // When
        let _ = group_acc.merge_batch(&[], &[], None, 1);

        // Then - mock should report one call
    }

    #[test]
    fn groups_accumulator_should_call_size() {
        // Given
        let mut mock_group_acc = MockUDFGroupAcc::new();
        mock_group_acc.expect_size().once().return_const(32usize);
        let group_acc = NonNullableGroupsAccumulator(Box::new(mock_group_acc));

        // When
        let _ = group_acc.size();

        // Then - mock should report one call
    }

    #[test]
    fn groups_accumulator_should_call_convert_to_state_success() {
        // Given
        let mut mock_group_acc = MockUDFGroupAcc::new();
        mock_group_acc
            .expect_convert_to_state()
            .once()
            .return_once(|_, _| Ok(vec![]));
        let group_acc = NonNullableGroupsAccumulator(Box::new(mock_group_acc));

        // When
        let result = group_acc.convert_to_state(&[], None);

        // Then
        assert!(result.is_ok());
    }

    #[test]
    fn groups_accumulator_should_fail_on_nullable_entry_convert_to_state() {
        // Given
        let mut mock_group_acc = MockUDFGroupAcc::new();
        mock_group_acc.expect_convert_to_state().never();
        let group_acc = NonNullableGroupsAccumulator(Box::new(mock_group_acc));
        let mut builder = Int64Builder::new();
        builder.append_value(1);
        builder.append_null();
        let array = builder.finish();

        // When
        let result = group_acc.convert_to_state(&[Arc::new(array)], None);

        // Then
        assert!(result.is_err());
    }

    #[test]
    fn groups_accumulator_should_call_supports_convert_to_state_return_true() {
        // Given
        let mut mock_group_acc = MockUDFGroupAcc::new();
        mock_group_acc
            .expect_supports_convert_to_state()
            .once()
            .return_const(true);
        let group_acc = NonNullableGroupsAccumulator(Box::new(mock_group_acc));

        // When
        let result = group_acc.supports_convert_to_state();
        // Then
        assert!(result);
    }
}
