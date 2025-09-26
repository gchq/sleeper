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
use crate::filter_aggregation_config::{
    aggregate::{AggOp, Aggregate, MapAggregateOp},
    filter::Filter,
};
use aggregator_udfs::{
    map_aggregate::{MapAggregator, UdfMapAggregatorOp},
    nonnull::{NonNullable, non_null_max, non_null_min, non_null_sum},
};
use datafusion::{
    common::{Column, DFSchema, HashSet, plan_err},
    dataframe::DataFrame,
    error::Result,
    logical_expr::{AggregateUDF, Expr, ExprSchemable, ScalarUDF, col},
};
use filter_udfs::ageoff::AgeOff;
use std::sync::Arc;

impl Filter {
    /// Creates a filtering expression for this filter instance. The returned
    /// expression can be passed to [`DataFrame::filter`].
    /// # Errors
    /// If any of:
    ///  * the `max_age` of an age off filter is not representable as a timestamp
    ///  * the local system time is before the UNIX epoch
    pub fn create_filter_expr(&self) -> Result<Expr> {
        match self {
            Self::Ageoff { column, max_age } => {
                Ok(ScalarUDF::from(AgeOff::try_from(*max_age)?).call(vec![col(column)]))
            }
        }
    }
}

impl Aggregate {
    /// Creates a `DataFusion` logical expression to represent this aggregation operation.
    /// # Errors
    /// If the type of the column could not be computed, or a map aggregation is set for a non-map column.
    pub fn to_expr(&self, frame: &DataFrame) -> Result<Expr> {
        Ok(match &self.operation {
            AggOp::Sum => non_null_sum(col(&self.column)),
            AggOp::Min => non_null_min(col(&self.column)),
            AggOp::Max => non_null_max(col(&self.column)),
            AggOp::MapAggregate(op) => {
                let col_dt = col(&self.column).get_type(frame.schema())?;
                let map_sum = Arc::new(MapAggregator::try_new(&col_dt, op.to_udf_op())?);
                AggregateUDF::new_from_impl(NonNullable::new(map_sum)).call(vec![col(&self.column)])
            }
        }
        // Rename column to original name
        .alias(&self.column))
    }
}

impl MapAggregateOp {
    fn to_udf_op(&self) -> UdfMapAggregatorOp {
        match self {
            MapAggregateOp::Sum => UdfMapAggregatorOp::Sum,
            MapAggregateOp::Min => UdfMapAggregatorOp::Min,
            MapAggregateOp::Max => UdfMapAggregatorOp::Max,
        }
    }
}

/// Validates the filtering and aggregation configuration.
///
/// Row key columns are implicitly "group by" columns. Extra columns may be added. Typically,
/// we expect this to include sort key columns.
///
/// In particular, validates that:
///  1. All columns that are NOT "group by" columns have an aggregation operation specified for them,
///  2. No "group by" columns have aggregations specified,
///  3. No "group by" column is duplicated.
///  4. No aggregation column is specified multiple times,
///  5. All columns must be valid in schema.
///
/// Raise an error if this is not the case.
pub fn validate_aggregations(
    group_by_cols: &[&str],
    schema: &DFSchema,
    agg_conf: &[Aggregate],
) -> Result<()> {
    if !agg_conf.is_empty() {
        // List of columns
        let all_cols_bind = schema.columns();
        let all_cols = all_cols_bind.iter().map(Column::name).collect::<Vec<_>>();
        // Check for duplicate aggregation columns
        let mut dup_check = HashSet::new();
        for col in group_by_cols {
            // Has this "group by" column been specified multiple times? Or is it a row key column?
            // Row key columns are implicitly "group by" columns.
            if !dup_check.insert(*col) {
                return plan_err!(
                    "Grouping column \"{col}\" is already a row key column or has been specified multiple times"
                );
            }
            // Is this column valid?
            if !all_cols.contains(col) {
                return plan_err!("Grouping column \"{col}\" doesn't exist");
            }
        }
        // Remove "group by" columns
        let mut non_row_key_cols = all_cols.clone();
        non_row_key_cols.retain(|col| !group_by_cols.contains(col));
        // Columns with aggregators
        let agg_cols = agg_conf
            .iter()
            .map(|agg| agg.column.as_str())
            .collect::<Vec<_>>();
        // Check for duplications in aggregation columns and row key aggregations
        let mut col_checks: HashSet<&str> = HashSet::new();
        for col in &agg_cols {
            if group_by_cols.contains(col) {
                return plan_err!(
                    "Row key/extra grouping column \"{col}\" cannot have an aggregation"
                );
            }
            if !col_checks.insert(col) {
                return plan_err!("Aggregation column \"{col}\" duplicated");
            }
            if !non_row_key_cols.contains(col) {
                return plan_err!("Aggregation column \"{col}\" doesn't exist");
            }
        }
        // Check all non row key columns exist in aggregation column list
        for col in non_row_key_cols {
            if !agg_cols.contains(&col) {
                return plan_err!(
                    "Column \"{col}\" doesn't have a aggregation operator specified!"
                );
            }
        }
    }
    Ok(())
}
