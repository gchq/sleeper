//! Module contains structs and functions relating to implementing Sleeper 'iterators' in Rust using
//! `DataFusion`.
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
use crate::datafusion::{
    ageoff_udf::AgeOff,
    aggregate_udf::nonnull::{NonNullable, non_null_max, non_null_min, non_null_sum},
};
use datafusion::{
    common::{DFSchema, HashSet, plan_datafusion_err, plan_err},
    error::{DataFusionError, Result},
    logical_expr::{AggregateUDF, Expr, ExprSchemable, ScalarUDF, col},
    prelude::DataFrame,
};
use map_aggregate::MapAggregator;
use num_traits::NumAssign;
use regex::Regex;
use std::sync::Arc;

pub mod map_aggregate;
pub mod nonnull;

pub const AGGREGATE_REGEX: &str = r"(\w+)\((\w+)\)";

/// Parsed details of prototype iterator configuration. We only allow one filter operation and simple aggregation.
///
/// Aggregation must be performed on all value columns, or none at all. This condition will be validated by the
/// [`validate_aggregations`] function.
#[derive(Debug, Default)]
pub struct FilterAggregationConfig {
    /// Extra columns beyond row key columns to aggregate
    pub agg_cols: Option<Vec<String>>,
    /// Single filtering option
    pub filter: Option<Filter>,
    /// Aggregation columns. These must not include any row key columns or columns mentioned in `agg_cols`.
    pub aggregation: Option<Vec<Aggregate>>,
}

/// Supported filters
#[derive(Debug)]
pub enum Filter {
    /// Skip any record where timestamp in named column is older than `max_age` seconds.
    Ageoff { column: String, max_age: i64 },
}

impl Filter {
    /// Creates a filtering expression for this filter instance. The returned
    /// expression can be passed to [`DataFrame::filter`].
    pub fn create_filter_expr(&self) -> Result<Expr> {
        match self {
            Self::Ageoff { column, .. } => {
                Ok(ScalarUDF::from(AgeOff::try_from(self)?).call(vec![col(column)]))
            }
        }
    }
}

/// Aggregation support. Consists of a column name and operation.
#[derive(Debug)]
pub struct Aggregate(String, AggOp);

impl Aggregate {
    // Create a DataFusion logical expression to represent this aggregation operation.
    pub fn to_expr(&self, frame: &DataFrame) -> Result<Expr> {
        Ok(match &self.1 {
            AggOp::Sum => non_null_sum(col(&self.0)),
            AggOp::Min => non_null_min(col(&self.0)),
            AggOp::Max => non_null_max(col(&self.0)),
            AggOp::MapAggregate(op) => {
                let col_dt = col(&self.0).get_type(frame.schema())?;
                let map_sum = Arc::new(MapAggregator::try_new(&col_dt, op.clone())?);
                AggregateUDF::new_from_impl(NonNullable::new(map_sum)).call(vec![col(&self.0)])
            }
        }
        // Rename column to original name
        .alias(&self.0))
    }
}

/// The aggregation operation to peform inside of each map. The values
/// of identical keys will be aggregated according to the specified operation.
#[derive(Debug, Clone)]
pub enum MapAggregatorOp {
    Sum,
    Min,
    Max,
}

impl MapAggregatorOp {
    pub fn op<T>(&self, acc: T, value: T) -> T
    where
        T: NumAssign + Ord,
    {
        match self {
            Self::Sum => acc + value,
            Self::Min => std::cmp::min(acc, value),
            Self::Max => std::cmp::max(acc, value),
        }
    }
}

/// Supported aggregating operations.
#[derive(Debug)]
pub enum AggOp {
    Sum,
    Min,
    Max,
    MapAggregate(MapAggregatorOp),
}

impl TryFrom<&str> for AggOp {
    type Error = DataFusionError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "sum" => Ok(Self::Sum),
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            "map_sum" => Ok(Self::MapAggregate(MapAggregatorOp::Sum)),
            "map_min" => Ok(Self::MapAggregate(MapAggregatorOp::Min)),
            "map_max" => Ok(Self::MapAggregate(MapAggregatorOp::Max)),
            _ => Err(Self::Error::NotImplemented(format!(
                "Aggregation operator {value} not recognised"
            ))),
        }
    }
}

impl TryFrom<&str> for FilterAggregationConfig {
    type Error = DataFusionError;

    /// This is a minimum viable parser for the configuration for filters/aggregators.
    /// It is a really good example of how NOT to do it. This routine has some odd behaviour.
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        // split aggregation columns out
        let (agg_cols, filter_agg) = value
            .split_once(';')
            .ok_or(plan_datafusion_err!("No ; in aggregation configuration"))?;
        // Convert to a vector of columns
        let agg_cols = if agg_cols.is_empty() {
            None
        } else {
            Some(
                agg_cols
                    .split(',')
                    .map(|s| s.trim().to_owned())
                    .collect::<Vec<_>>(),
            )
        };

        // Create list of strings delimited by comma as iterator
        let values = filter_agg.split(',').map(str::trim).collect::<Vec<_>>();
        let filter = if values[0].starts_with("ageoff=") {
            let column = values[0].split('=').collect::<Vec<_>>()[1].replace('\'', "");
            let max_age = values[1]
                .replace('\'', "")
                .parse::<i64>()
                .map_err(|_| DataFusionError::Internal(format!("Invalid number {}", values[1])))?;
            Some(Filter::Ageoff { column, max_age })
        } else {
            None
        };
        // Look for aggregators, skip first part if no we didn't have ageoff filter, otherwise skip 2
        // This is a really hacky implementation
        let iter = if filter.is_some() {
            values.iter().skip(2)
        } else {
            values.iter().skip(1)
        };
        let mut aggregation = Vec::new();
        let matcher =
            Regex::new(AGGREGATE_REGEX).map_err(|e| DataFusionError::External(Box::new(e)))?;
        for agg in iter {
            if let Some(captures) = matcher.captures(agg) {
                aggregation.push(Aggregate(captures[2].to_owned(), captures[1].try_into()?));
            }
        }
        let aggregation = if aggregation.is_empty() {
            None
        } else {
            Some(aggregation)
        };
        Ok(Self {
            agg_cols,
            filter,
            aggregation,
        })
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
    group_by_cols: &[String],
    schema: &DFSchema,
    agg_conf: &[Aggregate],
) -> Result<()> {
    if !agg_conf.is_empty() {
        // List of columns
        let all_cols = schema.clone().strip_qualifiers().field_names();
        // Check for duplicate aggregation columns
        let mut dup_check = HashSet::new();
        for col in group_by_cols {
            // Has this "group by" column been specified multiple times? Or is it a row key column?
            // Row key columns are implicitly "group by" columns.
            if !dup_check.insert(col) {
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
        let agg_cols = agg_conf.iter().map(|agg| &agg.0).collect::<Vec<_>>();
        // Check for duplications in aggregation columns and row key aggregations
        let mut col_checks: HashSet<&String> = HashSet::new();
        for col in &agg_cols {
            if group_by_cols.contains(*col) {
                return plan_err!(
                    "Row key/extra grouping column \"{col}\" cannot have an aggregation"
                );
            }
            if !col_checks.insert(*col) {
                return plan_err!("Aggregation column \"{col}\" duplicated");
            }
            if !non_row_key_cols.contains(*col) {
                return plan_err!("Aggregation column \"{col}\" doesn't exist");
            }
        }
        // Check all non row key columns exist in aggregation column list
        for col in &non_row_key_cols {
            if !agg_cols.contains(&col) {
                return plan_err!(
                    "Column \"{col}\" doesn't have a aggregation operator specified!"
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[macro_export]
    macro_rules! assert_error {
        ($err_expr: expr, $err_type: path, $err_contents: expr) => {
            let result = if let Err($err_type(err)) = $err_expr {
                assert_eq!(err, $err_contents);
                true
            } else {
                false
            };
            assert!(result, "Expected different error type");
        };
    }
}
