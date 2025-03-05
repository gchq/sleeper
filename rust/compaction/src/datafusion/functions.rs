/// Module contains structs and functions relating to implementing Sleeper 'iterators' in Rust using
/// `DataFusion`.
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
use ageoff::AgeOff;
use datafusion::common::{config_err, DFSchema, HashSet};
use datafusion::functions_aggregate::expr_fn::{count, max, min, sum};
use datafusion::logical_expr::col;
use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::{Expr, ScalarUDF},
};

pub mod ageoff;

/// Parsed details of prototype iterator configuration. We only allow one filter operation and simple aggregation.
#[derive(Debug, Default)]
pub struct FilterAggregationConfig {
    pub filter: Option<Filter>,
    pub aggregation: Option<Vec<Aggregate>>,
}

/// Supported filters
#[derive(Debug)]
pub enum Filter {
    // Skip any record where timestamp in named column is older than `max_age` seconds.
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
    pub fn to_expr(&self) -> Expr {
        match self.1 {
            AggOp::Count => count(col(&self.0)),
            AggOp::Sum => sum(col(&self.0)),
            AggOp::Min => min(col(&self.0)),
            AggOp::Max => max(col(&self.0)),
        }
        // Rename column to original name
        .alias(&self.0)
    }
}

// Supported aggregating operations.
#[derive(Debug)]
pub enum AggOp {
    Sum,
    Count,
    Min,
    Max,
}

impl TryFrom<&str> for AggOp {
    type Error = DataFusionError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "sum" => Ok(Self::Sum),
            "count" => Ok(Self::Count),
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            _ => Err(Self::Error::NotImplemented(format!(
                "Aggregation operator {value} not recognised"
            ))),
        }
    }
}

impl TryFrom<&str> for FilterAggregationConfig {
    type Error = DataFusionError;

    /// This is a minimum viable parser for the configuration for filters/aggregators.
    /// It is a really good example of how NOT to do it. This routine has some odd
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        // Create list of strings delimited by comma as iterator
        let values = value.split(',').map(str::trim).collect::<Vec<_>>();
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
        for agg in iter {
            if let Some((col, op)) = agg.split_once('=') {
                aggregation.push(Aggregate(col.replace('\'', ""), op.try_into()?));
            }
        }
        let aggregation = if aggregation.is_empty() {
            None
        } else {
            Some(aggregation)
        };
        Ok(Self {
            filter,
            aggregation,
        })
    }
}

/// Validate that
///  1. All columns that are NOT row key columns have an aggregation operation specified for them,
///  2. No row key columns have aggregations specified,
///  3. No aggregation column is specified multiple times.
///  4. Aggregation columns must be valid in schema.
///
/// Raise an error if this is not the case.
pub fn validate_aggregations(
    row_keys: &[String],
    schema: &DFSchema,
    agg_conf: &[Aggregate],
) -> Result<()> {
    if !agg_conf.is_empty() {
        // List of columns
        let mut non_row_key_cols = schema.clone().strip_qualifiers().field_names();
        // Remove row keys
        non_row_key_cols.retain(|col| !row_keys.contains(col));
        // Columns with aggregators
        let agg_cols = agg_conf.iter().map(|agg| &agg.0).collect::<Vec<_>>();
        // Check for duplications in aggregation columns and row key aggregations
        let mut col_checks: HashSet<&String> = HashSet::new();
        for col in &agg_cols {
            if row_keys.contains(*col) {
                return config_err!("Row key column \"{col}\" cannot have an aggregation");
            }
            if !col_checks.insert(*col) {
                return config_err!("Aggregation column \"{col}\" duplicated");
            }
            if !non_row_key_cols.contains(*col) {
                return config_err!("Aggregation column \"{col}\" doesn't exist");
            }
        }
        // Check all non row key columns exist in aggregation column list
        for col in non_row_key_cols {
            if !agg_cols.contains(&&col) {
                return config_err!(
                    "Column \"{col}\" doesn't have a aggregation operator specified!"
                );
            }
        }
    }
    Ok(())
}
