/// Module contains structs and functions relating to implementing Sleeper 'iterators' in Rust using
/// DataFusion.
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
    pub aggregation: Vec<Aggregate>,
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
                "Aggregation operator {} not recognised",
                value
            ))),
        }
    }
}

impl TryFrom<&str> for FilterAggregationConfig {
    type Error = DataFusionError;

    /// This is a minimum viable parser for the configuration for filters/aggregators.
    /// It is a really good example of how NOT to do it.
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        // Create list of strings delimited by comma as iterator
        let values = value.split(',').map(str::trim).collect::<Vec<_>>();
        let filter = if values[0].starts_with("ageoff=") {
            let column = values[0].split('=').collect::<Vec<_>>()[1].replace("'", "");
            let max_age = values[1]
                .replace("'", "")
                .parse::<i64>()
                .map_err(|_| DataFusionError::Internal(format!("Invalid number {}", values[1])))?;
            Some(Filter::Ageoff { column, max_age })
        } else {
            None
        };
        // Look for aggregators, skip first part if no we didn't have ageoff filter, otherwise skip 2
        // This is a really hacky implementation
        let iter = if let Some(_) = &filter {
            values.iter().skip(2)
        } else {
            values.iter().skip(1)
        };
        let mut aggregation = Vec::new();
        for agg in iter {
            if let Some((col, op)) = agg.split_once("=") {
                aggregation.push(Aggregate(col.replace("'", ""), op.try_into()?));
            }
        }
        Ok(Self {
            filter,
            aggregation,
        })
    }
}
