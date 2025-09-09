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
use aggregator_udfs::{
    map_aggregate::{MapAggregator, MapAggregatorOp},
    nonnull::{NonNullable, non_null_max, non_null_min, non_null_sum},
};
use datafusion::{
    dataframe::DataFrame,
    error::{DataFusionError, Result},
    logical_expr::{AggregateUDF, Expr, ExprSchemable, col},
};
use std::sync::Arc;

/// Aggregation support. Consists of a column name and operation.
#[derive(Debug)]
pub struct Aggregate {
    pub column: String,
    pub operation: AggOp,
}

impl Aggregate {
    // Create a DataFusion logical expression to represent this aggregation operation.
    pub fn to_expr(&self, frame: &DataFrame) -> Result<Expr> {
        Ok(match &self.operation {
            AggOp::Sum => non_null_sum(col(&self.column)),
            AggOp::Min => non_null_min(col(&self.column)),
            AggOp::Max => non_null_max(col(&self.column)),
            AggOp::MapAggregate(op) => {
                let col_dt = col(&self.column).get_type(frame.schema())?;
                let map_sum = Arc::new(MapAggregator::try_new(&col_dt, op.clone())?);
                AggregateUDF::new_from_impl(NonNullable::new(map_sum)).call(vec![col(&self.column)])
            }
        }
        // Rename column to original name
        .alias(&self.column))
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
