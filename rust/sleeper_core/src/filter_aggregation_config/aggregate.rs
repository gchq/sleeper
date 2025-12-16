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
use super::{
    function_call::{FunctionCall, FunctionCallError},
    function_reader::{FunctionReader, FunctionReaderError},
};
use thiserror::Error;

/// Aggregation support. Consists of a column name and operation.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Aggregate {
    pub column: String,
    pub operation: AggOp,
}

#[derive(Error, Debug)]
pub enum AggregateConfigError {
    #[error("{error}")]
    CouldNotReadConfig { error: FunctionReaderError },
    #[error("{error}")]
    InvalidFunctionCall { error: FunctionCallError },
    #[error("unrecognised aggregation function name \"{name}\"")]
    UnrecognisedAggregationFunction { name: String },
}

impl Aggregate {
    /// Parse an aggregation configuration from a string.
    /// # Errors
    /// If the configuration string could not be read, or an aggregation function was unrecognised or invalid.
    pub fn parse_config(config_string: &str) -> Result<Vec<Self>, AggregateConfigError> {
        FunctionReader::from(config_string)
            .map(|result| match result {
                Ok(call) => Self::try_from(&call),
                Err(error) => Err(AggregateConfigError::CouldNotReadConfig { error }),
            })
            .collect()
    }
}

impl TryFrom<&FunctionCall<'_>> for Aggregate {
    type Error = AggregateConfigError;

    fn try_from(call: &FunctionCall<'_>) -> Result<Self, Self::Error> {
        Ok(Aggregate {
            column: column(call)
                .map_err(|error| AggregateConfigError::InvalidFunctionCall { error })?,
            operation: AggOp::try_from(call.name)?,
        })
    }
}

fn column(call: &FunctionCall) -> Result<String, FunctionCallError> {
    call.validate_num_parameters(&["column"])?;
    Ok(call.word_param(0, "column")?.to_string())
}

/// Supported aggregating operations.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum AggOp {
    Sum,
    Min,
    Max,
    MapAggregate(MapAggregateOp),
}

/// The aggregation operation to peform inside of each map. The values
/// of identical keys will be aggregated according to the specified operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MapAggregateOp {
    Sum,
    Min,
    Max,
}

impl TryFrom<&str> for AggOp {
    type Error = AggregateConfigError;

    fn try_from(name: &str) -> Result<Self, Self::Error> {
        match name.to_lowercase().as_str() {
            "sum" => Ok(Self::Sum),
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            "map_sum" => Ok(Self::MapAggregate(MapAggregateOp::Sum)),
            "map_min" => Ok(Self::MapAggregate(MapAggregateOp::Min)),
            "map_max" => Ok(Self::MapAggregate(MapAggregateOp::Max)),
            _ => Err(AggregateConfigError::UnrecognisedAggregationFunction {
                name: name.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AggOp, Aggregate, MapAggregateOp};
    use crate::assert_error;
    use color_eyre::eyre::Result;
    use test_log::test;

    #[test]
    fn should_parse_sum() -> Result<()> {
        assert_eq!(Aggregate::parse_config("sum(count)")?, vec![sum("count")]);
        Ok(())
    }

    #[test]
    fn should_parse_min() -> Result<()> {
        assert_eq!(Aggregate::parse_config("min(count)")?, vec![min("count")]);
        Ok(())
    }

    #[test]
    fn should_parse_max() -> Result<()> {
        assert_eq!(Aggregate::parse_config("max(count)")?, vec![max("count")]);
        Ok(())
    }

    #[test]
    fn should_parse_map_sum() -> Result<()> {
        assert_eq!(
            Aggregate::parse_config("map_sum(some_map)")?,
            vec![map_sum("some_map")]
        );
        Ok(())
    }

    #[test]
    fn should_parse_map_min() -> Result<()> {
        assert_eq!(
            Aggregate::parse_config("map_min(some_map)")?,
            vec![map_min("some_map")]
        );
        Ok(())
    }

    #[test]
    fn should_parse_map_max() -> Result<()> {
        assert_eq!(
            Aggregate::parse_config("map_max(some_map)")?,
            vec![map_max("some_map")]
        );
        Ok(())
    }

    #[test]
    fn should_parse_no_aggregation() -> Result<()> {
        assert_eq!(Aggregate::parse_config("")?, vec![]);
        Ok(())
    }

    #[test]
    fn should_parse_two_aggregations() -> Result<()> {
        assert_eq!(
            Aggregate::parse_config("sum(a), sum(b)")?,
            vec![sum("a"), sum("b")]
        );
        Ok(())
    }

    #[test]
    fn should_ignore_case() -> Result<()> {
        assert_eq!(
            Aggregate::parse_config("SUM(a), MiN(b), Max(c), Map_Sum(d), MAP_MIN(e), MAP_max(f)")?,
            vec![
                sum("a"),
                min("b"),
                max("c"),
                map_sum("d"),
                map_min("e"),
                map_max("f")
            ]
        );
        Ok(())
    }

    #[test]
    fn should_fail_with_unrecognised_aggregation_name() {
        assert_error!(
            Aggregate::parse_config("combine(a)"),
            "unrecognised aggregation function name \"combine\""
        );
    }

    #[test]
    fn should_fail_with_too_many_arguments() {
        assert_error!(
            Aggregate::parse_config("sum(abc, 123)"),
            "sum expects 1 argument (column), found 2"
        );
    }

    #[test]
    fn should_fail_with_field_name_wrong_type() {
        assert_error!(
            Aggregate::parse_config("sum(123)"),
            "wrong type for sum parameter 0 (column), expected word, found \"123\""
        );
    }

    fn sum(column: &str) -> Aggregate {
        Aggregate {
            column: column.to_string(),
            operation: AggOp::Sum,
        }
    }

    fn min(column: &str) -> Aggregate {
        Aggregate {
            column: column.to_string(),
            operation: AggOp::Min,
        }
    }

    fn max(column: &str) -> Aggregate {
        Aggregate {
            column: column.to_string(),
            operation: AggOp::Max,
        }
    }

    fn map_sum(column: &str) -> Aggregate {
        Aggregate {
            column: column.to_string(),
            operation: AggOp::MapAggregate(MapAggregateOp::Sum),
        }
    }

    fn map_min(column: &str) -> Aggregate {
        Aggregate {
            column: column.to_string(),
            operation: AggOp::MapAggregate(MapAggregateOp::Min),
        }
    }

    fn map_max(column: &str) -> Aggregate {
        Aggregate {
            column: column.to_string(),
            operation: AggOp::MapAggregate(MapAggregateOp::Max),
        }
    }
}
