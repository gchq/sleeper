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
use aggregator_udfs::map_aggregate::MapAggregatorOp;
use color_eyre::eyre::{Result, eyre};

use super::{function_call::FunctionCall, function_reader::FunctionReader};

/// Aggregation support. Consists of a column name and operation.
#[derive(Debug, PartialEq, Eq)]
pub struct Aggregate {
    pub column: String,
    pub operation: AggOp,
}

impl Aggregate {
    pub fn parse_config(config_string: &str) -> Result<Vec<Self>> {
        let mut reader = FunctionReader::from(config_string);
        let mut aggregations = vec![];
        while let Some(call) = reader.read_function_call()? {
            aggregations.push(Self::from_function_call(&call)?);
        }
        Ok(aggregations)
    }

    fn from_function_call(call: &FunctionCall) -> Result<Self> {
        call.validate_num_parameters(&["column"])?;
        let column = call.word_param(0, "column")?.to_string();
        let operation: AggOp = AggOp::parse(call.name)?;
        Ok(Aggregate { column, operation })
    }
}

/// Supported aggregating operations.
#[derive(Debug, PartialEq, Eq)]
pub enum AggOp {
    Sum,
    Min,
    Max,
    MapAggregate(MapAggregatorOp),
}

impl AggOp {
    pub fn parse(value: &str) -> Result<Self> {
        match value.to_lowercase().as_str() {
            "sum" => Ok(Self::Sum),
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            "map_sum" => Ok(Self::MapAggregate(MapAggregatorOp::Sum)),
            "map_min" => Ok(Self::MapAggregate(MapAggregatorOp::Min)),
            "map_max" => Ok(Self::MapAggregate(MapAggregatorOp::Max)),
            _ => Err(eyre!("unrecognised aggregation function name \"{value}\"")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AggOp, Aggregate};
    use aggregator_udfs::map_aggregate::MapAggregatorOp;
    use color_eyre::eyre::Result;
    use test_log::test;

    macro_rules! assert_error {
        ($err_expr: expr, $err_contents: expr) => {
            assert_eq!(
                $err_expr.err().map(|e| e.to_string()),
                Some($err_contents.to_string())
            )
        };
    }

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
            "wrong type for sum parameter 0 (column), expected word, found Number(123)"
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
            operation: AggOp::MapAggregate(MapAggregatorOp::Sum),
        }
    }

    fn map_min(column: &str) -> Aggregate {
        Aggregate {
            column: column.to_string(),
            operation: AggOp::MapAggregate(MapAggregatorOp::Min),
        }
    }

    fn map_max(column: &str) -> Aggregate {
        Aggregate {
            column: column.to_string(),
            operation: AggOp::MapAggregate(MapAggregatorOp::Max),
        }
    }
}
