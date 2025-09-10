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

use crate::datafusion::filter_aggregation_config::{
    function_call::FunctionCall, function_reader::FunctionReader,
};
use color_eyre::eyre::{Report, Result, ensure, eyre};

#[derive(Debug, PartialEq, Eq)]
pub enum Filter {
    /// Skip any row where timestamp in named column is older than `max_age` milliseconds.
    Ageoff { column: String, max_age: i64 },
}

impl Filter {
    pub fn parse_config(config_string: &str) -> Result<Vec<Self>> {
        FunctionReader::from(config_string)
            .map(|result| match result {
                Ok(call) => Self::try_from(&call),
                Err(e) => Err(eyre!(e)),
            })
            .collect()
    }
}

impl TryFrom<&FunctionCall<'_>> for Filter {
    type Error = Report;
    fn try_from(call: &FunctionCall) -> Result<Self> {
        ensure!(
            call.name.eq_ignore_ascii_case("ageOff"),
            "unrecognised filter function name \"{}\"",
            call.name
        );
        call.expect_num_parameters(&["column", "max age"])?;
        Ok(Filter::Ageoff {
            column: call.word_param(0, "column")?.to_string(),
            max_age: call.number_param(1, "max age")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Filter;
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
    fn should_parse_age_off_filter() -> Result<()> {
        assert_eq!(
            Filter::parse_config("ageOff(value,1234)")?,
            vec![age_off("value", 1234)]
        );
        Ok(())
    }

    #[test]
    fn should_parse_two_filters() -> Result<()> {
        assert_eq!(
            Filter::parse_config("ageOff(value,123), ageOff(other, 456)")?,
            vec![age_off("value", 123), age_off("other", 456)]
        );
        Ok(())
    }

    #[test]
    fn should_parse_no_filters() -> Result<()> {
        assert_eq!(Filter::parse_config("")?, vec![]);
        Ok(())
    }

    #[test]
    fn should_ignore_case() -> Result<()> {
        assert_eq!(
            Filter::parse_config("AGEOFF(a, 1), ageoff(b, 2)")?,
            vec![age_off("a", 1), age_off("b", 2)]
        );
        Ok(())
    }

    #[test]
    fn should_fail_with_unrecognised_filter_name() {
        assert_error!(
            Filter::parse_config("go(abc, 123)"),
            "unrecognised filter function name \"go\""
        );
    }

    #[test]
    fn should_fail_with_no_filter_name() {
        assert_error!(
            Filter::parse_config("(abc, 123)"),
            "expected function name at position 0"
        );
    }

    #[test]
    fn should_fail_with_too_few_arguments() {
        assert_error!(
            Filter::parse_config("ageOff(abc)"),
            "ageOff expects 2 arguments (column, max age), found 1"
        );
    }

    #[test]
    fn should_fail_with_too_many_arguments() {
        assert_error!(
            Filter::parse_config("ageOff(abc, 123, 456)"),
            "ageOff expects 2 arguments (column, max age), found 3"
        );
    }

    #[test]
    fn should_fail_with_field_name_wrong_type() {
        assert_error!(
            Filter::parse_config("ageOff(123, 456)"),
            "wrong type for ageOff parameter 0 (column), expected word, found Number(123)"
        );
    }

    fn age_off(column: &str, max_age: i64) -> Filter {
        let column = column.to_string();
        Filter::Ageoff { column, max_age }
    }
}
