use std::fmt::Display;

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
use color_eyre::eyre::{Result as EyreResult, eyre};
use thiserror::Error;

#[derive(Debug, PartialEq, Eq)]
pub struct FunctionCall<'h> {
    pub name: &'h str,
    pub parameters: Vec<FunctionParameter<'h>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum FunctionParameter<'h> {
    Word(&'h str),
    Number(i64),
}

#[derive(Error, Debug)]
pub enum FunctionCallError {
    #[error(
        "{function_name} expects {} argument{} ({}), found {actual_num_parameters}",
        expected_parameters.len(), if expected_parameters.len() == 1 { ""} else {"s"}, expected_parameters.join(", ")
    )]
    WrongNumberOfParameters {
        function_name: String,
        expected_parameters: &'static [&'static str],
        actual_num_parameters: usize,
    },
    #[error(
        "wrong type for {function_name} parameter {index} ({parameter_name}), expected {expected_type}, found \"{actual_value}\""
    )]
    WrongParameterType {
        function_name: String,
        index: usize,
        parameter_name: String,
        expected_type: String,
        actual_value: String,
    },
    #[error("parameter not found at index {index}")]
    ParameterNotFound { index: usize },
}

impl<'h> FunctionCall<'h> {
    pub fn expect_num_parameters(
        &self,
        expected_parameters: &'static [&'static str],
    ) -> Result<(), FunctionCallError> {
        if self.parameters.len() == expected_parameters.len() {
            Ok(())
        } else {
            Err(FunctionCallError::WrongNumberOfParameters {
                function_name: self.name.to_string(),
                expected_parameters,
                actual_num_parameters: self.parameters.len(),
            })
        }
    }

    pub fn word_param(&self, index: usize, param_name: &str) -> EyreResult<&str> {
        let param = self.param(index)?;
        match param {
            FunctionParameter::Word(value) => Ok(value),
            FunctionParameter::Number(_) => {
                Err(eyre!(self.type_error(index, param_name, param, "word")))
            }
        }
    }

    pub fn number_param(&self, index: usize, param_name: &str) -> EyreResult<i64> {
        let param = self.param(index)?;
        match param {
            FunctionParameter::Number(value) => Ok(*value),
            FunctionParameter::Word(_) => {
                Err(eyre!(self.type_error(index, param_name, param, "number")))
            }
        }
    }

    fn type_error(
        &self,
        index: usize,
        parameter_name: &str,
        param: &FunctionParameter,
        expected_type: &str,
    ) -> FunctionCallError {
        FunctionCallError::WrongParameterType {
            function_name: self.name.to_string(),
            index,
            parameter_name: parameter_name.to_string(),
            expected_type: expected_type.to_string(),
            actual_value: param.to_string(),
        }
    }

    fn param(&'_ self, index: usize) -> EyreResult<&FunctionParameter<'h>> {
        self.parameters
            .get(index)
            .ok_or_else(|| eyre!(FunctionCallError::ParameterNotFound { index }))
    }
}

impl Display for FunctionParameter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionParameter::Word(word) => write!(f, "{word}"),
            FunctionParameter::Number(number) => write!(f, "{number}"),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::{FunctionCall, FunctionParameter};
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
    fn should_read_parameters() -> Result<()> {
        let call = call("fn", vec![word("abc"), number(123)]);
        call.expect_num_parameters(&["param1", "param2"])?;
        assert_eq!(call.word_param(0, "param1")?, "abc");
        assert_eq!(call.number_param(1, "param2")?, 123);
        Ok(())
    }

    #[test]
    fn should_fail_expected_number_of_parameters() {
        let call = call("fn", vec![word("abc"), number(123)]);
        assert_error!(
            call.expect_num_parameters(&["param1", "param2", "param3"]),
            "fn expects 3 arguments (param1, param2, param3), found 2"
        );
    }

    #[test]
    fn should_fail_expected_one_parameter() {
        let call = call("fn", vec![word("abc"), number(123)]);
        assert_error!(
            call.expect_num_parameters(&["param"]),
            "fn expects 1 argument (param), found 2"
        );
    }

    #[test]
    fn should_fail_reading_word_wrong_type() {
        let call = call("fn", vec![word("abc"), number(123)]);
        assert_error!(
            call.word_param(1, "param2"),
            "wrong type for fn parameter 1 (param2), expected word, found \"123\""
        );
    }

    #[test]
    fn should_fail_reading_number_wrong_type() {
        let call = call("fn", vec![word("abc"), number(123)]);
        assert_error!(
            call.number_param(0, "param1"),
            "wrong type for fn parameter 0 (param1), expected number, found \"abc\""
        );
    }

    fn call<'h>(name: &'static str, parameters: Vec<FunctionParameter<'h>>) -> FunctionCall<'h> {
        FunctionCall { name, parameters }
    }

    fn word(value: &'static str) -> FunctionParameter<'static> {
        FunctionParameter::Word(value)
    }

    fn number(value: i64) -> FunctionParameter<'static> {
        FunctionParameter::Number(value)
    }
}
