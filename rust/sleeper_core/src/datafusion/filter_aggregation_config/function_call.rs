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
use color_eyre::eyre::{Error, Result, ensure, eyre};

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

impl<'h> FunctionCall<'h> {
    pub fn expect_num_parameters(&self, param_names: &[&str]) -> Result<()> {
        ensure!(
            self.parameters.len() == param_names.len(),
            "{} expects {} argument{} ({}), found {}",
            self.name,
            param_names.len(),
            if param_names.len() == 1 { "" } else { "s" },
            param_names.join(", "),
            self.parameters.len()
        );
        Ok(())
    }

    pub fn word_param(&self, index: usize, param_name: &str) -> Result<&str> {
        let param = self.param(index)?;
        match param {
            FunctionParameter::Word(value) => Ok(value),
            _ => Err(self.type_error(index, param_name, param, "word")),
        }
    }

    pub fn number_param(&self, index: usize, param_name: &str) -> Result<i64> {
        let param = self.param(index)?;
        match param {
            FunctionParameter::Number(value) => Ok(*value),
            _ => Err(self.type_error(index, param_name, param, "number")),
        }
    }

    fn type_error(
        &self,
        index: usize,
        param_name: &str,
        param: &FunctionParameter,
        expected_type: &str,
    ) -> Error {
        eyre!(
            "wrong type for {} parameter {index} ({param_name}), expected {expected_type}, found {param:?}",
            self.name
        )
    }

    fn param(&'_ self, index: usize) -> Result<&FunctionParameter<'h>> {
        self.parameters
            .get(index)
            .ok_or_else(|| eyre!("parameter not found at index {index}"))
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
            "wrong type for fn parameter 1 (param2), expected word, found Number(123)"
        );
    }

    #[test]
    fn should_fail_reading_number_wrong_type() {
        let call = call("fn", vec![word("abc"), number(123)]);
        assert_error!(
            call.number_param(0, "param1"),
            "wrong type for fn parameter 0 (param1), expected number, found Word(\"abc\")"
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
