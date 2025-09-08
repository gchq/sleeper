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
use crate::datafusion::filter_aggregation_config::function_call::{
    FunctionCall, FunctionParameter,
};
use color_eyre::eyre::{Result, ensure, eyre};

pub struct FunctionReader<'h> {
    haystack: &'h str,
    pos: usize,
    first_function: bool,
}

impl<'h> FunctionReader<'h> {
    pub fn new(haystack: &'h str) -> Self {
        FunctionReader {
            haystack,
            pos: 0,
            first_function: true,
        }
    }

    pub fn read_function_call(&mut self) -> Result<Option<FunctionCall>> {
        if !self.first_function {
            let found_comma = self.read_expected_char(',');
            if self.at_end() {
                ensure!(
                    !found_comma,
                    "expected function call at position {}",
                    self.pos
                );
                return Ok(None);
            }
            ensure!(found_comma, "expected comma at position {}", self.pos);
        }
        match self.read_word() {
            Some(name) => {
                let parameters = self.read_parameters()?;
                self.first_function = false;
                Ok(Some(FunctionCall { name, parameters }))
            }
            None => {
                if self.at_end() {
                    Ok(None)
                } else {
                    Err(eyre!("expected function name at position {}", self.pos))
                }
            }
        }
    }

    fn read_parameters(&mut self) -> Result<Vec<FunctionParameter>> {
        ensure!(
            self.read_expected_char('('),
            "expected open parenthesis at position {}",
            self.pos
        );
        let mut parameters = vec![];
        loop {
            if self.read_expected_char(')') {
                return Ok(parameters);
            }
            if let Some(param) = self.read_parameter() {
                parameters.push(param);
                if self.is_next_parameter()? {
                    continue;
                }
                ensure!(
                    self.read_expected_char(')'),
                    "expected comma or close parenthesis at position {}",
                    self.pos
                );
            } else {
                return Err(eyre!(
                    "expected parameter or close parenthesis at position {}",
                    self.pos
                ));
            }
            return Ok(parameters);
        }
    }

    fn is_next_parameter(&mut self) -> Result<bool> {
        self.ignore_whitespace();
        if let Some(c) = self.read_char() {
            if c == ',' {
                self.pos += 1;
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Err(eyre!("expected close parenthesis at position {}", self.pos))
        }
    }

    fn read_parameter(&mut self) -> Option<FunctionParameter> {
        self.read_word().map(|word| match word.parse::<i64>() {
            Ok(number) => FunctionParameter::Number(number),
            Err(_) => FunctionParameter::Word(word),
        })
    }

    fn read_word(&mut self) -> Option<String> {
        self.ignore_whitespace();
        let mut result = String::new();
        while let Some(c) = self.read_char()
            && c.is_alphanumeric()
        {
            result.push(c);
            self.pos += 1;
        }
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    fn read_expected_char(&mut self, expected: char) -> bool {
        self.ignore_whitespace();
        if let Some(c) = self.read_char()
            && c == expected
        {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn ignore_whitespace(&mut self) {
        while let Some(c) = self.read_char()
            && c.is_whitespace()
        {
            self.pos += 1;
        }
    }

    fn read_char(&mut self) -> Option<char> {
        self.haystack.chars().nth(self.pos)
    }

    fn at_end(&self) -> bool {
        self.haystack.len() == self.pos
    }
}

#[cfg(test)]
mod tests {

    use super::FunctionReader;
    use crate::datafusion::filter_aggregation_config::function_call::{
        FunctionCall, FunctionParameter,
    };
    use color_eyre::eyre::{Result, eyre};
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
    fn should_read_function_call() -> Result<()> {
        let mut reader = FunctionReader::new("answer(42)");
        Ok(assert_eq!(
            expect_function_call(&mut reader)?,
            call("answer", vec![number(42)])
        ))
    }

    #[test]
    fn should_read_two_function_calls() -> Result<()> {
        let mut reader = FunctionReader::new("answer(42), question(earth)");
        assert_eq!(
            expect_function_call(&mut reader)?,
            call("answer", vec![number(42)])
        );
        Ok(assert_eq!(
            expect_function_call(&mut reader)?,
            call("question", vec![word("earth")])
        ))
    }

    #[test]
    fn should_read_function_call_with_two_parameters() -> Result<()> {
        let mut reader = FunctionReader::new("fn(a,123)");
        Ok(assert_eq!(
            expect_function_call(&mut reader)?,
            call("fn", vec![word("a"), number(123)])
        ))
    }

    #[test]
    fn should_read_function_call_with_no_parameters() -> Result<()> {
        let mut reader = FunctionReader::new("go()");
        Ok(assert_eq!(
            expect_function_call(&mut reader)?,
            call("go", vec![])
        ))
    }

    #[test]
    fn should_read_function_call_with_whitespace() -> Result<()> {
        let mut reader = FunctionReader::new(" fn ( a , 123 ) ");
        Ok(assert_eq!(
            expect_function_call(&mut reader)?,
            call("fn", vec![word("a"), number(123)])
        ))
    }

    #[test]
    fn should_find_bad_characters_at_function_name() {
        let mut reader = FunctionReader::new("@/ #!");
        assert_error!(
            reader.read_function_call(),
            "expected function name at position 0"
        )
    }

    #[test]
    fn should_find_no_function_call() -> Result<()> {
        let mut reader = FunctionReader::new("");
        Ok(assert_eq!(reader.read_function_call()?, None))
    }

    #[test]
    fn should_find_no_more_function_calls_after_first() -> Result<()> {
        let mut reader = FunctionReader::new("fn()");
        assert_eq!(expect_function_call(&mut reader)?, call("fn", vec![]));
        Ok(assert_eq!(reader.read_function_call()?, None))
    }

    #[test]
    fn should_find_no_more_function_calls_after_first_with_trailing_whitespace() -> Result<()> {
        let mut reader = FunctionReader::new("fn() ");
        assert_eq!(expect_function_call(&mut reader)?, call("fn", vec![]));
        Ok(assert_eq!(reader.read_function_call()?, None))
    }

    #[test]
    fn should_find_no_more_function_calls_after_first_with_trailing_comma() -> Result<()> {
        let mut reader = FunctionReader::new("fn(),");
        assert_eq!(expect_function_call(&mut reader)?, call("fn", vec![]));
        Ok(assert_error!(
            reader.read_function_call(),
            "expected function call at position 5"
        ))
    }

    #[test]
    fn should_find_no_comma_between_parameters() {
        let mut reader = FunctionReader::new("fn(a b)");
        assert_error!(
            reader.read_function_call(),
            "expected comma or close parenthesis at position 5"
        )
    }

    #[test]
    fn should_find_no_comma_between_functions() -> Result<()> {
        let mut reader = FunctionReader::new("fn(1) other(2)");
        assert_eq!(
            expect_function_call(&mut reader)?,
            call("fn", vec![number(1)])
        );
        Ok(assert_error!(
            reader.read_function_call(),
            "expected comma at position 6"
        ))
    }

    #[test]
    fn should_find_no_close_paren() {
        let mut reader = FunctionReader::new("fn(a, b");
        assert_error!(
            reader.read_function_call(),
            "expected close parenthesis at position 7"
        )
    }

    #[test]
    fn should_find_no_open_paren() {
        let mut reader = FunctionReader::new("fn");
        assert_error!(
            reader.read_function_call(),
            "expected open parenthesis at position 2"
        )
    }

    #[test]
    fn should_find_quoted_parameter() {
        let mut reader = FunctionReader::new("fn(\"abc\")");
        assert_error!(
            reader.read_function_call(),
            "expected parameter or close parenthesis at position 3"
        )
    }

    fn expect_function_call(reader: &mut FunctionReader) -> Result<FunctionCall> {
        reader
            .read_function_call()?
            .ok_or_else(|| eyre!("found no function call"))
    }

    fn call(name: &str, parameters: Vec<FunctionParameter>) -> FunctionCall {
        let name = name.to_string();
        FunctionCall { name, parameters }
    }

    fn word(value: &str) -> FunctionParameter {
        let value = value.to_string();
        FunctionParameter::Word(value)
    }

    fn number(value: i64) -> FunctionParameter {
        FunctionParameter::Number(value)
    }
}
