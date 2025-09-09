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
use std::str::CharIndices;

pub struct FunctionReader<'h> {
    haystack: &'h str,
    chars: CharIndices<'h>,
    current: Option<(usize, char)>,
    pos: usize,
    first_function: bool,
}

impl<'h> FunctionReader<'h> {
    pub fn new(haystack: &'h str) -> Self {
        let mut chars = haystack.char_indices();
        let current = chars.next();
        FunctionReader {
            haystack,
            chars,
            current,
            pos: 0,
            first_function: true,
        }
    }

    pub fn read_function_call(&mut self) -> Result<Option<FunctionCall<'h>>> {
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

    fn read_parameters(&mut self) -> Result<Vec<FunctionParameter<'h>>> {
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
        if let Some(c) = self.current_char() {
            if c == ',' {
                self.advance();
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Err(eyre!("expected close parenthesis at position {}", self.pos))
        }
    }

    fn read_parameter(&mut self) -> Option<FunctionParameter<'h>> {
        self.read_word().map(|word| match word.parse::<i64>() {
            Ok(number) => FunctionParameter::Number(number),
            Err(_) => FunctionParameter::Word(word),
        })
    }

    fn read_word(&mut self) -> Option<&'h str> {
        self.ignore_whitespace();
        let start_index = self.current_index()?;
        while let Some(c) = self.current_char()
            && (c.is_alphanumeric() || c == '_' || c == '-')
        {
            self.advance();
        }
        if let Some(end_index) = self.current_index() {
            if start_index == end_index {
                None
            } else {
                Some(&self.haystack[start_index..end_index])
            }
        } else {
            Some(&self.haystack[start_index..])
        }
    }

    fn read_expected_char(&mut self, expected: char) -> bool {
        self.ignore_whitespace();
        if let Some(c) = self.current_char()
            && c == expected
        {
            self.advance();
            true
        } else {
            false
        }
    }

    fn ignore_whitespace(&mut self) {
        while let Some(c) = self.current_char()
            && c.is_whitespace()
        {
            self.advance();
        }
    }

    fn current_index(&mut self) -> Option<usize> {
        self.current.map(|c| c.0)
    }

    fn current_char(&mut self) -> Option<char> {
        self.current.map(|c| c.1)
    }

    fn advance(&mut self) {
        self.current = self.chars.next();
        self.pos += 1;
    }

    fn at_end(&self) -> bool {
        self.current.is_none()
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
        assert_eq!(
            expect_function_call(&mut reader)?,
            call("answer", vec![number(42)])
        );
        Ok(())
    }

    #[test]
    fn should_read_two_function_calls() -> Result<()> {
        let mut reader = FunctionReader::new("answer(42), question(earth)");
        assert_eq!(
            expect_function_call(&mut reader)?,
            call("answer", vec![number(42)])
        );
        assert_eq!(
            expect_function_call(&mut reader)?,
            call("question", vec![word("earth")])
        );
        Ok(())
    }

    #[test]
    fn should_read_function_call_with_two_parameters() -> Result<()> {
        let mut reader = FunctionReader::new("fn(a,123)");
        assert_eq!(
            expect_function_call(&mut reader)?,
            call("fn", vec![word("a"), number(123)])
        );
        Ok(())
    }

    #[test]
    fn should_read_function_call_with_no_parameters() -> Result<()> {
        let mut reader = FunctionReader::new("go()");
        assert_eq!(expect_function_call(&mut reader)?, call("go", vec![]));
        Ok(())
    }

    #[test]
    fn should_read_function_call_with_whitespace() -> Result<()> {
        let mut reader = FunctionReader::new(" fn ( a , 123 ) ");
        assert_eq!(
            expect_function_call(&mut reader)?,
            call("fn", vec![word("a"), number(123)])
        );
        Ok(())
    }

    #[test]
    fn should_find_bad_characters_at_function_name() {
        let mut reader = FunctionReader::new("@/ #!");
        assert_error!(
            reader.read_function_call(),
            "expected function name at position 0"
        );
    }

    #[test]
    fn should_find_bad_characters_at_field_name() {
        let mut reader = FunctionReader::new("fn(@/ #!)");
        assert_error!(
            reader.read_function_call(),
            "expected parameter or close parenthesis at position 3"
        );
    }

    #[test]
    fn should_find_bad_character_in_function_name() {
        let mut reader = FunctionReader::new("fn@1()");
        assert_error!(
            reader.read_function_call(),
            "expected open parenthesis at position 2"
        );
    }

    #[test]
    fn should_find_bad_character_in_field_name() {
        let mut reader = FunctionReader::new("fn(field@0)");
        assert_error!(
            reader.read_function_call(),
            "expected comma or close parenthesis at position 8"
        );
    }

    #[test]
    fn should_read_function_call_with_underscore() -> Result<()> {
        let mut reader = FunctionReader::new("some_fn(a_field)");
        assert_eq!(
            expect_function_call(&mut reader)?,
            call("some_fn", vec![word("a_field")])
        );
        Ok(())
    }

    #[test]
    fn should_read_function_call_with_hyphen() -> Result<()> {
        let mut reader = FunctionReader::new("some-fn(a-field)");
        assert_eq!(
            expect_function_call(&mut reader)?,
            call("some-fn", vec![word("a-field")])
        );
        Ok(())
    }

    #[test]
    fn should_read_function_call_with_unicode_characters_spanning_multiple_bytes() -> Result<()> {
        let mut reader = FunctionReader::new("存在する(ひほわれよう)");
        assert_eq!(
            expect_function_call(&mut reader)?,
            call("存在する", vec![word("ひほわれよう")])
        );
        Ok(())
    }

    #[test]
    fn should_find_no_function_call() -> Result<()> {
        let mut reader = FunctionReader::new("");
        assert_eq!(reader.read_function_call()?, None);
        Ok(())
    }

    #[test]
    fn should_find_no_more_function_calls_after_first() -> Result<()> {
        let mut reader = FunctionReader::new("fn()");
        assert_eq!(expect_function_call(&mut reader)?, call("fn", vec![]));
        assert_eq!(reader.read_function_call()?, None);
        Ok(())
    }

    #[test]
    fn should_find_no_more_function_calls_after_first_with_trailing_whitespace() -> Result<()> {
        let mut reader = FunctionReader::new("fn() ");
        assert_eq!(expect_function_call(&mut reader)?, call("fn", vec![]));
        assert_eq!(reader.read_function_call()?, None);
        Ok(())
    }

    #[test]
    fn should_find_no_more_function_calls_after_first_with_trailing_comma() -> Result<()> {
        let mut reader = FunctionReader::new("fn(),");
        assert_eq!(expect_function_call(&mut reader)?, call("fn", vec![]));
        assert_error!(
            reader.read_function_call(),
            "expected function call at position 5"
        );
        Ok(())
    }

    #[test]
    fn should_find_no_comma_between_parameters() {
        let mut reader = FunctionReader::new("fn(a b)");
        assert_error!(
            reader.read_function_call(),
            "expected comma or close parenthesis at position 5"
        );
    }

    #[test]
    fn should_find_no_comma_between_functions() -> Result<()> {
        let mut reader = FunctionReader::new("fn(1) other(2)");
        assert_eq!(
            expect_function_call(&mut reader)?,
            call("fn", vec![number(1)])
        );
        assert_error!(reader.read_function_call(), "expected comma at position 6");
        Ok(())
    }

    #[test]
    fn should_find_no_close_paren() {
        let mut reader = FunctionReader::new("fn(a, b");
        assert_error!(
            reader.read_function_call(),
            "expected close parenthesis at position 7"
        );
    }

    #[test]
    fn should_find_no_open_paren() {
        let mut reader = FunctionReader::new("fn");
        assert_error!(
            reader.read_function_call(),
            "expected open parenthesis at position 2"
        );
    }

    #[test]
    fn should_find_quoted_parameter() {
        let mut reader = FunctionReader::new("fn(\"abc\")");
        assert_error!(
            reader.read_function_call(),
            "expected parameter or close parenthesis at position 3"
        );
    }

    fn expect_function_call<'h>(reader: &mut FunctionReader<'h>) -> Result<FunctionCall<'h>> {
        reader
            .read_function_call()?
            .ok_or_else(|| eyre!("found no function call"))
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
