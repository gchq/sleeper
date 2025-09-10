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
    iterator: CharIndices<'h>,
    current: Option<(usize, char)>,
    characters_read: usize,
    first_function: bool,
}

impl<'h> FunctionReader<'h> {
    pub fn new(haystack: &'h str) -> Self {
        let mut iterator = haystack.char_indices();
        let current = iterator.next();
        FunctionReader {
            haystack,
            iterator,
            current,
            characters_read: 0,
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
                    self.characters_read
                );
                return Ok(None);
            }
            ensure!(
                found_comma,
                "expected comma at position {}",
                self.characters_read
            );
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
                    Err(eyre!(
                        "expected function name at position {}",
                        self.characters_read
                    ))
                }
            }
        }
    }

    fn read_parameters(&mut self) -> Result<Vec<FunctionParameter<'h>>> {
        ensure!(
            self.read_expected_char('('),
            "expected open parenthesis at position {}",
            self.characters_read
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
                    self.characters_read
                );
            } else {
                return Err(eyre!(
                    "expected parameter or close parenthesis at position {}",
                    self.characters_read
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
            Err(eyre!(
                "expected close parenthesis at position {}",
                self.characters_read
            ))
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
        self.current = self.iterator.next();
        self.characters_read += 1;
    }

    fn at_end(&self) -> bool {
        self.current.is_none()
    }
}

impl<'h> Iterator for FunctionReader<'h> {
    type Item = Result<FunctionCall<'h>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_function_call() {
            Err(failure) => Some(Err(failure)),
            Ok(Some(call)) => Some(Ok(call)),
            Ok(None) => None,
        }
    }
}

#[cfg(test)]
mod tests {

    use super::FunctionReader;
    use crate::datafusion::filter_aggregation_config::function_call::{
        FunctionCall, FunctionParameter,
    };
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
    fn should_read_function_call() -> Result<()> {
        assert_eq!(
            read_function_calls("answer(42)")?,
            vec![call("answer", vec![number(42)])]
        );
        Ok(())
    }

    #[test]
    fn should_read_two_function_calls() -> Result<()> {
        assert_eq!(
            read_function_calls("answer(42), question(earth)")?,
            vec![
                call("answer", vec![number(42)]),
                call("question", vec![word("earth")])
            ]
        );
        Ok(())
    }

    #[test]
    fn should_read_function_call_with_two_parameters() -> Result<()> {
        assert_eq!(
            read_function_calls("fn(a,123)")?,
            vec![call("fn", vec![word("a"), number(123)])]
        );
        Ok(())
    }

    #[test]
    fn should_read_function_call_with_no_parameters() -> Result<()> {
        assert_eq!(read_function_calls("go()")?, vec![call("go", vec![])]);
        Ok(())
    }

    #[test]
    fn should_read_function_call_with_whitespace() -> Result<()> {
        assert_eq!(
            read_function_calls(" fn ( a , 123 ) ")?,
            vec![call("fn", vec![word("a"), number(123)])]
        );
        Ok(())
    }

    #[test]
    fn should_find_bad_characters_at_function_name() {
        assert_error!(
            read_function_calls("@/ #!"),
            "expected function name at position 0"
        );
    }

    #[test]
    fn should_find_bad_characters_at_field_name() {
        assert_error!(
            read_function_calls("fn(@/ #!)"),
            "expected parameter or close parenthesis at position 3"
        );
    }

    #[test]
    fn should_find_bad_character_in_function_name() {
        assert_error!(
            read_function_calls("fn@1()"),
            "expected open parenthesis at position 2"
        );
    }

    #[test]
    fn should_find_bad_character_in_field_name() {
        assert_error!(
            read_function_calls("fn(field@0)"),
            "expected comma or close parenthesis at position 8"
        );
    }

    #[test]
    fn should_read_function_call_with_underscore() -> Result<()> {
        assert_eq!(
            read_function_calls("some_fn(a_field)")?,
            vec![call("some_fn", vec![word("a_field")])]
        );
        Ok(())
    }

    #[test]
    fn should_read_function_call_with_hyphen() -> Result<()> {
        assert_eq!(
            read_function_calls("some-fn(a-field)")?,
            vec![call("some-fn", vec![word("a-field")])]
        );
        Ok(())
    }

    #[test]
    fn should_read_function_call_with_unicode_characters_spanning_multiple_bytes() -> Result<()> {
        assert_eq!(
            read_function_calls("存在する(ひほわれよう)")?,
            vec![call("存在する", vec![word("ひほわれよう")])]
        );
        Ok(())
    }

    #[test]
    fn should_find_no_function_call() -> Result<()> {
        assert_eq!(read_function_calls("")?, vec![]);
        Ok(())
    }

    #[test]
    fn should_find_trailing_comma() {
        assert_error!(
            read_function_calls("fn(),"),
            "expected function call at position 5"
        );
    }

    #[test]
    fn should_find_no_comma_between_parameters() {
        assert_error!(
            read_function_calls("fn(a b)"),
            "expected comma or close parenthesis at position 5"
        );
    }

    #[test]
    fn should_find_no_comma_between_functions() {
        assert_error!(
            read_function_calls("fn(1) other(2)"),
            "expected comma at position 6"
        );
    }

    #[test]
    fn should_find_no_close_paren() {
        assert_error!(
            read_function_calls("fn(a, b"),
            "expected close parenthesis at position 7"
        );
    }

    #[test]
    fn should_find_no_open_paren() {
        assert_error!(
            read_function_calls("fn"),
            "expected open parenthesis at position 2"
        );
    }

    #[test]
    fn should_find_quoted_parameter() {
        assert_error!(
            read_function_calls("fn(\"abc\")"),
            "expected parameter or close parenthesis at position 3"
        );
    }

    fn read_function_calls(haystack: &'static str) -> Result<Vec<FunctionCall<'static>>> {
        FunctionReader::new(haystack).collect()
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
