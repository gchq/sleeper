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
use color_eyre::eyre::{Result, ensure, eyre};

pub struct FunctionReader<'h> {
    haystack: &'h str,
    pos: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub struct FunctionCall {
    pub name: String,
    pub parameters: Vec<FunctionParameter>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum FunctionParameter {
    Word(String),
    Number(i64),
}

impl<'h> FunctionReader<'h> {
    pub fn new(haystack: &'h str) -> Self {
        FunctionReader { haystack, pos: 0 }
    }

    pub fn read_function_call(&mut self) -> Result<FunctionCall> {
        let name = self
            .read_word()
            .ok_or_else(|| eyre!("expected function name at position {}", self.pos))?;
        let parameters = self.read_parameters()?;
        return Ok(FunctionCall { name, parameters });
    }

    fn read_parameters(&mut self) -> Result<Vec<FunctionParameter>> {
        ensure!(
            self.read_expected_char('('),
            "expected open parenthesis at position {}",
            self.pos
        );
        let mut parameters = vec![];
        loop {
            if let Some(param) = self.read_parameter() {
                parameters.push(param);
                if self.is_next_parameter()? {
                    continue;
                } else {
                    ensure!(
                        self.read_expected_char(')'),
                        "expected comma or close parenthesis at position {}",
                        self.pos
                    );
                }
            } else {
                ensure!(
                    self.read_expected_char(')'),
                    "expected parameter or close parenthesis at position {}",
                    self.pos
                );
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
}

impl FunctionCall {
    pub fn word_param(&self, index: usize) -> Result<&String> {
        let param = self.param(index)?;
        if let FunctionParameter::Word(value) = param {
            Ok(value)
        } else {
            Err(eyre!(
                "expected string at parameter {index}, found {param:?}"
            ))
        }
    }

    pub fn number_param(&self, index: usize) -> Result<i64> {
        let param = self.param(index)?;
        if let FunctionParameter::Number(value) = param {
            Ok(*value)
        } else {
            Err(eyre!(
                "expected number at parameter {index}, found {param:?}"
            ))
        }
    }

    fn param(&self, index: usize) -> Result<&FunctionParameter> {
        self.parameters
            .get(index)
            .ok_or_else(|| eyre!("parameter not found at index {index}"))
    }
}

#[cfg(test)]
mod tests {

    use super::{FunctionCall, FunctionParameter, FunctionReader};
    use color_eyre::eyre::Result;
    use test_log::test;

    macro_rules! assert_error {
        ($err_expr: expr, $err_contents: expr) => {
            assert_eq!($err_expr.err().map(|e| e.to_string()), Some($err_contents))
        };
    }

    // Tests:
    // - One function with no parameters
    // - One function with two parameters
    // - Two functions
    // - No functions
    // - Explore invalid possibilities

    #[test]
    fn should_read_function_call() -> Result<()> {
        let mut reader = FunctionReader::new("answer(42)");
        Ok(assert_eq!(
            reader.read_function_call()?,
            FunctionCall {
                name: "answer".to_string(),
                parameters: vec![FunctionParameter::Number(42)]
            }
        ))
    }

    #[test]
    fn should_read_function_call_with_two_parameters() -> Result<()> {
        let mut reader = FunctionReader::new("fn(a,123)");
        Ok(assert_eq!(
            reader.read_function_call()?,
            FunctionCall {
                name: "fn".to_string(),
                parameters: vec![
                    FunctionParameter::Word("a".to_string()),
                    FunctionParameter::Number(123)
                ]
            }
        ))
    }

    #[test]
    fn should_read_function_call_with_no_parameters() -> Result<()> {
        let mut reader = FunctionReader::new("go()");
        Ok(assert_eq!(
            reader.read_function_call()?,
            FunctionCall {
                name: "go".to_string(),
                parameters: vec![]
            }
        ))
    }

    #[test]
    fn should_read_function_call_with_whitespace() -> Result<()> {
        let mut reader = FunctionReader::new(" fn ( a , 123 ) ");
        Ok(assert_eq!(
            reader.read_function_call()?,
            FunctionCall {
                name: "fn".to_string(),
                parameters: vec![
                    FunctionParameter::Word("a".to_string()),
                    FunctionParameter::Number(123)
                ]
            }
        ))
    }

    #[test]
    fn should_find_no_function_call() {
        let mut reader = FunctionReader::new("@/ #!");
        assert_error!(
            reader.read_function_call(),
            "expected function name at position 0".to_string()
        )
    }

    #[test]
    fn should_find_no_comma_between_parameters() {
        let mut reader = FunctionReader::new("fn(a b)");
        assert_error!(
            reader.read_function_call(),
            "expected comma or close parenthesis at position 5".to_string()
        )
    }

    #[test]
    fn should_find_no_close_paren() {
        let mut reader = FunctionReader::new("fn(a, b");
        assert_error!(
            reader.read_function_call(),
            "expected close parenthesis at position 7".to_string()
        )
    }

    #[test]
    fn should_find_no_open_paren() {
        let mut reader = FunctionReader::new("fn");
        assert_error!(
            reader.read_function_call(),
            "expected open parenthesis at position 2".to_string()
        )
    }

    #[test]
    fn should_find_quoted_parameter() {
        let mut reader = FunctionReader::new("fn(\"abc\")");
        assert_error!(
            reader.read_function_call(),
            "expected parameter or close parenthesis at position 3".to_string()
        )
    }
}
