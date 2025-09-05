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
use color_eyre::eyre::{Result, eyre};

pub struct FunctionReader<'h> {
    haystack: &'h str,
    pos: usize,
}

pub struct FunctionCall {
    pub name: String,
    parameters: Vec<FunctionParameter>,
}

#[derive(Debug)]
pub enum FunctionParameter {
    String(String),
    Number(i64),
}

impl<'h> FunctionReader<'h> {
    pub fn new(haystack: &'h str) -> Self {
        FunctionReader { haystack, pos: 0 }
    }

    pub fn read_function_call(&mut self) -> Result<FunctionCall> {
        return Ok(FunctionCall {
            name: self.read_word()?,
            parameters: vec![
                FunctionParameter::String("value".to_string()),
                FunctionParameter::Number(1234),
            ],
        });
    }

    fn read_word(&mut self) -> Result<String> {
        self.ignore_whitespace();
        let mut result = String::new();
        while let Some(c) = self.read_char()
            && c.is_alphanumeric()
        {
            result.push(c);
            self.pos += 1;
        }
        Ok(result)
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
    pub fn string_param(&self, index: usize) -> Result<&String> {
        let param = self.param(index)?;
        if let FunctionParameter::String(value) = param {
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
    use super::FunctionReader;
    use color_eyre::eyre::Result;
    use test_log::test;

    // Tests:
    // - One function with no parameters
    // - One function with two parameters
    // - Two functions
    // - No functions
    // - Explore invalid possibilities

    #[test]
    fn should_read_word() -> Result<()> {
        let mut reader = FunctionReader::new("ageOff");
        Ok(assert_eq!(reader.read_word()?, "ageOff"))
    }

    #[test]
    fn should_read_first_word() -> Result<()> {
        let mut reader = FunctionReader::new("hey there");
        Ok(assert_eq!(reader.read_word()?, "hey"))
    }

    #[test]
    fn should_read_words_separated_by_whitespace() -> Result<()> {
        let mut reader = FunctionReader::new("hey there");
        assert_eq!(reader.read_word()?, "hey");
        Ok(assert_eq!(reader.read_word()?, "there"))
    }
}
