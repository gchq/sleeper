//! Structs and functions relating to implementing Sleeper filters in Rust using
//! `DataFusion`.
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
pub mod ageoff;

#[cfg(test)]
mod tests {
    #[macro_export]
    macro_rules! assert_error {
        ($err_expr: expr, $err_type: path, $err_contents: expr) => {
            let result = if let Err($err_type(err)) = $err_expr {
                assert_eq!(err, $err_contents);
                true
            } else {
                false
            };
            assert!(result, "Expected different error type");
        };
    }
}
